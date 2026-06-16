import pandas as pd
from datetime import datetime
import pendulum
from pyrfc import Connection

from src.supplier_assessment_tasks.db_handler import DBHandler

from src.common.common import get_mssql_conn_str, get_sap_conn_params

current_year = datetime.now().year
last_year = current_year - 1
last_last_year = last_year - 1

def get_GR_data(): 
    """
    獲取 GR 資料
    """
    conn_params = get_sap_conn_params()
    conn = Connection(**conn_params)

    date_FM = str(last_last_year) + "0701"  # 前年7月1日
    date_TO = str(current_year) + "0630"  # 今年6月30日

    # 呼叫 RFC
    params = {'PI_LIFNR': "", 'PI_ZACCEPTDAT_B': date_FM, 'PI_ZACCEPTDAT_E': date_TO}
    rfc_result = conn.call('Z_MM_BPM_007', **params)

    # 取得回傳的資料表
    pt_out = rfc_result.get('PT_OUT', [])  # PT_OUT
    df = pd.DataFrame(pt_out)

    print("✅ 成功取得 GR 資料，前幾筆資料如下：")
    print(df.head())
    return df.to_dict("records")  # ❗XCom 不支援直接傳 df，要先轉成 dict

def clean_data(**context):
    """
    清理 GR 資料
    """
    ti = context["ti"]  # 取得 Task Instance
    gr_data = ti.xcom_pull(task_ids="get_GR_data")
    
    if not gr_data:
        raise ValueError("❌ 轉換成 DataFrame 後為空，請檢查上游任務")

    df = pd.DataFrame(gr_data)
    print("✅ 成功取得 GR 資料，前幾筆資料如下：")
    print(df.head())

    # 清理資料邏輯 --------------------------------------------------------------------------------------------
    db_handler = DBHandler()
    # 自行轉換年度 ( 7/1~ 6/30 為一年度 ) ***無法套用於和並過的資料***
    # df["BUDAT_Date"] = pd.to_datetime(df["BUDAT"], format="%Y%m%d", errors="coerce")
    # df["LFGJA"] = df["BUDAT_Date"].apply(lambda x: str(x.year) if x.month <= 6 else str(x.year + 1))
    # print("✅ 成功轉換年度，前幾筆資料如下：")
    # print(df[["BUDAT", "LFGJA"]].head())

    # 帶出匯率，金額轉換成台幣 ----------------------------------------------------------------------------------
    # 取得匯率資料
    fx_rate_df = db_handler.get_BPM_fx_rate()
    print("✅ 成功取得匯率資料，前幾筆資料如下：")
    print(fx_rate_df)
    # 將匯率資料轉換成字典，方便後續查詢
    rate_map = {}
    rate_map['TWD'] = 1.0  # TWD 為基準幣別
    rate_map[''] = 1.0
    for index, row in fx_rate_df.iterrows():
        key = row['Currency']
        rate_map[key] = row['Rate']

    # 確保 NETWR 是數值
    df["NETWR"] = pd.to_numeric(df["NETWR"], errors="coerce")
    # 去除 NA 幣別
    df["WAERS"] = df["WAERS"].fillna("")
    # 轉換金額
    df["NETWR"] = df.apply(lambda row: row["NETWR"] * float(rate_map.get(row["WAERS"], 0)), axis=1)
    df["WAERS"] = "TWD"  # 將幣別統一為 TWD
    print("✅ 成功轉換金額，前幾筆資料如下：")
    print(df[["NETWR", "WAERS"]].head())

    # if  去年有考核: 排除 prev year GR record ------------------------------------------------------------------
    # 取得BPM考核紀錄
    assessment_result_df = db_handler.get_BPM_assessment_result(last_year)
    print("✅ 成功取得 BPM 考核結果，前幾筆資料如下：")
    print(assessment_result_df.head())
    # 抓出去年有考核的廠商
    condition = (assessment_result_df["Comment"] != "")
    last_year_assessed_suppliers = assessment_result_df[condition].copy()
    print("✅ 成功取得去年有考核的廠商，前幾筆資料如下：")
    print(last_year_assessed_suppliers[["WERKS", "PARTNER"]].head())
    # 去年有考核的廠商，排除 prev year GR record
    last_year_assessed_suppliers_key = set(last_year_assessed_suppliers["PARTNER"] + "_" + last_year_assessed_suppliers["WERKS"])
    df_key = df["PARTNER"] + "_" + df["WERKS"]
    condition = (df["LFGJA"] == str(last_year)) & (df_key.isin(last_year_assessed_suppliers_key))
    df = df[~condition].copy()
    print("✅ 成功排除去年有考核的廠商，前幾筆資料如下：")
    print(df.head())

    # if 去年 AB級: 壓上 "去年考核AB級" ------------------------------------------------------------------------
    # 先抓出去年 AB級的廠商
    condition = (assessment_result_df["Comment"].isin(["A", "B"]))
    last_year_ab_suppliers = assessment_result_df[condition].copy()
    # 補上 "Comment" 欄位
    df = pd.merge(
        df,
        assessment_result_df,
        on=['WERKS', 'PARTNER'],
        how='left',  # 保留今年資料，左連接
        suffixes=('', '_AR')  # 防止欄位衝突
    )
    df = df.fillna("")
    # 補上 "SkipReason" 欄位
    df["SkipReason"] = df["UNW_REMARK"]
    df.loc[df["WERKS"]=='1002', "SkipReason"] = "" # 台中不套用
    # 去年 AB級的廠商，壓上 "去年考核AB級"
    last_year_ab_suppliers_key = set(last_year_ab_suppliers["PARTNER"] + "_" + last_year_ab_suppliers["WERKS"])
    df_key = df["PARTNER"] + "_" + df["WERKS"]
    condition = (df_key.isin(last_year_ab_suppliers_key))
    df.loc[condition, "SkipReason"] = "去年考核AB級"
    print("✅ 成功壓上去年考核AB級，前幾筆資料如下：")
    print(df[["PARTNER", "WERKS", "SkipReason"]].head())

    db_handler.shutdown()
    return df.to_dict("records")  # ❗XCom 不支援直接傳 df，要先轉成 dict

def group_data(**context):
    """
    將 GR 資料依照廠商和工廠分組
    """
    db_handler = DBHandler()

    ti = context["ti"]  # 取得 Task Instance --------------------------------------------------------------
    gr_data = ti.xcom_pull(task_ids="clean_data")
    
    if not gr_data:
        raise ValueError("❌ 轉換成 DataFrame 後為空，請檢查上游任務")

    df = pd.DataFrame(gr_data)
    print("✅ 成功取得 GR 資料，前幾筆資料如下：")
    print(df.head())

    # 執行 group by + 聚合 (年度、廠別、供應商) -------------------------------------------------------------
    # 2026-05-20 更新 group by 邏輯：不再以 ACCOUNT 為基礎，改為以 (WERKS, PARTNER) 為基礎，從原始資料中選出代表 ACCOUNT
        # 抓取邏輯如下：驗收人帳號 (ACCOUNT) 出現次數最多的 → 該帳號的 NETWR 總金額最高的 → ACCOUNT 字母序最大的
        
    # Step 1: 先針對 (WERKS, PARTNER, ACCOUNT) 計算 出現次數 與 NETWR 總和
    account_stats = (
        df.groupby(["WERKS", "PARTNER", "ACCOUNT"], as_index=False)
        .agg(
            AccountCount=("ACCOUNT", "size"),
            AccountAmount=("NETWR", "sum"),
        )
    )

    # Step 2: 依照 出現次數 desc → 該 account 總金額 desc → ACCOUNT desc 排序
    account_stats = account_stats.sort_values(
        by=["WERKS", "PARTNER", "AccountCount", "AccountAmount", "ACCOUNT"],
        ascending=[True, True, False, False, False]
    )

    # Step 3: 每個 (WERKS, PARTNER) 取第一筆作為代表 ACCOUNT
    representative_account = (
        account_stats.groupby(["WERKS", "PARTNER"], as_index=False)
                    .first()[["WERKS", "PARTNER", "ACCOUNT"]]
    )

    # Step 4: 主 group by(將原本的 ACCOUNT 聚合拿掉)
    grouped_df = df.groupby(["WERKS", "PARTNER"], as_index=False).agg({
        "LFGJA": lambda x: ', '.join(sorted(set(x))),  # 年度 e.g. 2023, 2024
        "NAME_ORG1": "first",   # 供應商名稱
        "TAXNUM": "first",      # 統編
        "NETWR": "sum",         # 總金額
        "WAERS": "first",       # 幣別
        # "ACCOUNT": 已改用 representative_account 處理
        "EKNAM": "first",       # 採購員 姓名
        "SkipReason": "first",  # 跳過原因
        "Comment": "first",     # 考核結果
    })

    # Step 5: 把代表 ACCOUNT merge 回主表
    grouped_df = pd.merge(
        grouped_df,
        representative_account,
        on=["WERKS", "PARTNER"],
        how="left"
    )
    # ---------------------------------------------------------------------------------------------------

    # 用 ACCOUNT 從原始 df 中對應出 NAME1_TEXT
    name_map = df[["ACCOUNT", "NAME1_TEXT"]].dropna().drop_duplicates()
    df = pd.merge(grouped_df, name_map, on="ACCOUNT", how="left")
    print("✅ 成功對應出 NAME1_TEXT，前幾筆資料如下：")
    print(df.head())

    # 抓出 BPM 帳號資料 -----------------------------------------------------------------------------------
    bpm_account_df = db_handler.get_BPM_account()
    print("✅ 成功取得 BPM 帳號資料，前幾筆資料如下：")
    print(bpm_account_df.head())
    # 依廠別指定 採購員
    buyer_account_df = pd.DataFrame([
        {"WERKS": "1001", "BuyerAccount": "lillian_wang", "BuyerName": "王靜慧 Lillian Wang"},
        {"WERKS": "1002", "BuyerAccount": "", "BuyerName": "台中採"}
        # {"WERKS": "1002", "BuyerAccount": "emily_wu", "BuyerName": "吳瑞景 Emily Wu"}
    ])

    # join BPM 帳號 (驗收人、採購員)
    df = pd.merge(
        df,
        bpm_account_df,
        left_on='ACCOUNT',
        right_on='HRID',
        how='left',
        suffixes=('', '_BPM')
    )
    df = pd.merge(
        df,
        buyer_account_df,
        left_on='WERKS',
        right_on='WERKS',
        how='left',
        suffixes=('', '_Buyer')
    )
    df = df.fillna("")
    # 若 對應不到BPM account，則使用 原驗收人 name
    df.loc[df["AssessorName"] == "", "AssessorName"] = df["NAME1_TEXT"]
    print("✅ 成功 join BPM 帳號，前幾筆資料如下：")
    print(df.head())

    # 整理欄位名稱、刪除多餘欄位 --------------------------------------------------------------------------------------
    df = df.rename(columns={
        'LFGJA': 'GR_Year',
        'WERKS': 'Plant',
        'PARTNER': 'PartnerCode',
        'NAME_ORG1': 'PartnerName',
        'TAXNUM': 'TaxNum',
        'NETWR': 'Amount',
        'Comment': 'LastYearComment'
    })
    df = df[[
        'Plant', 'PartnerCode', 'GR_Year', 'PartnerName', 'TaxNum', 'Amount',
        'WAERS', 'AssessorAccount', 'AssessorName', 'AssessorDept', 'AssessorDeptShort', 'BuyerAccount', 'BuyerName',
        'LastYearComment', "SkipReason"
    ]]
    # 補上 考核年度
    df["AssessmentYear"] = current_year
    print("✅ 成功整理欄位名稱，前幾筆資料如下：")
    print(df.head())

    db_handler.shutdown()
    return df.to_dict("records")  # ❗XCom 不支援直接傳 df，要先轉成 dict

def insert_pending_list(**context):
    """
    將分組後的 GR 資料寫入資料庫
    """
    db_handler = DBHandler()
    grouped_data = context["ti"].xcom_pull(task_ids="group_data")
    
    if not grouped_data:
        raise ValueError("❌ 轉換成 DataFrame 後為空，請檢查上游任務")

    df = pd.DataFrame(grouped_data)
    print("✅ 成功取得分組後的 GR 資料，前幾筆資料如下：")
    print(df.head())

    # 清空 TMP_PendingSupplierAssessment 資料表
    db_handler.del_table('TMP_PendingSupplierAssessment')
    print("✅ 成功清空 TMP_PendingSupplierAssessment 資料表")

    # 寫入資料庫
    db_handler.insert_to_TMP_PendingSupplierAssessment(df)
    print("✅ 成功寫入 BPM 考核結果資料表")

    db_handler.shutdown()

def insert_crawler_list(**context): # REF_partner_crawler_list
    """
    將爬蟲清單寫入資料庫
    """
    db_handler = DBHandler()
    grouped_data = context["ti"].xcom_pull(task_ids="group_data")
    
    if not grouped_data:
        raise ValueError("❌ 轉換成 DataFrame 後為空，請檢查上游任務")

    df = pd.DataFrame(grouped_data)
    print("✅ 成功取得分組後的 GR 資料，前幾筆資料如下：")
    print(df.head())
    print(df.info())

    # 排除 台北不考核 廠商
    partner_df = pd.DataFrame()
    partner_df['partner_name'] = df[~df["SkipReason"].str.contains("不考核")][["PartnerName"]].drop_duplicates()
    print("✅ 成功排除不考核廠商，前幾筆資料如下：")
    print(partner_df.head())

    # 清空 REF_partner_crawler_list 資料表
    db_handler.del_table('REF_partner_crawler_list')
    print("✅ 成功清空 REF_partner_crawler_list 資料表")

    # 寫入資料庫
    db_handler.insert_to_REF_partner_crawler_list(partner_df)
    print("✅ 成功寫入 BPM 考核結果資料表")

    db_handler.shutdown()

def gen_attchments(**context):
    """
    生成附件
    """
    db_handler = DBHandler()
    # 取得資料
    pending_list_df = db_handler.get_specific_table('TMP_PendingSupplierAssessment')
    crawler_list_df = db_handler.get_specific_table('REF_partner_crawler_list')
    clean_data = context["ti"].xcom_pull(task_ids="clean_data")
    clean_data_df = pd.DataFrame(clean_data)
    get_GR_data = context["ti"].xcom_pull(task_ids="get_GR_data")
    get_GR_data_df = pd.DataFrame(get_GR_data)

    print(pending_list_df.head())
    print(crawler_list_df.head())
    print(clean_data_df.head())
    print(get_GR_data_df.head())

    # 檔名與路徑
    tz = pendulum.timezone("Asia/Taipei")
    start_date = context["dag_run"].start_date
    if not isinstance(start_date, pendulum.DateTime):
        start_date = pendulum.instance(start_date)
    local_time = start_date.in_timezone(tz)
    file_name = f"FinalData__{context['dag'].dag_id}__{local_time.strftime('%Y%m%d_%H%M')}.xlsx"
    file_path = f"/opt/airflow/export/{file_name}"

    print("開始匯出結果至Excel...")

    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        sheet_written = False

        if not pending_list_df.empty:
            pending_list_df.to_excel(writer, sheet_name="待考核供應商", index=False)
            sheet_written = True

        if not crawler_list_df.empty:
            crawler_list_df.to_excel(writer, sheet_name="爬蟲清單", index=False)
            sheet_written = True

        if not clean_data_df.empty:
            clean_data_df.to_excel(writer, sheet_name="GR資料(Clean)", index=False)
            sheet_written = True
        
        if not get_GR_data_df.empty:
            get_GR_data_df.to_excel(writer, sheet_name="GR資料(Raw)", index=False)
            sheet_written = True

        # 如果都沒資料，至少寫入一個空 sheet
        if not sheet_written:
            pd.DataFrame({"empty": []}).to_excel(writer, sheet_name="EMPTY", index=False)
            print("❌ 查無任何結果，已生成空白 sheet。")
        else:
            print(f"✅ 成功寫入 Excel 檔案: {file_path}")

    db_handler.shutdown()  
    print("✅ 附件生成完成")
    return [file_path]  # 將檔案路徑放入列表中