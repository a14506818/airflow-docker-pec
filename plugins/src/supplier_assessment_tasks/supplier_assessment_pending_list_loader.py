import pandas as pd
from datetime import datetime
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
    # 定義最多次出現的值（mode）(重複值取最後一個)
    def most_common(x):
        return x.mode().iloc[-1] if not x.mode().empty else None

    ti = context["ti"]  # 取得 Task Instance --------------------------------------------------------------
    gr_data = ti.xcom_pull(task_ids="clean_data")
    
    if not gr_data:
        raise ValueError("❌ 轉換成 DataFrame 後為空，請檢查上游任務")

    df = pd.DataFrame(gr_data)
    print("✅ 成功取得 GR 資料，前幾筆資料如下：")
    print(df.head())

    # 執行 group by + 聚合 (年度、廠別、供應商)
    grouped_df = df.groupby(["WERKS", "PARTNER"], as_index=False).agg({
        "LFGJA": lambda x: ', '.join(sorted(set(x))), # 年度 e.g. 2023, 2024
        "NAME_ORG1": "first", # 供應商名稱
        "TAXNUM": "first", # 統編
        "NETWR": "sum", # 總金額
        "WAERS": "first", # 幣別
        "ACCOUNT": most_common, # 驗收人 工號
        # "NAME1_TEXT": most_common, # 要用 ACCOUNT 對應出 NAME1_TEXT
        "EKNAM": "first", # 採購員 姓名
        "SkipReason": "first", # 跳過原因
    })

    # 用 ACCOUNT 從原始 df 中對應出 NAME1_TEXT
    name_map = df[["ACCOUNT", "NAME1_TEXT"]].dropna().drop_duplicates()
    grouped_df = pd.merge(grouped_df, name_map, on="ACCOUNT", how="left")
    print("✅ 成功對應出 NAME1_TEXT，前幾筆資料如下：")
    print(grouped_df.head())
    
    