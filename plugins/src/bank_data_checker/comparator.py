import pandas as pd

import pendulum
from datetime import datetime, date

def compare_sap_and_crawl_bank_list(**context):
    """
    比對 SAP 與爬蟲銀行清單，找出 SAP 有但爬蟲沒有的銀行
    傳入兩個 DataFrame，需有欄位 bank_code
    回傳只存在於 SAP 的銀行清單
    """
     # get XCOM -----------------------------------------------------------------------------------------------
    ti = context["ti"] # 取得 Task Instance
    sap_dict = ti.xcom_pull(task_ids="get_SAP_partner_bank_list")
    sap_df = pd.DataFrame(sap_dict)
    if sap_df.empty:
        raise ValueError("❌ 轉換成 DataFrame 後為空，請檢查上游任務")
    print("✅ 成功取得xcom，前幾筆資料如下：")
    # print(sap_df)
# =
    crawl_dict = ti.xcom_pull(task_ids="crawl_bank_data")
    crawl_df = pd.DataFrame(crawl_dict)
    if crawl_df.empty:
        raise ValueError("❌ 轉換成 DataFrame 後為空，請檢查上游任務")
    print("✅ 成功取得xcom，前幾筆資料如下：")
    # print(crawl_df)

    crawl_2_dict = ti.xcom_pull(task_ids="crawl_bank_data_2")
    crawl_2_df = pd.DataFrame(crawl_2_dict)
    if crawl_2_df.empty:
        raise ValueError("❌ 轉換成 DataFrame 後為空，請檢查上游任務")
    print("✅ 成功取得xcom，前幾筆資料如下：")
    # print(crawl_2_df)

    # 合併兩個爬蟲結果 只保留 bank_code 欄位 (兩邊欄位不一致)
    crawl_df = pd.concat([crawl_df[["bank_code"]], crawl_2_df[["bank_code"]]], ignore_index=True)

    # 去除空值與重複 --------------------------------------------------------------------------------------------
    sap_df = sap_df.dropna(subset=["bank_code"]).drop_duplicates()
    crawl_df = crawl_df.dropna(subset=["bank_code"]).drop_duplicates()

    print("✅ sap_df")
    print(sap_df.head())
    print("✅ crawl_df")
    print(crawl_df.head())

    # 找出 SAP 有但爬蟲沒有的 bank_code
    diff_df = sap_df[~sap_df["bank_code"].isin(crawl_df["bank_code"])]

    print("✅ diff_df：")
    print(diff_df.head())

    return diff_df.to_dict("records")  # ❗XCom 不支援直接傳 df，要先轉成 dict

def gen_attchments(**context):
    ti = context["ti"] # 取得 Task Instance
    # get XCOM -----------------------------------------------------------------------------------------------
    sap_data = ti.xcom_pull(task_ids="get_SAP_partner_bank_list")
    if not sap_data:
        raise ValueError("❌ 轉換成 DataFrame 後為空，請檢查上游任務")
    sap_df = pd.DataFrame(sap_data)
    print("✅ 成功取得xcom，資料如下：")
    print(sap_df)

    # get XCOM -----------------------------------------------------------------------------------------------
    crawl_data = ti.xcom_pull(task_ids="crawl_bank_data")
    if not crawl_data:
        raise ValueError("❌ 轉換成 DataFrame 後為空，請檢查上游任務")
    crawl_df = pd.DataFrame(crawl_data)
    print("✅ 成功取得xcom，資料如下：")
    print(crawl_df)

    # get XCOM -----------------------------------------------------------------------------------------------
    crawl_2_data = ti.xcom_pull(task_ids="crawl_bank_data_2")
    if not crawl_2_data:
        raise ValueError("❌ 轉換成 DataFrame 後為空，請檢查上游任務")
    crawl_2_df = pd.DataFrame(crawl_2_data)
    print("✅ 成功取得xcom，資料如下：")
    print(crawl_2_df)

    # get XCOM -----------------------------------------------------------------------------------------------
    diff_data = ti.xcom_pull(task_ids="compare_sap_and_crawl_bank_list")
    if not diff_data:
        raise ValueError("❌ 轉換成 DataFrame 後為空，請檢查上游任務")
    diff_df = pd.DataFrame(diff_data)
    print("✅ 成功取得xcom，資料如下：")
    print(diff_df)

    # 檔名與路徑 ----------------------------------------------------------------------------------------------
    tz = pendulum.timezone("Asia/Taipei")
    start_date = context["dag_run"].start_date
    if not isinstance(start_date, pendulum.DateTime):
        start_date = pendulum.instance(start_date)
    local_time = start_date.in_timezone(tz)

    # 寫入 Excel 檔案 ----------------------------------------------------------------------------------------
    file_name = f"FinalData__{context['dag'].dag_id}__{local_time.strftime('%Y%m%d_%H%M')}.xlsx"
    file_path = f"/opt/airflow/export/{file_name}"

    print("開始匯出結果至Excel...")
    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        sheet_written = False
        if not diff_df.empty:
            diff_df.to_excel(writer, sheet_name="比對結果", index=False)
            sheet_written = True

        if not sap_df.empty:
            sap_df.to_excel(writer, sheet_name="SAP供應商_銀行清單", index=False)
            sheet_written = True

        if not crawl_df.empty:
            crawl_df.to_excel(writer, sheet_name="銀行局清單", index=False)
            sheet_written = True

        if not crawl_2_df.empty:
            crawl_2_df.to_excel(writer, sheet_name="金資中心清單", index=False)
            sheet_written = True
        # 如果都沒資料，至少寫入一個空 sheet
        if not sheet_written:
            pd.DataFrame({"empty": []}).to_excel(writer, sheet_name="EMPTY", index=False)
            print("❌ 查無任何結果，已生成空白 sheet。")
        else:
            print(f"✅ 成功寫入 Excel 檔案: {file_path}")

    file_path_list = [file_path]  # 將檔案路徑放入列表中
    return file_path_list