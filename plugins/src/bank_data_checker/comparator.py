import pandas as pd

def compare_sap_and_crawl_bank_list(**context):
    """
    比對 SAP 與爬蟲銀行清單，找出 SAP 有但爬蟲沒有的銀行
    傳入兩個 DataFrame，需有欄位 bank_code
    回傳只存在於 SAP 的銀行清單
    """
     # get XCOM -----------------------------------------------------------------------------------------------
    ti = context["ti"] # 取得 Task Instance
    sap_dict = ti.xcom_pull(task_ids="get_SAP_bank_list")
    sap_df = pd.DataFrame(sap_dict)
    if sap_df.empty:
        raise ValueError("❌ 轉換成 DataFrame 後為空，請檢查上游任務")
    print("✅ 成功取得xcom，前幾筆資料如下：")
    # print(sap_df)
# =
    scrawl_dict = ti.xcom_pull(task_ids="crawl_bank_data")
    crawl_df = pd.DataFrame(scrawl_dict)
    if crawl_df.empty:
        raise ValueError("❌ 轉換成 DataFrame 後為空，請檢查上游任務")
    print("✅ 成功取得xcom，前幾筆資料如下：")
    # print(crawl_df)

    # 去除空值與重複 --------------------------------------------------------------------------------------------
    sap_df = sap_df.dropna(subset=["bank_code"]).drop_duplicates()
    crawl_df = crawl_df.dropna(subset=["bank_code"]).drop_duplicates()

    print("✅ sap_df")
    print(sap_df)
    print(sap_df.info())
    print("✅ crawl_df")
    print(crawl_df)
    print(crawl_df.info())

    # 找出 SAP 有但爬蟲沒有的 bank_code
    diff_df = sap_df[~sap_df["bank_code"].isin(crawl_df["bank_code"])]

    print("✅ diff_df：")
    print(diff_df)
    print(diff_df.info())

    return diff_df.to_dict("records")  # ❗XCom 不支援直接傳 df，要先轉成 dict