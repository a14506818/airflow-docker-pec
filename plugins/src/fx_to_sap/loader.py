import os
from dotenv import load_dotenv
from pyrfc import Connection
import pandas as pd
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from pprint import pprint


def write_data_to_sap(**context):
    # get XCOM -----------------------------------------------------------------------------------------------
    ti = context["ti"] # 取得 Task Instance
    xcom = ti.xcom_pull(task_ids="clean_data_for_sap")
    df = pd.DataFrame(xcom) # 將 XCOM 轉換成 DataFrame
    if df.empty:
        raise ValueError("❌ 轉換成 DataFrame 後為空，請檢查上游任務")
    print("✅ 成功取得xcom，前幾筆資料如下：")
    print(df.head())

    # 載入環境變數 -------------------------------------------------------------------------------------------
    load_dotenv(dotenv_path="/opt/airflow/dags/.env") # 載入 .env 變數
    try:
        conn_params = {
            "user": os.getenv("SAP_USER"),
            "passwd": os.getenv("SAP_PASS"),
            "ashost": os.getenv("SAP_ASHOST"),
            "sysnr": os.getenv("SAP_SYSNR"),
            "client": os.getenv("SAP_CLIENT"),
            "lang": os.getenv("SAP_LANG", "EN"),
        }

        print("Connecting to SAP via RFC...")
        conn = Connection(**conn_params)

    except Exception as e:
        raise ConnectionError(f"❌ 無法連接到 SAP: {str(e)}")

    # 準備寫入資料 -------------------------------------------------------------------------------------------
    rows = []
    for _, row in df.iterrows():
        rows.append({
            "FROM_CURR": str(row["FROM_CURR"]),
            "TO_CURRNCY": str(row["TO_CURRNCY"]),
            "EXCH_RATE": f"{float(row['EXCH_RATE']):.5f}",  # 強制轉字串後轉 Decimal
            "VALID_FROM": row["VALID_FROM"].strftime("%Y%m%d"),
            "RATE_TYPE": str(row["RATE_TYPE"]), 
            # 強迫填寫欄位 ------------------------------
            "FROM_FACTOR": "1",             
            "TO_FACTOR": "1",
            "EXCH_RATE_V": "1.00000",
            "FROM_FACTOR_V": "1",
            "TO_FACTOR_V": "1",
            # ------------------------------------------
        })

    if not rows:
        raise ValueError("❌ 要寫入 SAP 的資料為空")

    print("✅ 準備寫入 SAP 的資料如下：")
    for row in rows:
        print(row)

    # 寫入 RFC
    try:
        result = conn.call("Z_FI_EXCHANGE_RATE_CREATE", LT_RATE=rows)
        print("✅ 寫入 SAP 成功:", result)
    except Exception as e:
        import traceback
        print("❌ 詳細錯誤回傳如下：")
        traceback.print_exc()
        raise RuntimeError(f"❌ 寫入 SAP 發生錯誤: {str(e)}")

