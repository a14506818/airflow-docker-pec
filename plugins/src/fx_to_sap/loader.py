from pyrfc import Connection
import pyodbc
import pandas as pd
import pendulum
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from pprint import pprint

from src.common.common import get_mssql_conn_str, get_sap_conn_params


def write_data_to_sap(**context):
    # get XCOM -----------------------------------------------------------------------------------------------
    ti = context["ti"] # 取得 Task Instance
    final_data = ti.xcom_pull(task_ids="clean_data_for_sap")
    
    if not final_data:
        raise ValueError("❌ 轉換成 DataFrame 後為空，請檢查上游任務")

    pprint("✅ 成功取得xcom，資料如下：")
    pprint(final_data)

    # 載入環境變數 -------------------------------------------------------------------------------------------
    conn_params = get_sap_conn_params()
    conn = Connection(**conn_params)

    # 寫入 RFC
    try:
        result = conn.call("Z_FI_EXCHANGE_RATE_CREATE", LT_RATE=final_data)
        print("✅ 寫入 SAP 成功:", result)
    except Exception as e:
        import traceback
        print("❌ 詳細錯誤回傳如下：")
        traceback.print_exc()
        raise RuntimeError(f"❌ 寫入 SAP 發生錯誤: {str(e)}")

def write_data_to_bpm(**context):
    # get XCOM -----------------------------------------------------------------------------------------------
    ti = context["ti"] # 取得 Task Instance
    final_data = ti.xcom_pull(task_ids="clean_data_for_bpm")
    
    if not final_data:
        raise ValueError("❌ 轉換成 DataFrame 後為空，請檢查上游任務")

    pprint("✅ 成功取得xcom，資料如下：")
    pprint(final_data)

    # 載入環境變數 -------------------------------------------------------------------------------------------
    conn_str = get_mssql_conn_str()
    conn = pyodbc.connect(conn_str, timeout=5)
    print("✅ MSSQL 連線成功")

    # 寫入資料庫
    cursor = conn.cursor()
    for row in final_data:
        # 檢查數值是否為有效的 Decimal
        try:
            row['Rate'] = Decimal(str(row['Rate']))
        except InvalidOperation:
            raise ValueError(f"❌ 無效的匯率數值: {row['Rate']}")

        cursor.execute("""
            MERGE INTO ExchangeRate AS target
            USING (SELECT ? AS Currency, ? AS Currency2, ? AS Rate, ? AS RateDate) AS source
            ON target.Currency = source.Currency AND target.Currency2 = source.Currency2 AND target.RateDate = source.RateDate
            WHEN MATCHED THEN
                UPDATE SET Rate = source.Rate
            WHEN NOT MATCHED THEN
                INSERT (Currency, Currency2, Rate, RateDate)
                VALUES (source.Currency, source.Currency2, source.Rate, source.RateDate);
        """, row['Currency'], row['Currency2'], row['Rate'], row['RateDate'])

    conn.commit()
    print("✅ 寫入 BPM 成功")

    conn.close()

def gen_attchments(**context):
    # get XCOM -----------------------------------------------------------------------------------------------
    ti = context["ti"] # 取得 Task Instance
    final_data = ti.xcom_pull(task_ids="clean_data_for_sap")
    
    if not final_data:
        raise ValueError("❌ 轉換成 DataFrame 後為空，請檢查上游任務")

    pprint("✅ 成功取得xcom，資料如下：")
    pprint(final_data)

    tz = pendulum.timezone("Asia/Taipei")
    local_time = context["execution_date"].in_timezone(tz)

    # 寫入 Excel 檔案 ----------------------------------------------------------------------------------------
    df = pd.DataFrame(final_data)
    file_name = f"FinalData__{context['dag'].dag_id}__{local_time.strftime('%Y%m%d_%H%M')}.xlsx"
    file_path = f"/opt/airflow/export/{file_name}"
    df.to_excel(file_path, index=False)
    print(f"✅ 成功寫入 Excel 檔案: {file_path}")

    file_path_list = [file_path]  # 將檔案路徑放入列表中
    return file_path_list