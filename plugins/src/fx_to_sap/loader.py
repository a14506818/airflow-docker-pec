from pyrfc import Connection
import pandas as pd
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
    print("SAP Connection Parameters:", conn_params)
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

