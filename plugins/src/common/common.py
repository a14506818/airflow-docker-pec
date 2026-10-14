import os
from dotenv import load_dotenv
import pendulum
from datetime import timedelta
from airflow.utils.types import DagRunType

load_dotenv(dotenv_path="/opt/airflow/dags/.env") # 載入 .env 變數

# connection -------------------------------------------------------------------------------------
def get_mssql_conn_str():
    driver = os.getenv("DB_DRIVER")
    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_DATABASE")
    username = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
    )
    return conn_str

def get_sap_conn_params():
    return {
        "user": os.getenv("SAP_USER"),
        "passwd": os.getenv("SAP_PASS"),
        "ashost": os.getenv("SAP_ASHOST"),
        "sysnr": os.getenv("SAP_SYSNR"),
        "client": os.getenv("SAP_CLIENT"),
        "lang": os.getenv("SAP_LANG", "EN"),
    }

def get_UAT_mssql_conn_str():
    driver = os.getenv("UAT_DB_DRIVER")
    server = os.getenv("UAT_DB_SERVER")
    database = os.getenv("UAT_DB_DATABASE")
    username = os.getenv("UAT_DB_USERNAME")
    password = os.getenv("UAT_DB_PASSWORD")

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
    )
    return conn_str

def get_PRD_mssql_conn_str():
    driver = os.getenv("PRD_DB_DRIVER")
    server = os.getenv("PRD_DB_SERVER")
    database = os.getenv("PRD_DB_DATABASE")
    username = os.getenv("PRD_DB_USERNAME")
    password = os.getenv("PRD_DB_PASSWORD")

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
    )
    return conn_str

# utils -------------------------------------------------------------------------------------
def check_rfc_return(ret_list: list):
    for ret in ret_list:
        msg_type = ret.get("TYPE")
        msg_text = ret.get("MESSAGE")
        if msg_type == "E":
            raise RuntimeError(f"❌ SAP RFC Error: {msg_text}")
        elif msg_type == "A":
            print(f"⚠️ SAP RFC Abort: {msg_text}")
        elif msg_type == "W":
            print(f"⚠️ SAP RFC Warning: {msg_text}")
        else:
            print(f"✅ SAP RFC Info: {msg_text}")

def is_on_schedule(max_delay_minutes=60, **context):
    """
    排程 run 若比預定時間晚超過 max_delay_minutes（例如剛啟用 DAG 時補跑最近一個區間），回傳 False 跳過後續 task
    手動觸發一律照跑
    """
    if context["dag_run"].run_type != DagRunType.SCHEDULED:
        print("✅ 非排程觸發，照常執行")
        return True

    delay = pendulum.now("UTC") - context["data_interval_end"]
    print(f"預定時間: {context['data_interval_end']}, 延遲: {delay}")
    if delay > timedelta(minutes=max_delay_minutes):
        print(f"⚠️ 延遲超過 {max_delay_minutes} 分鐘，視為補跑，跳過後續 task")
        return False
    return True
