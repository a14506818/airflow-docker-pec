import os
from dotenv import load_dotenv

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