import os
from dotenv import load_dotenv

load_dotenv(dotenv_path="/opt/airflow/dags/.env") # 載入 .env 變數

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