from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

import os
import pyodbc
from dotenv import load_dotenv

from pyrfc import Connection
from pyrfc._exception import ABAPApplicationError, ABAPRuntimeError, LogonError, CommunicationError

load_dotenv(dotenv_path="/opt/airflow/dags/.env") # 載入 .env 變數

def check_selenium():
    service = Service(executable_path=ChromeDriverManager().install())

    # 這些建議都加上，不開頁面、禁用GPU加速等等
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  
    options.add_argument("--disable-gpu") 
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-infobars")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-notifications")
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')


    driver = webdriver.Chrome(service=service, options=options)

    print("Chrome version:", driver.capabilities['browserVersion'])
    print("ChromeDriver version:", driver.capabilities['chrome']['chromedriverVersion'])

    driver.get("https://www.google.com")
    print("✅ Selenium Works, Title:", driver.title)
    driver.quit()

def check_mssql():
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
    print('conn_str => ',conn_str)
    conn = pyodbc.connect(conn_str, timeout=5)
    conn.close()
    print("✅ MSSQL 連線成功")
    return True

def check_rfc():
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

        # 呼叫測試函式
        result = conn.call("STFC_CONNECTION")
        print("RFC OK:", result)

    except (ABAPApplicationError, ABAPRuntimeError, LogonError, CommunicationError) as e:
        print("RFC ERROR:", e)
        raise e