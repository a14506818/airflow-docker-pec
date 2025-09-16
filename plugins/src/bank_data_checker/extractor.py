import os
import pandas as pd

from pyrfc import Connection
from src.common.common import get_sap_conn_params

import pendulum
from datetime import datetime, date

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait


def get_SAP_partner_bank_list(**context):
    conn_params = get_sap_conn_params()
    conn = Connection(**conn_params)

    # 呼叫 SAP RFC
    rfc_result = conn.call('Z_FI_BPM_017', PI_BANKS='TW')  # PI_BANKS 可選 TW 或空白
    print("rfc_result: ", rfc_result)

    # 正確轉成 DataFrame（注意不要用 [] 包住）
    df = pd.DataFrame(rfc_result['PT_OUT'])

    print("✅ SAP 銀行資料如下：")
    print(df.head())

    # 只保留需要的欄位，並重新命名
    df = df.rename(columns={
        "BANKL": "bank_code",
        "LIFNR": "Partner_Code",
        "KOINH": "Partner_Name",
    })[["bank_code", "Partner_Code", "Partner_Name"]]

    # 排除 BANKL 有非數字的行
    df = df[df['bank_code'].str.isnumeric()]

    df = df.dropna().drop_duplicates()

    print(df.head())
    
    return df.to_dict("records")  # ❗XCom 不支援直接傳 df，要先轉成 dict

def crawl_bank_data():
    """
    爬取銀行資料 銀行局
    """
    # 設定下載路徑
    download_dir = "/opt/airflow/downloads"
    os.makedirs(download_dir, exist_ok=True)

    # 刪除可能存在的舊檔案 -----------------------------------------------------------------------------------------------
    for f in os.listdir(download_dir):
        if f.endswith('.csv'):
            os.remove(os.path.join(download_dir, f))
            print(f"✅ 刪除舊檔案: {f}")

    # 使用 ChromeDriverManager 來自動管理 ChromeDriver ---------------------------------------------------------------  
    service = Service(executable_path=ChromeDriverManager().install())

    # 這些建議都加上，不開頁面、禁用GPU加速等等
    # 需要模擬真人，不然會被CPT網頁阻擋
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/113.0.0.0 Safari/537.36')
    options.add_argument("referer=https://portal.sw.nat.gov.tw/")
    options.add_argument("accept-language=zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7")
    options.add_experimental_option("prefs", {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })

    driver = webdriver.Chrome(service=service, options=options)

    print("Chrome version:", driver.capabilities['browserVersion'])
    print("ChromeDriver version:", driver.capabilities['chrome']['chromedriverVersion'])

    driver.get("https://www.banking.gov.tw/ch/ap/bankno_excel.jsp")
    print("✅ Selenium Works")

    # 等待檔案寫入
    try:
        WebDriverWait(driver, 30).until(lambda d: any(f.endswith('.csv') for f in os.listdir(download_dir)))
    except Exception as e:
        print('Time out ! ', e)
    print("csv Downloaded:", os.listdir(download_dir))

    # 找出剛下載的 CSV 檔案 -----------------------------------------------------------------------------------------------------
    csv_files = [f for f in os.listdir(download_dir) if f.endswith('.csv')]
    if not csv_files:
        raise FileNotFoundError("❌ 找不到下載的 csv 檔案")
    csv_path = os.path.join(download_dir, csv_files[0])
    
    print("✅ 找到 csv 檔案:", csv_path)

    # 讀取 csv/txt 檔（使用正確編碼）
    df = pd.read_csv(csv_path, sep='\t', encoding='utf-16')
    # 清理 ="xxx" 格式（Excel 匯出格式）
    df = df.applymap(lambda x: str(x).replace('="', '').replace('"', '').strip())
    df = df.iloc[:, :3]
    df.columns = ['code', 'bank_code', 'bank_name']
    print(df.head())

    return df.to_dict("records")  # ❗XCom 不支援直接傳 df，要先轉成 dict



def crawl_bank_data_2():
    """
    爬取銀行資料 銀行局
    """
    # 設定下載路徑
    download_dir = "/opt/airflow/downloads"
    os.makedirs(download_dir, exist_ok=True)

    # 刪除可能存在的舊檔案 -----------------------------------------------------------------------------------------------
    for f in os.listdir(download_dir):
        if f.endswith('.txt'):
            os.remove(os.path.join(download_dir, f))
            print(f"✅ 刪除舊檔案: {f}")

    # 使用 ChromeDriverManager 來自動管理 ChromeDriver ---------------------------------------------------------------  
    service = Service(executable_path=ChromeDriverManager().install())

    # 這些建議都加上，不開頁面、禁用GPU加速等等
    # 需要模擬真人，不然會被CPT網頁阻擋
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/113.0.0.0 Safari/537.36')
    options.add_argument("referer=https://portal.sw.nat.gov.tw/")
    options.add_argument("accept-language=zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7")
    options.add_experimental_option("prefs", {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })

    driver = webdriver.Chrome(service=service, options=options)

    print("Chrome version:", driver.capabilities['browserVersion'])
    print("ChromeDriver version:", driver.capabilities['chrome']['chromedriverVersion'])

    driver.get("https://www.fisc.com.tw/tc/download/twd.txt")
    print("✅ Selenium Works")

    # 等待檔案寫入
    try:
        WebDriverWait(driver, 30).until(lambda d: any(f.endswith('.txt') for f in os.listdir(download_dir)))
    except Exception as e:
        print('Time out ! ', e)
    print("txt Downloaded:", os.listdir(download_dir))

    # 找出剛下載的 TXT 檔案 -----------------------------------------------------------------------------------------------------
    txt_files = [f for f in os.listdir(download_dir) if f.endswith('.txt')]
    if not txt_files:
        raise FileNotFoundError("❌ 找不到下載的 txt 檔案")
    txt_path = os.path.join(download_dir, txt_files[0])
    
    print("✅ 找到 txt 檔案:", txt_path)

    # 讀取 csv/txt 檔（使用正確編碼） -------------------------------------------------------------------------------------------
    with open(txt_path, "r", encoding="big5") as f:
        lines = f.readlines()

    data = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        # 取代多個空白為單一空格，方便 split
        parts = ' '.join(line.split()).split(' ')
        # 前三碼為代碼，後面兩部分合起來為名稱與簡稱
        code = parts[0]
        # 中間所有字串拼接成分行名稱，最後一個為簡稱
        name = ''.join(parts[1:-1])
        short_name = parts[-1]
        data.append([code, name, short_name])

    # 建立 DataFrame
    df = pd.DataFrame(data, columns=["bank_code", "bank_name", "short_name"])
    print(df.head())

    return df.to_dict("records")  # ❗XCom 不支援直接傳 df，要先轉成 dict