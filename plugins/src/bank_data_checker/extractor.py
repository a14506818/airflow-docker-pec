import os
import pandas as pd

def get_SAP_bank_list(PI_BANKS: str = 'TW'): # ------------------------------------------------------------------------------
    from pyrfc import Connection
    from src.common.common import get_sap_conn_params

    conn_params = get_sap_conn_params()
    conn = Connection(**conn_params)

    # 呼叫 SAP RFC
    rfc_result = conn.call('Z_FI_BPM_008', PI_BANKS=PI_BANKS)
    print("rfc_result: ", rfc_result)

    # 正確轉成 DataFrame（注意不要用 [] 包住）
    df = pd.DataFrame(rfc_result['PT_OUT'])

    print("✅ SAP 銀行資料如下：")
    print(df.head())

    # 只保留需要的欄位，並重新命名
    df = df.rename(columns={
        "BANKL": "bank_code",
        "BANKA": "bank_name",
    })[["bank_code", "bank_name"]]

    print(df.head())
    
    return df.to_dict("records")  # ❗XCom 不支援直接傳 df，要先轉成 dict

def crawl_bank_data():
    """
    爬取銀行資料
    """
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver.support.ui import WebDriverWait

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

    driver.get("https://www.banking.gov.tw/ch/ap/bnx_excel.jsp")
    print("✅ Selenium Works")

    # 等待檔案寫入
    try:
        WebDriverWait(driver, 30).until(lambda d: any(f.endswith('.csv') for f in os.listdir(download_dir)))
    except Exception as e:
        print('Time out ! ', e)
    print("csv Downloaded:", os.listdir(download_dir))

    # 找出剛下載的 JSON 檔案 -----------------------------------------------------------------------------------------------------
    csv_files = [f for f in os.listdir(download_dir) if f.endswith('.csv')]
    if not csv_files:
        raise FileNotFoundError("❌ 找不到下載的 csv 檔案")
    csv_path = os.path.join(download_dir, csv_files[0])
    
    print("✅ 找到 csv 檔案:", csv_path)

    # 讀取 csv/txt 檔（使用正確編碼）
    df = pd.read_csv(csv_path, sep='\t', encoding='utf-16')
    # 清理 ="xxx" 格式（Excel 匯出格式）
    df = df.applymap(lambda x: str(x).replace('="', '').replace('"', '').strip())
    df = df.iloc[:, :2]
    df.columns = ['bank_code', 'bank_name']
    print(df.head())

    return df.to_dict("records")  # ❗XCom 不支援直接傳 df，要先轉成 dict



