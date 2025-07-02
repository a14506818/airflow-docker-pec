import os
import json
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from datetime import date

def crawl_cpt_fx(): # 海關常見幣別
    # 設定下載路徑
    download_dir = "/opt/airflow/downloads"
    os.makedirs(download_dir, exist_ok=True)

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

    driver.get("https://portal.sw.nat.gov.tw/APGQ/LoginFree?request_locale=zh_TW")
    print("✅ Selenium Works, Title:", driver.title)

    # 等待 ID 為 MENU_APGQ 的 <a> tag 出現並點擊 ---------------------------------------------------------------------------------
    WebDriverWait(driver, 30).until(
        EC.element_to_be_clickable((By.ID, "MENU_APGQ"))
    )
    button = driver.find_element(By.ID, "MENU_APGQ")
    driver.execute_script("arguments[0].click();", button)
    print("✅ Clicked 免證查詢服務")

    # 等待 ID 為 MENU_APGQ_6 的 <a> tag 出現並點擊 -------------------------------------------------------------------------------
    WebDriverWait(driver, 30).until(
        EC.element_to_be_clickable((By.ID, "MENU_APGQ_6"))
    )
    button = driver.find_element(By.ID, "MENU_APGQ_6")
    driver.execute_script("arguments[0].click();", button)
    print("✅ Clicked 其他相關查詢")

    # 等待直到包含 tabClick 的 <a> 出現 ---------------------------------------------------------------------------------
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.XPATH, "//a[contains(@onclick, \"tabClick('GC331')\")]"))
    )
    driver.execute_script("tabClick('GC331')")
    print("✅ Clicked (GC331)每旬報關適用外幣匯率")

    # 等待下載按鈕出現 -----------------------------------------------------------------------------------------------------------
    WebDriverWait(driver, 30).until(
        EC.element_to_be_clickable((By.ID, "current_json"))
    )
    # click
    print(os.getuid())  # 印出當前 UID
    button = driver.find_element(By.ID, "current_json")
    driver.execute_script("arguments[0].click();", button)
    print("✅ JSON Download Triggered")
    # 等待檔案寫入
    WebDriverWait(driver, 30).until(lambda d: any(f.endswith(".json") for f in os.listdir(download_dir)))
    print("JSON Downloaded:", os.listdir(download_dir))

    # 找出剛下載的 JSON 檔案 -----------------------------------------------------------------------------------------------------
    json_files = [f for f in os.listdir(download_dir) if f.endswith(".json")]
    if not json_files:
        raise FileNotFoundError("❌ 找不到下載的 JSON 檔案")
    json_path = os.path.join(download_dir, json_files[0])
    # 讀取 JSON 檔
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    print("✅ 讀取 JSON 完成") 

    # 取出 items 並轉成 DataFrame -----------------------------------------------------------------------------------------------
    df = pd.DataFrame(data["items"])
    # 若數值是字串格式，轉成 float
    df["buyValue"] = df["buyValue"].astype(float)
    df["sellValue"] = df["sellValue"].astype(float)
    # 可選：加上期間資訊（start, end）
    df["start"] = data["start"]
    df["end"] = data["end"]

    # ReFormat df 
    df["fx_rate"] = (df["buyValue"] + df["sellValue"]) / 2
    df["to_curr"] = "TWD"
    df = df.rename(columns={
        "code": "from_curr",
    })

    print("✅ JSON to DataFrame 完成，共有筆數:", len(df))
    print(df)

    driver.quit()

    return df.to_dict("records")  # ❗XCom 不支援直接傳 df，要先轉成 dict


def crawl_oanda_fx(): # 特殊幣別
    service = Service(executable_path=ChromeDriverManager().install())

    # 這些建議都加上，不開頁面、禁用GPU加速等等
    # 需要模擬真人，不然會被CPT網頁阻擋
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/113.0.0.0 Safari/537.36')
    options.add_argument("accept-language=zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7")


    driver = webdriver.Chrome(service=service, options=options)

    print("Chrome version:", driver.capabilities['browserVersion'])
    print("ChromeDriver version:", driver.capabilities['chrome']['chromedriverVersion'])

    # 特殊幣別清單 -----------------------------------------------------------------------------------------------
    from_currencies = ["HUF", "RUB", "TRY", "VND", "MOP"]
    print("from_currencies:", from_currencies)
    to_currencies = ["TWD", "USD"]
    print("to_currencies:", to_currencies)

    # start looping through currencies ---------------------------------------------------
    results = []
    for from_curr in from_currencies:
        for to_curr in to_currencies:
            # 開啟 OANDA 網站 -------------------------------------------------------------------------------------------
            driver.get(f"https://www.oanda.com/currency-converter/en/?from={from_curr}&to={to_curr}&amount=1")

            # get fx rate ---------------------------------------------------------------------------------
            wait = WebDriverWait(driver, 30)
            input_element = wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, 'input[name="numberformat"][tabindex="4"]')
            ))

            # 抓取 value
            value = input_element.get_attribute("value")
            print(f"{from_curr} to {to_curr} fx rate: ", value)

            # 檢查是否為空值
            if not value:
                print(f"❌ {from_curr} to {to_curr} fx rate is empty, skipping...")
                continue

            # insert to results
            results.append({
                "from_curr": from_curr,
                "to_curr": to_curr,
                "fx_rate": value,
                "start": date.today().strftime("%Y%m%d"),  # 日期設為今天
                "end": date.today().strftime("%Y%m%d"),  # 日期設為今天
            })

    print("✅ All fx rates fetched successfully:", results)
    driver.quit()

    return results