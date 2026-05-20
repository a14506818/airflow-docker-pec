import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime, timedelta
from time import sleep
import re

class ENVCrawler:
    def __init__(self, driver_path: str):
        self.service = Service(executable_path=ChromeDriverManager().install())
        self.options = webdriver.ChromeOptions()
        self.options.add_argument('--headless')
        self.options.add_argument('--disable-gpu')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')
        self.options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/113.0.0.0 Safari/537.36')
        self.options.add_argument("referer=https://portal.sw.nat.gov.tw/")
        self.options.add_argument("accept-language=zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7")
        self.base_url = 'https://thaubing.gcaa.org.tw/envmap?qt-front_content=1#{...}'

    def _clean(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        for col in df.columns:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)

        # 裁罰日與違規日拆分
        penalty_dates = []
        violation_dates = []
        for raw_date in df['裁罰/違規日']:
            raw_date = raw_date.replace('(', '').replace(')', '')
            if '\n' in raw_date:
                parts = raw_date.split('\n')
                penalty_dates.append(parts[0].strip())
                violation_dates.append(parts[1].strip())
            else:
                penalty_dates.append(raw_date.strip())
                violation_dates.append("")
        # 插入原欄位位置
        insert_at = df.columns.get_loc('裁罰/違規日')
        df.insert(insert_at, '裁罰日', penalty_dates)
        df.insert(insert_at + 1, '違規日', violation_dates)
        # 移除原始欄位
        df.drop(columns=['裁罰/違規日'], inplace=True)

        # 裁罰費用拆分
        fine_amounts = []
        fine_descriptions = []
        for raw_fine in df['裁罰費用']:
            if '\n' in raw_fine:
                parts = raw_fine.split('\n')
                fine_amounts.append(parts[0].strip())
                fine_descriptions.append(parts[1].strip())
            else:
                fine_amounts.append(raw_fine.strip())
                fine_descriptions.append("")
        # 插入原欄位位置
        insert_at = df.columns.get_loc('裁罰費用')
        df.insert(insert_at, '裁罰金額', fine_amounts)
        df.insert(insert_at + 1, '裁罰備註', fine_descriptions)
        # 移除原始欄位
        df.drop(columns=['裁罰費用'], inplace=True)

        return df

    def crawl(self, company_name: str):
        col_names = ['統一編號', '供應商名稱', '裁罰/違規日', '縣市', '裁罰內容', '訴願狀態', '限改日期-改善完妥', '裁罰費用']
        df = pd.DataFrame(columns=col_names)

        driver = webdriver.Chrome(service=self.service, options=self.options)

        try:
            driver.get(self.base_url)

            name_input = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, 'edit-facility-name'))
            )
            submit_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, 'edit-submit-corp'))
            )

            name_input.clear()
            name_input.send_keys(company_name)
            submit_button.click()
            print(f"正在查詢: {company_name} 環保法規違反紀錄...")

            WebDriverWait(driver, 60).until(
                EC.invisibility_of_element_located((By.CLASS_NAME, "throbber"))
            )

            links = driver.find_elements(By.XPATH, f".//a[contains(text(), '{company_name}')]")
            if not links:
                print(company_name, ': No Content Found')
                return df

            arr_links = [(link.text, link.get_attribute("href")) for link in links]
            for link_text, link_URL in arr_links:
                print("找到資料:", link_text, "| URL:", link_URL)

                driver.get(link_URL)
                sleep(0.2)

                try:
                    c_name = driver.find_element(By.ID, "page-title").text.strip()
                    c_code = driver.find_element(By.XPATH, "//span[@class='field-content']//a").text.strip()
                    record_rows = driver.find_elements(By.XPATH, "//div[@class='view-content']//tbody//tr")

                    for row in record_rows:
                        cells = row.find_elements(By.TAG_NAME, "td")
                        cell_texts = [cell.text.strip() for cell in cells]
                        # cell_texts = [cell.text.strip().encode('utf-8', 'ignore').decode('utf-8') for cell in cells]


                        row_data = [c_code, c_name] + cell_texts[:]
                        df.loc[len(df)] = row_data

                except Exception as e:
                    print(f"發生錯誤: {e}")

        except Exception as e:
            print(f"主頁面操作錯誤: {e}")
        finally:
            driver.quit()

        try:
            df = self._clean(df)
        except Exception as e:
            print(f"清理資料錯誤: {e}")

        # print(df)
        return df
