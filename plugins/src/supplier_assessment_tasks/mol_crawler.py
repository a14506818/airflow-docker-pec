import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from time import sleep

class MOLCrawler:
    def __init__(self, driver_path: str):
        service = Service(executable_path=ChromeDriverManager().install())
        # 需要模擬真人，不然會被CPT網頁阻擋
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/113.0.0.0 Safari/537.36')
        options.add_argument("referer=https://portal.sw.nat.gov.tw/")
        options.add_argument("accept-language=zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7")
        self.base_url = 'https://announcement.mol.gov.tw/'
        self.driver = webdriver.Chrome(service=service, options=options)

    def _get_column_names(self, table_type):
        base = ['查詢名稱', '法律類別', '序號', '縣市/單位別', '公告日期', '處分日期', '處分字號', '事業單位名稱(負責人)自然人姓名', '違法法規法條', '違反法規內容']
        if table_type == 1:
            return base + ['備註說明']
        return base + ['罰鍰金額', '備註說明']

    def _extract_max_page(self, xpath):
        element = self.driver.find_element(By.XPATH, xpath)
        return int(element.text.replace('共', '').replace('頁，跳至第 ', '').replace('頁', ''))

    def _scrape_table(self, page_count, change_func, content_xpath, table_id, df, query_name, law_category):
        for i in range(1, page_count + 1):
            print(f"Scraping {table_id} page {i}...")
            self.driver.execute_script(f"{change_func}({i});")
            wait_xpath = "//div[@id='" + content_xpath + "']//ul[@class='pagination']//li[@class='active']/a[text()='{i}']"
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, wait_xpath.format(i=i)))
            )
            try:
                sleep(0.2)
                rows = self.driver.find_elements(By.XPATH, f"//table[@id='{table_id}']//tbody//tr")
                for row in rows:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    row_data = [query_name, law_category] + [cell.text.strip() for cell in cells]
                    df.loc[len(df)] = row_data
            except Exception as e:
                print(f"{table_id} 發生錯誤: {e}")

    def _clean(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        for col in df.columns:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
        return df

    def crawl(self, company_name: str):
        try:
            self.driver.get(self.base_url)
            sleep(0.5)

            name_input = self.driver.find_element(By.ID, 'unitname')
            submit_button = self.driver.find_element(By.ID, 'search')
            name_input.clear()
            name_input.send_keys(company_name)
            self.driver.execute_script("arguments[0].click();", submit_button)
            print('正在查詢:', company_name, " 勞動法規違反紀錄...")
            sleep(0.5)

            df_1 = pd.DataFrame(columns=self._get_column_names(1))
            df_2 = pd.DataFrame(columns=self._get_column_names(2))
            df_3 = pd.DataFrame(columns=self._get_column_names(3))

            mp1 = self._extract_max_page("//div[@id='content2']//div[@class='pagination-ext']")
            mp2 = self._extract_max_page("//div[@id='content3']//div[@class='pagination-ext']")
            mp3 = self._extract_max_page("//div[@id='content1']//div[@class='pagination-ext']")

            self._scrape_table(mp1, "changePage", "content2", "table1", df_1, company_name, "就業服務法／中高齡者及高齡者就業促進法／職業安全衛生法")
            self._scrape_table(mp2, "changePage2", "content3", "table2", df_2, company_name, "勞工退休金條例／勞工職業災害保險及保護法")
            self._scrape_table(mp3, "changePage3", "content1", "table3", df_3, company_name, "勞動基準法／工會法／最低工資法／性別平等工作法")
        finally:
            self.driver.quit()

        final_df = pd.concat([df_1, df_2, df_3], ignore_index=True)
        try:
            final_df = self._clean(final_df)
        except Exception as e:
            print(f"清理資料錯誤: {e}")

        return final_df
