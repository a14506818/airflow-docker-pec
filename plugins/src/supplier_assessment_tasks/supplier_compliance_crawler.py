import pandas as pd
import pyodbc
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
import uuid

from src.supplier_assessment_tasks.mol_crawler import MOLCrawler
from src.supplier_assessment_tasks.env_crawler import ENVCrawler

from src.common.common import get_mssql_conn_str

def del_tmp_table():
    """
    刪除暫存資料表
    """
    conn_str = get_mssql_conn_str()
    conn = pyodbc.connect(conn_str, timeout=5)
    print("✅ MSSQL 連線成功")

    cursor = conn.cursor()
    cursor.execute("DELETE FROM TMP_MOL_compliance_result;")
    print("SQL: DELETE FROM TMP_MOL_compliance_result;")
    cursor.execute("DELETE FROM TMP_ENV_compliance_result;")
    print("SQL: DELETE FROM TMP_ENV_compliance_result;")
    conn.commit()
    print("✅ 刪除暫存資料表成功")

    cursor.close()
    conn.close()


def get_partner_list():
    """
    取得 REF 供應商評估任務的爬蟲清單
    """
    # 載入DB連線 -------------------------------------------------------------------------------------------
    conn_str = get_mssql_conn_str()
    conn = pyodbc.connect(conn_str, timeout=5)
    print("✅ MSSQL 連線成功")

    # 寫入資料庫
    cursor = conn.cursor()

    cursor.execute("SELECT partner_name FROM REF_partner_crawler_list_test") # REF_partner_crawler_list
    rows = cursor.fetchall()
    if not rows:
        raise ValueError("❌ REF_partner_crawler_list 資料表為空，請檢查資料庫")

    # 把 tuple 拆開
    company_names = [str(r[0]).strip() for r in rows if r[0]]
    company_names = list(set(company_names))  # 去重複，非必要但建議

    print("✅ 取得公司名稱清單成功:", company_names)

    cursor.close()
    conn.close()

    return company_names

def crawl_compliance_data(**context):
    """
    loop供應商，產生JOB，執行爬蟲，寫入TMP
    """
    def with_retry(func, args=(), job_id='', kwargs=None, max_retries=10, wait_sec=1):
        if kwargs is None:
            kwargs = {}
        for attempt in range(1, max_retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                print(f"❌ 第 {attempt} 次失敗: {e}")
                update_job_status(job_id, status="fail", error_msg=str(e))
                if attempt == max_retries:
                    raise
                print("🔁 重試中...\n")
                time.sleep(wait_sec)

    def crawl_mol(name):
        mol_crawler = MOLCrawler(driver_path)
        return mol_crawler.crawl(name)

    def crawl_env(name):
        env_crawler = ENVCrawler(driver_path)
        return env_crawler.crawl(name)

    def insert_job(job: dict):
        conn_str = get_mssql_conn_str()
        conn = pyodbc.connect(conn_str, timeout=5)
        print("✅ MSSQL 連線成功")

        # 寫入資料庫
        cursor = conn.cursor()

        cursor.execute("""
            insert into JOB_compliance_crawler (id, run_key, supplier_name, source_type, status)
            values (?, ?, ?, ?, ?)
        """, 
        job['id'], job['run_key'], job['supplier_name'], job['source_type'], 'pending')

        conn.commit()
        print("✅ 寫入 BPM 成功")

        conn.close()

    def insert_tmp_result(df: pd.DataFrame, source:str):
        """將查詢結果寫入 TMP__compliance_result"""
        table_name = f"TMP_{source}_compliance_result"
        if df.empty:
            print("⚠️ 傳入空的 DataFrame，未執行寫入。")
            return

        MOL_COLUMNS_MAPPING = {
            "查詢名稱": "search_name",
            "法律類別": "law_type",
            "序號": "seq",
            "縣市/單位別": "country",
            "公告日期": "announce_date",
            "處分日期": "penalty_date",
            "處分字號": "penalty_no",
            "事業單位名稱(負責人)自然人姓名": "partner_name",
            "違法法規法條": "law",
            "違反法規內容": "content",
            "備註說明": "remark",
            "罰鍰金額": "fine"  # 例如後處理產出的欄位名
        }
        ENV_COLUMNS_MAPPING = {
            "統一編號": "partner",
            "供應商名稱": "partner_name",
            "裁罰日": "penalty_date",
            "違規日": "violation_date",
            "縣市": "country",
            "裁罰內容": "description",
            "訴願狀態": "status",
            "限改日期-改善完妥": "refine",
            "裁罰金額": "fine_amount",
            "裁罰備註": "fine_description"  # 例如後處理產出的欄位名
        }
        #  source : MOL or ENV
        if source == "MOL":
            COLUMNS_MAPPING = MOL_COLUMNS_MAPPING
        elif source == "ENV":
            COLUMNS_MAPPING = ENV_COLUMNS_MAPPING
        else:
            raise ValueError("Invalid source type. Expected 'MOL' or 'ENV'.")
        # 欄位轉換
        df = df.rename(columns=COLUMNS_MAPPING)
        # 只保留 COLUMNS_MAPPING 定義的欄位
        keep_cols = list(COLUMNS_MAPPING.values())  # 取 rename 後的新欄位名
        df = df[keep_cols]
        # 將所有欄位的值轉為字串
        df = df.astype(str)

        # 將 DataFrame 寫入 SQL Server
        conn_str = get_mssql_conn_str()
        conn = pyodbc.connect(conn_str, timeout=5)
        print("✅ MSSQL 連線成功")
        
        # 寫入資料庫
        cursor = conn.cursor()

        # 寫入新資料
        for index, row in df.iterrows():
            print(f"SQL: INSERT INTO {table_name} ({', '.join(row.index)}) VALUES ({', '.join(['?' for _ in row])})")   
            cursor.execute(f"""
                INSERT INTO {table_name} ({', '.join(row.index)})
                VALUES ({', '.join(['?' for _ in row])})
            """, tuple(row))
            
            
        conn.commit()

        conn.close()
        print(f"✅ 寫入 {len(df)} 筆至 {table_name}")    

    def update_job_status(job_id: str, status: str, error_msg: str = None):
        conn_str = get_mssql_conn_str()
        conn = pyodbc.connect(conn_str, timeout=5)
        print("✅ MSSQL 連線成功")

        # 寫入資料庫
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE JOB_compliance_crawler
            SET status = ?, error_msg = ?
            WHERE id = ?
        """, status, error_msg, job_id)

        conn.commit()
        print("✅ 寫入 BPM 成功")

        conn.close()
    # get XCOM -----------------------------------------------------------------------------------------------
    ti = context["ti"] # 取得 Task Instance
    company_names = ti.xcom_pull(task_ids="get_partner_list")
    if not company_names:
        raise ValueError("❌ 取得公司名稱清單為空，請檢查上游任務")
    print("✅ 成功取得公司名稱清單，前幾筆資料如下：")
    print(company_names[:10])

    # init ---------------------------------------------------------------------------------------------------
    driver_path = "" # not needed, use ChromeDriverManager
    mol_results = []
    env_results = []
    error_logs = []
    run_key = "RK_" + time.strftime("%Y%m%d%H%M%S")

    # 並行查詢 MOL 與 ENV -------------------------------------------------------------------------------------
    with ThreadPoolExecutor(max_workers=4) as executor:
        future_to_info = {}
        job_ids = {} # 紀錄 JOB ID 對應的公司名稱與來源
        for name in company_names:
            # 產生JOB
            for source in ['MOL', 'ENV']:
                job_id = str(uuid.uuid4())
                job_data = {
                    "id": job_id,
                    "run_key": run_key,
                    "supplier_name": name,
                    "source_type": source
                }
                insert_job(job_data) 
                job_ids[(name, source)] = job_id
            # 執行爬蟲
            future_to_info[executor.submit(with_retry, crawl_mol, args=(name,), job_id=job_ids[(name, 'MOL')])] = (name, 'MOL')
            future_to_info[executor.submit(with_retry, crawl_env, args=(name,), job_id=job_ids[(name, 'ENV')])] = (name, 'ENV')

        for future in as_completed(future_to_info):
            name, source = future_to_info[future]
            job_id = job_ids[(name, source)]
            try:
                result = future.result()
                if not result.empty:
                    if source == 'MOL':
                        mol_results.append(result)
                        result["run_key"] = run_key
                        result["job_id"] = job_ids[(name, source)]
                        insert_tmp_result(result,'MOL')
                    elif source == 'ENV':
                        env_results.append(result)
                        result["run_key"] = run_key
                        result["job_id"] = job_ids[(name, source)]
                        insert_tmp_result(result,'ENV')
                update_job_status(job_id, status="success", error_msg='')
            except Exception as e:
                err = f"{source} 查詢 {name} 發生錯誤: {e}"
                print("❌ ",err)
                error_logs.append(err + '\n' + traceback.format_exc())
                update_job_status(job_id, status="fail", error_msg=str(e))