import pandas as pd
import pyodbc
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
import uuid

from src.supplier_assessment_tasks.db_handler import DBHandler
from src.supplier_assessment_tasks.mol_crawler import MOLCrawler
from src.supplier_assessment_tasks.env_crawler import ENVCrawler

from src.common.common import get_mssql_conn_str

def del_tmp_table():
    db_handler = DBHandler()
    db_handler.del_tmp_table()
    db_handler.shotdown()
    
def get_partner_list():
    db_handler = DBHandler()
    company_names = db_handler.get_partner_list()
    db_handler.shotdown()
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

    # get XCOM -----------------------------------------------------------------------------------------------
    ti = context["ti"] # 取得 Task Instance
    company_names = ti.xcom_pull(task_ids="get_partner_list")
    if not company_names:
        raise ValueError("❌ 取得公司名稱清單為空，請檢查上游任務")
    print("✅ 成功取得公司名稱清單，前幾筆資料如下：")
    print(company_names[:10])

    # init ---------------------------------------------------------------------------------------------------
    driver_path = "" # not needed, use ChromeDriverManager
    db_handler = DBHandler()
    insert_job = db_handler.insert_job
    update_job_status = db_handler.update_job_status
    insert_tmp_result = db_handler.insert_tmp_result
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
    
    db_handler.shotdown()
    print("✅ 所有爬蟲任務完成")