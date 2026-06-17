import pandas as pd
import pyodbc
import time
import pendulum
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
import uuid

from src.supplier_assessment_tasks.db_handler import DBHandler
from src.supplier_assessment_tasks.mol_crawler import MOLCrawler
from src.supplier_assessment_tasks.env_crawler import ENVCrawler

from src.common.common import get_mssql_conn_str

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait

def del_tmp_table():
    db_handler = DBHandler()
    db_handler.del_table('TMP_MOL_compliance_result')
    db_handler.del_table('TMP_ENV_compliance_result')
    db_handler.del_table('TMP_FAT_compliance_result')
    db_handler.shutdown()
    
def get_partner_list():
    db_handler = DBHandler()
    company_names = db_handler.get_partner_list()
    db_handler.shutdown()
    return company_names

def crawl_FAT_data(**context):
    import requests
    # get XCOM -----------------------------------------------------------------------------------------------
    ti = context["ti"] # 取得 Task Instance
    company_names = ti.xcom_pull(task_ids="get_partner_list")
    if not company_names:
        raise ValueError("❌ 取得公司名稱清單為空，請檢查上游任務")
    print("✅ 成功取得公司名稱清單，前幾筆資料如下：")
    print(company_names[:10])

    # init ---------------------------------------------------------------------------------------------------
    db_handler = DBHandler()
    insert_job = db_handler.insert_job
    update_job_status = db_handler.update_job_status
    insert_tmp_result = db_handler.insert_tmp_result
    run_key = "RK_" + time.strftime("%Y%m%d%H%M%S")
    soruce = "FAT"

    for name in company_names:
        # 產生JOB
        job_id = str(uuid.uuid4())
        job_data = {
            "id": job_id,
            "run_key": run_key,
            "supplier_name": name,
            "source_type": soruce
        }
        insert_job(job_data) 

        # 執行爬蟲
        url = f"https://pacs.osha.gov.tw/api/v1/getdangerocupation?info_q={name}"
        time.sleep(1)  # 每次請求前停 1 秒

        try:
            response = requests.get(url)
            data = response.json()
            df = pd.DataFrame(data)

            print(f"✅ 成功爬取 {name} 的 {soruce} 資料，前幾筆資料如下：")
            print(df)

            if not df.empty:
                df["run_key"] = run_key
                df["job_id"] = job_id
                insert_tmp_result(df, soruce)

        except Exception as e:
            print("❌ ", f"爬取 {name} 的 {soruce} 資料時發生錯誤: {e}")
            update_job_status(job_id, status="fail", error_msg=str(e))
            continue
        
        update_job_status(job_id, status="success", error_msg='')


    db_handler.shutdown()
    print("✅ FAT 爬蟲任務完成")

def crawl_compliance_data(**context):
    """
    loop供應商，產生JOB，執行爬蟲，寫入TMP
    """
    import threading

    # 每個 thread 自己的 DBHandler（解 pyodbc connection is busy）
    _tls = threading.local()
    def get_db():
        if not hasattr(_tls, "db"):
            _tls.db = DBHandler()
        return _tls.db

    def with_retry(func, args=(), job_id='', kwargs=None, max_retries=10, wait_sec=1):
        if kwargs is None:
            kwargs = {}
        for attempt in range(1, max_retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                print(f"❌ 第 {attempt} 次失敗: {e}")
                get_db().update_job_status(job_id, status="fail", error_msg=str(e))
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
    ti = context["ti"]  # 取得 Task Instance
    company_names = ti.xcom_pull(task_ids="get_partner_list")
    if not company_names:
        raise ValueError("❌ 取得公司名稱清單為空，請檢查上游任務")
    print("✅ 成功取得公司名稱清單，前幾筆資料如下：")
    print(company_names[:10])

    # init ---------------------------------------------------------------------------------------------------
    driver_path = ""  # not needed, use ChromeDriverManager
    main_db = DBHandler()  # 主 thread 專用，負責建 JOB
    mol_results = []
    env_results = []
    error_logs = []
    run_key = "RK_" + time.strftime("%Y%m%d%H%M%S")

    # 並行查詢 MOL 與 ENV -------------------------------------------------------------------------------------
    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_info = {}
        job_ids = {}  # 紀錄 JOB ID 對應的公司名稱與來源
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
                main_db.insert_job(job_data)
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
                    result["run_key"] = run_key
                    result["job_id"] = job_id
                    get_db().insert_tmp_result(result, source)
                    if source == 'MOL':
                        mol_results.append(result)
                    elif source == 'ENV':
                        env_results.append(result)
                get_db().update_job_status(job_id, status="success", error_msg='')
            except Exception as e:
                err = f"{source} 查詢 {name} 發生錯誤: {e}"
                print("❌ ", err)
                error_logs.append(err + '\n' + traceback.format_exc())
                get_db().update_job_status(job_id, status="fail", error_msg=str(e))

    main_db.shutdown()
    print("✅ 所有爬蟲任務完成")

def copy_tmp_to_his_and_prd():
    """
    將 TMP 資料表的資料複製到最終的資料表
    """
    db_handler = DBHandler()
    db_handler.copy_tmp_to_his_and_prd('MOL')
    db_handler.copy_tmp_to_his_and_prd('ENV')
    db_handler.copy_tmp_to_his_and_prd('FAT')
    db_handler.shutdown()
    print("✅ TMP 資料表資料已成功複製到最終資料表")

def gen_attchments(**context):
    """
    生成附件
    """
    db_handler = DBHandler()
    mol_df = db_handler.get_specific_table('PRD_MOL_compliance_result')
    env_df = db_handler.get_specific_table('PRD_ENV_compliance_result')
    fat_df = db_handler.get_specific_table('PRD_FAT_compliance_result')

    print(mol_df)
    print(env_df)
    print(fat_df)

    # 檔名與路徑
    tz = pendulum.timezone("Asia/Taipei")
    start_date = context["dag_run"].start_date
    if not isinstance(start_date, pendulum.DateTime):
        start_date = pendulum.instance(start_date)
    local_time = start_date.in_timezone(tz)
    file_name = f"FinalData__{context['dag'].dag_id}__{local_time.strftime('%Y%m%d_%H%M')}.xlsx"
    file_path = f"/opt/airflow/export/{file_name}"

    print("爬蟲結束，開始匯出結果至Excel...")

    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        sheet_written = False

        if not mol_df.empty:
            mol_df.to_excel(writer, sheet_name="MOL", index=False)
            sheet_written = True

        if not env_df.empty:
            env_df.to_excel(writer, sheet_name="ENV", index=False)
            sheet_written = True

        if not fat_df.empty:
            fat_df.to_excel(writer, sheet_name="FAT", index=False)
            sheet_written = True

        # 如果都沒資料，至少寫入一個空 sheet
        if not sheet_written:
            pd.DataFrame({"empty": []}).to_excel(writer, sheet_name="EMPTY", index=False)
            print("❌ 查無任何結果，已生成空白 sheet。")
        else:
            print(f"✅ 成功寫入 Excel 檔案: {file_path}")

    db_handler.shutdown()  
    print("✅ 附件生成完成")
    return [file_path]  # 將檔案路徑放入列表中

