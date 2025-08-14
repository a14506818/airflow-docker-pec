from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone

from src.common.check_connection import check_selenium, check_mssql, check_rfc

from src.bank_data_checker.extractor import crawl_bank_data, crawl_bank_data_2, get_SAP_partner_bank_list
from src.bank_data_checker.comparator import compare_sap_and_crawl_bank_list, gen_attchments

from src.common.email_utils import on_success, on_failure  

# 設定時區為 Asia/Taipei
local_tz = timezone("Asia/Taipei")

with DAG(
    dag_id="bank_data_checker",
    schedule="0 9 * * 2",  # 每周二的 09:00
    start_date = local_tz.datetime(2025, 1, 1, 0, 0, 0),
    catchup=False,
    tags=["finance", "bank", "data_checker"],
    description="銀行資料檢查，每周二執行",
    default_args={
        # "retries": 2,
        # "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=2),
        "on_failure_callback": on_failure,
    }
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(
        task_id="end",
        on_success_callback=on_success
    )
    mid = EmptyOperator(task_id="mid")

    check_selenium_task = PythonOperator(
        task_id="check_selenium_connection",
        python_callable=check_selenium
    )

    check_rfc_task = PythonOperator(
        task_id="check_rfc_connection",
        python_callable=check_rfc
    )

    get_SAP_partner_bank_list_task = PythonOperator(
        task_id="get_SAP_partner_bank_list",
        python_callable=get_SAP_partner_bank_list
    )

    crawl_bank_data_task = PythonOperator(
        task_id="crawl_bank_data",
        python_callable=crawl_bank_data
    )

    crawl_bank_data_2_task = PythonOperator(
        task_id="crawl_bank_data_2",
        python_callable=crawl_bank_data_2
    )

    compare_sap_and_crawl_bank_list_task = PythonOperator(
        task_id="compare_sap_and_crawl_bank_list",
        python_callable=compare_sap_and_crawl_bank_list
    )

    gen_attchments_task = PythonOperator(
        task_id="gen_attchments",
        python_callable=gen_attchments
    )
    

    start >> [check_selenium_task, check_rfc_task] >> mid \
        >> [get_SAP_partner_bank_list_task, crawl_bank_data_task, crawl_bank_data_2_task] \
        >> compare_sap_and_crawl_bank_list_task >> gen_attchments_task >> end


