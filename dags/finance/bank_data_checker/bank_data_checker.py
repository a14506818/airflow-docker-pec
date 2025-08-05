from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone

from src.common.check_connection import check_selenium, check_mssql, check_rfc

from src.bank_data_checker.extractor import crawl_bank_data

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
        # "execution_timeout": timedelta(minutes=15),
        # "on_failure_callback": on_failure,
    }
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(
        task_id="end",
        # on_success_callback=on_success
    )

    check_selenium_task = PythonOperator(
        task_id="check_selenium_connection",
        python_callable=check_selenium
    )

    check_rfc_task = PythonOperator(
        task_id="check_rfc_connection",
        python_callable=check_rfc
    )

    start >> [check_selenium_task, check_rfc_task] >> end

