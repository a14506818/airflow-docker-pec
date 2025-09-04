from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone

from src.common.check_connection import check_selenium, check_mssql, check_rfc
from src.supplier_assessment_tasks.supplier_compliance_crawler import del_tmp_table, get_partner_list, crawl_FAT_data, crawl_compliance_data, copy_tmp_to_his_and_prd, gen_attchments

from src.common.email_utils import on_success, on_failure  


# 設定時區為 Asia/Taipei
local_tz = timezone("Asia/Taipei")

with DAG(
    dag_id="supplier_compliance_crawler",
    schedule="30 12 1 7 *",  # 每年 7 月 1 號的 12:30
    start_date = local_tz.datetime(2025, 1, 1, 0, 0, 0),
    catchup=False,
    tags=["supplier", "partner", "compliance", "crawler"],
    description="供應商法尊資料爬蟲，7/1 執行",
    default_args={
        # "retries": 2,
        # "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=300),
        "on_failure_callback": on_failure,
    }
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(
        task_id="end",
        on_success_callback=on_success
    )

    check_selenium_task = PythonOperator(
        task_id="check_selenium_connection",
        python_callable=check_selenium
    )

    check_mssql_task = PythonOperator(
        task_id="check_mssql_connection",
        python_callable=check_mssql
    )

    check_rfc_task = PythonOperator(
        task_id="check_rfc_connection",
        python_callable=check_rfc
    )

    del_tmp_table_task = PythonOperator(
        task_id="del_tmp_table",
        python_callable=del_tmp_table
    )

    get_partner_list_task = PythonOperator(
        task_id="get_partner_list",
        python_callable=get_partner_list
    )

    crawl_FAT_data_task = PythonOperator(
        task_id="crawl_FAT_data",
        python_callable=crawl_FAT_data
    )

    crawl_compliance_data_task = PythonOperator(
        task_id="crawl_compliance_data",
        python_callable=crawl_compliance_data
    )

    copy_tmp_to_his_and_prd_task = PythonOperator(
        task_id="copy_tmp_to_his_and_prd",
        python_callable=copy_tmp_to_his_and_prd
    )

    gen_attchments_task = PythonOperator(
        task_id="gen_attchments",
        python_callable=gen_attchments
    )

    start >> [check_selenium_task, check_mssql_task, check_rfc_task] >> del_tmp_table_task \
        >> get_partner_list_task >> [crawl_compliance_data_task, crawl_FAT_data_task] \
        >> copy_tmp_to_his_and_prd_task >> gen_attchments_task >> end