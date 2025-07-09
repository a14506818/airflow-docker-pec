from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone

from src.common.check_connection import check_selenium, check_mssql, check_rfc
from src.fx_to_sap.extractor import crawl_cpt_fx
from src.fx_to_sap.transformer import gen_fx_to_other_currencies, clean_data_for_sap, clean_data_for_bpm
from src.fx_to_sap.loader import write_data_to_sap, write_data_to_bpm, gen_attchments
from src.common.email_utils import on_success, on_failure  

# 設定時區為 Asia/Taipei
local_tz = timezone("Asia/Taipei")

with DAG(
    dag_id="fx_common_to_sap_v",
    schedule="0 4 17 * *",  # 每月 17 號的 04:00
    start_date = local_tz.datetime(2025, 1, 1, 0, 0, 0),
    catchup=False,
    tags=["fx", "SAP", "common", "typeV"],
    description="常見幣別匯率寫入 SAP typeV，每月 17 執行",
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=15),
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

    crawl_cpt_fx_task = PythonOperator(
        task_id="crawl_fx_data",
        python_callable=crawl_cpt_fx
    )

    gen_fx_to_other_currencies_task = PythonOperator(
        task_id="gen_fx_to_other_currencies",
        python_callable=gen_fx_to_other_currencies,
        op_kwargs={"skipped": False}, # True or False
    )

    clean_data_for_sap_task = PythonOperator(
        task_id="clean_data_for_sap",
        python_callable=clean_data_for_sap,
        op_kwargs={"rate_type": "V"}, # M or V
    )

    write_data_to_sap_task = PythonOperator(
        task_id="write_data_to_sap",
        python_callable=write_data_to_sap
    )

    gen_attchments_task = PythonOperator(
        task_id="gen_attchments",
        python_callable=gen_attchments
    )

    start >> [check_selenium_task, check_mssql_task, check_rfc_task] >> crawl_cpt_fx_task

    crawl_cpt_fx_task >> gen_fx_to_other_currencies_task >> clean_data_for_sap_task >> write_data_to_sap_task >> gen_attchments_task >> end


