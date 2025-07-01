from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone

from src.common.check_connection import check_selenium, check_mssql, check_rfc
from src.fx_to_sap.extractor import crawl_cpt_fx
from src.fx_to_sap.transformer import gen_fx_to_USD, clean_data_for_sap


# 設定時區為 Asia/Taipei
local_tz = timezone("Asia/Taipei")

with DAG(
    dag_id="fx_common_to_sap_m",
    schedule="0 3 7,17,27 * *",  # 每月 7, 17, 27 號的 03:00
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["fx", "SAP", "common", "typeM"],
    description="常見幣別匯率寫入 SAP typeM，每月 7, 17, 27 執行",
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=10),
    }
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

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
        task_id="crawl_cpt_fx",
        python_callable=crawl_cpt_fx
    )

    gen_fx_to_USD_task = PythonOperator(
        task_id="gen_fx_to_USD",
        python_callable=gen_fx_to_USD,
        op_kwargs={"skipped": False}, # True or False
    )

    clean_data_for_sap_task = PythonOperator(
        task_id="clean_data_for_sap",
        python_callable=clean_data_for_sap,
        op_kwargs={"rate_type": "M"}, # M or V
    )

    start >> [check_selenium_task, check_mssql_task, check_rfc_task] \
      >> crawl_cpt_fx_task >> gen_fx_to_USD_task >> clean_data_for_sap_task >> end


