from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pendulum import timezone

from src.common.check_connection import check_selenium, check_mssql, check_rfc

local_tz = timezone("Asia/Taipei")

with DAG(
    dag_id="check_connetions",
    schedule="0 8 * * *", 
    start_date = local_tz.datetime(2025, 1, 1, 0, 0, 0),
    catchup=False,
    tags=["demo", "selenium"],
) as dag:
    
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


    check_selenium_task >> check_mssql_task >> check_rfc_task