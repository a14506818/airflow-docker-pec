from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from src.common.check_connection import check_selenium, check_mssql, check_rfc

with DAG(
    dag_id="check_connetions",
    start_date=datetime(2025, 6, 24),
    schedule_interval=None,
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