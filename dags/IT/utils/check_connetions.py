from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import datetime
from pendulum import timezone

from src.common.common import is_on_schedule
from src.common.check_connection import check_selenium, check_mssql, check_rfc

local_tz = timezone("Asia/Taipei")

with DAG(
    dag_id="check_connetions",
    schedule="0 8 * * *", 
    start_date = local_tz.datetime(2025, 1, 1, 0, 0, 0),
    catchup=False,
    tags=["demo", "selenium"],
) as dag:
    
    on_schedule_check = ShortCircuitOperator(
        task_id="on_schedule_check",
        python_callable=is_on_schedule
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


    on_schedule_check >> check_selenium_task >> check_mssql_task >> check_rfc_task