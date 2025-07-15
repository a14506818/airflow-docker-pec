from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone

from src.common.check_connection import check_mssql, check_rfc
from src.supplier_assessment_tasks.supplier_assessment_pending_list_loader import get_GR_data, clean_data, group_data

from src.common.email_utils import on_success, on_failure  


# 設定時區為 Asia/Taipei
local_tz = timezone("Asia/Taipei")

with DAG(
    dag_id="supplier_assessment_pending_list_loader",
    schedule="10 12 1 7 *",  # 每年 7 月 1 號的 12:10
    start_date = local_tz.datetime(2025, 1, 1, 0, 0, 0),
    catchup=False,
    tags=["supplier", "partner", "assessment", "pending list", "loader"],
    description="待考核供應商清單生成，7/1 執行",
    default_args={
        # "retries": 2,
        # "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=15),
        # "on_failure_callback": on_failure,
    }
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(
        task_id="end",
        # on_success_callback=on_success
    )

    check_mssql_task = PythonOperator(
        task_id="check_mssql_connection",
        python_callable=check_mssql
    )

    check_rfc_task = PythonOperator(
        task_id="check_rfc_connection",
        python_callable=check_rfc
    )

    get_GR_data_task = PythonOperator(
        task_id="get_GR_data",
        python_callable=get_GR_data
    )

    clean_data_task = PythonOperator(
        task_id="clean_data",
        python_callable=clean_data
    )

    group_data_task = PythonOperator(
        task_id="group_data",
        python_callable=group_data
    )




    start >> [check_mssql_task, check_rfc_task] \
        >> get_GR_data_task >> clean_data_task >> group_data_task >> end