from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone

from src.bpm_member_data_compare.extractor import get_UAT_member_data, get_PRD_member_data
from src.bpm_member_data_compare.comparator import compare_member_data, gen_attchments

from src.common.email_utils import on_success, on_failure  


# 設定時區為 Asia/Taipei
local_tz = timezone("Asia/Taipei")

with DAG(
    dag_id="bpm_member_data_compare",
    schedule="0 12 * * 1",  # 每周一的 12:00
    start_date = local_tz.datetime(2025, 1, 1, 0, 0, 0),
    catchup=False,
    tags=["BPM", "member", "compare"],
    description="BPM Member 資料比對，每周一的 12:00執行",
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

    get_UAT_member_data_task = PythonOperator(
        task_id="get_UAT_member_data",
        python_callable=get_UAT_member_data
    )

    get_PRD_member_data_task = PythonOperator(
        task_id="get_PRD_member_data",
        python_callable=get_PRD_member_data
    )
    
    compare_member_data_task = PythonOperator(
        task_id="compare_member_data",
        python_callable=compare_member_data
    )

    gen_attchments_task = PythonOperator(
        task_id="gen_attchments",
        python_callable=gen_attchments
    )


    start >> [get_UAT_member_data_task, get_PRD_member_data_task] >> compare_member_data_task >> gen_attchments_task >> end