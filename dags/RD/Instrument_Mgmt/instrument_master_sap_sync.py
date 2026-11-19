from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import timedelta
from pendulum import timezone

from src.common.common import is_on_schedule
from src.common.check_connection import check_mssql, check_rfc
from src.instrument_sync.extractor import get_sap_assets, get_hr_members
from src.instrument_sync.loader import sync_master, gen_attchments
from src.common.email_utils import on_success, on_failure, on_sla_miss

# 設定時區為 Asia/Taipei
local_tz = timezone("Asia/Taipei")

with DAG(
    dag_id="instrument_master_sap_sync",
    schedule="0 2 * * *",  # 每日 02:00
    start_date=local_tz.datetime(2025, 1, 1, 0, 0, 0),
    catchup=False,
    max_active_runs=1,  # 前一次尚未結束時，本次不啟動
    tags=["RD", "instrument", "SAP"],
    description="儀器主檔每日 SAP 資料同步（SRS-20261118-001），每日 02:00 執行",
    params={
        "dry_run": Param(False, type="boolean",
                         description="True：只比對並產出差異清單，不寫入主檔"),
    },
    sla_miss_callback=on_sla_miss,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
        "execution_timeout": timedelta(minutes=30),
        "on_failure_callback": on_failure,  # 報表與告警只寄 IT（email_receiver_list）
    }
) as dag:

    start = EmptyOperator(task_id="start")
    on_schedule_check = ShortCircuitOperator(
        task_id="on_schedule_check",
        python_callable=is_on_schedule
    )
    end = EmptyOperator(
        task_id="end",
        on_success_callback=on_success,
        sla=timedelta(hours=2),  # 02:00 排程 → 04:00 尚未完成即告警
    )

    check_mssql_task = PythonOperator(
        task_id="check_mssql_connection",
        python_callable=check_mssql
    )

    check_rfc_task = PythonOperator(
        task_id="check_rfc_connection",
        python_callable=check_rfc
    )

    get_sap_assets_task = PythonOperator(
        task_id="get_sap_assets",
        python_callable=get_sap_assets
    )

    get_hr_members_task = PythonOperator(
        task_id="get_hr_members",
        python_callable=get_hr_members
    )

    sync_master_task = PythonOperator(
        task_id="sync_master",
        python_callable=sync_master
    )

    gen_attchments_task = PythonOperator(
        task_id="gen_attchments",
        python_callable=gen_attchments
    )

    start >> on_schedule_check >> [check_mssql_task, check_rfc_task] >> get_sap_assets_task
    check_mssql_task >> get_hr_members_task
    [get_sap_assets_task, get_hr_members_task] >> sync_master_task >> gen_attchments_task >> end
