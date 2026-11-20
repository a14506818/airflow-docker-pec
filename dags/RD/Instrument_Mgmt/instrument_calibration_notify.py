from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import timedelta
from pendulum import timezone

from src.common.common import is_on_schedule
from src.common.check_connection import check_mssql
from src.instrument_notify.extractor import get_instruments, get_members
from src.instrument_notify.tasks import send_notifications, gen_attchments
from src.common.email_utils import on_success, on_failure

# 設定時區為 Asia/Taipei
local_tz = timezone("Asia/Taipei")

with DAG(
    dag_id="instrument_calibration_notify",
    schedule="0 8 1,15 * *",  # 每月 1、15 號 08:00
    start_date=local_tz.datetime(2025, 1, 1, 0, 0, 0),
    catchup=False,
    max_active_runs=1,
    tags=["RD", "instrument"],
    description="儀器校驗到期通知：每月 1、15 號寄給保管人（組保管人遞補、額外通知人副知），每人一封",
    params={
        "dry_run": Param(False, type="boolean",
                         description="True：只產出清單與 IT 摘要，不寄信給使用者"),
    },
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
        "on_failure_callback": on_failure,  # 摘要與告警只寄 IT（email_receiver_list）
    }
) as dag:

    start = EmptyOperator(task_id="start")
    on_schedule_check = ShortCircuitOperator(
        task_id="on_schedule_check",
        python_callable=is_on_schedule
    )
    end = EmptyOperator(
        task_id="end",
        on_success_callback=on_success
    )

    check_mssql_task = PythonOperator(
        task_id="check_mssql_connection",
        python_callable=check_mssql
    )

    get_instruments_task = PythonOperator(
        task_id="get_instruments",
        python_callable=get_instruments
    )

    get_members_task = PythonOperator(
        task_id="get_members",
        python_callable=get_members
    )

    send_notifications_task = PythonOperator(
        task_id="send_notifications",
        python_callable=send_notifications,
        op_kwargs={"kind_key": "calibration"},
        retries=0,  # 不自動重試，避免重複寄信給使用者
    )

    gen_attchments_task = PythonOperator(
        task_id="gen_attchments",
        python_callable=gen_attchments
    )

    start >> on_schedule_check >> check_mssql_task >> [get_instruments_task, get_members_task]
    [get_instruments_task, get_members_task] >> send_notifications_task >> gen_attchments_task >> end
