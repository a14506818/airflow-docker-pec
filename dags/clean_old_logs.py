from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone
import os
import shutil
from airflow.models import Variable

from src.common.email_utils import on_success, on_failure  

# 設定
LOG_DIR = "/opt/airflow/logs" 
DAYS_THRESHOLD = int(Variable.get("log_retention_days", default_var=30))


def human_readable_size(size_bytes):
    for unit in ['B','KB','MB','GB','TB']:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024

def delete_old_logs():
    cutoff_time = datetime.now() - timedelta(days=DAYS_THRESHOLD)
    print(f"days_threshold: {DAYS_THRESHOLD}")
    print(f"Deleting logs older than {cutoff_time} in {LOG_DIR}")

    total_deleted_files = 0
    total_deleted_size = 0

    for root, dirs, files in os.walk(LOG_DIR):
        for name in files:
            file_path = os.path.join(root, name)
            file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))

            if file_mtime < cutoff_time:
                file_size = os.path.getsize(file_path)  # 記錄刪除前大小
                os.remove(file_path)
                total_deleted_files += 1
                total_deleted_size += file_size
                print(f"✅ Deleted file: {file_path} ({human_readable_size(file_size)})")
        
        # 嘗試移除空資料夾
        for name in dirs:
            dir_path = os.path.join(root, name)
            if not os.listdir(dir_path):  # 空資料夾
                os.rmdir(dir_path)
                print(f"Deleted empty dir: {dir_path}")

    print(f"\n🧾 Total deleted files: {total_deleted_files}")
    print(f"📉 Total deleted size: {human_readable_size(total_deleted_size)}")

# 設定時區為 Asia/Taipei
local_tz = timezone("Asia/Taipei")

with DAG(
    dag_id='clean_airflow_old_logs',
    schedule="0 8 * * *",  # 每天的 08:00
    start_date=local_tz.datetime(2025, 1, 1, 0, 0, 0),
    catchup=False,
    description=f'每天自動清理超過{DAYS_THRESHOLD}天的 Airflow log',
    tags=['maintenance', 'logs'],
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

    clean_logs_task = PythonOperator(
        task_id='delete_old_logs',
        python_callable=delete_old_logs
    )

    start >> clean_logs_task >> end