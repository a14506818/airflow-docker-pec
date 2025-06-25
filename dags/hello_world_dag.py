from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# 定義任務執行的函數
def say_hello():
    print("Hello Airflow!")
    
# 建立 DAG 物件
with DAG(
    dag_id="hello_world_dag",
    description="最簡單的 Hello DAG",
    schedule_interval="@daily",  # 每日執行
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    task_hello = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello
    )
