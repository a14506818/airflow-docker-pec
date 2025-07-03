from airflow.utils.email import send_email
import pendulum
import logging
import os

def send_success_email(dag_id: str, description: str, execution_date, to: list, attachments: list = None):
    logging.info("📤 Preparing to send success notification email...")
    tz = pendulum.timezone("Asia/Taipei")
    local_time = execution_date.in_timezone(tz)

    subject = f"[Airflow] DAG Execution Success - {dag_id}"
    html_content = f"""
    <h3>✅ DAG executed successfully</h3>
    <ul>
        <li><b>DAG ID:</b> {dag_id}</li>
        <li><b>Description:</b> {description}</li>
        <li><b>Execution Date & Time:</b> {local_time.strftime('%Y-%m-%d %H:%M:%S')} (CST)</li>
    </ul>
    <p>This is an automated system notification. Please do not reply.</p>
    """
    # 過濾出存在的檔案
    valid_attachments = []
    if attachments:
        for file in attachments:
            if file and os.path.isfile(file):
                valid_attachments.append(file)
            else:
                logging.warning(f"⚠️ 附件不存在或為空: {file}")

    send_email(to=to, subject=subject, html_content=html_content, files=valid_attachments)

def on_success(context): # 前一個task 必須是 gen_attchments，才會寄附件
    # 取得前一個 task instance（這裡假設 end 只有一個 upstream）
    try:
        ti = context["ti"] 
        xcom_value = ti.xcom_pull(task_ids="gen_attchments") 
        logging.info(f"✅ 成功從上一個任務取得 XCom：{xcom_value}")
    except Exception as e:
        logging.error(f"❌ 無法取得上一個任務的 XCom 值: {e}")
        xcom_value = None  # 如果無法取得，則設為 None

    # 用來寄信
    send_success_email(
        dag_id=context['dag'].dag_id,
        description=context['dag'].description,
        execution_date=context['execution_date'],
        to=["justin_yang@pharmaessentia.com"],
        attachments=xcom_value  # 可加參數傳路徑
    )

def send_failure_email(dag_id: str, description: str, execution_date, to: list):
    logging.info("📤 Preparing to send failure notification email...")
    tz = pendulum.timezone("Asia/Taipei")
    local_time = execution_date.in_timezone(tz)

    subject = f"[Airflow] DAG Execution Failed - {dag_id}"
    html_content = f"""
    <h3>❌ DAG execution failed</h3>
    <ul>
        <li><b>DAG ID:</b> {dag_id}</li>
        <li><b>Description:</b> {description}</li>
        <li><b>Execution Date & Time:</b> {local_time.strftime('%Y-%m-%d %H:%M:%S')} (CST)</li>
    </ul>
    <p>This is an automated system notification. Please do not reply.</p>
    """
    send_email(to=to, subject=subject, html_content=html_content)

def on_failure(context):
    send_failure_email(
        dag_id=context['dag'].dag_id,
        description=context['dag'].description,
        execution_date=context['execution_date'],
        to=["justin_yang@pharmaessentia.com"]
    )