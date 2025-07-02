from airflow.utils.email import send_email
import pendulum
import logging

def send_success_email(dag_id: str, description: str, execution_date, to: list):
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
    send_email(to=to, subject=subject, html_content=html_content)

def on_success(context):
    send_success_email(
        dag_id=context['dag'].dag_id,
        description=context['dag'].description,
        execution_date=context['execution_date'],
        to=["justin_yang@pharmaessentia.com"]
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