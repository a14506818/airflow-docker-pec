"""儀器管理到期通知 — Airflow task callables"""
import pandas as pd
import pendulum
from airflow.utils.email import send_email

from src.instrument_notify.config import KINDS
from src.instrument_notify.mailer import send_all
from src.instrument_notify.report import build_summary_html, sheets, summary_rows
from src.instrument_notify.rules import missing_email, previous_send_date, resolve_recipients, select_due
from src.instrument_sync.loader import is_dry_run

TZ = "Asia/Taipei"
EXPORT_DIR = "/opt/airflow/export"


def send_notifications(kind_key, **context):
    """篩選到期儀器 → 決定收件人 → 每個帳號寄一封；dry-run 不寄給使用者"""
    kind = KINDS[kind_key]
    ti = context["ti"]
    dry_run = is_dry_run(context.get("params"))
    instruments = ti.xcom_pull(task_ids="get_instruments")
    members = ti.xcom_pull(task_ids="get_members")
    if not instruments or not members:
        raise ValueError("❌ 取不到儀器主檔或人事資料，請檢查上游任務")

    now = pendulum.now(TZ)
    today = now.date()
    prev_send = previous_send_date(today, kind.send_days)
    items, anomalies = select_due(instruments, kind, today, prev_send)
    mailboxes, undeliverable, issues = resolve_recipients(items, members)
    print(f"✅ {kind.heading}：今天 {today}、上次寄送日 {prev_send}；進清單 {len(items)} {kind.unit}、"
          f"收件人 {len(mailboxes)} 位、無法送達 {len(undeliverable)}、資料異常 {len(anomalies)}")

    sent = send_all(kind, mailboxes, today, sender=send_email, dry_run=dry_run)

    report = {
        "kind": kind.key, "heading": kind.heading,
        "mode": "DRY_RUN" if dry_run else "SEND",
        "run_time": now.format("YYYY/MM/DD HH:mm"),
        "items": items, "sent": sent, "undeliverable": undeliverable,
        "no_email": missing_email(mailboxes),
        "issues": issues, "anomalies": anomalies,
    }
    failed = [s["Account"] for s in sent if s["Status"].startswith("失敗")]
    if failed:
        ti.xcom_push(key="return_value", value=report)
        raise RuntimeError(f"❌ {len(failed)} 封寄送失敗：{', '.join(failed)}（其餘已寄出，請勿整批重跑）")
    return report


def gen_attchments(**context):
    """IT 摘要：Excel 附件 + Email 內文（XCom key summary_html，由 on_success 帶入信件）"""
    ti = context["ti"]
    report = ti.xcom_pull(task_ids="send_notifications")
    if not report:
        raise ValueError("❌ 取不到通知結果，請檢查上游任務")
    kind = KINDS[report["kind"]]

    local_time = pendulum.instance(context["dag_run"].start_date).in_timezone(TZ)
    suffix = "__DRYRUN" if report["mode"] == "DRY_RUN" else ""
    file_path = (f"{EXPORT_DIR}/FinalData__{context['dag'].dag_id}__"
                 f"{local_time.strftime('%Y%m%d_%H%M')}{suffix}.xlsx")

    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        pd.DataFrame(summary_rows(report), columns=["項目", "值"]).to_excel(
            writer, sheet_name="執行摘要", index=False)
        for title, key, columns in sheets(kind):
            rows = report.get(key, [])
            df = pd.DataFrame([[r.get(k, "") for k, _ in columns] for r in rows],
                              columns=[label for _, label in columns])
            df.to_excel(writer, sheet_name=title, index=False)
    print(f"✅ 成功寫入 Excel 檔案: {file_path}")

    ti.xcom_push(key="summary_html", value=build_summary_html(report))
    return [file_path]
