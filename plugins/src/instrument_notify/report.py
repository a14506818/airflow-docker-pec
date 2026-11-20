"""
儀器管理到期通知 — IT 摘要（Email 內文）與 Excel 分頁定義
純 Python，不 import airflow。
"""
from html import escape

BASE_COLS = [("Instrument_No", "儀器序號"), ("Asset_No", "固資編號"), ("Instrument_Name", "儀器名稱")]


def sheets(kind):
    """(分頁名稱, report key, [(欄位 key, 欄位標題)])"""
    return [
        ("通知清單", "items", BASE_COLS + [
            ("Due_Date", kind.date_label), ("Remaining", "剩餘天數"), ("Days_Before", "提前通知天數"),
        ] + list(kind.columns) + [
            ("is_GMP", "GMP"), ("is_Processing", "處理中"), ("is_New", "新進清單"),
            ("Keeper", "保管人"), ("Sub_Keeper", "組保管人"), ("Additional_Notify_Keeper", "額外通知人"),
            ("Recipients", "實際收件人"),
        ]),
        ("寄送紀錄", "sent", [
            ("Account", "帳號"), ("DisplayName", "姓名"), ("Email", "Email"), ("Count", "儀器數"),
            ("Subject", "主旨"), ("Status", "狀態"),
        ]),
        ("無法送達", "undeliverable", BASE_COLS + [
            ("Due_Date", kind.date_label), ("Remaining", "剩餘天數"),
            ("Keeper", "保管人"), ("Sub_Keeper", "組保管人"), ("Reason", "說明"),
        ]),
        ("無Email", "no_email", [
            ("Account", "帳號"), ("DisplayName", "姓名"), ("Count", "儀器數"), ("Instruments", "儀器（角色）"),
        ]),
        ("收件人異常", "issues", BASE_COLS + [("Role", "角色"), ("Value", "帳號"), ("Reason", "說明")]),
        ("資料異常", "anomalies", BASE_COLS + [("Reason", "說明")]),
    ]


def summary_rows(report):
    sent = report["sent"]
    failed = sum(1 for s in sent if s["Status"].startswith("失敗"))
    return [
        ("執行時間", report["run_time"]),
        ("模式", "Dry-run（未寄給使用者）" if report["mode"] == "DRY_RUN" else "正式寄送"),
        ("通知類型", report["heading"]),
        ("進清單儀器數", len(report["items"])),
        ("收件人數（有 email）", len(sent)),
        ("寄送失敗", failed),
        ("無法送達（保管人、組保管人皆無效）", len(report["undeliverable"])),
        ("無 Email（帳號有效但查不到 email，未寄出）", len(report.get("no_email", []))),
        ("收件人異常", len(report["issues"])),
        ("資料異常", len(report["anomalies"])),
    ]


def build_summary_html(report):
    cell = 'style="border:1px solid #ccc;padding:4px 8px;"'
    summary = "".join(
        f"<tr><td {cell}>{escape(str(k))}</td><td {cell}>{escape(str(v))}</td></tr>"
        for k, v in summary_rows(report)
    )
    warn = ""
    if report["mode"] == "DRY_RUN":
        warn = '<p style="color:#b45309;"><b>⚠️ 本次為 Dry-run，未寄信給使用者。</b></p>'
    undeliverable = ""
    if report["undeliverable"]:
        rows = "".join(
            f"<tr><td {cell}>{escape(r['Asset_No'])}</td><td {cell}>{escape(r['Instrument_Name'])}</td>"
            f"<td {cell}>{escape(r['Due_Date'])}</td><td {cell}>{escape(r['Reason'])}</td></tr>"
            for r in report["undeliverable"]
        )
        undeliverable = f"""
        <h4 style="color:#b91c1c;">無法送達：請協助轉知或更新主檔保管人</h4>
        <table style="border-collapse:collapse;">
          <tr><th {cell}>固資編號</th><th {cell}>儀器名稱</th><th {cell}>到期日</th><th {cell}>原因</th></tr>
          {rows}
        </table>"""
    no_email = ""
    if report.get("no_email"):
        rows = "".join(
            f"<tr><td {cell}>{escape(r['Account'])}</td><td {cell}>{escape(r['DisplayName'])}</td>"
            f"<td {cell}>{r['Count']}</td><td {cell}>{escape(r['Instruments'])}</td></tr>"
            for r in report["no_email"]
        )
        no_email = f"""
        <h4 style="color:#b91c1c;">無 Email：以下收件人查不到 email，通知未寄出，請補 email 或轉知</h4>
        <table style="border-collapse:collapse;">
          <tr><th {cell}>帳號</th><th {cell}>姓名</th><th {cell}>儀器數</th><th {cell}>儀器（角色）</th></tr>
          {rows}
        </table>"""
    return f"""
    <h3>{escape(report['heading'])}：執行摘要</h3>
    {warn}
    <table style="border-collapse:collapse;">{summary}</table>
    {undeliverable}
    {no_email}
    <p>明細（通知清單、寄送紀錄、無 Email、收件人異常、資料異常）見 Excel 附件。</p>
    """
