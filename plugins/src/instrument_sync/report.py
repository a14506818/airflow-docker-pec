"""
儀器主檔每日 SAP 資料同步 — 每日報表內容（Excel 分頁定義 + Email 內文）
純 Python，不 import airflow。
"""
from html import escape

from src.instrument_sync.config import FIELD_LABELS

# (分頁名稱, report key, [(欄位 key, 欄位標題)], 處理者)
SHEETS = [
    ("異動明細", "changes", [
        ("Instrument_No", "儀器序號"), ("Asset_No", "資產編號"), ("Instrument_Name", "儀器名稱"),
        ("Field_Label", "欄位"), ("Field", "欄位代碼"), ("Old", "舊值"), ("New", "新值"),
    ], "IT"),
    ("SAP查無資產", "sap_missing", [
        ("Instrument_No", "儀器序號"), ("Asset_No", "資產編號"), ("Instrument_Name", "儀器名稱"),
        ("Reason", "說明"),
    ], "儀器管理窗口"),
    ("保管人變更", "keeper_changes", [
        ("Instrument_No", "儀器序號"), ("Asset_No", "資產編號"), ("Instrument_Name", "儀器名稱"),
        ("Old_Keeper", "原保管人帳號"), ("Old_Keeper_Name", "原保管人姓名"),
        ("New_Keeper", "新保管人帳號"), ("New_Keeper_Name", "新保管人姓名"), ("SAP_Keeper", "SAP 保管人"),
    ], "儀器管理窗口"),
    ("保管人異常", "keeper_anomalies", [
        ("Instrument_No", "儀器序號"), ("Asset_No", "資產編號"), ("Instrument_Name", "儀器名稱"),
        ("Role", "角色"), ("Value", "帳號／SAP 帳號"), ("Reason", "說明"),
    ], "儀器管理窗口"),
    ("SAP資料略過", "sap_skipped", [
        ("Instrument_No", "儀器序號"), ("Asset_No", "資產編號"), ("Instrument_Name", "儀器名稱"),
        ("Reason", "說明"),
    ], "IT"),
]


def mode_label(report):
    return "Dry-run（預覽，未寫入主檔）" if report.get("mode") == "DRY_RUN" else "正式同步"


def summary_rows(report):
    """執行摘要：[(項目, 值)]"""
    s = report["stats"]
    rows = [
        ("執行時間", report.get("run_time", "")),
        ("模式", mode_label(report)),
        ("SAP 資產筆數", s["sap_rows"]),
        ("主檔儀器筆數", s["master_rows"]),
        ("主檔有資產編號", s["master_with_asset"]),
        ("與 SAP 比對成功", s["matched"]),
        ("異動儀器數", s["changed_instruments"]),
        ("異動欄位數", s["change_count"]),
        ("SAP 查無資產", s["sap_missing"]),
        ("保管人變更", s["keeper_changes"]),
        ("保管人異常", s["keeper_anomalies"]),
        ("SAP 資料略過", s["sap_skipped"]),
    ]
    for field, count in sorted(s["field_counts"].items(), key=lambda kv: -kv[1]):
        rows.append((f"　欄位異動：{FIELD_LABELS.get(field, field)}", count))
    return rows


def build_summary_html(report):
    """Email 內文：執行摘要 + 需人工處理區塊的筆數"""
    cell = 'style="border:1px solid #ccc;padding:4px 8px;"'
    summary = "".join(
        f"<tr><td {cell}>{escape(str(k))}</td><td {cell}>{escape(str(v))}</td></tr>"
        for k, v in summary_rows(report)
    )
    sections = "".join(
        f"<tr><td {cell}>{escape(title)}</td><td {cell}>{len(report.get(key, []))}</td>"
        f"<td {cell}>{escape(owner)}</td></tr>"
        for title, key, _, owner in SHEETS
    )
    warn = ""
    if report.get("mode") == "DRY_RUN":
        warn = '<p style="color:#b45309;"><b>⚠️ 本次為 Dry-run，主檔與異動紀錄皆未變更。</b></p>'
    return f"""
    <h3>儀器主檔 SAP 同步報表</h3>
    {warn}
    <table style="border-collapse:collapse;">{summary}</table>
    <h4>報表區塊（明細見 Excel 附件）</h4>
    <table style="border-collapse:collapse;">
      <tr><th {cell}>區塊</th><th {cell}>筆數</th><th {cell}>處理者</th></tr>
      {sections}
    </table>
    """
