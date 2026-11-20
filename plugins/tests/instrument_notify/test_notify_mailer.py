from datetime import date

import pytest

from src.instrument_notify.config import CALIBRATION, CONTRACT
from src.instrument_notify.mailer import build_html, build_subject, send_all
from src.instrument_notify.report import build_summary_html, summary_rows

TODAY = date(2026, 12, 1)


def item(no, remaining, **kw):
    i = {"Instrument_No": no, "Asset_No": f"A{no:03d}", "Instrument_Name": f"儀器{no}",
         "Due_Date": "2026/12/10", "Remaining": remaining, "Role": "保管人",
         "is_GMP": False, "is_Processing": False, "is_New": False,
         "Cal_Vendor_Name": "岑祥", "Last_PR_No": "", "Ariba_Contract_No": "A1", "BPM_Contract_FormNo": ""}
    i.update(kw)
    return i


def box(items, account="anna_chen", name="陳安娜", email="anna@example.com"):
    return {"Account": account, "DisplayName": name, "Email": email, "items": items}


# 主旨 ---------------------------------------------------------------------------------------------
@pytest.mark.parametrize("remainings,expected", [
    ([3, 5, 20, 40], "【儀器校驗到期】2 台 7 天內到期，另有 2 台待處理"),
    ([-11, 4, 20], "【儀器校驗到期】1 台已逾期、1 台 7 天內到期，另有 1 台待處理"),
    ([7], "【儀器校驗到期】1 台 7 天內到期"),
    ([8, 30], "【儀器校驗到期】2 台待處理"),
])
def test_calibration_subject(remainings, expected):
    assert build_subject(CALIBRATION, [item(n, r) for n, r in enumerate(remainings)]) == expected


def test_contract_subject():
    assert build_subject(CONTRACT, [item(1, 20), item(2, 50)]) == "【儀器合約到期】本月有 2 份維護保養合約即將到期"
    assert build_subject(CONTRACT, [item(1, -3), item(2, 50)]) == \
        "【儀器合約到期】本月有 2 份維護保養合約即將到期（其中 1 份已過期）"


# 內文 ---------------------------------------------------------------------------------------------
def test_html_names_recipient_and_roles():
    html = build_html(CALIBRATION, box([item(1, 3, Role="組保管人代收")]), TODAY)
    assert "本通知對象：陳安娜（anna_chen）" in html
    assert "組保管人代收" in html
    assert "2026/12/01" in html


def test_calibration_html_sections_and_tags():
    items = [item(1, -11, is_Processing=True), item(2, 4, is_New=True), item(3, 19, is_GMP=True)]
    html = build_html(CALIBRATION, box(items), TODAY)
    assert "需立即處理" in html and "逾期 11 天" in html
    assert "完整清單（3 台）" in html
    for label in ("處理中", "新", "GMP"):
        assert f">{label}</span>" in html
    assert "以下情況不會寄出通知" in html and "是否通知校驗" in html
    assert "已開始處理校驗" in html


def test_calibration_html_without_urgent_items_has_no_urgent_section():
    html = build_html(CALIBRATION, box([item(1, 20)]), TODAY)
    assert "需立即處理" not in html


def test_contract_html():
    html = build_html(CONTRACT, box([item(1, 30, is_New=True, Ariba_Contract_No="A234202600")]), TODAY)
    assert "合約清單（1 份）" in html and "A234202600" in html
    assert "合約編號" in html and "Ariba" not in html
    assert "待合約流程簽核完成" in html
    assert "有維護合約" in html
    assert "需立即處理" not in html and "已開始處理校驗" not in html
    assert ">GMP</span>" not in html


def test_html_escapes_data():
    html = build_html(CALIBRATION, box([item(1, 3, Instrument_Name="<script>x</script>")]), TODAY)
    assert "<script>" not in html and "&lt;script&gt;" in html


def test_empty_optional_columns_show_dash():
    html = build_html(CALIBRATION, box([item(1, 20, Last_PR_No="")]), TODAY)
    assert "<td>—</td>" in html


# 寄送 ---------------------------------------------------------------------------------------------
class FakeSender:
    def __init__(self, fail_for=()):
        self.sent, self.fail_for = [], set(fail_for)

    def __call__(self, to, subject, html_content):
        if to[0] in self.fail_for:
            raise RuntimeError("SMTP down")
        self.sent.append((to, subject))


def test_send_one_mail_per_box():
    sender = FakeSender()
    log = send_all(CALIBRATION, [box([item(1, 3)]), box([item(2, 20)], "bob", "Bob", "bob@example.com")],
                   TODAY, sender)
    assert [to for to, _ in sender.sent] == [["anna@example.com"], ["bob@example.com"]]
    assert [r["Status"] for r in log] == ["已寄出", "已寄出"]


def test_dry_run_sends_nothing():
    sender = FakeSender()
    log = send_all(CALIBRATION, [box([item(1, 3)])], TODAY, sender, dry_run=True)
    assert sender.sent == [] and log[0]["Status"].startswith("DRY_RUN")


def test_no_email_is_skipped_and_not_logged():
    sender = FakeSender()
    log = send_all(CALIBRATION, [box([item(1, 3)], email="")], TODAY, sender)
    assert sender.sent == [] and log == []


def test_one_failure_does_not_stop_others():
    sender = FakeSender(fail_for={"anna@example.com"})
    log = send_all(CALIBRATION, [box([item(1, 3)]), box([item(2, 20)], "bob", "Bob", "bob@example.com")],
                   TODAY, sender)
    assert log[0]["Status"].startswith("失敗") and log[1]["Status"] == "已寄出"


# IT 摘要 ------------------------------------------------------------------------------------------
def test_summary_counts_and_undeliverable_table():
    report = {
        "heading": "儀器校驗到期通知", "mode": "DRY_RUN", "run_time": "2026/12/01 08:00",
        "items": [item(1, 3)], "sent": [{"Status": "已寄出"}, {"Status": "失敗：x"}],
        "undeliverable": [{"Asset_No": "A9", "Instrument_Name": "儀器9", "Due_Date": "2026/12/03",
                           "Reason": "保管人：未設定；組保管人：未設定"}],
        "issues": [], "anomalies": [{}],
        "no_email": [{"Account": "a<b", "DisplayName": "王", "Count": 2, "Instruments": "A1 x（保管人）"}],
    }
    rows = dict(summary_rows(report))
    assert rows["寄送失敗"] == 1 and rows["資料異常"] == 1
    html = build_summary_html(report)
    assert "Dry-run" in html and "無法送達" in html and "A9" in html
    assert rows["無 Email（帳號有效但查不到 email，未寄出）"] == 1
    assert "無 Email" in html and "a&lt;b" in html and "<td style=\"border:1px solid #ccc;padding:4px 8px;\">a<b" not in html


def test_summary_without_no_email_section():
    report = {"heading": "儀器校驗到期通知", "mode": "SEND", "run_time": "x", "items": [], "sent": [],
              "undeliverable": [], "issues": [], "anomalies": [], "no_email": []}
    html = build_summary_html(report)
    assert "請補 email" not in html and "無法送達：" not in html
