"""
Airflow task callables 端到端（不連 DB、不寄真信）：
extractor 的資料轉換、send_notifications 的 dry-run／寄送／部分失敗、gen_attchments 的 Excel 與摘要
"""
from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace

import pandas as pd
import pendulum
import pytest

from src.instrument_notify import extractor, tasks
from src.instrument_notify.config import CALIBRATION, CONTRACT, KINDS
from src.instrument_notify.report import sheets
from notify_factories import inst, member


class FakeTI:
    def __init__(self, xcom):
        self.xcom = dict(xcom)

    def xcom_pull(self, task_ids, key="return_value"):
        return self.xcom.get((task_ids, key))

    def xcom_push(self, key, value):
        self.xcom[("__pushed__", key)] = value


class FakeSender:
    def __init__(self, fail_for=()):
        self.calls, self.fail_for = [], set(fail_for)

    def __call__(self, to, subject, html_content):
        if to[0] in self.fail_for:
            raise RuntimeError("SMTP down")
        self.calls.append({"to": to, "subject": subject, "html": html_content})


@pytest.fixture
def frozen_now(monkeypatch):
    fixed = pendulum.datetime(2026, 12, 1, 8, 0, tz=tasks.TZ)
    monkeypatch.setattr(pendulum, "now", lambda tz=None: fixed)
    return fixed


def portfolio(kind):
    return [
        inst(1, kind, due="2026/11/20", days=30),                                     # 逾期 → keeper_a
        inst(2, kind, due="2026/12/05", days=30, Additional_Notify_Keeper="addl_a"),  # 7 天內 → keeper_a + addl_a
        inst(3, kind, due="2026/12/20", days=30, Keeper="gone"),                      # 組保管人代收
        inst(4, kind, due="2026/12/25", days=30, Keeper="gone", Sub_Keeper=None),     # 無法送達
        inst(5, kind, due="2027/06/01", days=30),                                     # 不進清單
        inst(6, kind, due=None),                                                      # 資料異常
        inst(7, kind, due="2026/12/10", days=30, Keeper="noemail"),                   # 有效但沒 email
    ]


MEMBERS = [member("keeper_a"), member("sub_a"), member("addl_a"),
           member("gone", disabled=True), member("noemail", email=None)]


def run_send(kind, monkeypatch, dry_run=False, sender=None):
    sender = sender or FakeSender()
    monkeypatch.setattr(tasks, "send_email", sender)
    ti = FakeTI({("get_instruments", "return_value"): portfolio(kind),
                 ("get_members", "return_value"): MEMBERS})
    report = tasks.send_notifications(kind.key, ti=ti, params={"dry_run": dry_run})
    return report, sender, ti


@pytest.mark.parametrize("kind", [CALIBRATION, CONTRACT])
def test_send_notifications_end_to_end(kind, monkeypatch, frozen_now):
    report, sender, _ = run_send(kind, monkeypatch)
    assert report["mode"] == "SEND" and report["run_time"] == "2026/12/01 08:00"
    assert [i["Instrument_No"] for i in report["items"]] == [1, 2, 7, 3, 4]
    assert [a["Instrument_No"] for a in report["anomalies"]] == [6]
    assert [u["Instrument_No"] for u in report["undeliverable"]] == [4]
    assert [(i["Instrument_No"], i["Role"]) for i in report["issues"]] == [(3, "保管人")]
    assert report["no_email"] == [{"Account": "noemail", "DisplayName": "NOEMAIL", "Count": 1,
                                   "Instruments": "A0007 儀器7（保管人）"}]

    to = sorted(c["to"][0] for c in sender.calls)
    assert to == ["addl_a@example.com", "keeper_a@example.com", "sub_a@example.com"]  # noemail 不寄
    by_to = {c["to"][0]: c for c in sender.calls}
    assert by_to["keeper_a@example.com"]["subject"].startswith(kind.subject_tag)
    assert "本通知對象：KEEPER_A（keeper_a）" in by_to["keeper_a@example.com"]["html"]
    assert "組保管人代收" in by_to["sub_a@example.com"]["html"]
    assert [s["Status"] for s in report["sent"]] == ["已寄出"] * 3
    assert {s["Account"]: s["Count"] for s in report["sent"]} == {"addl_a": 1, "keeper_a": 2, "sub_a": 1}


@pytest.mark.parametrize("dry_run", [True, "true", "True", "1", "yes"])
def test_dry_run_sends_nothing(dry_run, monkeypatch, frozen_now):
    report, sender, _ = run_send(CALIBRATION, monkeypatch, dry_run=dry_run)
    assert sender.calls == [] and report["mode"] == "DRY_RUN"
    assert [r["Account"] for r in report["no_email"]] == ["noemail"]  # dry-run 也要列出
    assert all(s["Status"].startswith("DRY_RUN") for s in report["sent"])
    assert len(report["sent"]) == 3


@pytest.mark.parametrize("dry_run", [False, "false", "0", "", None])
def test_not_dry_run_values(dry_run, monkeypatch, frozen_now):
    _, sender, _ = run_send(CALIBRATION, monkeypatch, dry_run=dry_run)
    assert len(sender.calls) == 3


def test_partial_failure_raises_after_sending_others(monkeypatch, frozen_now):
    sender = FakeSender(fail_for={"keeper_a@example.com"})
    monkeypatch.setattr(tasks, "send_email", sender)
    ti = FakeTI({("get_instruments", "return_value"): portfolio(CALIBRATION),
                 ("get_members", "return_value"): MEMBERS})
    with pytest.raises(RuntimeError, match="keeper_a"):
        tasks.send_notifications("calibration", ti=ti, params={})
    assert sorted(c["to"][0] for c in sender.calls) == ["addl_a@example.com", "sub_a@example.com"]
    pushed = ti.xcom[("__pushed__", "return_value")]
    assert [s["Status"][:2] for s in pushed["sent"]] == ["已寄", "失敗", "已寄"]


@pytest.mark.parametrize("missing", ["get_instruments", "get_members"])
def test_missing_upstream_data_raises(missing, monkeypatch, frozen_now):
    xcom = {("get_instruments", "return_value"): portfolio(CALIBRATION),
            ("get_members", "return_value"): MEMBERS}
    xcom[(missing, "return_value")] = []
    monkeypatch.setattr(tasks, "send_email", FakeSender())
    with pytest.raises(ValueError):
        tasks.send_notifications("calibration", ti=FakeTI(xcom), params={})


def test_nothing_due_sends_nothing(monkeypatch, frozen_now):
    sender = FakeSender()
    monkeypatch.setattr(tasks, "send_email", sender)
    ti = FakeTI({("get_instruments", "return_value"): [inst(1, due="2027/12/31")],
                 ("get_members", "return_value"): MEMBERS})
    report = tasks.send_notifications("calibration", ti=ti, params={})
    assert sender.calls == [] and report["items"] == [] and report["sent"] == []
    assert report["no_email"] == []


# gen_attchments -----------------------------------------------------------------------------------
@pytest.mark.parametrize("kind", [CALIBRATION, CONTRACT])
@pytest.mark.parametrize("dry_run", [False, True])
def test_gen_attchments_excel_and_summary(kind, dry_run, monkeypatch, frozen_now, tmp_path):
    report, _, _ = run_send(kind, monkeypatch, dry_run=dry_run)
    monkeypatch.setattr(tasks, "EXPORT_DIR", str(tmp_path))
    ti = FakeTI({("send_notifications", "return_value"): report})
    context = {"ti": ti, "dag": SimpleNamespace(dag_id=f"instrument_{kind.key}_notify"),
               "dag_run": SimpleNamespace(start_date=datetime(2026, 12, 1, 0, 0, tzinfo=pendulum.UTC))}
    [path] = tasks.gen_attchments(**context)

    assert path.endswith("__20261201_0800" + ("__DRYRUN" if dry_run else "") + ".xlsx")
    book = pd.read_excel(path, sheet_name=None)
    assert list(book) == ["執行摘要"] + [title for title, _, _ in sheets(kind)]
    for title, key, columns in sheets(kind):
        assert list(book[title].columns) == [label for _, label in columns]
        assert len(book[title]) == len(report[key])
    summary = dict(zip(book["執行摘要"]["項目"], book["執行摘要"]["值"]))
    assert summary["進清單儀器數"] == 5 and summary["無法送達（保管人、組保管人皆無效）"] == 1
    assert summary["無 Email（帳號有效但查不到 email，未寄出）"] == 1
    assert book["無Email"]["帳號"].tolist() == ["noemail"]

    html = ti.xcom[("__pushed__", "summary_html")]
    assert kind.heading in html and "無法送達" in html
    assert "無 Email" in html and "noemail" in html and "A0007 儀器7（保管人）" in html
    assert ("Dry-run" in html) is dry_run


@pytest.mark.parametrize("kind", [CALIBRATION, CONTRACT])
def test_every_excel_column_exists_in_report_rows(kind, monkeypatch, frozen_now):
    """Excel 欄位定義與規則產出的欄位一致（避免欄位改名後 Excel 變空白）"""
    report, _, _ = run_send(kind, monkeypatch)
    for title, key, columns in sheets(kind):
        for row in report[key]:
            missing = [k for k, _ in columns if k not in row]
            assert missing == [], (title, missing)


def test_gen_attchments_without_report_raises():
    with pytest.raises(ValueError):
        tasks.gen_attchments(ti=FakeTI({}), dag=None, dag_run=None)


# extractor ---------------------------------------------------------------------------------------
def test_get_instruments_makes_values_json_safe(monkeypatch):
    raw = [dict(inst(1), Notify_Days_Before=Decimal("30"), Next_Due_Date=datetime(2026, 12, 1),
                is_Processing=1, Last_PR_No=None)]
    monkeypatch.setattr(extractor, "_fetch_dicts", lambda sql: [dict(r) for r in raw])
    [row] = extractor.get_instruments()
    assert row["Notify_Days_Before"] == "30" and row["Next_Due_Date"] == "2026-12-01 00:00:00"
    assert row["is_Processing"] == 1 and row["Last_PR_No"] is None
    # 轉成字串後仍可被規則正確解析
    from src.instrument_notify.rules import select_due
    from datetime import date
    items, _ = select_due([dict(row, is_Notify_Calibration="Y")], CALIBRATION, date(2026, 11, 20), date(2026, 11, 15))
    assert items[0]["Remaining"] == 11 and items[0]["Days_Before"] == 30


def test_get_instruments_sql_reads_processing_and_pr(monkeypatch):
    captured = {}
    monkeypatch.setattr(extractor, "_fetch_dicts", lambda sql: captured.setdefault("sql", sql) and [inst(1)])
    extractor.get_instruments()
    sql = captured["sql"]
    assert "Cal_Result = N'PROCESSING'" in sql and "Last_PR_No" in sql and "is_Processing" in sql


@pytest.mark.parametrize("fn", ["get_instruments", "get_members"])
def test_extractors_raise_on_empty(fn, monkeypatch):
    monkeypatch.setattr(extractor, "_fetch_dicts", lambda sql: [])
    with pytest.raises(ValueError):
        getattr(extractor, fn)()


@pytest.mark.parametrize("disabled,expected", [(1, True), (0, False), (True, True), (False, False), (None, False)])
def test_get_members_normalizes_disabled(disabled, expected, monkeypatch):
    monkeypatch.setattr(extractor, "_fetch_dicts", lambda sql: [
        {"Account": "a", "DisplayName": "A", "Disabled": disabled, "Email": None}])
    assert extractor.get_members()[0]["Disabled"] is expected


def test_kinds_registry():
    assert set(KINDS) == {"calibration", "contract"}
