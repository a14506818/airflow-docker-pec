"""
select_due 的排列組合：啟用狀態 × 通知開關 × 日期格式 × 提前天數 × 剩餘天數，兩種通知各跑一遍
期望值依規格逐條判斷（不呼叫被測程式），任何組合不符都會列出參數
"""
from datetime import date, datetime, timedelta
from decimal import Decimal
from itertools import product

import pytest

from src.instrument_notify.config import CALIBRATION, CONTRACT
from src.instrument_notify.rules import select_due
from notify_factories import KINDS, PREV, TODAY, inst

ACTIVE = ["Y", " Y ", "N", None, ""]
ENABLE = ["Y", "Y ", "N", None, ""]
# (名稱, 產生器: due date -> 欄位值, 是否可解析)
DATE_FORMATS = [
    ("slash", lambda d: d.strftime("%Y/%m/%d"), True),
    ("dash_time", lambda d: d.strftime("%Y-%m-%d 00:00:00"), True),
    ("dash", lambda d: d.strftime("%Y-%m-%d"), True),
    ("datetime_obj", lambda d: datetime(d.year, d.month, d.day, 13, 5), True),
    ("date_obj", lambda d: d, True),
    ("none", lambda d: None, False),
    ("blank", lambda d: "", False),
    ("spaces", lambda d: "   ", False),
    ("garbage", lambda d: "N/A", False),
    ("bad_month", lambda d: "2026/13/01", False),
]
# (原始值, 解析後天數；None = 無效)
DAYS = [(30, 30), ("30", 30), (Decimal("45"), 45), (60.0, 60), (0, 0), (7, 7), (None, None), ("abc", None)]
DELTAS = [-400, -31, -1, 0, 1, 2, 365]  # 剩餘天數 = 提前天數 + delta


def expected(active, enable, date_ok, days_value, delta):
    if (active or "").strip() != "Y" or (enable or "").strip() != "Y":
        return "skip"
    if not date_ok:
        return "anomaly_date"
    if days_value is None:
        return "anomaly_days"
    return "item" if delta <= 0 else "skip"


CASES = [
    pytest.param(kind, a, e, fmt, d, delta,
                 id=f"{kind.key}-{a!r}-{e!r}-{fmt[0]}-{d[0]!r}-{delta}")
    for kind, a, e, fmt, d, delta in product(KINDS, ACTIVE, ENABLE, DATE_FORMATS, DAYS, DELTAS)
]


@pytest.mark.parametrize("kind,active,enable,fmt,days,delta", CASES)
def test_filter_matrix(kind, active, enable, fmt, days, delta):
    raw_days, days_value = days
    remaining = (days_value if days_value is not None else 30) + delta
    due = TODAY + timedelta(days=remaining)
    _, make, date_ok = fmt
    row = inst(kind=kind, due=make(due), days=raw_days, is_Active=active)
    row[kind.enable_col] = enable

    items, anomalies = select_due([row], kind, TODAY, PREV)
    want = expected(active, enable, date_ok, days_value, delta)

    if want == "item":
        assert len(items) == 1 and anomalies == []
        assert items[0]["Remaining"] == remaining
        assert items[0]["Due_Date"] == due.strftime("%Y/%m/%d")
        assert items[0]["Days_Before"] == days_value
    elif want == "skip":
        assert items == [] and anomalies == []
    elif want == "anomaly_date":
        assert items == [] and anomalies[0]["Reason"] == kind.no_date_reason
    else:
        assert items == [] and "提前通知天數" in anomalies[0]["Reason"]


@pytest.mark.parametrize("kind,other", [(CALIBRATION, CONTRACT), (CONTRACT, CALIBRATION)])
def test_kinds_do_not_read_each_others_columns(kind, other):
    """只開另一種通知的儀器，不會出現在這種通知（也不算資料異常）"""
    row = inst(kind=other, due="2026/12/05", days=30)
    assert select_due([row], kind, TODAY, PREV) == ([], [])


def test_both_kinds_enabled_on_same_instrument_are_independent():
    row = inst(kind=CALIBRATION, due="2026/12/05", days=30)
    row.update(is_Maint_Contract="Y", Contract_Expiry_Date="2027/06/30", Contract_Notify_Days_Before=60)
    assert len(select_due([row], CALIBRATION, TODAY, PREV)[0]) == 1
    assert select_due([row], CONTRACT, TODAY, PREV)[0] == []  # 合約還有 211 天


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("gmp,processing", list(product(["Y", "N", None, ""], [1, 0, None, True, False])))
def test_flags_matrix(kind, gmp, processing):
    item = select_due([inst(kind=kind, is_GMP=gmp, is_Processing=processing)], kind, TODAY, PREV)[0][0]
    assert item["is_GMP"] is (gmp == "Y")
    assert item["is_Processing"] is bool(processing)


@pytest.mark.parametrize("kind", KINDS)
def test_extra_columns_are_cleaned_and_present(kind):
    row = inst(kind=kind, Cal_Vendor_Name="  岑祥 ", Last_PR_No=None,
               Ariba_Contract_No=" A1 ", BPM_Contract_FormNo=None, Asset_No=" 1616 ", Instrument_Name=None)
    item = select_due([row], kind, TODAY, PREV)[0][0]
    for col, _ in kind.columns:
        assert item[col] == {"Cal_Vendor_Name": "岑祥", "Last_PR_No": "",
                             "Ariba_Contract_No": "A1", "BPM_Contract_FormNo": ""}[col]
    assert item["Asset_No"] == "1616" and item["Instrument_Name"] == ""


@pytest.mark.parametrize("kind", KINDS)
def test_many_rows_mixed_results_and_sorting(kind):
    rows = [
        inst(1, kind, due="2026/12/31", days=30),           # 30 天，剛好進
        inst(2, kind, due="2027/01/01", days=30),           # 31 天，不進
        inst(3, kind, due="2026/11/01", days=30),           # 逾期 30 天
        inst(4, kind, due=None),                            # 資料異常
        inst(5, kind, due="2026/12/01", days=30),           # 今天到期
        inst(6, kind, due="2026/12/01", days=30, is_Active="N"),
        inst(7, kind, due="2026/12/01", days=30, Asset_No="A0000"),  # 同天數依資產編號
    ]
    items, anomalies = select_due(rows, kind, TODAY, PREV)
    assert [i["Instrument_No"] for i in items] == [3, 7, 5, 1]
    assert [a["Instrument_No"] for a in anomalies] == [4]


def test_empty_input():
    assert select_due([], CALIBRATION, TODAY, PREV) == ([], [])
