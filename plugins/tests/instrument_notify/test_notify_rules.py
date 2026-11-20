from datetime import date

import pytest

from src.instrument_notify.config import CALIBRATION, CONTRACT, ROLE_ADDL, ROLE_KEEPER, ROLE_SUB
from src.instrument_notify.rules import (
    parse_date, previous_send_date, resolve_recipients, select_due, sort_items,
)

TODAY = date(2026, 12, 1)
PREV = date(2026, 11, 15)


def inst(no=1, due="2026/12/10", days=30, **kw):
    row = {
        "Instrument_No": no, "Asset_No": f"A{no:03d}", "Instrument_Name": f"儀器{no}",
        "is_Active": "Y", "is_GMP": "N", "is_Notify_Calibration": "Y",
        "Next_Due_Date": due, "Notify_Days_Before": days, "Cal_Vendor_Name": "岑祥",
        "is_Maint_Contract": "N", "Contract_Expiry_Date": None, "Contract_Notify_Days_Before": None,
        "Ariba_Contract_No": None, "BPM_Contract_FormNo": None,
        "Keeper": "keeper_a", "Sub_Keeper": "sub_a", "Additional_Notify_Keeper": None,
        "is_Processing": 0, "Last_PR_No": None,
    }
    row.update(kw)
    return row


DEFAULT = object()


def member(account, disabled=False, email=DEFAULT):
    """email 不給 → <account>@example.com；給 None → 沒有 email"""
    return {"Account": account, "DisplayName": account.upper(), "Disabled": disabled,
            "Email": f"{account}@example.com" if email is DEFAULT else email}


MEMBERS = [member("keeper_a"), member("keeper_b"), member("sub_a"), member("addl_a")]


def due_items(rows, kind=CALIBRATION):
    items, _ = select_due(rows, kind, TODAY, PREV)
    return items


# 日期 ---------------------------------------------------------------------------------------------
@pytest.mark.parametrize("value", ["2026/12/10", "2026-12-10 00:00:00", "2026-12-10"])
def test_parse_date_formats(value):
    assert parse_date(value) == date(2026, 12, 10)


@pytest.mark.parametrize("value", [None, "", "abc", "2026/13/40"])
def test_parse_date_invalid(value):
    assert parse_date(value) is None


@pytest.mark.parametrize("today,expected", [
    (date(2026, 12, 1), date(2026, 11, 15)),
    (date(2026, 12, 15), date(2026, 12, 1)),
    (date(2026, 12, 20), date(2026, 12, 15)),
    (date(2027, 1, 1), date(2026, 12, 15)),
])
def test_previous_send_date_semi_monthly(today, expected):
    assert previous_send_date(today, (1, 15)) == expected


def test_previous_send_date_monthly_crosses_year():
    assert previous_send_date(date(2027, 1, 1), (1,)) == date(2026, 12, 1)
    assert previous_send_date(date(2027, 3, 1), (1,)) == date(2027, 2, 1)


# 篩選 ---------------------------------------------------------------------------------------------
def test_boundary_equal_to_days_before_is_included():
    assert [i["Remaining"] for i in due_items([inst(due="2026/12/31", days=30)])] == [30]


def test_one_day_beyond_days_before_is_excluded():
    assert due_items([inst(due="2027/01/01", days=30)]) == []


def test_overdue_is_included():
    assert due_items([inst(due="2026/11/20")])[0]["Remaining"] == -11


@pytest.mark.parametrize("override", [
    {"is_Notify_Calibration": "N"}, {"is_Notify_Calibration": None}, {"is_Active": "N"},
])
def test_not_enabled_or_inactive_is_skipped_without_anomaly(override):
    items, anomalies = select_due([inst(**override)], CALIBRATION, TODAY, PREV)
    assert items == [] and anomalies == []


def test_missing_due_date_goes_to_anomalies():
    items, anomalies = select_due([inst(due=None)], CALIBRATION, TODAY, PREV)
    assert items == []
    assert "沒有上次校驗日" in anomalies[0]["Reason"]


def test_missing_days_before_goes_to_anomalies():
    _, anomalies = select_due([inst(days=None)], CALIBRATION, TODAY, PREV)
    assert "提前通知天數" in anomalies[0]["Reason"]


def test_gmp_is_included_and_flagged():
    item = due_items([inst(is_GMP="Y")])[0]
    assert item["is_GMP"] is True


def test_processing_flag():
    assert due_items([inst(is_Processing=1)])[0]["is_Processing"] is True
    assert due_items([inst(is_Processing=0)])[0]["is_Processing"] is False


def test_new_entry_flag():
    # 上次寄送日 11/15：到期 12/20 → 當時剩 35 天 > 30（新）；到期 12/10 → 當時剩 25 天（舊）
    items = due_items([inst(1, due="2026/12/20"), inst(2, due="2026/12/10")])
    flags = {i["Instrument_No"]: i["is_New"] for i in items}
    assert flags == {1: True, 2: False}


def test_sort_overdue_first_then_remaining():
    items = due_items([inst(1, due="2026/12/20"), inst(2, due="2026/11/20"), inst(3, due="2026/12/05")])
    assert [i["Instrument_No"] for i in items] == [2, 3, 1]


def test_contract_kind_uses_contract_columns():
    row = inst(is_Notify_Calibration="N", is_Maint_Contract="Y", Contract_Expiry_Date="2026-12-31 00:00:00",
               Contract_Notify_Days_Before=60, Ariba_Contract_No="A234202600", BPM_Contract_FormNo="244500800")
    item = due_items([row], CONTRACT)[0]
    assert item["Due_Date"] == "2026/12/31"
    assert item["Ariba_Contract_No"] == "A234202600" and item["BPM_Contract_FormNo"] == "244500800"
    assert due_items([row], CALIBRATION) == []


def test_contract_missing_expiry_goes_to_anomalies():
    row = inst(is_Maint_Contract="Y", Contract_Expiry_Date="", Contract_Notify_Days_Before=60)
    _, anomalies = select_due([row], CONTRACT, TODAY, PREV)
    assert "未填合約到期日" in anomalies[0]["Reason"]


# 收件人 -------------------------------------------------------------------------------------------
def boxes_by_account(mailboxes):
    return {b["Account"]: b for b in mailboxes}


def test_keeper_receives_and_sub_keeper_does_not():
    boxes, undeliverable, issues = resolve_recipients(due_items([inst()]), MEMBERS)
    assert list(boxes_by_account(boxes)) == ["keeper_a"]
    assert boxes[0]["items"][0]["Role"] == ROLE_KEEPER
    assert undeliverable == [] and issues == []


@pytest.mark.parametrize("keeper", ["resigned", "ghost", ""])
def test_invalid_keeper_falls_back_to_sub_keeper(keeper):
    members = MEMBERS + [member("resigned", disabled=True)]
    boxes, undeliverable, issues = resolve_recipients(due_items([inst(Keeper=keeper)]), members)
    assert list(boxes_by_account(boxes)) == ["sub_a"]
    assert boxes[0]["items"][0]["Role"] == ROLE_SUB
    assert issues[0]["Role"] == "保管人" and "組保管人代收" in issues[0]["Reason"]
    assert undeliverable == []


def test_both_invalid_is_undeliverable():
    items = due_items([inst(Keeper="ghost", Sub_Keeper=None)])
    boxes, undeliverable, _ = resolve_recipients(items, MEMBERS)
    assert boxes == []
    assert undeliverable[0]["Asset_No"] == "A001"
    assert items[0]["Recipients"] == "（無法送達）"


def test_additional_keeper_always_receives():
    boxes, _, _ = resolve_recipients(due_items([inst(Additional_Notify_Keeper="addl_a")]), MEMBERS)
    by = boxes_by_account(boxes)
    assert set(by) == {"keeper_a", "addl_a"}
    assert by["addl_a"]["items"][0]["Role"] == ROLE_ADDL


def test_additional_keeper_receives_even_when_undeliverable():
    items = due_items([inst(Keeper=None, Sub_Keeper=None, Additional_Notify_Keeper="addl_a")])
    boxes, undeliverable, _ = resolve_recipients(items, MEMBERS)
    assert list(boxes_by_account(boxes)) == ["addl_a"] and len(undeliverable) == 1


def test_invalid_additional_keeper_is_an_issue():
    _, _, issues = resolve_recipients(due_items([inst(Additional_Notify_Keeper="ghost")]), MEMBERS)
    assert issues[0]["Role"] == "額外通知人" and issues[0]["Value"] == "ghost"


def test_one_mail_per_account_merges_instruments_and_roles():
    rows = [inst(1), inst(2, Keeper="keeper_b", Additional_Notify_Keeper="keeper_a")]
    boxes, _, _ = resolve_recipients(due_items(rows), MEMBERS)
    by = boxes_by_account(boxes)
    roles = {i["Instrument_No"]: i["Role"] for i in by["keeper_a"]["items"]}
    assert roles == {1: ROLE_KEEPER, 2: ROLE_ADDL}


def test_same_person_keeper_and_additional_keeps_highest_role_once():
    boxes, _, _ = resolve_recipients(due_items([inst(Additional_Notify_Keeper="KEEPER_A")]), MEMBERS)
    assert len(boxes) == 1
    assert [i["Role"] for i in boxes[0]["items"]] == [ROLE_KEEPER]


def test_accounts_sharing_email_still_get_separate_mails():
    members = [member("keeper_a", email="same@example.com"), member("keeper_b", email="same@example.com")]
    boxes, _, _ = resolve_recipients(due_items([inst(1), inst(2, Keeper="keeper_b")]), members)
    assert [b["Account"] for b in boxes] == ["keeper_a", "keeper_b"]


def test_valid_account_without_email_does_not_fall_back():
    members = [member("keeper_a", email=None), member("sub_a")]
    boxes, undeliverable, _ = resolve_recipients(due_items([inst()]), members)
    assert [(b["Account"], b["Email"]) for b in boxes] == [("keeper_a", "")]
    assert undeliverable == []


def test_mailbox_items_are_sorted():
    rows = [inst(1, due="2026/12/20"), inst(2, due="2026/11/20")]
    boxes, _, _ = resolve_recipients(due_items(rows), MEMBERS)
    assert [i["Instrument_No"] for i in boxes[0]["items"]] == [2, 1]


def test_recipients_column_for_excel():
    items = due_items([inst(Additional_Notify_Keeper="addl_a")])
    resolve_recipients(items, MEMBERS)
    assert items[0]["Recipients"] == "keeper_a(保管人), addl_a(額外通知)"


def test_sort_items_stable_by_asset():
    items = [{"Remaining": 5, "Asset_No": "B", "Instrument_No": 2},
             {"Remaining": 5, "Asset_No": "A", "Instrument_No": 1}]
    assert [i["Asset_No"] for i in sort_items(items)] == ["A", "B"]
