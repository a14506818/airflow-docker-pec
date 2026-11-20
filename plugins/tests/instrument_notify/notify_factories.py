"""instrument_notify 測試共用的資料工廠（非測試檔，pytest 不會收集）"""
from datetime import date, timedelta

from src.instrument_notify.config import CALIBRATION, CONTRACT

TODAY = date(2026, 12, 1)
PREV = date(2026, 11, 15)
DEFAULT = object()


def iso(d):
    return d.strftime("%Y/%m/%d")


def inst(no=1, kind=CALIBRATION, due=DEFAULT, days=30, **kw):
    """
    一筆主檔資料；kind 決定把 due/days 放在哪組欄位，另一組欄位維持不通知
    due 預設 = TODAY + 9 天
    """
    if due is DEFAULT:
        due = iso(TODAY + timedelta(days=9))
    row = {
        "Instrument_No": no, "Asset_No": f"A{no:04d}", "Instrument_Name": f"儀器{no}",
        "is_Active": "Y", "is_GMP": "N",
        "is_Notify_Calibration": "N", "Next_Due_Date": None, "Notify_Days_Before": None,
        "Cal_Vendor_Name": "岑祥",
        "is_Maint_Contract": "N", "Contract_Expiry_Date": None, "Contract_Notify_Days_Before": None,
        "Ariba_Contract_No": "A234202600", "BPM_Contract_FormNo": "244500800",
        "Keeper": "keeper_a", "Sub_Keeper": "sub_a", "Additional_Notify_Keeper": None,
        "is_Processing": 0, "Last_PR_No": "202611190002",
    }
    row[kind.enable_col] = "Y"
    row[kind.date_col] = due
    row[kind.days_col] = days
    row.update(kw)
    return row


def member(account, disabled=False, email=DEFAULT, name=None):
    """email 不給 → <account>@example.com；給 None / '' → 沒有 email"""
    return {"Account": account, "DisplayName": name or account.upper(), "Disabled": disabled,
            "Email": f"{account}@example.com" if email is DEFAULT else email}


def mail_item(no=1, remaining=5, **kw):
    """mailer 用的 item（已含 Role）"""
    i = {"Instrument_No": no, "Asset_No": f"A{no:04d}", "Instrument_Name": f"儀器{no}",
         "Due_Date": "2026/12/10", "Remaining": remaining, "Days_Before": 30, "Role": "保管人",
         "is_GMP": False, "is_Processing": False, "is_New": False,
         "Cal_Vendor_Name": "岑祥", "Last_PR_No": "202611190002",
         "Ariba_Contract_No": "A234202600", "BPM_Contract_FormNo": "244500800"}
    i.update(kw)
    return i


def box(items, account="anna_chen", name="陳安娜", email="anna@example.com"):
    return {"Account": account, "DisplayName": name, "Email": email, "items": items}


KINDS = [CALIBRATION, CONTRACT]
