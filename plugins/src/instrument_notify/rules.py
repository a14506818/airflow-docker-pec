"""
儀器管理到期通知（校驗／合約）— 篩選與收件人規則

純 Python：不 import airflow / pyodbc，方便單元測試（plugins/tests/instrument_notify/test_rules.py）。
"""
from datetime import date, datetime, timedelta

from src.instrument_notify.config import (
    ADDL_KEEPER_COL, KEEPER_COL, ROLE_ADDL, ROLE_KEEPER, ROLE_RANK, ROLE_SUB, SUB_KEEPER_COL,
    URGENT_DAYS,
)
from src.instrument_sync.rules import MemberIndex, clean


# 日期 ---------------------------------------------------------------------------------------------
def parse_date(value):
    """主檔日期欄位有 'yyyy/MM/dd' 與 'yyyy-MM-dd HH:mm:ss' 兩種格式；解析不了回傳 None"""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    s = clean(value)[:10].replace("-", "/")
    try:
        return datetime.strptime(s, "%Y/%m/%d").date()
    except ValueError:
        return None


def previous_send_date(today, send_days):
    """today 之前（不含 today）最近一次的寄送日，例如 send_days=(1, 15)：12/01 → 11/15、12/10 → 12/01"""
    d = today - timedelta(days=1)
    while d.day not in send_days:
        d -= timedelta(days=1)
    return d


def fmt_date(d):
    return d.strftime("%Y/%m/%d")


def _to_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# 篩選 ---------------------------------------------------------------------------------------------
def select_due(instruments, kind, today, prev_send):
    """
    依 kind 篩出要通知的儀器
    回傳 (items, anomalies)
      items      依剩餘天數排序（逾期在前）
      anomalies  要通知但資料不全、無法計算的儀器（給 IT 摘要）
    """
    items, anomalies = [], []
    for row in instruments:
        if clean(row.get("is_Active")) != "Y" or clean(row.get(kind.enable_col)) != "Y":
            continue
        base = {
            "Instrument_No": row["Instrument_No"],
            "Asset_No": clean(row.get("Asset_No")),
            "Instrument_Name": clean(row.get("Instrument_Name")),
        }
        due = parse_date(row.get(kind.date_col))
        if due is None:
            anomalies.append(dict(base, Reason=kind.no_date_reason))
            continue
        days_before = _to_int(row.get(kind.days_col))
        if days_before is None:
            anomalies.append(dict(base, Reason=f"未設定提前通知天數（{kind.days_col}）"))
            continue

        remaining = (due - today).days
        if remaining > days_before:
            continue

        item = dict(
            base,
            Due_Date=fmt_date(due),
            Remaining=remaining,
            Days_Before=days_before,
            is_GMP=clean(row.get("is_GMP")) == "Y",
            is_Processing=bool(row.get("is_Processing")),
            is_New=(due - prev_send).days > days_before,  # 上次寄送時還不在清單內
        )
        for col, _ in kind.columns:
            item[col] = clean(row.get(col))
        for _, col in (KEEPER_COL, SUB_KEEPER_COL, ADDL_KEEPER_COL):
            item[col] = clean(row.get(col))
        items.append(item)
    return sort_items(items), anomalies


def sort_items(items):
    """逾期在前，接著剩餘天數由少到多；同天數依資產編號"""
    return sorted(items, key=lambda i: (i["Remaining"], i["Asset_No"], i["Instrument_No"]))


def is_urgent(item):
    return item["Remaining"] <= URGENT_DAYS


# 收件人 -------------------------------------------------------------------------------------------
def _lookup(index, account):
    if not account:
        return None, "未設定"
    return index.lookup_account(account)


def resolve_recipients(items, members):
    """
    收件人：保管人 → 組保管人（遞補）；額外通知人一律收
    依帳號合併，每個帳號一封
    回傳 (mailboxes, undeliverable, issues)
      mailboxes      [{"Account", "DisplayName", "Email", "items": [item + Role]}]，依帳號排序
      undeliverable  保管人與組保管人都無效的儀器（改通知 IT）
      issues         個別角色帳號無效（保管人改由組保管人代收、額外通知人無效），給 IT 摘要
    每個 item 會加上 Recipients 欄位（給 Excel）
    """
    index = MemberIndex(members)
    emails = {}
    for m in members:
        account, email = clean(m.get("Account")), clean(m.get("Email"))
        if account and email:
            emails[account.casefold()] = email

    boxes = {}

    def add(member, role, item):
        key = member["Account"].casefold()
        box = boxes.setdefault(key, {
            "Account": member["Account"],
            "DisplayName": member["DisplayName"],
            "Email": emails.get(key, ""),
            "roles": {},
        })
        old = box["roles"].get(item["Instrument_No"])
        if old is None or ROLE_RANK[role] < ROLE_RANK[old[0]]:
            box["roles"][item["Instrument_No"]] = (role, item)

    undeliverable, issues = [], []
    for item in items:
        base = {k: item[k] for k in ("Instrument_No", "Asset_No", "Instrument_Name")}
        assigned = []

        keeper, k_reason = _lookup(index, item[KEEPER_COL[1]])
        if keeper:
            primary = (keeper, ROLE_KEEPER)
        else:
            sub, s_reason = _lookup(index, item[SUB_KEEPER_COL[1]])
            if sub:
                primary = (sub, ROLE_SUB)
                issues.append(dict(base, Role=KEEPER_COL[0], Value=item[KEEPER_COL[1]],
                                   Reason=f"{k_reason}，改由組保管人代收"))
            else:
                primary = None
                undeliverable.append(dict(
                    base, Due_Date=item["Due_Date"], Remaining=item["Remaining"],
                    Keeper=item[KEEPER_COL[1]], Sub_Keeper=item[SUB_KEEPER_COL[1]],
                    Reason=f"保管人：{k_reason}；組保管人：{s_reason}",
                ))
        if primary:
            add(primary[0], primary[1], item)
            assigned.append(f"{primary[0]['Account']}({primary[1]})")

        addl_account = item[ADDL_KEEPER_COL[1]]
        if addl_account:
            addl, a_reason = _lookup(index, addl_account)
            if addl:
                add(addl, ROLE_ADDL, item)
                assigned.append(f"{addl['Account']}({ROLE_ADDL})")
            else:
                issues.append(dict(base, Role=ADDL_KEEPER_COL[0], Value=addl_account, Reason=a_reason))

        item["Recipients"] = ", ".join(assigned) if assigned else "（無法送達）"

    mailboxes = []
    for key in sorted(boxes):
        # 注意：沒有 email 的帳號也保留 mailbox（不遞補給下一順位），由 missing_email() 列給 IT
        box = boxes[key]
        box_items = [dict(item, Role=role) for role, item in box.pop("roles").values()]
        box["items"] = sort_items(box_items)
        mailboxes.append(box)
    return mailboxes, undeliverable, issues


def missing_email(mailboxes):
    """帳號有效但查不到 email 的收件人：寄不出去，列給 IT 處理（補 email 或改主檔保管人）"""
    return [
        {
            "Account": box["Account"],
            "DisplayName": box["DisplayName"],
            "Count": len(box["items"]),
            "Instruments": "、".join(f"{i['Asset_No']} {i['Instrument_Name']}（{i['Role']}）"
                                    for i in box["items"]),
        }
        for box in mailboxes if not box["Email"]
    ]
