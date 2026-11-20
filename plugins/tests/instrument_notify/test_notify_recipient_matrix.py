"""
resolve_recipients 的排列組合
1. 保管人 × 組保管人 × 額外通知人 各 5 種帳號狀態（125 種），期望值依規格逐條推導
2. 同一人兼多個角色（帳號重疊）的所有組合
3. 隨機產生大量儀器，檢查合併後的不變量
"""
import random
from itertools import product

import pytest

from src.instrument_notify.config import CALIBRATION, ROLE_ADDL, ROLE_KEEPER, ROLE_RANK, ROLE_SUB
from src.instrument_notify.rules import missing_email, resolve_recipients, select_due
from notify_factories import PREV, TODAY, inst, member

# 帳號狀態 → (建立 member 的參數 or None=不在人事資料, 主檔欄位值是否留空)
STATES = {
    "ok":       {"member": {}, "blank": False},
    "no_email": {"member": {"email": None}, "blank": False},
    "disabled": {"member": {"disabled": True}, "blank": False},
    "unknown":  {"member": None, "blank": False},
    "empty":    {"member": None, "blank": True},
}
REASON = {"disabled": "已離職／帳號停用", "unknown": "人事資料查無此帳號", "empty": "未設定"}


def valid(state):
    return state in ("ok", "no_email")


def build(keeper_state, sub_state, addl_state):
    accounts = {"Keeper": ("k_user", keeper_state), "Sub_Keeper": ("s_user", sub_state),
                "Additional_Notify_Keeper": ("a_user", addl_state)}
    row, members = {}, []
    for col, (account, state) in accounts.items():
        cfg = STATES[state]
        row[col] = "" if cfg["blank"] else account
        if cfg["member"] is not None:
            members.append(member(account, **cfg["member"]))
    members.append(member("bystander"))  # 人事資料不可為空
    return inst(kind=CALIBRATION, **row), members


@pytest.mark.parametrize("k,s,a", list(product(STATES, STATES, STATES)))
def test_three_role_state_matrix(k, s, a):
    row, members = build(k, s, a)
    items, _ = select_due([row], CALIBRATION, TODAY, PREV)
    mailboxes, undeliverable, issues = resolve_recipients(items, members)
    got = {b["Account"]: (b["items"][0]["Role"], b["Email"]) for b in mailboxes}

    want, want_issues = {}, []
    if valid(k):
        want["k_user"] = ROLE_KEEPER
    elif valid(s):
        want["s_user"] = ROLE_SUB
        want_issues.append(("保管人", f"{REASON[k]}，改由組保管人代收"))
    if valid(a):
        want["a_user"] = ROLE_ADDL
    elif a in ("disabled", "unknown"):
        want_issues.append(("額外通知人", REASON[a]))

    assert {acc: role for acc, (role, _) in got.items()} == want
    # 有效但沒有 email：仍建立 mailbox（之後寄送時略過），不遞補給下一順位
    for acc, (_, email) in got.items():
        state = {"k_user": k, "s_user": s, "a_user": a}[acc]
        assert email == ("" if state == "no_email" else f"{acc}@example.com")

    assert [(i["Role"], i["Reason"]) for i in issues] == want_issues

    # 有效但沒有 email 的收件人，一定出現在給 IT 的「無 Email」清單
    state_of = {"k_user": k, "s_user": s, "a_user": a}
    assert [r["Account"] for r in missing_email(mailboxes)] == \
        sorted(acc for acc in want if state_of[acc] == "no_email")

    if valid(k) or valid(s):
        assert undeliverable == []
    else:
        assert len(undeliverable) == 1
        assert undeliverable[0]["Reason"] == f"保管人：{REASON[k]}；組保管人：{REASON[s]}"

    parts = [f"{acc}({role})" for acc, role in want.items()]
    assert items[0]["Recipients"] == (", ".join(parts) if parts else "（無法送達）")


ACCOUNTS = ["p", "q", ""]


@pytest.mark.parametrize("k,s,a", list(product(ACCOUNTS, ACCOUNTS, ACCOUNTS)))
def test_overlapping_accounts_matrix(k, s, a):
    """同一人可能同時是保管人／組保管人／額外通知人：每人一封、同台儀器只出現一次、角色取最高"""
    row = inst(kind=CALIBRATION, Keeper=k, Sub_Keeper=s, Additional_Notify_Keeper=a)
    items, _ = select_due([row], CALIBRATION, TODAY, PREV)
    mailboxes, undeliverable, _ = resolve_recipients(items, [member("p"), member("q")])

    want = {}
    primary = (k, ROLE_KEEPER) if k else ((s, ROLE_SUB) if s else None)
    for acc, role in filter(None, [primary, (a, ROLE_ADDL) if a else None]):
        if acc not in want or ROLE_RANK[role] < ROLE_RANK[want[acc]]:
            want[acc] = role

    got = {b["Account"]: [i["Role"] for i in b["items"]] for b in mailboxes}
    assert got == {acc: [role] for acc, role in want.items()}
    assert len(undeliverable) == (0 if primary else 1)


@pytest.mark.parametrize("variant", ["KEEPER_A", " keeper_a ", "Keeper_A", "keeper_a"])
def test_account_matching_is_case_and_space_insensitive(variant):
    items, _ = select_due([inst(Keeper=variant, Additional_Notify_Keeper="KEEPER_a")], CALIBRATION, TODAY, PREV)
    mailboxes, _, issues = resolve_recipients(items, [member("keeper_a"), member("sub_a")])
    assert [b["Account"] for b in mailboxes] == ["keeper_a"]  # 用人事資料的帳號寫法
    assert [i["Role"] for i in mailboxes[0]["items"]] == [ROLE_KEEPER]
    assert issues == []


@pytest.mark.parametrize("seed", range(40))
def test_random_portfolio_invariants(seed):
    rnd = random.Random(seed)
    pool = [f"u{n}" for n in range(12)]
    members = [member(u, disabled=rnd.random() < 0.2, email=None if rnd.random() < 0.15 else f"{u}@x.com")
               for u in pool[:10]]  # u10、u11 不在人事資料
    status = {m["Account"]: not m["Disabled"] for m in members}

    def pick():
        r = rnd.random()
        return "" if r < 0.15 else rnd.choice(pool)

    rows = [inst(n, kind=CALIBRATION, due=f"2026/12/{rnd.randint(1, 28):02d}", days=30,
                 Keeper=pick(), Sub_Keeper=pick(), Additional_Notify_Keeper=pick())
            for n in range(1, rnd.randint(1, 60) + 1)]
    items, _ = select_due(rows, CALIBRATION, TODAY, PREV)
    mailboxes, undeliverable, issues = resolve_recipients(items, members)
    by_no = {i["Instrument_No"]: i for i in items}
    is_valid = lambda acc: bool(acc) and status.get(acc, False)

    # 每個帳號只有一個 mailbox，且都是有效帳號
    accounts = [b["Account"] for b in mailboxes]
    assert len(accounts) == len(set(accounts)) and accounts == sorted(accounts)
    assert all(is_valid(acc) for acc in accounts)

    # 同一 mailbox 內同台儀器只出現一次，且已排序
    for b in mailboxes:
        nos = [i["Instrument_No"] for i in b["items"]]
        assert len(nos) == len(set(nos))
        assert [i["Remaining"] for i in b["items"]] == sorted(i["Remaining"] for i in b["items"])

    received = {}  # Instrument_No -> {account: role}
    for b in mailboxes:
        for i in b["items"]:
            received.setdefault(i["Instrument_No"], {})[b["Account"]] = i["Role"]

    undeliverable_nos = {u["Instrument_No"] for u in undeliverable}
    for no, row in by_no.items():
        got = received.get(no, {})
        k, s, a = row["Keeper"], row["Sub_Keeper"], row["Additional_Notify_Keeper"]
        if is_valid(k):
            assert got.get(k) == ROLE_KEEPER
            assert no not in undeliverable_nos
        elif is_valid(s):
            assert got.get(s) == ROLE_SUB  # 兼額外通知人時仍取較高的「組保管人代收」
            assert no not in undeliverable_nos
        else:
            assert no in undeliverable_nos
        if is_valid(a):
            assert a in got  # 額外通知人一律收（角色可能因兼保管人而更高）
        # 不該收到的人不會收到
        expected_accounts = {x for x in (a,) if is_valid(x)}
        expected_accounts |= {k} if is_valid(k) else ({s} if is_valid(s) else set())
        assert set(got) == expected_accounts

    # 每台儀器都有 Recipients 欄位（給 Excel）
    assert all("Recipients" in i for i in items)
    # 異常筆數：保管人無效但組保管人有效、額外通知人有填但無效
    want_issues = sum(1 for r in by_no.values() if not is_valid(r["Keeper"]) and is_valid(r["Sub_Keeper"]))
    want_issues += sum(1 for r in by_no.values()
                       if r["Additional_Notify_Keeper"] and not is_valid(r["Additional_Notify_Keeper"]))
    assert len(issues) == want_issues


def test_display_name_and_email_come_from_member_data():
    items, _ = select_due([inst()], CALIBRATION, TODAY, PREV)
    mailboxes, _, _ = resolve_recipients(items, [member("keeper_a", name="陳安娜", email=" anna@x.com ")])
    assert mailboxes[0]["DisplayName"] == "陳安娜"
    assert mailboxes[0]["Email"] == "anna@x.com"


def test_no_items_gives_no_mailboxes():
    assert resolve_recipients([], [member("keeper_a")]) == ([], [], [])


def test_items_are_not_shared_between_mailboxes():
    """各 mailbox 的 item 是複本：改一個人的 Role 不影響別人"""
    items, _ = select_due([inst(Additional_Notify_Keeper="addl_a")], CALIBRATION, TODAY, PREV)
    mailboxes, _, _ = resolve_recipients(items, [member("keeper_a"), member("addl_a")])
    mailboxes[0]["items"][0]["Role"] = "X"
    assert mailboxes[1]["items"][0]["Role"] != "X"
    assert "Role" not in items[0]


@pytest.mark.parametrize("seed", range(40))
def test_random_missing_email_covers_every_unsendable_mailbox(seed):
    """隨機資料：每個沒有 email 的 mailbox 都會列給 IT，且儀器數、儀器清單一致"""
    rnd = random.Random(1000 + seed)
    pool = [f"u{n}" for n in range(8)]
    members = [member(u, email=None if rnd.random() < 0.4 else f"{u}@x.com") for u in pool]
    rows = [inst(n, kind=CALIBRATION, due=f"2026/12/{rnd.randint(1, 28):02d}", days=30,
                 Keeper=rnd.choice(pool), Additional_Notify_Keeper=rnd.choice(pool + [""]))
            for n in range(1, rnd.randint(1, 30) + 1)]
    items, _ = select_due(rows, CALIBRATION, TODAY, PREV)
    mailboxes, _, _ = resolve_recipients(items, members)
    listed = {r["Account"]: r for r in missing_email(mailboxes)}
    for b in mailboxes:
        if b["Email"]:
            assert b["Account"] not in listed
        else:
            r = listed[b["Account"]]
            assert r["Count"] == len(b["items"])
            for i in b["items"]:
                assert f"{i['Asset_No']} {i['Instrument_Name']}（{i['Role']}）" in r["Instruments"]
