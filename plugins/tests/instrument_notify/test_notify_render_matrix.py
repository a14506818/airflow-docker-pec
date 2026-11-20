"""
信件主旨與 HTML 的排列組合
- 主旨：逾期 × 7 天內 × 其他 各 0–3 筆
- 標記：GMP × 處理中 × 新 × 兩種通知
- 剩餘天數上色邊界、空值欄位、HTML escape（每個欄位）
"""
import re
from itertools import product

import pytest

from src.instrument_notify.config import CALIBRATION, CONTRACT, URGENT_DAYS
from src.instrument_notify.mailer import build_html, build_subject, counts
from notify_factories import KINDS, TODAY, box, mail_item

RED, ORANGE = "#b91c1c", "#c2410c"


def make_items(overdue, soon, rest):
    items, n = [], 0
    for remaining, count in ((-3, overdue), (URGENT_DAYS, soon), (URGENT_DAYS + 1, rest)):
        for _ in range(count):
            n += 1
            items.append(mail_item(n, remaining))
    return items


COUNT_CASES = [c for c in product(range(4), range(4), range(4)) if sum(c)]


@pytest.mark.parametrize("overdue,soon,rest", COUNT_CASES)
def test_calibration_subject_matrix(overdue, soon, rest):
    subject = build_subject(CALIBRATION, make_items(overdue, soon, rest))
    assert subject.startswith("【儀器校驗到期】")
    body = subject[len("【儀器校驗到期】"):]
    head = []
    if overdue:
        head.append(f"{overdue} 台已逾期")
    if soon:
        head.append(f"{soon} 台 7 天內到期")
    if head:
        want = "、".join(head) + (f"，另有 {rest} 台待處理" if rest else "")
    else:
        want = f"{rest} 台待處理"
    assert body == want
    assert "0 台" not in subject  # 不會出現「0 台…」


@pytest.mark.parametrize("overdue,soon,rest", COUNT_CASES)
def test_contract_subject_matrix(overdue, soon, rest):
    subject = build_subject(CONTRACT, make_items(overdue, soon, rest))
    total = overdue + soon + rest
    want = f"【儀器合約到期】本月有 {total} 份維護保養合約即將到期"
    if overdue:
        want += f"（其中 {overdue} 份已過期）"
    assert subject == want


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("overdue,soon,rest", COUNT_CASES)
def test_stat_cards_and_sections_matrix(kind, overdue, soon, rest):
    items = make_items(overdue, soon, rest)
    html = build_html(kind, box(items), TODAY)
    assert counts(items) == (overdue, soon, rest)

    cards = re.findall(r'font-size:22px;font-weight:bold;color:[^;]+;">(\d+)</div>'
                       r'<div style="font-size:12px;color:[^;]+;">([^<]+)</div>', html)
    if kind.split_urgent:
        assert cards == [(str(overdue), "已逾期"), (str(soon), "7 天內到期"), (str(rest), "其他待處理")]
        assert ("需立即處理" in html) is bool(overdue or soon)
        assert f"完整清單（{len(items)} 台）" in html
    else:
        assert cards == [(str(overdue), "已過期"), (str(soon + rest), "即將到期")]
        assert "需立即處理" not in html
        assert f"合約清單（{len(items)} 份）" in html

    # 每台儀器在完整清單出現一次；在「需立即處理」再出現一次（僅校驗、且為急件）
    for i in items:
        expected = 1 + (1 if kind.split_urgent and i["Remaining"] <= URGENT_DAYS else 0)
        assert html.count(f"<td>{i['Asset_No']}</td>") == expected


TAGS = list(product([False, True], repeat=3))


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("gmp,processing,new", TAGS)
def test_tag_matrix(kind, gmp, processing, new):
    html = build_html(kind, box([mail_item(1, 20, is_GMP=gmp, is_Processing=processing, is_New=new)]), TODAY)
    name_cell = re.search(r"<td>儀器1(.*?)</td>", html).group(1)
    supports = set(kind.tags)
    assert (">GMP</span>" in name_cell) is (gmp and "GMP" in supports)
    assert (">處理中</span>" in name_cell) is (processing and "PROCESSING" in supports)
    assert (">新</span>" in name_cell) is (new and "NEW" in supports)


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("remaining,color,words", [
    (-30, RED, "逾期 30 天"), (-1, RED, "逾期 1 天"), (0, ORANGE, "0 天"),
    (URGENT_DAYS, ORANGE, f"{URGENT_DAYS} 天"), (URGENT_DAYS + 1, None, None), (90, None, None),
])
def test_remaining_cell_colors(kind, remaining, color, words):
    html = build_html(kind, box([mail_item(1, remaining)]), TODAY)
    full_cell = re.findall(r'<td align="right"([^>]*)>([^<]+)</td>', html)
    # 完整清單用數字
    assert (f' style="color:{color};font-weight:bold;"' if color else "", str(remaining)) in full_cell
    if kind.split_urgent and words:
        assert (f' style="color:{color};font-weight:bold;"', words) in full_cell


@pytest.mark.parametrize("kind", KINDS)
def test_column_headers_and_values(kind):
    html = build_html(kind, box([mail_item(1, 5)]), TODAY)
    for _, label in kind.columns:
        assert f">{label}</th>" in html
    assert f">{kind.date_label}</th>" in html and ">你的角色</th>" in html
    values = {"Cal_Vendor_Name": "岑祥", "Last_PR_No": "202611190002",
              "Ariba_Contract_No": "A234202600", "BPM_Contract_FormNo": "244500800"}
    for col, _ in kind.columns:
        assert f"<td>{values[col]}</td>" in html


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("empty", [None, ""])
def test_empty_optional_columns_render_dash(kind, empty):
    item = mail_item(1, 5, **{col: empty for col, _ in kind.columns})
    html = build_html(kind, box([item]), TODAY)
    assert html.count("<td>—</td>") == len(kind.columns)


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("field", ["Instrument_Name", "Asset_No", "Due_Date", "Role",
                                   "Cal_Vendor_Name", "Last_PR_No", "Ariba_Contract_No", "BPM_Contract_FormNo"])
def test_every_item_field_is_escaped(kind, field):
    payload = '<img src=x onerror="alert(1)">&'
    html = build_html(kind, box([mail_item(1, 5, **{field: payload})]), TODAY)
    assert "<img" not in html
    shown = field not in ("Cal_Vendor_Name", "Last_PR_No", "Ariba_Contract_No", "BPM_Contract_FormNo") \
        or field in dict(kind.columns)
    if shown:
        assert "&lt;img src=x onerror=&quot;alert(1)&quot;&gt;&amp;" in html


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("account,name,want", [
    ("anna_chen", "陳安娜", "本通知對象：陳安娜（anna_chen）"),
    ("anna_chen", "", "本通知對象：anna_chen（anna_chen）"),
    ("a<b", "<i>x</i>", "本通知對象：&lt;i&gt;x&lt;/i&gt;（a&lt;b）"),
])
def test_recipient_line(kind, account, name, want):
    assert want in build_html(kind, box([mail_item(1, 5)], account=account, name=name), TODAY)


@pytest.mark.parametrize("kind", KINDS)
def test_static_sections_per_kind(kind):
    html = build_html(kind, box([mail_item(1, 5)]), TODAY)
    assert kind.heading in html and kind.schedule_label in html and "2026/12/01" in html
    for text in kind.not_notified:
        assert f"<li>{text}</li>" in html
    assert kind.footer in html
    assert ("已開始處理校驗" in html) is (kind is CALIBRATION)
    assert ("待合約流程簽核完成" in html) is (kind is CONTRACT)
    # 區塊順序：提醒 → 清單 → 不會寄通知 → 處理中提醒 → 頁尾
    order = [html.index(kind.not_notified[0]), html.index(kind.footer)]
    if kind is CALIBRATION:
        order.insert(0, html.index("完整清單"))
        order.insert(2, html.index("已開始處理校驗"))
    else:
        order.insert(0, html.index("合約清單"))
        order.insert(0, html.index("待合約流程簽核完成"))
    assert order == sorted(order)


@pytest.mark.parametrize("kind", KINDS)
def test_html_is_well_formed_tables(kind):
    html = build_html(kind, box(make_items(2, 2, 2)), TODAY)
    for tag in ("table", "tr", "td", "th", "ul", "li", "span", "div"):
        opens = len(re.findall(rf"<{tag}[\s>]", html))
        closes = html.count(f"</{tag}>")
        assert opens == closes, tag
    assert "{" not in html and "}" not in html  # 沒有漏掉的 f-string 佔位


@pytest.mark.parametrize("kind", KINDS)
def test_rows_follow_item_order(kind):
    items = [mail_item(3, -5), mail_item(1, 2), mail_item(2, 40)]
    html = build_html(kind, box(items), TODAY)
    full = html[html.index("清單（"):]
    positions = [full.index(f"<td>{i['Asset_No']}</td>") for i in items]
    assert positions == sorted(positions)
