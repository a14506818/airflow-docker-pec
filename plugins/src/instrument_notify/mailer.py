"""
儀器管理到期通知 — 信件內容（HTML）與寄送

版型相容 Outlook：table 排版、inline style、不用外部 CSS 與圖片。
所有資料都經過 html.escape；只有 config 裡寫死的提醒文字（kind.tip）允許 HTML。
"""
from html import escape

from src.instrument_notify.config import URGENT_DAYS
from src.instrument_notify.rules import fmt_date, is_urgent

FONT = "'Microsoft JhengHei','PingFang TC',Arial,sans-serif"
GREEN = "#14432f"
RED = "#b91c1c"
ORANGE = "#c2410c"

TAG_STYLE = {
    "GMP": ("GMP", "#dbeafe", "#1e40af"),
    "PROCESSING": ("處理中", "#e5e7eb", "#374151"),
    "NEW": ("新", "#dcfce7", "#166534"),
}
TAG_FIELD = {"GMP": "is_GMP", "PROCESSING": "is_Processing", "NEW": "is_New"}
TAG_LEGEND = {
    "NEW": "<b>新</b>＝本次新進清單",
    "PROCESSING": "<b>處理中</b>＝已有進行中的校驗紀錄",
    "GMP": "<b>GMP</b>＝GMP 儀器",
}


def _e(value):
    return escape("" if value is None else str(value))


def tag_html(tag):
    label, bg, fg = TAG_STYLE[tag]
    return (f'<span style="background:{bg};color:{fg};border-radius:3px;padding:1px 6px;'
            f'font-size:11px;">{label}</span>')


def _name_cell(item, kind):
    tags = "".join(" " + tag_html(t) for t in kind.tags if item.get(TAG_FIELD[t]))
    return _e(item["Instrument_Name"]) + tags


def _remaining_style(item):
    if item["Remaining"] < 0:
        return f' style="color:{RED};font-weight:bold;"'
    if is_urgent(item):
        return f' style="color:{ORANGE};font-weight:bold;"'
    return ""


def _remaining_words(item):
    r = item["Remaining"]
    return f"逾期 {-r} 天" if r < 0 else f"{r} 天"


# 主旨 ---------------------------------------------------------------------------------------------
def counts(items):
    overdue = sum(1 for i in items if i["Remaining"] < 0)
    soon = sum(1 for i in items if 0 <= i["Remaining"] <= URGENT_DAYS)
    return overdue, soon, len(items) - overdue - soon


def build_subject(kind, items):
    overdue, soon, rest = counts(items)
    u = kind.unit
    if kind.split_urgent:
        head = "、".join(p for p in (
            f"{overdue} {u}已逾期" if overdue else "",
            f"{soon} {u} {URGENT_DAYS} 天內到期" if soon else "",
        ) if p)
        if head and rest:
            text = f"{head}，另有 {rest} {u}待處理"
        else:
            text = head or f"{rest} {u}待處理"
    else:
        text = f"本月有 {len(items)} {u}維護保養合約即將到期"
        if overdue:
            text += f"（其中 {overdue} {u}已過期）"
    return kind.subject_tag + text


# 內文 ---------------------------------------------------------------------------------------------
def _stat_card(number, label, bg, border, fg, label_fg):
    return (f'<td style="background:{bg};border:1px solid {border};border-radius:6px;padding:10px 18px;'
            f'text-align:center;"><div style="font-size:22px;font-weight:bold;color:{fg};">{number}</div>'
            f'<div style="font-size:12px;color:{label_fg};">{label}</div></td>')


def _stats_html(kind, items):
    overdue, soon, rest = counts(items)
    gray = ("#f3f4f6", "#e5e7eb", "#374151", "#4b5563")
    if kind.split_urgent:
        cards = [
            _stat_card(overdue, "已逾期", "#fef2f2", "#fecaca", RED, "#7f1d1d"),
            _stat_card(soon, f"{URGENT_DAYS} 天內到期", "#fff7ed", "#fed7aa", ORANGE, "#7c2d12"),
            _stat_card(rest, "其他待處理", *gray),
        ]
    else:
        cards = [
            _stat_card(overdue, "已過期", "#fef2f2", "#fecaca", RED, "#7f1d1d"),
            _stat_card(soon + rest, "即將到期", *gray),
        ]
    return ('<table cellpadding="0" cellspacing="0"><tr>'
            + '<td width="10"></td>'.join(cards) + "</tr></table>")


def _urgent_table(kind, items):
    urgent = [i for i in items if is_urgent(i)]
    if not urgent:
        return ""
    rows = "".join(
        f'<tr style="border-bottom:1px solid #f1f1f1;"><td>{_name_cell(i, kind)}</td>'
        f'<td>{_e(i["Asset_No"])}</td><td>{_e(i["Due_Date"])}</td>'
        f'<td align="right"{_remaining_style(i)}>{_remaining_words(i)}</td></tr>'
        for i in urgent
    )
    return f"""
  <tr><td style="padding:8px 28px 4px;font-size:15px;font-weight:bold;color:{RED};">需立即處理</td></tr>
  <tr><td style="padding:0 28px 16px;">
    <table width="100%" cellpadding="8" cellspacing="0" style="border-collapse:collapse;font-size:13px;">
      <tr style="background:#fef2f2;color:#7f1d1d;"><th align="left">儀器名稱</th><th align="left">固資編號</th><th align="left">{_e(kind.date_label)}</th><th align="right">剩餘天數</th></tr>
      {rows}
    </table>
  </td></tr>"""


def _full_table(kind, items):
    heads = (["儀器名稱", "固資編號", kind.date_label, "剩餘"]
             + [label for _, label in kind.columns] + ["你的角色"])
    th = "".join(
        f'<th align="{"right" if h == "剩餘" else "left"}">{_e(h)}</th>' for h in heads
    )
    rows = []
    for n, i in enumerate(items):
        bg = "background:#fafbfa;" if n % 2 else ""
        extra = "".join(f"<td>{_e(i.get(col) or '—')}</td>" for col, _ in kind.columns)
        rows.append(
            f'<tr style="border-bottom:1px solid #eef0ef;{bg}"><td>{_name_cell(i, kind)}</td>'
            f'<td>{_e(i["Asset_No"])}</td><td>{_e(i["Due_Date"])}</td>'
            f'<td align="right"{_remaining_style(i)}>{i["Remaining"]}</td>{extra}'
            f'<td>{_e(i["Role"])}</td></tr>'
        )
    legend = "、".join(TAG_LEGEND[t] for t in ("NEW", "PROCESSING", "GMP") if t in kind.tags)
    order = "逾期" if kind.split_urgent else "已過期"
    title = "完整清單" if kind.split_urgent else "合約清單"
    return f"""
  <tr><td style="padding:8px 28px 4px;font-size:15px;font-weight:bold;color:{GREEN};">{title}（{len(items)} {kind.unit}）</td></tr>
  <tr><td style="padding:0 28px 16px;">
    <table width="100%" cellpadding="7" cellspacing="0" style="border-collapse:collapse;font-size:12px;">
      <tr style="background:{GREEN};color:#ffffff;">{th}</tr>
      {"".join(rows)}
    </table>
    <div style="font-size:11px;color:#6b7280;margin-top:6px;">排序：{order} → 剩餘天數由少到多。標記：{legend}</div>
  </td></tr>"""


def _tip_html(kind):
    if not kind.tip:
        return ""
    return f"""
  <tr><td style="padding:8px 28px 12px;">
    <table width="100%" cellpadding="10" cellspacing="0" style="background:#ecfdf5;border:1px solid #a7f3d0;border-radius:6px;font-size:13px;color:#065f46;">
      <tr><td>{kind.tip}</td></tr>
    </table>
  </td></tr>"""


def _not_notified_html(kind):
    lis = "".join(f"<li>{_e(t)}</li>" for t in kind.not_notified)
    return f"""
  <tr><td style="padding:4px 28px 20px;">
    <table width="100%" cellpadding="12" cellspacing="0" style="background:#fffbeb;border:1px solid #fde68a;border-radius:6px;font-size:12px;color:#78350f;">
      <tr><td><b>以下情況不會寄出通知，請自行留意：</b>
        <ul style="margin:6px 0 0 18px;padding:0;">{lis}</ul></td></tr>
    </table>
  </td></tr>"""


def _processing_html(kind):
    if "PROCESSING" not in kind.tags:
        return ""
    return f"""
  <tr><td style="padding:0 28px 20px;">
    <table width="100%" cellpadding="12" cellspacing="0" style="background:#fef2f2;border:1px solid #fecaca;border-left:4px solid {RED};border-radius:6px;font-size:13px;color:#7f1d1d;">
      <tr><td><b>⚠️ 已開始處理校驗？</b><br>
        即使校驗尚未完成，也請先到 BPM 新增校驗紀錄，結果選「處理中」。下次通知會在該儀器標上 {tag_html("PROCESSING")}，方便你聚焦還沒處理的儀器。
      </td></tr>
    </table>
  </td></tr>"""


def build_html(kind, mailbox, today):
    """一個收件人一封：mailbox = {"Account", "DisplayName", "items"}"""
    items = mailbox["items"]
    who = f'{_e(mailbox["DisplayName"] or mailbox["Account"])}（{_e(mailbox["Account"])}）'
    return f"""<table width="100%" cellpadding="0" cellspacing="0" style="background:#f3f5f4;padding:24px 0;font-family:{FONT};">
<tr><td align="center">
<table width="720" cellpadding="0" cellspacing="0" style="background:#ffffff;border:1px solid #dfe5e2;border-radius:8px;">
  <tr><td style="background:{GREEN};padding:20px 28px;border-radius:8px 8px 0 0;">
    <div style="color:#ffffff;font-size:20px;font-weight:bold;">{_e(kind.heading)}</div>
    <div style="color:#b7d3c5;font-size:13px;margin-top:4px;">{fmt_date(today)} · {_e(kind.schedule_label)}</div>
  </td></tr>
  <tr><td style="padding:20px 28px 8px;font-size:14px;color:#1f2937;">
    <b>本通知對象：{who}</b><br>
    {_e(kind.intro)}
  </td></tr>
  <tr><td style="padding:8px 28px {16 if kind.split_urgent else 8}px;">{_stats_html(kind, items)}</td></tr>
  {_tip_html(kind)}
  {_urgent_table(kind, items) if kind.split_urgent else ""}
  {_full_table(kind, items)}
  {_not_notified_html(kind)}
  {_processing_html(kind)}
  <tr><td style="padding:14px 28px;border-top:1px solid #eef0ef;font-size:11px;color:#9ca3af;">
    {_e(kind.footer)}
  </td></tr>
</table>
</td></tr>
</table>"""


# 寄送 ---------------------------------------------------------------------------------------------
def send_all(kind, mailboxes, today, sender, dry_run=False):
    """
    每個帳號寄一封；帳號有效但沒有 email 只記 log
    單封失敗不中斷，回傳寄送紀錄 [{"Account", "DisplayName", "Email", "Count", "Subject", "Status"}]
    """
    log = []
    for box in mailboxes:
        subject = build_subject(kind, box["items"])
        row = {"Account": box["Account"], "DisplayName": box["DisplayName"], "Email": box["Email"],
               "Count": len(box["items"]), "Subject": subject}
        if not box["Email"]:
            print(f"⚠️ {box['Account']} 沒有 email，略過")
            continue
        if dry_run:
            row["Status"] = "DRY_RUN（未寄出）"
        else:
            try:
                sender(to=[box["Email"]], subject=subject, html_content=build_html(kind, box, today))
                row["Status"] = "已寄出"
                print(f"✅ 已寄給 {box['Account']} <{box['Email']}>：{subject}")
            except Exception as e:  # 單封失敗繼續寄其他人，最後由 task 統一判定失敗
                row["Status"] = f"失敗：{e}"
                print(f"❌ 寄給 {box['Account']} <{box['Email']}> 失敗：{e}")
        log.append(row)
    return log
