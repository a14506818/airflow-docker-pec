"""寄送日推算與【新】標記：逐日窮舉 2026–2028（含閏年 2028/02/29）"""
from datetime import date, timedelta
from itertools import product

import pytest

from src.instrument_notify.config import CALIBRATION, CONTRACT
from src.instrument_notify.rules import parse_date, previous_send_date, select_due
from notify_factories import inst, iso

ALL_DAYS = [date(2026, 1, 1) + timedelta(days=n) for n in range((date(2029, 1, 1) - date(2026, 1, 1)).days)]


@pytest.mark.parametrize("send_days", [CALIBRATION.send_days, CONTRACT.send_days])
def test_previous_send_date_every_day(send_days):
    for today in ALL_DAYS:
        prev = previous_send_date(today, send_days)
        assert prev < today, today
        assert prev.day in send_days, today
        # prev 與 today 之間（不含兩端）不應該還有寄送日
        d = prev + timedelta(days=1)
        while d < today:
            assert d.day not in send_days, (today, d)
            d += timedelta(days=1)
        assert (today - prev).days <= 31


@pytest.mark.parametrize("today,send_days,expected", [
    (date(2026, 12, 1), (1, 15), date(2026, 11, 15)),
    (date(2026, 12, 2), (1, 15), date(2026, 12, 1)),
    (date(2026, 12, 15), (1, 15), date(2026, 12, 1)),
    (date(2026, 12, 16), (1, 15), date(2026, 12, 15)),
    (date(2027, 1, 1), (1, 15), date(2026, 12, 15)),
    (date(2028, 3, 1), (1, 15), date(2028, 2, 15)),
    (date(2028, 3, 1), (1,), date(2028, 2, 1)),
    (date(2027, 1, 1), (1,), date(2026, 12, 1)),
    (date(2026, 12, 31), (1,), date(2026, 12, 1)),
])
def test_previous_send_date_examples(today, send_days, expected):
    assert previous_send_date(today, send_days) == expected


def send_dates(start, end, send_days):
    d = start
    while d <= end:
        if d.day in send_days:
            yield d
        d += timedelta(days=1)


@pytest.mark.parametrize("kind,days_before", list(product([CALIBRATION, CONTRACT], [0, 7, 14, 30, 45, 60, 90])))
def test_new_flag_appears_exactly_once_per_due_date(kind, days_before):
    """
    固定到期日、模擬每個寄送日都跑一次：
    儀器第一次進清單那次標【新】，之後（含逾期）都不再標；進清單後就一直在清單內
    """
    for due in [date(2027, 3, 1) + timedelta(days=n) for n in range(0, 62, 3)]:
        seen_in_list = False
        new_count = 0
        for today in send_dates(date(2026, 9, 1), due + timedelta(days=60), kind.send_days):
            prev = previous_send_date(today, kind.send_days)
            items, _ = select_due([inst(kind=kind, due=iso(due), days=days_before)], kind, today, prev)
            if items:
                assert items[0]["is_New"] is (not seen_in_list), (due, today)
                new_count += items[0]["is_New"]
                seen_in_list = True
            else:
                assert not seen_in_list, f"進清單後又消失：due={due} today={today}"
        assert new_count == 1, (due, days_before)


@pytest.mark.parametrize("remaining_at_prev,remaining_today,days_before,is_new", [
    (31, 30, 30, True),     # 上次差一天沒進
    (30, 29, 30, False),    # 上次剛好在邊界上已進
    (45, 15, 30, True),
    (14, -1, 30, False),    # 上次就在清單，今天變逾期
    (-5, -20, 30, False),
    (1, 0, 0, True),        # 提前 0 天：到期當天才進
    (0, -15, 0, False),
])
def test_new_flag_examples(remaining_at_prev, remaining_today, days_before, is_new):
    today = date(2026, 12, 1)
    prev = today - timedelta(days=remaining_at_prev - remaining_today)
    due = today + timedelta(days=remaining_today)
    item = select_due([inst(due=iso(due), days=days_before)], CALIBRATION, today, prev)[0][0]
    assert item["is_New"] is is_new


@pytest.mark.parametrize("value,expected", [
    ("2028/02/29", date(2028, 2, 29)),
    ("2028-02-29 00:00:00", date(2028, 2, 29)),
    ("2027/02/29", None),
    ("2026/1/5", date(2026, 1, 5)),
    ("2026-01-05T08:00:00", date(2026, 1, 5)),
    (" 2026/01/05 ", date(2026, 1, 5)),  # 前後空白先去掉再解析
    ("20260105", None),
    (20260105, None),
    ("0000/00/00", None),
    ("1900/01/01", date(1900, 1, 1)),
])
def test_parse_date_edge_cases(value, expected):
    assert parse_date(value) == expected
