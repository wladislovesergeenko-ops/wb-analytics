import pytest

from src.agents.daily_sql_agent import _tag, rows_to_brief


@pytest.mark.parametrize(
    "row,expected",
    [
        ({"drr_percent": 70, "revenue_total": 1000, "ad_orders": 1}, "PAUSE"),
        ({"drr_percent": 50, "revenue_total": 5000, "ad_orders": 1}, "CUT"),
        ({"drr_percent": 10, "revenue_total": 2000, "ad_orders": 5}, "SCALE"),
        ({"drr_percent": 20, "revenue_total": 2000, "ad_orders": 1}, "WATCH"),
    ],
)
def test_tag(row, expected):
    assert _tag(row) == expected


def test_rows_to_brief():
    rows = [
        {
            "nmid": 123,
            "revenue_total": 1000.0,
            "ad_spend": 100.0,
            "drr_percent": 10.0,
            "ad_clicks": 5,
            "ad_orders": 2,
            "title": "Test title",
            "tag": "SCALE",
        }
    ]
    brief = rows_to_brief(rows, top_n=1)
    assert "tag=SCALE nmid=123" in brief
