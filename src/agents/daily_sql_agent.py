from __future__ import annotations

from typing import Any, Dict, List, Tuple
import logging

from .utils import normalize_row

logger = logging.getLogger(__name__)


def _tag(r: Dict[str, Any]) -> str:
    # ожидаем, что поля уже нормализованы
    drr = float(r.get("drr_percent") or 0)
    rev = float(r.get("revenue_total") or 0)
    orders = int(r.get("ad_orders") or 0)
    if drr >= 60 and rev < 5000:
        return "PAUSE"
    if drr >= 40 and rev < 10000:
        return "CUT"
    if drr <= 15 and orders >= 5:
        return "SCALE"
    return "WATCH"


def run_daily_sql_report(supabase, *, limit: int = 20) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    # читаем view как таблицу
    resp = (
        supabase.table("v_daily_report_yesterday")
        .select("*")
        .order("ad_spend", desc=True)
        .limit(limit)
        .execute()
    )
    rows = resp.data or []

    # нормализуем данные и вычисляем теги
    normalized_rows: List[Dict[str, Any]] = []
    for r in rows:
        nr = normalize_row(r)
        nr["tag"] = _tag(nr)
        normalized_rows.append(nr)

    logger.info("daily_report_yesterday rows=%d (top %d by ad_spend)", len(normalized_rows), limit)
    if not normalized_rows:
        empty_totals = {
            "top_n": 0,
            "total_revenue": 0.0,
            "total_spend": 0.0,
            "drr_percent": 0.0,
            "total_clicks": 0,
            "total_orders": 0,
        }
        return empty_totals, normalized_rows

    # короткий summary по топу
    total_revenue = sum((r.get("revenue_total") or 0) for r in normalized_rows)
    total_spend = sum((r.get("ad_spend") or 0) for r in normalized_rows)
    drr = (total_spend / total_revenue * 100) if total_revenue else 0
    logger.info("TOP%d revenue_total=%.2f ad_spend=%.2f drr%%=%.2f", len(normalized_rows), total_revenue, total_spend, drr)
    logger.info("--- top items ---")

    for r in normalized_rows[:10]:
        logger.info(
            "nmid=%s rev=%.2f spend=%.2f drr%%=%.2f clicks=%d orders=%d title=%s",
            r.get("nmid"),
            r.get("revenue_total"),
            r.get("ad_spend"),
            r.get("drr_percent"),
            r.get("ad_clicks"),
            r.get("ad_orders"),
            str(r.get("title") or "")[:40],
        )

    totals = {
        "top_n": len(normalized_rows),
        "total_revenue": float(total_revenue),
        "total_spend": float(total_spend),
        "drr_percent": float(drr),
        "total_clicks": int(sum((r.get("ad_clicks") or 0) for r in normalized_rows)),
        "total_orders": int(sum((r.get("ad_orders") or 0) for r in normalized_rows)),
    }
    return totals, normalized_rows
    

def rows_to_brief(rows, top_n: int = 10) -> str:
    lines = []
    for r in rows[:top_n]:
        tag = r.get("tag") or "WATCH"
        nmid = r.get("nmid")
        rev = float(r.get("revenue_total") or 0)
        spend = float(r.get("ad_spend") or 0)
        drr = float(r.get("drr_percent") or 0)
        clicks = int(r.get("ad_clicks") or 0)
        orders = int(r.get("ad_orders") or 0)
        title = str(r.get("title") or "")[:60]
        lines.append(
            f"tag={tag} nmid={nmid} rev={rev} spend={spend} drr%={drr:.2f} clicks={clicks} orders={orders} title={title}"
        )
    return "\n".join(lines)
