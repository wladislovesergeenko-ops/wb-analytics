from typing import Any, Dict
import logging

logger = logging.getLogger(__name__)


def float_or_zero(v: Any) -> float:
    try:
        if v is None:
            return 0.0
        return float(v)
    except Exception:
        return 0.0


def int_or_zero(v: Any) -> int:
    try:
        if v is None:
            return 0
        return int(v)
    except Exception:
        return 0


def normalize_row(r: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "nmid": r.get("nmid"),
        "revenue_total": float_or_zero(r.get("revenue_total")),
        "ad_spend": float_or_zero(r.get("ad_spend")),
        "drr_percent": float_or_zero(r.get("drr_percent")),
        "ad_clicks": int_or_zero(r.get("ad_clicks")),
        "ad_orders": int_or_zero(r.get("ad_orders")),
        "title": (r.get("title") or "")[:200],
    }
