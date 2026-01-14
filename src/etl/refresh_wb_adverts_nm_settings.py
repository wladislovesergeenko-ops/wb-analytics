from __future__ import annotations

from datetime import datetime, date
from typing import Any, Iterable

import httpx
import pandas as pd


ADVERTS_URL = "https://advert-api.wildberries.ru/api/advert/v2/adverts"


def _to_date_str(x) -> str | None:
    """WB timestamps -> 'YYYY-MM-DD' (строка, чтобы supabase insert не падал)."""
    if not x:
        return None
    try:
        # WB отдает ISO с +03:00, pandas/py нормально парсит
        dt = pd.to_datetime(x, errors="coerce", utc=False)
        if pd.isna(dt):
            return None
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


def fetch_adverts(api_key: str) -> dict:
    headers = {
        "Authorization": api_key,
        "Accept": "application/json",
    }
    with httpx.Client(timeout=60.0) as client:
        r = client.get(ADVERTS_URL, headers=headers)
        r.raise_for_status()
        return r.json()


def adverts_to_df(payload: dict, statuses: set[int]) -> pd.DataFrame:
    """
    payload: ответ /api/advert/v2/adverts
    Разворачиваем nm_settings в плоскую таблицу.
    """
    adverts = payload.get("adverts", []) or []
    if not adverts:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []

    for adv in adverts:
        status = adv.get("status")
        if status not in statuses:
            continue

        advert_id = adv.get("id")
        bid_type = adv.get("bid_type")

        settings = adv.get("settings") or {}
        campaign_name = settings.get("name")
        payment_type = settings.get("payment_type")

        placements = settings.get("placements") or {}
        place_search = bool(placements.get("search"))
        place_recommendations = bool(placements.get("recommendations"))

        ts = adv.get("timestamps") or {}
        ts_created = _to_date_str(ts.get("created"))
        ts_started = _to_date_str(ts.get("started"))
        ts_updated = _to_date_str(ts.get("updated"))
        ts_deleted = _to_date_str(ts.get("deleted"))

        nm_settings = adv.get("nm_settings") or []
        for s in nm_settings:
            nmid = s.get("nm_id")

            bids = (s.get("bids_kopecks") or {})
            bid_search_kopecks = bids.get("search")
            bid_recommendations_kopecks = bids.get("recommendations")

            subj = (s.get("subject") or {})
            subject_id = subj.get("id")
            subject_name = subj.get("name")

            rows.append({
                "advert_id": advert_id,
                "nmid": nmid,
                "status": status,
                "bid_type": bid_type,
                "payment_type": payment_type,
                "campaign_name": campaign_name,
                "place_search": place_search,
                "place_recommendations": place_recommendations,
                "bid_search_kopecks": bid_search_kopecks,
                "bid_recommendations_kopecks": bid_recommendations_kopecks,
                "subject_id": subject_id,
                "subject_name": subject_name,
                "ts_created": ts_created,
                "ts_started": ts_started,
                "ts_updated": ts_updated,
                "ts_deleted": ts_deleted,
            })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # NaN -> None (supabase)
    df = df.where(pd.notnull(df), None)
    return df


def refresh_wb_adverts_nm_settings(
    wb_key: str,
    supabase,
    table_name: str = "wb_adverts_nm_settings",
    statuses: Iterable[int] = (7, 9, 11),
    batch_size: int = 500,
) -> int:
    """
    Полная перезаливка таблицы: delete -> insert.
    """
    statuses_set = set(int(x) for x in statuses)

    payload = fetch_adverts(wb_key)
    df = adverts_to_df(payload, statuses_set)

    # 1) чистим таблицу
    # Удаляем только те статусы, которые заливаем (чтобы таблица была "актуальные кампании")
    # Если хочешь чистить вообще всё — можно заменить на delete().neq("advert_id", 0)
    supabase.table(table_name).delete().in_("status", list(statuses_set)).execute()

    # 2) вставляем батчами
    records = df.to_dict(orient="records")
    for i in range(0, len(records), batch_size):
        supabase.table(table_name).insert(records[i:i + batch_size]).execute()

    print(f"refreshed {table_name}: rows={len(records)} statuses={sorted(statuses_set)}")
    return len(records)