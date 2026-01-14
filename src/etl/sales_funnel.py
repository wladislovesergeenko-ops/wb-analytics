#для загрузки за несколько дней
from __future__ import annotations

import time
from datetime import date, timedelta
import pandas as pd
import httpx
#
# --- 1) API fetch (твоя рабочая функция) ---
def fetch_sales_funnel_products(api_key: str, start: str, end: str):
    url = "https://seller-analytics-api.wildberries.ru/api/analytics/v3/sales-funnel/products"
    headers = {"Authorization": api_key, "Content-Type": "application/json", "Accept": "application/json"}
    payload = {"selectedPeriod": {"start": start, "end": end}}

    with httpx.Client(timeout=30.0) as client:
        r = client.post(url, headers=headers, json=payload)
        r.raise_for_status()
        return r.json()

# --- 2) JSON -> df_out (как мы обсуждали) ---
def build_df_out(res: dict) -> pd.DataFrame:
    rows = res["data"]["products"]
    df = pd.json_normalize(rows, sep="__")

    keep = [
        # product
        "product__nmId",
        "product__title",
        "product__vendorCode",
        "product__brandName",
        "product__subjectId",
        "product__subjectName",
        "product__feedbackRating",
        "product__stocks__wb",
    
        # statistic.selected
        "statistic__selected__openCount",
        "statistic__selected__cartCount",
        "statistic__selected__orderCount",
        "statistic__selected__orderSum",
        "statistic__selected__buyoutCount",
        "statistic__selected__buyoutSum",
        "statistic__selected__cancelCount",
        "statistic__selected__cancelSum",
        "statistic__selected__avgPrice",
        "statistic__selected__localizationPercent",
    
        # period
        "statistic__selected__period__start",
        "statistic__selected__period__end",
    
        # timeToReady
        "statistic__selected__timeToReady__days",
        "statistic__selected__timeToReady__hours",
    ]

    df_out = df[[c for c in keep if c in df.columns]].copy()

    df_out = df_out.rename(columns={
        "product__nmId": "nmId",
        "product__title": "title",
        "product__vendorCode": "vendorCode",
        "product__brandName": "brandName",
        "product__subjectId": "subjectId",
        "product__subjectName": "subjectName",
        "product__feedbackRating": "feedbackRating",
        "product__stocks__wb": "stocks",
    
        "statistic__selected__openCount": "openCount",
        "statistic__selected__cartCount": "cartCount",
        "statistic__selected__orderCount": "orderCount",
        "statistic__selected__orderSum": "orderSum",
        "statistic__selected__buyoutCount": "buyoutCount",
        "statistic__selected__buyoutSum": "buyoutSum",
        "statistic__selected__cancelCount": "cancelCount",
        "statistic__selected__cancelSum": "cancelSum",
        "statistic__selected__avgPrice": "avgPrice",
        "statistic__selected__localizationPercent": "localizationPercent",
    
        "statistic__selected__period__start": "periodStart",
        "statistic__selected__period__end": "periodEnd",
    
        "statistic__selected__timeToReady__days": "timeToReady_days",
        "statistic__selected__timeToReady__hours": "timeToReady_hours",
    })


    # NaN -> None (важно для Supabase insert/upsert)
    df_out = df_out.where(pd.notnull(df_out), None)

    # ✅ сначала приведи имена к lower
    df_out.columns = df_out.columns.str.lower()
    # reportDate если период = 1 день
  
    if "periodstart" in df_out.columns and "periodend" in df_out.columns:
        df_out["reportdate"] = df_out["periodstart"].where(df_out["periodstart"] == df_out["periodend"])

    return df_out

# --- 3) загрузка в Supabase (upsert батчами) ---
def supabase_upsert_df(supabase, table_name: str, df: pd.DataFrame, batch: int = 500):
    records = df.to_dict(orient="records")
    for i in range(0, len(records), batch):
        supabase.table(table_name) \
            .upsert(records[i:i+batch], on_conflict="nmid,periodstart,periodend") \
            .execute()

# --- 4) главная функция: выгрузить по дням и залить ---
def load_sales_funnel_by_days(
    wb_key: str,
    supabase,
    table_name: str,
    date_from: str,   # "2026-01-02"
    date_to: str,     # "2026-01-12"
    *,
    sleep_seconds: int = 21,
    verbose: bool = True,
):
    start_d = date.fromisoformat(date_from)
    end_d = date.fromisoformat(date_to)

    d = start_d
    while d <= end_d:
        day = d.isoformat()
        try:
            if verbose:
                print(f"\n=== {day} ===")

            res = fetch_sales_funnel_products(wb_key, day, day)
            df_out = build_df_out(res)

            if verbose:
                print("rows:", len(df_out))

            if len(df_out) > 0:
                supabase_upsert_df(supabase, table_name, df_out)
                if verbose:
                    print("upsert ok")
            else:
                if verbose:
                    print("no data")

        except Exception as e:
            print("ERROR for", day, "->", repr(e))

        # лимит: 1 запрос / ~20 сек
        if d < end_d:
            time.sleep(sleep_seconds)

        d += timedelta(days=1)

# --- пример запуска ---
# load_sales_funnel_by_days(
#     wb_key=wb_key,
#     supabase=supabase,
#     table_name="wb_sales_funnel_products",
#     date_from="2026-01-02",
#     date_to="2026-01-12",
#     sleep_seconds=21,
# )
