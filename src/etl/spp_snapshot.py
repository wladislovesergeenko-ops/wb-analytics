import datetime as dt
import time
import pandas as pd
import httpx



def load_spp_snapshot_to_supabase(
    wb_key: str,
    supabase,
    date_from: str,
    date_to: str,
    *,
    table_name: str = "wb_spp_daily",
    flag: int = 1,
    only_not_canceled: bool = True,
    sleep_seconds: float = 0.0,
    batch_size: int = 1000,
    verbose: bool = True,
):
    """
    По дням дергает /api/v1/supplier/orders (flag=1),
    делает snapshot: 1 строка на nmid (берём first spp и first finishedPrice),
    upsert в Supabase по (date, nmid).
    """

    def _jsonable(v):
        if v is None:
            return None
        if hasattr(v, "item"):
            try:
                return v.item()
            except Exception:
                return str(v)
        return v

    total_rows = 0

    for d in pd.date_range(date_from, date_to, freq="D"):
        day = d.strftime("%Y-%m-%d")
        if verbose:
            print("\nDAY:", day)

        url = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
        headers = {"Authorization": wb_key, "Accept": "application/json"}
        params = {"dateFrom": day, "flag": flag}

        with httpx.Client(timeout=60.0) as client:
            r = client.get(url, headers=headers, params=params)
            if verbose:
                print("status:", r.status_code, "url:", str(r.request.url))
            r.raise_for_status()
            data = r.json() or []

        if not data:
            if verbose:
                print("no rows")
            continue

        df = pd.DataFrame(data)

        if only_not_canceled and "isCancel" in df.columns:
            df = df[df["isCancel"] == False]

        # важно: проверим наличие нужных колонок
        need_cols = ["nmId", "spp", "finishedPrice"]
        missing = [c for c in need_cols if c not in df.columns]
        if missing:
            raise KeyError(f"missing columns in orders response: {missing}")

        df = df[need_cols].copy()
        df["date"] = day

        snap = (
            df.dropna(subset=["nmId"])
              .groupby("nmId", as_index=False)
              .agg(
                  spp=("spp", "first"),
                  finished_price=("finishedPrice", "first"),
                  date=("date", "first"),
              )
              .rename(columns={"nmId": "nmid"})
        )

        snap = snap.where(pd.notnull(snap), None)
        records = [{k: _jsonable(v) for k, v in row.items()} for row in snap.to_dict("records")]

        for i in range(0, len(records), batch_size):
            supabase.table(table_name) \
                .upsert(records[i:i + batch_size], on_conflict="date,nmid") \
                .execute()

        if verbose:
            print("upserted:", len(records))
        total_rows += len(records)

        if sleep_seconds:
            time.sleep(sleep_seconds)

    if verbose:
        print("\nDONE. total upserted rows:", total_rows)
    return total_rows


def load_spp_snapshot_yesterday(
    wb_key: str,
    supabase,
    *,
    overlap_days: int = 2,
    table_name: str = "wb_spp_daily",
    flag: int = 1,
    only_not_canceled: bool = True,
    sleep_seconds: float = 0.0,
    batch_size: int = 1000,
    verbose: bool = True,
):
    """
    Daily-режим: грузим вчера + overlap_days (перезатираем хвост, если WB досчитал)
    """
    end_d = (dt.date.today() - dt.timedelta(days=1))
    start_d = end_d - dt.timedelta(days=max(overlap_days - 1, 0))

    return load_spp_snapshot_to_supabase(
        wb_key=wb_key,
        supabase=supabase,
        date_from=start_d.strftime("%Y-%m-%d"),
        date_to=end_d.strftime("%Y-%m-%d"),
        table_name=table_name,
        flag=flag,
        only_not_canceled=only_not_canceled,
        sleep_seconds=sleep_seconds,
        batch_size=batch_size,
        verbose=verbose,
    )

