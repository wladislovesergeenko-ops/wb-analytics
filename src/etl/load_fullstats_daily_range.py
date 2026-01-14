from __future__ import annotations

import time
from typing import Any, Iterable, List

import httpx
import pandas as pd


FULLSTATS_URL = "https://advert-api.wildberries.ru/adv/v3/fullstats"


def _chunked(seq: List[int], size: int) -> List[List[int]]:
    return [seq[i : i + size] for i in range(0, len(seq), size)]


def get_active_advert_ids_from_supabase(
    supabase,
    table_name: str = "wb_adverts_nm_settings",
    statuses: Iterable[int] = (9, 11),
    limit: int = 10000,
) -> List[int]:
    """
    Берём уникальные advert_id из таблицы кампаний (актуальные статусы 7/9/11).
    """
    statuses_list = [int(s) for s in statuses]

    resp = (
        supabase.table(table_name)
        .select("advert_id")
        .in_("status", statuses_list)
        .limit(limit)
        .execute()
    )
    data = resp.data or []

    ids = sorted({int(r["advert_id"]) for r in data if r.get("advert_id") is not None})
    return ids


def fetch_adv_fullstats_chunked(
    api_key: str,
    advert_ids: List[int],
    begin_date: str,  # "YYYY-MM-DD"
    end_date: str,    # "YYYY-MM-DD"
    *,
    chunk_size: int = 50,
    sleep_seconds: int = 15,
    timeout_seconds: float = 60.0,
    verbose: bool = True,
) -> List[dict]:
    """
    Дёргаем /adv/v3/fullstats чанками по 50 ids.
    Возвращаем список ответов (каждый ответ — list[dict] по кампаниям).
    """
    headers = {
        "Authorization": api_key,
        "Accept": "application/json",
    }

    results: List[dict] = []

    if not advert_ids:
        return results

    chunks = _chunked(advert_ids, chunk_size)

    with httpx.Client(timeout=timeout_seconds) as client:
        for idx, chunk in enumerate(chunks, start=1):
            ids_param = ",".join(str(x) for x in chunk)
            params = {"ids": ids_param, "beginDate": begin_date, "endDate": end_date}

            if verbose:
                print(f"fullstats chunk {idx}/{len(chunks)} ids={len(chunk)} range={begin_date}->{end_date}")

            r = client.get(FULLSTATS_URL, headers=headers, params=params)

            # иногда WB может вернуть 200, но пустой body → это реально бывает при проблемах на стороне API/ids
            if r.status_code != 200:
                # покажем небольшой хвост текста, чтобы понять причину
                preview = (r.text or "")[:300]
                raise RuntimeError(f"fullstats HTTP {r.status_code}: {preview}")

            # проверка content-type не обязательна, но полезна для дебага
            # ct = r.headers.get("Content-Type", "")
            # if "json" not in ct.lower(): ...

            try:
                payload = r.json()
            except Exception:
                preview = (r.text or "")[:300]
                raise RuntimeError(f"fullstats: response is not JSON. text preview: {preview}")

            # endpoint возвращает list на верхнем уровне
            if payload:
                results.extend(payload)

            if idx < len(chunks):
                time.sleep(sleep_seconds)

    return results


def fullstats_to_daily_df(payload: List[dict]) -> pd.DataFrame:
    """
    Нормализация ответа /adv/v3/fullstats -> дневные строки.

    Важно:
    - boosterStats НЕ трогаем.
    - дату берём из days[*].date.
    - остальное из days[*] расплющиваем как есть (scalar поля → колонки).
    """
    if not payload:
        return pd.DataFrame(columns=["advert_id", "date"])

    rows: List[dict[str, Any]] = []

    for adv in payload:
        advert_id = adv.get("advertId") or adv.get("advert_id") or adv.get("id")
        if advert_id is None:
            continue

        days = adv.get("days") or []
        for d in days:
            # date must come from days.date
            day_date = d.get("date")
            # сделаем плоскую запись: advert_id + всё из day (кроме boosterStats)
            day_copy = dict(d)
            day_copy.pop("boosterStats", None)  # полностью игнорируем
            row = {"advert_id": int(advert_id), **day_copy}

            # нормализуем date в строку 'YYYY-MM-DD'
            dt_parsed = pd.to_datetime(day_date, errors="coerce")
            row["date"] = None if pd.isna(dt_parsed) else dt_parsed.strftime("%Y-%m-%d")

            rows.append(row)

    if not rows:
        return pd.DataFrame(columns=["advert_id", "date"])

    df = pd.json_normalize(rows, sep="__")

    # гарантируем наличие ключевых колонок
    if "advert_id" not in df.columns:
        df["advert_id"] = None
    if "date" not in df.columns:
        df["date"] = None

    # типы
    df["advert_id"] = pd.to_numeric(df["advert_id"], errors="coerce")
    df["date"] = df["date"].astype("string")
    # 0) гарантируем нужные колонки таблицы (строго по схеме)
    schema_cols = [
        "advert_id", "date", "atbs", "views", "clicks", "orders", "canceled", "shks",
        "sum", "sum_price", "cpc", "ctr", "cr", "raw"
        # loaded_at лучше НЕ слать, пусть БД сама ставит default now()
    ]

    # 1) добавим отсутствующие колонки как None
    for c in schema_cols:
        if c not in df.columns:
            df[c] = None

    # 2) выберем и упорядочим строго по схеме
    df = df[schema_cols].copy()

    # 3) типы (опционально, но полезно)
    df["advert_id"] = pd.to_numeric(df["advert_id"], errors="coerce")
    df["atbs"] = pd.to_numeric(df["atbs"], errors="coerce")
    for c in ["views", "clicks", "orders", "canceled", "shks"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in ["sum", "sum_price", "cpc", "ctr", "cr"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # 4) date -> строка YYYY-MM-DD (и не datetime/date объект)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # 5) NaN -> None (для supabase)
    df = df.where(pd.notnull(df), None)

    # lower-case columns
    df.columns = df.columns.str.lower()

    return df


def supabase_upsert_df(
    supabase,
    table_name: str,
    df: pd.DataFrame,
    *,
    on_conflict: str,
    batch_size: int = 500,
) -> int:
    records = df.to_dict(orient="records")
    for i in range(0, len(records), batch_size):
        supabase.table(table_name).upsert(records[i : i + batch_size], on_conflict=on_conflict).execute()
    return len(records)


def load_fullstats_daily_range(
    wb_key: str,
    supabase,
    *,
    begin_date: str,  # "YYYY-MM-DD"
    end_date: str,    # "YYYY-MM-DD"
    adverts_table: str = "wb_adverts_nm_settings",
    target_table: str = "wb_adv_fullstats_daily",
    statuses: Iterable[int] = (9, 11),
    chunk_size: int = 50,
    sleep_seconds: int = 15,
    verbose: bool = True,
) -> int:
    """
    Полный шаг:
    1) берём advert_id из wb_adverts_nm_settings (statuses 7/9/11)
    2) дёргаем /adv/v3/fullstats чанками по 50 на диапазон дат
    3) нормализуем days -> df
    4) upsert в wb_adv_fullstats_daily по (advert_id, date)
    """
    advert_ids = get_active_advert_ids_from_supabase(
        supabase,
        table_name=adverts_table,
        statuses=statuses,
    )
    if verbose:
        print(f"active advert_ids: {len(advert_ids)} (statuses={list(statuses)})")

    if not advert_ids:
        if verbose:
            print("no active advert_ids -> skip fullstats")
        return 0

    payload = fetch_adv_fullstats_chunked(
        api_key=wb_key,
        advert_ids=advert_ids,
        begin_date=begin_date,
        end_date=end_date,
        chunk_size=chunk_size,
        sleep_seconds=sleep_seconds,
        verbose=verbose,
    )

    df = fullstats_to_daily_df(payload)
    if verbose:
        print("fullstats rows (daily):", len(df))

    if len(df) == 0:
        return 0

    # upsert by (advert_id, date)
    inserted = supabase_upsert_df(
        supabase,
        table_name=target_table,
        df=df,
        on_conflict="advert_id,date",
        batch_size=500,
    )
    if verbose:
        print(f"✅ upsert ok -> {target_table} rows={inserted}")

    return inserted