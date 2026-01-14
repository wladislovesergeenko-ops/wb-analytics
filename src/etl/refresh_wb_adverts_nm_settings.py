def refresh_wb_adverts_nm_settings(
    wb_key: str,
    supabase,
    table_name: str = "wb_adverts_nm_settings",
    актуальные_статусы=(4, 7, 9, 11),   # ready, active, paused
    batch_size: int = 500,
):
    """
    1) Забирает кампании WB adverts v2
    2) Превращает в df (1 строка = advert_id + nmid)
    3) Оставляет только актуальные статусы (по умолчанию 4/9/11)
    4) Полностью очищает таблицу в Supabase
    5) Вставляет свежие данные батчами
    """

    def _jsonable(v):
        # None / already JSON-friendly
        if v is None:
            return None
        # pandas Timestamp / python date/datetime -> ISO string
        if hasattr(v, "isoformat"):
            return v.isoformat()
        # numpy scalar -> python scalar
        if hasattr(v, "item"):
            try:
                return v.item()
            except Exception:
                return str(v)
        return v

    # 1) API -> df
    res = fetch_adverts_v2(wb_key)
    df = adverts_to_df(res)

    # 2) Фильтр актуальных кампаний
    if "status" in df.columns:
        df = df[df["status"].isin(актуальные_статусы)].copy()

    # 3) ts_* -> 'YYYY-MM-DD' (строки, чтобы Supabase JSON не падал)
    date_cols = ["ts_created", "ts_started", "ts_updated", "ts_deleted"]
    for c in date_cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.strftime("%Y-%m-%d")

    # 4) На всякий случай приводим имена колонок к lower
    df.columns = df.columns.str.lower()

    # 5) NaN/NA/NaT -> None
    df = df.where(pd.notnull(df), None)

    # 6) records: только JSON-сериализуемые типы
    records = [
        {k: _jsonable(v) for k, v in row.items()}
        for row in df.to_dict("records")
    ]

    # 7) Полный replace: удалить все старые строки
    # PostgREST delete обычно требует фильтр — используем gte по advert_id
    supabase.table(table_name).delete().gte("advert_id", 1).execute()

    # 8) Вставить свежие батчами
    for i in range(0, len(records), batch_size):
        supabase.table(table_name).insert(records[i:i + batch_size]).execute()

    print(f"refreshed {table_name}: rows={len(records)} (statuses={актуальные_статусы})")
    return len(records)