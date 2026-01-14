import os
from dotenv import load_dotenv
from supabase import create_client
import datetime as dt
from .sales_funnel import fetch_sales_funnel_products, build_df_out, supabase_upsert_df
from .sales_funnel import load_sales_funnel_by_days


def main():
    # 1) ENV
    load_dotenv(dotenv_path=".env")

    wb_key = os.getenv("WB_KEY")
    sb_url = os.getenv("SUPABASE_URL")
    sb_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")

    if not wb_key:
        raise RuntimeError("WB_KEY is missing")
    if not sb_url or not sb_key:
        raise RuntimeError("SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_ANON_KEY) is missing")
    
    # 2) Supabase client
    supabase = create_client(sb_url, sb_key)
    
    # 3) FLAGS (✅ ВОТ СЮДА)
    run_adverts_settings = os.getenv("RUN_ADVERTS_SETTINGS", "1") == "1"
    run_sales_funnel = os.getenv("RUN_SALES_FUNNEL", "1") == "1"
    run_adverts_fullstats = os.getenv("RUN_ADVERTS_FULLSTATS", "0") == "1"
    
    # 4) PARAMS
    table = os.getenv("SALES_FUNNEL_TABLE", "wb_sales_funnel_products")
    fullstats_sleep = int(os.getenv("FULLSTATS_SLEEP_SECONDS", "15"))
    fullstats_chunk = int(os.getenv("FULLSTATS_CHUNK_SIZE", "50"))
    overlap_days = int(os.getenv("OVERLAP_DAYS", "2"))
    sleep_seconds = int(os.getenv("SLEEP_SECONDS", "21"))

    date_to = dt.date.today() - dt.timedelta(days=1)          # вчера
    date_from = date_to - dt.timedelta(days=overlap_days)     # overlap

    date_from = date_from.strftime("%Y-%m-%d")
    date_to = date_to.strftime("%Y-%m-%d")

    # 5) RUN STEPS
    from .refresh_wb_adverts_nm_settings import refresh_wb_adverts_nm_settings
    if run_adverts_settings:
        refresh_wb_adverts_nm_settings(wb_key, supabase)

    if run_adverts_fullstats:
        from .load_fullstats_daily_range import load_fullstats_daily_range

        # только вчера
        yday = (dt.date.today() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
        print("RUN adverts fullstats for day:", yday)

        load_fullstats_daily_range(
            wb_key=wb_key,
            supabase=supabase,
            begin_date=yday,
            end_date=yday,
            sleep_seconds=fullstats_sleep,
            chunk_size=fullstats_chunk,
            verbose=True,
        )
    
    if run_sales_funnel:
        print("RUN sales_funnel range:", date_from, "->", date_to)
        load_sales_funnel_by_days(
            wb_key=wb_key,
            supabase=supabase,
            table_name=table,
            date_from=date_from,
            date_to=date_to,
            sleep_seconds=sleep_seconds,
            verbose=True,
        )

        print("✅ range upsert ok ->", table)


if __name__ == "__main__":
    main()