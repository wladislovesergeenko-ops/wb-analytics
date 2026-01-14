import os
from dotenv import load_dotenv
from supabase import create_client
import datetime as dt
from .sales_funnel import fetch_sales_funnel_products, build_df_out, supabase_upsert_df
from .sales_funnel import load_sales_funnel_by_days

def main():
    load_dotenv(dotenv_path=".env")

    wb_key = os.getenv("WB_KEY")
    sb_url = os.getenv("SUPABASE_URL")
    sb_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")

    if not wb_key:
        raise RuntimeError("WB_KEY is missing")
    if not sb_url or not sb_key:
        raise RuntimeError("SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_ANON_KEY) is missing")

    supabase = create_client(sb_url, sb_key)
    table = os.getenv("SALES_FUNNEL_TABLE", "wb_sales_funnel_products")

    overlap_days = int(os.getenv("OVERLAP_DAYS", "2"))
    sleep_seconds = int(os.getenv("SLEEP_SECONDS", "21"))

    date_to = dt.date.today() - dt.timedelta(days=1)          # вчера
    date_from = date_to - dt.timedelta(days=overlap_days)     # overlap

    date_from = date_from.strftime("%Y-%m-%d")
    date_to = date_to.strftime("%Y-%m-%d")

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