"""
Скрипт загрузки исторических данных в Supabase.

Загружает данные за период 2026-01-01 — 2026-01-18:
1. wb_tariffs_commission — комиссии (единоразово, UPSERT)
2. wb_search_report_products — отчёт по поисковым позициям (по дням)
3. wb_product_search_texts — поисковые запросы по товарам (по дням)

Запуск:
    python scripts/load_historical_data.py

Флаги (можно комбинировать):
    --commission      Загрузить только комиссии
    --search-report   Загрузить только search_report
    --search-texts    Загрузить только search_texts
    --all             Загрузить всё (по умолчанию)
    --start 2026-01-01  Начальная дата
    --end 2026-01-18    Конечная дата
"""

import os
import sys
import argparse
import time
from datetime import date, datetime, timedelta
from dotenv import load_dotenv

# Добавляем корень проекта в путь
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from supabase import create_client, Client
from src.connectors.wb import WBConnector
from src.etl.transformers import WBTransformer
from src.logging_config.logger import configure_logging, setup_logger

# Загружаем переменные окружения
load_dotenv()

# Настраиваем логирование
configure_logging()
logger = setup_logger(__name__)

# Константы
BATCH_SIZE = 500
SLEEP_BETWEEN_DAYS = 21  # Rate limit: 3 req/min


def get_supabase_client() -> Client:
    """Создаёт клиент Supabase"""
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")

    if not url or not key:
        raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required")

    return create_client(url, key)


def upsert_batch(supabase: Client, table: str, records: list, on_conflict: str) -> int:
    """Вставляет/обновляет записи батчами"""
    if not records:
        return 0

    total = 0
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i : i + BATCH_SIZE]
        supabase.table(table).upsert(batch, on_conflict=on_conflict).execute()
        total += len(batch)
        logger.info(f"  Upserted {total}/{len(records)} records to {table}")

    return total


def load_commission(connector: WBConnector, supabase: Client) -> int:
    """Загружает комиссии (UPSERT по subject_id)"""
    logger.info("=" * 60)
    logger.info("Loading tariffs commission...")
    logger.info("=" * 60)

    raw_data = connector.fetch_tariffs_commission()
    df = WBTransformer.transform_tariffs_commission(raw_data)

    if df.empty:
        logger.warning("No commission data to load")
        return 0

    records = df.to_dict(orient="records")
    count = upsert_batch(supabase, "wb_tariffs_commission", records, on_conflict="subject_id")

    logger.info(f"✅ Loaded {count} commission records")
    return count


def load_search_report_for_date(
    connector: WBConnector,
    supabase: Client,
    date_str: str,
) -> int:
    """Загружает search_report за один день"""
    logger.info(f"  Fetching search_report for {date_str}...")

    # Получаем все группы с пагинацией
    groups = connector.fetch_search_report_all(
        start=date_str,
        end=date_str,
        sleep_seconds=SLEEP_BETWEEN_DAYS,
    )

    if not groups:
        logger.warning(f"  No groups for {date_str}")
        return 0

    df = WBTransformer.transform_search_report_groups(groups, date_str, date_str)

    if df.empty:
        return 0

    records = df.to_dict(orient="records")
    count = upsert_batch(
        supabase,
        "wb_search_report_products",
        records,
        on_conflict="nm_id,period_start,period_end",
    )

    return count


def load_search_report(
    connector: WBConnector,
    supabase: Client,
    start_date: date,
    end_date: date,
) -> int:
    """Загружает search_report за диапазон дат"""
    logger.info("=" * 60)
    logger.info(f"Loading search_report: {start_date} -> {end_date}")
    logger.info("=" * 60)

    total = 0
    current = start_date

    while current <= end_date:
        date_str = current.isoformat()
        count = load_search_report_for_date(connector, supabase, date_str)
        total += count
        logger.info(f"  {date_str}: loaded {count} products (total: {total})")

        current += timedelta(days=1)

        if current <= end_date:
            logger.info(f"  Sleeping {SLEEP_BETWEEN_DAYS}s (rate limit)...")
            time.sleep(SLEEP_BETWEEN_DAYS)

    logger.info(f"✅ Loaded {total} search_report records")
    return total


def load_search_texts_for_date(
    connector: WBConnector,
    supabase: Client,
    date_str: str,
    nm_ids: list,
) -> int:
    """Загружает search_texts за один день"""
    logger.info(f"  Fetching search_texts for {date_str} ({len(nm_ids)} products)...")

    if not nm_ids:
        logger.warning(f"  No nm_ids for {date_str}")
        return 0

    # Получаем данные с чанкингом (max 50 nmIds per request)
    items = connector.fetch_product_search_texts_chunked(
        nm_ids=nm_ids,
        start=date_str,
        end=date_str,
        sleep_seconds=SLEEP_BETWEEN_DAYS,
    )

    if not items:
        logger.warning(f"  No search texts for {date_str}")
        return 0

    df = WBTransformer.transform_product_search_texts(items, date_str, date_str)

    if df.empty:
        return 0

    records = df.to_dict(orient="records")
    count = upsert_batch(
        supabase,
        "wb_product_search_texts",
        records,
        on_conflict="nm_id,text,period_start,period_end",
    )

    return count


def get_nm_ids_from_search_report(supabase: Client, date_str: str) -> list:
    """Получает nm_ids из уже загруженного search_report"""
    response = (
        supabase.table("wb_search_report_products")
        .select("nm_id")
        .eq("period_start", date_str)
        .execute()
    )

    nm_ids = [r["nm_id"] for r in response.data if r.get("nm_id")]
    return list(set(nm_ids))


def load_search_texts(
    connector: WBConnector,
    supabase: Client,
    start_date: date,
    end_date: date,
) -> int:
    """Загружает search_texts за диапазон дат"""
    logger.info("=" * 60)
    logger.info(f"Loading search_texts: {start_date} -> {end_date}")
    logger.info("=" * 60)

    total = 0
    current = start_date

    while current <= end_date:
        date_str = current.isoformat()

        # Получаем nm_ids из search_report за этот день
        nm_ids = get_nm_ids_from_search_report(supabase, date_str)

        if not nm_ids:
            logger.warning(f"  {date_str}: no nm_ids in search_report, skipping")
            current += timedelta(days=1)
            continue

        count = load_search_texts_for_date(connector, supabase, date_str, nm_ids)
        total += count
        logger.info(f"  {date_str}: loaded {count} search texts (total: {total})")

        current += timedelta(days=1)

        if current <= end_date:
            logger.info(f"  Sleeping {SLEEP_BETWEEN_DAYS}s (rate limit)...")
            time.sleep(SLEEP_BETWEEN_DAYS)

    logger.info(f"✅ Loaded {total} search_texts records")
    return total


def main():
    parser = argparse.ArgumentParser(description="Load historical WB data to Supabase")
    parser.add_argument("--commission", action="store_true", help="Load commission only")
    parser.add_argument("--search-report", action="store_true", help="Load search_report only")
    parser.add_argument("--search-texts", action="store_true", help="Load search_texts only")
    parser.add_argument("--all", action="store_true", help="Load all data (default)")
    parser.add_argument("--start", type=str, default="2026-01-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default="2026-01-18", help="End date (YYYY-MM-DD)")

    args = parser.parse_args()

    # Если ничего не выбрано — загружаем всё
    load_all = args.all or not (args.commission or args.search_report or args.search_texts)

    start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end, "%Y-%m-%d").date()

    logger.info("=" * 60)
    logger.info("   HISTORICAL DATA LOADER")
    logger.info("=" * 60)
    logger.info(f"Period: {start_date} -> {end_date}")
    logger.info(f"Load commission: {load_all or args.commission}")
    logger.info(f"Load search_report: {load_all or args.search_report}")
    logger.info(f"Load search_texts: {load_all or args.search_texts}")

    # Инициализация
    api_key = os.getenv("WB_KEY")
    if not api_key:
        logger.error("WB_KEY not found in .env")
        sys.exit(1)

    connector = WBConnector(api_key=api_key)
    supabase = get_supabase_client()

    results = {}

    try:
        # 1. Комиссии
        if load_all or args.commission:
            results["commission"] = load_commission(connector, supabase)

        # 2. Search Report (нужно загрузить до search_texts!)
        if load_all or args.search_report:
            results["search_report"] = load_search_report(connector, supabase, start_date, end_date)

        # 3. Search Texts (зависит от search_report)
        if load_all or args.search_texts:
            results["search_texts"] = load_search_texts(connector, supabase, start_date, end_date)

    except Exception as e:
        logger.error(f"Error during load: {e}", exc_info=True)
        sys.exit(1)

    # Итоги
    logger.info("=" * 60)
    logger.info("   RESULTS")
    logger.info("=" * 60)
    for key, count in results.items():
        logger.info(f"  {key}: {count} records")

    logger.info("=" * 60)
    logger.info("🎉 Done!")

    return 0


if __name__ == "__main__":
    sys.exit(main())
