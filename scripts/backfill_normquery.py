#!/usr/bin/env python3
"""
Backfill script for normquery stats: Jan 1 - Jan 23, 2026
Loads data DAY BY DAY for proper time-series tracking.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import date, timedelta
from supabase import create_client

from src.config.settings import get_settings
from src.connectors.wb import WBConnector
from src.etl.transformers import WBTransformer
from src.etl.main import sanitize_records

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


def main():
    settings = get_settings()

    sb_key = settings.get_supabase_key()
    supabase = create_client(settings.SUPABASE_URL, sb_key)
    logger.info("✅ Supabase client initialized")

    connector = WBConnector(settings.WB_KEY)
    if not connector.validate_connection():
        raise RuntimeError("Failed to validate WB API connection")
    logger.info("✅ WB API connection validated")

    # Get advert_id + nmid pairs (fetch once, reuse for all days)
    resp = supabase.table(settings.ADVERTS_TABLE).select("advert_id,nmid").in_(
        "status", settings.get_fullstats_statuses()
    ).limit(10000).execute()

    items = []
    seen = set()
    for r in (resp.data or []):
        advert_id = r.get("advert_id")
        nmid = r.get("nmid")
        if advert_id and nmid:
            key = (int(advert_id), int(nmid))
            if key not in seen:
                seen.add(key)
                items.append({"advert_id": int(advert_id), "nm_id": int(nmid)})

    if not items:
        logger.warning("No items found")
        return

    logger.info(f"Found {len(items)} advert+nm pairs")

    # Delete old data first (1-23 January range)
    logger.info("Deleting old historical data (date_from between 2026-01-01 and 2026-01-23)...")
    supabase.table(settings.NORMQUERY_STATS_TABLE).delete().gte(
        "date_from", "2026-01-01"
    ).lte("date_from", "2026-01-23").execute()
    logger.info("✅ Old data deleted")

    # Historical period: Jan 1 - Jan 23, 2026 (day by day)
    start_date = date(2026, 1, 1)
    end_date = date(2026, 1, 23)

    current_date = start_date
    total_records = 0

    while current_date <= end_date:
        date_str = current_date.isoformat()
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing day: {date_str}")

        try:
            # Fetch normquery stats for single day
            all_stats = connector.fetch_normquery_stats_chunked(
                items=items,
                date_start=date_str,
                date_end=date_str,
                chunk_size=100,
                sleep_seconds=0.25,
            )

            if not all_stats:
                logger.info(f"No data for {date_str}")
                current_date += timedelta(days=1)
                continue

            # Transform (same date for from and to)
            df = WBTransformer.transform_normquery_stats(
                all_stats,
                date_str,
                date_str
            )

            if df.empty:
                logger.info(f"No valid data for {date_str}")
                current_date += timedelta(days=1)
                continue

            # Batch upsert
            records = sanitize_records(df.to_dict(orient="records"))
            batch_size = settings.BATCH_SIZE
            for i in range(0, len(records), batch_size):
                batch = records[i : i + batch_size]
                supabase.table(settings.NORMQUERY_STATS_TABLE).upsert(
                    batch,
                    on_conflict="advert_id,nm_id,date_from,date_to,norm_query"
                ).execute()

            logger.info(f"✅ {date_str}: {len(records)} records upserted")
            total_records += len(records)

        except Exception as e:
            logger.error(f"Failed to process {date_str}: {e}")

        current_date += timedelta(days=1)

    logger.info(f"\n{'='*60}")
    logger.info(f"✅ Backfill completed: {total_records} total records across {(end_date - start_date).days + 1} days")


if __name__ == "__main__":
    main()
