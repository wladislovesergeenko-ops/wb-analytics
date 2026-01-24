#!/usr/bin/env python3
"""
Backfill script for normquery stats: Jan 1 - Jan 23, 2026
One-time historical data load.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import date
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

    # Historical period: Jan 1 - Jan 23, 2026
    date_from = date(2026, 1, 1)
    date_to = date(2026, 1, 23)

    logger.info(f"Backfill period: {date_from} -> {date_to}")

    # Get advert_id + nmid pairs
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

    # Fetch normquery stats
    all_stats = connector.fetch_normquery_stats_chunked(
        items=items,
        date_start=date_from.isoformat(),
        date_end=date_to.isoformat(),
        chunk_size=100,
        sleep_seconds=0.25,
    )

    logger.info(f"Fetched {len(all_stats)} cluster records")

    if not all_stats:
        logger.warning("No data returned from API")
        return

    # Transform
    df = WBTransformer.transform_normquery_stats(
        all_stats,
        date_from.isoformat(),
        date_to.isoformat()
    )

    if df.empty:
        logger.warning("No data to upsert")
        return

    logger.info(f"Transformed {len(df)} rows")

    # Batch upsert
    records = sanitize_records(df.to_dict(orient="records"))
    batch_size = settings.BATCH_SIZE
    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        supabase.table(settings.NORMQUERY_STATS_TABLE).upsert(
            batch,
            on_conflict="advert_id,nm_id,date_from,date_to,norm_query"
        ).execute()
        logger.info(f"Upserted batch {i//batch_size + 1}: {len(batch)} records")

    logger.info(f"✅ Backfill completed: {len(records)} total records")


if __name__ == "__main__":
    main()
