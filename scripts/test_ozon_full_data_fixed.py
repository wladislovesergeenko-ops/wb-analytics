"""Test Ozon API with ALL metrics and ALL SKUs - FIXED UPSERT"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
import json
import os
from dotenv import load_dotenv
import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.connectors.ozon import OzonConnector
from src.etl.ozon_transformer import OzonTransformer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()


def fetch_all_analytics():
    """Fetch ALL metrics for ALL SKUs with pagination"""
    
    api_key = os.getenv("OZON_API_KEY")
    client_id = os.getenv("OZON_CLIENT_ID")
    
    if not api_key or not client_id:
        logger.error("OZON_API_KEY and OZON_CLIENT_ID must be set")
        return
    
    # Initialize connector
    connector = OzonConnector(api_key=api_key, client_id=client_id)
    
    # Date range
    today = datetime.now().date()
    date_from = datetime(2026, 1, 1).date()
    date_to = datetime(2026, 1, 17).date()
    
    logger.info(f"Fetching data: {date_from} -> {date_to}")
    
    # ВСЕ метрики
    all_metrics = [
        "revenue",
        "ordered_units",
        "delivered_units",
        "hits_view_search",
        "hits_view_pdp",
        "hits_view",
        "hits_tocart_search",
        "hits_tocart_pdp",
        "hits_tocart",
        "session_view_search",
        "session_view_pdp",
        "session_view",
        "position_category"
    ]
    
    # Пагинация
    all_records = []
    limit = 1000
    offset = 0
    
    logger.info(f"Requesting {len(all_metrics)} metrics...")
    
    while True:
        logger.info(f"Fetching batch: offset={offset}, limit={limit}")
        
        try:
            response = connector.fetch_analytics_data(
                date_from=date_from.isoformat(),
                date_to=date_to.isoformat(),
                metrics=all_metrics,
                dimensions=["day", "sku"],
                limit=limit,
                offset=offset
            )
            
            if "result" not in response or "data" not in response["result"]:
                logger.warning("No result/data in response")
                break
            
            batch = response["result"]["data"]
            
            if not batch:
                logger.info("No more data")
                break
            
            all_records.extend(batch)
            logger.info(f"Fetched {len(batch)} records, total: {len(all_records)}")
            
            if len(batch) < limit:
                logger.info("Last page reached")
                break
            
            offset += limit
            
        except Exception as e:
            logger.error(f"Error fetching batch: {e}", exc_info=True)
            break
    
    logger.info(f"✅ Total fetched: {len(all_records)} records")
    
    # Transform
    logger.info("Transforming data...")
    transformer = OzonTransformer()
    transformed = transformer.transform_analytics_data(
        {"result": {"data": all_records}},
        all_metrics
    )
    logger.info(f"✅ Transformed {len(transformed)} records")
    
    # Show sample with non-zero values
    logger.info("\nLooking for records with sales...")
    records_with_sales = [r for r in transformed if r.get("revenue", 0) > 0]
    if records_with_sales:
        logger.info(f"Found {len(records_with_sales)} records with sales")
        logger.info("\nSample record WITH sales:")
        print(json.dumps(records_with_sales[0], indent=2, ensure_ascii=False))
    else:
        logger.info("No records with sales in this period")
        logger.info("\nSample record (views/sessions only):")
        records_with_views = [r for r in transformed if r.get("hits_view", 0) > 0]
        if records_with_views:
            print(json.dumps(records_with_views[0], indent=2, ensure_ascii=False))
    
    # Upload to Supabase
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    
    if not supabase_url or not supabase_key:
        logger.error("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set")
        return
    
    table_name = "ozon_analytics_data"
    rest_url = f"{supabase_url}/rest/v1/{table_name}"
    
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal"
    }
    
    try:
        logger.info(f"Uploading {len(transformed)} records to Supabase (with upsert)...")
        
        # Batch upload (по 500 записей)
        batch_size = 500
        total_uploaded = 0
        
        for i in range(0, len(transformed), batch_size):
            batch = transformed[i:i + batch_size]
            
            # ВАЖНО: добавляем on_conflict для upsert
            upload_url = f"{rest_url}?on_conflict=date,sku"
            
            with httpx.Client(timeout=60.0) as client:
                response = client.post(upload_url, headers=headers, json=batch)
                response.raise_for_status()
            
            total_uploaded += len(batch)
            logger.info(f"Uploaded batch {i//batch_size + 1}: {len(batch)} records (total: {total_uploaded})")
        
        logger.info(f"✅ Successfully uploaded/updated all {total_uploaded} records")
        
        # Statistics
        logger.info("\nGetting statistics from database...")
        
        # Count total records
        count_url = f"{rest_url}?select=count"
        with httpx.Client(timeout=30.0) as client:
            count_response = client.get(count_url, headers={**headers, "Prefer": "count=exact"})
            count_response.raise_for_status()
        
        # Records per date
        stats_url = f"{supabase_url}/rest/v1/rpc/get_ozon_stats_by_date"
        # Fallback to simple query
        simple_stats_url = f"{rest_url}?select=date,sku&order=date.desc"
        
        with httpx.Client(timeout=30.0) as client:
            stats_response = client.get(simple_stats_url, headers=headers)
            stats_response.raise_for_status()
            all_data = stats_response.json()
        
        # Group by date
        from collections import defaultdict
        by_date = defaultdict(int)
        for record in all_data:
            by_date[record['date']] += 1
        
        logger.info(f"\nTotal records in database: {len(all_data)}")
        logger.info(f"\nRecords per date:")
        for date in sorted(by_date.keys(), reverse=True):
            logger.info(f"  {date}: {by_date[date]} SKUs")
        
        logger.info("\n" + "="*80)
        logger.info("✅ FULL DATA PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info("="*80)
        
    except Exception as e:
        logger.error(f"Upload failed: {e}", exc_info=True)


if __name__ == "__main__":
    fetch_all_analytics()