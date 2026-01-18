"""Load Ozon Performance data for ALL campaigns (BATCHED, optimized)"""

import sys
from pathlib import Path
import os
from dotenv import load_dotenv
import httpx
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.connectors.ozon_performance import OzonPerformanceConnector
from src.etl.ozon_performance_transformer import OzonPerformanceTransformer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()


def chunk_list(lst, size):
    """Split list into chunks"""
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def load_all_campaigns_batched():
    """Load performance data for ALL campaigns (batched, optimized)"""
    
    # Initialize
    client_id = os.getenv("OZON_PERF_CLIENT_ID")
    client_secret = os.getenv("OZON_PERF_CLIENT_SECRET")
    
    connector = OzonPerformanceConnector(client_id=client_id, client_secret=client_secret)
    transformer = OzonPerformanceTransformer()
    
    # Fetch all campaigns
    logger.info("="*80)
    logger.info("Fetching campaigns...")
    logger.info("="*80)
    
    campaigns_data = connector.fetch_campaigns()
    all_campaigns = campaigns_data.get("list", [])
    
    if not all_campaigns:
        logger.error("No campaigns found")
        return
    
    logger.info(f"\nTotal campaigns: {len(all_campaigns)}")
    
    # Filter only RUNNING campaigns (optional)
    FILTER_ACTIVE_ONLY = True  # Измени на False чтобы загрузить все
    
    if FILTER_ACTIVE_ONLY:
        campaigns = [c for c in all_campaigns if c.get("state") == "CAMPAIGN_STATE_RUNNING"]
        logger.info(f"Filtered to ACTIVE campaigns: {len(campaigns)}")
    else:
        campaigns = all_campaigns
    
    if not campaigns:
        logger.warning("No campaigns to process after filtering")
        return
    
    # Show sample campaigns
    logger.info("\nSample campaigns:")
    for i, campaign in enumerate(campaigns[:5], 1):
        logger.info(f"{i}. ID: {campaign['id']} - {campaign.get('title')} ({campaign.get('state')})")
    
    if len(campaigns) > 5:
        logger.info(f"... and {len(campaigns) - 5} more")
    
    # Date range
    today = datetime.now().date()
    date_from = (today - timedelta(days=30)).isoformat()  # Последние 30 дней
    date_to = (today - timedelta(days=1)).isoformat()
    
    logger.info(f"\nPeriod: {date_from} -> {date_to}")
    
    # Prepare for upload
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    
    table_name = "ozon_campaign_product_stats"
    rest_url = f"{supabase_url}/rest/v1/{table_name}"
    
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal"
    }
    
    # Process in batches (max 10 campaigns per report)
    campaign_ids = [c["id"] for c in campaigns]
    batches = list(chunk_list(campaign_ids, 10))
    
    logger.info(f"\nProcessing {len(batches)} batches (10 campaigns each)...")
    
    total_records = 0
    successful_batches = 0
    
    for batch_idx, batch_ids in enumerate(batches, 1):
        logger.info("\n" + "="*80)
        logger.info(f"Processing batch {batch_idx}/{len(batches)}")
        logger.info(f"Campaign IDs: {', '.join(batch_ids)}")
        logger.info("="*80)
        
        try:
            # Fetch stats for batch
            logger.info(f"Fetching report for {len(batch_ids)} campaigns...")
            report_data = connector.fetch_campaign_product_stats(
                campaign_ids=batch_ids,
                date_from=date_from,
                date_to=date_to,
                max_wait_seconds=300,
                poll_interval=10
            )
            
            # Check if ZIP (multiple campaigns) or CSV (single)
            is_zip = report_data[:2] == b'PK'  # ZIP magic bytes
            
            if is_zip:
                logger.info("Report is ZIP (multiple campaigns)")
                
                import zipfile
                import io
                
                # Extract and parse each CSV
                all_records = []
                
                with zipfile.ZipFile(io.BytesIO(report_data)) as zip_file:
                    csv_files = [f for f in zip_file.namelist() if f.endswith('.csv')]
                    logger.info(f"Found {len(csv_files)} CSV files in ZIP")
                    
                    for csv_filename in csv_files:
                        logger.info(f"  Processing {csv_filename}...")
                        csv_data = zip_file.read(csv_filename)
                        records = transformer.parse_csv_report(csv_data)
                        all_records.extend(records)
                        logger.info(f"    → {len(records)} records")
                
                records = all_records
            else:
                logger.info("Report is CSV (single campaign)")
                records = transformer.parse_csv_report(report_data)
            
            if not records:
                logger.warning(f"No data in batch {batch_idx}")
                continue
            
            logger.info(f"✅ Transformed {len(records)} total records")
            
            # Show stats
            unique_campaigns = set(r['campaign_id'] for r in records)
            unique_skus = set(r['sku'] for r in records)
            logger.info(f"   Campaigns: {len(unique_campaigns)}")
            logger.info(f"   Unique SKUs: {len(unique_skus)}")
            
            # Upload
            logger.info(f"Uploading to Supabase...")
            upload_url = f"{rest_url}?on_conflict=campaign_id,date,sku"
            
            # Split into chunks of 500 for upload
            upload_chunks = list(chunk_list(records, 500))
            for chunk_idx, chunk in enumerate(upload_chunks, 1):
                with httpx.Client(timeout=60.0) as client:
                    response = client.post(upload_url, headers=headers, json=chunk)
                    response.raise_for_status()
                logger.info(f"   Uploaded chunk {chunk_idx}/{len(upload_chunks)}: {len(chunk)} records")
            
            logger.info(f"✅ Batch {batch_idx} uploaded: {len(records)} records")
            
            total_records += len(records)
            successful_batches += 1
            
        except Exception as e:
            logger.error(f"Failed batch {batch_idx}: {e}", exc_info=True)
            continue
    
    # Summary
    logger.info("\n" + "="*80)
    logger.info("SUMMARY")
    logger.info("="*80)
    logger.info(f"Total campaigns: {len(campaigns)}")
    logger.info(f"Total batches: {len(batches)}")
    logger.info(f"Successfully processed: {successful_batches}/{len(batches)}")
    logger.info(f"Total records uploaded: {total_records}")
    
    # Verify
    logger.info("\nVerifying data in database...")
    
    with httpx.Client(timeout=30.0) as client:
        count_url = f"{rest_url}?select=campaign_id,sku"
        count_response = client.get(count_url, headers=headers)
        count_response.raise_for_status()
        all_data = count_response.json()
    
    # Group by campaign
    from collections import defaultdict
    by_campaign = defaultdict(set)
    for record in all_data:
        by_campaign[record['campaign_id']].add(record['sku'])
    
    logger.info(f"\nTotal unique campaigns in DB: {len(by_campaign)}")
    logger.info(f"Total records in DB: {len(all_data)}")
    
    logger.info("\nTop 10 campaigns by SKU count:")
    sorted_campaigns = sorted(by_campaign.items(), key=lambda x: len(x[1]), reverse=True)
    for campaign_id, skus in sorted_campaigns[:10]:
        logger.info(f"  Campaign {campaign_id}: {len(skus)} unique SKUs")
    
    logger.info("\n" + "="*80)
    logger.info("✅ ALL CAMPAIGNS LOADED SUCCESSFULLY!")
    logger.info("="*80)


if __name__ == "__main__":
    load_all_campaigns_batched()