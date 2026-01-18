"""Test Ozon Performance data upload to Supabase"""

import sys
from pathlib import Path
import os
from dotenv import load_dotenv
import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.connectors.ozon_performance import OzonPerformanceConnector
from src.etl.ozon_performance_transformer import OzonPerformanceTransformer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()


def test_performance_upload():
    """Test full pipeline: fetch -> transform -> upload"""
    
    # Initialize connector
    client_id = os.getenv("OZON_PERF_CLIENT_ID")
    client_secret = os.getenv("OZON_PERF_CLIENT_SECRET")
    
    connector = OzonPerformanceConnector(client_id=client_id, client_secret=client_secret)
    
    # Fetch campaigns
    logger.info("Fetching campaigns...")
    campaigns_data = connector.fetch_campaigns()
    campaigns = campaigns_data.get("list", [])
    
    if not campaigns:
        logger.error("No campaigns found")
        return
    
    logger.info(f"Found {len(campaigns)} campaigns")
    
    # Use first campaign
    first_campaign = campaigns[0]
    campaign_id = first_campaign["id"]
    
    logger.info(f"Using campaign: {campaign_id} - {first_campaign.get('title')}")
    
    # Fetch stats
    from datetime import datetime, timedelta
    today = datetime.now().date()
    date_from = (today - timedelta(days=7)).isoformat()
    date_to = (today - timedelta(days=1)).isoformat()
    
    logger.info(f"Fetching stats: {date_from} -> {date_to}")
    
    report_data = connector.fetch_campaign_product_stats(
        campaign_ids=[campaign_id],
        date_from=date_from,
        date_to=date_to
    )
    
    # Transform
    logger.info("Transforming data...")
    transformer = OzonPerformanceTransformer()
    records = transformer.parse_csv_report(report_data)
    
    logger.info(f"✅ Transformed {len(records)} records")
    
    if records:
        logger.info("\nSample record:")
        import json
        print(json.dumps(records[0], indent=2, ensure_ascii=False, default=str))
    
    # Upload to Supabase
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
    
    logger.info(f"Uploading {len(records)} records to Supabase...")
    
    upload_url = f"{rest_url}?on_conflict=campaign_id,date,sku"
    
    with httpx.Client(timeout=60.0) as client:
        response = client.post(upload_url, headers=headers, json=records)
        response.raise_for_status()
    
    logger.info(f"✅ Successfully uploaded {len(records)} records")
    
    # Verify
    logger.info("\nVerifying data...")
    verify_url = f"{rest_url}?campaign_id=eq.{campaign_id}&limit=5&order=date.desc"
    
    with httpx.Client(timeout=30.0) as client:
        verify_response = client.get(verify_url, headers=headers)
        verify_response.raise_for_status()
        verify_data = verify_response.json()
    
    logger.info(f"Found {len(verify_data)} records in database for campaign {campaign_id}")
    
    logger.info("\n✅ Performance data pipeline completed!")


if __name__ == "__main__":
    test_performance_upload()