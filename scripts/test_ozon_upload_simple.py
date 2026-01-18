"""Test uploading Ozon data to Supabase (simple version)"""

import sys
from pathlib import Path
import json
import os
from dotenv import load_dotenv
import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.etl.ozon_transformer import OzonTransformer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()


def test_upload_simple():
    """Test uploading using direct HTTP requests"""
    
    # Load test response
    test_file = Path("logs/ozon_test_response.json")
    if not test_file.exists():
        logger.error("Test file not found. Run test_ozon_simple.py first.")
        return
    
    with open(test_file, "r", encoding="utf-8") as f:
        raw_data = json.load(f)
    
    # Transform data
    logger.info("Transforming data...")
    transformer = OzonTransformer()
    metrics_order = ["revenue", "ordered_units"]
    transformed = transformer.transform_analytics_data(raw_data, metrics_order)
    
    logger.info(f"✅ Transformed {len(transformed)} records")
    
    # Supabase credentials
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    
    if not supabase_url or not supabase_key:
        logger.error("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set")
        return
    
    # PostgREST endpoint
    table_name = "ozon_analytics_data"
    rest_url = f"{supabase_url}/rest/v1/{table_name}"
    
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"  # Upsert mode
    }
    
    try:
        logger.info(f"Uploading {len(transformed)} records to {table_name}...")
        
        with httpx.Client(timeout=30.0) as client:
            response = client.post(
                rest_url,
                headers=headers,
                json=transformed
            )
            response.raise_for_status()
        
        logger.info(f"✅ Successfully uploaded {len(transformed)} records")
        logger.info(f"Status: {response.status_code}")
        
        # Verify data
        logger.info("\nVerifying uploaded data...")
        verify_url = f"{rest_url}?date=eq.{transformed[0]['date']}&limit=3"
        
        with httpx.Client(timeout=30.0) as client:
            verify_response = client.get(verify_url, headers=headers)
            verify_response.raise_for_status()
            verify_data = verify_response.json()
        
        logger.info(f"Found {len(verify_data)} records in database")
        
        if verify_data:
            logger.info("\nFirst record from database:")
            print(json.dumps(verify_data[0], indent=2, ensure_ascii=False, default=str))
        
        logger.info("\n✅ Upload test completed successfully!")
        
    except httpx.HTTPError as e:
        logger.error(f"Upload failed: {e}", exc_info=True)
        if hasattr(e, 'response') and e.response:
            logger.error(f"Response: {e.response.text}")


if __name__ == "__main__":
    test_upload_simple()