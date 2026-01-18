"""Test uploading Ozon data to Supabase"""

import sys
from pathlib import Path
import json
import os
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.etl.ozon_transformer import OzonTransformer
from supabase import create_client, Client
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()


def test_upload():
    """Test uploading transformed data to Supabase"""
    
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
    
    # Connect to Supabase
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    
    if not supabase_url or not supabase_key:
        logger.error("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set")
        return
    
    logger.info("Connecting to Supabase...")
    supabase: Client = create_client(supabase_url, supabase_key)
    
    # Upload data (upsert)
    table_name = "ozon_analytics_data"
    
    try:
        logger.info(f"Uploading {len(transformed)} records to {table_name}...")
        
        response = supabase.table(table_name).upsert(
            transformed,
            on_conflict="date,sku"  # Обновляем если есть дубликаты
        ).execute()
        
        logger.info(f"✅ Successfully uploaded {len(transformed)} records")
        logger.info(f"Response: {response}")
        
        # Verify data
        logger.info("\nVerifying uploaded data...")
        verify_response = supabase.table(table_name)\
            .select("*")\
            .eq("date", transformed[0]["date"])\
            .limit(3)\
            .execute()
        
        logger.info(f"Found {len(verify_response.data)} records in database")
        
        if verify_response.data:
            logger.info("\nFirst record from database:")
            print(json.dumps(verify_response.data[0], indent=2, ensure_ascii=False, default=str))
        
        logger.info("\n✅ Upload test completed successfully!")
        
    except Exception as e:
        logger.error(f"Upload failed: {e}", exc_info=True)


if __name__ == "__main__":
    test_upload()