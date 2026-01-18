"""Simple test script for Ozon Analytics API"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
import json

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.connectors.ozon import OzonConnector
import logging

# Simple logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_ozon_analytics():
    """Test Ozon Analytics API"""
    
    # Get credentials from environment
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    api_key = os.getenv("OZON_API_KEY")
    client_id = os.getenv("OZON_CLIENT_ID")
    
    if not api_key or not client_id:
        logger.error("OZON_API_KEY and OZON_CLIENT_ID must be set in .env")
        return
    
    # Initialize connector
    logger.info("Initializing Ozon connector...")
    connector = OzonConnector(api_key=api_key, client_id=client_id)
    
    # Validate connection
    logger.info("Validating connection...")
    if not connector.validate_connection():
        logger.error("Connection validation failed")
        return
    
    logger.info("✅ Connection validated")
    
    # Test dates
    today = datetime.now().date()
    date_from = today - timedelta(days=10)
    date_to = today - timedelta(days=4)
    
    logger.info(f"Testing period: {date_from} -> {date_to}")
    
    # Test basic metrics
    try:
        response = connector.fetch_analytics_data(
            date_from=date_from.isoformat(),
            date_to=date_to.isoformat(),
            metrics=["revenue", "ordered_units"],
            dimensions=["day", "sku"],
            limit=10
        )
        
        logger.info(f"Response keys: {response.keys()}")
        
        if "result" in response and "data" in response["result"]:
            data = response["result"]["data"]
            logger.info(f"✅ Received {len(data)} records")
            
            # Show first record
            if data:
                logger.info("First record:")
                print(json.dumps(data[0], indent=2, ensure_ascii=False))
        
        # Save response
        output_dir = Path("logs")
        output_dir.mkdir(exist_ok=True)
        
        output_file = output_dir / "ozon_test_response.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(response, f, indent=2, ensure_ascii=False)
        
        logger.info(f"✅ Response saved to: {output_file}")
        
    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)
        return
    
    logger.info("✅ Test completed successfully!")


if __name__ == "__main__":
    test_ozon_analytics()