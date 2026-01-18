"""Test Ozon Performance API"""

import sys
from pathlib import Path
import json
import os
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.connectors.ozon_performance import OzonPerformanceConnector
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()


def test_performance_api():
    """Test Performance API connection and campaigns"""
    
    client_id = os.getenv("OZON_PERF_CLIENT_ID")
    client_secret = os.getenv("OZON_PERF_CLIENT_SECRET")
    
    if not client_id or not client_secret:
        logger.error("OZON_PERF_CLIENT_ID and OZON_PERF_CLIENT_SECRET must be set")
        return
    
    # Initialize connector
    logger.info("Initializing Performance API connector...")
    connector = OzonPerformanceConnector(
        client_id=client_id,
        client_secret=client_secret
    )
    
    # Validate connection
    logger.info("Validating connection...")
    if not connector.validate_connection():
        logger.error("Connection validation failed")
        return
    
    logger.info("✅ Connection validated")
    
    # Fetch campaigns
    logger.info("\n" + "="*80)
    logger.info("Fetching campaigns...")
    logger.info("="*80)
    
    campaigns_data = connector.fetch_campaigns()
    
    # Save full response
    output_dir = Path("logs")
    output_dir.mkdir(exist_ok=True)
    
    campaigns_file = output_dir / "ozon_performance_campaigns.json"
    with open(campaigns_file, "w", encoding="utf-8") as f:
        json.dump(campaigns_data, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Campaigns data saved to: {campaigns_file}")
    
    # Show campaigns summary
    campaigns = campaigns_data.get("list", [])
    logger.info(f"\nFound {len(campaigns)} campaigns:")
    
    for campaign in campaigns[:5]:  # Show first 5
        logger.info(f"\nCampaign ID: {campaign.get('id')}")
        logger.info(f"  Title: {campaign.get('title')}")
        logger.info(f"  State: {campaign.get('state')}")
        logger.info(f"  Type: {campaign.get('advObjectType')}")
        logger.info(f"  Budget: {campaign.get('dailyBudget')}")
    
    if len(campaigns) > 5:
        logger.info(f"\n... and {len(campaigns) - 5} more campaigns")
    
    # Test campaign product stats for first campaign
    if campaigns:
        logger.info("\n" + "="*80)
        logger.info("Testing campaign statistics report...")
        logger.info("="*80)
        
        first_campaign_id = campaigns[0]["id"]
        logger.info(f"Using campaign ID: {first_campaign_id}")
        
        from datetime import datetime, timedelta
        today = datetime.now().date()
        date_from = (today - timedelta(days=7)).isoformat()
        date_to = (today - timedelta(days=1)).isoformat()
        
        try:
            # Fetch stats (async flow)
            report_data = connector.fetch_campaign_product_stats(
                campaign_ids=[first_campaign_id],
                date_from=date_from,
                date_to=date_to
            )
            
            # Save report (CSV or ZIP)
            report_file = output_dir / "ozon_performance_report.csv"
            with open(report_file, "wb") as f:
                f.write(report_data)
            
            logger.info(f"✅ Report saved to: {report_file}")
            
            # Try to parse CSV
            try:
                import csv
                import io
                
                csv_text = report_data.decode('utf-8')
                reader = csv.DictReader(io.StringIO(csv_text), delimiter=';')
                rows = list(reader)
                
                logger.info(f"\nReport contains {len(rows)} rows")
                
                if rows:
                    logger.info("\nFirst row:")
                    for key, value in rows[0].items():
                        logger.info(f"  {key}: {value}")
            
            except Exception as e:
                logger.warning(f"Could not parse CSV: {e}")
                logger.info("Report might be in ZIP format for multiple campaigns")
            
        except Exception as e:
            logger.error(f"Failed to fetch stats: {e}", exc_info=True)
    
    logger.info("\n" + "="*80)
    logger.info("✅ Performance API test completed!")
    logger.info("="*80)


if __name__ == "__main__":
    test_performance_api()