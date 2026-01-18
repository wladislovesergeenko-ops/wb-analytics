"""Test Ozon API with ALL metrics and ALL SKUs"""

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
    
    # Date range (последние 7 доступных дней, учитывая 3-дневную задержку)
    today = datetime.now().date()
    date_from = today - timedelta(days=10)
    date_to = today - timedelta(days=4)
    
    logger.info(f"Fetching data: {date_from} -> {date_to}")
    
    # ВСЕ метрики
    all_metrics = [
        "revenue",              # заказано на сумму
        "ordered_units",        # заказано товаров
        "delivered_units",      # доставлено товаров
        "hits_view_search",     # показы в поиске и категории
        "hits_view_pdp",        # показы на карточке товара
        "hits_view",            # всего показов
        "hits_tocart_search",   # в корзину из поиска/категории
        "hits_tocart_pdp",      # в корзину из карточки
        "hits_tocart",          # всего в корзину
        "session_view_search",  # сессии с показом в поиске
        "session_view_pdp",     # сессии с показом на карточке
        "session_view",         # всего сессий
        "position_category"     # позиция в поиске
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
            
            # Если получили меньше чем limit - это последняя страница
            if len(batch) < limit:
                logger.info("Last page reached")
                break
            
            offset += limit
            
        except Exception as e:
            logger.error(f"Error fetching batch: {e}", exc_info=True)
            break
    
    logger.info(f"✅ Total fetched: {len(all_records)} records")
    
    # Сохраняем raw response
    output_dir = Path("logs")
    output_dir.mkdir(exist_ok=True)
    
    raw_file = output_dir / "ozon_full_data_raw.json"
    with open(raw_file, "w", encoding="utf-8") as f:
        json.dump({"result": {"data": all_records}}, f, indent=2, ensure_ascii=False)
    logger.info(f"Raw data saved to: {raw_file}")
    
    # Transform
    logger.info("Transforming data...")
    transformer = OzonTransformer()
    transformed = transformer.transform_analytics_data(
        {"result": {"data": all_records}},
        all_metrics
    )
    logger.info(f"✅ Transformed {len(transformed)} records")
    
    # Show sample
    if transformed:
        logger.info("\nSample transformed record:")
        print(json.dumps(transformed[0], indent=2, ensure_ascii=False))
    
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
        "Prefer": "resolution=merge-duplicates"
    }
    
    try:
        logger.info(f"Uploading {len(transformed)} records to Supabase...")
        
        # Batch upload (по 500 записей)
        batch_size = 500
        total_uploaded = 0
        
        for i in range(0, len(transformed), batch_size):
            batch = transformed[i:i + batch_size]
            
            with httpx.Client(timeout=60.0) as client:
                response = client.post(rest_url, headers=headers, json=batch)
                response.raise_for_status()
            
            total_uploaded += len(batch)
            logger.info(f"Uploaded batch {i//batch_size + 1}: {len(batch)} records (total: {total_uploaded})")
        
        logger.info(f"✅ Successfully uploaded all {total_uploaded} records")
        
        # Verify
        logger.info("\nVerifying data in database...")
        verify_url = f"{rest_url}?select=date,count&group_by=date&order=date.desc"
        
        with httpx.Client(timeout=30.0) as client:
            verify_response = client.get(verify_url, headers=headers)
            verify_response.raise_for_status()
            stats = verify_response.json()
        
        logger.info(f"\nRecords per date:")
        for stat in stats[:10]:
            logger.info(f"  {stat.get('date')}: {stat.get('count')} SKUs")
        
        logger.info("\n" + "="*80)
        logger.info("✅ FULL DATA PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info("="*80)
        
    except Exception as e:
        logger.error(f"Upload failed: {e}", exc_info=True)


if __name__ == "__main__":
    fetch_all_analytics()