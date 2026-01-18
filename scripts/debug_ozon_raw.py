"""Debug script to check raw API response"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
import json
import os
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.connectors.ozon import OzonConnector
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()


def debug_api_response():
    """Check what API actually returns"""
    
    api_key = os.getenv("OZON_API_KEY")
    client_id = os.getenv("OZON_CLIENT_ID")
    
    connector = OzonConnector(api_key=api_key, client_id=client_id)
    
    # Точно такой же период как на скриншоте
    date_from = "2026-01-08"
    date_to = "2026-01-14"
    
    logger.info(f"Fetching data: {date_from} -> {date_to}")
    
    # Все метрики
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
    
    # Запрос БЕЗ пагинации, чтобы видеть первую порцию
    response = connector.fetch_analytics_data(
        date_from=date_from,
        date_to=date_to,
        metrics=all_metrics,
        dimensions=["day", "sku"],
        limit=1000,
        offset=0
    )
    
    # Сохраняем полный ответ
    output_file = Path("logs/ozon_debug_raw.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(response, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Full response saved to: {output_file}")
    
    # Анализируем данные
    if "result" not in response or "data" not in response["result"]:
        logger.error("No data in response!")
        return
    
    records = response["result"]["data"]
    logger.info(f"\nTotal records: {len(records)}")
    
    # Ищем записи с продажами за 8 января
    logger.info("\n" + "="*80)
    logger.info("Records for 2026-01-08 WITH revenue:")
    logger.info("="*80)
    
    records_jan8_with_sales = []
    
    for record in records:
        dims = record.get("dimensions", [])
        metrics = record.get("metrics", [])
        
        if len(dims) >= 2 and len(metrics) >= 2:
            date = dims[0].get("id", "")
            sku = dims[1].get("id", "")
            name = dims[1].get("name", "")
            
            revenue = metrics[0]  # первая метрика
            ordered_units = metrics[1]  # вторая метрика
            
            if date == "2026-01-08" and revenue > 0:
                records_jan8_with_sales.append({
                    "sku": sku,
                    "name": name[:80],
                    "revenue": revenue,
                    "ordered_units": ordered_units
                })
    
    logger.info(f"Found {len(records_jan8_with_sales)} records with sales on 2026-01-08")
    
    for r in sorted(records_jan8_with_sales, key=lambda x: x["revenue"], reverse=True):
        logger.info(f"\nSKU: {r['sku']}")
        logger.info(f"Name: {r['name']}")
        logger.info(f"Revenue: {r['revenue']}₽")
        logger.info(f"Orders: {r['ordered_units']}")
    
    # Проверяем конкретные SKU из скриншота
    logger.info("\n" + "="*80)
    logger.info("Checking specific SKU from screenshot:")
    logger.info("="*80)
    
    target_sku = "1691632648"  # Это SKU с revenue 1189₽
    
    for record in records:
        dims = record.get("dimensions", [])
        if len(dims) >= 2 and dims[1].get("id") == target_sku:
            logger.info(f"\nFound SKU {target_sku}:")
            logger.info(f"Date: {dims[0].get('id')}")
            logger.info(f"Name: {dims[1].get('name')[:80]}")
            logger.info(f"Metrics: {record.get('metrics')}")
            logger.info(f"Full record:")
            print(json.dumps(record, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    debug_api_response()