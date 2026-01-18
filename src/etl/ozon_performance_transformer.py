"""Ozon Performance API data transformers"""

from typing import List, Dict, Any, Optional
import csv
import io
import re
from datetime import datetime


class OzonPerformanceTransformer:
    """Transform Ozon Performance API CSV reports into normalized format"""
    
    @staticmethod
    def parse_campaign_header(first_line: str) -> Dict[str, Any]:
        """
        Parse campaign info from first line
        
        Example: ";Кампания по продвижению товаров № 20476484, период 11.01.2026-18.01.2026"
        
        Returns:
            Dict with campaign_id and period
        """
        # Extract campaign ID
        campaign_match = re.search(r'№\s*(\d+)', first_line)
        campaign_id = campaign_match.group(1) if campaign_match else None
        
        # Extract period
        period_match = re.search(r'период\s+([\d.]+)-([\d.]+)', first_line)
        if period_match:
            date_from = period_match.group(1)
            date_to = period_match.group(2)
        else:
            date_from = None
            date_to = None
        
        return {
            "campaign_id": campaign_id,
            "date_from": date_from,
            "date_to": date_to
        }
    
    @staticmethod
    def parse_csv_report(
        csv_data: bytes,
        encoding: str = 'utf-8'
    ) -> List[Dict[str, Any]]:
        """
        Parse Performance API CSV report
        
        Args:
            csv_data: Raw CSV bytes
            encoding: Text encoding (default: utf-8)
        
        Returns:
            List of normalized records
        """
        # Decode CSV
        csv_text = csv_data.decode(encoding)
        lines = csv_text.strip().split('\n')
        
        if len(lines) < 3:
            return []
        
        # Parse campaign info from first line
        campaign_info = OzonPerformanceTransformer.parse_campaign_header(lines[0])
        
        # Parse CSV (skip first line, use second line as header)
        csv_reader = csv.DictReader(
            io.StringIO('\n'.join(lines[1:])),
            delimiter=';'
        )
        
        records = []
        fetched_at = datetime.utcnow().isoformat()
        
        for row in csv_reader:
            # Skip "Всего" (totals) row
            day = row.get('День', '').strip()
            if not day or day.lower() == 'всего':
                continue
            
            # Convert date format: DD.MM.YYYY -> YYYY-MM-DD
            try:
                date_obj = datetime.strptime(day, '%d.%m.%Y')
                date_normalized = date_obj.strftime('%Y-%m-%d')
            except ValueError:
                # Skip if date parsing fails
                continue
            
            # Build record
            record = {
                'campaign_id': campaign_info['campaign_id'],
                'date': date_normalized,
                'sku': row.get('sku', '').strip(),
                'product_name': row.get('Название товара', '').strip(),
                
                # Metrics
                'price': OzonPerformanceTransformer._parse_decimal(row.get('Цена товара, ₽', '0')),
                'impressions': OzonPerformanceTransformer._parse_int(row.get('Показы', '0')),
                'clicks': OzonPerformanceTransformer._parse_int(row.get('Клики', '0')),
                'ctr': OzonPerformanceTransformer._parse_decimal(row.get('CTR (%)', '0')),
                'add_to_cart': OzonPerformanceTransformer._parse_int(row.get('В корзину', '0')),
                'avg_cpc': OzonPerformanceTransformer._parse_decimal(row.get('Средняя стоимость клика, ₽', '0')),
                'cost': OzonPerformanceTransformer._parse_decimal(row.get('Расход, ₽, с НДС', '0')),
                'orders': OzonPerformanceTransformer._parse_int(row.get('Заказы', '0')),
                'revenue': OzonPerformanceTransformer._parse_decimal(row.get('Продажи, ₽', '0')),
                'model_orders': OzonPerformanceTransformer._parse_int(row.get('Заказы модели', '0')),
                'model_revenue': OzonPerformanceTransformer._parse_decimal(row.get('Продажи с заказов модели, ₽', '0')),
                'drr': OzonPerformanceTransformer._parse_decimal(row.get('ДРР, %', '0')),
                'date_added': row.get('Дата добавления', '').strip(),
                
                # Metadata
                'fetched_at': fetched_at
            }
            
            records.append(record)
        
        return records
    
    @staticmethod
    def _parse_decimal(value: str) -> Optional[float]:
        """Parse decimal value (handles comma as separator)"""
        if not value or value.strip() == '':
            return None
        try:
            # Replace comma with dot and remove spaces
            normalized = value.replace(',', '.').replace(' ', '').strip()
            return float(normalized)
        except (ValueError, AttributeError):
            return None
    
    @staticmethod
    def _parse_int(value: str) -> Optional[int]:
        """Parse integer value"""
        if not value or value.strip() == '':
            return None
        try:
            # Remove spaces
            normalized = value.replace(' ', '').strip()
            return int(normalized)
        except (ValueError, AttributeError):
            return None
    
    @staticmethod
    def validate_record(record: Dict[str, Any]) -> bool:
        """Validate that record has required fields"""
        required_fields = ['campaign_id', 'date', 'sku']
        return all(field in record and record[field] for field in required_fields)