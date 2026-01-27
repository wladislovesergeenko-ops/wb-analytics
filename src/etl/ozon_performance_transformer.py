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


class OzonSkuPromoTransformer:
    """
    Transform Ozon SKU Promo (Оплата за заказ) API reports into normalized format.

    SKU Promo Orders Report fields:
    - период отчёта
    - дату
    - ID заказа
    - номер заказа
    - SKU
    - SKU продвигаемого товара
    - артикул
    - наименование
    - источник заказа
    - количество
    - стоимость, ₽
    - ставку, %
    - ставку, ₽
    - расход, ₽
    """

    @staticmethod
    def parse_orders_report(
        report_data: bytes,
        encoding: str = 'utf-8'
    ) -> List[Dict[str, Any]]:
        """
        Parse SKU Promo orders report (JSON or CSV)

        Args:
            report_data: Raw report bytes
            encoding: Text encoding

        Returns:
            List of normalized order records
        """
        import json

        fetched_at = datetime.utcnow().isoformat()
        records = []

        # Try JSON first
        try:
            text = report_data.decode(encoding)
            data = json.loads(text)

            # JSON format with "rows" wrapper: {"rows": [...]}
            if isinstance(data, dict) and "rows" in data:
                for item in data["rows"]:
                    record = OzonSkuPromoTransformer._parse_json_order(item, fetched_at)
                    if record:
                        records.append(record)
                return records

            # JSON format - direct list of orders
            if isinstance(data, list):
                for item in data:
                    record = OzonSkuPromoTransformer._parse_json_order(item, fetched_at)
                    if record:
                        records.append(record)
                return records
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass

        # Try CSV
        try:
            records = OzonSkuPromoTransformer._parse_csv_orders(report_data, encoding, fetched_at)
        except Exception:
            pass

        return records

    @staticmethod
    def _parse_json_order(item: Dict[str, Any], fetched_at: str) -> Optional[Dict[str, Any]]:
        """
        Parse single order from JSON response.

        API fields mapping:
        - date: "24.01.2026" (DD.MM.YYYY format)
        - orderId: order ID
        - orderNumber: order number
        - sku: SKU товара
        - advSku: SKU продвигаемого товара
        - offerId: артикул
        - title: название товара
        - ordersSource: источник заказа ("Кампания за клики", etc.)
        - quantity: количество (string "1")
        - price: стоимость товара ("1818,00")
        - cost: та же стоимость
        - bid: ставка % ("10,00")
        - bidValue: ставка в рублях ("181,80")
        - moneySpent: расход на продвижение ("181,80")
        """
        # Extract and normalize date (DD.MM.YYYY -> YYYY-MM-DD)
        order_date = item.get('date', '')
        if order_date:
            # Handle DD.MM.YYYY format
            if '.' in order_date and len(order_date) == 10:
                try:
                    parts = order_date.split('.')
                    order_date = f"{parts[2]}-{parts[1]}-{parts[0]}"
                except (IndexError, ValueError):
                    pass
            # Handle ISO format with time
            elif 'T' in order_date:
                order_date = order_date.split('T')[0]

        # Parse numeric values (handle comma decimal separator)
        def parse_num(val):
            if val is None:
                return 0
            if isinstance(val, (int, float)):
                return val
            try:
                return float(str(val).replace(',', '.').replace(' ', ''))
            except (ValueError, TypeError):
                return 0

        return {
            'date': order_date,
            'order_id': item.get('orderId') or item.get('order_id'),
            'order_number': item.get('orderNumber') or item.get('order_number'),
            'sku': str(item.get('sku', '')),
            'promoted_sku': str(item.get('advSku', '') or item.get('promotedSku', '') or item.get('promoted_sku', '')),
            'article': item.get('offerId', '') or item.get('article', ''),
            'product_name': item.get('title', '') or item.get('name', '') or item.get('productName', ''),
            'order_source': item.get('ordersSource', '') or item.get('orderSource', '') or item.get('source', ''),
            'quantity': int(parse_num(item.get('quantity', 1))),
            'price': parse_num(item.get('price', 0)),
            'rate_percent': parse_num(item.get('bid', 0) or item.get('ratePercent', 0)),
            'rate_rub': parse_num(item.get('bidValue', 0) or item.get('rateRub', 0)),
            'cost': parse_num(item.get('moneySpent', 0) or item.get('cost', 0) or item.get('expense', 0)),
            'fetched_at': fetched_at
        }

    @staticmethod
    def _parse_csv_orders(
        csv_data: bytes,
        encoding: str,
        fetched_at: str
    ) -> List[Dict[str, Any]]:
        """Parse orders from CSV format"""
        csv_text = csv_data.decode(encoding)
        lines = csv_text.strip().split('\n')

        if len(lines) < 2:
            return []

        # Use DictReader
        csv_reader = csv.DictReader(io.StringIO(csv_text), delimiter=';')

        records = []
        for row in csv_reader:
            # Parse date
            date_str = row.get('Дата', '').strip()
            if date_str:
                try:
                    date_obj = datetime.strptime(date_str, '%d.%m.%Y')
                    date_normalized = date_obj.strftime('%Y-%m-%d')
                except ValueError:
                    date_normalized = date_str
            else:
                date_normalized = None

            record = {
                'date': date_normalized,
                'order_id': row.get('ID заказа', '').strip(),
                'order_number': row.get('Номер заказа', '').strip(),
                'sku': row.get('SKU', '').strip(),
                'promoted_sku': row.get('SKU продвигаемого товара', '').strip(),
                'article': row.get('Артикул', '').strip(),
                'product_name': row.get('Наименование', '').strip(),
                'order_source': row.get('Источник заказа', '').strip(),
                'quantity': OzonPerformanceTransformer._parse_int(row.get('Количество', '0')),
                'price': OzonPerformanceTransformer._parse_decimal(row.get('Стоимость, ₽', '0')),
                'rate_percent': OzonPerformanceTransformer._parse_decimal(row.get('Ставка, %', '0')),
                'rate_rub': OzonPerformanceTransformer._parse_decimal(row.get('Ставка, ₽', '0')),
                'cost': OzonPerformanceTransformer._parse_decimal(row.get('Расход, ₽', '0')),
                'fetched_at': fetched_at
            }

            if record['sku']:
                records.append(record)

        return records

    @staticmethod
    def aggregate_by_sku_date(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Aggregate orders by SKU and date for dashboard

        Args:
            records: List of individual order records

        Returns:
            Aggregated records by SKU+date with totals
        """
        from collections import defaultdict

        aggregated = defaultdict(lambda: {
            'orders': 0,
            'quantity': 0,
            'revenue': 0.0,
            'cost': 0.0,
            'product_name': ''
        })

        for r in records:
            if not r.get('date') or not r.get('sku'):
                continue

            key = (r['date'], r['sku'])
            agg = aggregated[key]

            agg['orders'] += 1
            agg['quantity'] += r.get('quantity', 0) or 0
            agg['revenue'] += r.get('price', 0) or 0
            agg['cost'] += r.get('cost', 0) or 0
            if not agg['product_name']:
                agg['product_name'] = r.get('product_name', '')

        result = []
        fetched_at = datetime.utcnow().isoformat()

        for (date, sku), data in aggregated.items():
            result.append({
                'date': date,
                'sku': sku,
                'product_name': data['product_name'],
                'orders': data['orders'],
                'quantity': data['quantity'],
                'revenue': data['revenue'],
                'cost': data['cost'],
                'drr': round((data['cost'] / data['revenue'] * 100), 2) if data['revenue'] > 0 else 0,
                'fetched_at': fetched_at
            })

        return result

    @staticmethod
    def validate_record(record: Dict[str, Any]) -> bool:
        """Validate that record has required fields"""
        required_fields = ['date', 'sku']
        return all(field in record and record[field] for field in required_fields)