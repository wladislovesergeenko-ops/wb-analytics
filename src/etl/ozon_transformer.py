"""Ozon data transformers"""

from typing import List, Dict, Any
from datetime import datetime


class OzonTransformer:
    """Transform Ozon API responses into normalized format for database storage"""
    
    @staticmethod
    def transform_analytics_data(
        raw_data: Dict[str, Any],
        metrics_order: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Transform analytics data response into flat records
        
        Args:
            raw_data: Raw API response from Ozon analytics endpoint
            metrics_order: Order of metrics in the request (e.g., ['revenue', 'ordered_units'])
        
        Returns:
            List of normalized records ready for database insertion
            
        Example input:
            {
                "result": {
                    "data": [
                        {
                            "dimensions": [
                                {"id": "2026-01-08", "name": ""},
                                {"id": "1418756574", "name": "Товар название"}
                            ],
                            "metrics": [1500.50, 5]
                        }
                    ]
                }
            }
            
        Example output:
            [
                {
                    "date": "2026-01-08",
                    "sku": "1418756574",
                    "product_name": "Товар название",
                    "revenue": 1500.50,
                    "ordered_units": 5,
                    "fetched_at": "2026-01-18T15:30:00"
                }
            ]
        """
        if "result" not in raw_data or "data" not in raw_data["result"]:
            return []
        
        records = raw_data["result"]["data"]
        transformed = []
        fetched_at = datetime.utcnow().isoformat()
        
        for record in records:
            dimensions = record.get("dimensions", [])
            metrics = record.get("metrics", [])
            
            # Extract dimensions
            # dimensions[0] = date, dimensions[1] = sku
            if len(dimensions) < 2:
                continue
            
            date_dimension = dimensions[0]
            sku_dimension = dimensions[1]
            
            # Build base record
            transformed_record = {
                "date": date_dimension.get("id", ""),
                "sku": sku_dimension.get("id", ""),
                "product_name": sku_dimension.get("name", ""),
                "fetched_at": fetched_at
            }
            
            # Map metrics to their names
            for idx, metric_name in enumerate(metrics_order):
                if idx < len(metrics):
                    transformed_record[metric_name] = metrics[idx]
                else:
                    transformed_record[metric_name] = None
            
            transformed.append(transformed_record)
        
        return transformed
    
    @staticmethod
    def validate_record(record: Dict[str, Any]) -> bool:
        """
        Validate that a record has required fields
        
        Args:
            record: Transformed record
        
        Returns:
            True if valid, False otherwise
        """
        required_fields = ["date", "sku"]
        return all(field in record and record[field] for field in required_fields)