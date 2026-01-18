"""Ozon API connector"""

from typing import Dict, List, Any, Optional
from datetime import date
import httpx

from src.core.base_connector import BaseConnector
from src.core.exceptions import OzonConnectorError
from src.utils.retry import retry_on_exception


class OzonConnector(BaseConnector):
    """
    Ozon API connector.
    Handles all API endpoints for Ozon data extraction.
    """
    
    # API endpoints
    BASE_URL = "https://api-seller.ozon.ru"
    ANALYTICS_DATA_URL = f"{BASE_URL}/v1/analytics/data"
    
    def __init__(self, api_key: str, client_id: str, timeout: float = 60.0):
        """
        Initialize Ozon connector
        
        Args:
            api_key: Ozon API key
            client_id: Ozon Client ID
            timeout: HTTP request timeout in seconds
        """
        super().__init__(api_key, "ozon")
        self.client_id = client_id
        self.timeout = timeout
        
        if not self.client_id:
            raise OzonConnectorError("Client ID is required for Ozon API")
        
        self.log_info(f"Ozon connector initialized with Client ID: {self.client_id[:8]}...")
    
    def fetch_data(
        self,
        date_from: date,
        date_to: date,
        endpoint: str,
        **kwargs
    ) -> Dict[str, Any] | List[Dict[str, Any]]:
        """
        Fetch data from Ozon API
        
        Args:
            date_from: Start date
            date_to: End date
            endpoint: Endpoint type ('analytics_data')
            **kwargs: Additional parameters for specific endpoint
        
        Returns:
            Raw API response
        """
        if endpoint == "analytics_data":
            metrics = kwargs.get("metrics", ["revenue", "ordered_units"])
            dimensions = kwargs.get("dimensions", ["day", "sku"])
            filters = kwargs.get("filters", [])
            limit = kwargs.get("limit", 1000)
            offset = kwargs.get("offset", 0)
            
            return self.fetch_analytics_data(
                date_from=date_from.isoformat(),
                date_to=date_to.isoformat(),
                metrics=metrics,
                dimensions=dimensions,
                filters=filters,
                limit=limit,
                offset=offset
            )
        else:
            raise OzonConnectorError(f"Unknown endpoint: {endpoint}")
    
    @retry_on_exception(exception_types=(httpx.HTTPError,), max_retries=3, delay_seconds=5)
    def fetch_analytics_data(
        self,
        date_from: str,
        date_to: str,
        metrics: List[str],
        dimensions: List[str],
        filters: Optional[List[Dict[str, Any]]] = None,
        limit: int = 1000,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        Fetch analytics data from Ozon
        
        Args:
            date_from: Start date in 'YYYY-MM-DD' format
            date_to: End date in 'YYYY-MM-DD' format
            metrics: List of metrics to fetch
            dimensions: List of dimensions for grouping
            filters: Optional filters for the query
            limit: Maximum number of records to return
            offset: Pagination offset
        
        Returns:
            API response with analytics data
            
        Available metrics:
            - revenue: заказано на сумму
            - ordered_units: заказано товаров
            - hits_view_search: показы в поиске и в категории
            - hits_view_pdp: показы на карточке товара
            - hits_view: всего показов
            - hits_tocart_search: в корзину из поиска или категории
            - hits_tocart_pdp: в корзину из карточки товара
            - hits_tocart: всего добавлено в корзину
            - session_view_search: сессии с показом в поиске или каталоге
            - session_view_pdp: сессии с показом на карточке товара
            - session_view: всего сессий
            - delivered_units: доставлено товаров
            - position_category: позиция в поиске и категории
            
        Available dimensions:
            - day: По дням
            - week: По неделям  
            - month: По месяцам
            - sku: По артикулу Ozon
            - category: По категории
        """
        headers = self._get_headers()
        
        payload = {
            "date_from": date_from,
            "date_to": date_to,
            "metrics": metrics,
            "dimension": dimensions,
            "limit": limit,
            "offset": offset
        }
        
        if filters:
            payload["filters"] = filters
        
        self.log_info(
            f"Fetching analytics data: {date_from} -> {date_to}, "
            f"metrics={metrics}, dimensions={dimensions}, limit={limit}, offset={offset}"
        )
        
        with httpx.Client(timeout=self.timeout) as client:
            r = client.post(self.ANALYTICS_DATA_URL, headers=headers, json=payload)
            r.raise_for_status()
            
            response_data = r.json()
            
            # Log summary
            if "result" in response_data and "data" in response_data["result"]:
                records_count = len(response_data["result"]["data"])
                self.log_info(f"Fetched {records_count} analytics records")
            
            return response_data
    
    @retry_on_exception(exception_types=(httpx.HTTPError,), max_retries=3, delay_seconds=5)
    def validate_connection(self) -> bool:
        """
        Validate Ozon API connection by making a test request
        
        Returns:
            True if connection is valid, False otherwise
        """
        try:
            self.log_info("Validating connection...")
            
            # Make a simple analytics request with minimal data
            from datetime import datetime, timedelta
            
            today = datetime.now().date()
            yesterday = today - timedelta(days=1)
            
            headers = self._get_headers()
            payload = {
                "date_from": yesterday.isoformat(),
                "date_to": yesterday.isoformat(),
                "metrics": ["revenue"],
                "dimension": ["day"],
                "limit": 1
            }
            
            with httpx.Client(timeout=10.0) as client:
                r = client.post(self.ANALYTICS_DATA_URL, headers=headers, json=payload)
                r.raise_for_status()
            
            self.log_info("Connection validated successfully")
            return True
            
        except Exception as e:
            self.log_error(f"Connection validation failed: {e}", exc_info=True)
            return False
    
    def _get_headers(self) -> Dict[str, str]:
        """Get standard headers for Ozon API requests"""
        return {
            "Client-Id": self.client_id,
            "Api-Key": self.api_key,
            "Content-Type": "application/json"
        }