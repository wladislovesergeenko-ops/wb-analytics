"""Wildberries API connector"""

from typing import Dict, List, Any
from datetime import date
import httpx

from src.core.base_connector import BaseConnector
from src.core.exceptions import WBConnectorError
from src.utils.retry import retry_on_exception


class WBConnector(BaseConnector):
    """
    Wildberries API connector.
    Handles all API endpoints for Wildberries data extraction.
    """
    
    # API endpoints
    SALES_FUNNEL_URL = "https://seller-analytics-api.wildberries.ru/api/analytics/v3/sales-funnel/products"
    ADVERTS_URL = "https://advert-api.wildberries.ru/api/advert/v2/adverts"
    FULLSTATS_URL = "https://advert-api.wildberries.ru/adv/v3/fullstats"
    ORDERS_URL = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
    
    def __init__(self, api_key: str, timeout: float = 60.0):
        """
        Initialize WB connector
        
        Args:
            api_key: Wildberries API key
            timeout: HTTP request timeout in seconds
        """
        super().__init__(api_key, "wildberries")
        self.timeout = timeout
    
    def fetch_data(self, date_from: date, date_to: date, endpoint: str, **kwargs) -> Dict[str, Any] | List[Dict[str, Any]]:
        """
        Fetch data from Wildberries API
        
        Args:
            date_from: Start date
            date_to: End date
            endpoint: Endpoint type ('sales_funnel', 'adverts', 'fullstats', 'orders')
            **kwargs: Additional parameters for specific endpoint
        
        Returns:
            Raw API response
        """
        if endpoint == "sales_funnel":
            return self.fetch_sales_funnel(date_from.isoformat(), date_to.isoformat())
        elif endpoint == "adverts":
            return self.fetch_adverts()
        elif endpoint == "fullstats":
            advert_ids = kwargs.get("advert_ids", [])
            chunk_size = kwargs.get("chunk_size", 50)
            sleep_seconds = kwargs.get("sleep_seconds", 15)
            return self.fetch_fullstats_chunked(
                advert_ids=advert_ids,
                begin_date=date_from.isoformat(),
                end_date=date_to.isoformat(),
                chunk_size=chunk_size,
                sleep_seconds=sleep_seconds,
            )
        elif endpoint == "orders":
            return self.fetch_orders(date_from.isoformat(), kwargs.get("flag", 1))
        else:
            raise WBConnectorError(f"Unknown endpoint: {endpoint}")
    
    @retry_on_exception(exception_types=(httpx.HTTPError,), max_retries=3, delay_seconds=5)
    def fetch_sales_funnel(self, start: str, end: str) -> Dict[str, Any]:
        """
        Fetch sales funnel data (ОТОМ данные за период)
        
        Args:
            start: Start date in 'YYYY-MM-DD' format
            end: End date in 'YYYY-MM-DD' format
        
        Returns:
            API response with sales funnel data
        """
        headers = self._get_headers()
        payload = {"selectedPeriod": {"start": start, "end": end}}
        
        with httpx.Client(timeout=self.timeout) as client:
            r = client.post(self.SALES_FUNNEL_URL, headers=headers, json=payload)
            r.raise_for_status()
            self.log_info(f"Fetched sales funnel data: {start} -> {end}")
            return r.json()
    
    @retry_on_exception(exception_types=(httpx.HTTPError,), max_retries=3, delay_seconds=5)
    def fetch_adverts(self) -> Dict[str, Any]:
        """
        Fetch all active adverts
        
        Returns:
            API response with adverts data
        """
        headers = self._get_headers()
        
        with httpx.Client(timeout=self.timeout) as client:
            r = client.get(self.ADVERTS_URL, headers=headers)
            r.raise_for_status()
            self.log_info("Fetched adverts data")
            return r.json()
    
    def fetch_fullstats_chunked(
        self,
        advert_ids: List[int],
        begin_date: str,
        end_date: str,
        *,
        chunk_size: int = 50,
        sleep_seconds: float = 15,
    ) -> List[Dict[str, Any]]:
        """
        Fetch fullstats data for given advert IDs (chunked to respect API limits)
        
        Args:
            advert_ids: List of advert IDs to fetch
            begin_date: Start date in 'YYYY-MM-DD' format
            end_date: End date in 'YYYY-MM-DD' format
            chunk_size: Number of IDs per request
            sleep_seconds: Sleep time between requests
        
        Returns:
            List of API responses
        """
        import time
        
        if not advert_ids:
            self.log_info("No advert IDs provided for fullstats")
            return []
        
        headers = self._get_headers()
        results: List[Dict[str, Any]] = []
        
        chunks = self._chunked(advert_ids, chunk_size)
        
        with httpx.Client(timeout=self.timeout) as client:
            for idx, chunk in enumerate(chunks, start=1):
                ids_param = ",".join(str(x) for x in chunk)
                params = {"ids": ids_param, "beginDate": begin_date, "endDate": end_date}
                
                self.log_info(f"Fetching fullstats chunk {idx}/{len(chunks)}: {len(chunk)} IDs, range {begin_date}->{end_date}")
                
                try:
                    r = client.get(self.FULLSTATS_URL, headers=headers, params=params)
                    r.raise_for_status()
                    
                    payload = r.json()
                    if payload:
                        results.extend(payload if isinstance(payload, list) else [payload])
                    
                except httpx.HTTPError as e:
                    self.log_error(f"Failed to fetch fullstats chunk {idx}: {e}", exc_info=True)
                    raise
                
                # Rate limiting
                if idx < len(chunks):
                    time.sleep(sleep_seconds)
        
        return results
    
    @retry_on_exception(exception_types=(httpx.HTTPError,), max_retries=3, delay_seconds=5)
    def fetch_orders(self, date_from: str, flag: int = 1) -> List[Dict[str, Any]]:
        """
        Fetch order statistics (для SPP snapshot)
        
        Args:
            date_from: Date in 'YYYY-MM-DD' format
            flag: WB flag parameter
        
        Returns:
            List of orders data
        """
        headers = {"Authorization": self.api_key, "Accept": "application/json"}
        params = {"dateFrom": date_from, "flag": flag}
        
        with httpx.Client(timeout=self.timeout) as client:
            r = client.get(self.ORDERS_URL, headers=headers, params=params)
            r.raise_for_status()
            self.log_info(f"Fetched orders data for date: {date_from}")
            return r.json() or []
    
    def validate_connection(self) -> bool:
        """
        Validate WB API connection by making a test request
        
        Returns:
            True if connection is valid, False otherwise
        """
        try:
            self.log_info("Validating connection...")
            
            # Try fetching adverts as a simple connection test
            headers = self._get_headers()
            with httpx.Client(timeout=10.0) as client:
                r = client.get(self.ADVERTS_URL, headers=headers)
                r.raise_for_status()
            
            self.log_info("Connection validated successfully")
            return True
        except Exception as e:
            self.log_error(f"Connection validation failed: {e}", exc_info=True)
            return False
    
    def _get_headers(self) -> Dict[str, str]:
        """Get standard headers for WB API requests"""
        return {
            "Authorization": self.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
    
    @staticmethod
    def _chunked(seq: List[int], size: int) -> List[List[int]]:
        """Split list into chunks"""
        return [seq[i : i + size] for i in range(0, len(seq), size)]
