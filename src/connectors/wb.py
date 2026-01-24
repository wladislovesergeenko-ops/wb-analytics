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

    # New endpoints
    TARIFFS_COMMISSION_URL = "https://common-api.wildberries.ru/api/v1/tariffs/commission"
    SEARCH_REPORT_URL = "https://seller-analytics-api.wildberries.ru/api/v2/search-report/report"
    SEARCH_TEXTS_URL = "https://seller-analytics-api.wildberries.ru/api/v2/search-report/product/search-texts"
    NORMQUERY_STATS_URL = "https://advert-api.wildberries.ru/adv/v0/normquery/stats"
    
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

    # ==================== NEW METHODS ====================

    @retry_on_exception(exception_types=(httpx.HTTPError,), max_retries=3, delay_seconds=5)
    def fetch_tariffs_commission(self, locale: str = "ru") -> Dict[str, Any]:
        """
        Fetch commission tariffs for all categories.
        Rate limit: 1 request/min

        Args:
            locale: Response language ('ru', 'en', 'zh')

        Returns:
            API response with commission data by subject
        """
        headers = self._get_headers()
        params = {"locale": locale}

        with httpx.Client(timeout=self.timeout) as client:
            r = client.get(self.TARIFFS_COMMISSION_URL, headers=headers, params=params)
            r.raise_for_status()
            self.log_info(f"Fetched tariffs commission data (locale={locale})")
            return r.json()

    @retry_on_exception(exception_types=(httpx.HTTPError,), max_retries=3, delay_seconds=5)
    def fetch_search_report(
        self,
        start: str,
        end: str,
        *,
        past_start: str = None,
        past_end: str = None,
        nm_ids: List[int] = None,
        subject_ids: List[int] = None,
        brand_names: List[str] = None,
        tag_ids: List[int] = None,
        position_cluster: str = "all",
        order_by_field: str = "avgPosition",
        order_by_mode: str = "asc",
        limit: int = 1000,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        Fetch search report with product positions and metrics.
        Rate limit: 3 requests/min (20 sec interval)

        Args:
            start: Current period start date 'YYYY-MM-DD'
            end: Current period end date 'YYYY-MM-DD'
            past_start: Past period start date for comparison (optional)
            past_end: Past period end date for comparison (optional)
            nm_ids: Filter by product IDs (optional)
            subject_ids: Filter by subject IDs (optional)
            brand_names: Filter by brand names (optional)
            tag_ids: Filter by tag IDs (optional)
            position_cluster: 'all', 'firstHundred', 'secondHundred', 'below'
            order_by_field: Sort field ('avgPosition', 'openCard', 'addToCart', 'orders', etc.)
            order_by_mode: Sort direction ('asc', 'desc')
            limit: Max items per response (max 1000)
            offset: Items to skip for pagination

        Returns:
            API response with groups data
        """
        headers = self._get_headers()

        payload = {
            "currentPeriod": {"start": start, "end": end},
            "positionCluster": position_cluster,
            "orderBy": {"field": order_by_field, "mode": order_by_mode},
            "limit": limit,
            "offset": offset,
        }

        # Optional past period for dynamics calculation
        if past_start and past_end:
            payload["pastPeriod"] = {"start": past_start, "end": past_end}

        # Optional filters
        if nm_ids:
            payload["nmIds"] = nm_ids
        if subject_ids:
            payload["subjectIds"] = subject_ids
        if brand_names:
            payload["brandNames"] = brand_names
        if tag_ids:
            payload["tagIds"] = tag_ids

        with httpx.Client(timeout=self.timeout) as client:
            r = client.post(self.SEARCH_REPORT_URL, headers=headers, json=payload)
            r.raise_for_status()
            self.log_info(f"Fetched search report: {start} -> {end}, offset={offset}, limit={limit}")
            return r.json()

    def fetch_search_report_all(
        self,
        start: str,
        end: str,
        *,
        past_start: str = None,
        past_end: str = None,
        nm_ids: List[int] = None,
        subject_ids: List[int] = None,
        brand_names: List[str] = None,
        tag_ids: List[int] = None,
        position_cluster: str = "all",
        order_by_field: str = "avgPosition",
        order_by_mode: str = "asc",
        sleep_seconds: float = 21,
    ) -> List[Dict[str, Any]]:
        """
        Fetch ALL search report data with automatic pagination.

        Returns:
            List of all groups from all pages
        """
        import time

        all_groups = []
        offset = 0
        limit = 1000
        page = 1

        while True:
            response = self.fetch_search_report(
                start=start,
                end=end,
                past_start=past_start,
                past_end=past_end,
                nm_ids=nm_ids,
                subject_ids=subject_ids,
                brand_names=brand_names,
                tag_ids=tag_ids,
                position_cluster=position_cluster,
                order_by_field=order_by_field,
                order_by_mode=order_by_mode,
                limit=limit,
                offset=offset,
            )

            groups = response.get("data", {}).get("groups", [])
            if not groups:
                break

            all_groups.extend(groups)
            self.log_info(f"Search report page {page}: got {len(groups)} groups, total {len(all_groups)}")

            if len(groups) < limit:
                break

            offset += limit
            page += 1
            time.sleep(sleep_seconds)

        return all_groups

    @retry_on_exception(exception_types=(httpx.HTTPError,), max_retries=3, delay_seconds=5)
    def fetch_product_search_texts(
        self,
        nm_ids: List[int],
        start: str,
        end: str,
        *,
        past_start: str = None,
        past_end: str = None,
        top_order_by: str = "openToCart",
        order_by_field: str = "avgPosition",
        order_by_mode: str = "asc",
        limit: int = 30,
    ) -> Dict[str, Any]:
        """
        Fetch top search queries for specific products.
        Rate limit: 3 requests/min (20 sec interval)

        Args:
            nm_ids: Product IDs (max 50 per request)
            start: Current period start date 'YYYY-MM-DD'
            end: Current period end date 'YYYY-MM-DD'
            past_start: Past period start date (optional)
            past_end: Past period end date (optional)
            top_order_by: Metric for top queries ('openCard', 'addToCart', 'openToCart', 'orders', 'cartToOrder')
            order_by_field: Sort field
            order_by_mode: Sort direction ('asc', 'desc')
            limit: Max queries per product (30 standard, 100 advanced tier)

        Returns:
            API response with search texts data
        """
        if len(nm_ids) > 50:
            raise WBConnectorError("fetch_product_search_texts: max 50 nmIds per request")

        headers = self._get_headers()

        payload = {
            "currentPeriod": {"start": start, "end": end},
            "nmIds": nm_ids,
            "topOrderBy": top_order_by,
            "orderBy": {"field": order_by_field, "mode": order_by_mode},
            "limit": limit,
        }

        if past_start and past_end:
            payload["pastPeriod"] = {"start": past_start, "end": past_end}

        with httpx.Client(timeout=self.timeout) as client:
            r = client.post(self.SEARCH_TEXTS_URL, headers=headers, json=payload)
            r.raise_for_status()
            self.log_info(f"Fetched product search texts: {len(nm_ids)} products, {start} -> {end}")
            return r.json()

    def fetch_product_search_texts_chunked(
        self,
        nm_ids: List[int],
        start: str,
        end: str,
        *,
        past_start: str = None,
        past_end: str = None,
        top_order_by: str = "openToCart",
        order_by_field: str = "avgPosition",
        order_by_mode: str = "asc",
        limit: int = 30,
        chunk_size: int = 50,
        sleep_seconds: float = 21,
    ) -> List[Dict[str, Any]]:
        """
        Fetch search texts for many products with chunking (max 50 per request).

        Returns:
            List of all items from all chunks
        """
        import time

        if not nm_ids:
            return []

        all_items = []
        chunks = self._chunked(nm_ids, chunk_size)

        for idx, chunk in enumerate(chunks, start=1):
            response = self.fetch_product_search_texts(
                nm_ids=chunk,
                start=start,
                end=end,
                past_start=past_start,
                past_end=past_end,
                top_order_by=top_order_by,
                order_by_field=order_by_field,
                order_by_mode=order_by_mode,
                limit=limit,
            )

            items = response.get("data", {}).get("items", [])
            all_items.extend(items)
            self.log_info(f"Search texts chunk {idx}/{len(chunks)}: got {len(items)} items, total {len(all_items)}")

            if idx < len(chunks):
                time.sleep(sleep_seconds)

        return all_items

    # ==================== NORMQUERY STATS ====================

    @retry_on_exception(exception_types=(httpx.HTTPError,), max_retries=3, delay_seconds=5)
    def fetch_normquery_stats(
        self,
        items: List[Dict[str, int]],
        date_start: str,
        date_end: str,
    ) -> List[Dict[str, Any]]:
        """
        Fetch normquery (search cluster) statistics for advert+nm pairs.
        Rate limit: 5 requests/sec (200ms interval)

        Args:
            items: List of {"advert_id": int, "nm_id": int} dicts (max 100 per request)
            date_start: Start date 'YYYY-MM-DD'
            date_end: End date 'YYYY-MM-DD' (max 31 days from start)

        Returns:
            List of cluster statistics
        """
        if not items:
            return []

        if len(items) > 100:
            raise WBConnectorError("fetch_normquery_stats: max 100 items per request")

        headers = self._get_headers()
        # API expects: from, to, items[{advert_id, nm_id}]
        payload = {
            "from": date_start,
            "to": date_end,
            "items": items,
        }

        with httpx.Client(timeout=self.timeout) as client:
            r = client.post(self.NORMQUERY_STATS_URL, headers=headers, json=payload)
            r.raise_for_status()

            data = r.json()
            # API returns: {"stats": [{"advert_id": X, "nm_id": Y, "stats": [...]}, ...]}
            # We need to flatten the nested structure
            outer_stats = data.get("stats") or [] if isinstance(data, dict) else []

            # Flatten: extract inner stats and add advert_id/nm_id to each
            flattened: List[Dict[str, Any]] = []
            for item in outer_stats:
                advert_id = item.get("advert_id")
                nm_id = item.get("nm_id")
                inner_stats = item.get("stats") or []
                for cluster in inner_stats:
                    cluster["advert_id"] = advert_id
                    cluster["nm_id"] = nm_id
                    flattened.append(cluster)

            self.log_info(f"Fetched normquery stats: {len(items)} items, {len(flattened)} clusters")
            return flattened

    def fetch_normquery_stats_chunked(
        self,
        items: List[Dict[str, int]],
        date_start: str,
        date_end: str,
        *,
        chunk_size: int = 100,
        sleep_seconds: float = 0.25,
    ) -> List[Dict[str, Any]]:
        """
        Fetch normquery stats for many advert+nm pairs with chunking.
        Rate limit: 5 requests/sec (200ms interval)

        Args:
            items: List of {"advert_id": int, "nm_id": int} dicts
            date_start: Start date 'YYYY-MM-DD'
            date_end: End date 'YYYY-MM-DD'
            chunk_size: Max items per request (API limit is 100)
            sleep_seconds: Sleep between requests

        Returns:
            Combined list of all cluster stats
        """
        import time

        if not items:
            return []

        all_stats: List[Dict[str, Any]] = []
        chunks = [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]

        for idx, chunk in enumerate(chunks, start=1):
            try:
                stats = self.fetch_normquery_stats(chunk, date_start, date_end)
                all_stats.extend(stats)
                self.log_info(
                    f"Normquery stats chunk {idx}/{len(chunks)}: {len(chunk)} items, {len(stats)} clusters"
                )
            except httpx.HTTPStatusError as e:
                self.log_error(f"Failed to fetch normquery stats chunk {idx}: {e}")
            except Exception as e:
                self.log_error(f"Unexpected error in normquery stats chunk {idx}: {e}")

            # Rate limiting
            if idx < len(chunks):
                time.sleep(sleep_seconds)

        return all_stats
