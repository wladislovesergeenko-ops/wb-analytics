"""Ozon Performance API connector for advertising analytics"""

from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import httpx
import time

from src.core.base_connector import BaseConnector
from src.core.exceptions import OzonConnectorError
from src.utils.retry import retry_on_exception


class OzonPerformanceConnector(BaseConnector):
    """
    Ozon Performance API connector.
    Handles advertising campaign statistics and product performance.
    """
    
    # API endpoints (NEW HOST since Jan 15, 2025)
    BASE_URL = "https://api-performance.ozon.ru"
    TOKEN_URL = f"{BASE_URL}/api/client/token"
    CAMPAIGNS_URL = f"{BASE_URL}/api/client/campaign"
    CAMPAIGN_PRODUCT_STATS_URL = f"{BASE_URL}/api/client/statistics/campaign/product"
    
    def __init__(self, client_id: str, client_secret: str, timeout: float = 60.0):
        """
        Initialize Ozon Performance connector
        
        Args:
            client_id: Ozon Performance Client ID
            client_secret: Ozon Performance Client Secret
            timeout: HTTP request timeout in seconds
        """
        super().__init__(api_key="", marketplace_name="ozon_performance")
        self.client_id = client_id
        self.client_secret = client_secret
        self.timeout = timeout
        self.access_token: Optional[str] = None
        self.token_expires_at: Optional[datetime] = None
        
        if not self.client_id or not self.client_secret:
            raise OzonConnectorError("Client ID and Client Secret are required for Performance API")
        
        self.log_info("Ozon Performance connector initialized")
    
    def _get_access_token(self) -> str:
        """
        Get or refresh access token
        
        Returns:
            Valid access token
        """
        # Check if we have valid token
        if self.access_token and self.token_expires_at:
            if datetime.now() < self.token_expires_at - timedelta(minutes=5):
                return self.access_token
        
        # Get new token
        self.log_info("Requesting new access token...")
        
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials"
        }
        
        with httpx.Client(timeout=30.0) as client:
            response = client.post(self.TOKEN_URL, json=payload)
            response.raise_for_status()
            
            data = response.json()
            self.access_token = data["access_token"]
            expires_in = data.get("expires_in", 1800)  # default 30 min
            
            self.token_expires_at = datetime.now() + timedelta(seconds=expires_in)
            self.log_info(f"Access token obtained, expires in {expires_in}s")
            
            return self.access_token
    
    def _get_headers(self) -> Dict[str, str]:
        """Get headers with Bearer token"""
        token = self._get_access_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
    
    @retry_on_exception(exception_types=(httpx.HTTPError,), max_retries=3, delay_seconds=5)
    def fetch_campaigns(self) -> Dict[str, Any]:
        """
        Fetch all advertising campaigns
        
        Returns:
            List of campaigns with details
        """
        headers = self._get_headers()
        
        self.log_info("Fetching campaigns...")
        
        with httpx.Client(timeout=self.timeout) as client:
            response = client.get(self.CAMPAIGNS_URL, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            campaigns = data.get("list", [])
            
            self.log_info(f"Fetched {len(campaigns)} campaigns")
            return data
    
    @retry_on_exception(exception_types=(httpx.HTTPError,), max_retries=3, delay_seconds=5)
    def create_statistics_report(
        self,
        campaign_ids: List[str],
        date_from: str,
        date_to: str,
        group_by: str = "DATE"
    ) -> str:
        """
        Create asynchronous statistics report
        
        Args:
            campaign_ids: List of campaign IDs
            date_from: Start date 'YYYY-MM-DD'
            date_to: End date 'YYYY-MM-DD'
            group_by: Grouping ('DATE', 'NO_GROUP_BY')
        
        Returns:
            Report UUID
        """
        headers = self._get_headers()
        
        # Convert to ISO format with timezone
        date_from_iso = f"{date_from}T00:00:00Z"
        date_to_iso = f"{date_to}T23:59:59Z"
        
        payload = {
            "campaigns": campaign_ids,
            "from": date_from_iso,
            "to": date_to_iso,
            "groupBy": group_by
        }
        
        self.log_info(
            f"Creating statistics report: {len(campaign_ids)} campaigns, "
            f"{date_from} -> {date_to}"
        )
        
        stats_url = f"{self.BASE_URL}/api/client/statistics"
        
        with httpx.Client(timeout=self.timeout) as client:
            response = client.post(stats_url, headers=headers, json=payload)
            response.raise_for_status()
            
            data = response.json()
            uuid = data.get("UUID")
            
            self.log_info(f"Report created, UUID: {uuid}")
            return uuid

    def check_report_status(self, uuid: str) -> Dict[str, Any]:
        """
        Check report generation status
        
        Args:
            uuid: Report UUID
        
        Returns:
            Report status info
        """
        headers = self._get_headers()
        status_url = f"{self.BASE_URL}/api/client/statistics/{uuid}"
        
        with httpx.Client(timeout=self.timeout) as client:
            response = client.get(status_url, headers=headers)
            response.raise_for_status()
            
            return response.json()

    def download_report(self, uuid: str) -> bytes:
        """
        Download ready report
        
        Args:
            uuid: Report UUID
        
        Returns:
            Report data (CSV/ZIP)
        """
        headers = self._get_headers()
        download_url = f"{self.BASE_URL}/api/client/statistics/report?UUID={uuid}"
        
        self.log_info(f"Downloading report {uuid}...")
        
        with httpx.Client(timeout=self.timeout) as client:
            response = client.get(download_url, headers=headers)
            response.raise_for_status()
            
            self.log_info(f"Report downloaded, size: {len(response.content)} bytes")
            return response.content

    def fetch_campaign_product_stats(
        self,
        campaign_ids: List[str],
        date_from: str,
        date_to: str,
        max_wait_seconds: int = 300,
        poll_interval: int = 10
    ) -> bytes:
        """
        Fetch campaign statistics (full async flow)
        
        Args:
            campaign_ids: List of campaign IDs
            date_from: Start date 'YYYY-MM-DD'
            date_to: End date 'YYYY-MM-DD'
            max_wait_seconds: Maximum time to wait for report
            poll_interval: Seconds between status checks
        
        Returns:
            Report data (CSV/ZIP bytes)
        """
        # Step 1: Create report
        uuid = self.create_statistics_report(campaign_ids, date_from, date_to)
        
        # Step 2: Wait for report to be ready
        start_time = time.time()
        
        while True:
            if time.time() - start_time > max_wait_seconds:
                raise OzonConnectorError(f"Report {uuid} timeout after {max_wait_seconds}s")
            
            status_data = self.check_report_status(uuid)
            state = status_data.get("state", "UNKNOWN")
            
            self.log_info(f"Report {uuid} state: {state}")
            
            if state == "OK":
                # Report is ready
                break
            elif state in ["ERROR", "FAILED"]:
                raise OzonConnectorError(f"Report {uuid} failed with state: {state}")
            
            # Wait before next check
            time.sleep(poll_interval)
        
        # Step 3: Download report
        return self.download_report(uuid)
    
    def fetch_data(
        self,
        date_from: str,
        date_to: str,
        endpoint: str,
        **kwargs
    ) -> Dict[str, Any] | List[Dict[str, Any]]:
        """
        Fetch data from Performance API
        
        Args:
            date_from: Start date
            date_to: End date
            endpoint: Endpoint type ('campaigns', 'campaign_product_stats')
            **kwargs: Additional parameters
        
        Returns:
            Raw API response
        """
        if endpoint == "campaigns":
            return self.fetch_campaigns()
        
        elif endpoint == "campaign_product_stats":
            campaign_ids = kwargs.get("campaign_ids", [])
            if not campaign_ids:
                raise OzonConnectorError("campaign_ids required for campaign_product_stats")
            
            return self.fetch_campaign_product_stats(
                campaign_ids=campaign_ids,
                date_from=date_from,
                date_to=date_to
            )
        
        else:
            raise OzonConnectorError(f"Unknown endpoint: {endpoint}")
    
    def validate_connection(self) -> bool:
        """
        Validate Performance API connection
        
        Returns:
            True if connection is valid
        """
        try:
            self.log_info("Validating Performance API connection...")
            
            # Try to get token and fetch campaigns
            self._get_access_token()
            self.fetch_campaigns()
            
            self.log_info("Connection validated successfully")
            return True
            
        except Exception as e:
            self.log_error(f"Connection validation failed: {e}", exc_info=True)
            return False