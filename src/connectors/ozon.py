"""Ozon API connector (placeholder for future implementation)"""

from typing import Dict, List, Any
from datetime import date

from src.core.base_connector import BaseConnector
from src.core.exceptions import OzonConnectorError


class OzonConnector(BaseConnector):
    """
    Ozon API connector (placeholder).
    
    This connector will be implemented when Ozon integration is required.
    """
    
    # Define Ozon API endpoints here when ready
    # PRODUCTS_URL = "https://api-seller.ozon.ru/v1/products/..."
    # ORDERS_URL = "https://api-seller.ozon.ru/v1/orders/..."
    
    def __init__(self, api_key: str, client_id: str = "", timeout: float = 60.0):
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
        self.log_info("Ozon connector initialized (placeholder)")
    
    def fetch_data(
        self,
        date_from: date,
        date_to: date,
        endpoint: str,
        **kwargs
    ) -> Dict[str, Any] | List[Dict[str, Any]]:
        """
        Fetch data from Ozon API
        
        This method will be implemented when Ozon integration is ready.
        """
        raise NotImplementedError(
            f"Ozon connector.fetch_data() is not implemented yet for endpoint: {endpoint}. "
            "Please implement this method or contact the development team."
        )
    
    def validate_connection(self) -> bool:
        """
        Validate Ozon API connection
        
        This method will be implemented when Ozon integration is ready.
        """
        raise NotImplementedError(
            "Ozon connector.validate_connection() is not implemented yet. "
            "Please implement this method or contact the development team."
        )
