"""Base connector class for all marketplace integrations"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from datetime import date
from src.logging_config.logger import setup_logger


class BaseConnector(ABC):
    """
    Abstract base class for marketplace connectors.
    Subclasses must implement fetch_data and validate_connection methods.
    """
    
    def __init__(self, api_key: str, marketplace_name: str):
        """
        Initialize connector
        
        Args:
            api_key: API key for marketplace
            marketplace_name: Name of marketplace (e.g., 'wildberries', 'ozon')
        """
        self.api_key = api_key
        self.marketplace_name = marketplace_name
        self.logger = setup_logger(f"{self.__class__.__module__}.{self.__class__.__name__}")
    
    @abstractmethod
    def fetch_data(
        self,
        date_from: date,
        date_to: date,
        endpoint: str,
        **kwargs
    ) -> Dict[str, Any] | List[Dict[str, Any]]:
        """
        Fetch data from marketplace API for a given period
        
        Args:
            date_from: Start date
            date_to: End date
            endpoint: Specific endpoint/method name
            **kwargs: Additional parameters specific to the endpoint
        
        Returns:
            Raw data from API (dict or list of dicts)
        
        Raises:
            WBConnectorError or OzonConnectorError on failure
        """
        pass
    
    @abstractmethod
    def validate_connection(self) -> bool:
        """
        Validate that API connection is working
        
        Returns:
            True if connection is valid, False otherwise
        """
        pass
    
    def log_info(self, message: str):
        """Log info message"""
        self.logger.info(f"[{self.marketplace_name}] {message}")
    
    def log_warning(self, message: str):
        """Log warning message"""
        self.logger.warning(f"[{self.marketplace_name}] {message}")
    
    def log_error(self, message: str, exc_info: bool = False):
        """Log error message"""
        self.logger.error(f"[{self.marketplace_name}] {message}", exc_info=exc_info)
