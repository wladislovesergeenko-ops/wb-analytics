from pydantic_settings import BaseSettings
from pydantic import Field, field_validator
from typing import Optional


class Settings(BaseSettings):
    """Основной класс конфигурации приложения"""

    # Supabase
    SUPABASE_URL: str
    SUPABASE_SERVICE_ROLE_KEY: Optional[str] = None
    SUPABASE_ANON_KEY: Optional[str] = None

    # Wildberries API
    WB_KEY: str

    # Ozon Seller API (Analytics)
    OZON_API_KEY: Optional[str] = None
    OZON_CLIENT_ID: Optional[str] = None

    # Ozon Performance API (Ads)
    OZON_PERF_CLIENT_ID: Optional[str] = None
    OZON_PERF_CLIENT_SECRET: Optional[str] = None
    
    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_DIR: str = "logs"
    LOG_TO_FILE: bool = True
    
    # Runflags / ETL Pipeline Controls - Wildberries
    RUN_ADVERTS_SETTINGS: bool = True
    RUN_ADVERTS_FULLSTATS: bool = False
    RUN_SALES_FUNNEL: bool = True
    RUN_SPP: bool = False
    RUN_SEARCH_REPORT: bool = False
    RUN_SEARCH_TEXTS: bool = False
    RUN_NORMQUERY_STATS: bool = False

    # Runflags / ETL Pipeline Controls - Ozon
    RUN_OZON_ANALYTICS: bool = False
    RUN_OZON_PERFORMANCE: bool = False

    # Table names - Wildberries
    SALES_FUNNEL_TABLE: str = "wb_sales_funnel_products"
    ADVERTS_TABLE: str = "wb_adverts_nm_settings"
    FULLSTATS_TABLE: str = "wb_adv_fullstats_daily"
    SPP_TABLE: str = "wb_spp_daily"
    SEARCH_REPORT_TABLE: str = "wb_search_report_products"
    SEARCH_TEXTS_TABLE: str = "wb_product_search_texts"
    NORMQUERY_STATS_TABLE: str = "wb_normquery_stats"

    # Table names - Ozon
    OZON_ANALYTICS_TABLE: str = "ozon_analytics_data"
    OZON_PERFORMANCE_TABLE: str = "ozon_campaign_product_stats"
    
    # Timing and batch settings
    SLEEP_SECONDS: int = 21
    FULLSTATS_SLEEP_SECONDS: int = 15
    FULLSTATS_CHUNK_SIZE: int = 50
    NORMQUERY_SLEEP_SECONDS: float = 0.25  # 5 req/sec rate limit
    BATCH_SIZE: int = 500
    OVERLAP_DAYS: int = 2
    SPP_OVERLAP_DAYS: int = 1
    
    # Statuses for campaigns
    ADVERTS_STATUSES: str = "7,9,11"  # 7: paused, 9: active, 11: completed
    FULLSTATS_STATUSES: str = "9,11"  # Only active and completed
    
    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "allow"  # Разрешить дополнительные переменные окружения
    
    @field_validator("SUPABASE_URL")
    @classmethod
    def validate_supabase_url(cls, v):
        if not v:
            raise ValueError("SUPABASE_URL is required")
        if not v.startswith("https://"):
            raise ValueError("SUPABASE_URL must start with https://")
        return v
    
    @field_validator("WB_KEY")
    @classmethod
    def validate_wb_key(cls, v):
        if not v:
            raise ValueError("WB_KEY is required")
        return v
    
    def get_supabase_key(self) -> str:
        """Возвращает ключ для Supabase (SERVICE_ROLE преимущественнее ANON)"""
        return self.SUPABASE_SERVICE_ROLE_KEY or self.SUPABASE_ANON_KEY or ""
    
    def get_adverts_statuses(self) -> list[int]:
        """Парсит строка адвертс статусов в список"""
        try:
            return [int(s.strip()) for s in self.ADVERTS_STATUSES.split(",")]
        except (ValueError, AttributeError):
            return [7, 9, 11]
    
    def get_fullstats_statuses(self) -> list[int]:
        """Парсит строка статусов fullstats в список"""
        try:
            return [int(s.strip()) for s in self.FULLSTATS_STATUSES.split(",")]
        except (ValueError, AttributeError):
            return [9, 11]


# Глобальный экземпляр settings (синглтон)
_settings_instance: Optional[Settings] = None


def get_settings() -> Settings:
    """Получить глобальный экземпляр Settings"""
    global _settings_instance
    if _settings_instance is None:
        _settings_instance = Settings()
    return _settings_instance

