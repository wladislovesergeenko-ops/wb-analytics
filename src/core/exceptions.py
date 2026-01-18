"""Custom exceptions for ETL application"""


class ETLException(Exception):
    """Base exception for ETL operations"""
    pass


class WBConnectorError(ETLException):
    """Wildberries API connector error"""
    pass


class OzonConnectorError(ETLException):
    """Ozon API connector error"""
    pass


class SupabaseError(ETLException):
    """Supabase database error"""
    pass


class TransformationError(ETLException):
    """Data transformation error"""
    pass


class ValidationError(ETLException):
    """Data validation error"""
    pass
