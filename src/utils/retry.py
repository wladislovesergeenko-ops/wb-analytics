import time
import functools
from typing import Callable, Type, Tuple, Optional
from src.logging_config.logger import setup_logger

logger = setup_logger(__name__)


def retry_on_exception(
    exception_types: Tuple[Type[Exception], ...] = (Exception,),
    max_retries: int = 3,
    delay_seconds: float = 5,
    backoff_factor: float = 1.0,
) -> Callable:
    """
    Декоратор для повторного выполнения функции при ошибке
    
    Args:
        exception_types: Кортеж типов исключений, на которые нужно реагировать
        max_retries: Максимальное количество попыток
        delay_seconds: Задержка между попытками в секундах
        backoff_factor: Множитель для экспоненциального увеличения задержки (1.0 = без увеличения)
    
    Returns:
        Декоратор функции
    
    Пример:
        @retry_on_exception(exception_types=(httpx.HTTPError,), max_retries=3, delay_seconds=5)
        def fetch_data():
            return client.get(url)
    """
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            current_delay = delay_seconds
            
            while attempt < max_retries:
                try:
                    return func(*args, **kwargs)
                except exception_types as e:
                    attempt += 1
                    
                    if attempt >= max_retries:
                        logger.error(
                            f"Failed after {max_retries} attempts in {func.__name__}: {e}",
                            exc_info=True
                        )
                        raise
                    
                    logger.warning(
                        f"Attempt {attempt}/{max_retries} failed in {func.__name__}. "
                        f"Retrying in {current_delay:.1f}s... Error: {e}"
                    )
                    
                    time.sleep(current_delay)
                    current_delay *= backoff_factor
        
        return wrapper
    return decorator
