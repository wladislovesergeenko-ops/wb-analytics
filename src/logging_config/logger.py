import logging
import logging.handlers
from pathlib import Path
from typing import Optional

# Глобальный конфиг для логирования
_log_config = {
    "level": "INFO",
    "log_dir": "logs",
    "log_to_file": True,
}


def configure_logging(log_level: str = "INFO", log_dir: str = "logs", log_to_file: bool = True):
    """Настроить глобальные параметры логирования"""
    global _log_config
    _log_config.update({
        "level": log_level,
        "log_dir": log_dir,
        "log_to_file": log_to_file,
    })


def setup_logger(name: str, log_level: Optional[str] = None) -> logging.Logger:
    """
    Создать и настроить логгер для модуля
    
    Args:
        name: Имя логгера (обычно __name__)
        log_level: Уровень логирования (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    
    Returns:
        Настроенный логгер
    """
    logger = logging.getLogger(name)
    
    # Избежать добавления дубликатов handlers
    if logger.handlers:
        return logger
    
    # Использовать переданный уровень или глобальный
    level_str = log_level or _log_config["level"]
    logger.setLevel(getattr(logging, level_str.upper(), logging.INFO))
    
    # Формат логов
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Console handler (всегда)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, level_str.upper(), logging.INFO))
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (опционально)
    if _log_config["log_to_file"]:
        log_dir = Path(_log_config["log_dir"])
        log_dir.mkdir(exist_ok=True, parents=True)
        
        log_file = log_dir / "app.log"
        
        try:
            file_handler = logging.handlers.RotatingFileHandler(
                str(log_file),
                maxBytes=10_000_000,  # 10 MB
                backupCount=5,
                encoding="utf-8",
            )
            file_handler.setLevel(getattr(logging, level_str.upper(), logging.INFO))
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        except Exception as e:
            logger.warning(f"Failed to setup file logger: {e}")
    
    # Не передавать вверх по иерархии
    logger.propagate = False
    
    return logger
