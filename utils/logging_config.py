"""
Centralized logging configuration
"""
import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler
from config import Config


def setup_logging(app=None, log_level=None):
    """
    Setup application-wide logging

    Args:
        app: Flask app instance (optional)
        log_level: Override log level (optional)
    """
    # Get log level from config or parameter
    if log_level is None:
        log_level = getattr(logging, Config.LOG_LEVEL.upper(), logging.INFO)

    # Create logs directory if it doesn't exist
    log_file = Path(Config.LOG_FILE)
    log_file.parent.mkdir(parents=True, exist_ok=True)

    # Root logger configuration
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Remove existing handlers
    root_logger.handlers.clear()

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    # File handler (rotating)
    file_handler = RotatingFileHandler(
        Config.LOG_FILE,
        maxBytes=Config.LOG_MAX_BYTES,
        backupCount=Config.LOG_BACKUP_COUNT,
        encoding='utf-8'
    )
    file_handler.setLevel(log_level)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)

    # Flask app logger
    if app:
        app.logger.setLevel(log_level)
        app.logger.handlers = root_logger.handlers

    # Suppress noisy loggers
    logging.getLogger('werkzeug').setLevel(logging.WARNING)
    logging.getLogger('selenium').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

    logging.info("Logging configured successfully")
    return root_logger


def get_logger(name):
    """Get a logger instance with the given name"""
    return logging.getLogger(name)
