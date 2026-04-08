"""
Structured logging setup for the weather trading bot.
"""
import logging
import os
from datetime import datetime

from config import LOGS_DIR


def setup_logging(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Configure and return a named logger.

    Writes to both console and a dated log file in LOGS_DIR.
    """
    os.makedirs(LOGS_DIR, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    if logger.handlers:
        return logger

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console handler
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # File handler — one file per date per module
    date_str = datetime.utcnow().strftime("%Y%m%d")
    log_file = os.path.join(LOGS_DIR, f"{name}_{date_str}.log")
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger
