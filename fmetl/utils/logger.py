import logging
import sys
from typing import Optional
from pathlib import Path


def get_logger(name: str, level: int = logging.INFO, log_file: Optional[Path] = None) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(level)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    handler: logging.Handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(fmt)
    logger.addHandler(handler)

    if log_file:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    return logger
