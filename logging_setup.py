"""
logging_setup.py
-----------------
Replaces ad-hoc print() statements with a real logging pipeline so log
output is timestamped, leveled, and redirectable to a file/log
aggregator (Azure Monitor, CloudWatch, etc.) without touching call sites.
"""
import logging
import sys


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger