import logging
import os
from logging.handlers import RotatingFileHandler

def setup_logger(service_name):
    """
    service_name: 'producer' hoặc 'worker'
    """

    WORKER_ID = os.getenv("WORKER_ID", "1")

    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # tránh duplicate handler
    if logger.handlers:
        return logger

    # ===== FORMAT =====
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    # ===== INFO LOG =====
    info_handler = RotatingFileHandler(
        f"{log_dir}/{service_name}_{WORKER_ID}.log",
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5
    )
    info_handler.setLevel(logging.INFO)
    info_handler.setFormatter(formatter)

    # ===== ERROR LOG =====
    error_handler = RotatingFileHandler(
        f"{log_dir}/error.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=5
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)

    # ===== CONSOLE =====
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(info_handler)
    logger.addHandler(error_handler)
    logger.addHandler(console_handler)

    return logger