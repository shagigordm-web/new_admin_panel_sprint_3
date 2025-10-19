import time
import logging
from functools import wraps
from typing import Callable, Any

logger = logging.getLogger(__name__)

def pg_backoff(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(*args, **kwargs):
        max_retries = 5
        base_delay = 1
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Max retries reached for {func.__name__}: {e}")
                    raise e
                delay = base_delay * (2 ** attempt)
                logger.warning(f"PostgreSQL error in {func.__name__}: {e}. Retrying in {delay}s...")
                time.sleep(delay)
    return wrapper

def elastic_backoff(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(*args, **kwargs):
        max_retries = 5
        base_delay = 1
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Max retries reached for {func.__name__}: {e}")
                    raise e
                delay = base_delay * (2 ** attempt)
                logger.warning(f"Elasticsearch error in {func.__name__}: {e}. Retrying in {delay}s...")
                time.sleep(delay)
    return wrapper