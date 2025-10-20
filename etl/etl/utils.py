# etl/utils.py
import time
import logging
from functools import wraps
from typing import Callable, Any

logger = logging.getLogger(__name__)

def backoff(func: Callable) -> Callable:

    @wraps(func)
    def wrapper(*args, **kwargs):
        max_retries = 5  # Максимальное количество попыток
        base_delay = 1   # Базовая задержка в секундах
        for attempt in range(max_retries):
            try:
                # Выполняем декорируемую функцию
                return func(*args, **kwargs)
            except Exception as e:
                # Если это последняя попытка, поднимаем исключение
                if attempt == max_retries - 1:
                    logger.error(f"Max retries reached for {func.__name__}: {e}")
                    raise e
                # Вычисляем задержку (1, 2, 4, 8, 16 секунд)
                delay = base_delay * (2 ** attempt)
                # Логируем ошибку с именем функции
                logger.warning(f"Error in {func.__name__}: {e}. Retrying in {delay}s...")
                time.sleep(delay)  # Ждем перед следующей попыткой
    return wrapper