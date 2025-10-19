import logging
import os
import time
from datetime import datetime
from .extractor import PostgresExtractor
from .transformer import transform_movies
from .loader import ElasticLoader
from .state import State
from .config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_etl_loop():
    extractor = PostgresExtractor()
    loader = ElasticLoader()
    state_manager = State()

    # Проверяем, нужно ли выполнить полную перезагрузку при старте.
    # Это может быть опционально, например, если индекс пуст или если задана переменная окружения.
    # Для простоты, будем считать, что при запуске мы НЕ очищаем индекс и начинаем с последнего сохраненного состояния.
    # Если состояние отсутствует, начнем с очень ранней даты.
    initial_start_date = "1900-01-01T00:00:00.000+00:00"
    last_modified_str = state_manager.get_value('last_processed_modified', default=initial_start_date)

    logger.info(f"Starting ETL loop, will check for data modified after {last_modified_str}...")

    while True: # Бесконечный цикл
        logger.info(f"Checking for new data modified after {last_modified_str}...")

        # Извлекаем данные, измененные после last_modified_str
        raw_data = extractor.extract_all(start_modified=last_modified_str)

        if raw_data: # Исправлено: raw_data, а не raw_
            logger.info(f"Found {len(raw_data)} new/updated records.")
            # Трансформируем данные
            logger.info("Starting transformation...")
            transformed_data = transform_movies(raw_data)
            logger.info(f"Transformed {len(transformed_data)} records.")

            # Загружаем данные в Elasticsearch
            logger.info("Starting loading into Elasticsearch...")
            loader.load(transformed_data)
            logger.info("Loading completed.")

            # Находим самую последнюю дату модификации в загруженных данных
            latest_modified = max(row['modified'] for row in raw_data)
            latest_modified_iso = latest_modified.isoformat()
            # Сохраняем эту дату в состояние
            state_manager.set_value('last_processed_modified', latest_modified_iso)
            logger.info(f"Updated last processed modified time: {latest_modified_iso}")
            last_modified_str = latest_modified_iso
        else:
            logger.info("No new data found.")

        logger.info(f"Sleeping for {settings.poll_delay} seconds...")
        time.sleep(settings.poll_delay)

def run_etl_initial_load():
    """Функция для выполнения полной перезагрузки (если требуется)."""
    extractor = PostgresExtractor()
    loader = ElasticLoader()
    state_manager = State()

    state_file_path = state_manager.state_file_path
    if os.path.exists(state_file_path):
        os.remove(state_file_path)
        logger.info(f"State file {state_file_path} removed.")
    else:
        logger.info(f"State file {state_file_path} not found, no need to remove.")

    loader.clear_index()

    start_date = "1900-01-01T00:00:00.000+00:00"
    logger.info(f"Starting initial ETL load, extracting data modified after {start_date}...")

    raw_data = extractor.extract_all(start_modified=start_date)
    logger.info(f"Extracted {len(raw_data)} raw records for initial load.")

    if raw_data: 

        logger.info("Starting transformation...")
        transformed_data = transform_movies(raw_data)
        logger.info(f"Transformed {len(transformed_data)} records.")

        logger.info("Starting loading into Elasticsearch...")
        loader.load(transformed_data)
        logger.info("Initial ETL load completed successfully.")

        if raw_data:
            latest_modified = max(row['modified'] for row in raw_data)
            latest_modified_iso = latest_modified.isoformat()
            state_manager.set_value('last_processed_modified', latest_modified_iso)
            logger.info(f"Saved last processed modified time after initial load: {latest_modified_iso}")
    else:
        logger.info("No data found for initial load.")

if __name__ == "__main__":
    import sys
    # Опционально: используем аргумент командной строки или переменную окружения
    # для выбора режима работы.
    # Например: python -m etl.main full_load или MODE=full_load python -m etl.main
    mode = os.getenv("ETL_MODE", "loop") # По умолчанию - цикл

    if mode == "full_load":
        logger.info("Running in 'full_load' mode.")
        run_etl_initial_load()
        logger.info("Exiting after initial load.")
    else:
        logger.info("Running in 'loop' mode.")
        run_etl_loop() # Запускаем цикл
