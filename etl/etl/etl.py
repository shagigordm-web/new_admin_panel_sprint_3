import time
from datetime import datetime, timezone
from .extractor import PostgresExtractor
from .transformer import transform_movies
from .loader import ElasticLoader
from .state import State
from .config import settings
import logging

logger = logging.getLogger(__name__)

class ETLPipeline:
    def __init__(self):
        self.extractor = PostgresExtractor()
        self.loader = ElasticLoader()
        self.state = State(settings.state_file_path)
        self.batch_size = settings.batch_size

    def run(self):
        logger.info("Starting ETL pipeline...")
        while True:
            last_modified = self.state.get_last_modified() or "1970-01-01 00:00:00.000000+00"
            logger.info("Fetching films modified after %s", last_modified)

            raw_data = self.extractor.extract(last_modified, self.batch_size)
            if not raw_data:
                logger.info("No new data. Sleeping...")
                time.sleep(settings.poll_delay)
                continue

            transformed = transform_movies(raw_data)
            if not transformed:
                logger.warning("No valid data to load.")
                continue

            self.loader.load(transformed)

            latest = max(row["modified"] for row in raw_data)
            self.state.set_last_modified(latest)
            logger.info("Loaded %d films. Updated state to %s", len(transformed), latest)