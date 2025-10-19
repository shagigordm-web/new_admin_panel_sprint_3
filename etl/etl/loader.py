# etl/loader.py
from elasticsearch import Elasticsearch, ConflictError
from elasticsearch.helpers import bulk
from typing import List, Dict, Any
from .utils import elastic_backoff
from .config import settings
from .index_mapping import INDEX_MAPPING_BODY
import logging

logger = logging.getLogger(__name__)

class ElasticLoader:
    def __init__(self):
        self.client = Elasticsearch(hosts=[str(settings.elastic_host)], max_retries=3, retry_on_timeout=True)
        self.index_name = settings.elastic_index

    @elastic_backoff
    def load(self, documents: List[Dict[str, Any]]):
        if not documents:
            logger.info("No documents to load.")
            return

        actions = [
            {
                "_index": self.index_name,
                "_id": doc["id"],
                "_source": doc,
            }
            for doc in documents
        ]

        success_count, failed_items = bulk(self.client, actions, refresh='wait_for', max_retries=3, initial_backoff=1, request_timeout=60)
        logger.info(f"Successfully loaded {success_count} documents into {self.index_name}. Failed: {len(failed_items)}")
        if failed_items:
            logger.error(f"Failed to load {len(failed_items)} documents: {failed_items}")

    def create_index_if_not_exists(self):
        """Создает индекс, если он не существует, с заданным маппингом."""
        if not self.client.indices.exists(index=self.index_name):
            self.client.indices.create(index=self.index_name, body=INDEX_MAPPING_BODY)
            logger.info(f"Index {self.index_name} created with predefined mapping.")
        else:
            logger.info(f"Index {self.index_name} already exists.")

    def clear_index(self):
        """Очищает индекс перед загрузкой."""
        if self.client.indices.exists(index=self.index_name):
            self.client.indices.delete(index=self.index_name)
            logger.info(f"Index {self.index_name} deleted.")
        self.create_index_if_not_exists()
