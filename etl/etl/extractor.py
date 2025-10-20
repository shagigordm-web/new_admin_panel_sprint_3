import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Dict, Any, Optional
from .utils import backoff # Импортируем универсальный декоратор
from .config import settings
from .queries import EXTRACT_FILMWORK_QUERY

class PostgresExtractor:
    def __init__(self):
        self.dsn = str(settings.postgres_dsn)
        self.batch_size = settings.batch_size

    @backoff # Применяем универсальный декоратор
    def extract_batch(self, last_modified: str) -> List[Dict[str, Any]]:
        query = EXTRACT_FILMWORK_QUERY
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, (last_modified, self.batch_size))
                return cur.fetchall()

    def extract_all(self, start_modified: str = "1900-01-01T00:00:00.000+00:00") -> List[Dict[str, Any]]:
        all_data = []
        current_last_modified = start_modified
        while True:
            rows = self.extract_batch(current_last_modified)
            if not rows:
                break
            all_data.extend(rows)
            current_last_modified = max(row['modified'] for row in rows).isoformat()
        return all_data
