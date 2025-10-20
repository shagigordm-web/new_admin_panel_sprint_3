import json
import os
import logging
from typing import Optional
from .config import settings

logger = logging.getLogger(__name__)

class State:
    def __init__(self, state_file_path: str = None):
        # Используем путь из конфига, если не передан явно
        self.state_file_path = state_file_path if state_file_path is not None else settings.state_file_path
        # Загружаем текущее состояние из файла при инициализации
        self._state = self._load_state()

    def _load_state(self) -> dict:
        if os.path.exists(self.state_file_path):
            try:
                with open(self.state_file_path, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                # Используем logger.error для записи ошибки
                logger.error(f"Error loading state file {self.state_file_path}, starting fresh. Error: {e}")
                return {}
        return {}

    def save_state(self):
        try:
            with open(self.state_file_path, 'w') as f:
                json.dump(self._state, f)
        except IOError as e:
            # Используем logger.error для записи ошибки
            logger.error(f"Error saving state to file {self.state_file_path}: {e}")

    def set_value(self, key: str, value):
        self._state[key] = value
        self.save_state()

    def get_value(self, key: str, default=None):

        return self._state.get(key, default)

    def get_last_modified(self):
        return self.get_value('last_processed_modified')

    def set_last_modified(self, value):
        self.set_value('last_processed_modified', value)
    # --- Конец добавленных методов ---
