import json
import os
from typing import Optional
from .config import settings

class State:
    def __init__(self, state_file_path: str = None):
        self.state_file_path = state_file_path if state_file_path is not None else settings.state_file_path
        self._state = self._load_state()

    def _load_state(self) -> dict:
        """Загружает состояние из файла. Возвращает пустой словарь, если файл не найден."""
        if os.path.exists(self.state_file_path):
            try:
                with open(self.state_file_path, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                print(f"Error loading state file {self.state_file_path}, starting fresh. Error: {e}")
                return {}
        return {}

    def save_state(self):
        """Сохраняет текущее состояние в файл."""
        try:
            with open(self.state_file_path, 'w') as f:
                json.dump(self._state, f)
        except IOError as e:
            print(f"Error saving state to file {self.state_file_path}: {e}")

    def set_value(self, key: str, value):
        """Устанавливает значение для ключа в состоянии."""
        self._state[key] = value
        self.save_state()

    def get_value(self, key: str, default=None):
        """Получает значение для ключа из состояния."""
        return self._state.get(key, default)
