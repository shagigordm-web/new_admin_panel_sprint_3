from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    postgres_dsn: str = "postgresql://app:123qwe@postgres:5432/test_data"
    elastic_host: str = "http://elasticsearch:9200"
    elastic_index: str = "movies"
    batch_size: int = 100
    poll_delay: float = 5.0
    state_file_path: str = "state.json"

    class Config:
        env_file = ".env"
        env_file_encoding = 'utf-8'

settings = Settings()