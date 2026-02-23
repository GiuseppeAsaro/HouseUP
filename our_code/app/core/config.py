import os
from functools import lru_cache


class Settings:
    app_name: str
    environment: str

    mongo_uri: str
    mongo_db: str

    redis_urls: list[str]

    def __init__(self) -> None:
        self.app_name = os.getenv("APP_NAME", "HouseUp")
        self.environment = os.getenv("ENVIRONMENT", "development")
        self.mongo_uri = os.getenv("MONGO_URI")
        self.mongo_db = os.getenv("MONGO_DB")

        redis_urls_raw = os.getenv("REDIS_URLS", "")
        self.redis_urls = [u.strip() for u in redis_urls_raw.split(",") if u.strip()]


@lru_cache()
def get_settings() -> Settings:
    return Settings()
