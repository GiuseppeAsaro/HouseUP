from typing import Any, Optional

from pymongo import MongoClient
from redis import Redis
from redis.exceptions import RedisError

from app.core.config import get_settings

settings = get_settings()


class RedisConnectionManager:
    _WRITE_METHODS = {"set", "setex", "delete", "eval"}

    def __init__(self, urls: list[str]) -> None:
        self.urls = urls
        self._master_url = urls[0] if urls else None
        self._current_url: Optional[str] = None
        self._client: Optional[Redis] = None
        self._connected = False
        self._connect_to_first_available()

    @property
    def is_connected(self) -> bool:
        return self._connected and self._client is not None

    def _connect_to_first_available(self) -> None:
        for url in self.urls:
            if self._try_connect(url):
                self._connected = True
                return
        print("Nessun nodo Redis raggiungibile.")
        self._connected = False

    def _try_connect(self, url: str) -> bool:
        try:
            print(f"Provo Redis su {url}")
            client = Redis.from_url(url, socket_connect_timeout=2)
            client.ping()
            self._client = client
            self._current_url = url
            self._connected = True
            print(f"Redis collegato a {url}")
            return True
        except RedisError:
            print(" Redis non disponibile")
        return False

    def _active_client(self) -> Redis:
        if self._client is None:
            raise RedisError("Nessun nodo Redis raggiungibile.")
        return self._client

    def ensure_node_available(self) -> bool:
        if self._client is not None:
            try:
                self._client.ping()
                return True
            except RedisError:
                pass
        for url in self.urls:
            if self._try_connect(url):
                return True
        return False

    def ensure_master_available(self) -> bool:
        if self._master_url is None:
            return self.ensure_node_available()
        if self._current_url == self._master_url and self._client is not None:
            return True
        return self._try_connect(self._master_url)

    def _run_write_with_master_retry(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        if not self.ensure_master_available():
            raise RedisError("Master Redis node is not reachable.")
        client = self._active_client()
        try:
            return getattr(client, method_name)(*args, **kwargs)
        except RedisError:
            print("Scrittura fallita, riprovo sul master.")
            master_was_current = self._current_url == self._master_url
            if self._try_connect(self._master_url):
                if not master_was_current:
                    print(f"Switchato di nuovo sul master {self._master_url}")
                return getattr(self._active_client(), method_name)(*args, **kwargs)
            raise

    def _run_read_with_node_retry(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        if not self.ensure_node_available():
            raise RedisError("No reachable Redis nodes.")
        client = self._active_client()
        try:
            return getattr(client, method_name)(*args, **kwargs)
        except RedisError:
            if self.ensure_node_available():
                return getattr(self._active_client(), method_name)(*args, **kwargs)
            raise

    @property
    def current_role(self) -> str:
        if self._client is None:
            return "unknown"
        if not self._master_url:
            return "master"
        return "master" if self._current_url == self._master_url else "replica"

    def __getattr__(self, name: str) -> Any:
        if name in self._WRITE_METHODS:
            return lambda *args, **kwargs: self._run_write_with_master_retry(name, *args, **kwargs)
        return lambda *args, **kwargs: self._run_read_with_node_retry(name, *args, **kwargs)


def create_redis_client(urls: list[str]) -> RedisConnectionManager | None:
    if not urls:
        print("Nessun nodo Redis configurato.")
        return None
    manager = RedisConnectionManager(urls)
    if not manager.is_connected:
        return None
    return manager


#Mongo
mongo_client: MongoClient = MongoClient(str(settings.mongo_uri))
mongo_db = mongo_client[settings.mongo_db]

#Redis con fallback
redis_client = create_redis_client(settings.redis_urls)


def get_redis_node_role() -> str:
    if redis_client is None:
        return "unknown"
    return redis_client.current_role
