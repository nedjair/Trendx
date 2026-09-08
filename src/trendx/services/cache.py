from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

try:
    import redis as sync_redis

    HAS_REDIS = True
except ImportError:
    HAS_REDIS = False
    # Redis optionnel (profil minimal sans Redis) ; garde-fou HAS_REDIS à l'exécution.
    sync_redis = None  # type: ignore[assignment]
from loguru import logger
from trendx.config import settings

DEFAULT_TTL_BY_NAMESPACE: dict[str, int] = {
    "metadata": 3600,
    "topology": 1800,
    "metrics": 900,
    "views": 600,
    "ml_results": 300,
    "reports": 3600,
}


class CacheService:
    """Redis-backed cache with namespace isolation and cache-aside pattern.

    Namespaces
    ----------
    - ``metadata``: entity metadata, device info (TTL 1h)
    - ``topology``: entity relations, tree structure (TTL 30min)
    - ``metrics``: telemetry keys, latest values (TTL 15min)
    - ``views``: dashboard/view configurations (TTL 10min)
    - ``ml_results``: cached predictions, anomaly scores (TTL 5min)
    - ``reports``: generated reports (TTL 1h)
    """

    def __init__(
        self,
        redis_url: str | None = None,
        default_ttl: int = 900,
        ttl_by_namespace: dict[str, int] | None = None,
    ) -> None:
        if not HAS_REDIS:
            raise RuntimeError("redis package is required for CacheService")
        self._redis_url = redis_url or settings.redis_url
        self._default_ttl = default_ttl
        self._ttl_by_namespace = {**DEFAULT_TTL_BY_NAMESPACE, **(ttl_by_namespace or {})}
        self._client: sync_redis.Redis[Any] | None = None
        self._stats: dict[str, int] = {
            "hits": 0,
            "misses": 0,
            "sets": 0,
            "deletes": 0,
        }

    def _connect(self) -> sync_redis.Redis[Any]:
        if self._client is None:
            self._client = sync_redis.from_url(
                self._redis_url,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=10,
            )
            logger.debug("Connected to Redis at {}", self._redis_url)
        return self._client

    def _namespaced_key(self, key: str, namespace: str) -> str:
        return f"trendx:{namespace}:{key}"

    def _ttl_for(self, namespace: str) -> int:
        return self._ttl_by_namespace.get(namespace, self._default_ttl)

    def get(
        self,
        key: str,
        namespace: str = "metadata",
    ) -> Any:
        client = self._connect()
        full_key = self._namespaced_key(key, namespace)
        try:
            value = client.get(full_key)
            if value is not None:
                self._stats["hits"] += 1
                return json.loads(value)
            self._stats["misses"] += 1
            return None
        except Exception as exc:
            logger.warning("Cache get failed for {}: {}", full_key, exc)
            self._stats["misses"] += 1
            return None

    def set(
        self,
        key: str,
        value: Any,
        ttl: int | None = None,
        namespace: str = "metadata",
    ) -> bool:
        client = self._connect()
        full_key = self._namespaced_key(key, namespace)
        effective_ttl = ttl if ttl is not None else self._ttl_for(namespace)
        try:
            serialized = json.dumps(value, default=str)
            client.setex(full_key, effective_ttl, serialized)
            self._stats["sets"] += 1
            logger.debug("Cached {} (TTL={}s, namespace='{}')", full_key, effective_ttl, namespace)
            return True
        except Exception as exc:
            logger.warning("Cache set failed for {}: {}", full_key, exc)
            return False

    def delete(
        self,
        key: str,
        namespace: str = "metadata",
    ) -> bool:
        client = self._connect()
        full_key = self._namespaced_key(key, namespace)
        try:
            result = client.delete(full_key)
            if result:
                self._stats["deletes"] += 1
                logger.debug("Deleted cache key {}", full_key)
            return bool(result)
        except Exception as exc:
            logger.warning("Cache delete failed for {}: {}", full_key, exc)
            return False

    def invalidate(self, pattern: str) -> int:
        client = self._connect()
        try:
            cursor = 0
            deleted = 0
            while True:
                cursor, keys = client.scan(cursor=cursor, match=pattern, count=100)
                if keys:
                    deleted += client.delete(*keys)
                if cursor == 0:
                    break
            if deleted:
                logger.info("Invalidated {} keys matching '{}'", deleted, pattern)
            return deleted
        except Exception as exc:
            logger.warning("Cache invalidate failed for pattern '{}': {}", pattern, exc)
            return 0

    def invalidate_namespace(self, namespace: str) -> int:
        pattern = f"trendx:{namespace}:*"
        return self.invalidate(pattern)

    def get_stats(self) -> dict[str, Any]:
        total = self._stats["hits"] + self._stats["misses"]
        hit_rate = self._stats["hits"] / total if total > 0 else 0.0
        client = self._connect()
        info: dict[str, Any] = {}
        try:
            info_raw = client.info("stats")
            if isinstance(info_raw, dict):
                info = {
                    "keyspace_hits": info_raw.get("keyspace_hits", 0),
                    "keyspace_misses": info_raw.get("keyspace_misses", 0),
                    "connected_clients": info_raw.get("connected_clients", 0),
                    "used_memory_human": info_raw.get("used_memory_human", "?"),
                }
        except Exception as exc:
            logger.warning("Failed to get Redis info: {}", exc)
        return {
            "app_stats": {
                "hits": self._stats["hits"],
                "misses": self._stats["misses"],
                "sets": self._stats["sets"],
                "deletes": self._stats["deletes"],
                "hit_rate": round(hit_rate, 4),
            },
            "redis_info": info,
            "ttl_by_namespace": self._ttl_by_namespace,
        }

    def get_or_compute(
        self,
        key: str,
        ttl: int | None = None,
        compute_fn: Callable[[], Any] | None = None,
        namespace: str = "metadata",
    ) -> Any:
        cached = self.get(key, namespace=namespace)
        if cached is not None:
            return cached
        if compute_fn is None:
            return None
        try:
            value = compute_fn()
        except Exception as exc:
            logger.error("Cache compute_fn failed for {}: {}", key, exc)
            return None
        if value is not None:
            self.set(key, value, ttl=ttl, namespace=namespace)
        return value

    def clear_all(self) -> bool:
        client = self._connect()
        try:
            cursor = 0
            deleted = 0
            while True:
                cursor, keys = client.scan(cursor=cursor, match="trendx:*", count=500)
                if keys:
                    deleted += client.delete(*keys)
                if cursor == 0:
                    break
            logger.info("Cleared {} Trendx cache keys", deleted)
            return True
        except Exception as exc:
            logger.warning("Failed to clear all cache: {}", exc)
            return False

    def close(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
            except Exception as exc:
                logger.debug("Erreur ignorée à la fermeture Redis : {}", exc)
            self._client = None
            logger.debug("Redis connection closed")

    def __enter__(self) -> CacheService:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        self.close()
