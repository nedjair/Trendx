from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from trendx.services.cache import CacheService


@pytest.fixture
def cache_service():
    svc = CacheService(
        redis_url="redis://localhost:6379/0",
        default_ttl=900,
    )
    svc._client = None
    return svc


@pytest.mark.unit
def test_set_get(cache_service):
    mock_redis = MagicMock()
    mock_redis.get.return_value = '"cached_value"'
    cache_service._client = mock_redis

    cache_service.set("my-key", "cached_value", namespace="metadata")
    mock_redis.setex.assert_called_once()

    value = cache_service.get("my-key", namespace="metadata")
    assert value == "cached_value"
    assert cache_service._stats["hits"] == 1


@pytest.mark.unit
def test_ttl_expiration(cache_service):
    mock_redis = MagicMock()
    mock_redis.get.return_value = None
    cache_service._client = mock_redis

    value = cache_service.get("expired-key", namespace="metadata")
    assert value is None
    assert cache_service._stats["misses"] == 1


@pytest.mark.unit
def test_delete(cache_service):
    mock_redis = MagicMock()
    mock_redis.delete.return_value = 1
    cache_service._client = mock_redis

    result = cache_service.delete("my-key", namespace="metadata")
    assert result is True
    assert cache_service._stats["deletes"] == 1


@pytest.mark.unit
def test_invalidate_pattern(cache_service):
    mock_redis = MagicMock()
    mock_redis.scan.return_value = (0, ["trendx:metadata:key1", "trendx:metadata:key2"])
    mock_redis.delete.return_value = 2
    cache_service._client = mock_redis

    deleted = cache_service.invalidate("trendx:metadata:*")
    assert deleted == 2


@pytest.mark.unit
def test_invalidate_namespace(cache_service):
    mock_redis = MagicMock()
    mock_redis.scan.return_value = (0, ["trendx:topology:dev-001"])
    mock_redis.delete.return_value = 1
    cache_service._client = mock_redis

    deleted = cache_service.invalidate_namespace("topology")
    assert deleted == 1


@pytest.mark.unit
def test_get_or_compute(cache_service):
    mock_redis = MagicMock()
    mock_redis.get.side_effect = [None, '"computed_value"']
    cache_service._client = mock_redis

    def compute():
        return "computed_value"

    result = cache_service.get_or_compute(
        "my-key", compute_fn=compute, namespace="metadata"
    )
    assert result == "computed_value"

    result = cache_service.get_or_compute(
        "my-key", compute_fn=compute, namespace="metadata"
    )
    assert result == "computed_value"


@pytest.mark.unit
def test_get_or_compute_no_fn(cache_service):
    mock_redis = MagicMock()
    mock_redis.get.return_value = None
    cache_service._client = mock_redis

    result = cache_service.get_or_compute("missing-key")
    assert result is None


@pytest.mark.unit
def test_clear_all(cache_service):
    mock_redis = MagicMock()
    mock_redis.scan.return_value = (0, ["trendx:metadata:k1", "trendx:topology:k2"])
    mock_redis.delete.return_value = 2
    cache_service._client = mock_redis

    result = cache_service.clear_all()
    assert result is True


@pytest.mark.unit
def test_get_stats(cache_service):
    mock_redis = MagicMock()
    mock_redis.info.return_value = {"keyspace_hits": 10, "keyspace_misses": 2}
    cache_service._client = mock_redis
    cache_service._stats["hits"] = 5
    cache_service._stats["misses"] = 1

    stats = cache_service.get_stats()
    assert stats["app_stats"]["hit_rate"] > 0.0
    assert "redis_info" in stats
    assert "ttl_by_namespace" in stats


@pytest.mark.unit
def test_redis_failure_returns_none(cache_service):
    mock_redis = MagicMock()
    mock_redis.get.side_effect = Exception("Connection lost")
    cache_service._client = mock_redis

    value = cache_service.get("fail-key")
    assert value is None
    assert cache_service._stats["misses"] == 1


@pytest.mark.unit
def test_context_manager(cache_service):
    mock_redis = MagicMock()
    cache_service._client = mock_redis

    with cache_service as svc:
        assert svc is cache_service

    mock_redis.close.assert_called_once()
