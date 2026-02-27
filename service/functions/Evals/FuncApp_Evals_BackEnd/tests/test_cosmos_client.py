from __future__ import annotations

from typing import Any, Dict, List

import pytest
from azure.core.exceptions import ServiceRequestError

from config.models import CosmosConfig
from data import cosmos_client as cosmos_module
from data.cosmos_client import CosmosDbClient


class _FakeContainer:
    def __init__(self) -> None:
        self.upsert_calls = 0
        self.query_calls = 0
        self.batch_calls = 0
        self.fail_upsert_attempts = 0
        self.batch_sizes: List[int] = []

    def upsert_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        self.upsert_calls += 1
        if self.fail_upsert_attempts > 0:
            self.fail_upsert_attempts -= 1
            raise ServiceRequestError("transient failure")
        return item

    def query_items(self, **_: Any) -> List[Dict[str, Any]]:
        self.query_calls += 1
        return [{"id": "ok"}]

    def execute_item_batch(self, batch_operations: List[Any], partition_key: str) -> None:
        self.batch_calls += 1
        assert partition_key == "app1"
        self.batch_sizes.append(len(batch_operations))


class _FakeDatabase:
    def __init__(self, telemetry: _FakeContainer, results: _FakeContainer) -> None:
        self._telemetry = telemetry
        self._results = results

    def create_container_if_not_exists(self, id: str, partition_key: Any) -> _FakeContainer:
        _ = partition_key
        if id == "telemetry":
            return self._telemetry
        return self._results


class _FakeCosmosClient:
    init_calls = 0
    last_kwargs: Dict[str, Any] = {}
    telemetry_container = _FakeContainer()
    results_container = _FakeContainer()

    def __init__(self, endpoint: str, credential: str, **kwargs: Any) -> None:
        _ = endpoint
        _ = credential
        _FakeCosmosClient.init_calls += 1
        _FakeCosmosClient.last_kwargs = kwargs

    def create_database_if_not_exists(self, id: str) -> _FakeDatabase:
        _ = id
        return _FakeDatabase(_FakeCosmosClient.telemetry_container, _FakeCosmosClient.results_container)


def _make_config(**overrides: Any) -> CosmosConfig:
    cfg = CosmosConfig(
        endpoint="https://example.documents.azure.com:443/",
        key="key",
        database_name="ai-eval",
        telemetry_container="telemetry",
        results_container="evaluation_results",
        operation_retry_attempts=3,
        operation_retry_base_delay_seconds=0.0,
        operation_retry_max_delay_seconds=0.0,
        operation_retry_jitter_seconds=0.0,
    )
    for key, value in overrides.items():
        setattr(cfg, key, value)
    return cfg


@pytest.fixture(autouse=True)
def _reset_pool_and_fakes(monkeypatch):
    cosmos_module._CLIENT_POOL.clear()
    _FakeCosmosClient.init_calls = 0
    _FakeCosmosClient.last_kwargs = {}
    _FakeCosmosClient.telemetry_container = _FakeContainer()
    _FakeCosmosClient.results_container = _FakeContainer()
    monkeypatch.setattr(cosmos_module, "CosmosClient", _FakeCosmosClient)


def test_client_pool_reuses_client_for_same_config() -> None:
    cfg = _make_config()
    CosmosDbClient(cfg)
    CosmosDbClient(cfg)

    assert _FakeCosmosClient.init_calls == 1


def test_client_passes_bulk_and_pool_settings() -> None:
    cfg = _make_config(enable_bulk=False, pool_max_connection_size=42)
    CosmosDbClient(cfg)

    assert _FakeCosmosClient.last_kwargs["enable_bulk"] is False
    assert _FakeCosmosClient.last_kwargs["connection_max_size"] == 42


def test_upsert_result_retries_transient_failures() -> None:
    cfg = _make_config()
    client = CosmosDbClient(cfg)
    _FakeCosmosClient.results_container.fail_upsert_attempts = 2

    item = {"id": "x", "pk": "app1"}
    result = client.upsert_result(item)

    assert result == item
    assert _FakeCosmosClient.results_container.upsert_calls == 3


def test_upsert_results_batch_chunks_to_max_100() -> None:
    cfg = _make_config()
    client = CosmosDbClient(cfg)

    items = [{"id": str(i), "pk": "app1"} for i in range(150)]
    client.upsert_results_batch(items, partition_key="app1")

    assert _FakeCosmosClient.results_container.batch_calls == 2
    assert _FakeCosmosClient.results_container.batch_sizes == [100, 50]
