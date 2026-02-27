from __future__ import annotations

import hashlib
import logging
import random
import threading
import time
from typing import Any, Callable, Dict, List, Tuple

from azure.cosmos import CosmosClient, PartitionKey
from azure.cosmos.exceptions import CosmosBatchOperationError
from azure.core.exceptions import ServiceRequestError

from config.models import CosmosConfig

logger = logging.getLogger(__name__)

_CLIENT_POOL: Dict[str, CosmosClient] = {}
_POOL_LOCK = threading.Lock()
_TRANSIENT_STATUS_CODES = {408, 429, 500, 502, 503, 504}


class CosmosDbClient:
    def __init__(self, config: CosmosConfig) -> None:
        if not config.endpoint or not config.key:
            raise ValueError("Cosmos endpoint/key are required")
        self._config = config

        # Connection pooling: reuse CosmosClient instances per configuration key to avoid
        # socket exhaustion in serverless environments like Azure Functions.
        pool_key = self._pool_key(config)
        with _POOL_LOCK:
            if pool_key not in _CLIENT_POOL:
                _CLIENT_POOL[pool_key] = CosmosClient(
                    config.endpoint,
                    credential=config.key,
                    enable_bulk=config.enable_bulk,
                    connection_max_size=config.pool_max_connection_size,
                    # SDK-level retry for transient HTTP failures.
                    retry_total=config.client_retry_total,
                    retry_backoff_max=config.client_retry_backoff_max,
                    retry_backoff_factor=config.client_retry_backoff_factor,
                    retry_on_status_codes=list(_TRANSIENT_STATUS_CODES),
                    connection_timeout=config.client_connection_timeout,
                )

        self._client = _CLIENT_POOL[pool_key]
        self._database = self._client.create_database_if_not_exists(id=config.database_name)
        self._telemetry_container = self._database.create_container_if_not_exists(
            id=config.telemetry_container,
            partition_key=PartitionKey(path="/pk"),
        )
        self._results_container = self._database.create_container_if_not_exists(
            id=config.results_container,
            partition_key=PartitionKey(path="/pk"),
        )

    def upsert_telemetry(self, item: Dict[str, Any]) -> Dict[str, Any]:
        return self._with_retry("upsert_telemetry", self._telemetry_container.upsert_item, item)

    def query_telemetry(self, query: str, parameters: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return self._with_retry(
            "query_telemetry",
            lambda: list(
                self._telemetry_container.query_items(
                    query=query,
                    parameters=parameters,
                    enable_cross_partition_query=True,
                )
            )
        )

    def upsert_result(self, item: Dict[str, Any]) -> Dict[str, Any]:
        return self._with_retry("upsert_result", self._results_container.upsert_item, item)

    def upsert_results_batch(self, items: List[Dict[str, Any]], partition_key: str) -> None:
        """Upsert a batch of up to 100 items within the same logical partition."""
        if not items:
            return

        chunk_size = 100
        for i in range(0, len(items), chunk_size):
            chunk = items[i : i + chunk_size]
            batch_operations: List[Tuple[str, Tuple[Any, ...]]] = [("upsert", (item,)) for item in chunk]
            self._with_retry(
                "upsert_results_batch",
                self._results_container.execute_item_batch,
                batch_operations,
                partition_key=partition_key,
            )

    def query_results(self, query: str, parameters: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return self._with_retry(
            "query_results",
            lambda: list(
                self._results_container.query_items(
                    query=query,
                    parameters=parameters,
                    enable_cross_partition_query=True,
                )
            )
        )

    @staticmethod
    def _pool_key(config: CosmosConfig) -> str:
        key_material = "|".join(
            [
                config.endpoint,
                config.key,
                str(config.enable_bulk),
                str(config.pool_max_connection_size),
                str(config.client_retry_total),
                str(config.client_retry_backoff_max),
                str(config.client_retry_backoff_factor),
                str(config.client_connection_timeout),
            ]
        )
        return hashlib.sha256(key_material.encode("utf-8")).hexdigest()

    @staticmethod
    def _is_transient_error(exc: Exception) -> bool:
        if isinstance(exc, ServiceRequestError):
            return True

        status_code = getattr(exc, "status_code", None)
        if status_code in _TRANSIENT_STATUS_CODES:
            return True

        if isinstance(exc, CosmosBatchOperationError):
            for failure in getattr(exc, "failed_operations", []):
                if getattr(failure, "status_code", None) in _TRANSIENT_STATUS_CODES:
                    return True
            return False

        return False

    def _with_retry(
        self,
        operation_name: str,
        fn: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        attempts = max(1, self._config.operation_retry_attempts)
        base_delay = max(0.0, self._config.operation_retry_base_delay_seconds)
        max_delay = max(base_delay, self._config.operation_retry_max_delay_seconds)
        jitter = max(0.0, self._config.operation_retry_jitter_seconds)

        for attempt in range(1, attempts + 1):
            try:
                return fn(*args, **kwargs)
            except Exception as exc:
                if attempt >= attempts or not self._is_transient_error(exc):
                    raise
                delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
                sleep_seconds = delay + random.uniform(0.0, jitter)
                logger.warning(
                    "Cosmos transient failure in %s (attempt %d/%d): %s. Retrying in %.2fs",
                    operation_name,
                    attempt,
                    attempts,
                    exc,
                    sleep_seconds,
                )
                time.sleep(sleep_seconds)
