from __future__ import annotations

from typing import Any, Dict, List

from azure.cosmos import CosmosClient, PartitionKey

from config.models import CosmosConfig


class CosmosDbClient:
    def __init__(self, config: CosmosConfig) -> None:
        if not config.endpoint or not config.key:
            raise ValueError("Cosmos endpoint/key are required")

        self._client = CosmosClient(config.endpoint, credential=config.key)
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
        return self._telemetry_container.upsert_item(item)

    def query_telemetry(self, query: str, parameters: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return list(
            self._telemetry_container.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=True,
            )
        )

    def upsert_result(self, item: Dict[str, Any]) -> Dict[str, Any]:
        return self._results_container.upsert_item(item)

    def query_results(self, query: str, parameters: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return list(
            self._results_container.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=True,
            )
        )
