from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Dict, List, Protocol

from data.cosmos_client import CosmosDbClient
from data.models import EvaluationResult, TelemetryRecord


class TelemetryRepository(Protocol):
    async def fetch_telemetry(self, app_id: str, start_ts: str, end_ts: str) -> List[TelemetryRecord]:
        ...


class EvaluationRepository(Protocol):
    async def save_result(self, result: EvaluationResult) -> None:
        ...

    async def latest_results(self, app_id: str, limit: int = 20) -> List[Dict]:
        ...

    async def result_exists(self, result_id: str) -> bool:
        ...


class CosmosTelemetryRepository:
    def __init__(self, client: CosmosDbClient) -> None:
        self._client = client

    async def fetch_telemetry(self, app_id: str, start_ts: str, end_ts: str) -> List[TelemetryRecord]:
        def _fetch() -> List[TelemetryRecord]:
            query = (
                "SELECT * FROM c WHERE c.type = 'telemetry' "
                "AND c.app_id = @app_id AND c.timestamp >= @start_ts AND c.timestamp < @end_ts"
            )
            rows = self._client.query_telemetry(
                query,
                [
                    {"name": "@app_id", "value": app_id},
                    {"name": "@start_ts", "value": start_ts},
                    {"name": "@end_ts", "value": end_ts},
                ],
            )
            return [TelemetryRecord(**_pick_telemetry_fields(row)) for row in rows]

        return await asyncio.to_thread(_fetch)


class CosmosEvaluationRepository:
    def __init__(self, client: CosmosDbClient) -> None:
        self._client = client

    async def save_result(self, result: EvaluationResult) -> None:
        await asyncio.to_thread(self._client.upsert_result, result.to_dict())

    async def latest_results(self, app_id: str, limit: int = 20) -> List[Dict]:
        def _fetch() -> List[Dict]:
            # Cosmos DB SQL API does not support parameterised TOP values;
            # limit is a typed int so it is safe to inline directly.
            query = (
                f"SELECT TOP {limit} c.app_id, c.timestamp, c.policy_name, c.metrics, c.breaches "
                "FROM c WHERE c.type = 'evaluation_result' AND c.app_id = @app_id "
                "ORDER BY c.timestamp DESC"
            )
            return self._client.query_results(
                query,
                [{"name": "@app_id", "value": app_id}],
            )

        return await asyncio.to_thread(_fetch)

    async def result_exists(self, result_id: str) -> bool:
        def _fetch() -> bool:
            rows = self._client.query_results(
                "SELECT TOP 1 c.id FROM c WHERE c.id = @id",
                [{"name": "@id", "value": result_id}],
            )
            return bool(rows)

        return await asyncio.to_thread(_fetch)


@dataclass
class InMemoryStore:
    telemetry: List[TelemetryRecord]
    results: List[EvaluationResult]


class InMemoryTelemetryRepository:
    def __init__(self, store: InMemoryStore) -> None:
        self._store = store

    async def fetch_telemetry(self, app_id: str, start_ts: str, end_ts: str) -> List[TelemetryRecord]:
        return [
            row
            for row in self._store.telemetry
            if row.app_id == app_id and start_ts <= row.timestamp < end_ts
        ]


class InMemoryEvaluationRepository:
    def __init__(self, store: InMemoryStore) -> None:
        self._store = store

    async def save_result(self, result: EvaluationResult) -> None:
        self._store.results.append(result)

    async def latest_results(self, app_id: str, limit: int = 20) -> List[Dict]:
        items = [r.to_dict() for r in self._store.results if r.app_id == app_id]
        return sorted(items, key=lambda r: r["timestamp"], reverse=True)[:limit]

    async def result_exists(self, result_id: str) -> bool:
        return any(r.id == result_id for r in self._store.results)


def _pick_telemetry_fields(row: Dict) -> Dict:
    allowed = {
        "id",
        "app_id",
        "timestamp",
        "model_id",
        "model_version",
        "input_text",
        "output_text",
        "expected_output",
        "user_id",
        "latency_ms",
        "metadata",
    }
    payload = {key: value for key, value in row.items() if key in allowed}

    # Normalise trace identity for dedupe:
    # - prefer metadata.trace_id when present
    # - fall back to top-level trace_id/traceparent fields from telemetry ingestion
    metadata = dict(payload.get("metadata") or {})
    if not metadata.get("trace_id"):
        trace_id = row.get("trace_id") or row.get("traceId") or row.get("traceparent")
        if trace_id:
            metadata["trace_id"] = str(trace_id)
    payload["metadata"] = metadata
    return payload
