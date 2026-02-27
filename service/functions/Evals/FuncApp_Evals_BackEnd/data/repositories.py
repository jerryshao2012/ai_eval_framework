from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import AsyncIterator, Dict, List, Protocol

logger = logging.getLogger(__name__)

from data.cosmos_client import CosmosDbClient
from data.models import EvaluationResult, TelemetryRecord


def _iterator_next_page(iterator: object) -> tuple[bool, object]:
    """Return (has_value, page) without propagating StopIteration across threads."""
    try:
        return True, next(iterator)  # type: ignore[arg-type]
    except StopIteration:
        return False, None


class TelemetryRepository(Protocol):
    async def fetch_telemetry(self, app_id: str, start_ts: str, end_ts: str) -> AsyncIterator[List[TelemetryRecord]]:
        ...


class EvaluationRepository(Protocol):
    async def save_result(self, result: EvaluationResult) -> None:
        ...

    async def save_results(self, results: List[EvaluationResult]) -> None:
        ...

    async def latest_results(self, app_id: str, limit: int = 20) -> List[Dict]:
        ...

    async def result_exists(self, result_id: str) -> bool:
        ...

    async def results_exist(self, result_ids: List[str]) -> List[str]:
        ...


class CosmosTelemetryRepository:
    def __init__(self, client: CosmosDbClient, page_size: int = 100) -> None:
        self._client = client
        self._page_size = max(1, int(page_size))

    async def fetch_telemetry(self, app_id: str, start_ts: str, end_ts: str) -> AsyncIterator[List[TelemetryRecord]]:
        dt_start = datetime.fromisoformat(start_ts.replace("Z", "+00:00"))
        dt_end = datetime.fromisoformat(end_ts.replace("Z", "+00:00"))
        
        # Calculate all YYYY-MM-DD strings that overlap with the time range
        date_slices = []
        current = dt_start
        while current.date() <= dt_end.date():
            date_slices.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)
            
        query = (
            "SELECT * FROM c WHERE c.type = 'telemetry' "
            "AND c.app_id = @app_id AND c.timestamp >= @start_ts AND c.timestamp < @end_ts"
        )
        parameters = [
            {"name": "@app_id", "value": app_id},
            {"name": "@start_ts", "value": start_ts},
            {"name": "@end_ts", "value": end_ts},
        ]
        
        for date_slice in date_slices:
            partition_key = f"{app_id}:{date_slice}"
            iterator = self._client.query_telemetry_paged(
                query,
                parameters,
                max_item_count=self._page_size,
                partition_key=partition_key,
            ).by_page()

            while True:
                has_page, page = await asyncio.to_thread(_iterator_next_page, iterator)
                if not has_page:
                    break
                if page:
                    yield [TelemetryRecord(**_pick_telemetry_fields(row)) for row in page]


class CosmosEvaluationRepository:
    def __init__(self, client: CosmosDbClient) -> None:
        self._client = client
        self._exists_query_chunk_size = 100

    async def save_result(self, result: EvaluationResult) -> None:
        await asyncio.to_thread(self._client.upsert_result, result.to_dict())

    async def save_results(self, results: List[EvaluationResult]) -> None:
        if not results:
            return

        def _save_all() -> None:
            # Group by partition key
            grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
            for r in results:
                payload = r.to_dict()
                grouped[payload["pk"]].append(payload)

            for pk, items in grouped.items():
                # Transactional batch inserts limit is 100 per request
                chunk_size = 100
                for i in range(0, len(items), chunk_size):
                    chunk = items[i : i + chunk_size]
                    try:
                        self._client.upsert_results_batch(chunk, partition_key=pk)
                        logger.debug("Batch upserted %d results to partition %s", len(chunk), pk)
                    except Exception as e:
                        logger.warning(
                            "Batch upsert failed for partition %s (%s). Falling back to individual requests.",
                            pk,
                            e,
                        )
                        for item in chunk:
                            self._client.upsert_result(item)

        await asyncio.to_thread(_save_all)

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

    async def results_exist(self, result_ids: List[str]) -> List[str]:
        if not result_ids:
            return []

        def _fetch() -> List[str]:
            found: List[str] = []
            for start in range(0, len(result_ids), self._exists_query_chunk_size):
                chunk = result_ids[start : start + self._exists_query_chunk_size]
                parameters = [{"name": f"@id{i}", "value": rid} for i, rid in enumerate(chunk)]
                placeholders = ", ".join(p["name"] for p in parameters)
                query = (
                    "SELECT c.id FROM c WHERE c.type = 'evaluation_result' "
                    f"AND c.id IN ({placeholders})"
                )
                rows = self._client.query_results(query, parameters)
                found.extend(row["id"] for row in rows)
            return found

        return await asyncio.to_thread(_fetch)


@dataclass
class InMemoryStore:
    telemetry: List[TelemetryRecord]
    results: List[EvaluationResult]


class InMemoryTelemetryRepository:
    def __init__(self, store: InMemoryStore) -> None:
        self._store = store

    async def fetch_telemetry(self, app_id: str, start_ts: str, end_ts: str) -> AsyncIterator[List[TelemetryRecord]]:
        records = [
            row
            for row in self._store.telemetry
            if row.app_id == app_id and start_ts <= row.timestamp < end_ts
        ]
        chunk_size = 100
        for i in range(0, len(records), chunk_size):
            yield records[i : i + chunk_size]


class InMemoryEvaluationRepository:
    def __init__(self, store: InMemoryStore) -> None:
        self._store = store

    async def save_result(self, result: EvaluationResult) -> None:
        self._store.results.append(result)

    async def save_results(self, results: List[EvaluationResult]) -> None:
        self._store.results.extend(results)

    async def latest_results(self, app_id: str, limit: int = 20) -> List[Dict]:
        items = [r.to_dict() for r in self._store.results if r.app_id == app_id]
        return sorted(items, key=lambda r: r["timestamp"], reverse=True)[:limit]

    async def result_exists(self, result_id: str) -> bool:
        return any(r.id == result_id for r in self._store.results)

    async def results_exist(self, result_ids: List[str]) -> List[str]:
        existing = {r.id for r in self._store.results}
        return [rid for rid in result_ids if rid in existing]


def _pick_telemetry_fields(row: Dict[str, Any]) -> Dict[str, Any]:
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
