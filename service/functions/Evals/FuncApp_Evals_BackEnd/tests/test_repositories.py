from __future__ import annotations

from typing import Any, Dict, List

import pytest

from data.repositories import CosmosTelemetryRepository, _pick_telemetry_fields


class _FakePager:
    def __init__(self, pages: List[List[Dict[str, Any]]]) -> None:
        self._pages = pages

    def by_page(self):
        return iter(self._pages)


class _FakeCosmosClient:
    def __init__(self) -> None:
        self.partition_keys: List[str] = []

    def query_telemetry_paged(
        self,
        query: str,
        parameters: List[Dict[str, Any]],
        max_item_count: int = 100,
        partition_key: str | None = None,
    ) -> _FakePager:
        if partition_key is not None:
            self.partition_keys.append(partition_key)
        # Return one page with one minimal telemetry item for each partition query.
        return _FakePager(
            [
                [
                    {
                        "id": f"row-{partition_key}",
                        "app_id": "app1",
                        "timestamp": "2026-02-24T00:00:00Z",
                        "model_id": "m",
                        "model_version": "v",
                        "input_text": "in",
                        "output_text": "out",
                        "metadata": {"trace_id": "t"},
                    }
                ]
            ]
        )

def test_pick_telemetry_fields_preserves_existing_metadata_trace_id() -> None:
    row = {
        "id": "t1",
        "app_id": "app1",
        "timestamp": "2026-02-24T00:00:00Z",
        "model_id": "m",
        "model_version": "v",
        "input_text": "in",
        "output_text": "out",
        "metadata": {"trace_id": "from-metadata"},
        "trace_id": "from-top-level",
        "ignored": "x",
    }

    payload = _pick_telemetry_fields(row)
    assert payload["metadata"]["trace_id"] == "from-metadata"
    assert "ignored" not in payload


def test_pick_telemetry_fields_promotes_top_level_trace_id() -> None:
    row = {
        "id": "t2",
        "app_id": "app1",
        "timestamp": "2026-02-24T00:00:00Z",
        "model_id": "m",
        "model_version": "v",
        "input_text": "in",
        "output_text": "out",
        "trace_id": "top-level-trace",
    }

    payload = _pick_telemetry_fields(row)
    assert payload["metadata"]["trace_id"] == "top-level-trace"


@pytest.mark.asyncio
async def test_cosmos_telemetry_repository_queries_date_sliced_partitions() -> None:
    client = _FakeCosmosClient()
    repo = CosmosTelemetryRepository(client, page_size=10)

    chunks = [
        chunk
        async for chunk in repo.fetch_telemetry(
            app_id="app1",
            start_ts="2026-02-24T23:00:00Z",
            end_ts="2026-02-26T01:00:00Z",
        )
    ]

    assert chunks
    assert client.partition_keys == [
        "app1:2026-02-24",
        "app1:2026-02-25",
        "app1:2026-02-26",
    ]
