from __future__ import annotations

import gzip
import json
import sys
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

import pytest

from telemetry.emitter import (
    AsyncTelemetryEmitter,
    BackpressureError,
    _normalize_event_for_emission,
    _validate_emission_event,
)


def test_validate_emission_event_accepts_top_level_trace_id() -> None:
    event = {"app_id": "app1", "trace_id": "trace-123"}
    _validate_emission_event(event)


def test_validate_emission_event_accepts_metadata_trace_id() -> None:
    event = {"app_id": "app1", "metadata": {"trace_id": "trace-456"}}
    _validate_emission_event(event)


def test_validate_emission_event_rejects_missing_trace_id() -> None:
    event = {"app_id": "app1"}
    try:
        _validate_emission_event(event)
    except ValueError as exc:
        assert "trace_id" in str(exc)
    else:
        raise AssertionError("Expected trace_id validation error")


def test_normalize_event_for_emission_applies_explicit_trace_id() -> None:
    payload = _normalize_event_for_emission({"app_id": "app1"}, trace_id="trace-explicit")
    assert payload["trace_id"] == "trace-explicit"
    assert payload["metadata"]["trace_id"] == "trace-explicit"


def test_normalize_event_for_emission_preserves_existing_trace_id() -> None:
    payload = _normalize_event_for_emission({"app_id": "app1", "metadata": {"trace_id": "trace-existing"}})
    assert payload["trace_id"] == "trace-existing"
    assert payload["metadata"]["trace_id"] == "trace-existing"


@pytest.mark.asyncio
async def test_async_emitter_backpressure_when_queue_is_full() -> None:
    emitter = AsyncTelemetryEmitter(
        connection_string="Endpoint=sb://fake",
        eventhub_name="hub",
        queue_max_size=1,
        enqueue_timeout_seconds=0.01,
    )

    await emitter.enqueue_event({"app_id": "app1", "trace_id": "trace-1"})
    with pytest.raises(BackpressureError):
        await emitter.enqueue_event({"app_id": "app1", "trace_id": "trace-2"})


def test_send_batch_sync_emits_gzip_json_array_with_properties() -> None:
    sent_payloads: List[bytes] = []
    sent_properties: List[Dict[bytes, bytes]] = []

    class FakeEventData:
        def __init__(self, body: bytes) -> None:
            self.body = body
            self.properties: Dict[bytes, bytes] = {}

    class FakeBatch:
        def __init__(self) -> None:
            self.items: List[FakeEventData] = []

        def try_add(self, data: FakeEventData) -> bool:
            self.items.append(data)
            sent_payloads.append(data.body)
            sent_properties.append(data.properties)
            return True

    class FakeProducer:
        def __enter__(self) -> "FakeProducer":
            return self

        def __exit__(self, exc_type, exc, tb) -> Optional[bool]:
            return None

        def create_batch(self, partition_key: Optional[str] = None) -> FakeBatch:
            return FakeBatch()

        def send_batch(self, batch: FakeBatch) -> None:
            return None

    fake_client_factory = MagicMock()
    fake_client_factory.from_connection_string.return_value = FakeProducer()

    mock_azure = MagicMock()
    mock_azure.EventData = FakeEventData
    mock_azure.EventHubProducerClient = fake_client_factory
    sys.modules["azure.eventhub"] = mock_azure
    try:
        emitter = AsyncTelemetryEmitter(connection_string="Endpoint=sb://fake", eventhub_name="hub")
        emitter._send_batch_sync(
            [
                (None, {"app_id": "app1", "trace_id": "trace-a"}),
                (None, {"app_id": "app1", "trace_id": "trace-b"}),
            ]
        )
    finally:
        sys.modules.pop("azure.eventhub", None)

    assert len(sent_payloads) == 1
    decoded = gzip.decompress(sent_payloads[0]).decode("utf-8")
    payload = json.loads(decoded)
    assert len(payload) == 2
    assert sent_properties[0][b"content-encoding"] == b"gzip"
    assert sent_properties[0][b"batch-type"] == b"json-array"
