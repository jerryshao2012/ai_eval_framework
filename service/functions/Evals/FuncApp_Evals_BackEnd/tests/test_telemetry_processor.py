from __future__ import annotations

from typing import Any, Dict, List

from telemetry.processor import TelemetryEventProcessor, enrich_telemetry_event, validate_telemetry_event


class InMemoryTelemetrySink:
    def __init__(self) -> None:
        self.items: List[Dict[str, Any]] = []

    def upsert_telemetry(self, item: Dict[str, Any]) -> Dict[str, Any]:
        self.items.append(item)
        return item


def _sample_event() -> Dict[str, Any]:
    return {
        "app_id": "app1",
        "timestamp": "2026-02-27T00:00:00Z",
        "model_id": "m1",
        "model_version": "v1",
        "trace_id": "trace-proc-1",
        "input_text": "what is this",
        "output_text": "this is output",
        "latency_ms": 123.0,
    }


def test_validate_telemetry_event_required_fields() -> None:
    event = _sample_event()
    validate_telemetry_event(event)


def test_enrich_telemetry_event_adds_id_and_metadata() -> None:
    enriched = enrich_telemetry_event(_sample_event(), enqueued_time_utc="2026-02-27T00:00:01Z")
    assert "id" in enriched
    assert enriched["metadata"]["trace_id"] == "trace-proc-1"
    assert enriched["metadata"]["ingest_source"] == "event_hub_processor"
    assert "processed_at_utc" in enriched["metadata"]
    assert enriched["metadata"]["event_hub_enqueued_time_utc"] == "2026-02-27T00:00:01Z"


def test_processor_writes_telemetry_document_with_partition_key() -> None:
    sink = InMemoryTelemetrySink()
    processor = TelemetryEventProcessor(sink)
    processor.process_event(_sample_event())

    assert len(sink.items) == 1
    doc = sink.items[0]
    assert doc["type"] == "telemetry"
    assert doc["pk"].startswith("app1:")
    assert doc["metadata"]["trace_id"] == "trace-proc-1"
    assert doc["metadata"]["ingest_source"] == "event_hub_processor"


def test_validate_telemetry_event_requires_trace_id() -> None:
    event = _sample_event()
    event.pop("trace_id", None)
    try:
        validate_telemetry_event(event)
    except ValueError as exc:
        assert "trace_id" in str(exc)
    else:
        raise AssertionError("Expected trace_id validation error")
