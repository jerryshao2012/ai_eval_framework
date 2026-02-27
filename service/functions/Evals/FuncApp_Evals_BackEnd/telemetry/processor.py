from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional, Protocol
from uuid import uuid4

from data.models import TelemetryRecord

logger = logging.getLogger(__name__)


class TelemetrySink(Protocol):
    def upsert_telemetry(self, item: Dict[str, Any]) -> Dict[str, Any]:
        ...


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def validate_telemetry_event(event: Dict[str, Any]) -> None:
    required = ["app_id", "timestamp", "model_id", "model_version", "input_text", "output_text"]
    missing = [key for key in required if key not in event or event[key] in (None, "")]
    if missing:
        raise ValueError(f"Telemetry event missing required fields: {', '.join(missing)}")
    trace_id = event.get("trace_id") or (event.get("metadata") or {}).get("trace_id")
    if trace_id in (None, ""):
        raise ValueError("Telemetry event missing required field: trace_id")


def enrich_telemetry_event(
    event: Dict[str, Any],
    source: str = "event_hub_processor",
    enqueued_time_utc: Optional[str] = None,
) -> Dict[str, Any]:
    enriched = dict(event)
    enriched.setdefault("id", f"{enriched.get('app_id', 'unknown')}:{uuid4().hex}")
    enriched.setdefault("metadata", {})
    metadata = dict(enriched.get("metadata") or {})
    # Keep trace identity in metadata for downstream dedupe logic.
    trace_id = enriched.get("trace_id") or metadata.get("trace_id")
    if trace_id:
        metadata["trace_id"] = str(trace_id)
    metadata["ingest_source"] = source
    metadata["processed_at_utc"] = _utc_now_iso()
    if enqueued_time_utc:
        metadata["event_hub_enqueued_time_utc"] = enqueued_time_utc
    enriched["metadata"] = metadata
    return enriched


class TelemetryEventProcessor:
    def __init__(self, sink: TelemetrySink) -> None:
        self._sink = sink

    def process_event(self, event: Dict[str, Any], enqueued_time_utc: Optional[str] = None) -> Dict[str, Any]:
        validate_telemetry_event(event)
        enriched = enrich_telemetry_event(event, enqueued_time_utc=enqueued_time_utc)
        record = TelemetryRecord(
            id=str(enriched["id"]),
            app_id=str(enriched["app_id"]),
            timestamp=str(enriched["timestamp"]),
            model_id=str(enriched["model_id"]),
            model_version=str(enriched["model_version"]),
            input_text=str(enriched["input_text"]),
            output_text=str(enriched["output_text"]),
            expected_output=enriched.get("expected_output"),
            user_id=enriched.get("user_id"),
            latency_ms=float(enriched["latency_ms"]) if enriched.get("latency_ms") is not None else None,
            metadata=dict(enriched.get("metadata", {})),
        )
        return self._sink.upsert_telemetry(record.to_dict())

    def process_events(self, events: Iterable[Dict[str, Any]]) -> int:
        count = 0
        for event in events:
            self.process_event(event)
            count += 1
        return count


def run_eventhub_processor_loop(
    processor: TelemetryEventProcessor,
    connection_string: str,
    eventhub_name: str,
    consumer_group: str = "$Default",
) -> None:
    try:
        from azure.eventhub import EventHubConsumerClient
    except ImportError as exc:
        raise RuntimeError("azure-eventhub is required for Event Hubs processing.") from exc

    client = EventHubConsumerClient.from_connection_string(
        conn_str=connection_string,
        eventhub_name=eventhub_name,
        consumer_group=consumer_group,
    )

    def on_event(partition_context, event) -> None:
        body = event.body_as_str(encoding="UTF-8")
        enqueued_time = event.enqueued_time.isoformat() if event.enqueued_time else None
        try:
            payload = json.loads(body)
            processor.process_event(payload, enqueued_time_utc=enqueued_time)
            partition_context.update_checkpoint(event)
        except Exception:
            logger.exception("Failed to process telemetry event from partition=%s", partition_context.partition_id)

    with client:
        client.receive(
            on_event=on_event,
            starting_position="-1",
        )
