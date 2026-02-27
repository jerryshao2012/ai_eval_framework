from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Optional


def _trace_id_from_current_otel_span() -> Optional[str]:
    try:
        from opentelemetry import trace  # type: ignore
    except Exception:
        return None

    try:
        span = trace.get_current_span()
        span_context = span.get_span_context()
        trace_id_int = int(getattr(span_context, "trace_id", 0))
        if trace_id_int == 0:
            return None
        return f"{trace_id_int:032x}"
    except Exception:
        return None


def _normalize_event_for_emission(event: Dict[str, Any], trace_id: Optional[str] = None) -> Dict[str, Any]:
    payload = dict(event)
    metadata = dict(payload.get("metadata") or {})
    resolved_trace_id = (
        trace_id
        or payload.get("trace_id")
        or metadata.get("trace_id")
        or _trace_id_from_current_otel_span()
    )
    if resolved_trace_id:
        payload["trace_id"] = str(resolved_trace_id)
        metadata["trace_id"] = str(resolved_trace_id)
    payload["metadata"] = metadata
    return payload


def _validate_emission_event(event: Dict[str, Any]) -> None:
    trace_id = event.get("trace_id") or (event.get("metadata") or {}).get("trace_id")
    if trace_id in (None, ""):
        raise ValueError("Telemetry event missing required field: trace_id")


def emit_telemetry_event(
    event: Dict[str, Any],
    connection_string: str,
    eventhub_name: str,
    partition_key: Optional[str] = None,
    trace_id: Optional[str] = None,
) -> int:
    return emit_telemetry_events(
        events=[event],
        connection_string=connection_string,
        eventhub_name=eventhub_name,
        partition_key=partition_key,
        trace_id=trace_id,
    )


def emit_telemetry_events(
    events: Iterable[Dict[str, Any]],
    connection_string: str,
    eventhub_name: str,
    partition_key: Optional[str] = None,
    trace_id: Optional[str] = None,
) -> int:
    event_list = [_normalize_event_for_emission(event, trace_id=trace_id) for event in events]
    for event in event_list:
        _validate_emission_event(event)

    try:
        from azure.eventhub import EventData, EventHubProducerClient
    except ImportError as exc:
        raise RuntimeError("azure-eventhub is required for telemetry emission.") from exc

    payloads: List[str] = [json.dumps(event) for event in event_list]
    if not payloads:
        return 0

    producer = EventHubProducerClient.from_connection_string(
        conn_str=connection_string,
        eventhub_name=eventhub_name,
    )
    sent = 0
    with producer:
        batch = producer.create_batch(partition_key=partition_key)
        for payload in payloads:
            data = EventData(payload)
            if not batch.try_add(data):
                producer.send_batch(batch)
                sent += len(batch)
                batch = producer.create_batch(partition_key=partition_key)
                if not batch.try_add(data):
                    raise ValueError("Single telemetry event is too large for Event Hubs batch.")
        if len(batch) > 0:
            producer.send_batch(batch)
            sent += len(batch)
    return sent
