from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Optional


def emit_telemetry_event(
    event: Dict[str, Any],
    connection_string: str,
    eventhub_name: str,
    partition_key: Optional[str] = None,
) -> int:
    return emit_telemetry_events(
        events=[event],
        connection_string=connection_string,
        eventhub_name=eventhub_name,
        partition_key=partition_key,
    )


def emit_telemetry_events(
    events: Iterable[Dict[str, Any]],
    connection_string: str,
    eventhub_name: str,
    partition_key: Optional[str] = None,
) -> int:
    try:
        from azure.eventhub import EventData, EventHubProducerClient
    except ImportError as exc:
        raise RuntimeError("azure-eventhub is required for telemetry emission.") from exc

    payloads: List[str] = [json.dumps(event) for event in events]
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
