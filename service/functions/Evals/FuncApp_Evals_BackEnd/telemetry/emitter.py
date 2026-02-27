from __future__ import annotations

import asyncio
import gzip
import json
import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)


class BackpressureError(RuntimeError):
    """Raised when the telemetry queue is saturated and enqueue times out."""


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


class AsyncTelemetryEmitter:
    """Asynchronous telemetry emitter with queueing, batching and compression."""

    def __init__(
        self,
        connection_string: str,
        eventhub_name: str,
        batch_size: int = 100,
        flush_interval_seconds: float = 2.0,
        queue_max_size: int = 10000,
        enqueue_timeout_seconds: float = 1.0,
    ) -> None:
        self._connection_string = connection_string
        self._eventhub_name = eventhub_name
        self._batch_size = max(1, int(batch_size))
        self._flush_interval = max(0.01, float(flush_interval_seconds))
        self._enqueue_timeout_seconds = max(0.01, float(enqueue_timeout_seconds))

        self._queue: asyncio.Queue[Tuple[Optional[str], Dict[str, Any]]] = asyncio.Queue(
            maxsize=max(1, int(queue_max_size))
        )
        self._worker_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

    @property
    def queue_size(self) -> int:
        return self._queue.qsize()

    @property
    def queue_capacity(self) -> int:
        return int(self._queue.maxsize)

    async def start(self) -> None:
        if self._worker_task is None:
            self._stop_event.clear()
            self._worker_task = asyncio.create_task(self._worker_loop())
            logger.info("AsyncTelemetryEmitter started.")

    async def stop(self) -> None:
        if self._worker_task:
            self._stop_event.set()
            await self._worker_task
            self._worker_task = None
            logger.info("AsyncTelemetryEmitter stopped.")

    async def enqueue_event(
        self,
        event: Dict[str, Any],
        partition_key: Optional[str] = None,
        trace_id: Optional[str] = None,
    ) -> None:
        normalized = _normalize_event_for_emission(event, trace_id=trace_id)
        _validate_emission_event(normalized)
        item = (partition_key, normalized)
        try:
            await asyncio.wait_for(self._queue.put(item), timeout=self._enqueue_timeout_seconds)
        except asyncio.TimeoutError as exc:
            raise BackpressureError(
                f"Telemetry queue is full ({self._queue.qsize()}/{self._queue.maxsize})."
            ) from exc

    async def enqueue_events(
        self,
        events: Sequence[Dict[str, Any]],
        partition_key: Optional[str] = None,
    ) -> None:
        for event in events:
            await self.enqueue_event(event, partition_key=partition_key)

    async def _worker_loop(self) -> None:
        while not self._stop_event.is_set() or not self._queue.empty():
            batch: List[Tuple[Optional[str], Dict[str, Any]]] = []
            try:
                first = await asyncio.wait_for(self._queue.get(), timeout=self._flush_interval)
                batch.append(first)

                while len(batch) < self._batch_size and not self._queue.empty():
                    batch.append(self._queue.get_nowait())
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Error reading telemetry queue")
                continue

            try:
                await asyncio.to_thread(self._send_batch_sync, batch)
            except Exception:
                logger.exception("Failed to emit telemetry batch")
            finally:
                for _ in batch:
                    self._queue.task_done()

    def _send_batch_sync(self, batch: List[Tuple[Optional[str], Dict[str, Any]]]) -> None:
        try:
            from azure.eventhub import EventData, EventHubProducerClient
        except ImportError as exc:
            raise RuntimeError("azure-eventhub is required for telemetry emission.") from exc

        grouped: Dict[Optional[str], List[Dict[str, Any]]] = defaultdict(list)
        for pkey, evt in batch:
            grouped[pkey].append(evt)

        producer = EventHubProducerClient.from_connection_string(
            conn_str=self._connection_string,
            eventhub_name=self._eventhub_name,
        )

        with producer:
            for pkey, events in grouped.items():
                self._send_grouped_events(producer, EventData, pkey, events)

    def _send_grouped_events(
        self,
        producer: Any,
        event_data_cls: Any,
        partition_key: Optional[str],
        events: Sequence[Dict[str, Any]],
    ) -> None:
        start = 0
        total = len(events)
        while start < total:
            end = min(total, start + self._batch_size)
            sent = self._send_largest_fitting_chunk(
                producer,
                event_data_cls,
                partition_key,
                events,
                start,
                end,
            )
            start = sent

    def _send_largest_fitting_chunk(
        self,
        producer: Any,
        event_data_cls: Any,
        partition_key: Optional[str],
        events: Sequence[Dict[str, Any]],
        start: int,
        end: int,
    ) -> int:
        while end > start:
            window = list(events[start:end])
            payload_json = json.dumps(window, separators=(",", ":")).encode("utf-8")
            compressed_payload = gzip.compress(payload_json)

            eh_batch = producer.create_batch(partition_key=partition_key)
            data = event_data_cls(compressed_payload)
            data.properties = {b"content-encoding": b"gzip", b"batch-type": b"json-array"}

            if eh_batch.try_add(data):
                producer.send_batch(eh_batch)
                return end

            if end - start == 1:
                raise ValueError("Compressed telemetry event exceeds Event Hubs message limit.")

            end = start + max(1, (end - start) // 2)

        raise ValueError("Unable to fit telemetry payload into Event Hubs batch.")
