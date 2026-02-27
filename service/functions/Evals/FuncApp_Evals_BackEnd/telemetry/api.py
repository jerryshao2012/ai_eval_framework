from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from flask import Flask, jsonify, request

import threading
import asyncio
from telemetry.emitter import AsyncTelemetryEmitter, BackpressureError
from telemetry.processor import validate_telemetry_event

# Manage a background thread for the asyncio event loop since Flask WSGI is synchronous
_loop: Optional[asyncio.AbstractEventLoop] = None
_emitter: Optional[AsyncTelemetryEmitter] = None
_thread: Optional[threading.Thread] = None

def _start_background_loop(loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(loop)
    loop.run_forever()

def _get_emitter_sync(
    conn: str,
    hub_name: str,
    batch_size: int,
    flush_interval_seconds: float,
    queue_max_size: int,
    enqueue_timeout_seconds: float,
) -> AsyncTelemetryEmitter:
    global _emitter, _loop, _thread
    if _emitter is None:
        _loop = asyncio.new_event_loop()
        _thread = threading.Thread(target=_start_background_loop, args=(_loop,), daemon=True)
        _thread.start()
        
        _emitter = AsyncTelemetryEmitter(
            connection_string=conn,
            eventhub_name=hub_name,
            batch_size=batch_size,
            flush_interval_seconds=flush_interval_seconds,
            queue_max_size=queue_max_size,
            enqueue_timeout_seconds=enqueue_timeout_seconds,
        )
        asyncio.run_coroutine_threadsafe(_emitter.start(), _loop).result()
    return _emitter

def create_app() -> Flask:
    app = Flask(__name__)

    @app.route("/health", methods=["GET"])
    def health() -> Any:
        return jsonify({"status": "ok"})

    @app.route("/api/telemetry", methods=["POST"])
    def ingest_telemetry() -> Any:
        payload = request.get_json(silent=True)
        if payload is None:
            return jsonify({"error": "Request body must be JSON."}), 400

        events: List[Dict[str, Any]]
        if isinstance(payload, list):
            events = payload
        elif isinstance(payload, dict) and isinstance(payload.get("events"), list):
            events = payload["events"]
        elif isinstance(payload, dict):
            events = [payload]
        else:
            return jsonify({"error": "Unsupported telemetry payload format."}), 400

        try:
            for event in events:
                validate_telemetry_event(event)
        except Exception as exc:
            return jsonify({"error": str(exc)}), 400

        conn = os.getenv("EVENTHUB_CONNECTION_STRING", "")
        hub_name = os.getenv("EVENTHUB_NAME", "")
        if not conn or not hub_name:
            return jsonify({"error": "EVENTHUB_CONNECTION_STRING and EVENTHUB_NAME must be set."}), 500

        batch_size = int(os.getenv("TELEMETRY_EMITTER_BATCH_SIZE", "100"))
        flush_interval_seconds = float(os.getenv("TELEMETRY_EMITTER_FLUSH_INTERVAL_SECONDS", "2.0"))
        queue_max_size = int(os.getenv("TELEMETRY_EMITTER_QUEUE_MAX_SIZE", "10000"))
        enqueue_timeout_seconds = float(os.getenv("TELEMETRY_EMITTER_ENQUEUE_TIMEOUT_SECONDS", "1.0"))

        emitter = _get_emitter_sync(
            conn,
            hub_name,
            batch_size=batch_size,
            flush_interval_seconds=flush_interval_seconds,
            queue_max_size=queue_max_size,
            enqueue_timeout_seconds=enqueue_timeout_seconds,
        )
        try:
            asyncio.run_coroutine_threadsafe(emitter.enqueue_events(events), _loop).result()
        except BackpressureError as exc:
            return jsonify({"error": str(exc)}), 429

        return jsonify({"accepted": len(events), "emitted": len(events)}), 202

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "7001")), debug=False)
