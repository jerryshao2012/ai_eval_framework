from __future__ import annotations

import os
from typing import Any, Dict, List

from flask import Flask, jsonify, request

from telemetry.emitter import emit_telemetry_events
from telemetry.processor import validate_telemetry_event


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

        emitted = emit_telemetry_events(events, connection_string=conn, eventhub_name=hub_name)
        return jsonify({"accepted": len(events), "emitted": emitted}), 202

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "7001")), debug=False)
