from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from flask import Flask, jsonify, render_template, request, Response

from config.loader import load_config
from config.models import ThresholdConfig
from data.models import MetricValueVersioned, ThresholdBreach
from evaluation.thresholds import evaluate_thresholds

BASE_DIR = Path(__file__).resolve().parent
MOCK_FILE = BASE_DIR / "mock_results.json"
STATUS_FILE = BASE_DIR / "batch_status.json"
CONFIG_FILE = BASE_DIR.parent / "config" / "config.yaml"

app = Flask(__name__, template_folder="templates", static_folder="static")

try:
    ROOT_CONFIG = load_config(str(CONFIG_FILE))
    DEFAULT_THRESHOLD_MAP = ROOT_CONFIG.global_thresholds
except Exception:
    DEFAULT_THRESHOLD_MAP = {}


def _openapi_spec() -> Dict[str, Any]:
    return {
        "openapi": "3.0.3",
        "info": {
            "title": "AI Evaluation Dashboard API",
            "version": "1.0.0",
            "description": "APIs for evaluation metrics, alerts, thresholds, and batch job execution status.",
        },
        "paths": {
            "/api/latest": {"get": {"summary": "Get latest app evaluation status"}},
            "/api/trends/{app_id}": {
                "get": {
                    "summary": "Get trend points for one app",
                    "parameters": [
                        {"name": "app_id", "in": "path", "required": True, "schema": {"type": "string"}}
                    ],
                }
            },
            "/api/alerts": {"get": {"summary": "Get threshold breaches for latest data"}},
            "/api/thresholds": {"get": {"summary": "Get active threshold configuration"}},
            "/api/batch/current": {"get": {"summary": "Get current (or latest) batch run"}},
            "/api/batch/history": {"get": {"summary": "Get batch run history"}},
            "/api/batch/run/{run_id}": {
                "get": {
                    "summary": "Get details for a batch run",
                    "parameters": [
                        {"name": "run_id", "in": "path", "required": True, "schema": {"type": "string"}}
                    ],
                }
            },
            "/api/batch/run/{run_id}/item/{item_id}/logs": {
                "get": {
                    "summary": "Get item logs and traceback for one batch run item",
                    "parameters": [
                        {"name": "run_id", "in": "path", "required": True, "schema": {"type": "string"}},
                        {"name": "item_id", "in": "path", "required": True, "schema": {"type": "string"}},
                    ],
                }
            },
            "/api/openapi.json": {"get": {"summary": "Get OpenAPI spec"}},
            "/api/docs": {"get": {"summary": "Swagger UI documentation"}},
        },
    }


def _load_mock_data() -> Dict[str, Any]:
    return json.loads(MOCK_FILE.read_text())


def _load_status_data() -> Dict[str, Any]:
    if not STATUS_FILE.exists():
        return {"runs": []}
    return json.loads(STATUS_FILE.read_text())


def _compute_run_stats(run: Dict[str, Any]) -> Dict[str, Any]:
    items = run.get("items", [])
    total = len(items)
    completed = sum(1 for i in items if i.get("status") == "completed")
    failed = sum(1 for i in items if i.get("status") == "failed")
    running = sum(1 for i in items if i.get("status") == "running")
    pending = sum(1 for i in items if i.get("status") == "pending")
    breaches = sum(int(i.get("breach_count", 0)) for i in items)
    policy_runs = sum(int(i.get("policy_runs", 0)) for i in items)
    return {
        "total_items": total,
        "completed_items": completed,
        "failed_items": failed,
        "running_items": running,
        "pending_items": pending,
        "total_breaches": breaches,
        "total_policy_runs": policy_runs,
        "success_rate": (completed / total) if total else 0.0,
    }


def _sorted_runs() -> List[Dict[str, Any]]:
    data = _load_status_data()
    runs = data.get("runs", [])
    return sorted(runs, key=lambda r: r.get("started_at", ""), reverse=True)


def _run_with_stats(run: Dict[str, Any]) -> Dict[str, Any]:
    enriched = dict(run)
    enriched["stats"] = _compute_run_stats(run)
    return enriched


def _clone_threshold_map(
    threshold_map: Dict[str, List[ThresholdConfig]],
) -> Dict[str, List[ThresholdConfig]]:
    return {
        metric: [
            ThresholdConfig(level=t.level, value=t.value, direction=t.direction)
            for t in entries
        ]
        for metric, entries in threshold_map.items()
    }


def _upsert_threshold(
    threshold_map: Dict[str, List[ThresholdConfig]],
    metric_name: str,
    level: str,
    value: float,
    direction: str,
) -> None:
    entries = threshold_map.setdefault(metric_name, [])
    for i, entry in enumerate(entries):
        if entry.level == level:
            entries[i] = ThresholdConfig(level=level, value=value, direction=direction)
            return
    entries.append(ThresholdConfig(level=level, value=value, direction=direction))


def _active_threshold_map() -> Dict[str, List[ThresholdConfig]]:
    threshold_map = _clone_threshold_map(DEFAULT_THRESHOLD_MAP)
    use_dynamic = request.args.get("dynamic_thresholds", "").lower() in {"1", "true", "yes", "on"}
    if not use_dynamic:
        return threshold_map

    for key, raw_value in request.args.items():
        if not key.startswith("threshold."):
            continue
        parts = key.split(".")
        if len(parts) != 3:
            continue
        _, metric_name, level = parts
        direction = request.args.get(f"direction.{metric_name}.{level}", "min").strip().lower()
        if direction not in {"min", "max"}:
            direction = "min"
        _upsert_threshold(
            threshold_map=threshold_map,
            metric_name=metric_name,
            level=level,
            value=float(raw_value),
            direction=direction,
        )
    return threshold_map


def _make_metric(metric: Dict[str, Any], fallback_ts: str) -> MetricValueVersioned:
    return MetricValueVersioned(
        metric_name=metric["metric_name"],
        value=float(metric["value"]),
        version=str(metric.get("version", "dashboard")),
        timestamp=str(metric.get("timestamp", fallback_ts)),
        metadata=metric.get("metadata", {}),
    )


def _status_for_breaches(breaches: List[ThresholdBreach]) -> str:
    if any(b.level == "critical" for b in breaches):
        return "critical"
    if breaches:
        return "warning"
    return "healthy"


def _latest_with_breaches(
    latest_rows: List[Dict[str, Any]],
    threshold_map: Dict[str, List[ThresholdConfig]],
) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    for row in latest_rows:
        metrics = [_make_metric(metric, row.get("timestamp", "")) for metric in row.get("metrics", [])]
        breaches = evaluate_thresholds(metrics, threshold_map)
        enriched = dict(row)
        enriched["breaches"] = [b.to_dict() for b in breaches]
        enriched["status"] = _status_for_breaches(breaches)
        output.append(enriched)
    return output


@app.route("/")
def index() -> str:
    return render_template("index.html")


@app.route("/api/latest")
def latest() -> Any:
    data = _load_mock_data()
    threshold_map = _active_threshold_map()
    return jsonify(_latest_with_breaches(data["latest"], threshold_map))


@app.route("/api/trends/<app_id>")
def trends(app_id: str) -> Any:
    data = _load_mock_data()
    trend = data["trends"].get(app_id, [])
    return jsonify(trend)


@app.route("/api/alerts")
def alerts() -> Any:
    data = _load_mock_data()
    threshold_map = _active_threshold_map()
    latest_rows = _latest_with_breaches(data["latest"], threshold_map)
    all_alerts: List[Dict[str, Any]] = []
    for item in latest_rows:
        all_alerts.extend(item.get("breaches", []))
    return jsonify(all_alerts)


@app.route("/api/thresholds")
def thresholds() -> Any:
    threshold_map = _active_threshold_map()
    serialized = {
        metric: [t.__dict__ for t in entries]
        for metric, entries in threshold_map.items()
    }
    dynamic = request.args.get("dynamic_thresholds", "").lower() in {"1", "true", "yes", "on"}
    return jsonify({"dynamic_thresholds": dynamic, "thresholds": serialized})


@app.route("/api/batch/current")
def batch_current() -> Any:
    runs = _sorted_runs()
    running = next((r for r in runs if r.get("status") == "running"), None)
    run = running or (runs[0] if runs else None)
    if run is None:
        return jsonify(None)
    return jsonify(_run_with_stats(run))


@app.route("/api/batch/history")
def batch_history() -> Any:
    runs = [_run_with_stats(r) for r in _sorted_runs()]
    return jsonify(runs)


@app.route("/api/batch/run/<run_id>")
def batch_run_detail(run_id: str) -> Any:
    run = next((r for r in _sorted_runs() if r.get("run_id") == run_id), None)
    if run is None:
        return jsonify({"error": "run not found"}), 404
    return jsonify(_run_with_stats(run))


@app.route("/api/batch/run/<run_id>/item/<item_id>/logs")
def batch_item_logs(run_id: str, item_id: str) -> Any:
    run = next((r for r in _sorted_runs() if r.get("run_id") == run_id), None)
    if run is None:
        return jsonify({"error": "run not found"}), 404
    item = next((i for i in run.get("items", []) if i.get("item_id") == item_id), None)
    if item is None:
        return jsonify({"error": "item not found"}), 404
    return jsonify(
        {
            "run_id": run_id,
            "item_id": item_id,
            "status": item.get("status"),
            "error": item.get("error"),
            "traceback": item.get("traceback"),
            "logs": item.get("logs", []),
        }
    )


@app.route("/api/openapi.json")
def openapi() -> Any:
    return jsonify(_openapi_spec())


@app.route("/api/docs")
def docs() -> Response:
    html = """<!doctype html>
<html>
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>AI Evaluation API Docs</title>
    <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist/swagger-ui.css" />
  </head>
  <body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist/swagger-ui-bundle.js"></script>
    <script>
      SwaggerUIBundle({
        url: '/api/openapi.json',
        dom_id: '#swagger-ui'
      });
    </script>
  </body>
</html>
"""
    return Response(html, mimetype="text/html")


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
