from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from flask import Flask, jsonify, render_template, request

from config.loader import load_config
from config.models import ThresholdConfig
from data.models import MetricValueVersioned, ThresholdBreach
from evaluation.thresholds import evaluate_thresholds

BASE_DIR = Path(__file__).resolve().parent
MOCK_FILE = BASE_DIR / "mock_results.json"
CONFIG_FILE = BASE_DIR.parent / "config" / "config.yaml"

app = Flask(__name__, template_folder="templates", static_folder="static")

try:
    ROOT_CONFIG = load_config(str(CONFIG_FILE))
    DEFAULT_THRESHOLD_MAP = ROOT_CONFIG.global_thresholds
except Exception:
    DEFAULT_THRESHOLD_MAP = {}


def _load_mock_data() -> Dict[str, Any]:
    return json.loads(MOCK_FILE.read_text())


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


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
