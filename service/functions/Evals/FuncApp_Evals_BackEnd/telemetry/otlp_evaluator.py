from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import resource
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Tuple

import ijson
from flask import Flask, jsonify, request

from config.loader import load_config, resolve_app_config
from data.cosmos_client import CosmosDbClient
from data.models import EvaluationResult, TelemetryRecord
from evaluation.policies import EvaluationPolicy, build_policy_registry

logger = logging.getLogger(__name__)


def _value_from_otlp_attr(value_obj: Dict[str, Any]) -> Any:
    for key in (
        "stringValue",
        "intValue",
        "doubleValue",
        "boolValue",
    ):
        if key in value_obj:
            return value_obj[key]
    return None


def _otlp_attrs_to_dict(attrs: List[Dict[str, Any]]) -> Dict[str, Any]:
    output: Dict[str, Any] = {}
    for attr in attrs:
        key = attr.get("key")
        value_obj = attr.get("value", {})
        if key:
            output[str(key)] = _value_from_otlp_attr(value_obj)
    return output


def _iso_from_unix_nano(raw: str | int | None) -> str:
    if not raw:
        return datetime.now(timezone.utc).isoformat()
    nanos = int(raw)
    seconds = nanos / 1_000_000_000
    return datetime.fromtimestamp(seconds, tz=timezone.utc).isoformat()


def _stable_result_id(app_id: str, policy_name: str, trace_id: str, value_version: str) -> str:
    digest = hashlib.sha1(f"{app_id}|{policy_name}|{trace_id}|{value_version}".encode("utf-8")).hexdigest()[:16]
    return f"{app_id}:{policy_name}:{trace_id}:{value_version}:{digest}"


class OtlpTraceEvaluationService:
    def __init__(self, config_path: str, cosmos_client: Any | None = None) -> None:
        self.root_config = load_config(config_path)
        if self.root_config.cosmos is None:
            raise ValueError("Cosmos DB configuration is required.")
        self.cosmos = cosmos_client or CosmosDbClient(self.root_config.cosmos)
        self.policy_registry = build_policy_registry()
        self._max_events_per_request = max(1, int(self.root_config.otlp_max_events_per_request))
        self._memory_warn_mb = max(1, int(self.root_config.memory_usage_warn_mb))
        self._memory_hard_limit_mb = max(0, int(self.root_config.memory_usage_hard_limit_mb))

    def process_otlp_traces(self, payload: Dict[str, Any]) -> Dict[str, int]:
        return self._process_records(self._extract_telemetry_records(payload))

    def process_otlp_trace_file(self, otlp_file_path: str) -> Dict[str, int]:
        path = Path(otlp_file_path)
        if not path.exists():
            raise FileNotFoundError(f"OTLP file not found: {path}")
        return self._process_records(self._extract_telemetry_records_from_file(path))

    def _process_records(self, records: Iterator[TelemetryRecord]) -> Dict[str, int]:
        created = 0
        skipped = 0
        seen: set[Tuple[str, str]] = set()
        count = 0

        for record in records:
            count += 1
            if count > self._max_events_per_request:
                raise ValueError(
                    f"OTLP request exceeds max event limit ({self._max_events_per_request})."
                )
            if count % 500 == 0:
                self._enforce_memory_limits(count)
            self.cosmos.upsert_telemetry(record.to_dict())
            trace_id = str(record.metadata.get("trace_id", ""))
            dedupe_key = (record.app_id, trace_id)
            if trace_id and dedupe_key in seen:
                skipped += 1
                continue
            seen.add(dedupe_key)
            c, s = self._evaluate_trace_record(record)
            created += c
            skipped += s

        self._enforce_memory_limits(count)
        return {
            "telemetry_events_ingested": count,
            "evaluations_created": created,
            "evaluations_skipped_duplicate": skipped,
        }

    def _extract_telemetry_records(self, payload: Dict[str, Any]) -> Iterator[TelemetryRecord]:
        for resource_spans in payload.get("resourceSpans", []):
            resource_attrs = _otlp_attrs_to_dict(resource_spans.get("resource", {}).get("attributes", []))
            for scope_spans in resource_spans.get("scopeSpans", []):
                for span in scope_spans.get("spans", []):
                    span_attrs = _otlp_attrs_to_dict(span.get("attributes", []))
                    attrs = {**resource_attrs, **span_attrs}

                    app_id = str(attrs.get("app_id", attrs.get("service.name", "unknown-app")))
                    trace_id = str(span.get("traceId", attrs.get("trace_id", "")))
                    span_id = str(span.get("spanId", attrs.get("span_id", "")))
                    ts = _iso_from_unix_nano(span.get("startTimeUnixNano"))
                    latency_ms = attrs.get("latency_ms", attrs.get("duration_ms"))
                    latency_value = float(latency_ms) if latency_ms not in (None, "") else None

                    record = TelemetryRecord(
                        id=str(attrs.get("event_id", f"{app_id}:{trace_id}:{span_id or 'span'}")),
                        app_id=app_id,
                        timestamp=ts,
                        model_id=str(attrs.get("model_id", attrs.get("llm.model", "unknown-model"))),
                        model_version=str(attrs.get("model_version", attrs.get("llm.model_version", "unknown-version"))),
                        input_text=str(attrs.get("input_text", attrs.get("llm.input", ""))),
                        output_text=str(attrs.get("output_text", attrs.get("llm.output", ""))),
                        user_id=str(attrs.get("user_id")) if attrs.get("user_id") is not None else None,
                        latency_ms=latency_value,
                        metadata={
                            "trace_id": trace_id,
                            "span_id": span_id,
                            "service_name": attrs.get("service.name"),
                            "otlp_attributes": attrs,
                        },
                    )
                    self._validate_minimum_record(record)
                    yield record

    def _extract_telemetry_records_from_file(self, path: Path) -> Iterator[TelemetryRecord]:
        with path.open("rb") as f:
            for resource_spans in ijson.items(f, "resourceSpans.item"):
                resource_attrs = _otlp_attrs_to_dict(resource_spans.get("resource", {}).get("attributes", []))
                for scope_spans in resource_spans.get("scopeSpans", []):
                    for span in scope_spans.get("spans", []):
                        span_attrs = _otlp_attrs_to_dict(span.get("attributes", []))
                        attrs = {**resource_attrs, **span_attrs}

                        app_id = str(attrs.get("app_id", attrs.get("service.name", "unknown-app")))
                        trace_id = str(span.get("traceId", attrs.get("trace_id", "")))
                        span_id = str(span.get("spanId", attrs.get("span_id", "")))
                        ts = _iso_from_unix_nano(span.get("startTimeUnixNano"))
                        latency_ms = attrs.get("latency_ms", attrs.get("duration_ms"))
                        latency_value = float(latency_ms) if latency_ms not in (None, "") else None

                        record = TelemetryRecord(
                            id=str(attrs.get("event_id", f"{app_id}:{trace_id}:{span_id or 'span'}")),
                            app_id=app_id,
                            timestamp=ts,
                            model_id=str(attrs.get("model_id", attrs.get("llm.model", "unknown-model"))),
                            model_version=str(attrs.get("model_version", attrs.get("llm.model_version", "unknown-version"))),
                            input_text=str(attrs.get("input_text", attrs.get("llm.input", ""))),
                            output_text=str(attrs.get("output_text", attrs.get("llm.output", ""))),
                            user_id=str(attrs.get("user_id")) if attrs.get("user_id") is not None else None,
                            latency_ms=latency_value,
                            metadata={
                                "trace_id": trace_id,
                                "span_id": span_id,
                                "service_name": attrs.get("service.name"),
                                "otlp_attributes": attrs,
                            },
                        )
                        self._validate_minimum_record(record)
                        yield record

    def _validate_minimum_record(self, record: TelemetryRecord) -> None:
        required = {
            "app_id": record.app_id,
            "model_id": record.model_id,
            "model_version": record.model_version,
            "input_text": record.input_text,
            "output_text": record.output_text,
        }
        missing = [key for key, value in required.items() if value in ("", None)]
        if missing:
            raise ValueError(f"OTLP event missing required telemetry fields: {', '.join(missing)}")

    def _evaluate_trace_record(self, record: TelemetryRecord) -> Tuple[int, int]:
        app_cfg = resolve_app_config(self.root_config, record.app_id)
        created = 0
        skipped = 0
        trace_id = str(record.metadata.get("trace_id", ""))

        for policy_name in app_cfg.policy_names:
            if policy_name not in self.policy_registry:
                logger.warning("Skipping unregistered policy=%s for app_id=%s", policy_name, record.app_id)
                continue
            policy_cfg = next((p for p in app_cfg.policies if p.name == policy_name), None)
            if policy_cfg is None:
                logger.warning("Skipping missing policy config policy=%s for app_id=%s", policy_name, record.app_id)
                continue

            value_version = str(policy_cfg.parameters.get("version", "1.0"))
            result_id = _stable_result_id(record.app_id, policy_name, trace_id or "no-trace", value_version)
            if self._result_exists(result_id):
                skipped += 1
                continue

            policy_type = self.policy_registry[policy_name]
            policy: EvaluationPolicy = policy_type(policy_cfg)
            metrics = asyncio.run(policy.evaluate(record.app_id, [record]))
            for metric in metrics:
                metric.metadata = {
                    **metric.metadata,
                    "trace_id": trace_id,
                    "span_id": record.metadata.get("span_id"),
                    "policy_name": policy_name,
                    "policy_version": value_version,
                    "value_object_type": "metric_value_versioned",
                    "value_object_version": metric.version or value_version,
                }
                if not metric.metric_type:
                    metric.metric_type = metric.metric_name

            result = EvaluationResult(
                id=result_id,
                app_id=record.app_id,
                timestamp=datetime.now(timezone.utc).isoformat(),
                policy_name=policy_name,
                metrics=metrics,
                breaches=[],
            )
            self.cosmos.upsert_result(result.to_dict())
            created += 1
        return created, skipped

    def _result_exists(self, result_id: str) -> bool:
        rows = self.cosmos.query_results(
            "SELECT TOP 1 c.id FROM c WHERE c.id = @id",
            [{"name": "@id", "value": result_id}],
        )
        return bool(rows)

    def _current_memory_mb(self) -> float:
        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if sys.platform == "darwin":
            return usage / (1024 * 1024)
        return usage / 1024

    def _enforce_memory_limits(self, processed_count: int) -> None:
        current_mb = self._current_memory_mb()
        if current_mb >= self._memory_warn_mb:
            logger.warning(
                "OTLP evaluator memory usage high: %.1f MB after %d events",
                current_mb,
                processed_count,
            )
        if self._memory_hard_limit_mb > 0 and current_mb >= self._memory_hard_limit_mb:
            raise MemoryError(
                f"OTLP evaluator memory usage {current_mb:.1f} MB exceeded hard limit "
                f"{self._memory_hard_limit_mb} MB."
            )


def create_otlp_eval_app(config_path: str | None = None) -> Flask:
    app = Flask(__name__)
    cfg_path = config_path or os.getenv("EVAL_CONFIG_PATH", "config/config.yaml")
    service = OtlpTraceEvaluationService(cfg_path)

    @app.route("/health", methods=["GET"])
    def health() -> Any:
        return jsonify({"status": "ok"})

    @app.route("/api/otlp/v1/traces", methods=["POST"])
    def ingest_otlp_traces() -> Any:
        content_length = request.content_length or 0
        if content_length > service.root_config.otlp_max_payload_bytes:
            return (
                jsonify(
                    {
                        "error": (
                            f"Payload too large ({content_length} bytes). "
                            f"Limit is {service.root_config.otlp_max_payload_bytes} bytes."
                        )
                    }
                ),
                413,
            )
        payload = request.get_json(silent=True)
        if payload is None:
            return jsonify({"error": "Expected OTLP JSON payload."}), 400
        try:
            result = service.process_otlp_traces(payload)
            return jsonify(result), 202
        except Exception as exc:
            logger.exception("Failed OTLP trace ingestion/evaluation.")
            return jsonify({"error": str(exc)}), 400

    @app.route("/api/otlp/v1/traces/file", methods=["POST"])
    def ingest_otlp_trace_file() -> Any:
        payload = request.get_json(silent=True) or {}
        otlp_file_path = str(payload.get("otlp_file_path", "")).strip()
        if not otlp_file_path:
            return jsonify({"error": "Missing required field: otlp_file_path"}), 400
        try:
            result = service.process_otlp_trace_file(otlp_file_path)
            return jsonify(result), 202
        except Exception as exc:
            logger.exception("Failed OTLP file ingestion/evaluation.")
            return jsonify({"error": str(exc)}), 400

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app = create_otlp_eval_app()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "7002")), debug=False)
