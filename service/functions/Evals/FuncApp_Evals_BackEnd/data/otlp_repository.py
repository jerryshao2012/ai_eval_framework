from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from data.models import TelemetryRecord


def _value_from_otlp_attr(value_obj: Dict[str, Any]) -> Any:
    for key in ("stringValue", "intValue", "doubleValue", "boolValue"):
        if key in value_obj:
            return value_obj[key]
    return None


def _otlp_attrs_to_dict(attrs: List[Dict[str, Any]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for attr in attrs:
        key = attr.get("key")
        if key is None:
            continue
        out[str(key)] = _value_from_otlp_attr(attr.get("value", {}))
    return out


def _iso_from_unix_nano(raw: str | int | None) -> str:
    if not raw:
        return datetime.now(timezone.utc).isoformat()
    nanos = int(raw)
    return datetime.fromtimestamp(nanos / 1_000_000_000, tz=timezone.utc).isoformat()


def _parse_dt(value: str) -> datetime:
    text = value.replace("Z", "+00:00")
    return datetime.fromisoformat(text)


class OtlpTelemetryRepository:
    def __init__(self, otlp_file_path: str) -> None:
        if not otlp_file_path:
            raise ValueError("otlp_file_path is required when telemetry_source.type=otlp")
        self._path = Path(otlp_file_path)

    async def fetch_telemetry(self, app_id: str, start_ts: str, end_ts: str) -> List[TelemetryRecord]:
        if not self._path.exists():
            raise FileNotFoundError(f"OTLP file not found: {self._path}")
        payload = json.loads(self._path.read_text())
        start = _parse_dt(start_ts)
        end = _parse_dt(end_ts)

        records: List[TelemetryRecord] = []
        for resource_spans in payload.get("resourceSpans", []):
            resource_attrs = _otlp_attrs_to_dict(resource_spans.get("resource", {}).get("attributes", []))
            for scope_spans in resource_spans.get("scopeSpans", []):
                for span in scope_spans.get("spans", []):
                    span_attrs = _otlp_attrs_to_dict(span.get("attributes", []))
                    attrs = {**resource_attrs, **span_attrs}
                    rec_app_id = str(attrs.get("app_id", attrs.get("service.name", "")))
                    if rec_app_id != app_id:
                        continue
                    ts = _iso_from_unix_nano(span.get("startTimeUnixNano"))
                    ts_dt = _parse_dt(ts)
                    if not (start <= ts_dt < end):
                        continue
                    latency_raw: Optional[Any] = attrs.get("latency_ms", attrs.get("duration_ms"))
                    records.append(
                        TelemetryRecord(
                            id=str(attrs.get("event_id", f"{app_id}:{span.get('traceId','') or span.get('spanId','')}")),
                            app_id=rec_app_id,
                            timestamp=ts,
                            model_id=str(attrs.get("model_id", attrs.get("llm.model", "unknown-model"))),
                            model_version=str(attrs.get("model_version", attrs.get("llm.model_version", "unknown-version"))),
                            input_text=str(attrs.get("input_text", attrs.get("llm.input", ""))),
                            output_text=str(attrs.get("output_text", attrs.get("llm.output", ""))),
                            user_id=str(attrs.get("user_id")) if attrs.get("user_id") is not None else None,
                            latency_ms=float(latency_raw) if latency_raw not in (None, "") else None,
                            metadata={
                                "trace_id": span.get("traceId"),
                                "span_id": span.get("spanId"),
                                "service_name": attrs.get("service.name"),
                            },
                        )
                    )
        return records
