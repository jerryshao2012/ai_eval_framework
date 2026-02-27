from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pytest

from telemetry.otlp_evaluator import OtlpTraceEvaluationService, _stable_result_id

BASE_DIR = Path(__file__).resolve().parents[1]
CONFIG_PATH = str(BASE_DIR / "config" / "config.yaml")


class FakeCosmos:
    def __init__(self) -> None:
        self.telemetry: List[Dict[str, Any]] = []
        self.results: Dict[str, Dict[str, Any]] = {}

    def upsert_telemetry(self, item: Dict[str, Any]) -> Dict[str, Any]:
        self.telemetry.append(item)
        return item

    def upsert_telemetry_batch(self, items: List[Dict[str, Any]], partition_key: str) -> None:
        self.telemetry.extend(items)

    def upsert_result(self, item: Dict[str, Any]) -> Dict[str, Any]:
        self.results[item["id"]] = item
        return item

    def upsert_results_batch(self, items: List[Dict[str, Any]], partition_key: str) -> None:
        for item in items:
            self.results[item["id"]] = item

    def query_results(self, query: str, parameters: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        # Mock simple single-id or IN-clause lookups
        matches = []
        for p in parameters:
            target_id = p.get("value")
            if target_id in self.results:
                matches.append({"id": target_id})
        return matches


def _sample_otlp_payload(trace_id: str = "abc123", span_count: int = 1) -> Dict[str, Any]:
    spans = []
    for i in range(span_count):
        spans.append(
            {
                "traceId": trace_id,
                "spanId": f"span-{i+1}",
                "startTimeUnixNano": "1700000000000000000",
                "attributes": [
                    {"key": "app_id", "value": {"stringValue": "app1"}},
                    {"key": "model_id", "value": {"stringValue": "m1"}},
                    {"key": "model_version", "value": {"stringValue": "v1"}},
                    {"key": "input_text", "value": {"stringValue": "hello"}},
                    {"key": "output_text", "value": {"stringValue": f"world-{i+1}"}},
                    {"key": "latency_ms", "value": {"doubleValue": 100.0 + i}},
                ],
            }
        )
    return {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [{"key": "service.name", "value": {"stringValue": "svc-evals"}}]
                },
                "scopeSpans": [
                    {
                        "spans": spans
                    }
                ],
            }
        ]
    }


def test_otlp_trace_evaluator_dedup_by_trace_and_version(monkeypatch, tmp_path) -> None:
    fake_cosmos = FakeCosmos()
    service = OtlpTraceEvaluationService(CONFIG_PATH, cosmos_client=fake_cosmos)

    payload = _sample_otlp_payload("trace-dup")
    first = service.process_otlp_traces(payload)
    second = service.process_otlp_traces(payload)

    assert first["telemetry_events_ingested"] == 1
    assert first["evaluations_created"] > 0
    assert second["evaluations_created"] == 0
    assert second["evaluations_skipped_duplicate"] > 0


def test_otlp_trace_recompute_when_value_version_changes() -> None:
    fake_cosmos = FakeCosmos()
    service = OtlpTraceEvaluationService(CONFIG_PATH, cosmos_client=fake_cosmos)

    payload = _sample_otlp_payload("trace-version")
    first = service.process_otlp_traces(payload)
    assert first["evaluations_created"] > 0

    # Bump version for one policy; evaluator should create a new result id for that policy.
    policy_name = service.root_config.default_evaluation_policies[0]
    service.root_config.evaluation_policies[policy_name].parameters["version"] = "2.0"
    second = service.process_otlp_traces(payload)
    assert second["evaluations_created"] >= 1


def test_result_id_stable_for_same_trace_policy_and_version() -> None:
    a = _stable_result_id("app1", "safety_toxicity", "trace-a", "1.0")
    b = _stable_result_id("app1", "safety_toxicity", "trace-a", "1.0")
    c = _stable_result_id("app1", "safety_toxicity", "trace-a", "2.0")
    assert a == b
    assert a != c


def test_otlp_trace_evaluator_enforces_max_events_per_request() -> None:
    service = OtlpTraceEvaluationService(CONFIG_PATH, cosmos_client=FakeCosmos())
    service._max_events_per_request = 1

    payload = {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [{"key": "service.name", "value": {"stringValue": "svc-evals"}}]
                },
                "scopeSpans": [
                    {
                        "spans": [
                            {
                                "traceId": "trace-1",
                                "spanId": "span-1",
                                "startTimeUnixNano": "1700000000000000000",
                                "attributes": [
                                    {"key": "app_id", "value": {"stringValue": "app1"}},
                                    {"key": "model_id", "value": {"stringValue": "m1"}},
                                    {"key": "model_version", "value": {"stringValue": "v1"}},
                                    {"key": "input_text", "value": {"stringValue": "hello"}},
                                    {"key": "output_text", "value": {"stringValue": "world"}},
                                ],
                            },
                            {
                                "traceId": "trace-2",
                                "spanId": "span-2",
                                "startTimeUnixNano": "1700000000000000000",
                                "attributes": [
                                    {"key": "app_id", "value": {"stringValue": "app1"}},
                                    {"key": "model_id", "value": {"stringValue": "m1"}},
                                    {"key": "model_version", "value": {"stringValue": "v1"}},
                                    {"key": "input_text", "value": {"stringValue": "hello"}},
                                    {"key": "output_text", "value": {"stringValue": "world"}},
                                ],
                            },
                        ]
                    }
                ],
            }
        ]
    }

    with pytest.raises(ValueError, match="max event limit"):
        service.process_otlp_traces(payload)


def test_otlp_trace_evaluator_can_stream_from_file(tmp_path) -> None:
    fake_cosmos = FakeCosmos()
    service = OtlpTraceEvaluationService(CONFIG_PATH, cosmos_client=fake_cosmos)

    payload = _sample_otlp_payload("trace-file")
    file_path = tmp_path / "otlp.json"
    file_path.write_text(json.dumps(payload))

    result = service.process_otlp_trace_file(str(file_path))
    assert result["telemetry_events_ingested"] == 1
    assert result["evaluations_created"] > 0


def test_otlp_trace_group_evaluates_with_all_records_in_same_trace() -> None:
    fake_cosmos = FakeCosmos()
    service = OtlpTraceEvaluationService(CONFIG_PATH, cosmos_client=fake_cosmos)

    payload = _sample_otlp_payload("trace-group", span_count=2)
    result = service.process_otlp_traces(payload)

    assert result["telemetry_events_ingested"] == 2
    assert result["evaluations_created"] > 0

    # At least one policy metric should see both records in the trace group.
    sample_counts = []
    for doc in fake_cosmos.results.values():
        for metric in doc.get("metrics", []):
            metadata = metric.get("metadata", {})
            if "samples" in metadata:
                sample_counts.append(metadata["samples"])
    assert sample_counts
    assert max(sample_counts) >= 2
