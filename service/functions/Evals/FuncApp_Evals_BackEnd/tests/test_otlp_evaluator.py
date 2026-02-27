from __future__ import annotations

from typing import Any, Dict, List

from telemetry.otlp_evaluator import OtlpTraceEvaluationService, _stable_result_id


class FakeCosmos:
    def __init__(self) -> None:
        self.telemetry: List[Dict[str, Any]] = []
        self.results: Dict[str, Dict[str, Any]] = {}

    def upsert_telemetry(self, item: Dict[str, Any]) -> Dict[str, Any]:
        self.telemetry.append(item)
        return item

    def upsert_result(self, item: Dict[str, Any]) -> Dict[str, Any]:
        self.results[item["id"]] = item
        return item

    def query_results(self, query: str, parameters: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        target_id = next((p["value"] for p in parameters if p["name"] == "@id"), None)
        if target_id in self.results:
            return [{"id": target_id}]
        return []


def _sample_otlp_payload(trace_id: str = "abc123") -> Dict[str, Any]:
    return {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [{"key": "service.name", "value": {"stringValue": "svc-evals"}}]
                },
                "scopeSpans": [
                    {
                        "spans": [
                            {
                                "traceId": trace_id,
                                "spanId": "span-1",
                                "startTimeUnixNano": "1700000000000000000",
                                "attributes": [
                                    {"key": "app_id", "value": {"stringValue": "app1"}},
                                    {"key": "model_id", "value": {"stringValue": "m1"}},
                                    {"key": "model_version", "value": {"stringValue": "v1"}},
                                    {"key": "input_text", "value": {"stringValue": "hello"}},
                                    {"key": "output_text", "value": {"stringValue": "world"}},
                                    {"key": "latency_ms", "value": {"doubleValue": 100.0}},
                                ],
                            }
                        ]
                    }
                ],
            }
        ]
    }


def test_otlp_trace_evaluator_dedup_by_trace_and_version(monkeypatch, tmp_path) -> None:
    cfg_path = "config/config.yaml"
    fake_cosmos = FakeCosmos()
    service = OtlpTraceEvaluationService(cfg_path, cosmos_client=fake_cosmos)

    payload = _sample_otlp_payload("trace-dup")
    first = service.process_otlp_traces(payload)
    second = service.process_otlp_traces(payload)

    assert first["telemetry_events_ingested"] == 1
    assert first["evaluations_created"] > 0
    assert second["evaluations_created"] == 0
    assert second["evaluations_skipped_duplicate"] > 0


def test_otlp_trace_recompute_when_value_version_changes() -> None:
    cfg_path = "config/config.yaml"
    fake_cosmos = FakeCosmos()
    service = OtlpTraceEvaluationService(cfg_path, cosmos_client=fake_cosmos)

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
