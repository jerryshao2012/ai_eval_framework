import json

import pytest

from data.otlp_repository import OtlpTelemetryRepository


@pytest.mark.asyncio
async def test_otlp_repository_fetches_by_app_and_window(tmp_path) -> None:
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
                                    {"key": "latency_ms", "value": {"doubleValue": 100.0}},
                                ],
                            },
                            {
                                "traceId": "trace-2",
                                "spanId": "span-2",
                                "startTimeUnixNano": "1700000000000000000",
                                "attributes": [
                                    {"key": "app_id", "value": {"stringValue": "app2"}},
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
    file_path = tmp_path / "otlp.json"
    file_path.write_text(json.dumps(payload))

    repo = OtlpTelemetryRepository(str(file_path))
    records = await repo.fetch_telemetry(
        app_id="app1",
        start_ts="2023-11-14T22:00:00+00:00",
        end_ts="2023-11-14T23:00:00+00:00",
    )
    assert len(records) == 1
    assert records[0].app_id == "app1"
    assert records[0].metadata["trace_id"] == "trace-1"
