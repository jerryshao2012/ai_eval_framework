from __future__ import annotations

from telemetry.api import create_app


def _sample_event():
    return {
        "app_id": "app1",
        "timestamp": "2026-02-27T00:00:00Z",
        "model_id": "m1",
        "model_version": "v1",
        "trace_id": "trace-api-1",
        "input_text": "hello",
        "output_text": "world",
    }


def test_telemetry_api_accepts_single_event(monkeypatch) -> None:
    app = create_app()
    captured = {"count": 0}

    def fake_emit(events, connection_string, eventhub_name, partition_key=None):
        captured["count"] = len(list(events))
        return captured["count"]

    monkeypatch.setenv("EVENTHUB_CONNECTION_STRING", "Endpoint=sb://test/")
    monkeypatch.setenv("EVENTHUB_NAME", "telemetry")
    monkeypatch.setattr("telemetry.api.emit_telemetry_events", fake_emit)

    client = app.test_client()
    resp = client.post("/api/telemetry", json=_sample_event())
    assert resp.status_code == 202
    assert resp.get_json()["accepted"] == 1
    assert captured["count"] == 1


def test_telemetry_api_accepts_batch_events(monkeypatch) -> None:
    app = create_app()

    def fake_emit(events, connection_string, eventhub_name, partition_key=None):
        return len(list(events))

    monkeypatch.setenv("EVENTHUB_CONNECTION_STRING", "Endpoint=sb://test/")
    monkeypatch.setenv("EVENTHUB_NAME", "telemetry")
    monkeypatch.setattr("telemetry.api.emit_telemetry_events", fake_emit)

    client = app.test_client()
    resp = client.post("/api/telemetry", json={"events": [_sample_event(), _sample_event()]})
    assert resp.status_code == 202
    assert resp.get_json()["accepted"] == 2
    assert resp.get_json()["emitted"] == 2


def test_telemetry_api_rejects_invalid_event(monkeypatch) -> None:
    app = create_app()
    monkeypatch.setenv("EVENTHUB_CONNECTION_STRING", "Endpoint=sb://test/")
    monkeypatch.setenv("EVENTHUB_NAME", "telemetry")
    client = app.test_client()

    resp = client.post("/api/telemetry", json={"app_id": "app1"})
    assert resp.status_code == 400
    assert "missing required fields" in resp.get_json()["error"].lower()


def test_telemetry_api_rejects_missing_trace_id(monkeypatch) -> None:
    app = create_app()
    monkeypatch.setenv("EVENTHUB_CONNECTION_STRING", "Endpoint=sb://test/")
    monkeypatch.setenv("EVENTHUB_NAME", "telemetry")
    client = app.test_client()

    bad = _sample_event()
    bad.pop("trace_id", None)
    resp = client.post("/api/telemetry", json=bad)
    assert resp.status_code == 400
    assert "trace_id" in resp.get_json()["error"]
