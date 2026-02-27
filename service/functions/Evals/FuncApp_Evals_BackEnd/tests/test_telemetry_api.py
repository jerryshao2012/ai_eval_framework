from __future__ import annotations

import asyncio

from telemetry.emitter import BackpressureError
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

class FakeEmitter:
    def __init__(self, target_dict):
        self.target_dict = target_dict
    async def enqueue_events(self, events):
        self.target_dict["count"] += len(events)

def fake_run_coroutine_threadsafe(coro, loop):
    asyncio.run(coro)
    class FakeFuture:
        def result(self): return None
    return FakeFuture()

def test_telemetry_api_accepts_single_event(monkeypatch) -> None:
    app = create_app()
    captured = {"count": 0}
    
    monkeypatch.setenv("EVENTHUB_CONNECTION_STRING", "Endpoint=sb://test/")
    monkeypatch.setenv("EVENTHUB_NAME", "telemetry")
    monkeypatch.setattr("telemetry.api._get_emitter_sync", lambda *args, **kwargs: FakeEmitter(captured))
    monkeypatch.setattr("telemetry.api.asyncio.run_coroutine_threadsafe", fake_run_coroutine_threadsafe)

    client = app.test_client()
    resp = client.post("/api/telemetry", json=_sample_event())
    assert resp.status_code == 202
    assert resp.get_json()["accepted"] == 1
    assert captured["count"] == 1


def test_telemetry_api_accepts_batch_events(monkeypatch) -> None:
    app = create_app()
    captured = {"count": 0}

    monkeypatch.setenv("EVENTHUB_CONNECTION_STRING", "Endpoint=sb://test/")
    monkeypatch.setenv("EVENTHUB_NAME", "telemetry")
    monkeypatch.setattr("telemetry.api._get_emitter_sync", lambda *args, **kwargs: FakeEmitter(captured))
    monkeypatch.setattr("telemetry.api.asyncio.run_coroutine_threadsafe", fake_run_coroutine_threadsafe)

    client = app.test_client()
    resp = client.post("/api/telemetry", json={"events": [_sample_event(), _sample_event()]})
    assert resp.status_code == 202
    assert resp.get_json()["accepted"] == 2
    assert resp.get_json()["emitted"] == 2
    assert captured["count"] == 2


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


def test_telemetry_api_returns_429_on_backpressure(monkeypatch) -> None:
    app = create_app()
    monkeypatch.setenv("EVENTHUB_CONNECTION_STRING", "Endpoint=sb://test/")
    monkeypatch.setenv("EVENTHUB_NAME", "telemetry")

    class SaturatedEmitter:
        async def enqueue_events(self, events):
            raise BackpressureError("Telemetry queue is full")

    monkeypatch.setattr("telemetry.api._get_emitter_sync", lambda *args, **kwargs: SaturatedEmitter())
    monkeypatch.setattr("telemetry.api.asyncio.run_coroutine_threadsafe", fake_run_coroutine_threadsafe)

    client = app.test_client()
    resp = client.post("/api/telemetry", json=_sample_event())
    assert resp.status_code == 429
    assert "queue is full" in resp.get_json()["error"].lower()
