import asyncio
import pytest
from config.models import AlertingConfig, EmailAlertConfig, TeamsAlertConfig
from data.models import ThresholdBreach
from orchestration import notifier
from orchestration.notifier import (
    filter_breaches_by_min_level,
    enqueue_alert,
    _alert_queue,
    CircuitBreaker,
    alert_worker,
    drain_alert_queue,
)


def test_filter_breaches_by_min_level() -> None:
    breaches = [
        ThresholdBreach(
            metric_name="performance_precision_coherence",
            level="warning",
            threshold_value=0.93,
            actual_value=0.91,
            direction="min",
        ),
        ThresholdBreach(
            metric_name="system_reliability_latency",
            level="critical",
            threshold_value=2000,
            actual_value=2500,
            direction="max",
        ),
    ]

    critical_only = filter_breaches_by_min_level(breaches, "critical")
    assert len(critical_only) == 1
    assert critical_only[0].level == "critical"


@pytest.mark.asyncio
async def test_enqueue_alert_no_channel_enabled_noop() -> None:
    # Clear queue first just in case
    while not _alert_queue.empty():
        _alert_queue.get_nowait()

    cfg = AlertingConfig(
        enabled=True,
        min_level="warning",
        email=EmailAlertConfig(enabled=False),
        teams=TeamsAlertConfig(enabled=False),
    )
    breaches = [
        ThresholdBreach(
            metric_name="safety_toxicity",
            level="warning",
            threshold_value=0.93,
            actual_value=0.91,
            direction="min",
        )
    ]

    await enqueue_alert(cfg, app_id="app1", window_start="2026-02-25T00:00:00Z", window_end="2026-02-25T01:00:00Z", breaches=breaches)
    
    assert _alert_queue.empty()


@pytest.mark.asyncio
async def test_enqueue_alert_disabled_config_noop() -> None:
    while not _alert_queue.empty():
        _alert_queue.get_nowait()

    cfg = AlertingConfig(enabled=False)
    await enqueue_alert(cfg, app_id="app1", window_start="none", window_end="none", breaches=[])
    
    assert _alert_queue.empty()


@pytest.mark.asyncio
async def test_circuit_breaker_thresholds() -> None:
    breaker = CircuitBreaker(failure_threshold=2, recovery_timeout=0.1)
    
    async def failing_func():
        raise ValueError("Simulated network failure")

    async def success_func():
        return "OK"

    # 1. Failure 1
    with pytest.raises(ValueError, match="Simulated network failure"):
        await breaker.call(failing_func)
    assert breaker.state == "CLOSED"

    # 2. Failure 2 hits threshold -> OPEN
    with pytest.raises(ValueError, match="Simulated network failure"):
        await breaker.call(failing_func)
    assert breaker.state == "OPEN"

    # 3. Request while OPEN fast-fails
    with pytest.raises(RuntimeError, match="Circuit breaker is OPEN"):
        await breaker.call(failing_func)

    # 4. Wait for recovery -> HALF_OPEN
    await asyncio.sleep(0.15)
    
    # 5. Success while HALF_OPEN -> CLOSED
    assert await breaker.call(success_func) == "OK"
    assert breaker.state == "CLOSED"
    assert breaker.failures == 0


@pytest.mark.asyncio
async def test_circuit_breaker_success_resets_failures() -> None:
    breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=1.0)

    async def flaky_once():
        raise ValueError("fail")

    async def success_func():
        return "OK"

    with pytest.raises(ValueError):
        await breaker.call(flaky_once)
    assert breaker.failures == 1
    assert await breaker.call(success_func) == "OK"
    assert breaker.failures == 0
    assert breaker.state == "CLOSED"


@pytest.mark.asyncio
async def test_alert_worker_batches_alerts_per_app(monkeypatch) -> None:
    while not _alert_queue.empty():
        _alert_queue.get_nowait()

    sent = []

    def _fake_send_email(config, subject, body):
        sent.append((subject, body))

    monkeypatch.setattr(notifier, "_send_email", _fake_send_email)

    cfg = AlertingConfig(
        enabled=True,
        min_level="warning",
        batch_window_seconds=0.01,
        shutdown_drain_timeout_seconds=1.0,
        email=EmailAlertConfig(enabled=True),
        teams=TeamsAlertConfig(enabled=False),
    )
    breaches = [
        ThresholdBreach(
            metric_name="safety_toxicity",
            level="warning",
            threshold_value=0.9,
            actual_value=0.7,
            direction="min",
        )
    ]

    stop_event = asyncio.Event()
    worker = asyncio.create_task(alert_worker(batch_window_seconds=0.01, stop_event=stop_event))
    try:
        await enqueue_alert(cfg, app_id="app1", window_start="2026-02-25T00:00:00Z", window_end="2026-02-25T01:00:00Z", breaches=breaches)
        await enqueue_alert(cfg, app_id="app1", window_start="2026-02-25T01:00:00Z", window_end="2026-02-25T02:00:00Z", breaches=breaches)
        await drain_alert_queue(timeout_seconds=1.0)
        stop_event.set()
        await worker
    finally:
        if not worker.done():
            worker.cancel()
            with pytest.raises(asyncio.CancelledError):
                await worker

    assert len(sent) == 1
    assert "2 breach(es)" in sent[0][0]
    assert "Breaches: 2" in sent[0][1]
