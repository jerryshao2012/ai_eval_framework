import pytest

from config.models import PolicyConfig, ResolvedAppConfig, ThresholdConfig
from data.models import TelemetryRecord
from data.repositories import (
    InMemoryEvaluationRepository,
    InMemoryStore,
    InMemoryTelemetryRepository,
)
from orchestration.batch_runner import BatchEvaluationRunner

_TELEMETRY = [
    TelemetryRecord(
        id="1",
        app_id="app1",
        timestamp="2026-02-24T00:00:01Z",
        model_id="m1",
        model_version="2.3",
        input_text="hello",
        output_text="A",
        expected_output="A",
        latency_ms=100,
    ),
    TelemetryRecord(
        id="2",
        app_id="app1",
        timestamp="2026-02-24T00:00:02Z",
        model_id="m1",
        model_version="2.3",
        input_text="world",
        output_text="B",
        expected_output="A",
        latency_ms=200,
    ),
]

_APP_CFG = ResolvedAppConfig(
    app_id="app1",
    batch_time="0 * * * *",
    policy_names=["accuracy", "latency"],
    policies=[
        PolicyConfig(name="accuracy", metrics=["accuracy"], parameters={"version": "1.0"}),
        PolicyConfig(name="latency", metrics=["latency_avg_ms", "latency_p95_ms"], parameters={}),
    ],
    thresholds={
        "accuracy": [ThresholdConfig(level="warning", value=0.75, direction="min")],
        "latency_p95_ms": [ThresholdConfig(level="warning", value=150, direction="max")],
    },
)


@pytest.mark.asyncio
async def test_batch_runner_generates_and_saves_results() -> None:
    store = InMemoryStore(telemetry=list(_TELEMETRY), results=[])
    runner = BatchEvaluationRunner(
        InMemoryTelemetryRepository(store),
        InMemoryEvaluationRepository(store),
    )

    results = await runner.run_for_application(
        _APP_CFG,
        start_ts="2026-02-24T00:00:00Z",
        end_ts="2026-02-24T23:59:59Z",
    )

    assert len(results) == 2
    assert len(store.results) == 2
    assert any(r.policy_name == "accuracy" for r in store.results)
    assert all(not r.breaches for r in store.results)


@pytest.mark.asyncio
async def test_batch_runner_unknown_policy_raises() -> None:
    """BatchEvaluationRunner should raise KeyError for unregistered policy names."""
    store = InMemoryStore(telemetry=list(_TELEMETRY), results=[])
    runner = BatchEvaluationRunner(
        InMemoryTelemetryRepository(store),
        InMemoryEvaluationRepository(store),
    )

    bad_cfg = ResolvedAppConfig(
        app_id="app1",
        batch_time="0 * * * *",
        policy_names=["nonexistent_policy"],
        policies=[
            PolicyConfig(name="nonexistent_policy", metrics=[], parameters={}),
        ],
        thresholds={},
    )

    with pytest.raises(KeyError, match="nonexistent_policy"):
        await runner.run_for_application(
            bad_cfg,
            start_ts="2026-02-24T00:00:00Z",
            end_ts="2026-02-24T23:59:59Z",
        )


@pytest.mark.asyncio
async def test_batch_runner_empty_records_produces_zero_metrics() -> None:
    """When no telemetry exists in the window, policies should still return metrics (zeroed)."""
    store = InMemoryStore(telemetry=[], results=[])
    runner = BatchEvaluationRunner(
        InMemoryTelemetryRepository(store),
        InMemoryEvaluationRepository(store),
    )

    results = await runner.run_for_application(
        _APP_CFG,
        start_ts="2026-02-24T00:00:00Z",
        end_ts="2026-02-24T23:59:59Z",
    )

    assert len(results) == 2
    accuracy_result = next(r for r in results if r.policy_name == "accuracy")
    assert accuracy_result.metrics[0].value == 0.0
