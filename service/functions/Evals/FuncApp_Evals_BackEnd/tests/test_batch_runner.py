import asyncio

import pytest

from config.models import PolicyConfig, ResolvedAppConfig, ThresholdConfig
from data.models import MetricValueVersioned, TelemetryRecord
from data.repositories import (
    InMemoryEvaluationRepository,
    InMemoryStore,
    InMemoryTelemetryRepository,
)
from orchestration.batch_runner import BatchEvaluationRunner
from evaluation.taxonomy import (
    PERFORMANCE_PRECISION_COHERENCE,
    SYSTEM_RELIABILITY_LATENCY,
)

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
        metadata={"trace_id": "trace-1"},
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
        metadata={"trace_id": "trace-2"},
    ),
]

_APP_CFG = ResolvedAppConfig(
    app_id="app1",
    batch_time="0 * * * *",
    policy_names=["performance_precision_coherence", "system_reliability_latency"],
    policies=[
        PolicyConfig(name="performance_precision_coherence", metrics=["performance_precision_coherence"], parameters={"version": "1.0"}),
        PolicyConfig(name="system_reliability_latency", metrics=["system_reliability_latency"], parameters={}),
    ],
    thresholds={
        "performance_precision_coherence": [ThresholdConfig(level="warning", value=0.75, direction="min")],
        "system_reliability_latency": [ThresholdConfig(level="warning", value=150, direction="max")],
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
    assert any(r.policy_name == "performance_precision_coherence" for r in store.results)
    assert all(not r.breaches for r in store.results)
    for result in store.results:
        for metric in result.metrics:
            assert metric.metric_type
            assert metric.version
            assert metric.timestamp
            assert metric.metadata["value_object_type"] == "metric_value_versioned"
            assert metric.metadata["value_object_version"] == metric.version
            assert metric.metadata["policy_name"] == result.policy_name
            assert "window_start" in metric.metadata
            assert "window_end" in metric.metadata
            assert "dedupe_trace_id" in metric.metadata
    precision_result = next(r for r in store.results if r.policy_name == "performance_precision_coherence")
    assert precision_result.metrics[0].metric_type == PERFORMANCE_PRECISION_COHERENCE
    latency_result = next(r for r in store.results if r.policy_name == "system_reliability_latency")
    assert all(m.metric_type == SYSTEM_RELIABILITY_LATENCY for m in latency_result.metrics)


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
    precision_result = next(r for r in results if r.policy_name == "performance_precision_coherence")
    assert precision_result.metrics[0].value == 0.0


@pytest.mark.asyncio
async def test_batch_runner_skips_duplicate_trace_version_for_same_value_object_version() -> None:
    store = InMemoryStore(telemetry=list(_TELEMETRY), results=[])
    runner = BatchEvaluationRunner(
        InMemoryTelemetryRepository(store),
        InMemoryEvaluationRepository(store),
    )

    first = await runner.run_for_application(
        _APP_CFG,
        start_ts="2026-02-24T00:00:00Z",
        end_ts="2026-02-24T23:59:59Z",
    )
    second = await runner.run_for_application(
        _APP_CFG,
        start_ts="2026-02-24T00:00:00Z",
        end_ts="2026-02-24T23:59:59Z",
    )

    assert len(first) == 2
    assert len(second) == 0
    assert len(store.results) == 2


@pytest.mark.asyncio
async def test_batch_runner_recomputes_when_value_object_version_changes() -> None:
    store = InMemoryStore(telemetry=list(_TELEMETRY), results=[])
    runner = BatchEvaluationRunner(
        InMemoryTelemetryRepository(store),
        InMemoryEvaluationRepository(store),
    )

    await runner.run_for_application(
        _APP_CFG,
        start_ts="2026-02-24T00:00:00Z",
        end_ts="2026-02-24T23:59:59Z",
    )

    bumped_cfg = ResolvedAppConfig(
        app_id=_APP_CFG.app_id,
        batch_time=_APP_CFG.batch_time,
        policy_names=list(_APP_CFG.policy_names),
        policies=[
            PolicyConfig(name="performance_precision_coherence", metrics=["performance_precision_coherence"], parameters={"version": "2.0"}),
            PolicyConfig(name="system_reliability_latency", metrics=["system_reliability_latency"], parameters={}),
        ],
        thresholds=dict(_APP_CFG.thresholds),
    )

    second = await runner.run_for_application(
        bumped_cfg,
        start_ts="2026-02-24T00:00:00Z",
        end_ts="2026-02-24T23:59:59Z",
    )

    # precision/coherence should re-run due to policy version bump.
    # latency policy has no explicit version and remains deduped at default 1.0
    assert len(second) == 1
    assert second[0].policy_name == "performance_precision_coherence"


class _SpyEvaluationRepository(InMemoryEvaluationRepository):
    def __init__(self, store: InMemoryStore) -> None:
        super().__init__(store)
        self.results_exist_calls = 0
        self.result_exists_calls = 0
        self.save_results_calls = 0

    async def result_exists(self, result_id: str) -> bool:
        self.result_exists_calls += 1
        return await super().result_exists(result_id)

    async def results_exist(self, result_ids: list[str]) -> list[str]:
        self.results_exist_calls += 1
        return await super().results_exist(result_ids)

    async def save_results(self, results):
        self.save_results_calls += 1
        await super().save_results(results)


@pytest.mark.asyncio
async def test_batch_runner_uses_bulk_existence_check_and_batch_write() -> None:
    store = InMemoryStore(telemetry=list(_TELEMETRY), results=[])
    repo = _SpyEvaluationRepository(store)
    runner = BatchEvaluationRunner(
        InMemoryTelemetryRepository(store),
        repo,
    )

    results = await runner.run_for_application(
        _APP_CFG,
        start_ts="2026-02-24T00:00:00Z",
        end_ts="2026-02-24T23:59:59Z",
    )

    assert len(results) == 2
    assert repo.results_exist_calls == 1
    assert repo.result_exists_calls == 0
    assert repo.save_results_calls == 1


@pytest.mark.asyncio
async def test_batch_runner_respects_policy_concurrency_limit() -> None:
    store = InMemoryStore(telemetry=list(_TELEMETRY), results=[])
    runner = BatchEvaluationRunner(
        InMemoryTelemetryRepository(store),
        InMemoryEvaluationRepository(store),
        policy_concurrency=2,
    )

    class SlowPolicy:
        inflight = 0
        max_inflight = 0

        def __init__(self, cfg):
            _ = cfg

        async def evaluate(self, app_id, records):
            _ = app_id
            _ = records
            SlowPolicy.inflight += 1
            SlowPolicy.max_inflight = max(SlowPolicy.max_inflight, SlowPolicy.inflight)
            await asyncio.sleep(0.05)
            SlowPolicy.inflight -= 1
            return [
                MetricValueVersioned(
                    metric_name="performance_precision_coherence",
                    value=1.0,
                    version="1.0",
                    timestamp="2026-02-24T00:00:00Z",
                )
            ]

    cfg = ResolvedAppConfig(
        app_id="app1",
        batch_time="0 * * * *",
        policy_names=["p1", "p2", "p3", "p4"],
        policies=[
            PolicyConfig(name="p1", metrics=["performance_precision_coherence"], parameters={}),
            PolicyConfig(name="p2", metrics=["performance_precision_coherence"], parameters={}),
            PolicyConfig(name="p3", metrics=["performance_precision_coherence"], parameters={}),
            PolicyConfig(name="p4", metrics=["performance_precision_coherence"], parameters={}),
        ],
        thresholds={},
    )
    runner.policy_registry = {name: SlowPolicy for name in cfg.policy_names}

    results = await runner.run_for_application(
        cfg,
        start_ts="2026-02-24T00:00:00Z",
        end_ts="2026-02-24T23:59:59Z",
    )

    assert len(results) == 4
    assert SlowPolicy.max_inflight <= 2
