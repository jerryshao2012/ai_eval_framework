from __future__ import annotations

import math
from abc import ABC, abstractmethod
from statistics import mean
from typing import Dict, List

from config.models import PolicyConfig
from data.models import MetricValueVersioned, TelemetryRecord, utc_now_iso


class EvaluationPolicy(ABC):
    def __init__(self, config: PolicyConfig) -> None:
        self.config = config

    @abstractmethod
    async def evaluate(self, app_id: str, records: List[TelemetryRecord]) -> List[MetricValueVersioned]:
        ...


class AccuracyPolicy(EvaluationPolicy):
    async def evaluate(self, app_id: str, records: List[TelemetryRecord]) -> List[MetricValueVersioned]:
        comparable = [r for r in records if r.expected_output is not None]
        if not comparable:
            value = 0.0
        else:
            correct = sum(1 for r in comparable if r.expected_output == r.output_text)
            value = correct / len(comparable)

        return [
            MetricValueVersioned(
                metric_name="accuracy",
                value=value,
                version=self.config.parameters.get("version", "1.0"),
                timestamp=utc_now_iso(),
                metadata={
                    "samples": len(comparable),
                    "app_id": app_id,
                },
            )
        ]


class LatencyPolicy(EvaluationPolicy):
    async def evaluate(self, app_id: str, records: List[TelemetryRecord]) -> List[MetricValueVersioned]:
        latencies = [r.latency_ms for r in records if r.latency_ms is not None]
        if not latencies:
            avg = 0.0
            p95 = 0.0
        else:
            sorted_values = sorted(latencies)
            # Standard ceiling-based percentile index (0-indexed).
            p95_idx = max(0, math.ceil(len(sorted_values) * 0.95) - 1)
            avg = float(mean(sorted_values))
            p95 = float(sorted_values[p95_idx])

        version = self.config.parameters.get("version", "1.0")
        timestamp = utc_now_iso()

        return [
            MetricValueVersioned(
                metric_name="latency_avg_ms",
                value=avg,
                version=version,
                timestamp=timestamp,
                metadata={"samples": len(latencies), "app_id": app_id},
            ),
            MetricValueVersioned(
                metric_name="latency_p95_ms",
                value=p95,
                version=version,
                timestamp=timestamp,
                metadata={"samples": len(latencies), "app_id": app_id},
            ),
        ]


class DriftPolicy(EvaluationPolicy):
    async def evaluate(self, app_id: str, records: List[TelemetryRecord]) -> List[MetricValueVersioned]:
        baseline = float(self.config.parameters.get("baseline_input_length", 100.0))
        if not records:
            drift = 0.0
        else:
            avg_length = mean([len(r.input_text) for r in records])
            drift = abs(avg_length - baseline) / max(baseline, 1.0)

        return [
            MetricValueVersioned(
                metric_name="input_length_drift",
                value=float(drift),
                version=self.config.parameters.get("version", "1.0"),
                timestamp=utc_now_iso(),
                metadata={
                    "baseline_input_length": baseline,
                    "samples": len(records),
                    "app_id": app_id,
                },
            )
        ]


class PerformancePolicy(EvaluationPolicy):
    """Composite performance score combining accuracy and latency signals.

    The score is computed as::

        performance_score = accuracy_rate * latency_penalty

    where ``latency_penalty = target_latency_ms / max(avg_latency_ms, 1)``,
    clipped to [0, 1].  Configure ``target_latency_ms`` in the policy
    parameters (default: 500 ms).

    Override this class to supply a more domain-appropriate formula.
    """

    async def evaluate(self, app_id: str, records: List[TelemetryRecord]) -> List[MetricValueVersioned]:
        target_ms = float(self.config.parameters.get("target_latency_ms", 500.0))

        comparable = [r for r in records if r.expected_output is not None]
        accuracy_rate = (
            sum(1 for r in comparable if r.expected_output == r.output_text) / len(comparable)
            if comparable
            else 0.0
        )

        latencies = [r.latency_ms for r in records if r.latency_ms is not None]
        avg_latency = float(mean(latencies)) if latencies else 0.0
        latency_penalty = min(1.0, target_ms / max(avg_latency, 1.0))

        score = accuracy_rate * latency_penalty

        return [
            MetricValueVersioned(
                metric_name="performance_score",
                value=round(score, 4),
                version=self.config.parameters.get("version", "1.0"),
                timestamp=utc_now_iso(),
                metadata={
                    "accuracy_rate": round(accuracy_rate, 4),
                    "avg_latency_ms": round(avg_latency, 2),
                    "target_latency_ms": target_ms,
                    "samples": len(records),
                    "app_id": app_id,
                },
            )
        ]


def build_policy_registry() -> Dict[str, type[EvaluationPolicy]]:
    return {
        "accuracy": AccuracyPolicy,
        "latency": LatencyPolicy,
        "drift": DriftPolicy,
        "performance": PerformancePolicy,
    }
