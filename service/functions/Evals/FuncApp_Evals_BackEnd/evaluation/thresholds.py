from __future__ import annotations

from typing import Dict, List

from config.models import ThresholdConfig
from data.models import MetricValueVersioned, ThresholdBreach


def evaluate_thresholds(
    metrics: List[MetricValueVersioned],
    threshold_map: Dict[str, List[ThresholdConfig]],
) -> List[ThresholdBreach]:
    breaches: List[ThresholdBreach] = []

    for metric in metrics:
        thresholds = threshold_map.get(metric.metric_name, [])
        for threshold in thresholds:
            if _is_breached(metric.value, threshold):
                breaches.append(
                    ThresholdBreach(
                        metric_name=metric.metric_name,
                        level=threshold.level,
                        threshold_value=threshold.value,
                        actual_value=metric.value,
                        direction=threshold.direction,
                    )
                )

    return breaches


def _is_breached(value: float, threshold: ThresholdConfig) -> bool:
    if threshold.direction == "min":
        return value < threshold.value
    if threshold.direction == "max":
        return value > threshold.value
    raise ValueError(f"Unsupported threshold direction: {threshold.direction}")
