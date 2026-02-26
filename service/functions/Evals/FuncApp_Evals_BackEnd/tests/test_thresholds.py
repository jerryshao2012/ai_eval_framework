from config.models import ThresholdConfig
from data.models import MetricValueVersioned
from evaluation.thresholds import evaluate_thresholds


def test_evaluate_thresholds_detects_breach() -> None:
    metrics = [
        MetricValueVersioned(
            metric_name="accuracy",
            value=0.9,
            version="1.0",
            timestamp="2026-02-24T00:00:00Z",
        )
    ]
    thresholds = {
        "accuracy": [
            ThresholdConfig(level="warning", value=0.93, direction="min"),
            ThresholdConfig(level="critical", value=0.88, direction="min"),
        ]
    }

    breaches = evaluate_thresholds(metrics, thresholds)

    assert len(breaches) == 1
    assert breaches[0].level == "warning"
