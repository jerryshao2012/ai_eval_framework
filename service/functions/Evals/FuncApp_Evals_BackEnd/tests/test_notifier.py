from config.models import AlertingConfig, EmailAlertConfig, TeamsAlertConfig
from data.models import ThresholdBreach
from orchestration.notifier import filter_breaches_by_min_level, send_alerts


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


def test_send_alerts_no_channel_enabled_noop() -> None:
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

    # Should not raise even though channels are disabled.
    send_alerts(cfg, app_id="app1", window_start="2026-02-25T00:00:00Z", window_end="2026-02-25T01:00:00Z", breaches=breaches)
