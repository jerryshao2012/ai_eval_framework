from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def partition_key_for(app_id: str, timestamp: str) -> str:
    # Single synthetic partition key to co-locate app/time-slice data.
    date_slice = timestamp[:10]
    return f"{app_id}:{date_slice}"


@dataclass
class TelemetryRecord:
    id: str
    app_id: str
    timestamp: str
    model_id: str
    model_version: str
    input_text: str
    output_text: str
    expected_output: Optional[str] = None
    user_id: Optional[str] = None
    latency_ms: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["type"] = "telemetry"
        payload["pk"] = partition_key_for(self.app_id, self.timestamp)
        return payload


@dataclass
class MetricValueVersioned:
    metric_name: str
    value: float
    version: str
    timestamp: str
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ThresholdBreach:
    metric_name: str
    level: str
    threshold_value: float
    actual_value: float
    direction: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class EvaluationResult:
    id: str
    app_id: str
    timestamp: str
    policy_name: str
    metrics: List[MetricValueVersioned]
    breaches: List[ThresholdBreach] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "type": "evaluation_result",
            "app_id": self.app_id,
            "timestamp": self.timestamp,
            "pk": partition_key_for(self.app_id, self.timestamp),
            "policy_name": self.policy_name,
            "metrics": [metric.to_dict() for metric in self.metrics],
            "breaches": [breach.to_dict() for breach in self.breaches],
        }
