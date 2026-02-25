from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class ThresholdConfig:
    level: str
    value: float
    direction: str  # "min" => alert when value < threshold; "max" => alert when value > threshold


@dataclass
class PolicyConfig:
    name: str
    metrics: List[str]
    parameters: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AppConfig:
    app_id: str
    batch_time: Optional[str] = None
    evaluation_policies: List[str] = field(default_factory=list)
    thresholds: Dict[str, List[ThresholdConfig]] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CosmosConfig:
    endpoint: str
    key: str
    database_name: str
    telemetry_container: str = "telemetry"
    results_container: str = "evaluation_results"


@dataclass
class RootConfig:
    default_batch_time: str
    evaluation_policies: Dict[str, PolicyConfig] = field(default_factory=dict)
    default_evaluation_policies: List[str] = field(default_factory=list)
    global_thresholds: Dict[str, List[ThresholdConfig]] = field(default_factory=dict)
    applications: Dict[str, AppConfig] = field(default_factory=dict)
    cosmos: Optional[CosmosConfig] = None


@dataclass
class ResolvedAppConfig:
    app_id: str
    batch_time: str
    policy_names: List[str]
    policies: List[PolicyConfig]
    thresholds: Dict[str, List[ThresholdConfig]]
    metadata: Dict[str, Any] = field(default_factory=dict)
