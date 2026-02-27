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
    enable_bulk: bool = True
    pool_max_connection_size: int = 100
    client_retry_total: int = 10
    client_retry_backoff_max: int = 30
    client_retry_backoff_factor: float = 1.0
    client_connection_timeout: int = 60
    operation_retry_attempts: int = 5
    operation_retry_base_delay_seconds: float = 0.5
    operation_retry_max_delay_seconds: float = 8.0
    operation_retry_jitter_seconds: float = 0.25


@dataclass
class TelemetrySourceConfig:
    type: str = "cosmos"  # "cosmos" | "otlp"
    otlp_file_path: str = ""


@dataclass
class EmailAlertConfig:
    enabled: bool = False
    smtp_host: str = ""
    smtp_port: int = 587
    username: str = ""
    password: str = ""
    from_address: str = ""
    to_addresses: List[str] = field(default_factory=list)
    use_tls: bool = True


@dataclass
class TeamsAlertConfig:
    enabled: bool = False
    webhook_url: str = ""


@dataclass
class AlertingConfig:
    enabled: bool = False
    min_level: str = "warning"
    email: EmailAlertConfig = field(default_factory=EmailAlertConfig)
    teams: TeamsAlertConfig = field(default_factory=TeamsAlertConfig)


@dataclass
class RootConfig:
    default_batch_time: str
    batch_app_concurrency: int = 10
    batch_policy_concurrency: int = 10
    cosmos_telemetry_page_size: int = 100
    otlp_stream_chunk_size: int = 100
    otlp_max_payload_bytes: int = 10_485_760  # 10 MiB
    otlp_max_events_per_request: int = 50_000
    memory_usage_warn_mb: int = 1024
    memory_usage_hard_limit_mb: int = 0
    evaluation_policies: Dict[str, PolicyConfig] = field(default_factory=dict)
    default_evaluation_policies: List[str] = field(default_factory=list)
    global_thresholds: Dict[str, List[ThresholdConfig]] = field(default_factory=dict)
    applications: Dict[str, AppConfig] = field(default_factory=dict)
    cosmos: Optional[CosmosConfig] = None
    telemetry_source: TelemetrySourceConfig = field(default_factory=TelemetrySourceConfig)
    alerting: AlertingConfig = field(default_factory=AlertingConfig)


@dataclass
class ResolvedAppConfig:
    app_id: str
    batch_time: str
    policy_names: List[str]
    policies: List[PolicyConfig]
    thresholds: Dict[str, List[ThresholdConfig]]
    metadata: Dict[str, Any] = field(default_factory=dict)
