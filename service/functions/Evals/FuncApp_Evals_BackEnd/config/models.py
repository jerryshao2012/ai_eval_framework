from __future__ import annotations

from dataclasses import dataclass, field
from functools import cached_property
from typing import Any, Dict, List, Optional
import os


def _expand_env_vars(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {key: _expand_env_vars(value) for key, value in obj.items()}
    if isinstance(obj, list):
        return [_expand_env_vars(item) for item in obj]
    if isinstance(obj, str):
        return os.path.expandvars(obj)
    return obj


def _to_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


@dataclass
class ThresholdConfig:
    level: str
    value: float
    direction: str  # "min" => alert when value < threshold; "max" => alert when value > threshold


def _parse_thresholds(raw: Dict[str, Any]) -> Dict[str, List[ThresholdConfig]]:
    parsed: Dict[str, List[ThresholdConfig]] = {}
    for metric_name, entries in raw.items():
        parsed[metric_name] = [
            ThresholdConfig(
                level=item["level"],
                value=float(item["value"]),
                direction=item.get("direction", "min"),
            )
            for item in entries
        ]
    return parsed


@dataclass
class PolicyConfig:
    name: str
    metrics: List[str]
    parameters: Dict[str, Any] = field(default_factory=dict)


def _parse_policies(raw: Dict[str, Any]) -> Dict[str, PolicyConfig]:
    parsed: Dict[str, PolicyConfig] = {}
    for name, cfg in raw.items():
        parsed[name] = PolicyConfig(
            name=name,
            metrics=cfg.get("metrics", []),
            parameters=cfg.get("parameters", {}),
        )
    return parsed


@dataclass
class AppConfig:
    app_id: str
    batch_time: Optional[str] = None
    evaluation_policies: List[str] = field(default_factory=list)
    thresholds: Dict[str, List[ThresholdConfig]] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


def _parse_applications(raw: Dict[str, Any]) -> Dict[str, AppConfig]:
    parsed: Dict[str, AppConfig] = {}
    for app_id, cfg in raw.items():
        policies_raw = cfg.get("evaluation_policies", [])
        policies = [p.strip() for p in policies_raw.split(",")] if isinstance(policies_raw, str) else policies_raw

        parsed[app_id] = AppConfig(
            app_id=app_id,
            batch_time=cfg.get("batch_time"),
            evaluation_policies=policies,
            thresholds=_parse_thresholds(cfg.get("thresholds", {})),
            metadata=cfg.get("metadata", {}),
        )
    return parsed


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


def _parse_telemetry_source(raw: Dict[str, Any]) -> TelemetrySourceConfig:
    source_type = str(raw.get("type", "cosmos")).strip().lower()
    if source_type not in {"cosmos", "otlp"}:
        raise ValueError("telemetry_source.type must be either 'cosmos' or 'otlp'.")
    return TelemetrySourceConfig(
        type=source_type,
        otlp_file_path=str(raw.get("otlp_file_path", "")),
    )


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


def _parse_alerting(raw: Dict[str, Any]) -> AlertingConfig:
    email_raw = raw.get("email", {})
    teams_raw = raw.get("teams", {})
    to_addresses_raw = email_raw.get("to_addresses", [])
    to_addresses = (
        [item.strip() for item in to_addresses_raw.split(",") if item.strip()]
        if isinstance(to_addresses_raw, str)
        else list(to_addresses_raw)
    )

    return AlertingConfig(
        enabled=bool(raw.get("enabled", False)),
        min_level=str(raw.get("min_level", "warning")).lower(),
        email=EmailAlertConfig(
            enabled=bool(email_raw.get("enabled", False)),
            smtp_host=str(email_raw.get("smtp_host", "")),
            smtp_port=int(email_raw.get("smtp_port", 587)),
            username=str(email_raw.get("username", "")),
            password=str(email_raw.get("password", "")),
            from_address=str(email_raw.get("from_address", "")),
            to_addresses=to_addresses,
            use_tls=bool(email_raw.get("use_tls", True)),
        ),
        teams=TeamsAlertConfig(
            enabled=bool(teams_raw.get("enabled", False)),
            webhook_url=str(teams_raw.get("webhook_url", "")),
        ),
    )


class RootConfig:
    def __init__(self, raw: Dict[str, Any]) -> None:
        self._raw = _expand_env_vars(raw)

    @cached_property
    def default_batch_time(self) -> str:
        return self._raw.get("default_batch_time", "0 * * * *")

    @cached_property
    def batch_app_concurrency(self) -> int:
        return int(self._raw.get("batch_app_concurrency", os.getenv("BATCH_APP_CONCURRENCY", 10)))

    @cached_property
    def batch_policy_concurrency(self) -> int:
        return int(self._raw.get("batch_policy_concurrency", os.getenv("BATCH_POLICY_CONCURRENCY", 10)))

    @cached_property
    def cosmos_telemetry_page_size(self) -> int:
        return int(self._raw.get("cosmos_telemetry_page_size", os.getenv("COSMOS_TELEMETRY_PAGE_SIZE", 100)))

    @cached_property
    def otlp_stream_chunk_size(self) -> int:
        return int(self._raw.get("otlp_stream_chunk_size", os.getenv("OTLP_STREAM_CHUNK_SIZE", 100)))

    @cached_property
    def otlp_max_payload_bytes(self) -> int:
        return int(self._raw.get("otlp_max_payload_bytes", os.getenv("OTLP_MAX_PAYLOAD_BYTES", 10_485_760)))

    @cached_property
    def otlp_max_events_per_request(self) -> int:
        return int(self._raw.get("otlp_max_events_per_request", os.getenv("OTLP_MAX_EVENTS_PER_REQUEST", 50_000)))

    @cached_property
    def memory_usage_warn_mb(self) -> int:
        return int(self._raw.get("memory_usage_warn_mb", os.getenv("MEMORY_USAGE_WARN_MB", 1024)))

    @cached_property
    def memory_usage_hard_limit_mb(self) -> int:
        return int(self._raw.get("memory_usage_hard_limit_mb", os.getenv("MEMORY_USAGE_HARD_LIMIT_MB", 0)))

    @cached_property
    def evaluation_policies(self) -> Dict[str, PolicyConfig]:
        return _parse_policies(self._raw.get("evaluation_policies", {}))

    @cached_property
    def default_evaluation_policies(self) -> List[str]:
        default_policy_raw = self._raw.get("default_evaluation_policies")
        if default_policy_raw is None:
            return list(self.evaluation_policies.keys())
        elif isinstance(default_policy_raw, str):
            return [p.strip() for p in default_policy_raw.split(",") if p.strip()]
        else:
            return list(default_policy_raw)

    @cached_property
    def global_thresholds(self) -> Dict[str, List[ThresholdConfig]]:
        return _parse_thresholds(self._raw.get("global_thresholds", {}))

    @cached_property
    def applications(self) -> Dict[str, AppConfig]:
        return _parse_applications(self._raw.get("app_config", {}))

    @cached_property
    def telemetry_source(self) -> TelemetrySourceConfig:
        return _parse_telemetry_source(self._raw.get("telemetry_source", {}))

    @cached_property
    def alerting(self) -> AlertingConfig:
        return _parse_alerting(self._raw.get("alerting", {}))

    @cached_property
    def cosmos(self) -> Optional[CosmosConfig]:
        cosmos_cfg = self._raw.get("cosmos", {})
        if cosmos_cfg or os.getenv("COSMOS_ENDPOINT"):
            return CosmosConfig(
                endpoint=cosmos_cfg.get("endpoint", os.getenv("COSMOS_ENDPOINT", "")),
                key=cosmos_cfg.get("key", os.getenv("COSMOS_KEY", "")),
                database_name=cosmos_cfg.get("database_name", os.getenv("COSMOS_DATABASE", "ai-eval")),
                telemetry_container=cosmos_cfg.get("telemetry_container", "telemetry"),
                results_container=cosmos_cfg.get("results_container", "evaluation_results"),
                enable_bulk=_to_bool(
                    cosmos_cfg.get("enable_bulk", os.getenv("COSMOS_ENABLE_BULK")),
                    True,
                ),
                pool_max_connection_size=int(
                    cosmos_cfg.get(
                        "pool_max_connection_size",
                        os.getenv("COSMOS_POOL_MAX_CONNECTION_SIZE", 100),
                    )
                ),
                client_retry_total=int(
                    cosmos_cfg.get("client_retry_total", os.getenv("COSMOS_CLIENT_RETRY_TOTAL", 10))
                ),
                client_retry_backoff_max=int(
                    cosmos_cfg.get(
                        "client_retry_backoff_max",
                        os.getenv("COSMOS_CLIENT_RETRY_BACKOFF_MAX", 30),
                    )
                ),
                client_retry_backoff_factor=float(
                    cosmos_cfg.get(
                        "client_retry_backoff_factor",
                        os.getenv("COSMOS_CLIENT_RETRY_BACKOFF_FACTOR", 1),
                    )
                ),
                client_connection_timeout=int(
                    cosmos_cfg.get(
                        "client_connection_timeout",
                        os.getenv("COSMOS_CLIENT_CONNECTION_TIMEOUT", 60),
                    )
                ),
                operation_retry_attempts=int(
                    cosmos_cfg.get(
                        "operation_retry_attempts",
                        os.getenv("COSMOS_OPERATION_RETRY_ATTEMPTS", 5),
                    )
                ),
                operation_retry_base_delay_seconds=float(
                    cosmos_cfg.get(
                        "operation_retry_base_delay_seconds",
                        os.getenv("COSMOS_OPERATION_RETRY_BASE_DELAY_SECONDS", 0.5),
                    )
                ),
                operation_retry_max_delay_seconds=float(
                    cosmos_cfg.get(
                        "operation_retry_max_delay_seconds",
                        os.getenv("COSMOS_OPERATION_RETRY_MAX_DELAY_SECONDS", 8),
                    )
                ),
                operation_retry_jitter_seconds=float(
                    cosmos_cfg.get(
                        "operation_retry_jitter_seconds",
                        os.getenv("COSMOS_OPERATION_RETRY_JITTER_SECONDS", 0.25),
                    )
                ),
            )
        return None


@dataclass
class ResolvedAppConfig:
    app_id: str
    batch_time: str
    policy_names: List[str]
    policies: List[PolicyConfig]
    thresholds: Dict[str, List[ThresholdConfig]]
    metadata: Dict[str, Any] = field(default_factory=dict)
