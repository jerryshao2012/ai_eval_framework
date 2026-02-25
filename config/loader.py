from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

import yaml

from config.models import (
    AppConfig,
    CosmosConfig,
    PolicyConfig,
    ResolvedAppConfig,
    RootConfig,
    ThresholdConfig,
)


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


def _expand_env_vars(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {key: _expand_env_vars(value) for key, value in obj.items()}
    if isinstance(obj, list):
        return [_expand_env_vars(item) for item in obj]
    if isinstance(obj, str):
        return os.path.expandvars(obj)
    return obj


def _parse_policies(raw: Dict[str, Any]) -> Dict[str, PolicyConfig]:
    parsed: Dict[str, PolicyConfig] = {}
    for name, cfg in raw.items():
        parsed[name] = PolicyConfig(
            name=name,
            metrics=cfg.get("metrics", []),
            parameters=cfg.get("parameters", {}),
        )
    return parsed


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


def load_config(config_path: str) -> RootConfig:
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    raw: Dict[str, Any]
    if path.suffix.lower() in {".yaml", ".yml"}:
        raw = yaml.safe_load(path.read_text())
    elif path.suffix.lower() == ".json":
        raw = json.loads(path.read_text())
    else:
        raise ValueError("Unsupported config file format. Use YAML or JSON.")
    raw = _expand_env_vars(raw)

    cosmos_cfg = raw.get("cosmos", {})
    cosmos = None
    if cosmos_cfg or os.getenv("COSMOS_ENDPOINT"):
        cosmos = CosmosConfig(
            endpoint=cosmos_cfg.get("endpoint", os.getenv("COSMOS_ENDPOINT", "")),
            key=cosmos_cfg.get("key", os.getenv("COSMOS_KEY", "")),
            database_name=cosmos_cfg.get("database_name", os.getenv("COSMOS_DATABASE", "ai-eval")),
            telemetry_container=cosmos_cfg.get("telemetry_container", "telemetry"),
            results_container=cosmos_cfg.get("results_container", "evaluation_results"),
        )

    parsed_policies = _parse_policies(raw.get("evaluation_policies", {}))
    default_policy_raw = raw.get("default_evaluation_policies")
    if default_policy_raw is None:
        default_policy_names = list(parsed_policies.keys())
    elif isinstance(default_policy_raw, str):
        default_policy_names = [p.strip() for p in default_policy_raw.split(",") if p.strip()]
    else:
        default_policy_names = list(default_policy_raw)

    return RootConfig(
        default_batch_time=raw.get("default_batch_time", "0 * * * *"),
        evaluation_policies=parsed_policies,
        default_evaluation_policies=default_policy_names,
        global_thresholds=_parse_thresholds(raw.get("global_thresholds", {})),
        applications=_parse_applications(raw.get("app_config", {})),
        cosmos=cosmos,
    )


def resolve_app_config(root: RootConfig, app_id: str) -> ResolvedAppConfig:
    if app_id in root.applications:
        app = root.applications[app_id]
    else:
        logger.info(
            "app_id=%s is not configured under app_config; applying root defaults.",
            app_id,
        )
        app = AppConfig(app_id=app_id)

    policy_names = app.evaluation_policies or root.default_evaluation_policies

    unknown = [name for name in policy_names if name not in root.evaluation_policies]
    if unknown:
        logger.warning(
            "app_id=%s references unknown evaluation policies (will be skipped): %s",
            app_id,
            unknown,
        )

    policies = [root.evaluation_policies[name] for name in policy_names if name in root.evaluation_policies]

    merged_thresholds = dict(root.global_thresholds)
    merged_thresholds.update(app.thresholds)

    return ResolvedAppConfig(
        app_id=app_id,
        batch_time=app.batch_time or root.default_batch_time,
        policy_names=policy_names,
        policies=policies,
        thresholds=merged_thresholds,
        metadata=app.metadata,
    )


def list_resolved_apps(root: RootConfig) -> List[ResolvedAppConfig]:
    return [resolve_app_config(root, app_id) for app_id in root.applications]
