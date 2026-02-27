from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from config.models import AppConfig, ResolvedAppConfig, RootConfig

logger = logging.getLogger(__name__)


@dataclass
class _ConfigCacheEntry:
    mtime_ns: int
    loaded_at: float
    config: RootConfig


class _ConfigLoaderSingleton:
    _instance: "_ConfigLoaderSingleton | None" = None

    def __new__(cls) -> "_ConfigLoaderSingleton":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._lock = threading.RLock()
            cls._instance._cache: Dict[str, _ConfigCacheEntry] = {}
        return cls._instance

    @staticmethod
    def _default_ttl_seconds() -> int:
        return int(os.getenv("CONFIG_CACHE_TTL_SECONDS", "60"))

    @staticmethod
    def _read_raw(path: Path) -> Dict[str, Any]:
        if path.suffix.lower() in {".yaml", ".yml"}:
            return yaml.safe_load(path.read_text())
        if path.suffix.lower() == ".json":
            return json.loads(path.read_text())
        raise ValueError("Unsupported config file format. Use YAML or JSON.")

    def load(
        self,
        config_path: str,
        ttl_seconds: Optional[int] = None,
        force_reload: bool = False,
    ) -> RootConfig:
        path = Path(config_path).resolve()
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        ttl = self._default_ttl_seconds() if ttl_seconds is None else int(ttl_seconds)
        now = time.time()
        mtime_ns = path.stat().st_mtime_ns
        key = str(path)

        with self._lock:
            entry = self._cache.get(key)
            cache_valid = (
                entry is not None
                and not force_reload
                and entry.mtime_ns == mtime_ns
                and (ttl <= 0 or (now - entry.loaded_at) <= ttl)
            )
            if cache_valid:
                return entry.config

            config = RootConfig(self._read_raw(path))
            self._cache[key] = _ConfigCacheEntry(
                mtime_ns=mtime_ns,
                loaded_at=now,
                config=config,
            )
            return config


_CONFIG_LOADER = _ConfigLoaderSingleton()


def load_config(
    config_path: str,
    ttl_seconds: Optional[int] = None,
    force_reload: bool = False,
) -> RootConfig:
    return _CONFIG_LOADER.load(
        config_path=config_path,
        ttl_seconds=ttl_seconds,
        force_reload=force_reload,
    )


def load_config_section(
    config_path: str,
    section: str,
    ttl_seconds: Optional[int] = None,
    force_reload: bool = False,
) -> Any:
    root = load_config(config_path, ttl_seconds=ttl_seconds, force_reload=force_reload)
    section_map = {
        "default_batch_time": "default_batch_time",
        "batch_app_concurrency": "batch_app_concurrency",
        "batch_policy_concurrency": "batch_policy_concurrency",
        "cosmos_telemetry_page_size": "cosmos_telemetry_page_size",
        "otlp_stream_chunk_size": "otlp_stream_chunk_size",
        "otlp_max_payload_bytes": "otlp_max_payload_bytes",
        "otlp_max_events_per_request": "otlp_max_events_per_request",
        "memory_usage_warn_mb": "memory_usage_warn_mb",
        "memory_usage_hard_limit_mb": "memory_usage_hard_limit_mb",
        "evaluation_policies": "evaluation_policies",
        "default_evaluation_policies": "default_evaluation_policies",
        "global_thresholds": "global_thresholds",
        "app_config": "applications",
        "telemetry_source": "telemetry_source",
        "alerting": "alerting",
        "cosmos": "cosmos",
    }
    if section not in section_map:
        raise KeyError(f"Unsupported config section: {section}")
    return getattr(root, section_map[section])


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
