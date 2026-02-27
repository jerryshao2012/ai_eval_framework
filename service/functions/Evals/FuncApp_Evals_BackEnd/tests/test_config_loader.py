import json
import pytest
from pathlib import Path

from config.loader import load_config, load_config_section, resolve_app_config

BASE_DIR = Path(__file__).resolve().parents[1]
CONFIG_PATH = str(BASE_DIR / "config" / "config.yaml")


def test_load_config_parses_policies_and_thresholds() -> None:
    cfg = load_config(CONFIG_PATH)

    assert "app1" in cfg.applications
    assert "performance_precision_coherence" in cfg.evaluation_policies

    app_cfg = resolve_app_config(cfg, "app1")
    assert app_cfg.batch_time == "0 2 * * *"
    assert "performance_precision_coherence" in app_cfg.policy_names
    assert cfg.telemetry_source.type == "cosmos"


def test_load_config_json(tmp_path: Path) -> None:
    payload = {
        "default_batch_time": "0 * * * *",
        "evaluation_policies": {
            "performance_precision_coherence": {
                "metrics": ["performance_precision_coherence"],
                "parameters": {"version": "1.1"},
            }
        },
        "app_config": {
            "appx": {
                "batch_time": "0 1 * * *",
                "evaluation_policies": ["performance_precision_coherence"],
                "thresholds": {
                    "performance_precision_coherence": [{"level": "warning", "value": 0.9, "direction": "min"}]
                },
            }
        },
    }
    file_path = tmp_path / "cfg.json"
    file_path.write_text(json.dumps(payload))

    cfg = load_config(str(file_path))
    app_cfg = resolve_app_config(cfg, "appx")

    assert app_cfg.policy_names == ["performance_precision_coherence"]
    assert app_cfg.thresholds["performance_precision_coherence"][0].value == 0.9


def test_missing_config_raises() -> None:
    """load_config should raise FileNotFoundError for a non-existent path."""
    with pytest.raises(FileNotFoundError, match="not found"):
        load_config("/tmp/does_not_exist_ever.yaml")


def test_global_thresholds_merged_with_app_thresholds() -> None:
    """Per-app thresholds should override globals; non-overridden globals should persist."""
    cfg = load_config(CONFIG_PATH)
    app_cfg = resolve_app_config(cfg, "app1")

    # app1 should inherit global thresholds
    assert "system_reliability_latency" in app_cfg.thresholds
    assert "safety_toxicity" in app_cfg.thresholds


def test_unsupported_config_format_raises(tmp_path: Path) -> None:
    """load_config should raise ValueError for unsupported file extensions."""
    bad_file = tmp_path / "config.toml"
    bad_file.write_text("default_batch_time = '0 * * * *'\n")

    with pytest.raises(ValueError, match="Unsupported"):
        load_config(str(bad_file))


def test_app_without_policies_uses_root_default_policies(tmp_path: Path) -> None:
    payload = {
        "default_batch_time": "0 * * * *",
        "evaluation_policies": {
            "performance_precision_coherence": {"metrics": ["performance_precision_coherence"], "parameters": {"version": "1.1"}},
            "system_reliability_latency": {"metrics": ["system_reliability_latency"], "parameters": {}},
        },
        "default_evaluation_policies": ["performance_precision_coherence"],
        "app_config": {"appx": {"batch_time": "0 1 * * *"}},
    }
    file_path = tmp_path / "cfg.json"
    file_path.write_text(json.dumps(payload))

    cfg = load_config(str(file_path))
    app_cfg = resolve_app_config(cfg, "appx")

    assert app_cfg.policy_names == ["performance_precision_coherence"]


def test_default_policies_fall_back_to_all_defined_policies(tmp_path: Path) -> None:
    payload = {
        "default_batch_time": "0 * * * *",
        "evaluation_policies": {
            "performance_precision_coherence": {"metrics": ["performance_precision_coherence"], "parameters": {"version": "1.1"}},
            "system_reliability_latency": {"metrics": ["system_reliability_latency"], "parameters": {}},
        },
        "app_config": {"appx": {"batch_time": "0 1 * * *"}},
    }
    file_path = tmp_path / "cfg.json"
    file_path.write_text(json.dumps(payload))

    cfg = load_config(str(file_path))
    app_cfg = resolve_app_config(cfg, "appx")

    assert app_cfg.policy_names == ["performance_precision_coherence", "system_reliability_latency"]


def test_unknown_app_id_uses_root_defaults() -> None:
    cfg = load_config(CONFIG_PATH)

    app_cfg = resolve_app_config(cfg, "app_not_configured")

    assert app_cfg.app_id == "app_not_configured"
    assert app_cfg.batch_time == cfg.default_batch_time
    assert app_cfg.policy_names == cfg.default_evaluation_policies
    assert app_cfg.metadata == {}
    assert app_cfg.thresholds == cfg.global_thresholds


def test_alerting_config_parsed_from_json(tmp_path: Path) -> None:
    payload = {
        "default_batch_time": "0 * * * *",
        "evaluation_policies": {
            "performance_precision_coherence": {"metrics": ["performance_precision_coherence"], "parameters": {}}
        },
        "alerting": {
            "enabled": True,
            "min_level": "critical",
            "batch_window_seconds": 2.5,
            "shutdown_drain_timeout_seconds": 22,
            "circuit_failure_threshold": 4,
            "circuit_recovery_timeout_seconds": 75,
            "email": {
                "enabled": True,
                "smtp_host": "smtp.example.com",
                "smtp_port": 2525,
                "from_address": "noreply@example.com",
                "to_addresses": "a@example.com,b@example.com",
            },
            "teams": {
                "enabled": True,
                "webhook_url": "https://example.webhook",
            },
        },
        "app_config": {"appx": {}},
    }
    file_path = tmp_path / "cfg.json"
    file_path.write_text(json.dumps(payload))

    cfg = load_config(str(file_path))
    assert cfg.alerting.enabled is True
    assert cfg.alerting.min_level == "critical"
    assert cfg.alerting.batch_window_seconds == 2.5
    assert cfg.alerting.shutdown_drain_timeout_seconds == 22
    assert cfg.alerting.circuit_failure_threshold == 4
    assert cfg.alerting.circuit_recovery_timeout_seconds == 75
    assert cfg.alerting.email.enabled is True
    assert cfg.alerting.email.smtp_port == 2525
    assert cfg.alerting.email.to_addresses == ["a@example.com", "b@example.com"]
    assert cfg.alerting.teams.enabled is True


def test_telemetry_source_otlp_parsed_from_json(tmp_path: Path) -> None:
    payload = {
        "default_batch_time": "0 * * * *",
        "telemetry_source": {
            "type": "otlp",
            "otlp_file_path": "/tmp/otlp.json",
        },
        "evaluation_policies": {
            "performance_precision_coherence": {
                "metrics": ["performance_precision_coherence"],
                "parameters": {},
            }
        },
        "app_config": {"appx": {}},
    }
    file_path = tmp_path / "cfg.json"
    file_path.write_text(json.dumps(payload))

    cfg = load_config(str(file_path))
    assert cfg.telemetry_source.type == "otlp"
    assert cfg.telemetry_source.otlp_file_path == "/tmp/otlp.json"


def test_batch_concurrency_parsed_from_json(tmp_path: Path) -> None:
    payload = {
        "default_batch_time": "0 * * * *",
        "batch_app_concurrency": 4,
        "batch_policy_concurrency": 3,
        "evaluation_policies": {
            "performance_precision_coherence": {
                "metrics": ["performance_precision_coherence"],
                "parameters": {},
            }
        },
        "app_config": {"appx": {}},
    }
    file_path = tmp_path / "cfg.json"
    file_path.write_text(json.dumps(payload))

    cfg = load_config(str(file_path))
    assert cfg.batch_app_concurrency == 4
    assert cfg.batch_policy_concurrency == 3


def test_memory_and_otlp_limits_parsed_from_json(tmp_path: Path) -> None:
    payload = {
        "default_batch_time": "0 * * * *",
        "cosmos_telemetry_page_size": 250,
        "otlp_stream_chunk_size": 64,
        "otlp_max_payload_bytes": 2048,
        "otlp_max_events_per_request": 123,
        "memory_usage_warn_mb": 256,
        "memory_usage_hard_limit_mb": 512,
        "evaluation_policies": {
            "performance_precision_coherence": {
                "metrics": ["performance_precision_coherence"],
                "parameters": {},
            }
        },
        "app_config": {"appx": {}},
    }
    file_path = tmp_path / "cfg.json"
    file_path.write_text(json.dumps(payload))

    cfg = load_config(str(file_path))
    assert cfg.cosmos_telemetry_page_size == 250
    assert cfg.otlp_stream_chunk_size == 64
    assert cfg.otlp_max_payload_bytes == 2048
    assert cfg.otlp_max_events_per_request == 123
    assert cfg.memory_usage_warn_mb == 256
    assert cfg.memory_usage_hard_limit_mb == 512


def test_cosmos_resilience_settings_parsed_from_json(tmp_path: Path) -> None:
    payload = {
        "default_batch_time": "0 * * * *",
        "cosmos": {
            "endpoint": "https://example.documents.azure.com:443/",
            "key": "secret",
            "database_name": "ai-eval",
            "enable_bulk": False,
            "pool_max_connection_size": 200,
            "operation_retry_attempts": 7,
            "operation_retry_base_delay_seconds": 0.25,
            "operation_retry_max_delay_seconds": 12.0,
            "operation_retry_jitter_seconds": 0.1,
        },
        "evaluation_policies": {
            "performance_precision_coherence": {
                "metrics": ["performance_precision_coherence"],
                "parameters": {},
            }
        },
        "app_config": {"appx": {}},
    }
    file_path = tmp_path / "cfg.json"
    file_path.write_text(json.dumps(payload))

    cfg = load_config(str(file_path))
    assert cfg.cosmos is not None
    assert cfg.cosmos.enable_bulk is False
    assert cfg.cosmos.pool_max_connection_size == 200
    assert cfg.cosmos.operation_retry_attempts == 7
    assert cfg.cosmos.operation_retry_base_delay_seconds == 0.25
    assert cfg.cosmos.operation_retry_max_delay_seconds == 12.0
    assert cfg.cosmos.operation_retry_jitter_seconds == 0.1


def test_load_config_uses_singleton_cache(tmp_path: Path) -> None:
    payload = {
        "default_batch_time": "0 * * * *",
        "evaluation_policies": {
            "performance_precision_coherence": {
                "metrics": ["performance_precision_coherence"],
                "parameters": {},
            }
        },
        "app_config": {"appx": {}},
    }
    file_path = tmp_path / "cfg.json"
    file_path.write_text(json.dumps(payload))

    cfg1 = load_config(str(file_path), ttl_seconds=3600)
    cfg2 = load_config(str(file_path), ttl_seconds=3600)
    assert cfg1 is cfg2


def test_load_config_force_reload_ignores_cache(tmp_path: Path) -> None:
    payload = {
        "default_batch_time": "0 * * * *",
        "evaluation_policies": {
            "performance_precision_coherence": {
                "metrics": ["performance_precision_coherence"],
                "parameters": {},
            }
        },
        "app_config": {"appx": {}},
    }
    file_path = tmp_path / "cfg.json"
    file_path.write_text(json.dumps(payload))

    cfg1 = load_config(str(file_path))
    payload["default_batch_time"] = "0 1 * * *"
    file_path.write_text(json.dumps(payload))
    cfg2 = load_config(str(file_path), force_reload=True)

    assert cfg1.default_batch_time == "0 * * * *"
    assert cfg2.default_batch_time == "0 1 * * *"


def test_load_config_section_lazy_access(tmp_path: Path) -> None:
    payload = {
        "default_batch_time": "0 * * * *",
        "telemetry_source": {
            "type": "otlp",
            "otlp_file_path": "/tmp/otlp.json",
        },
        "evaluation_policies": {
            "performance_precision_coherence": {
                "metrics": ["performance_precision_coherence"],
                "parameters": {},
            }
        },
        "app_config": {"appx": {}},
    }
    file_path = tmp_path / "cfg.json"
    file_path.write_text(json.dumps(payload))

    telemetry_source = load_config_section(str(file_path), "telemetry_source")
    policies = load_config_section(str(file_path), "evaluation_policies")

    assert telemetry_source.type == "otlp"
    assert telemetry_source.otlp_file_path == "/tmp/otlp.json"
    assert "performance_precision_coherence" in policies
