import json
import pytest
from pathlib import Path

from config.loader import load_config, resolve_app_config

BASE_DIR = Path(__file__).resolve().parents[1]
CONFIG_PATH = str(BASE_DIR / "config" / "config.yaml")


def test_load_config_parses_policies_and_thresholds() -> None:
    cfg = load_config(CONFIG_PATH)

    assert "app1" in cfg.applications
    assert "performance_precision_coherence" in cfg.evaluation_policies

    app_cfg = resolve_app_config(cfg, "app1")
    assert app_cfg.batch_time == "0 2 * * *"
    assert "performance_precision_coherence" in app_cfg.policy_names


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
    assert cfg.alerting.email.enabled is True
    assert cfg.alerting.email.smtp_port == 2525
    assert cfg.alerting.email.to_addresses == ["a@example.com", "b@example.com"]
    assert cfg.alerting.teams.enabled is True
