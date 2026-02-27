from telemetry.emitter import _normalize_event_for_emission, _validate_emission_event


def test_validate_emission_event_accepts_top_level_trace_id() -> None:
    event = {"app_id": "app1", "trace_id": "trace-123"}
    _validate_emission_event(event)


def test_validate_emission_event_accepts_metadata_trace_id() -> None:
    event = {"app_id": "app1", "metadata": {"trace_id": "trace-456"}}
    _validate_emission_event(event)


def test_validate_emission_event_rejects_missing_trace_id() -> None:
    event = {"app_id": "app1"}
    try:
        _validate_emission_event(event)
    except ValueError as exc:
        assert "trace_id" in str(exc)
    else:
        raise AssertionError("Expected trace_id validation error")


def test_normalize_event_for_emission_applies_explicit_trace_id() -> None:
    payload = _normalize_event_for_emission({"app_id": "app1"}, trace_id="trace-explicit")
    assert payload["trace_id"] == "trace-explicit"
    assert payload["metadata"]["trace_id"] == "trace-explicit"


def test_normalize_event_for_emission_preserves_existing_trace_id() -> None:
    payload = _normalize_event_for_emission({"app_id": "app1", "metadata": {"trace_id": "trace-existing"}})
    assert payload["trace_id"] == "trace-existing"
    assert payload["metadata"]["trace_id"] == "trace-existing"
