from data.repositories import _pick_telemetry_fields


def test_pick_telemetry_fields_preserves_existing_metadata_trace_id() -> None:
    row = {
        "id": "t1",
        "app_id": "app1",
        "timestamp": "2026-02-24T00:00:00Z",
        "model_id": "m",
        "model_version": "v",
        "input_text": "in",
        "output_text": "out",
        "metadata": {"trace_id": "from-metadata"},
        "trace_id": "from-top-level",
        "ignored": "x",
    }

    payload = _pick_telemetry_fields(row)
    assert payload["metadata"]["trace_id"] == "from-metadata"
    assert "ignored" not in payload


def test_pick_telemetry_fields_promotes_top_level_trace_id() -> None:
    row = {
        "id": "t2",
        "app_id": "app1",
        "timestamp": "2026-02-24T00:00:00Z",
        "model_id": "m",
        "model_version": "v",
        "input_text": "in",
        "output_text": "out",
        "trace_id": "top-level-trace",
    }

    payload = _pick_telemetry_fields(row)
    assert payload["metadata"]["trace_id"] == "top-level-trace"
