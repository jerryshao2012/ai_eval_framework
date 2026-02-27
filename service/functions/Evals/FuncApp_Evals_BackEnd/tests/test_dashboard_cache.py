from __future__ import annotations

import json
import os

import dashboard.app as dashboard_app


def test_latest_and_alerts_use_mock_file_cache_with_mtime_invalidation(tmp_path, monkeypatch) -> None:
    mock_file = tmp_path / "mock_results.json"
    mock_file.write_text(
        json.dumps(
            {
                "latest": [
                    {
                        "app_id": "app1",
                        "timestamp": "2026-02-27T00:00:00Z",
                        "policy_name": "p",
                        "metrics": [
                            {
                                "metric_name": "performance_precision_coherence",
                                "value": 0.8,
                                "version": "1.0",
                            }
                        ],
                        "breaches": [],
                    }
                ],
                "trends": {},
            }
        )
    )

    original_read_text = dashboard_app.Path.read_text
    read_count = {"count": 0}

    def counting_read_text(path_obj, *args, **kwargs):
        if path_obj == mock_file:
            read_count["count"] += 1
        return original_read_text(path_obj, *args, **kwargs)

    monkeypatch.setattr(dashboard_app, "MOCK_FILE", mock_file)
    monkeypatch.setattr(dashboard_app, "_FILE_CACHE", dashboard_app.FileCache())
    monkeypatch.setattr(dashboard_app.Path, "read_text", counting_read_text)

    client = dashboard_app.app.test_client()

    resp1 = client.get("/api/latest")
    assert resp1.status_code == 200
    resp2 = client.get("/api/alerts")
    assert resp2.status_code == 200
    assert read_count["count"] == 1

    mock_file.write_text(
        json.dumps(
            {
                "latest": [
                    {
                        "app_id": "app1",
                        "timestamp": "2026-02-27T00:05:00Z",
                        "policy_name": "p",
                        "metrics": [
                            {
                                "metric_name": "performance_precision_coherence",
                                "value": 0.95,
                                "version": "1.0",
                            }
                        ],
                        "breaches": [],
                    }
                ],
                "trends": {},
            }
        )
    )
    stat = mock_file.stat()
    os.utime(mock_file, ns=(stat.st_atime_ns, stat.st_mtime_ns + 2_000_000_000))

    resp3 = client.get("/api/latest")
    assert resp3.status_code == 200
    rows = resp3.get_json()
    assert rows[0]["timestamp"] == "2026-02-27T00:05:00Z"
    assert read_count["count"] == 2


def test_batch_endpoints_use_status_file_cache_with_mtime_invalidation(tmp_path, monkeypatch) -> None:
    status_file = tmp_path / "batch_status.json"
    status_file.write_text(
        json.dumps(
            {
                "runs": [
                    {
                        "run_id": "run-1",
                        "status": "completed",
                        "started_at": "2026-02-27T00:00:00Z",
                        "ended_at": "2026-02-27T00:05:00Z",
                        "items": [],
                    }
                ]
            }
        )
    )

    original_read_text = dashboard_app.Path.read_text
    read_count = {"count": 0}

    def counting_read_text(path_obj, *args, **kwargs):
        if path_obj == status_file:
            read_count["count"] += 1
        return original_read_text(path_obj, *args, **kwargs)

    monkeypatch.setattr(dashboard_app, "STATUS_DB_FILE", tmp_path / "does-not-exist.db")
    monkeypatch.setattr(dashboard_app, "STATUS_FILE", status_file)
    monkeypatch.setattr(dashboard_app, "_FILE_CACHE", dashboard_app.FileCache())
    monkeypatch.setattr(dashboard_app.Path, "read_text", counting_read_text)

    client = dashboard_app.app.test_client()

    resp1 = client.get("/api/batch/current")
    assert resp1.status_code == 200
    resp2 = client.get("/api/batch/history")
    assert resp2.status_code == 200
    assert read_count["count"] == 1

    status_file.write_text(
        json.dumps(
            {
                "runs": [
                    {
                        "run_id": "run-2",
                        "status": "completed",
                        "started_at": "2026-02-27T01:00:00Z",
                        "ended_at": "2026-02-27T01:05:00Z",
                        "items": [],
                    }
                ]
            }
        )
    )
    stat = status_file.stat()
    os.utime(status_file, ns=(stat.st_atime_ns, stat.st_mtime_ns + 2_000_000_000))

    resp3 = client.get("/api/batch/current")
    assert resp3.status_code == 200
    assert resp3.get_json()["run_id"] == "run-2"
    assert read_count["count"] == 2
