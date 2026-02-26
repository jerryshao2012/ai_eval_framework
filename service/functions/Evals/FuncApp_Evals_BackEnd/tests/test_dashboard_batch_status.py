import json

import dashboard.app as dashboard_app


def test_batch_history_and_trace_endpoints(tmp_path) -> None:
    status_file = tmp_path / "batch_status.json"
    status_file.write_text(
        json.dumps(
            {
                "runs": [
                    {
                        "run_id": "run-x",
                        "status": "partial_failed",
                        "started_at": "2026-02-25T00:00:00Z",
                        "ended_at": "2026-02-25T00:05:00Z",
                        "items": [
                            {
                                "item_id": "app1",
                                "status": "completed",
                                "policy_runs": 2,
                                "breach_count": 1,
                                "logs": [],
                            },
                            {
                                "item_id": "app2",
                                "status": "failed",
                                "policy_runs": 0,
                                "breach_count": 0,
                                "error": "bad",
                                "traceback": "trace",
                                "logs": [{"timestamp": "t", "level": "ERROR", "message": "bad"}],
                            },
                        ],
                    }
                ]
            }
        )
    )
    dashboard_app.STATUS_FILE = status_file
    client = dashboard_app.app.test_client()

    current = client.get("/api/batch/current")
    assert current.status_code == 200
    current_payload = current.get_json()
    assert current_payload["run_id"] == "run-x"
    assert current_payload["stats"]["failed_items"] == 1

    history = client.get("/api/batch/history")
    assert history.status_code == 200
    history_payload = history.get_json()
    assert len(history_payload) == 1

    logs = client.get("/api/batch/run/run-x/item/app2/logs")
    assert logs.status_code == 200
    logs_payload = logs.get_json()
    assert logs_payload["status"] == "failed"
    assert logs_payload["traceback"] == "trace"
