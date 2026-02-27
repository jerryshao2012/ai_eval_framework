import json

import dashboard.app as dashboard_app
from orchestration.job_tracking import SqliteJobStatusStore


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

    history_paged = client.get("/api/batch/history?page=1&page_size=1")
    assert history_paged.status_code == 200
    history_paged_payload = history_paged.get_json()
    assert history_paged_payload["page"] == 1
    assert history_paged_payload["page_size"] == 1
    assert history_paged_payload["total"] == 1
    assert len(history_paged_payload["items"]) == 1

    logs = client.get("/api/batch/run/run-x/item/app2/logs")
    assert logs.status_code == 200
    logs_payload = logs.get_json()
    assert logs_payload["status"] == "failed"
    assert logs_payload["traceback"] == "trace"


def test_batch_status_endpoints_prefer_sqlite_store(tmp_path) -> None:
    db_file = tmp_path / "batch_status.db"
    store = SqliteJobStatusStore(db_file)
    try:
        run_id = "run-db"
        store.start_run(
            run_id=run_id,
            app_ids=["app1"],
            window_start="2026-02-25T00:00:00Z",
            window_end="2026-02-25T01:00:00Z",
            group_size=100,
            group_index=0,
        )
        store.mark_item_running(run_id, "app1")
        store.append_item_log(run_id, "app1", "INFO", "started")
        store.mark_item_completed(
            run_id=run_id,
            item_id="app1",
            policy_runs=3,
            breach_count=0,
            next_batch_run_utc="2026-02-26T00:00:00Z",
        )
        store.finalize_run(run_id)
    finally:
        store.close()

    dashboard_app.STATUS_DB_FILE = db_file
    dashboard_app.STATUS_FILE = tmp_path / "batch_status.json"
    client = dashboard_app.app.test_client()

    current = client.get("/api/batch/current")
    assert current.status_code == 200
    current_payload = current.get_json()
    assert current_payload["run_id"] == "run-db"
    assert current_payload["stats"]["completed_items"] == 1
