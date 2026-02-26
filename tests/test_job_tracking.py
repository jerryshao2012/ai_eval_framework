import json
from pathlib import Path

from orchestration.job_tracking import FileJobStatusStore


def test_file_job_status_store_lifecycle(tmp_path: Path) -> None:
    store = FileJobStatusStore(tmp_path / "batch_status.json")
    run_id = "run-1"

    store.start_run(
        run_id=run_id,
        app_ids=["app1", "app2"],
        window_start="2026-02-25T00:00:00Z",
        window_end="2026-02-25T01:00:00Z",
        group_size=100,
        group_index=0,
    )
    store.mark_item_running(run_id, "app1")
    store.append_item_log(run_id, "app1", "INFO", "started")
    store.mark_item_completed(
        run_id,
        "app1",
        policy_runs=2,
        breach_count=1,
        next_batch_run_utc="2026-02-26T00:00:00Z",
    )
    store.mark_item_running(run_id, "app2")
    store.mark_item_failed(run_id, "app2", "boom", "traceback")
    store.finalize_run(run_id)

    payload = json.loads((tmp_path / "batch_status.json").read_text())
    run = payload["runs"][0]
    assert run["status"] == "partial_failed"
    app1 = next(i for i in run["items"] if i["item_id"] == "app1")
    app2 = next(i for i in run["items"] if i["item_id"] == "app2")
    assert app1["status"] == "completed"
    assert app1["policy_runs"] == 2
    assert app2["status"] == "failed"
    assert app2["error"] == "boom"
