from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class FileJobStatusStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self._write({"runs": []})

    def load_runs(self) -> List[Dict[str, Any]]:
        return self._read().get("runs", [])

    def start_run(
        self,
        run_id: str,
        app_ids: List[str],
        window_start: str,
        window_end: str,
        group_size: int,
        group_index: int,
    ) -> None:
        data = self._read()
        items = [
            {
                "item_id": app_id,
                "status": "pending",
                "started_at": None,
                "ended_at": None,
                "policy_runs": 0,
                "breach_count": 0,
                "next_batch_run_utc": None,
                "error": None,
                "traceback": None,
                "logs": [],
            }
            for app_id in app_ids
        ]
        run = {
            "run_id": run_id,
            "status": "running",
            "started_at": utc_now_iso(),
            "ended_at": None,
            "window_start": window_start,
            "window_end": window_end,
            "group_size": group_size,
            "group_index": group_index,
            "items": items,
        }
        data.setdefault("runs", []).append(run)
        self._write(data)

    def mark_item_running(self, run_id: str, item_id: str) -> None:
        self._update_item(
            run_id,
            item_id,
            {
                "status": "running",
                "started_at": utc_now_iso(),
            },
        )

    def mark_item_completed(
        self,
        run_id: str,
        item_id: str,
        policy_runs: int,
        breach_count: int,
        next_batch_run_utc: str,
    ) -> None:
        self._update_item(
            run_id,
            item_id,
            {
                "status": "completed",
                "ended_at": utc_now_iso(),
                "policy_runs": policy_runs,
                "breach_count": breach_count,
                "next_batch_run_utc": next_batch_run_utc,
            },
        )

    def mark_item_failed(self, run_id: str, item_id: str, error: str, traceback: str) -> None:
        self._update_item(
            run_id,
            item_id,
            {
                "status": "failed",
                "ended_at": utc_now_iso(),
                "error": error,
                "traceback": traceback,
            },
        )

    def append_item_log(self, run_id: str, item_id: str, level: str, message: str) -> None:
        data = self._read()
        item = self._find_item(data, run_id, item_id)
        if item is None:
            return
        item.setdefault("logs", []).append(
            {
                "timestamp": utc_now_iso(),
                "level": level,
                "message": message,
            }
        )
        self._write(data)

    def finalize_run(self, run_id: str) -> None:
        data = self._read()
        run = self._find_run(data, run_id)
        if run is None:
            return

        statuses = [item.get("status") for item in run.get("items", [])]
        if statuses and all(status == "completed" for status in statuses):
            run["status"] = "completed"
        elif any(status == "failed" for status in statuses):
            run["status"] = "partial_failed" if any(status == "completed" for status in statuses) else "failed"
        else:
            run["status"] = "running"

        run["ended_at"] = utc_now_iso()
        self._write(data)

    def _update_item(self, run_id: str, item_id: str, updates: Dict[str, Any]) -> None:
        data = self._read()
        item = self._find_item(data, run_id, item_id)
        if item is None:
            return
        item.update(updates)
        self._write(data)

    def _find_run(self, data: Dict[str, Any], run_id: str) -> Optional[Dict[str, Any]]:
        for run in data.get("runs", []):
            if run.get("run_id") == run_id:
                return run
        return None

    def _find_item(self, data: Dict[str, Any], run_id: str, item_id: str) -> Optional[Dict[str, Any]]:
        run = self._find_run(data, run_id)
        if run is None:
            return None
        for item in run.get("items", []):
            if item.get("item_id") == item_id:
                return item
        return None

    def _read(self) -> Dict[str, Any]:
        return json.loads(self.path.read_text())

    def _write(self, payload: Dict[str, Any]) -> None:
        self.path.write_text(json.dumps(payload, indent=2))
