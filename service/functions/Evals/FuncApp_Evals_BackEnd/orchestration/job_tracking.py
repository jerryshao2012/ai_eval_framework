from __future__ import annotations

import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class SqliteJobStatusStore:
    def __init__(self, path: str | Path) -> None:
        import sqlite3
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        # timeout=10.0 enables waiting for locks during concurrent access
        # isolation_level=None enables autocommit mode
        self.conn = sqlite3.connect(
            str(self.path),
            timeout=10.0,
            isolation_level=None,
            check_same_thread=False,
        )
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self) -> None:
        with self._lock:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    status TEXT,
                    started_at TEXT,
                    ended_at TEXT,
                    window_start TEXT,
                    window_end TEXT,
                    group_size INTEGER,
                    group_index INTEGER
                )
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS items (
                    run_id TEXT,
                    item_id TEXT,
                    status TEXT,
                    started_at TEXT,
                    ended_at TEXT,
                    policy_runs INTEGER DEFAULT 0,
                    breach_count INTEGER DEFAULT 0,
                    next_batch_run_utc TEXT,
                    error TEXT,
                    traceback TEXT,
                    PRIMARY KEY (run_id, item_id),
                    FOREIGN KEY (run_id) REFERENCES runs (run_id)
                )
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT,
                    item_id TEXT,
                    timestamp TEXT,
                    level TEXT,
                    message TEXT,
                    FOREIGN KEY (run_id, item_id) REFERENCES items (run_id, item_id)
                )
            """)
            # Pragmas for better concurrency/performance.
            self.conn.execute("PRAGMA journal_mode=WAL;")
            self.conn.execute("PRAGMA synchronous=NORMAL;")
            self.conn.execute("PRAGMA busy_timeout=10000;")

    def load_runs(self) -> List[Dict[str, Any]]:
        with self._lock:
            runs = []
            cur = self.conn.cursor()
            cur.execute("SELECT * FROM runs")
            for run_row in cur.fetchall():
                run_dict = dict(run_row)

                # Fetch matching items
                cur.execute("SELECT * FROM items WHERE run_id = ?", (run_dict["run_id"],))
                items = []
                for item_row in cur.fetchall():
                    item_dict = dict(item_row)

                    # Fetch matching logs
                    cur.execute(
                        "SELECT timestamp, level, message FROM logs "
                        "WHERE run_id = ? AND item_id = ? ORDER BY id ASC",
                        (run_dict["run_id"], item_dict["item_id"]),
                    )
                    item_dict["logs"] = [dict(log_row) for log_row in cur.fetchall()]
                    items.append(item_dict)

                run_dict["items"] = items
                runs.append(run_dict)
            return runs

    def start_run(
        self,
        run_id: str,
        app_ids: List[str],
        window_start: str,
        window_end: str,
        group_size: int,
        group_index: int,
    ) -> None:
        with self._lock:
            started_at = utc_now_iso()
            self.conn.execute(
                """
                INSERT OR IGNORE INTO runs 
                (run_id, status, started_at, window_start, window_end, group_size, group_index)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (run_id, "running", started_at, window_start, window_end, group_size, group_index)
            )

            for app_id in app_ids:
                self.conn.execute(
                    """
                    INSERT OR IGNORE INTO items 
                    (run_id, item_id, status)
                    VALUES (?, ?, ?)
                    """,
                    (run_id, app_id, "pending")
                )

    def mark_item_running(self, run_id: str, item_id: str) -> None:
        with self._lock:
            self.conn.execute(
                "UPDATE items SET status = ?, started_at = ? WHERE run_id = ? AND item_id = ?",
                ("running", utc_now_iso(), run_id, item_id)
            )

    def mark_item_completed(
        self,
        run_id: str,
        item_id: str,
        policy_runs: int,
        breach_count: int,
        next_batch_run_utc: str,
    ) -> None:
        with self._lock:
            self.conn.execute(
                """
                UPDATE items 
                SET status = ?, ended_at = ?, policy_runs = ?, breach_count = ?, next_batch_run_utc = ? 
                WHERE run_id = ? AND item_id = ?
                """,
                ("completed", utc_now_iso(), policy_runs, breach_count, next_batch_run_utc, run_id, item_id)
            )

    def mark_item_failed(self, run_id: str, item_id: str, error: str, traceback: str) -> None:
        with self._lock:
            self.conn.execute(
                """
                UPDATE items 
                SET status = ?, ended_at = ?, error = ?, traceback = ? 
                WHERE run_id = ? AND item_id = ?
                """,
                ("failed", utc_now_iso(), error, traceback, run_id, item_id)
            )

    def append_item_log(self, run_id: str, item_id: str, level: str, message: str) -> None:
        with self._lock:
            self.conn.execute(
                """
                INSERT INTO logs (run_id, item_id, timestamp, level, message)
                VALUES (?, ?, ?, ?, ?)
                """,
                (run_id, item_id, utc_now_iso(), level, message)
            )

    def finalize_run(self, run_id: str) -> None:
        with self._lock:
            cur = self.conn.cursor()
            cur.execute("SELECT status FROM items WHERE run_id = ?", (run_id,))
            statuses = [row["status"] for row in cur.fetchall()]

            if not statuses:
                final_status = "completed"
            elif all(s == "completed" for s in statuses):
                final_status = "completed"
            elif any(s == "failed" for s in statuses):
                final_status = "partial_failed" if any(s == "completed" for s in statuses) else "failed"
            else:
                final_status = "running"

            self.conn.execute(
                "UPDATE runs SET status = ?, ended_at = ? WHERE run_id = ?",
                (final_status, utc_now_iso(), run_id)
            )

    def close(self) -> None:
        with self._lock:
            self.conn.close()
