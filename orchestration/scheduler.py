from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from croniter import croniter

from config.models import ResolvedAppConfig


class CronScheduler:
    def __init__(self) -> None:
        self._last_run: Dict[str, datetime] = {}

    def due_apps(self, apps: List[ResolvedAppConfig], now: Optional[datetime] = None) -> List[ResolvedAppConfig]:
        current = now or datetime.now(timezone.utc)
        due: List[ResolvedAppConfig] = []

        for app in apps:
            last_run = self._last_run.get(app.app_id)
            if last_run is None:
                due.append(app)
                continue

            base = current - timedelta(minutes=1)
            prev_tick = croniter(app.batch_time, base).get_prev(datetime)
            if prev_tick <= current and last_run < prev_tick:
                due.append(app)

        return due

    def mark_run(self, app_id: str, when: Optional[datetime] = None) -> None:
        self._last_run[app_id] = when or datetime.now(timezone.utc)

    def next_run_time(self, app: ResolvedAppConfig, now: Optional[datetime] = None) -> datetime:
        current = now or datetime.now(timezone.utc)
        return croniter(app.batch_time, current).get_next(datetime)
