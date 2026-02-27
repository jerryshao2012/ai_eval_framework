from __future__ import annotations

import asyncio
import json
import logging
import smtplib
import ssl
import time
from dataclasses import dataclass
from email.message import EmailMessage
from typing import List, Dict, Any, Optional
from urllib.request import Request, urlopen

from config.models import AlertingConfig
from data.models import ThresholdBreach

logger = logging.getLogger(__name__)

_LEVEL_ORDER = {"warning": 1, "critical": 2}


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 3, recovery_timeout: float = 60.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failures = 0
        self.last_failure_time = 0.0
        self.state = "CLOSED"  # "CLOSED", "OPEN", "HALF_OPEN"
        self._lock = asyncio.Lock()

    async def call(self, func, *args, **kwargs) -> Any:
        async with self._lock:
            if self.state == "OPEN":
                if time.time() - self.last_failure_time > self.recovery_timeout:
                    self.state = "HALF_OPEN"
                else:
                    raise RuntimeError("Circuit breaker is OPEN. Fast-failing downstream request.")

        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                # Any successful call resets accumulated failures.
                self.state = "CLOSED"
                self.failures = 0
            return result
        except Exception as e:
            async with self._lock:
                if self.state == "HALF_OPEN":
                    # Probe failed: reopen immediately.
                    self.failures = self.failure_threshold
                else:
                    self.failures += 1
                self.last_failure_time = time.time()
                if self.failures >= self.failure_threshold:
                    self.state = "OPEN"
            raise e


@dataclass
class AlertItem:
    config: AlertingConfig
    app_id: str
    window_start: str
    window_end: str
    breaches: List[ThresholdBreach]


_alert_queue: asyncio.Queue[AlertItem] = asyncio.Queue()
_email_breaker = CircuitBreaker()
_teams_breaker = CircuitBreaker()


def filter_breaches_by_min_level(breaches: List[ThresholdBreach], min_level: str) -> List[ThresholdBreach]:
    threshold = _LEVEL_ORDER.get(min_level.lower(), 1)
    return [b for b in breaches if _LEVEL_ORDER.get(b.level.lower(), 1) >= threshold]


async def enqueue_alert(
    config: AlertingConfig,
    app_id: str,
    window_start: str,
    window_end: str,
    breaches: List[ThresholdBreach],
) -> None:
    if not config.enabled:
        return
    if not (config.email.enabled or config.teams.enabled):
        return

    selected = filter_breaches_by_min_level(breaches, config.min_level)
    if not selected:
        return

    await _alert_queue.put(
        AlertItem(
            config=config,
            app_id=app_id,
            window_start=window_start,
            window_end=window_end,
            breaches=selected,
        )
    )


async def drain_alert_queue(timeout_seconds: float = 15.0) -> None:
    await asyncio.wait_for(_alert_queue.join(), timeout=timeout_seconds)


async def alert_worker(
    batch_window_seconds: float = 5.0,
    stop_event: Optional[asyncio.Event] = None,
    email_breaker: Optional[CircuitBreaker] = None,
    teams_breaker: Optional[CircuitBreaker] = None,
) -> None:
    logger.info("Starting async alert worker task.")
    email_breaker = email_breaker or _email_breaker
    teams_breaker = teams_breaker or _teams_breaker
    while True:
        try:
            if stop_event and stop_event.is_set() and _alert_queue.empty():
                logger.info("Alert worker stop requested and queue drained; exiting.")
                break

            # Wait for at least one item
            try:
                first_item = await asyncio.wait_for(_alert_queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            batch: List[AlertItem] = [first_item]

            # Sleep briefly to batch subsequent rapid alerts
            await asyncio.sleep(batch_window_seconds)

            # Drain queue synchronously into this batch
            while not _alert_queue.empty():
                try:
                    batch.append(_alert_queue.get_nowait())
                except asyncio.QueueEmpty:
                    break

            # Group alerts by App ID so we can send aggregated notifications
            grouped: Dict[str, AlertItem] = {}
            for item in batch:
                if item.app_id not in grouped:
                    grouped[item.app_id] = AlertItem(
                        config=item.config,
                        app_id=item.app_id,
                        window_start=item.window_start,
                        window_end=item.window_end,
                        breaches=list(item.breaches)
                    )
                else:
                    # Update window to cover both, and extend breaches
                    existing = grouped[item.app_id]
                    existing.window_start = min(existing.window_start, item.window_start)
                    existing.window_end = max(existing.window_end, item.window_end)
                    existing.breaches.extend(item.breaches)

            try:
                for app_id, agg_item in grouped.items():
                    subject = f"[AI Eval] {app_id} threshold alert ({len(agg_item.breaches)} breach(es))"
                    body = _build_body(
                        app_id=agg_item.app_id,
                        window_start=agg_item.window_start,
                        window_end=agg_item.window_end,
                        breaches=agg_item.breaches
                    )

                    if agg_item.config.email.enabled:
                        try:
                            await email_breaker.call(asyncio.to_thread, _send_email, agg_item.config, subject, body)
                        except Exception as e:
                            logger.error(f"Failed to send email alert for {app_id}: {e}")

                    if agg_item.config.teams.enabled:
                        try:
                            await teams_breaker.call(asyncio.to_thread, _send_teams, agg_item.config, subject, body)
                        except Exception as e:
                            logger.error(f"Failed to send Teams alert for {app_id}: {e}")
            finally:
                # Ensure queue bookkeeping is always advanced, even on send errors.
                for _ in batch:
                    _alert_queue.task_done()

        except asyncio.CancelledError:
            logger.info("Alert worker cancelled.")
            break
        except Exception as e:
            logger.exception("Unexpected error in alert_worker: %s", e)


def _build_body(app_id: str, window_start: str, window_end: str, breaches: List[ThresholdBreach]) -> str:
    lines = [
        f"Application: {app_id}",
        f"Window: {window_start} .. {window_end}",
        f"Breaches: {len(breaches)}",
        "",
    ]
    for breach in breaches:
        lines.append(
            f"- metric={breach.metric_name} level={breach.level} "
            f"actual={breach.actual_value} threshold={breach.threshold_value} direction={breach.direction}"
        )
    return "\n".join(lines)


def _send_email(config: AlertingConfig, subject: str, body: str) -> None:
    email_cfg = config.email
    if not email_cfg.smtp_host or not email_cfg.from_address or not email_cfg.to_addresses:
        logger.warning("Email alert channel is enabled but required SMTP/email fields are missing.")
        return

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = email_cfg.from_address
    message["To"] = ", ".join(email_cfg.to_addresses)
    message.set_content(body)

    if email_cfg.use_tls:
        with smtplib.SMTP(email_cfg.smtp_host, email_cfg.smtp_port, timeout=15) as smtp:
            smtp.starttls(context=ssl.create_default_context())
            if email_cfg.username:
                smtp.login(email_cfg.username, email_cfg.password)
            smtp.send_message(message)
    else:
        with smtplib.SMTP(email_cfg.smtp_host, email_cfg.smtp_port, timeout=15) as smtp:
            if email_cfg.username:
                smtp.login(email_cfg.username, email_cfg.password)
            smtp.send_message(message)


def _send_teams(config: AlertingConfig, subject: str, body: str) -> None:
    teams_cfg = config.teams
    if not teams_cfg.webhook_url:
        logger.warning("Teams alert channel is enabled but webhook_url is missing.")
        return

    payload = {
        "text": f"{subject}\n\n{body}",
    }
    data = json.dumps(payload).encode("utf-8")
    req = Request(
        teams_cfg.webhook_url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(req, timeout=15) as resp:
        if resp.status >= 300:
            raise RuntimeError(f"Teams webhook returned HTTP {resp.status}")
