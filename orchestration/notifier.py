from __future__ import annotations

import json
import logging
import smtplib
import ssl
from email.message import EmailMessage
from typing import List
from urllib.request import Request, urlopen

from config.models import AlertingConfig
from data.models import ThresholdBreach

logger = logging.getLogger(__name__)

_LEVEL_ORDER = {"warning": 1, "critical": 2}


def filter_breaches_by_min_level(breaches: List[ThresholdBreach], min_level: str) -> List[ThresholdBreach]:
    threshold = _LEVEL_ORDER.get(min_level.lower(), 1)
    return [b for b in breaches if _LEVEL_ORDER.get(b.level.lower(), 1) >= threshold]


def send_alerts(
    config: AlertingConfig,
    app_id: str,
    window_start: str,
    window_end: str,
    breaches: List[ThresholdBreach],
) -> None:
    if not config.enabled:
        return

    selected = filter_breaches_by_min_level(breaches, config.min_level)
    if not selected:
        return

    subject = f"[AI Eval] {app_id} threshold alert ({len(selected)} breach(es))"
    body = _build_body(app_id=app_id, window_start=window_start, window_end=window_end, breaches=selected)

    if config.email.enabled:
        _send_email(config, subject, body)
    if config.teams.enabled:
        _send_teams(config, subject, body)


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
