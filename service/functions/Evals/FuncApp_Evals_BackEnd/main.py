from __future__ import annotations

import argparse
import asyncio
import logging
import traceback
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from uuid import uuid4

from config.loader import list_resolved_apps, load_config, resolve_app_config
from config.models import ResolvedAppConfig
from data.cosmos_client import CosmosDbClient
from data.otlp_repository import OtlpTelemetryRepository
from data.repositories import (
    CosmosEvaluationRepository,
    CosmosTelemetryRepository,
)
from evaluation.thresholds import evaluate_thresholds
from orchestration.batch_partition import select_group, total_groups
from orchestration.batch_runner import BatchEvaluationRunner
from orchestration.job_tracking import FileJobStatusStore
from orchestration.notifier import send_alerts
from orchestration.scheduler import CronScheduler

logger = logging.getLogger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run AI evaluation batch job")
    parser.add_argument(
        "--config",
        default="config/config.yaml",
        help="Path to YAML/JSON config",
    )
    parser.add_argument("--app-id", help="Run for a single application")
    parser.add_argument(
        "--window-hours",
        type=int,
        default=24,
        help="Telemetry lookback window in hours",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity",
    )
    parser.add_argument(
        "--group-size",
        type=int,
        default=0,
        help="Apps per batch group. Use with --group-index to shard runs across VMs.",
    )
    parser.add_argument(
        "--group-index",
        type=int,
        default=0,
        help="Zero-based group index to execute when --group-size is set.",
    )
    parser.add_argument(
        "--app-concurrency",
        type=int,
        help="Maximum number of applications evaluated concurrently.",
    )
    parser.add_argument(
        "--policy-concurrency",
        type=int,
        help="Maximum number of policies evaluated concurrently per application.",
    )
    return parser.parse_args()


async def run_batch(
    config_path: str,
    app_id: Optional[str],
    window_hours: int,
    group_size: int = 0,
    group_index: int = 0,
    app_concurrency: Optional[int] = None,
    policy_concurrency: Optional[int] = None,
) -> None:
    if group_size < 0:
        raise ValueError("--group-size must be >= 0")
    if group_index < 0:
        raise ValueError("--group-index must be >= 0")

    root_config = load_config(config_path)
    if root_config.cosmos is None:
        raise ValueError("Cosmos DB configuration is required to run batch jobs.")

    resolved_app_concurrency = (
        app_concurrency if app_concurrency is not None else root_config.batch_app_concurrency
    )
    resolved_policy_concurrency = (
        policy_concurrency
        if policy_concurrency is not None
        else root_config.batch_policy_concurrency
    )
    if resolved_app_concurrency <= 0:
        raise ValueError("App concurrency must be >= 1.")
    if resolved_policy_concurrency <= 0:
        raise ValueError("Policy concurrency must be >= 1.")

    cosmos = CosmosDbClient(root_config.cosmos)
    telemetry_source_type = root_config.telemetry_source.type
    if telemetry_source_type == "cosmos":
        telemetry_repo = CosmosTelemetryRepository(
            cosmos,
            page_size=root_config.cosmos_telemetry_page_size,
        )
    elif telemetry_source_type == "otlp":
        telemetry_repo = OtlpTelemetryRepository(
            root_config.telemetry_source.otlp_file_path,
            chunk_size=root_config.otlp_stream_chunk_size,
        )
    else:
        raise ValueError(f"Unsupported telemetry source type: {telemetry_source_type}")
    evaluation_repo = CosmosEvaluationRepository(cosmos)
    runner = BatchEvaluationRunner(
        telemetry_repo,
        evaluation_repo,
        policy_concurrency=resolved_policy_concurrency,
    )
    scheduler = CronScheduler()

    if app_id:
        target_apps: List[ResolvedAppConfig] = [resolve_app_config(root_config, app_id)]
        if group_size > 0:
            logger.info("--group-size ignored when --app-id is specified (single app mode).")
    else:
        all_apps = sorted(list_resolved_apps(root_config), key=lambda a: a.app_id)
        if group_size > 0:
            groups = total_groups(len(all_apps), group_size)
            target_apps = select_group(all_apps, group_size, group_index)
            logger.info(
                "Batch group selection: group_index=%d total_groups=%d group_size=%d apps_in_group=%d",
                group_index,
                groups,
                group_size,
                len(target_apps),
            )
        else:
            if group_index != 0:
                logger.info("--group-index ignored because --group-size is 0.")
            target_apps = all_apps

    if not target_apps:
        if app_id:
            logger.warning("No matching applications found for app_id=%s", app_id)
        else:
            logger.warning("No applications selected for this batch group.")
        return

    now = datetime.now(timezone.utc)
    start = (now - timedelta(hours=window_hours)).isoformat()
    end = now.isoformat()

    logger.info(
        "Starting batch run: apps=%d window=%s..%s app_concurrency=%d policy_concurrency=%d",
        len(target_apps),
        start,
        end,
        resolved_app_concurrency,
        resolved_policy_concurrency,
    )
    run_id = f"run-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:8]}"
    status_store = FileJobStatusStore(
        Path(__file__).resolve().parent.parent / "WebApp_Evals_FrontEnd" / "dashboard" / "batch_status.json"
    )
    status_store.start_run(
        run_id=run_id,
        app_ids=[app.app_id for app in target_apps],
        window_start=start,
        window_end=end,
        group_size=group_size,
        group_index=group_index,
    )

    async def _process_app(app: ResolvedAppConfig, sem: asyncio.Semaphore) -> None:
        async with sem:
            status_store.mark_item_running(run_id, app.app_id)
            status_store.append_item_log(run_id, app.app_id, "INFO", "Evaluation started.")
            try:
                results = await runner.run_for_application(app, start_ts=start, end_ts=end)
                notification_breaches = []
                for result in results:
                    notification_breaches.extend(evaluate_thresholds(result.metrics, app.thresholds))
                total_breaches = len(notification_breaches)
                next_run = scheduler.next_run_time(app, now=now)
                logger.info(
                    "app_id=%s policy_runs=%d breaches=%d window=%s..%s",
                    app.app_id,
                    len(results),
                    total_breaches,
                    start,
                    end,
                )
                status_store.append_item_log(
                    run_id,
                    app.app_id,
                    "INFO",
                    f"Evaluation completed: policy_runs={len(results)} breaches={total_breaches}",
                )
                try:
                    send_alerts(
                        config=root_config.alerting,
                        app_id=app.app_id,
                        window_start=start,
                        window_end=end,
                        breaches=notification_breaches,
                    )
                    status_store.append_item_log(run_id, app.app_id, "INFO", "Alert notification step completed.")
                except Exception:
                    logger.exception("Failed to send alerts for app_id=%s", app.app_id)
                    status_store.append_item_log(run_id, app.app_id, "ERROR", "Alert notification failed.")
                logger.info("app_id=%s next_batch_run_utc=%s", app.app_id, next_run.isoformat())
                status_store.mark_item_completed(
                    run_id=run_id,
                    item_id=app.app_id,
                    policy_runs=len(results),
                    breach_count=total_breaches,
                    next_batch_run_utc=next_run.isoformat(),
                )
            except Exception as exc:
                trace = traceback.format_exc()
                logger.exception("Batch item failed for app_id=%s", app.app_id)
                status_store.append_item_log(run_id, app.app_id, "ERROR", f"Evaluation failed: {exc}")
                status_store.mark_item_failed(
                    run_id=run_id,
                    item_id=app.app_id,
                    error=str(exc),
                    traceback=trace,
                )

    semaphore = asyncio.Semaphore(resolved_app_concurrency)
    await asyncio.gather(*[_process_app(app, semaphore) for app in target_apps])
    status_store.finalize_run(run_id)


if __name__ == "__main__":
    args = _parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    asyncio.run(
        run_batch(
            args.config,
            args.app_id,
            args.window_hours,
            group_size=args.group_size,
            group_index=args.group_index,
            app_concurrency=args.app_concurrency,
            policy_concurrency=args.policy_concurrency,
        )
    )
