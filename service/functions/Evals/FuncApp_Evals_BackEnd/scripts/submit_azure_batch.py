from __future__ import annotations

import argparse
import logging
import os
import shlex
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Sequence

# Ensure local project imports work when running as:
# `python scripts/submit_azure_batch.py`
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.loader import list_resolved_apps, load_config
from orchestration.batch_partition import total_groups

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BatchSubmitConfig:
    account_url: str
    account_name: str
    account_key: str
    pool_id: str
    job_id: str
    task_id_prefix: str
    group_size: int
    window_hours: int
    log_level: str
    config_path: str
    python_bin: str
    workspace_dir: str
    max_task_retries: int
    dry_run: bool


def _chunks(items: Sequence, size: int) -> Iterable[Sequence]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _require(value: str | None, flag_name: str) -> str:
    if not value:
        raise ValueError(f"{flag_name} is required.")
    return value


def build_task_command(
    workspace_dir: str,
    python_bin: str,
    config_path: str,
    window_hours: int,
    group_size: int,
    group_index: int,
    log_level: str,
) -> str:
    cmd = " ".join(
        [
            shlex.quote(python_bin),
            "main.py",
            "--config",
            shlex.quote(config_path),
            "--window-hours",
            str(window_hours),
            "--group-size",
            str(group_size),
            "--group-index",
            str(group_index),
            "--log-level",
            shlex.quote(log_level),
        ]
    )
    return f"/bin/bash -lc {shlex.quote(f'cd {workspace_dir} && {cmd}')}"


def parse_args() -> BatchSubmitConfig:
    parser = argparse.ArgumentParser(
        description="Submit one Azure Batch task per (group_index, group_size) shard."
    )
    parser.add_argument("--config", default="config/config.yaml", help="Path to framework config file.")
    parser.add_argument("--group-size", type=int, required=True, help="Applications per shard.")
    parser.add_argument("--window-hours", type=int, default=24, help="Telemetry lookback window passed to main.py.")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level passed to task main.py invocations.",
    )
    parser.add_argument("--batch-account-url", default=os.getenv("AZURE_BATCH_ACCOUNT_URL"))
    parser.add_argument("--batch-account-name", default=os.getenv("AZURE_BATCH_ACCOUNT_NAME"))
    parser.add_argument("--batch-account-key", default=os.getenv("AZURE_BATCH_ACCOUNT_KEY"))
    parser.add_argument("--pool-id", default=os.getenv("AZURE_BATCH_POOL_ID"))
    parser.add_argument("--job-id", required=True, help="Azure Batch job id.")
    parser.add_argument("--task-id-prefix", default="ai-eval", help="Task id prefix.")
    parser.add_argument("--python-bin", default="python3", help="Python executable available on Batch nodes.")
    parser.add_argument(
        "--workspace-dir",
        default=".",
        help="Path on Batch nodes where this repo is available.",
    )
    parser.add_argument(
        "--max-task-retries",
        type=int,
        default=2,
        help="Max retries per Batch task.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print planned tasks without submitting.")

    args = parser.parse_args()
    if args.group_size <= 0:
        raise ValueError("--group-size must be greater than 0.")
    if args.window_hours <= 0:
        raise ValueError("--window-hours must be greater than 0.")
    if args.max_task_retries < 0:
        raise ValueError("--max-task-retries must be >= 0.")

    if args.dry_run:
        account_url = args.batch_account_url or ""
        account_name = args.batch_account_name or ""
        account_key = args.batch_account_key or ""
        pool_id = args.pool_id or ""
    else:
        account_url = _require(args.batch_account_url, "--batch-account-url / AZURE_BATCH_ACCOUNT_URL")
        account_name = _require(args.batch_account_name, "--batch-account-name / AZURE_BATCH_ACCOUNT_NAME")
        account_key = _require(args.batch_account_key, "--batch-account-key / AZURE_BATCH_ACCOUNT_KEY")
        pool_id = _require(args.pool_id, "--pool-id / AZURE_BATCH_POOL_ID")

    return BatchSubmitConfig(
        account_url=account_url,
        account_name=account_name,
        account_key=account_key,
        pool_id=pool_id,
        job_id=args.job_id,
        task_id_prefix=args.task_id_prefix,
        group_size=args.group_size,
        window_hours=args.window_hours,
        log_level=args.log_level,
        config_path=args.config,
        python_bin=args.python_bin,
        workspace_dir=args.workspace_dir,
        max_task_retries=args.max_task_retries,
        dry_run=args.dry_run,
    )


def _build_batch_client(account_url: str, account_name: str, account_key: str):
    try:
        from azure.batch import BatchServiceClient
        from azure.batch.batch_auth import SharedKeyCredentials
    except ImportError as exc:
        raise RuntimeError(
            "azure-batch is not installed. Install requirements or run: pip install azure-batch"
        ) from exc

    credentials = SharedKeyCredentials(account_name, account_key)
    return BatchServiceClient(credentials, batch_url=account_url)


def _ensure_job(client, job_id: str, pool_id: str) -> None:
    from azure.batch.models import BatchErrorException, JobAddParameter, PoolInformation

    try:
        client.job.add(JobAddParameter(id=job_id, pool_info=PoolInformation(pool_id=pool_id)))
        logger.info("Created Batch job: %s (pool=%s)", job_id, pool_id)
    except BatchErrorException as exc:
        if getattr(getattr(exc, "error", None), "code", None) == "JobExists":
            logger.info("Batch job already exists: %s", job_id)
            return
        raise


def _submit_tasks(client, cfg: BatchSubmitConfig, shard_count: int) -> int:
    from azure.batch.models import AddTaskCollectionParameter, TaskAddParameter, TaskConstraints

    tasks: List[TaskAddParameter] = []
    for group_index in range(shard_count):
        task_id = f"{cfg.task_id_prefix}-g{group_index:05d}"
        cmd = build_task_command(
            workspace_dir=cfg.workspace_dir,
            python_bin=cfg.python_bin,
            config_path=cfg.config_path,
            window_hours=cfg.window_hours,
            group_size=cfg.group_size,
            group_index=group_index,
            log_level=cfg.log_level,
        )
        tasks.append(
            TaskAddParameter(
                id=task_id,
                command_line=cmd,
                constraints=TaskConstraints(max_task_retry_count=cfg.max_task_retries),
            )
        )

    submitted = 0
    for batch in _chunks(tasks, 100):
        client.task.add_collection(cfg.job_id, AddTaskCollectionParameter(value=list(batch)))
        submitted += len(batch)
        logger.info("Submitted %d/%d tasks", submitted, len(tasks))
    return submitted


def main() -> int:
    cfg = parse_args()
    logging.basicConfig(level=getattr(logging, cfg.log_level), format="%(asctime)s [%(levelname)s] %(message)s")

    root = load_config(cfg.config_path)
    app_count = len(list_resolved_apps(root))
    shard_count = total_groups(app_count, cfg.group_size)

    logger.info(
        "Planned shard submission: apps=%d group_size=%d shard_count=%d job_id=%s",
        app_count,
        cfg.group_size,
        shard_count,
        cfg.job_id,
    )
    if shard_count == 0:
        logger.warning("No configured applications found. Nothing to submit.")
        return 0

    if cfg.dry_run:
        for group_index in range(shard_count):
            task_id = f"{cfg.task_id_prefix}-g{group_index:05d}"
            cmd = build_task_command(
                workspace_dir=cfg.workspace_dir,
                python_bin=cfg.python_bin,
                config_path=cfg.config_path,
                window_hours=cfg.window_hours,
                group_size=cfg.group_size,
                group_index=group_index,
                log_level=cfg.log_level,
            )
            logger.info("[DRY RUN] task_id=%s command=%s", task_id, cmd)
        return 0

    client = _build_batch_client(cfg.account_url, cfg.account_name, cfg.account_key)
    _ensure_job(client, cfg.job_id, cfg.pool_id)
    submitted = _submit_tasks(client, cfg, shard_count)
    logger.info("Batch submission complete. job_id=%s tasks_submitted=%d", cfg.job_id, submitted)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
