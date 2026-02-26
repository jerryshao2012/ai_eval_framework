from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from uuid import uuid4

from config.models import ResolvedAppConfig
from data.models import EvaluationResult, MetricValueVersioned, TelemetryRecord
from data.repositories import EvaluationRepository, TelemetryRepository
from evaluation.policies import EvaluationPolicy, build_policy_registry

logger = logging.getLogger(__name__)


class BatchEvaluationRunner:
    def __init__(self, telemetry_repo: TelemetryRepository, evaluation_repo: EvaluationRepository) -> None:
        self.telemetry_repo = telemetry_repo
        self.evaluation_repo = evaluation_repo
        self.policy_registry = build_policy_registry()

    async def run_for_application(
        self,
        app_cfg: ResolvedAppConfig,
        start_ts: Optional[str] = None,
        end_ts: Optional[str] = None,
    ) -> List[EvaluationResult]:
        now = datetime.now(timezone.utc)
        start = start_ts or (now - timedelta(hours=24)).isoformat()
        end = end_ts or now.isoformat()

        logger.debug(
            "Fetching telemetry for app_id=%s window=%s..%s",
            app_cfg.app_id,
            start,
            end,
        )
        records = await self.telemetry_repo.fetch_telemetry(app_cfg.app_id, start, end)
        logger.debug("Fetched %d telemetry records for app_id=%s", len(records), app_cfg.app_id)

        tasks = [
            self._evaluate_policy(policy_name, app_cfg, records, start, end)
            for policy_name in app_cfg.policy_names
        ]
        results = await asyncio.gather(*tasks)

        for result in results:
            await self.evaluation_repo.save_result(result)
            logger.info(
                "Saved evaluation result: app_id=%s policy=%s metrics=%d breaches=%d",
                result.app_id,
                result.policy_name,
                len(result.metrics),
                len(result.breaches),
            )

        return list(results)

    async def _evaluate_policy(
        self,
        policy_name: str,
        app_cfg: ResolvedAppConfig,
        records: List[TelemetryRecord],
        window_start: str,
        window_end: str,
    ) -> EvaluationResult:
        if policy_name not in self.policy_registry:
            raise KeyError(f"Policy not registered: {policy_name}")

        policy_cfg = next((p for p in app_cfg.policies if p.name == policy_name), None)
        if policy_cfg is None:
            raise KeyError(f"Policy config missing for: {policy_name}")

        policy_type = self.policy_registry[policy_name]
        policy: EvaluationPolicy = policy_type(policy_cfg)

        metrics = await policy.evaluate(app_cfg.app_id, records)
        metrics = self._normalize_metrics_for_traceability(
            metrics=metrics,
            app_id=app_cfg.app_id,
            policy_name=policy_name,
            policy_version=str(policy_cfg.parameters.get("version", "1.0")),
            window_start=window_start,
            window_end=window_end,
        )

        return EvaluationResult(
            id=f"{app_cfg.app_id}:{policy_name}:{uuid4().hex}",
            app_id=app_cfg.app_id,
            timestamp=datetime.now(timezone.utc).isoformat(),
            policy_name=policy_name,
            metrics=metrics,
            breaches=[],
        )

    def _normalize_metrics_for_traceability(
        self,
        metrics: List[MetricValueVersioned],
        app_id: str,
        policy_name: str,
        policy_version: str,
        window_start: str,
        window_end: str,
    ) -> List[MetricValueVersioned]:
        normalized: List[MetricValueVersioned] = []
        for metric in metrics:
            if not metric.version:
                metric.version = policy_version
            if not metric.timestamp:
                metric.timestamp = datetime.now(timezone.utc).isoformat()
            if not metric.metric_type:
                metric.metric_type = metric.metric_name
            metric.metadata = {
                **metric.metadata,
                "app_id": app_id,
                "policy_name": policy_name,
                "policy_version": policy_version,
                "value_object_type": "metric_value_versioned",
                "value_object_version": metric.version,
                "window_start": window_start,
                "window_end": window_end,
            }
            normalized.append(metric)
        return normalized
