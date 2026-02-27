from __future__ import annotations

import asyncio
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Optional

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
        results = [result for result in results if result is not None]

        if results:
            await self.evaluation_repo.save_results(results)
            logger.info(
                "Saved %d evaluation results for app_id=%s (breaches=%d)",
                len(results),
                app_cfg.app_id,
                sum(len(r.breaches) for r in results),
            )

        return list(results)

    async def _evaluate_policy(
        self,
        policy_name: str,
        app_cfg: ResolvedAppConfig,
        records: List[TelemetryRecord],
        window_start: str,
        window_end: str,
    ) -> EvaluationResult | None:
        if policy_name not in self.policy_registry:
            raise KeyError(f"Policy not registered: {policy_name}")

        policy_cfg = next((p for p in app_cfg.policies if p.name == policy_name), None)
        if policy_cfg is None:
            raise KeyError(f"Policy config missing for: {policy_name}")

        policy_version = str(policy_cfg.parameters.get("version", "1.0"))
        dedupe_trace_id = self._derive_trace_identity(records, window_start, window_end)
        result_id = self._stable_batch_result_id(
            app_id=app_cfg.app_id,
            policy_name=policy_name,
            trace_id=dedupe_trace_id,
            value_object_version=policy_version,
        )
        if await self.evaluation_repo.result_exists(result_id):
            logger.info(
                "Skipping duplicate evaluation: app_id=%s policy=%s trace_id=%s version=%s",
                app_cfg.app_id,
                policy_name,
                dedupe_trace_id,
                policy_version,
            )
            return None

        policy_type = self.policy_registry[policy_name]
        policy: EvaluationPolicy = policy_type(policy_cfg)

        metrics = await policy.evaluate(app_cfg.app_id, records)
        metrics = self._normalize_metrics_for_traceability(
            metrics=metrics,
            app_id=app_cfg.app_id,
            policy_name=policy_name,
            policy_version=policy_version,
            window_start=window_start,
            window_end=window_end,
            dedupe_trace_id=dedupe_trace_id,
        )

        return EvaluationResult(
            id=result_id,
            app_id=app_cfg.app_id,
            timestamp=datetime.now(timezone.utc).isoformat(),
            policy_name=policy_name,
            metrics=metrics,
            breaches=[],
        )

    def _stable_batch_result_id(
        self,
        app_id: str,
        policy_name: str,
        trace_id: str,
        value_object_version: str,
    ) -> str:
        fingerprint = f"{app_id}|{policy_name}|{trace_id}|{value_object_version}"
        digest = hashlib.sha1(fingerprint.encode("utf-8")).hexdigest()[:16]
        return f"{app_id}:{policy_name}:{trace_id}:{value_object_version}:{digest}"

    def _derive_trace_identity(
        self,
        records: List[TelemetryRecord],
        window_start: str,
        window_end: str,
    ) -> str:
        trace_ids = sorted(
            {
                str(record.metadata.get("trace_id"))
                for record in records
                if record.metadata.get("trace_id")
            }
        )
        if len(trace_ids) == 1:
            return trace_ids[0]
        if len(trace_ids) > 1:
            digest = hashlib.sha1("|".join(trace_ids).encode("utf-8")).hexdigest()[:16]
            return f"trace_set:{digest}"

        record_ids = sorted({record.id for record in records if record.id})
        if record_ids:
            digest = hashlib.sha1("|".join(record_ids).encode("utf-8")).hexdigest()[:16]
            return f"record_set:{digest}"

        digest = hashlib.sha1(f"{window_start}|{window_end}".encode("utf-8")).hexdigest()[:16]
        return f"window:{digest}"

    def _normalize_metrics_for_traceability(
        self,
        metrics: List[MetricValueVersioned],
        app_id: str,
        policy_name: str,
        policy_version: str,
        window_start: str,
        window_end: str,
        dedupe_trace_id: str,
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
                "dedupe_trace_id": dedupe_trace_id,
                "window_start": window_start,
                "window_end": window_end,
            }
            normalized.append(metric)
        return normalized
