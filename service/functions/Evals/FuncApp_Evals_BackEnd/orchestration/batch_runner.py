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
    def __init__(
        self,
        telemetry_repo: TelemetryRepository,
        evaluation_repo: EvaluationRepository,
        policy_concurrency: int = 10,
    ) -> None:
        self.telemetry_repo = telemetry_repo
        self.evaluation_repo = evaluation_repo
        self.policy_registry = build_policy_registry()
        self._policy_concurrency = max(1, int(policy_concurrency))

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
        
        all_results = []
        total_records = 0
        policy_sem = asyncio.Semaphore(self._policy_concurrency)
        handled_chunks = False

        async for chunk in self.telemetry_repo.fetch_telemetry(app_cfg.app_id, start, end):
            if not chunk:
                continue
            
            handled_chunks = True
            total_records += len(chunk)
            dedupe_trace_id = self._derive_trace_identity(chunk, start, end)
            
            # Pre-calculate all expected result IDs to bulk check for duplicates
            expected_ids = []
            for policy_name in app_cfg.policy_names:
                policy_cfg = next((p for p in app_cfg.policies if p.name == policy_name), None)
                if policy_cfg:
                    policy_version = str(policy_cfg.parameters.get("version", "1.0"))
                    rid = self._stable_batch_result_id(
                        app_cfg.app_id, policy_name, dedupe_trace_id, policy_version
                    )
                    expected_ids.append(rid)

            existing_ids = set()
            if expected_ids:
                existing_list = await self.evaluation_repo.results_exist(expected_ids)
                existing_ids = set(existing_list)
                if existing_ids:
                    logger.info(
                        "Skipping %d/%d previously evaluated policies for app_id=%s chunk",
                        len(existing_ids),
                        len(expected_ids),
                        app_cfg.app_id,
                    )

            async def _run_policy(policy_name: str, current_chunk: List[TelemetryRecord] = chunk, current_dedupe: str = dedupe_trace_id, current_existing: set[str] = existing_ids) -> EvaluationResult | None:
                async with policy_sem:
                    return await self._evaluate_policy(
                        policy_name,
                        app_cfg,
                        current_chunk,
                        start,
                        end,
                        current_dedupe,
                        current_existing,
                    )

            tasks = [_run_policy(policy_name) for policy_name in app_cfg.policy_names]
            chunk_results = await asyncio.gather(*tasks)
            all_results.extend([result for result in chunk_results if result is not None])

        if not handled_chunks:
            # If no telemetry was found, evaluate once with empty list to produce zero metrics
            dedupe_trace_id = self._derive_trace_identity([], start, end)
            async def _run_empty_policy(policy_name: str) -> EvaluationResult | None:
                async with policy_sem:
                    return await self._evaluate_policy(
                        policy_name,
                        app_cfg,
                        [],
                        start,
                        end,
                        dedupe_trace_id,
                        set(),
                    )
            tasks = [_run_empty_policy(policy_name) for policy_name in app_cfg.policy_names]
            empty_results = await asyncio.gather(*tasks)
            all_results.extend([result for result in empty_results if result is not None])

        logger.debug("Fetched %d telemetry records total for app_id=%s", total_records, app_cfg.app_id)

        if all_results:
            await self.evaluation_repo.save_results(all_results)
            logger.info(
                "Saved %d evaluation results for app_id=%s (breaches=%d)",
                len(all_results),
                app_cfg.app_id,
                sum(len(r.breaches) for r in all_results),
            )

        return all_results

    async def _evaluate_policy(
        self,
        policy_name: str,
        app_cfg: ResolvedAppConfig,
        records: List[TelemetryRecord],
        window_start: str,
        window_end: str,
        dedupe_trace_id: str,
        existing_result_ids: set[str],
    ) -> EvaluationResult | None:
        if policy_name not in self.policy_registry:
            raise KeyError(f"Policy not registered: {policy_name}")

        policy_cfg = next((p for p in app_cfg.policies if p.name == policy_name), None)
        if policy_cfg is None:
            raise KeyError(f"Policy config missing for: {policy_name}")

        policy_version = str(policy_cfg.parameters.get("version", "1.0"))
        result_id = self._stable_batch_result_id(
            app_id=app_cfg.app_id,
            policy_name=policy_name,
            trace_id=dedupe_trace_id,
            value_object_version=policy_version,
        )
        if result_id in existing_result_ids:
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
