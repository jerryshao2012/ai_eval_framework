# AI Evaluation Framework (Python + Azure Cosmos DB)

A modular Python framework for continuous monitoring of AI systems across multiple applications, triggered by configurable batch jobs and backed by Azure Cosmos DB.

## Overview

This project provides:
- Configurable batch-triggered monitoring per application (with global fallback schedule).
- Asynchronous policy execution (`asyncio`) for concurrent metric computation.
- Cosmos DB storage for telemetry-derived evaluation data and evaluation results.
- Versioned metric objects for traceability and replay.
- Configurable warning/critical thresholds per metric.
- A sample dashboard (Flask + Chart.js) for latest status, trends, and alerts.
- Azure deployment recommendations for scheduling and event streaming.

## Why Cosmos DB SQL API

This implementation uses **Cosmos DB SQL API** via `azure-cosmos`.

Rationale:
- Native first-party Python SDK support.
- Flexible JSON document model for telemetry and evaluation results.
- Straightforward query model for app/time-window and policy-based retrieval.
- Good fit for mixed workload patterns (batch writes, dashboard reads, alert scans).

## High-Level Architecture

```text
Telemetry Producers (apps/services)
            |
            v
     Azure Event Hubs (optional)
            |
            v
Telemetry ingestion (Function/worker)
            |
            v
Cosmos DB: telemetry container
            |
            v
Batch trigger (cron/timer)
            |
            v
Batch runner (per app)
    |                 \
    |                  +--> Async policy tasks (accuracy/latency/drift/...)
    |                                  |
    |                                  v
    +--------------------------> Cosmos DB: evaluation_results (metrics only)
                                       |
                                       v
                        Dashboard/API threshold evaluator (on read)
                                       |
                                       v
                           Alerts and status visualization
```

## Project Structure

```text
ai_eval_framework/
├── config/                # Configuration loading and schemas
├── data/                  # Cosmos client, repositories, document models
├── evaluation/            # Metric policies and threshold evaluation
├── orchestration/         # Batch scheduling and async orchestration
├── dashboard/             # Sample Flask dashboard + static assets
├── tests/                 # Unit tests for critical components
├── requirements.txt
├── README.md
└── main.py                # Batch entry point
```

## Setup (Python 3.9+)

1. Create/activate a virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Set Cosmos DB credentials (or place them in config):

```bash
export COSMOS_ENDPOINT="https://<account>.documents.azure.com:443/"
export COSMOS_KEY="<primary-key>"
export COSMOS_DATABASE="ai-eval"
```

4. Configure applications, policies, and thresholds in `config/config.yaml`.

## Configuration Management

Configuration supports **root defaults** plus **application-specific overrides**.

### Root configuration (global)
- `default_batch_time`: fallback schedule when app-level schedule is missing.
- `evaluation_policies`: policy definitions and parameters.
- `default_evaluation_policies`: default policies for apps that do not define `evaluation_policies`.
  If omitted, all policy names defined in `evaluation_policies` are used.
- `global_thresholds`: default thresholds applied to all apps.
- `app_config`: app-specific overrides.
- `cosmos`: endpoint/key/database/container settings.
- `alerting`: optional email/Teams notification settings.

### Application configuration (override)
- `batch_time`: cron expression per app.
- `evaluation_policies`: policies to run (comma-separated string or list).
- `thresholds`: app-level metric threshold overrides.
- `metadata`: app identifiers such as `project_code`.

Effective behavior:
- If `app_config.<app>.batch_time` is missing, `default_batch_time` is used.
- If `app_config.<app>.evaluation_policies` is missing or empty, `default_evaluation_policies` is used.
- If app thresholds are missing, `global_thresholds` are used.
- If app thresholds overlap global thresholds for the same metric, app thresholds override global ones.
- If an `app_id` is requested but not present in `app_config`, the run still proceeds using root defaults.

Example:

```yaml
default_batch_time: "0 * * * *"
default_evaluation_policies: "accuracy,latency"

app_config:
  app1:
    batch_time: "0 2 * * *"            # daily at 2 AM
    evaluation_policies: "accuracy,latency"
  app2:
    batch_time: "0 */6 * * *"           # every 6 hours
  app3:                                  # no app-specific settings -> root defaults apply
    metadata:
      project_code: "PROJ-APP3"
```

Alerting example:

```yaml
alerting:
  enabled: true
  min_level: "warning"  # warning | critical
  email:
    enabled: true
    smtp_host: "${ALERT_SMTP_HOST}"
    smtp_port: 587
    username: "${ALERT_SMTP_USER}"
    password: "${ALERT_SMTP_PASS}"
    from_address: "${ALERT_FROM_EMAIL}"
    to_addresses: "${ALERT_TO_EMAILS}"  # comma-separated or list
    use_tls: true
  teams:
    enabled: true
    webhook_url: "${ALERT_TEAMS_WEBHOOK_URL}"
```

## Continuous Monitoring Trigger (Batch Jobs)

Monitoring is triggered by batch runs from `main.py`:
- Resolve all applications from config.
- Select one app (`--app-id`) or all apps.
- Compute evaluation window (`--window-hours`).
- Orchestrate asynchronous policy execution per application.
- Persist raw evaluation results (metrics only) to Cosmos DB.
- Log the next scheduled batch launch time per app (`next_batch_run_utc`).
- Optionally shard the app list by fixed-size groups for horizontal VM scaling.
- Optionally send notifications through email and/or Microsoft Teams based on configured thresholds.

Manual run examples:

```bash
python3 main.py --config config/config.yaml --window-hours 24
python3 main.py --config config/config.yaml --app-id app1 --window-hours 6
python3 main.py --config config/config.yaml --log-level DEBUG
```

### Batch Sharding Across VMs (Group by Size)

Use these flags to execute a deterministic subset of apps:
- `--group-size`: number of apps per group.
- `--group-index`: zero-based group index handled by this VM.

Example for 3 VMs, group size 100:

```bash
# VM 0
python3 main.py --config config/config.yaml --group-size 100 --group-index 0

# VM 1
python3 main.py --config config/config.yaml --group-size 100 --group-index 1

# VM 2
python3 main.py --config config/config.yaml --group-size 100 --group-index 2
```

Runtime logs include:
- selected group metadata (`group_index`, `total_groups`, `group_size`, `apps_in_group`)
- next scheduled run per app (`next_batch_run_utc`)

### Azure Batch Task Submission Script

Use `scripts/submit_azure_batch.py` to submit one Azure Batch task per shard.

Required inputs:
- Batch account URL/name/key
- Batch pool id
- Batch job id
- shard size (`--group-size`)

Example:

```bash
python3 scripts/submit_azure_batch.py \
  --config config/config.yaml \
  --group-size 100 \
  --job-id ai-eval-20260225-1200 \
  --batch-account-url https://<account>.<region>.batch.azure.com \
  --batch-account-name <account-name> \
  --batch-account-key <account-key> \
  --pool-id <pool-id>
```

Dry-run (prints tasks/commands without submitting):

```bash
python3 scripts/submit_azure_batch.py \
  --config config/config.yaml \
  --group-size 100 \
  --job-id ai-eval-dry-run \
  --batch-account-url https://<account>.<region>.batch.azure.com \
  --batch-account-name <account-name> \
  --batch-account-key <account-key> \
  --pool-id <pool-id> \
  --dry-run
```

## Asynchronous Evaluation Tasks

`orchestration/batch_runner.py` uses `asyncio.gather(...)` to run multiple policy evaluations concurrently for each app.

Task flow:
1. Fetch telemetry records for `(app_id, time window)`.
2. Launch async policy evaluations in parallel.
3. Save `EvaluationResult` metric documents to Cosmos DB.
4. Evaluate thresholds in-memory for notifications only (when alerting is enabled).

This design is extensible to Celery, Durable Functions, or Azure Batch for larger workloads.

## Cosmos DB Data Model

### Telemetry data (`type = telemetry`)
Required fields for effective monitoring:
- `id`
- `app_id` (or mapped from app catalog ID/project code)
- `timestamp` (ISO-8601 UTC)
- `model_id`
- `model_version`
- `input_text`
- `output_text`
- `expected_output` (needed for accuracy)
- `latency_ms`
- `user_id` (optional, preferably pseudonymized)
- `metadata` (slice tags, channel, locale, etc.)

Synthetic partition key:
- `pk = "<app_id>:<YYYY-MM-DD>"`

### Evaluation results (`type = evaluation_result`)
Each document includes:
- `id`, `app_id`, `timestamp`, `policy_name`, `pk`
- `metrics`: list of versioned metric objects
- `breaches`: list of threshold breaches

## Versioned Evaluation Metrics

Metrics are stored as value-versioned objects for traceability:

```json
{
  "metric_name": "accuracy",
  "value": 0.95,
  "version": "1.0",
  "timestamp": "2025-02-24T12:00:00Z",
  "metadata": {
    "model_version": "2.3",
    "data_slice": "2025-02-23"
  }
}
```

## Configurable Thresholds (Dashboard-Time Evaluation)

Thresholds are metric-scoped and can include multiple levels:
- `warning`
- `critical`

Supported directions:
- `min`: breach when value is **below** threshold.
- `max`: breach when value is **above** threshold.

Thresholds are evaluated when dashboard data is requested (not during batch persistence). This keeps batch outputs stable and allows runtime threshold tuning without re-running batch jobs.
For notifications, thresholds are evaluated at batch runtime in-memory; alerts are sent via configured channels, but breach records are still not persisted to Cosmos DB.

Dynamic thresholds are available on dashboard APIs when needed:
- enable with `dynamic_thresholds=1`
- override values with query params:
  - `threshold.<metric>.<level>=<value>`
  - optional `direction.<metric>.<level>=min|max`

Example:

```text
/api/latest?dynamic_thresholds=1&threshold.accuracy.warning=0.90&direction.accuracy.warning=min
```

## Built-in Evaluation Policies

| Policy | Metrics | Purpose |
|---|---|---|
| `accuracy` | `accuracy` | Exact-match quality check against expected outputs |
| `latency` | `latency_avg_ms`, `latency_p95_ms` | Responsiveness tracking |
| `drift` | `input_length_drift` | Input distribution shift proxy |
| `performance` | `performance_score` | Composite quality/latency score |

## How to Add a New Policy

1. Add policy config under `evaluation_policies` in `config/config.yaml`.
2. Implement a class extending `EvaluationPolicy` in `evaluation/policies.py`.
3. Register it in `build_policy_registry()`.
4. Add thresholds for each metric it emits.
5. Add/extend tests in `tests/`.

## Cosmos DB Query Examples

Latest results for one app:

```sql
SELECT TOP 20 c.app_id, c.timestamp, c.policy_name, c.metrics, c.breaches
FROM c
WHERE c.type = 'evaluation_result' AND c.app_id = @app_id
ORDER BY c.timestamp DESC
```

Telemetry in a time window:

```sql
SELECT * FROM c
WHERE c.type = 'telemetry'
  AND c.app_id = @app_id
  AND c.timestamp >= @start_ts
  AND c.timestamp < @end_ts
```

Critical threshold breaches:

```sql
SELECT c.app_id, c.timestamp, b
FROM c
JOIN b IN c.breaches
WHERE c.type = 'evaluation_result' AND b.level = 'critical'
```

## Sample Dashboard

The sample dashboard is in `dashboard/` and uses Flask endpoints with mock JSON data.

Run:

```bash
python3 dashboard/app.py
```

Open: `http://localhost:5000`

Dashboard capabilities:
- Applications list with latest evaluation status.
- Trend chart for selected application (accuracy, p95 latency, drift).
- Alert list for threshold breaches.
- Optional runtime threshold overrides without changing stored batch data.
- Batch execution monitoring: current run status, history, aggregate statistics, and failed item trace logs.

Key endpoints:
- `GET /api/latest`
- `GET /api/trends/<app_id>`
- `GET /api/alerts`
- `GET /api/thresholds`
- `GET /api/batch/current`
- `GET /api/batch/history`
- `GET /api/batch/run/<run_id>`
- `GET /api/batch/run/<run_id>/item/<item_id>/logs`
- `GET /api/openapi.json` (OpenAPI spec)
- `GET /api/docs` (Swagger UI)

## Testing

Run unit tests:

```bash
pytest tests -q
```

Covered critical paths:
- config parsing and app override resolution,
- threshold breach evaluation,
- async batch orchestration with in-memory repositories.

## Azure Batch Job & Event Streaming Recommendations

See `AZURE_RECOMMENDATIONS.md` for detailed options and tradeoffs.

Summary:
- Batch scheduling/orchestration options: Azure Functions (Timer Trigger), Durable Functions, AKS CronJobs, Azure Batch.
- Recommended evolution path: Timer Trigger entrypoint -> Durable fan-out/fan-in -> Azure Batch for heavy compute.
- Event streaming: ingest telemetry via Azure Event Hubs, enrich/validate in worker, persist to Cosmos DB, evaluate via batch runner.

For large/heavy batch computations, prefer Azure Batch with:
- one logical job per evaluation window,
- many independent idempotent tasks (partitioned by app/time group),
- autoscale pools based on queued tasks,
- retries and task-level checkpointing,
- outputs written to durable storage (Cosmos DB/Blob) for replay/resume.

## Notes on pip crash error

Use below command to fix pip crash error:
```shell
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python get-pip.py --force-reinstall
rm get-pip.py
```
