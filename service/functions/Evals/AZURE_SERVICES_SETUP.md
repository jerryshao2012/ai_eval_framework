# Azure Cloud Services and OpenTelemetry Configuration Steps

This document lists the Azure services used (or recommended) by this AI Evals solution and gives concrete setup steps for each, including OpenTelemetry integration points.

## Service List

1. Azure Resource Group
2. Azure Cosmos DB (SQL API)
3. Azure Event Hubs
4. Azure Functions (Timer/API/Processor option)
5. Azure Batch
6. Azure Storage Account (for Function/Batch support)
7. Azure Monitor + Application Insights
8. Azure Key Vault
9. OpenTelemetry (OTLP) exporters/collector integration (implementation-level dependency)

---

## 1) Azure Resource Group

Purpose:
- Logical container for all Evals resources.

Steps:
1. Create a resource group in your target region.
2. Apply standard tags (`env`, `owner`, `cost_center`, `application`).
3. Define RBAC roles for app teams and ops teams.
4. Enable resource locks for production where required.

---

## 2) Azure Cosmos DB (SQL API)

Purpose:
- Store telemetry documents (`type=telemetry`) and evaluation results (`type=evaluation_result`).

Steps:
1. Create Cosmos DB account with SQL API.
2. Create database (for example `ai-eval`).
3. Create containers:
   - `telemetry`
   - `evaluation_results`
4. Set partition key path to `/pk` for both containers.
5. Configure throughput:
   - Start with autoscale RU/s for variable load.
6. Set network/security:
   - Private endpoint or selected network allowlist.
   - Restrict key access via Key Vault.
7. Capture credentials as secrets:
   - `COSMOS_ENDPOINT`
   - `COSMOS_KEY`
   - `COSMOS_DATABASE`

---

## 3) Azure Event Hubs

Purpose:
- Ingestion bus for telemetry events before validation/enrichment and Cosmos write.

Steps:
1. Create Event Hubs namespace.
2. Create Event Hub (for example `ai-eval-telemetry`).
3. Set partitions (for expected producer/consumer parallelism).
4. Set retention based on replay window requirements.
5. Create shared access policy for producer/consumer access.
6. Store these secrets/config:
   - `EVENTHUB_CONNECTION_STRING`
   - `EVENTHUB_NAME`
   - `EVENTHUB_CONSUMER_GROUP` (default `$Default` or dedicated group)

---

## 4) Azure Functions

Purpose:
- Recommended control plane for scheduled runs and optional stream processing/API hosting.

Steps:
1. Create Function App (Python runtime).
2. Connect to Storage Account and Application Insights.
3. Configure app settings:
   - Cosmos and Event Hubs secrets (prefer Key Vault references).
4. Implement/deploy one or more functions:
   - Timer Trigger for periodic batch invocation.
   - HTTP Trigger for telemetry ingestion endpoint (optional alternative to standalone Flask API).
   - Event Hub Trigger for telemetry processing (optional alternative to Event Processor client loop).
5. Set scale and timeout settings per workload profile.

---

## 5) Azure Batch

Purpose:
- Run large, heavy sharded eval jobs across many compute nodes.

Steps:
1. Create Batch account.
2. Create pool with VM image/runtime matching your backend.
3. Configure autoscale formula and VM bounds.
4. Upload/runtime-stage code dependencies.
5. Create Batch job per evaluation window.
6. Submit one task per shard using:
   - `FuncApp_Evals_BackEnd/scripts/submit_azure_batch.py`
7. Configure retries/checkpointing:
   - Task retries (`max_task_retry_count`)
   - Idempotent task behavior
8. Monitor queue depth, task failures, and execution latency.

---

## 6) Azure Storage Account

Purpose:
- Required backing for Function App and optional staging/checkpoint artifacts for Batch.

Steps:
1. Create Storage Account (General Purpose v2).
2. Create blob containers/queues as needed.
3. Use private networking and RBAC.
4. Configure lifecycle policies for log/artifact retention.

---

## 7) Azure Monitor + Application Insights

Purpose:
- Observability for ingestion, processing, and batch execution.

Steps:
1. Create/attach Application Insights to Function App and services.
2. Enable diagnostic settings on Cosmos, Event Hubs, and Batch.
3. Route logs/metrics to Log Analytics workspace.
4. Create alerts:
   - Event Hub consumer lag
   - Cosmos RU throttling
   - Batch task failure rate
   - API 5xx/error rate
5. Build dashboards for SLO metrics and incident triage.

---

## 8) Azure Key Vault

Purpose:
- Central secret management for Cosmos/Event Hubs/SMTP/Teams credentials.

Steps:
1. Create Key Vault.
2. Add secrets:
   - `COSMOS_ENDPOINT`, `COSMOS_KEY`, `COSMOS_DATABASE`
   - `EVENTHUB_CONNECTION_STRING`, `EVENTHUB_NAME`
   - Alerting credentials (`ALERT_*`)
3. Grant managed identity access for Function App/Batch nodes.
4. Use Key Vault references in app settings.

---

## 9) OpenTelemetry (OTLP) Integration

Purpose:
- Standardized telemetry export path for traces/events used by AI evaluation (`telemetry_source.type=otlp` and `POST /api/otlp/v1/traces`).

Steps:
1. Enable OpenTelemetry instrumentation in producer applications/services.
2. Configure OTLP exporter endpoint/protocol for your deployment model.
3. Ensure telemetry includes `app_id`, timestamps, model metadata, and trace identity (`trace_id`).
4. Validate OTLP payload compatibility with evaluator endpoint.
5. Confirm dedupe behavior:
   - deterministic evaluation ID uses `app_id + policy_name + trace_id + value_object_version`.

---

## Deployment Order (Recommended)

1. Resource Group
2. Storage Account + Key Vault
3. Cosmos DB
4. Event Hubs
5. Function App (or equivalent runtime host)
6. Batch Account/Pool (if needed for scale-out)
7. Monitoring/alerts

---

## Minimal Runtime Environment Variables

- `COSMOS_ENDPOINT`
- `COSMOS_KEY`
- `COSMOS_DATABASE`
- `EVENTHUB_CONNECTION_STRING`
- `EVENTHUB_NAME`
- `EVENTHUB_CONSUMER_GROUP` (optional)
- `OTLP_TELEMETRY_FILE_PATH` (when using batch `telemetry_source.type=otlp`)
- `ALERT_*` values (optional, for email/Teams alerts)
