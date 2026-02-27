# AI Evals Architecture Diagram (Azure + OpenTelemetry)

This diagram includes Azure services, OpenTelemetry integration paths, and implementation building blocks used by this repository.

```mermaid
flowchart LR
    A["Client Apps / AI Services"] --> B["Telemetry SDK Integration\n(telemetry.emit_telemetry_event)"]
    A --> C["Telemetry API\nPOST /api/telemetry"]
    C --> EH["Azure Event Hubs\n(ai-eval-telemetry)"]
    B --> EH

    EH --> P["Stream Processor\n(Event Processor Client / Azure Functions Trigger)\nvalidate + enrich events"]
    P --> CT["Azure Cosmos DB\nContainer: telemetry"]

    SCH["Scheduler\n(Azure Functions Timer / Cron / Durable Orchestrator)"] --> BR["Batch Runner\nmain.py + orchestration/batch_runner.py"]
    CT --> BR
    OTLPFILE["OTLP Trace File/Input\n(telemetry_source.type=otlp)"] --> BR
    BRD["Batch Dedupe Gate\n(app_id + policy + trace_id + value_object_version)\n(fallback: trace_set/record_set/window hash)"] --> BR
    BR --> CT
    BR --> ER["Azure Cosmos DB\nContainer: evaluation_results\n(metrics-only value objects)"]

    BR --> AL["Alerting Module\n(orchestration/notifier.py)"]
    AL --> EM["SMTP Email"]
    AL --> TM["Microsoft Teams Webhook"]

    ER --> DAPI["Dashboard Backend API\n(WebApp_Evals_FrontEnd/dashboard/app.py)"]
    CT --> DAPI
    DAPI --> DUI["Dashboard UI\n(Trends, Alerts, Batch Status, Trace Logs)"]

    BR --> JS["Batch Job Status Store\nbatch_status.json"]
    JS --> DAPI

    ORCH["Heavy Compute Orchestration\nscripts/submit_azure_batch.py"] --> AB["Azure Batch\nJob/Pool/Tasks (group shards)"]
    AB --> BR

    KV["Azure Key Vault\n(COSMOS_*, EVENTHUB_*, ALERT_*)"] --> C
    KV --> P
    KV --> BR
    KV --> AB

    MON["Azure Monitor + Application Insights\n+ Log Analytics"] -. telemetry/logs .-> C
    MON -. telemetry/logs .-> P
    MON -. telemetry/logs .-> BR
    MON -. telemetry/logs .-> AB
    MON -. telemetry/logs .-> EH
    MON -. telemetry/logs .-> ER
    MON -. telemetry/logs .-> CT

    SA["Azure Storage Account\n(Function runtime + optional staging/checkpoints)"] --> P
    SA --> AB
```

## Azure Services in This Diagram

- Azure Event Hubs
- Azure Cosmos DB (SQL API)
- Azure Functions (Timer Trigger and/or Event Hub Trigger option)
- Azure Batch
- Azure Key Vault
- Azure Monitor + Application Insights + Log Analytics
- Azure Storage Account

## Implementation Building Blocks in This Repository

- `FuncApp_Evals_BackEnd/telemetry/api.py`
- `FuncApp_Evals_BackEnd/telemetry/emitter.py`
- `FuncApp_Evals_BackEnd/telemetry/processor.py`
- `FuncApp_Evals_BackEnd/main.py`
- `FuncApp_Evals_BackEnd/orchestration/batch_runner.py`
- `FuncApp_Evals_BackEnd/orchestration/notifier.py`
- `FuncApp_Evals_BackEnd/scripts/submit_azure_batch.py`
- `WebApp_Evals_FrontEnd/dashboard/app.py`
- `WebApp_Evals_FrontEnd/dashboard/static/*`
