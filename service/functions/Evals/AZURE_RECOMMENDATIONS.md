# Azure Recommendations

## Batch Job Management Options

### 1) Azure Functions (Timer Trigger)
- Best for: lightweight, serverless periodic evaluation runs.
- Pros: low ops overhead, native cron scheduling, easy integration with Key Vault and Event Hubs.
- Cons: execution timeout/scale limits for very large workloads unless split into fan-out tasks.

### 2) Durable Functions (Fan-out/Fan-in)
- Best for: orchestrating many per-app or per-policy async tasks with retries/checkpoints.
- Pros: strong workflow semantics, resilience, replay-safe orchestration, monitorable state.
- Cons: more orchestration complexity and learning curve.

### 3) AKS CronJobs
- Best for: containerized workloads with strict runtime/dependency control.
- Pros: flexible scaling, full control, suitable for heavy compute and custom networking.
- Cons: requires Kubernetes operations maturity.

### 4) Azure Batch
- Best for: bursty, large-scale compute and parallel metric jobs (especially CPU-heavy evaluations).
- Pros: elastic pool management, job/task abstraction, high parallel throughput.
- Cons: more setup for pool/image management than Functions.

## Recommended Pattern
- Start with Azure Functions Timer Trigger for schedule entry point.
- Use Durable Functions for fan-out by `app_id` and policy for concurrent evaluations.
- Move heavy metric computations to Azure Batch when CPU demand grows.

## Best Practice for Large, Heavy Calculation Batch Jobs in Azure

Use **Azure Batch** as the compute plane and partition work into many small, independent tasks.

Recommended implementation pattern:
1. Scheduler (Functions Timer Trigger or external orchestrator) creates a job per time window.
2. Partition workload into deterministic shards (for example, app groups using fixed `group_size` and `group_index`).
3. Submit one Azure Batch task per shard with explicit retry policy (see `FuncApp_Evals_BackEnd/scripts/submit_azure_batch.py`).
4. Enable pool autoscaling so VM count follows queue depth.
5. Keep tasks idempotent and checkpoint progress to durable storage to support retries/restarts.
6. Persist outputs to Cosmos DB (and optional Blob staging) to avoid loss on node preemption.

Operational guidance:
- Prefer smaller task granularity to improve parallelism and reduce blast radius of failures.
- Use low-priority/Spot nodes when cost-sensitive, with retry-safe task design.
- Keep VM images and startup lightweight to reduce provisioning latency.
- Monitor task failure rates and queue latency; scale pool bounds based on SLO targets.

## References

- Azure Batch best practices: https://learn.microsoft.com/en-us/azure/batch/best-practices
- Automatically scale nodes in an Azure Batch pool: https://learn.microsoft.com/en-us/azure/batch/batch-automatic-scaling
- Run tasks concurrently on compute nodes: https://learn.microsoft.com/en-us/azure/batch/batch-parallel-node-tasks
- Add a very large number of tasks to a Batch job: https://learn.microsoft.com/en-us/azure/batch/large-number-tasks

## Event Streaming Integration

### Ingestion Path
1. Applications emit telemetry events to Azure Event Hubs.
2. Stream processor (Azure Functions/Event Processor Client) validates and enriches events.
3. Processor writes telemetry documents to the Cosmos DB telemetry container.
4. Batch evaluation reads telemetry windows from Cosmos DB and writes evaluation results back.

### Why Event Hubs
- High-throughput ingestion and partitioned consumer scaling.
- Strong fit for near-real-time + batch hybrid monitoring.

### Optional Extensions
- Push threshold breach events to Event Hubs or Service Bus for alerting workflows.
- Build a real-time “hot path” drift/latency monitor from stream while retaining batch “cold path” evaluations.
