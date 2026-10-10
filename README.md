# Context-Aware Function-Level Tracing Benchmark

Reproducible microservices benchmark for the thesis **"Perancangan Context-Aware Function-Level Tracing untuk Meningkatkan Observabilitas Internal pada Arsitektur Microservices dengan Pendekatan Asynchronous Event Processing"**.

Untuk menjalankan dataset komparatif terbaru C0–C3 dan memilih data yang layak dianalisis, baca [README-COMPARATIVE-BENCHMARK.md](README-COMPARATIVE-BENCHMARK.md).

## Current stage: audit-ready benchmark

This repository contains a small, deterministic three-service application with isolated comparison conditions:

| Condition | Status | Purpose |
| --- | --- | --- |
| Baseline (no tracing) | implemented | Reference performance of the same application. |
| Conventional tracing | implemented | Synchronous, inter-service/request-flow tracing. |
| Proposed tracing | implemented | Boundary-level function events, propagated context, bounded async queue, and reconstruction worker. |

The gateway's `GET /api/v1/quote?productId=sku-001&quantity=2` endpoint calls both `catalog` and `inventory`. Each service uses the architectural layers required by the proposal:

```text
HTTP handler -> service -> repository
gateway handler -> gateway service -> HTTP client -> catalog / inventory
```

The repositories use fixed in-memory data at this stage. This is an **experimental design decision**, not a claim from the proposal: it keeps storage, cache warm-up, and network database effects from confounding the first tracing-overhead measurements. A database-backed variant can be added later as a separately documented factor.

## Run

Prerequisites: Docker Compose v2. The image is built from the committed
`pnpm-lock.yaml`, so the application dependencies are fixed across reruns.

```sh
docker compose -p tracing-baseline up --build
```

Then request:

```sh
curl "http://localhost:8080/api/v1/quote?productId=sku-001&quantity=2"
```

Stop the environment with `docker compose -p tracing-baseline down`.

## Methodology alignment

Taken from the proposal:

- controlled container environment using Docker Compose;
- layered handlers, services, and repositories;
- comparison of conventional and proposed tracing under the same environment;
- measurements for latency, CPU, memory, throughput, queue drop rate, and reconstruction success rate;
- proposed tracing will use wrapper instrumentation, request/span context, asynchronous event processing, and call-graph reconstruction.

Not yet specified by the proposal and therefore treated as **experimental design decisions**:

- Node.js 22, TypeScript, and Fastify;
- product-quote domain and the gateway/catalog/inventory topology;
- fixed in-memory repository data;
- a third baseline condition, retained to make each tracing overhead calculation auditable;
- workload descriptors for computational signature, structural signature, input size, and request intensity.

## Measurement validity

The fixed measurement boundary, metric classes, saturation signals, validation
rules, output datasets, and legacy-data policy are documented in
[MEASUREMENT_FRAMEWORK.md](MEASUREMENT_FRAMEWORK.md). Use normal mode
(`INTERNAL_OBSERVABILITY=false`, the default) to collect `primary_results.csv`.
Use diagnostic mode only in a separate explanatory run; its results are written
to `observability_results.csv` and are not automatically eligible for primary
comparison.

## Workload descriptors and experiment runner

`workloads/v1/` contains versioned JSON descriptors. Each pins the computational
signature, structural signature, input size, traffic profile, exact target query,
warm-up, duration, and in-flight cap. The application applies a profile only when
the `profile` query parameter is present; ordinary quote requests remain the
unprofiled base path.

| Descriptor | Controlled profile | Nominal traffic |
| --- | --- | --- |
| `o1-shallow-low.json` | O(1), shallow | 50 RPS |
| `ologn-moderate-mid.json` | O(log n), moderate | 200 RPS |
| `on-complex-high.json` | O(n), complex | 800 RPS |

The structural profiles are deterministic internal boundary shapes and the compute
loops are deterministic; neither introduces random sleeps, external data, or
database cache effects. These profiles are experimental-design decisions and may
be revised only by adding a new descriptor/profile version, not by overwriting a
configuration already used for results.

To perform one full repeatable run after `docker compose -p tracing-baseline up -d`:

```sh
node tools/run-experiment.mjs --descriptor workloads/v1/o1-shallow-low.json --condition baseline
```

Each run creates `results/<run-id>/` containing `manifest.json` (condition,
descriptor hash, containers, timestamps), an immutable descriptor copy,
`http-summary.json` (throughput, HTTP statuses, generator drops, Request Latency,
and Business Processing Time with p50/p95/p99),
and `container-metrics.json` (one-second Docker CPU/memory samples). `results/`
is intentionally ignored by Git so raw measurements do not get mixed with source.

The runner performs a warm-up first, drains the proposed tracing pipeline, then
captures all reported tracing values as deltas for the measurement phase. Thus
`http-summary.json` records **Request Latency** (client request start to HTTP
response) and **Business Processing Time** (Gateway `buildQuote` start to quote
ready, including its downstream business calls and deterministic workload).
`tracing-metrics.json` records the separate asynchronous path:
events produced/enqueued/dropped/reconstructed, queue state, and reconstruction
state. Reconstruction Success Rate uses only successful measurement requests as
its denominator; warm-up requests are excluded.

## Conventional synchronous tracing condition

The conventional condition is isolated in [compose.conventional.yaml](compose.conventional.yaml), not mixed into the baseline configuration. It records only conventional HTTP request-flow spans: each server request and each gateway-to-service HTTP call. Span export is deliberately awaited on the request path. It does **not** instrument handler, service, or repository functions.

Start it with a separate Compose project (first stop the baseline stack because both use port 8080):

```sh
docker compose -p tracing-baseline down
docker compose -p tracing-conventional -f compose.yaml -f compose.conventional.yaml up --build -d
```

The normal quote endpoint remains on port `8080`. During a run, inspect collected conventional spans at `http://localhost:16686/v1/spans`; individual traces are available at `/v1/traces/<traceId>`. The collector is intentionally a separate container and is not part of the baseline condition.

Run its workload with the matching project name:

```sh
node tools/run-experiment.mjs --descriptor workloads/v1/o1-shallow-low.json --condition conventional --compose-project tracing-conventional
```

## Proposed asynchronous function-level tracing condition

The proposed condition is isolated in [compose.proposed.yaml](compose.proposed.yaml). It adds two containers:

```text
instrumented application functions
  -> non-blocking HTTP submission
  -> bounded trace-event queue (capacity 10,000)
  -> reconstruction worker
  -> per-request call graph
```

The wrapper creates `ENTRY` and `EXIT` events at the handler, service, and repository boundaries. `AsyncLocalStorage` carries `requestId` and the active parent span across nested calls; the gateway also propagates this context to catalog and inventory. Submitting events does not await queue admission or graph reconstruction, so neither operation blocks the business response.

Start the proposed condition after stopping the preceding condition:

```sh
docker compose -p tracing-conventional down
docker compose -p tracing-proposed -f compose.yaml -f compose.proposed.yaml up --build -d
```

The queue metrics endpoint is `http://localhost:16686/v1/metrics`; it reports accepted, dropped, queued, and dequeued events. The worker metrics endpoint is `http://localhost:16687/v1/metrics`; a reconstructed graph is available at `http://localhost:16687/v1/traces/<request-id>`. The quote response returns that request ID in the `x-trace-id` header.

Run the proposed experiment using its project name:

```sh
node tools/run-experiment.mjs --descriptor workloads/v1/o1-shallow-low.json --condition proposed --compose-project tracing-proposed
```

For proposed runs, the runner additionally stores `tracing-metrics.json` with measurement-only deltas for the queue and reconstruction worker. These values provide the raw inputs for Queue Drop Rate and Reconstruction Success Rate analysis.

### Internal diagnostic observability

Diagnostic instrumentation is disabled by default and does not alter tracing architecture, workload, queue capacity, worker concurrency, batch size, or the deliberately non-awaited proposed submission. To run it in PowerShell, start the selected Compose stack and benchmark in the same shell with:

```powershell
$env:INTERNAL_OBSERVABILITY = "true"
node tools/run-experiment.mjs --descriptor workloads/v1/o1-shallow-low.json --condition proposed --compose-project tracing-proposed --internal-observability true
```

Each diagnostic run adds `latency.json`, `resource.json`, and `internal-observability.json`. The latter distinguishes request/business timing from critical-path wrapper, event-capture, JSON serialization, submission-initiation, queue admission/wait/depth, worker batch processing, reconstruction, event flow, per-process system metrics, and correlation fields. Raw timing observations are capped (50,000 per metric; 10,000 request summaries) and the cap is recorded in the artifact. Metric fields are measurement-only because the runner resets diagnostics after warm-up.

`async_trace_tail` is intentionally not reported as a fabricated exact duration: the current proposed transport is detached `void fetch`, so application code has no reliable response-sent timestamp that can be correlated with the detached completion. Submission initiation is measured; queue/worker/reconstruction timing is measured independently. Likewise, conventional collector reconstruction is measured from its actual in-memory collector grouping operation; it is not claimed to be a separate tracing backend stage.

### Docker smoke test for diagnostic instrumentation

Run this before a long matrix. It starts only the proposed stack, executes one low-load workload, and writes one diagnostic artifact. The final cleanup is important because port `8080` is shared by all conditions.

```powershell
$env:INTERNAL_OBSERVABILITY = "true"
docker compose -p tracing-observability-smoke -f compose.yaml -f compose.proposed.yaml up --build -d --wait
node tools/run-experiment.mjs --descriptor workloads/v1/o1-shallow-low.json --condition proposed --compose-project tracing-observability-smoke --internal-observability true
docker compose -p tracing-observability-smoke -f compose.yaml -f compose.proposed.yaml down --remove-orphans
```

Confirm that the new run folder contains `internal-observability.json` and that `diagnosticMode` is `true`. This validates instrumentation only; do not use it as a final performance result.

## Automated experiment matrix

Use `run-experiment` for one condition and one descriptor. Use `run-matrix` to run every selected descriptor automatically. Collect the primary comparison dataset with diagnostic instrumentation off:

```powershell
node tools/run-matrix.mjs --measurement-mode primary --conditions baseline --repetitions 3
node tools/run-matrix.mjs --measurement-mode primary --conditions conventional --repetitions 3
node tools/run-matrix.mjs --measurement-mode primary --conditions proposed --repetitions 3
```

Run a separate diagnostic matrix only when explaining a finding. It produces
observability artifacts but is intentionally excluded from `primary_results.csv`:

```powershell
node tools/run-matrix.mjs --measurement-mode diagnostic --conditions proposed --repetitions 3
```

It runs the conditions in this order: `baseline`, `conventional`, then `proposed`.
Within each condition, descriptors run from low to high RPS. Before every
workload repetition it starts a fresh isolated Compose stack and waits for health
checks; it always stops that stack afterward. This state reset prevents an
overloaded workload from affecting the next measurement. The default is one
repetition, a five-second cooldown, and a maximum transport-error rate of 5%.

Each invocation writes `results/<matrix-id>/` containing `matrix-manifest.json`,
per-run raw artifacts under `runs/`, and analysis-ready `analysis.json` and
`analysis.csv`. `primary_results.csv` contains only valid, non-saturated, diagnostic-off runs for direct primary comparison. `observability_results.csv` contains the tracing-pipeline fields, and `run_validation.csv` contains validity, saturation, comparison eligibility, and reasons. `condition-summary.csv` gives one row per condition/workload with mean and minimum `success_rate_percent`, achieved RPS, generator-drop rate, p95 latency, and completed/saturated counts. Therefore conventional success rate is explicit both per-run in `analysis.csv`/`stress-test.csv` and per-workload in `condition-summary.csv`. When `INTERNAL_OBSERVABILITY=true`, `internal-observability-summary.csv` also aggregates wrapper, serialization, submission-initiation, queue, worker, reconstruction, business p95, and request p95 metrics per condition/workload. The flat analysis files include only valid measurements. Rejected
runs are recorded separately in `failed-runs.csv` and the matrix manifest, with
their error details. A run is rejected when it has no successful HTTP response or
its transport-error rate exceeds 5%; the command continues to remaining runs,
cleans up the relevant Compose stack, and exits non-zero.

Runs are classified as follows:

- `completed`: the target RPS and successful-request rate both reach at least 95%.
- `saturated`: the application and runner completed the measurement but either
  rate falls below 95%. These runs remain valuable stress-test results and are
  written to `stress-test.csv`, not discarded as technical failures.
- `failed`: a technical failure, such as an unhealthy Compose stack, Docker
  unavailability, or a missing run artifact. These are written to
  `failed-runs.csv`. The matrix performs one cleanup-and-retry when a Compose
  stack fails to start before classifying the run as failed.

Every row now includes `success_rate_percent`, `generator_drop_rate_percent`,
target-rate attainment, and aggregate resource headroom. CPU headroom is based
on Docker's reported host CPU cores; memory headroom is based on Docker's total
memory capacity. The resource values include every container in the selected
condition, including queue and worker containers for proposed tracing.

At the end of a matrix, `charts.html` is generated automatically in the matrix
directory. It contains line charts for request latency, success rate, successful
requests, CPU, memory, business processing time, and reconstruction success.
Generate it again manually when needed:

```sh
node tools/generate-charts.mjs --matrix-dir results/<matrix-id>
```

The default host endpoint is `http://127.0.0.1:8080`, avoiding Windows systems
that resolve `localhost` to an unavailable IPv6 listener. When a request cannot
connect, `http-summary.json` records the detailed transport error rather than
only a counter.

Examples:

```sh
# Three repetitions of all conditions and workloads.
node tools/run-matrix.mjs --repetitions 3

# A compact pilot: mixed workload only, one run per condition.
node tools/run-matrix.mjs --descriptors mixed-complex-high --cooldown-seconds 0

# Select specific conditions or descriptors.
node tools/run-matrix.mjs --conditions baseline,proposed --descriptors o1-shallow-low,ologn-moderate-mid

# Classify a run as saturated below 98% success/target-rate attainment.
node tools/run-matrix.mjs --saturation-threshold-percent 98

# Override the host endpoint when Docker uses a different published address.
node tools/run-matrix.mjs --base-url http://127.0.0.1:8080
```

## Next implementation stages

1. Add automated experiment-matrix execution and analysis-ready CSV/JSON results.
# System-level asynchronous tracing variants

The historical `proposed` condition remains unchanged. For the refactored
system-level variants, use the following explicit conditions:

```powershell
# C2: bounded in-memory record buffer and asynchronous batches
docker compose -f compose.yaml -f compose.proposed-memory.yaml up --build --detach --wait
node tools/run-experiment.mjs --descriptor workloads/v1/o1-shallow-low.json --condition proposed-memory

# C3: same record contract, plus asynchronous local WAL group commits
docker compose -f compose.yaml -f compose.proposed-durable.yaml up --build --detach --wait
node tools/run-experiment.mjs --descriptor workloads/v1/o1-shallow-low.json --condition proposed-durable
```

Use the same workload descriptor, warm-up, duration, Docker allocation, and
load-generator policy when comparing C0 (`baseline`), C1 (`conventional`), C2,
and C3. Do not merge their historical and refactored results into one aggregate.

`proposed-memory` captures one completion `ExecutionRecord` per selected
function boundary. Record admission is synchronous and bounded; batch JSON
serialization and HTTP transport are background work. `proposed-durable` first
writes records with an asynchronous group commit to `/var/lib/tracing` and
replays unacknowledged records after a producer restart. A record created before
the next group commit can still be lost in a crash; queue acknowledgement does
not prove reconstruction.

Run the refactored conditions in the matrix runner with:

```powershell
node tools/run-matrix.mjs --conditions baseline,conventional,proposed-memory,proposed-durable --measurement-mode primary
```

See `ASYNC_TRACING_AUDIT.md` for the audited boundaries, the protected V4.1
artifact, and current limitations.

