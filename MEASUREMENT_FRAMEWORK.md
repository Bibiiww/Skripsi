# Measurement Framework Audit

## Fixed primary boundary

```text
load generator request start ─────────────────────────────── response fully received
                              request latency (PRIMARY)

gateway: immediately before buildQuote() ─────────────── buildQuote() resolves
                         business latency (PRIMARY)
```

The gateway business interval is identical for baseline, conventional, and
proposed. It deliberately includes any work that actually happens before
`buildQuote()` can resolve. Therefore proposed wrapper/context/UUID/timestamp,
event creation, JSON serialization, and detached-fetch initiation remain in the
business metric when they occur in that interval. They are not removed or
reclassified to make a condition appear faster.

The conventional gateway-to-service export is awaited inside `buildQuote()` and
is consequently inside business latency. Conventional server-span export runs in
Fastify's `onResponse` hook after the business interval; it remains visible in
external request latency. Proposed detached transport completion, queue
admission, worker processing, and reconstruction are outside business latency.

## Measurement architecture

```text
Baseline ──────┐
Conventional ─┼── common gateway business boundary ── primary performance
Proposed ─────┘                                      request/business/RPS/CPU/RAM

Proposed ── event submission ── queue ── worker ── reconstruction ── diagnostics
Conventional ── synchronous export ── collector reconstruction ─── diagnostics
```

## Metric classes

| Class | Metrics |
| --- | --- |
| PRIMARY | request latency, business latency, achieved RPS, successful requests, CPU, memory |
| SECONDARY | success rate, generator drop rate, queue drop rate, reconstruction success rate |
| DIAGNOSTIC | wrapper, capture, serialization, submission initiation, queue depth/wait, worker, reconstruction internals |

## Run status

- **valid**: fixed boundary, descriptor identity, condition identity, primary
  metrics, and resource samples are all present.
- **saturated**: valid or otherwise completed run with success/target-rate below
  the configured threshold, or proposed queue drops / an undrained async
  pipeline. All reasons are retained in `saturationSignals`.
- **comparison eligible**: valid, non-saturated run with diagnostic mode off.
  Diagnostic mode is intentionally excluded from the primary comparison dataset
  because collection itself adds measurement work.
- **invalid**: required primary/resource/boundary evidence is missing. The raw
  run is kept and listed in `run_validation.csv`.

The configured 95% success/target-rate criterion is retained as a throughput
classification rule. Queue drops and a failed drain are system-state saturation
signals, not invented queue-utilization thresholds.

## Results and legacy data

New runs create `primary/` and `observability/` subdirectories. `run-matrix`
defaults to `--measurement-mode primary`, which forcibly supplies
`INTERNAL_OBSERVABILITY=false` both while Compose starts and while the runner
executes. It verifies that the gateway reports the same state. Matrix output:

- `primary_results.csv`: only valid, non-saturated, diagnostic-off comparisons.
- `observability_results.csv`: proposed/conventional diagnostic pipeline data.
- `run_validation.csv`: validity, saturation, comparison eligibility, and reasons.

Results created before manifest schema `2.0.0` are **legacy /
measurement-boundary-uncertain**. They are retained without mathematical
conversion and should be rerun for direct comparison with the new primary
dataset.
