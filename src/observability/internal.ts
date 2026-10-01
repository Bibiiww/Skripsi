import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import { monitorEventLoopDelay, performance } from "node:perf_hooks";

type Sample = { count: number; sum: number; min: number; max: number; values: number[]; discarded: number };
type RequestSummary = { request_id: string; operation: string; request_duration_ms: number; business_duration_ms?: number; handler_duration_ms?: number; service_duration_ms: number; repository_duration_ms: number; events_produced: number; events_enqueued: number; events_dropped: number; wrapper_duration_ms: number; capture_duration_ms: number; serialization_duration_ms: number; submission_init_duration_ms: number };
type Context = Omit<RequestSummary, "request_duration_ms"> & { startedAt: number };
const enabled = process.env.INTERNAL_OBSERVABILITY === "true";
const samples = new Map<string, Sample>();
const counters = new Map<string, number>();
const requests: RequestSummary[] = [];
const storage = new AsyncLocalStorage<Context>();
const MAX_VALUES = 50_000;
const MAX_REQUESTS = 10_000;
const eventLoopDelay = monitorEventLoopDelay({ resolution: 20 });
let eluBaseline = performance.eventLoopUtilization();
if (enabled) eventLoopDelay.enable();

function sample(key: string, value: number): void {
  if (!enabled || !Number.isFinite(value)) return;
  const current = samples.get(key) ?? { count: 0, sum: 0, min: value, max: value, values: [], discarded: 0 };
  current.count += 1; current.sum += value; current.min = Math.min(current.min, value); current.max = Math.max(current.max, value);
  if (current.values.length < MAX_VALUES) current.values.push(value); else current.discarded += 1;
  samples.set(key, current);
}
function counter(key: string, amount = 1): void { if (enabled) counters.set(key, (counters.get(key) ?? 0) + amount); }
function percentile(values: number[], p: number): number | null { if (!values.length) return null; const ordered = [...values].sort((a,b) => a-b); return ordered[Math.min(ordered.length - 1, Math.ceil((p / 100) * ordered.length) - 1)]; }
function summary(value: Sample | undefined) { if (!value) return { count: 0, total: 0, mean: null, min: null, p50: null, p95: null, p99: null, max: null, discarded_raw_observations: 0 }; return { count: value.count, total: value.sum, mean: value.sum / value.count, min: value.min, p50: percentile(value.values, 50), p95: percentile(value.values, 95), p99: percentile(value.values, 99), max: value.max, discarded_raw_observations: value.discarded }; }
function requestContext(): Context | undefined { return storage.getStore(); }

export function internalObservabilityEnabled(): boolean { return enabled; }
export async function observeRequest<T>(operation: string, execute: () => Promise<T>): Promise<T> {
  if (!enabled) return execute();
  const context: Context = { request_id: randomUUID(), operation, startedAt: performance.now(), service_duration_ms: 0, repository_duration_ms: 0, events_produced: 0, events_enqueued: 0, events_dropped: 0, wrapper_duration_ms: 0, capture_duration_ms: 0, serialization_duration_ms: 0, submission_init_duration_ms: 0 };
  return storage.run(context, async () => {
    try { return await execute(); }
    finally {
      const request_duration_ms = performance.now() - context.startedAt;
      sample("REQUEST.request_duration_ms", request_duration_ms);
      if (requests.length < MAX_REQUESTS) requests.push({ ...context, request_duration_ms });
      else counter("REQUEST.request_summaries_discarded");
    }
  });
}
export async function observeBusiness<T>(layer: "handler" | "service" | "repository", execute: () => Promise<T>, root = false): Promise<T> {
  if (!enabled) return execute(); const started = performance.now();
  try { return await execute(); } finally { const duration = performance.now() - started; sample(`BUSINESS.${layer}_duration_ms`, duration); const context = requestContext(); if (context) { if (layer === "handler") context.handler_duration_ms = (context.handler_duration_ms ?? 0) + duration; else if (layer === "service") context.service_duration_ms += duration; else context.repository_duration_ms += duration; if (root) context.business_duration_ms = duration; } }
}
export function observeTiming(component: string, metric: string, durationMs: number): void { sample(`${component}.${metric}`, durationMs); }
export function increment(component: string, metric: string, amount = 1): void { counter(`${component}.${metric}`, amount); }
export function addRequestValue(key: keyof Pick<Context, "events_produced" | "events_enqueued" | "events_dropped" | "wrapper_duration_ms" | "capture_duration_ms" | "serialization_duration_ms" | "submission_init_duration_ms">, amount: number): void { const context = requestContext(); if (context) context[key] += amount; }
export function internalMetrics() {
  const timing = Object.fromEntries([...samples].map(([key, value]) => [key, summary(value)]));
  const elu = performance.eventLoopUtilization(eluBaseline);
  return { schemaVersion: "1.0.0", enabled, captured_at: new Date().toISOString(), limits: { raw_timing_observations_per_metric: MAX_VALUES, per_request_summaries: MAX_REQUESTS }, counters: Object.fromEntries(counters), timings: timing, per_request: { count: requests.length, summaries: requests }, system: enabled ? { process_memory_bytes: process.memoryUsage(), event_loop_utilization: elu.utilization, event_loop_lag_ms: { mean: eventLoopDelay.mean / 1e6, p95: eventLoopDelay.percentile(95) / 1e6, max: eventLoopDelay.max / 1e6 } } : null };
}
export function resetInternalMetrics(): void { samples.clear(); counters.clear(); requests.length = 0; eluBaseline = performance.eventLoopUtilization(); eventLoopDelay.reset(); }
