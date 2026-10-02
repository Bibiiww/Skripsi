import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";
import { addRequestValue, increment, internalObservabilityEnabled, observeTiming } from "../observability/internal.js";

export type ProposedTraceContext = { requestId: string; currentSpanId: string | null };
export type TraceEvent = {
  request_id: string;
  span_id: string;
  parent_span_id: string | null;
  event_type: "ENTRY" | "EXIT";
  function_name: string;
  layer: "handler" | "service" | "repository";
  service_name: string;
  start_timestamp: string | null;
  end_timestamp: string | null;
  outcome: "ok" | "error" | null;
  event_created_at_ms?: number;
  event_enqueued_at_ms?: number;
};

const enabled = process.env.TRACING_MODE === "proposed";
const serviceName = process.env.SERVICE_ROLE ?? "gateway";
const queueUrl = process.env.TRACE_QUEUE_URL ?? "http://localhost:4319";
const storage = new AsyncLocalStorage<ProposedTraceContext>();

function headerValue(value: string | string[] | undefined): string | undefined {
  return Array.isArray(value) ? value[0] : value;
}

function emit(event: TraceEvent): void {
  const diagnostic = internalObservabilityEnabled();
  const serializeStarted = diagnostic ? performance.now() : 0;
  const body = JSON.stringify(event);
  const serializationMs = diagnostic ? performance.now() - serializeStarted : 0;
  if (diagnostic) { increment("EVENT_CAPTURE", "events_produced"); addRequestValue("events_produced", 1); observeTiming("SERIALIZATION", "duration_ms", serializationMs); addRequestValue("serialization_duration_ms", serializationMs); increment("SERIALIZATION", "bytes", Buffer.byteLength(body)); }
  // Intentionally not awaited: bounded queue submission is off the request path.
  const submissionStarted = diagnostic ? performance.now() : 0;
  let pending: Promise<Response>;
  try { pending = fetch(`${queueUrl}/v1/events`, { method: "POST", headers: { "content-type": "application/json" }, body }); }
  catch { increment("EVENT_SUBMISSION", "initiation_errors"); return; }
  const initiationMs = diagnostic ? performance.now() - submissionStarted : 0;
  if (diagnostic) { increment("EVENT_SUBMISSION", "attempts"); increment("EVENT_SUBMISSION", "initiated"); increment("EVENT_SUBMISSION", "bytes", Buffer.byteLength(body)); observeTiming("EVENT_SUBMISSION", "init_duration_ms", initiationMs); addRequestValue("submission_init_duration_ms", initiationMs); }
  void pending.then(() => increment("EVENT_SUBMISSION", "completed")).catch(() => increment("EVENT_SUBMISSION", "errors"));
}

export function proposedTraceHeaders(): Record<string, string> | undefined {
  if (!enabled) return undefined;
  const context = storage.getStore();
  if (!context?.currentSpanId) return undefined;
  return { "x-trace-id": context.requestId, "x-parent-span-id": context.currentSpanId };
}

export function currentProposedTraceId(): string | undefined {
  return enabled ? storage.getStore()?.requestId : undefined;
}

export async function withProposedSpan<T>(
  functionName: string,
  layer: TraceEvent["layer"],
  execute: () => Promise<T>
): Promise<T> {
  if (!enabled) return execute();
  const diagnostic = internalObservabilityEnabled();
  const lookupStarted = diagnostic ? performance.now() : 0; const parent = storage.getStore(); if (diagnostic) observeTiming("CONTEXT", "lookup_ms", performance.now() - lookupStarted);
  if (!parent) return execute();
  const preStarted = diagnostic ? performance.now() : 0; const idStarted = diagnostic ? performance.now() : 0; const spanId = randomUUID(); if (diagnostic) observeTiming("WRAPPER", "span_id_generation_ms", performance.now() - idStarted);
  const timestampStarted = diagnostic ? performance.now() : 0; const startTimestamp = new Date().toISOString(); if (diagnostic) observeTiming("WRAPPER", "timestamp_generation_ms", performance.now() - timestampStarted);
  const entryStarted = diagnostic ? performance.now() : 0; const entry = { request_id: parent.requestId, span_id: spanId, parent_span_id: parent.currentSpanId, event_type: "ENTRY" as const, function_name: functionName, layer, service_name: serviceName, start_timestamp: startTimestamp, end_timestamp: null, outcome: null, event_created_at_ms: diagnostic ? Date.now() : undefined }; const entryMs = diagnostic ? performance.now() - entryStarted : 0; if (diagnostic) { increment("EVENT_CAPTURE", "entry_events"); observeTiming("EVENT_CAPTURE", "entry_event_creation_ms", entryMs); observeTiming("EVENT_CAPTURE", "duration_ms", entryMs); addRequestValue("capture_duration_ms", entryMs); } emit(entry);
  const preMs = diagnostic ? performance.now() - preStarted : 0; if (diagnostic) observeTiming("WRAPPER", "pre_ms", preMs);
  const alsStarted = diagnostic ? performance.now() : 0; const result = storage.run({ requestId: parent.requestId, currentSpanId: spanId }, async () => {
    let outcome: TraceEvent["outcome"] = "ok";
    try {
      return await execute();
    } catch (error) {
      outcome = "error";
      throw error;
    } finally {
      const postStarted = diagnostic ? performance.now() : 0; const exitStarted = diagnostic ? performance.now() : 0; const exit = { request_id: parent.requestId, span_id: spanId, parent_span_id: parent.currentSpanId, event_type: "EXIT" as const, function_name: functionName, layer, service_name: serviceName, start_timestamp: null, end_timestamp: new Date().toISOString(), outcome, event_created_at_ms: diagnostic ? Date.now() : undefined }; const exitMs = diagnostic ? performance.now() - exitStarted : 0; if (diagnostic) { increment("EVENT_CAPTURE", "exit_events"); observeTiming("EVENT_CAPTURE", "exit_event_creation_ms", exitMs); observeTiming("EVENT_CAPTURE", "duration_ms", exitMs); addRequestValue("capture_duration_ms", exitMs); } emit(exit); const postMs = diagnostic ? performance.now() - postStarted : 0; if (diagnostic) { observeTiming("WRAPPER", "post_ms", postMs); observeTiming("WRAPPER", "total_overhead_ms", preMs + postMs); addRequestValue("wrapper_duration_ms", preMs + postMs); }
    }
  }); if (diagnostic) observeTiming("WRAPPER", "async_local_storage_run_ms", performance.now() - alsStarted); return result;
}

export async function runProposedRequest<T>(
  headers: Record<string, string | string[] | undefined>,
  operation: string,
  execute: () => Promise<T>
): Promise<T> {
  if (!enabled) return execute();
  const requestId = headerValue(headers["x-trace-id"]) ?? randomUUID();
  const parentSpanId = headerValue(headers["x-parent-span-id"]) ?? null;
  return storage.run({ requestId, currentSpanId: parentSpanId }, () => withProposedSpan(operation, "handler", execute));
}
