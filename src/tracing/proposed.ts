import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";

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
};

const enabled = process.env.TRACING_MODE === "proposed";
const serviceName = process.env.SERVICE_ROLE ?? "gateway";
const queueUrl = process.env.TRACE_QUEUE_URL ?? "http://localhost:4319";
const storage = new AsyncLocalStorage<ProposedTraceContext>();

function headerValue(value: string | string[] | undefined): string | undefined {
  return Array.isArray(value) ? value[0] : value;
}

function emit(event: TraceEvent): void {
  // Intentionally not awaited: bounded queue submission is off the request path.
  void fetch(`${queueUrl}/v1/events`, {
    method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(event)
  }).catch(() => undefined);
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
  const parent = storage.getStore();
  if (!parent) return execute();
  const spanId = randomUUID();
  const startTimestamp = new Date().toISOString();
  emit({ request_id: parent.requestId, span_id: spanId, parent_span_id: parent.currentSpanId, event_type: "ENTRY", function_name: functionName, layer, service_name: serviceName, start_timestamp: startTimestamp, end_timestamp: null, outcome: null });
  return storage.run({ requestId: parent.requestId, currentSpanId: spanId }, async () => {
    let outcome: TraceEvent["outcome"] = "ok";
    try {
      return await execute();
    } catch (error) {
      outcome = "error";
      throw error;
    } finally {
      emit({ request_id: parent.requestId, span_id: spanId, parent_span_id: parent.currentSpanId, event_type: "EXIT", function_name: functionName, layer, service_name: serviceName, start_timestamp: null, end_timestamp: new Date().toISOString(), outcome });
    }
  });
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
