import { randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";
import { increment, observeTiming } from "../observability/internal.js";

export type TraceContext = { traceId: string; currentSpanId: string };
export type TraceSpan = {
  traceId: string;
  spanId: string;
  parentSpanId: string | null;
  serviceName: string;
  operation: string;
  kind: "server" | "client";
  startedAt: string;
  endedAt?: string;
  durationMs?: number;
  statusCode?: number;
};
export type ActiveServerTrace = { context: TraceContext; span: TraceSpan; startedAt: number };

function headerValue(value: string | string[] | undefined): string | undefined {
  return Array.isArray(value) ? value[0] : value;
}

export class ConventionalTracer {
  constructor(private readonly serviceName: string, private readonly collectorUrl: string) {}

  beginServerSpan(headers: Record<string, string | string[] | undefined>, operation: string): ActiveServerTrace {
    const started = performance.now();
    const traceId = headerValue(headers["x-trace-id"]) ?? randomUUID();
    const parentSpanId = headerValue(headers["x-parent-span-id"]) ?? null;
    const spanId = randomUUID();
    const result: ActiveServerTrace = {
      context: { traceId, currentSpanId: spanId },
      span: { traceId, spanId, parentSpanId, serviceName: this.serviceName, operation, kind: "server", startedAt: new Date().toISOString() },
      startedAt: performance.now()
    }; increment("CONVENTIONAL", "spans_created"); observeTiming("CONVENTIONAL", "span_creation_duration_ms", performance.now() - started); return result;
  }

  async endServerSpan(active: ActiveServerTrace, statusCode: number): Promise<void> {
    await this.export({ ...active.span, endedAt: new Date().toISOString(), durationMs: performance.now() - active.startedAt, statusCode });
  }

  async fetch(context: TraceContext, operation: string, url: string): Promise<Response> {
    const creationStarted = performance.now();
    const spanId = randomUUID();
    const startedAt = performance.now();
    const span: TraceSpan = {
      traceId: context.traceId, spanId, parentSpanId: context.currentSpanId, serviceName: this.serviceName,
      operation, kind: "client", startedAt: new Date().toISOString()
    };
    increment("CONVENTIONAL", "spans_created"); observeTiming("CONVENTIONAL", "span_creation_duration_ms", performance.now() - creationStarted);
    try {
      const response = await fetch(url, { headers: { "x-trace-id": context.traceId, "x-parent-span-id": spanId } });
      await this.export({ ...span, endedAt: new Date().toISOString(), durationMs: performance.now() - startedAt, statusCode: response.status });
      return response;
    } catch (error) {
      await this.export({ ...span, endedAt: new Date().toISOString(), durationMs: performance.now() - startedAt, statusCode: 599 });
      throw error;
    }
  }

  private async export(span: TraceSpan): Promise<void> {
    // Deliberately awaited: this is the conventional synchronous comparison condition.
    // Export failures are isolated from business responses but can be observed in collector logs.
    const serializationStarted = performance.now(); const body = JSON.stringify(span); observeTiming("CONVENTIONAL", "serialization_duration_ms", performance.now() - serializationStarted); increment("CONVENTIONAL", "serialization_bytes", Buffer.byteLength(body)); increment("CONVENTIONAL", "export_attempts");
    const exportStarted = performance.now();
    try {
      const response = await fetch(`${this.collectorUrl}/v1/spans`, { method: "POST", headers: { "content-type": "application/json" }, body });
      observeTiming("CONVENTIONAL", "export_duration_ms", performance.now() - exportStarted); increment("CONVENTIONAL", response.ok ? "export_success" : "export_failed");
    } catch {
      observeTiming("CONVENTIONAL", "export_duration_ms", performance.now() - exportStarted); increment("CONVENTIONAL", "export_failed");
      // The proposed condition will expose queue drops explicitly; conventional exporters may lose events on failure.
    }
  }
}
