import { randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";

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
    const traceId = headerValue(headers["x-trace-id"]) ?? randomUUID();
    const parentSpanId = headerValue(headers["x-parent-span-id"]) ?? null;
    const spanId = randomUUID();
    return {
      context: { traceId, currentSpanId: spanId },
      span: { traceId, spanId, parentSpanId, serviceName: this.serviceName, operation, kind: "server", startedAt: new Date().toISOString() },
      startedAt: performance.now()
    };
  }

  async endServerSpan(active: ActiveServerTrace, statusCode: number): Promise<void> {
    await this.export({ ...active.span, endedAt: new Date().toISOString(), durationMs: performance.now() - active.startedAt, statusCode });
  }

  async fetch(context: TraceContext, operation: string, url: string): Promise<Response> {
    const spanId = randomUUID();
    const startedAt = performance.now();
    const span: TraceSpan = {
      traceId: context.traceId, spanId, parentSpanId: context.currentSpanId, serviceName: this.serviceName,
      operation, kind: "client", startedAt: new Date().toISOString()
    };
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
    try {
      await fetch(`${this.collectorUrl}/v1/spans`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(span) });
    } catch {
      // The proposed condition will expose queue drops explicitly; conventional exporters may lose events on failure.
    }
  }
}
