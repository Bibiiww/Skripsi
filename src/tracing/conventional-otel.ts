import { AsyncLocalStorage } from "node:async_hooks";
import { context, propagation, SpanKind, SpanStatusCode, trace } from "@opentelemetry/api";
import { BatchSpanProcessor, type ReadableSpan, type SpanExporter } from "@opentelemetry/sdk-trace-base";
import { NodeTracerProvider } from "@opentelemetry/sdk-trace-node";
import { performance } from "node:perf_hooks";
import type { ActiveServerTrace, TraceContext, TraceSpan } from "./conventional.js";
import type { TraceLayer } from "./async-execution.js";

let installedTracer: ConventionalOtelTracer | undefined;

/** One tracer exists per application process. Boundary wrappers use this only
 * when the process is running the conventional-otel condition. */
export function installConventionalOtelTracer(tracer: ConventionalOtelTracer | undefined): void {
  installedTracer = tracer;
}

export function activeConventionalOtelTracer(): ConventionalOtelTracer | undefined {
  return installedTracer;
}

class HttpCollectorExporter implements SpanExporter {
  constructor(private readonly collectorUrl: string, private readonly serviceName: string) {}
  export(spans: ReadableSpan[], callback: Parameters<SpanExporter["export"]>[1]): void {
    const payload: TraceSpan[] = spans.map((span) => ({
      traceId: span.spanContext().traceId, spanId: span.spanContext().spanId,
      parentSpanId: span.parentSpanContext?.spanId ?? null, serviceName: this.serviceName,
      operation: span.name, kind: span.kind === SpanKind.CLIENT ? "client" : "server",
      startedAt: new Date(span.startTime[0] * 1e3 + span.startTime[1] / 1e6).toISOString(),
      endedAt: new Date(span.endTime[0] * 1e3 + span.endTime[1] / 1e6).toISOString(),
      durationMs: (span.endTime[0] - span.startTime[0]) * 1e3 + (span.endTime[1] - span.startTime[1]) / 1e6,
      statusCode: span.status.code === SpanStatusCode.ERROR ? 500 : 200
    }));
    void fetch(`${this.collectorUrl}/v1/spans/batch`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ spans: payload }) })
      .then((response) => callback({ code: response.ok ? 0 : 1 }))
      .catch(() => callback({ code: 1 }));
  }
  async shutdown(): Promise<void> { /* BatchSpanProcessor flushes on application shutdown. */ }
}

export type OtelActiveTrace = Omit<ActiveServerTrace, "span"> & { span: ReturnType<ReturnType<typeof trace.getTracer>["startSpan"]> };

export class ConventionalOtelTracer {
  private readonly provider: NodeTracerProvider;
  private readonly tracer;
  private readonly activeSpans = new Map<string, OtelActiveTrace["span"]>();
  private readonly requestSpans = new AsyncLocalStorage<OtelActiveTrace["span"]>();
  constructor(private readonly serviceName: string, private readonly collectorUrl: string) {
    this.provider = new NodeTracerProvider({ spanProcessors: [new BatchSpanProcessor(new HttpCollectorExporter(collectorUrl, serviceName), { maxQueueSize: Number(process.env.OTEL_BSP_MAX_QUEUE_SIZE ?? 2048), scheduledDelayMillis: Number(process.env.OTEL_BSP_SCHEDULE_DELAY_MS ?? 100), maxExportBatchSize: Number(process.env.OTEL_BSP_MAX_EXPORT_BATCH_SIZE ?? 256) })] });
    this.provider.register(); this.tracer = trace.getTracer(`benchmark.${serviceName}`);
  }
  beginServerSpan(headers: Record<string, string | string[] | undefined>, operation: string): OtelActiveTrace {
    const parent = propagation.extract(context.active(), headers); const span = this.tracer.startSpan(operation, { kind: SpanKind.SERVER }, parent); const spanContext = span.spanContext();
    this.activeSpans.set(spanContext.spanId, span);
    return { context: { traceId: spanContext.traceId, currentSpanId: spanContext.spanId }, span, startedAt: performance.now() };
  }
  async endServerSpan(active: OtelActiveTrace, statusCode: number): Promise<void> { if (statusCode >= 500) active.span.setStatus({ code: SpanStatusCode.ERROR }); active.span.end(); this.activeSpans.delete(active.context.currentSpanId); }

  /**
   * Makes the incoming controller span the active parent for the full handler.
   * This gives function-level spans and outgoing RPC spans the same parent-child
   * semantics as the proposed execution-record context.
   */
  runWithActiveRequest<T>(active: OtelActiveTrace, execute: () => Promise<T>): Promise<T> {
    const activeContext = trace.setSpan(context.active(), active.span);
    return context.with(activeContext, () => this.requestSpans.run(active.span, execute));
  }

  async withBoundary<T>(operation: string, layer: TraceLayer, execute: () => Promise<T>): Promise<T> {
    const parentSpan = this.requestSpans.getStore();
    if (!parentSpan) return execute();
    const parentContext = trace.setSpan(context.active(), parentSpan);
    return context.with(parentContext, async () => {
      const span = this.tracer.startSpan(operation, { kind: layer === "rpc" ? SpanKind.CLIENT : SpanKind.INTERNAL });
      try {
        return await this.requestSpans.run(span, execute);
      } catch (error) {
        span.setStatus({ code: SpanStatusCode.ERROR });
        throw error;
      } finally {
        span.end();
      }
    });
  }

  async fetch(active: TraceContext, operation: string, url: string): Promise<Response> {
    const parentSpan = this.requestSpans.getStore() ?? this.activeSpans.get(active.currentSpanId);
    const parentContext = parentSpan ? trace.setSpan(context.active(), parentSpan) : context.active();
    return context.with(parentContext, async () => { const span = this.tracer.startSpan(operation, { kind: SpanKind.CLIENT }); const headers: Record<string, string> = {}; propagation.inject(trace.setSpan(context.active(), span), headers); try { const response = await fetch(url, { headers }); if (response.status >= 500) span.setStatus({ code: SpanStatusCode.ERROR }); return response; } catch (error) { span.setStatus({ code: SpanStatusCode.ERROR }); throw error; } finally { span.end(); } });
  }
  async shutdown(): Promise<void> { await this.provider.shutdown(); }
}
