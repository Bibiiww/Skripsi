import { context, propagation, SpanKind, SpanStatusCode, trace, type Context } from "@opentelemetry/api";
import { ExportResultCode } from "@opentelemetry/core";
import { type ReadableSpan, type SpanExporter } from "@opentelemetry/sdk-trace-base";
import { BatchSpanProcessor, NodeTracerProvider } from "@opentelemetry/sdk-trace-node";

export type OtelActiveTrace = { context: Context; span: import("@opentelemetry/api").Span };
type CollectorSpan = { traceId: string; spanId: string; parentSpanId: string | null; serviceName: string; operation: string; kind: "server" | "client"; startedAt: string; endedAt?: string; durationMs?: number; statusCode?: number };

class JsonBatchExporter implements SpanExporter {
  constructor(private readonly collectorUrl: string, private readonly serviceName: string) {}
  export(spans: ReadableSpan[], done: (result: { code: ExportResultCode; error?: Error }) => void): void {
    const payload: CollectorSpan[] = spans.map((span) => ({ traceId: span.spanContext().traceId, spanId: span.spanContext().spanId, parentSpanId: span.parentSpanContext?.spanId ?? null, serviceName: this.serviceName, operation: span.name, kind: span.kind === SpanKind.SERVER ? "server" : "client", startedAt: new Date(span.startTime[0] * 1e3 + span.startTime[1] / 1e6).toISOString(), endedAt: new Date(span.endTime[0] * 1e3 + span.endTime[1] / 1e6).toISOString(), durationMs: (span.endTime[0] - span.startTime[0]) * 1e3 + (span.endTime[1] - span.startTime[1]) / 1e6, statusCode: span.status.code === SpanStatusCode.ERROR ? 599 : 200 }));
    void fetch(`${this.collectorUrl}/v1/spans/batch`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ spans: payload }) }).then((response) => done(response.ok ? { code: ExportResultCode.SUCCESS } : { code: ExportResultCode.FAILED })).catch((error: unknown) => done({ code: ExportResultCode.FAILED, error: error instanceof Error ? error : new Error(String(error)) }));
  }
  shutdown(): Promise<void> { return Promise.resolve(); }
}

export class ConventionalOtelTracer {
  private readonly tracer;
  constructor(private readonly serviceName: string, collectorUrl: string) {
    const provider = new NodeTracerProvider({ spanProcessors: [new BatchSpanProcessor(new JsonBatchExporter(collectorUrl, serviceName), { scheduledDelayMillis: Number(process.env.OTEL_BSP_SCHEDULE_DELAY_MS ?? 200), maxExportBatchSize: Number(process.env.OTEL_BSP_MAX_EXPORT_BATCH_SIZE ?? 200) })] });
    provider.register(); this.tracer = trace.getTracer(`benchmark.${serviceName}`);
  }
  beginServerSpan(headers: Record<string, string | string[] | undefined>, operation: string): OtelActiveTrace { const extracted = propagation.extract(context.active(), headers, { get: (carrier, key) => carrier[key] }); const span = this.tracer.startSpan(operation, { kind: SpanKind.SERVER }, extracted); return { span, context: trace.setSpan(extracted, span) }; }
  run<T>(active: OtelActiveTrace, execute: () => Promise<T>): Promise<T> { return context.with(active.context, execute); }
  endServerSpan(active: OtelActiveTrace, statusCode: number): void { if (statusCode >= 500) active.span.setStatus({ code: SpanStatusCode.ERROR }); active.span.end(); }
  async fetch(parent: Context, operation: string, url: string): Promise<Response> { const span = this.tracer.startSpan(operation, { kind: SpanKind.CLIENT }, parent); const child = trace.setSpan(parent, span); const headers: Record<string, string> = {}; propagation.inject(child, headers); try { const response = await fetch(url, { headers }); if (!response.ok) span.setStatus({ code: SpanStatusCode.ERROR }); return response; } catch (error) { span.setStatus({ code: SpanStatusCode.ERROR }); throw error; } finally { span.end(); } }
}
