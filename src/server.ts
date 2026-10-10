import Fastify from "fastify";
import { performance } from "node:perf_hooks";
import { getProduct } from "./catalog/service.js";
import { buildQuote } from "./gateway/service.js";
import { checkAvailability } from "./inventory/service.js";
import { createWorkloadProfile, getWorkloadProfile, type ComputationalSignature, type StructuralSignature } from "./workload/profiles.js";
import { ConventionalTracer, type ActiveServerTrace, type TraceSpan } from "./tracing/conventional.js";
import { currentProposedTraceId, runProposedRequest, type TraceEvent } from "./tracing/proposed.js";
import { BoundedEventQueue } from "./tracing/queue.js";
import { ReconstructionStore } from "./tracing/reconstruction.js";
import { BoundedExecutionQueue } from "./tracing/execution-queue.js";
import { ExecutionReconstructionStore } from "./tracing/execution-reconstruction.js";
import type { ExecutionRecord } from "./tracing/async-execution.js";
import { internalMetrics, observeBusiness, observeRequest, observeTiming, increment, resetInternalMetrics } from "./observability/internal.js";

declare module "fastify" {
  interface FastifyRequest { conventionalTrace?: ActiveServerTrace; }
}

const role = process.env.SERVICE_ROLE ?? "gateway";
const port = Number(process.env.PORT ?? 3000);
const catalogUrl = process.env.CATALOG_URL ?? "http://localhost:3001";
const inventoryUrl = process.env.INVENTORY_URL ?? "http://localhost:3002";
const app = Fastify({ logger: true });
const tracingMode = process.env.TRACING_MODE ?? "off";
const tracer = tracingMode === "conventional" && role !== "trace-collector"
  ? new ConventionalTracer(role, process.env.TRACE_COLLECTOR_URL ?? "http://localhost:4318")
  : undefined;

if (tracer) {
  app.addHook("onRequest", async (request, reply) => {
    request.conventionalTrace = tracer.beginServerSpan(request.headers, `${request.method} ${request.routeOptions.url}`);
    reply.header("x-trace-id", request.conventionalTrace.context.traceId);
  });
  app.addHook("onResponse", async (request, reply) => {
    if (request.conventionalTrace) await tracer.endServerSpan(request.conventionalTrace, reply.statusCode);
  });
}

app.get("/health", async () => ({ status: "ok", role }));
app.get("/v1/internal-observability", async () => ({ role, ...internalMetrics() }));
app.post("/v1/internal-observability/reset", async () => { resetInternalMetrics(); return { reset: true, role }; });

if (role === "trace-event-queue") {
  const queue = new BoundedEventQueue(Number(process.env.TRACE_QUEUE_CAPACITY ?? 10000));
  const executionQueue = new BoundedExecutionQueue(Number(process.env.TRACE_QUEUE_CAPACITY ?? 10000));
  app.post<{ Body: TraceEvent }>("/v1/events", async (request, reply) => {
    return queue.enqueue(request.body) ? reply.code(202).send({ accepted: true }) : reply.code(429).send({ accepted: false, reason: "queue_full" });
  });
  app.post<{ Body: { limit?: number } }>("/v1/events/dequeue", async (request) => ({ events: queue.dequeue(request.body?.limit ?? 100) }));
  app.get("/v1/metrics", async () => queue.metrics());
  app.post<{ Body: { records?: ExecutionRecord[] } }>("/v2/records", async (request, reply) => {
    const result = executionQueue.enqueue(request.body?.records ?? []);
    return reply.code(result.dropped ? 429 : 202).send(result);
  });
  app.post<{ Body: { limit?: number } }>("/v2/records/dequeue", async (request) => ({ records: executionQueue.dequeue(request.body?.limit ?? 100) }));
  app.get("/v2/metrics", async () => executionQueue.metrics());
} else if (role === "trace-reconstruction-worker") {
  const store = new ReconstructionStore();
  const executionStore = new ExecutionReconstructionStore();
  app.get("/v1/metrics", async () => store.summary());
  app.get("/v2/metrics", async () => executionStore.summary());
  app.get<{ Params: { traceId: string } }>("/v2/traces/:traceId", async (request, reply) => executionStore.get(request.params.traceId) ?? reply.code(404).send({ error: "trace_not_found" }));
  app.get<{ Params: { requestId: string } }>("/v1/traces/:requestId", async (request, reply) => {
    const trace = store.get(request.params.requestId);
    return trace ? trace : reply.code(404).send({ error: "trace_not_found" });
  });
  const workerQueueUrl = process.env.TRACE_QUEUE_URL ?? "http://trace-event-queue:3000";
  const workerIntervalMs = Number(process.env.TRACE_WORKER_POLL_INTERVAL_MS ?? 100);
  let lastPollFinished = performance.now();
  const poll = async (): Promise<void> => {
    const pollStarted = performance.now();
    observeTiming("WORKER", "idle_time_ms", pollStarted - lastPollFinished);
    try {
      const response = await fetch(`${workerQueueUrl}/v1/events/dequeue`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ limit: 200 }) });
      if (response.ok) {
        const events = (await response.json() as { events: TraceEvent[] }).events;
        increment("WORKER", "batches"); increment("WORKER", "events_dequeued", events.length);
        const processingStarted = performance.now(); store.accept(events);
        observeTiming("WORKER", "processing_duration_ms", performance.now() - processingStarted);
        observeTiming("WORKER", "batch_duration_ms", performance.now() - pollStarted); increment("WORKER", "events_processed", events.length);
      }
      const executionResponse = await fetch(`${workerQueueUrl}/v2/records/dequeue`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ limit: 200 }) });
      if (executionResponse.ok) executionStore.accept((await executionResponse.json() as { records: ExecutionRecord[] }).records);
    } catch { /* queue not ready; next poll retries */ }
    finally { lastPollFinished = performance.now(); }
  };
  setInterval(() => void poll(), workerIntervalMs);
} else if (role === "trace-collector") {
  const spans: TraceSpan[] = [];
  app.post<{ Body: TraceSpan }>("/v1/spans", async (request, reply) => {
    const started = performance.now(); spans.push(request.body); increment("COLLECTOR", "spans_received"); observeTiming("COLLECTOR", "processing_duration_ms", performance.now() - started);
    return reply.code(202).send({ accepted: true });
  });
  app.get<{ Params: { traceId: string } }>("/v1/traces/:traceId", async (request) => {
    return { traceId: request.params.traceId, spans: spans.filter((span) => span.traceId === request.params.traceId) };
  });
  app.get("/v1/spans", async () => ({ count: spans.length, spans }));
  app.get("/v1/metrics", async () => {
    const started = performance.now(); const byTrace = new Map<string, TraceSpan[]>();
    for (const span of spans) byTrace.set(span.traceId, [...(byTrace.get(span.traceId) ?? []), span]);
    const completeTraces = [...byTrace.values()].filter((trace) => trace.some((span) => span.kind === "server")).length;
    const reconstructionDurationMs = performance.now() - started;
    observeTiming("RECONSTRUCTION", "duration_ms", reconstructionDurationMs); increment("RECONSTRUCTION", "attempts", byTrace.size); increment("RECONSTRUCTION", "success", completeTraces); increment("RECONSTRUCTION", "failed", byTrace.size - completeTraces);
    return { spansCreated: spans.length, spansReceived: spans.length, spansProcessed: spans.length, observedTraces: byTrace.size, completeTraces, reconstructionDurationMs };
  });
} else if (role === "catalog") {
  app.get<{ Params: { productId: string } }>("/products/:productId", async (request, reply) => observeRequest("catalog.handler.getProduct", () => runProposedRequest(request.headers, "catalog.handler.getProduct", () => observeBusiness("handler", async () => {
    const traceId = currentProposedTraceId(); if (traceId) reply.header("x-trace-id", traceId);
    try { return await getProduct(request.params.productId); }
    catch { return reply.code(404).send({ error: "product_not_found" }); }
  }))));
} else if (role === "inventory") {
  app.get<{ Params: { productId: string }; Querystring: { quantity?: string } }>("/availability/:productId", async (request, reply) => {
    return observeRequest("inventory.handler.checkAvailability", () => runProposedRequest(request.headers, "inventory.handler.checkAvailability", () => observeBusiness("handler", async () => {
      const traceId = currentProposedTraceId(); if (traceId) reply.header("x-trace-id", traceId);
      const quantity = Number(request.query.quantity);
      if (!Number.isInteger(quantity) || quantity < 1) return reply.code(400).send({ error: "quantity_must_be_a_positive_integer" });
      try { return await checkAvailability(request.params.productId, quantity); }
      catch { return reply.code(404).send({ error: "product_not_found" }); }
    })));
  });
} else if (role === "gateway") {
  app.get<{ Querystring: { productId?: string; quantity?: string; profile?: string; compute?: ComputationalSignature; structure?: StructuralSignature; inputSize?: string } }>("/api/v1/quote", async (request, reply) => {
    return observeRequest("gateway.handler.quote", () => runProposedRequest(request.headers, "gateway.handler.quote", () => observeBusiness("handler", async () => {
      const traceId = currentProposedTraceId(); if (traceId) reply.header("x-trace-id", traceId);
      const productId = request.query.productId;
      const quantity = Number(request.query.quantity);
      if (!productId || !Number.isInteger(quantity) || quantity < 1) return reply.code(400).send({ error: "productId_and_positive_integer_quantity_are_required" });
      const hasExplicitWorkload = request.query.compute || request.query.structure || request.query.inputSize;
      const profile = hasExplicitWorkload
        ? createWorkloadProfile(request.query.compute!, request.query.structure!, Number(request.query.inputSize))
        : request.query.profile ? getWorkloadProfile(request.query.profile) : undefined;
      if (hasExplicitWorkload && (!request.query.compute || !request.query.structure || !request.query.inputSize)) return reply.code(400).send({ error: "compute_structure_and_inputSize_must_be_provided_together" });
      if (request.query.profile && !profile) return reply.code(400).send({ error: "unknown_workload_profile" });
      if (hasExplicitWorkload && !profile) return reply.code(400).send({ error: "invalid_workload_parameters" });
      try {
        const trace = tracer && request.conventionalTrace ? { tracer, active: request.conventionalTrace } : undefined;
        // Business Processing Time starts when the gateway invokes business logic
        // and ends when the quote is ready. It excludes HTTP parsing/validation
        // and client-to-gateway transfer, while retaining tracing overhead that
        // occurs inside the comparable business path for each condition.
        const businessStarted = performance.now();
        const quote = await observeBusiness("service", () => buildQuote(catalogUrl, inventoryUrl, productId, quantity, profile, trace), true);
        reply.header("x-business-processing-ms", (performance.now() - businessStarted).toFixed(3));
        return quote;
      } catch (error) {
        const message = error instanceof Error ? error.message : "REQUEST_FAILED";
        return reply.code(message === "INSUFFICIENT_STOCK" ? 409 : 502).send({ error: message.toLowerCase() });
      }
    })));
  });
} else {
  throw new Error(`Unsupported SERVICE_ROLE: ${role}`);
}

await app.listen({ host: "0.0.0.0", port });
