import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import { appendFile, mkdir, readFile } from "node:fs/promises";
import { dirname } from "node:path";

export type TraceLayer = "handler" | "service" | "repository";
export type ExecutionRecord = {
  recordId: string; traceId: string; invocationId: string; parentInvocationId: string | null;
  serviceName: string; functionName: string; layer: TraceLayer;
  startedAt: string; endedAt: string; durationMs: number; outcome: "ok" | "error";
  attributes?: Record<string, string | number | boolean>; events?: Array<{ name: string; at: string; attributes?: Record<string, string | number | boolean> }>;
};
type Context = { traceId: string; currentInvocationId: string | null; attributes: Record<string, string | number | boolean>; events: NonNullable<ExecutionRecord["events"]> };
type WalLine = { type: "record"; record: ExecutionRecord } | { type: "ack"; recordId: string };

const mode = process.env.TRACING_MODE;
const enabled = mode === "proposed-memory" || mode === "proposed-durable";
const durable = mode === "proposed-durable";
const serviceName = process.env.SERVICE_ROLE ?? "gateway";
const queueUrl = process.env.TRACE_QUEUE_URL ?? "http://localhost:4319";
const capacity = Math.max(1, Number(process.env.TRACE_LOCAL_BUFFER_CAPACITY ?? 10_000));
const batchSize = Math.max(1, Number(process.env.TRACE_BATCH_SIZE ?? 200));
const flushMs = Math.max(1, Number(process.env.TRACE_FLUSH_INTERVAL_MS ?? 100));
const walPath = process.env.TRACE_WAL_PATH ?? `/tmp/${serviceName}-trace.wal`;
const storage = new AsyncLocalStorage<Context>();

class AsyncPublisher {
  private queued: ExecutionRecord[] = [];
  private pendingCommit: ExecutionRecord[] = [];
  private flushing = false;
  private committing = false;
  private recovered = false;
  private readonly timer = setInterval(() => void this.flush(), flushMs);

  constructor() { this.timer.unref(); }

  async recover(): Promise<void> {
    if (!durable || this.recovered) return;
    this.recovered = true;
    try {
      const text = await readFile(walPath, "utf8");
      const records = new Map<string, ExecutionRecord>();
      for (const line of text.split(/\r?\n/)) {
        if (!line) continue;
        try { const item = JSON.parse(line) as WalLine; if (item.type === "record") records.set(item.record.recordId, item.record); else records.delete(item.recordId); } catch { /* retain readable records; corrupt lines are ignored */ }
      }
      this.queued.push(...records.values());
    } catch (error: unknown) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
  }

  capture(record: ExecutionRecord): boolean {
    if (this.queued.length + this.pendingCommit.length >= capacity) return false;
    if (durable) this.pendingCommit.push(record); else this.queued.push(record);
    return true;
  }

  private async commitPending(): Promise<void> {
    if (!durable || this.committing || !this.pendingCommit.length) return;
    this.committing = true;
    const batch = this.pendingCommit.splice(0, this.pendingCommit.length);
    try {
      await mkdir(dirname(walPath), { recursive: true });
      await appendFile(walPath, batch.map((record) => JSON.stringify({ type: "record", record })).join("\n") + "\n", "utf8");
      this.queued.push(...batch);
    } catch {
      this.pendingCommit.unshift(...batch);
    } finally { this.committing = false; }
  }

  async flush(): Promise<void> {
    await this.recover();
    await this.commitPending();
    if (this.flushing || !this.queued.length) return;
    this.flushing = true;
    const batch = this.queued.splice(0, batchSize);
    try {
      const response = await fetch(`${queueUrl}/v2/records`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ records: batch }) });
      if (!response.ok) throw new Error(`queue returned ${response.status}`);
      if (durable) await appendFile(walPath, batch.map((record) => JSON.stringify({ type: "ack", recordId: record.recordId })).join("\n") + "\n", "utf8");
    } catch {
      this.queued.unshift(...batch);
    } finally { this.flushing = false; }
  }
}

const publisher = new AsyncPublisher();

function header(value: string | string[] | undefined): string | undefined { return Array.isArray(value) ? value[0] : value; }
export function isAsyncExecutionTracing(): boolean { return enabled; }
export function executionTraceHeaders(): Record<string, string> | undefined { const context = storage.getStore(); return enabled && context?.currentInvocationId ? { "x-trace-id": context.traceId, "x-parent-span-id": context.currentInvocationId } : undefined; }
export function currentExecutionTraceId(): string | undefined { return enabled ? storage.getStore()?.traceId : undefined; }

export async function withExecutionRecord<T>(functionName: string, layer: TraceLayer, execute: () => Promise<T>): Promise<T> {
  if (!enabled) return execute();
  const parent = storage.getStore(); if (!parent) return execute();
  const invocationId = randomUUID(); const started = performance.now(); const startedAt = new Date().toISOString();
  const context: Context = { traceId: parent.traceId, currentInvocationId: invocationId, attributes: {}, events: [] };
  let outcome: ExecutionRecord["outcome"] = "ok";
  try { return await storage.run(context, execute); }
  catch (error) { outcome = "error"; throw error; }
  finally {
    const record: ExecutionRecord = { recordId: randomUUID(), traceId: parent.traceId, invocationId, parentInvocationId: parent.currentInvocationId, serviceName, functionName, layer, startedAt, endedAt: new Date().toISOString(), durationMs: performance.now() - started, outcome, ...(Object.keys(context.attributes).length ? { attributes: context.attributes } : {}), ...(context.events.length ? { events: context.events } : {}) };
    publisher.capture(record); // Local admission is deliberately bounded and synchronous; transport stays asynchronous.
  }
}

export function addTraceAttribute(key: string, value: string | number | boolean): void { const context = storage.getStore(); if (enabled && context) context.attributes[key] = value; }
export function addTraceEvent(name: string, attributes?: Record<string, string | number | boolean>): void { const context = storage.getStore(); if (enabled && context) context.events.push({ name, at: new Date().toISOString(), ...(attributes ? { attributes } : {}) }); }
export async function runExecutionRequest<T>(headers: Record<string, string | string[] | undefined>, operation: string, execute: () => Promise<T>): Promise<T> {
  if (!enabled) return execute();
  await publisher.recover();
  return storage.run({ traceId: header(headers["x-trace-id"]) ?? randomUUID(), currentInvocationId: header(headers["x-parent-span-id"]) ?? null, attributes: {}, events: [] }, () => withExecutionRecord(operation, "handler", execute));
}
