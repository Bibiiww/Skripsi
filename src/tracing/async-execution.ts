import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import { createReadStream } from "node:fs";
import { mkdir, open, rename, stat, unlink } from "node:fs/promises";
import { createInterface } from "node:readline";
import { dirname } from "node:path";

export type TraceLayer = "handler" | "service" | "repository";
export type ExecutionRecord = {
  recordId: string;
  traceId: string;
  invocationId: string;
  parentInvocationId: string | null;
  serviceName: string;
  functionName: string;
  layer: TraceLayer;
  startedAt: string;
  endedAt: string;
  durationMs: number;
  outcome: "ok" | "error";
  /**
   * Contract for one request's selected function boundaries. It is propagated
   * with the trace context so the worker can distinguish a complete trace from
   * a structurally valid but incomplete subset.
   */
  expectedRecordCount?: number;
  attributes?: Record<string, string | number | boolean>;
  events?: Array<{
    name: string;
    at: string;
    attributes?: Record<string, string | number | boolean>;
  }>;
};
type Context = {
  traceId: string;
  currentInvocationId: string | null;
  attributes: Record<string, string | number | boolean>;
  events: NonNullable<ExecutionRecord["events"]>;
  expectedRecordCount?: number;
};
type WalLine =
  | { type: "record"; record: ExecutionRecord }
  | { type: "ack"; recordId: string };
type WalReadResult = {
  records: Map<string, ExecutionRecord>;
  malformedLines: number;
};

const mode = process.env.TRACING_MODE;
const enabled = mode === "proposed-memory" || mode === "proposed-durable";
const durable = mode === "proposed-durable";
const serviceName = process.env.SERVICE_ROLE ?? "gateway";
const queueUrl = process.env.TRACE_QUEUE_URL ?? "http://localhost:4319";
const capacity = Math.max(1, Number(process.env.TRACE_LOCAL_BUFFER_CAPACITY ?? 10_000));
const batchSize = Math.max(1, Number(process.env.TRACE_BATCH_SIZE ?? 200));
const flushMs = Math.max(1, Number(process.env.TRACE_FLUSH_INTERVAL_MS ?? 100));
const walPath = process.env.TRACE_WAL_PATH ?? `/tmp/${serviceName}-trace.wal`;
const walCompactBytes = Math.max(
  1024 * 1024,
  Number(process.env.TRACE_WAL_COMPACT_BYTES ?? 64 * 1024 * 1024),
);
const storage = new AsyncLocalStorage<Context>();

class AsyncPublisher {
  private queued: ExecutionRecord[] = [];
  private pendingCommit: ExecutionRecord[] = [];
  private flushing = false;
  private committing = false;
  private inFlightCount = 0;
  private committingCount = 0;
  private recoveryPromise: Promise<void> | undefined;
  private walTail: Promise<void> = Promise.resolve();
  private nextCompactionSize = walCompactBytes;

  private readonly timer = setInterval(() => {
    void this.flush().catch((error: unknown) => {
      console.error("[tracing] asynchronous flush failed", error);
    });
  }, flushMs);

  constructor() {
    this.timer.unref();
  }

  /** Serialize every WAL read-modify-replace operation with appends and ACKs. */
  private serializeWal<T>(operation: () => Promise<T>): Promise<T> {
    const result = this.walTail.then(operation);
    this.walTail = result.then(() => undefined, () => undefined);
    return result;
  }

  /** Stream the WAL. A malformed line prevents compaction to avoid data loss. */
  private async readActiveWal(): Promise<WalReadResult> {
    const records = new Map<string, ExecutionRecord>();
    let input: ReturnType<typeof createReadStream> | undefined;
    let malformedLines = 0;

    try {
      input = createReadStream(walPath, { encoding: "utf8" });
      const lines = createInterface({ input, crlfDelay: Infinity });
      try {
        for await (const line of lines) {
          if (!line) continue;
          try {
            const item = JSON.parse(line) as {
              type?: unknown;
              record?: ExecutionRecord;
              recordId?: unknown;
            };
            if (
              item.type === "record" &&
              item.record &&
              typeof item.record.recordId === "string"
            ) {
              records.set(item.record.recordId, item.record);
            } else if (
              item.type === "ack" &&
              typeof item.recordId === "string"
            ) {
              records.delete(item.recordId);
            } else {
              malformedLines++;
            }
          } catch {
            malformedLines++;
          }
        }
      } finally {
        lines.close();
      }
    } catch (error: unknown) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    } finally {
      input?.destroy();
    }

    if (malformedLines > 0) {
      console.warn(
        `[tracing] found ${malformedLines} malformed WAL line(s); compaction skipped`,
      );
    }
    return { records, malformedLines };
  }

  /** Caller must hold the WAL serialization queue. */
  private async writeCompactedWal(
    records: Map<string, ExecutionRecord>,
    originalSize: number,
  ): Promise<boolean> {
    const tempPath = `${walPath}.compact-${process.pid}-${randomUUID()}`;
    let handle: Awaited<ReturnType<typeof open>> | undefined;
    let tempSize = 0;
    let renamed = false;

    try {
      await mkdir(dirname(walPath), { recursive: true });
      handle = await open(tempPath, "wx");
      let chunk = "";

      const writeChunk = async (): Promise<void> => {
        if (!chunk) return;
        const data = chunk;
        chunk = "";
        await handle!.writeFile(data, "utf8");
        tempSize += Buffer.byteLength(data, "utf8");
      };

      for (const record of records.values()) {
        chunk += `${JSON.stringify({ type: "record", record } satisfies WalLine)}\n`;
        if (Buffer.byteLength(chunk, "utf8") >= 1024 * 1024) {
          await writeChunk();
        }
      }
      await writeChunk();
      await handle.sync();
      await handle.close();
      handle = undefined;

      // Avoid replacing the WAL if the snapshot does not shrink it enough.
      if (originalSize > 0 && tempSize >= originalSize * 0.75) {
        await unlink(tempPath);
        return false;
      }

      // Same-directory rename is atomic on the Linux benchmark filesystem.
      await rename(tempPath, walPath);
      renamed = true;

      try {
        const directory = await open(dirname(walPath), "r");
        try {
          await directory.sync();
        } finally {
          await directory.close();
        }
      } catch (error: unknown) {
        console.warn("[tracing] WAL directory sync failed after compaction", error);
      }
      return true;
    } finally {
      if (handle) await handle.close().catch(() => undefined);
      if (!renamed) await unlink(tempPath).catch(() => undefined);
    }
  }

  /** Must be called only from within serializeWal(). */
  private async maybeCompactWalLocked(): Promise<void> {
    let walSize: number;
    try {
      walSize = (await stat(walPath)).size;
    } catch (error: unknown) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
      throw error;
    }
    if (walSize < this.nextCompactionSize) return;

    const { records, malformedLines } = await this.readActiveWal();
    if (malformedLines > 0) {
      this.nextCompactionSize = Math.max(walCompactBytes, walSize * 2);
      return;
    }

    const compacted = await this.writeCompactedWal(records, walSize);
    this.nextCompactionSize = compacted
      ? walCompactBytes
      : Math.max(walCompactBytes, walSize * 2);
  }

  private async appendWalLines(lines: WalLine[]): Promise<void> {
    if (!durable || lines.length === 0) return;
    const content = `${lines.map((line) => JSON.stringify(line)).join("\n")}\n`;

    await this.serializeWal(async () => {
      await mkdir(dirname(walPath), { recursive: true });
      const handle = await open(walPath, "a");
      try {
        await handle.writeFile(content, "utf8");
        await handle.sync();
      } finally {
        await handle.close();
      }
      try {
        await this.maybeCompactWalLocked();
      } catch (error: unknown) {
        console.warn("[tracing] WAL compaction failed; retaining appended WAL", error);
      }
    });
  }

  async recover(): Promise<void> {
    if (!durable) return;
    if (!this.recoveryPromise) {
      this.recoveryPromise = this.serializeWal(async () => {
        // Reading and optional replacement happen under the same lock as writes.
        const { records, malformedLines } = await this.readActiveWal();
        let walSize = 0;
        try {
          walSize = (await stat(walPath)).size;
        } catch (error: unknown) {
          if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
        }

        if (walSize >= walCompactBytes) {
          if (malformedLines > 0) {
            this.nextCompactionSize = Math.max(walCompactBytes, walSize * 2);
          } else {
            try {
              const compacted = await this.writeCompactedWal(records, walSize);
              this.nextCompactionSize = compacted
                ? walCompactBytes
                : Math.max(walCompactBytes, walSize * 2);
            } catch (error: unknown) {
              console.warn("[tracing] startup WAL compaction failed; retaining WAL", error);
              this.nextCompactionSize = Math.max(walCompactBytes, walSize * 2);
            }
          }
        }

        // Do this last so a retry cannot enqueue the same recovered records twice.
        for (const record of records.values()) this.queued.push(record);
      }).catch((error: unknown) => {
        this.recoveryPromise = undefined;
        throw error;
      });
    }
    await this.recoveryPromise;
  }

  capture(record: ExecutionRecord): boolean {
    const occupied =
      this.queued.length +
      this.pendingCommit.length +
      this.committingCount +
      this.inFlightCount;
    if (occupied >= capacity) return false;
    if (durable) this.pendingCommit.push(record);
    else this.queued.push(record);
    return true;
  }

  private async commitPending(): Promise<void> {
    if (!durable || this.committing || this.pendingCommit.length === 0) return;
    this.committing = true;
    const batch = this.pendingCommit.splice(0, this.pendingCommit.length);
    this.committingCount = batch.length;
    try {
      await this.appendWalLines(batch.map((record) => ({ type: "record", record } satisfies WalLine)));
      for (const record of batch) this.queued.push(record);
    } catch (error: unknown) {
      this.pendingCommit = batch.concat(this.pendingCommit);
      console.error("[tracing] failed to persist pending records to WAL", error);
    } finally {
      this.committingCount = 0;
      this.committing = false;
    }
  }

  async flush(): Promise<void> {
    await this.recover();
    await this.commitPending();
    if (this.flushing || this.queued.length === 0) return;

    this.flushing = true;
    const batch = this.queued.splice(0, batchSize);
    this.inFlightCount = batch.length;
    try {
      const response = await fetch(`${queueUrl}/v2/records`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ records: batch }),
      });
      if (!response.ok) throw new Error(`queue returned ${response.status}`);

      if (durable) {
        await this.appendWalLines(
          batch.map((record) => ({ type: "ack", recordId: record.recordId } satisfies WalLine)),
        );
      }
    } catch (error: unknown) {
      // Delivery may have succeeded before ACK persistence failed: at-least-once.
      this.queued = batch.concat(this.queued);
      console.error("[tracing] batch delivery or acknowledgement failed", error);
    } finally {
      this.inFlightCount = 0;
      this.flushing = false;
    }
  }
}

const publisher = new AsyncPublisher();

function header(value: string | string[] | undefined): string | undefined {
  return Array.isArray(value) ? value[0] : value;
}

export function isAsyncExecutionTracing(): boolean {
  return enabled;
}

export function executionTraceHeaders(): Record<string, string> | undefined {
  const context = storage.getStore();
  return enabled && context?.currentInvocationId
    ? {
        "x-trace-id": context.traceId,
        "x-parent-span-id": context.currentInvocationId,
        ...(context.expectedRecordCount === undefined
          ? {}
          : { "x-trace-expected-records": String(context.expectedRecordCount) }),
      }
    : undefined;
}

export function currentExecutionTraceId(): string | undefined {
  return enabled ? storage.getStore()?.traceId : undefined;
}

/** Records the workload-specific boundary contract without changing execution. */
export function setExpectedExecutionRecordCount(count: number): void {
  const context = storage.getStore();
  if (enabled && context && Number.isInteger(count) && count > 0) {
    context.expectedRecordCount = count;
  }
}

export async function withExecutionRecord<T>(
  functionName: string,
  layer: TraceLayer,
  execute: () => Promise<T>,
): Promise<T> {
  if (!enabled) return execute();
  const parent = storage.getStore();
  if (!parent) return execute();

  const invocationId = randomUUID();
  const started = performance.now();
  const startedAt = new Date().toISOString();
  const context: Context = {
    traceId: parent.traceId,
    currentInvocationId: invocationId,
    attributes: {},
    events: [],
    expectedRecordCount: parent.expectedRecordCount,
  };
  let outcome: ExecutionRecord["outcome"] = "ok";

  try {
    return await storage.run(context, execute);
  } catch (error) {
    outcome = "error";
    throw error;
  } finally {
    const record: ExecutionRecord = {
      recordId: randomUUID(),
      traceId: parent.traceId,
      invocationId,
      parentInvocationId: parent.currentInvocationId,
      serviceName,
      functionName,
      layer,
      startedAt,
      endedAt: new Date().toISOString(),
      durationMs: performance.now() - started,
      outcome,
      ...(context.expectedRecordCount === undefined
        ? {}
        : { expectedRecordCount: context.expectedRecordCount }),
      ...(Object.keys(context.attributes).length
        ? { attributes: context.attributes }
        : {}),
      ...(context.events.length ? { events: context.events } : {}),
    };
    publisher.capture(record);
  }
}

export function addTraceAttribute(
  key: string,
  value: string | number | boolean,
): void {
  const context = storage.getStore();
  if (enabled && context) context.attributes[key] = value;
}

export function addTraceEvent(
  name: string,
  attributes?: Record<string, string | number | boolean>,
): void {
  const context = storage.getStore();
  if (enabled && context) {
    context.events.push({
      name,
      at: new Date().toISOString(),
      ...(attributes ? { attributes } : {}),
    });
  }
}

export async function runExecutionRequest<T>(
  headers: Record<string, string | string[] | undefined>,
  operation: string,
  execute: () => Promise<T>,
): Promise<T> {
  if (!enabled) return execute();
  await publisher.recover();
  return storage.run(
    {
      traceId: header(headers["x-trace-id"]) ?? randomUUID(),
      currentInvocationId: header(headers["x-parent-span-id"]) ?? null,
      attributes: {},
      events: [],
      expectedRecordCount: (() => {
        const raw = header(headers["x-trace-expected-records"]);
        const parsed = raw === undefined ? NaN : Number(raw);
        return Number.isInteger(parsed) && parsed > 0 ? parsed : undefined;
      })(),
    },
    () => withExecutionRecord(operation, "handler", execute),
  );
}
