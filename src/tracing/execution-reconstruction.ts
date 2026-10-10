import type { ExecutionRecord } from "./async-execution.js";

export class ExecutionReconstructionStore {
  private readonly recordsByTrace = new Map<string, Map<string, ExecutionRecord>>();
  accept(records: ExecutionRecord[]) { for (const record of records) { const trace = this.recordsByTrace.get(record.traceId) ?? new Map<string, ExecutionRecord>(); trace.set(record.recordId, record); this.recordsByTrace.set(record.traceId, trace); } }
  summary() { const traces = [...this.recordsByTrace.values()]; const complete = traces.filter((trace) => [...trace.values()].some((record) => record.parentInvocationId === null)).length; return { observedTraces: traces.length, completeTraces: complete, reconstructedSpans: traces.reduce((total, trace) => total + trace.size, 0) }; }
  get(traceId: string) { const records = this.recordsByTrace.get(traceId); return records ? { traceId, records: [...records.values()] } : undefined; }
}
