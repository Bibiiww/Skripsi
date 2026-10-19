import type { ExecutionRecord } from "./async-execution.js";

export class ExecutionReconstructionStore {
  private readonly recordsByTrace = new Map<string, Map<string, ExecutionRecord>>();
  private readonly expectedRecords: number;

  constructor(expectedRecords = Number(process.env.TRACE_EXPECTED_RECORDS_PER_SUCCESSFUL_TRACE ?? 9)) {
    this.expectedRecords = expectedRecords;
  }

  accept(records: ExecutionRecord[]) { for (const record of records) { const trace = this.recordsByTrace.get(record.traceId) ?? new Map<string, ExecutionRecord>(); trace.set(record.recordId, record); this.recordsByTrace.set(record.traceId, trace); } }
  private assessment(records: ExecutionRecord[]) {
    const invocationIds = new Set(records.map((record) => record.invocationId));
    const roots = records.filter((record) => record.parentInvocationId === null);
    const missingParents = records.filter((record) => record.parentInvocationId !== null && !invocationIds.has(record.parentInvocationId)).length;
    const byId = new Map(records.map((record) => [record.invocationId, record]));
    let cycle = false;
    for (const record of records) {
      const visited = new Set<string>(); let current: ExecutionRecord | undefined = record;
      while (current?.parentInvocationId) { if (visited.has(current.invocationId)) { cycle = true; break; } visited.add(current.invocationId); current = byId.get(current.parentInvocationId); }
      if (cycle) break;
    }
    const complete = records.length === this.expectedRecords && roots.length === 1 && missingParents === 0 && !cycle && roots[0]?.outcome === "ok";
    return { complete, roots: roots.length, missingParents, cycle, expectedRecords: this.expectedRecords, observedRecords: records.length };
  }
  summary() { const traces = [...this.recordsByTrace.entries()].map(([traceId, records]) => ({ traceId, records: [...records.values()], assessment: this.assessment([...records.values()]) })); const complete = traces.filter((trace) => trace.assessment.complete).length; return { observedTraces: traces.length, completeTraces: complete, incompleteTraces: traces.length - complete, reconstructedSpans: traces.reduce((total, trace) => total + trace.records.length, 0), expectedRecordsPerSuccessfulTrace: this.expectedRecords }; }
  get(traceId: string) { const records = this.recordsByTrace.get(traceId); return records ? { traceId, records: [...records.values()], assessment: this.assessment([...records.values()]) } : undefined; }
  list() { return [...this.recordsByTrace.entries()].map(([traceId, records]) => ({ traceId, ...this.assessment([...records.values()]) })); }
}
