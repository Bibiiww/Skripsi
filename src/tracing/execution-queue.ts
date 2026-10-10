import type { ExecutionRecord } from "./async-execution.js";

export class BoundedExecutionQueue {
  private readonly records: ExecutionRecord[] = [];
  private readonly seen = new Set<string>();
  private accepted = 0; private dropped = 0; private duplicates = 0;
  constructor(private readonly capacity: number) {}
  enqueue(records: ExecutionRecord[]) {
    let accepted = 0; let dropped = 0;
    for (const record of records) {
      if (this.seen.has(record.recordId)) { this.duplicates += 1; continue; }
      if (this.records.length >= this.capacity) { this.dropped += 1; dropped += 1; continue; }
      this.seen.add(record.recordId); this.records.push(record); this.accepted += 1; accepted += 1;
    }
    return { accepted, dropped };
  }
  dequeue(limit: number) { return this.records.splice(0, Math.max(1, Math.min(limit, this.records.length))); }
  metrics() { return { capacity: this.capacity, queued: this.records.length, accepted: this.accepted, dropped: this.dropped, duplicates: this.duplicates, utilization: this.records.length / this.capacity }; }
}
