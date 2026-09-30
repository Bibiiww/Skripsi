import type { TraceEvent } from "./proposed.js";

export class BoundedEventQueue {
  private readonly events: TraceEvent[] = [];
  private accepted = 0;
  private dropped = 0;
  private dequeued = 0;

  constructor(private readonly capacity: number) {}

  enqueue(event: TraceEvent): boolean {
    if (this.events.length >= this.capacity) { this.dropped += 1; return false; }
    this.events.push(event); this.accepted += 1; return true;
  }

  dequeue(limit: number): TraceEvent[] {
    const batch = this.events.splice(0, Math.max(1, Math.min(limit, this.events.length)));
    this.dequeued += batch.length;
    return batch;
  }

  metrics() {
    return { capacity: this.capacity, queued: this.events.length, accepted: this.accepted, dropped: this.dropped, dequeued: this.dequeued };
  }
}
