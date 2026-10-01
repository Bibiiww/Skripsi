import type { TraceEvent } from "./proposed.js";
import { performance } from "node:perf_hooks";
import { increment, observeTiming } from "../observability/internal.js";

export class BoundedEventQueue {
  private readonly events: TraceEvent[] = [];
  private accepted = 0;
  private dropped = 0;
  private dequeued = 0;
  private depthTotal = 0;
  private depthSamples = 0;
  private depthMax = 0;

  constructor(private readonly capacity: number) {}

  enqueue(event: TraceEvent): boolean {
    const started = performance.now();
    increment("QUEUE", "enqueue_attempts");
    if (this.events.length >= this.capacity) { this.dropped += 1; increment("QUEUE", "enqueue_dropped"); observeTiming("QUEUE", "enqueue_duration_ms", performance.now() - started); return false; }
    event.event_enqueued_at_ms = Date.now();
    this.events.push(event); this.accepted += 1; increment("QUEUE", "enqueue_success");
    this.observeDepth(); observeTiming("QUEUE", "enqueue_duration_ms", performance.now() - started); return true;
  }

  dequeue(limit: number): TraceEvent[] {
    const batch = this.events.splice(0, Math.max(1, Math.min(limit, this.events.length)));
    this.dequeued += batch.length;
    const now = Date.now();
    for (const event of batch) if (event.event_enqueued_at_ms) observeTiming("QUEUE", "wait_ms", now - event.event_enqueued_at_ms);
    increment("QUEUE", "events_dequeued", batch.length); this.observeDepth();
    return batch;
  }

  private observeDepth(): void { this.depthTotal += this.events.length; this.depthSamples += 1; this.depthMax = Math.max(this.depthMax, this.events.length); }

  metrics() {
    this.observeDepth();
    return { capacity: this.capacity, queued: this.events.length, accepted: this.accepted, dropped: this.dropped, dequeued: this.dequeued, queueDepthMean: this.depthSamples ? this.depthTotal / this.depthSamples : 0, queueDepthMax: this.depthMax, queueUtilization: this.capacity ? this.events.length / this.capacity : null, queueUtilizationMax: this.capacity ? this.depthMax / this.capacity : null };
  }
}
