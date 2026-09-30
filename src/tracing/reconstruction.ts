import type { TraceEvent } from "./proposed.js";

export type ReconstructedSpan = {
  spanId: string; parentSpanId: string | null; functionName: string; layer: TraceEvent["layer"]; serviceName: string;
  startTimestamp: string; endTimestamp: string; outcome: "ok" | "error";
};
export type ReconstructedTrace = {
  requestId: string; events: TraceEvent[]; nodes: ReconstructedSpan[];
  edges: Array<{ parentSpanId: string; spanId: string }>; complete: boolean; incompleteSpans: number;
};

export class ReconstructionStore {
  private readonly eventsByRequest = new Map<string, TraceEvent[]>();

  accept(events: TraceEvent[]): void {
    for (const event of events) this.eventsByRequest.set(event.request_id, [...(this.eventsByRequest.get(event.request_id) ?? []), event]);
  }

  get(requestId: string): ReconstructedTrace | undefined {
    const events = this.eventsByRequest.get(requestId);
    if (!events) return undefined;
    const pairs = new Map<string, { entry?: TraceEvent; exit?: TraceEvent }>();
    for (const event of events) {
      const pair = pairs.get(event.span_id) ?? {};
      if (event.event_type === "ENTRY") pair.entry = event; else pair.exit = event;
      pairs.set(event.span_id, pair);
    }
    const nodes: ReconstructedSpan[] = [];
    for (const [spanId, pair] of pairs) {
      if (pair.entry && pair.exit && pair.entry.start_timestamp && pair.exit.end_timestamp && pair.exit.outcome) {
        nodes.push({ spanId, parentSpanId: pair.entry.parent_span_id, functionName: pair.entry.function_name, layer: pair.entry.layer, serviceName: pair.entry.service_name, startTimestamp: pair.entry.start_timestamp, endTimestamp: pair.exit.end_timestamp, outcome: pair.exit.outcome });
      }
    }
    const ids = new Set(nodes.map((node) => node.spanId));
    const edges = nodes.filter((node) => node.parentSpanId && ids.has(node.parentSpanId)).map((node) => ({ parentSpanId: node.parentSpanId!, spanId: node.spanId }));
    const roots = nodes.filter((node) => node.parentSpanId === null);
    const incompleteSpans = pairs.size - nodes.length;
    const complete = roots.length === 1 && incompleteSpans === 0 && nodes.every((node) => node.parentSpanId === null || ids.has(node.parentSpanId));
    return { requestId, events, nodes, edges, complete, incompleteSpans };
  }

  summary() {
    const traces = [...this.eventsByRequest.keys()].map((requestId) => this.get(requestId)!);
    return {
      observedRequests: traces.length, completeTraces: traces.filter((trace) => trace.complete).length,
      reconstructedSpans: traces.reduce((total, trace) => total + trace.nodes.length, 0),
      events: traces.reduce((total, trace) => total + trace.events.length, 0),
      incompleteSpans: traces.reduce((total, trace) => total + trace.incompleteSpans, 0)
    };
  }
}
