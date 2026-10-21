import type { MatchingResult, Personalization } from "../domain/types.js";
import type { ActiveServerTrace, ConventionalTracer } from "../tracing/conventional.js";
import type { OtelActiveTrace, ConventionalOtelTracer } from "../tracing/conventional-otel.js";
import { proposedTraceHeaders, withApplicationSpan } from "../tracing/proposed.js";

export type ConventionalRequestTrace = { tracer: ConventionalTracer; active: ActiveServerTrace } | { tracer: ConventionalOtelTracer; active: OtelActiveTrace };

async function getJson<T>(url: string, operation: string, trace?: ConventionalRequestTrace): Promise<T> {
  const response = trace
    ? await trace.tracer.fetch(trace.active.context, operation, url)
    : await fetch(url, { headers: proposedTraceHeaders() });
  if (!response.ok) throw new Error(`UPSTREAM_${response.status}`);
  return response.json() as Promise<T>;
}

/** The single inter-service boundary shared by OTel and proposed tracing. */
export function fetchMatchingData(matchingUrl: string, productId: string, quantity: number, personalization: Personalization, trace?: ConventionalRequestTrace): Promise<MatchingResult> {
  const url = `${matchingUrl}/matches/${encodeURIComponent(productId)}?quantity=${quantity}&segment=${encodeURIComponent(personalization.customerSegment)}`;
  if (trace) return getJson<MatchingResult>(url, "personalization.rpc.matching", trace);
  return withApplicationSpan("personalization.rpc.matching", "rpc", () => getJson<MatchingResult>(url, "personalization.rpc.matching"));
}
