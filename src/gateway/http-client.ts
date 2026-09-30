import type { Availability, Product } from "../domain/types.js";
import type { ActiveServerTrace, ConventionalTracer } from "../tracing/conventional.js";
import { proposedTraceHeaders } from "../tracing/proposed.js";

type ConventionalRequestTrace = { tracer: ConventionalTracer; active: ActiveServerTrace };

async function getJson<T>(url: string, operation: string, trace?: ConventionalRequestTrace): Promise<T> {
  const response = trace
    ? await trace.tracer.fetch(trace.active.context, operation, url)
    : await fetch(url, { headers: proposedTraceHeaders() });
  if (!response.ok) throw new Error(`UPSTREAM_${response.status}`);
  return response.json() as Promise<T>;
}

export function fetchProduct(catalogUrl: string, productId: string, trace?: ConventionalRequestTrace): Promise<Product> {
  return getJson<Product>(`${catalogUrl}/products/${encodeURIComponent(productId)}`, "GET catalog.product", trace);
}

export function fetchAvailability(inventoryUrl: string, productId: string, quantity: number, trace?: ConventionalRequestTrace): Promise<Availability> {
  return getJson<Availability>(`${inventoryUrl}/availability/${encodeURIComponent(productId)}?quantity=${quantity}`, "GET inventory.availability", trace);
}
