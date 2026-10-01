import type { Availability, Product, Quote } from "../domain/types.js";
import { executeControlledWorkload, type WorkloadExecution } from "../workload/executor.js";
import type { WorkloadProfile } from "../workload/profiles.js";
import { fetchAvailability, fetchProduct } from "./http-client.js";
import { withProposedSpan } from "../tracing/proposed.js";
import { observeBusiness } from "../observability/internal.js";

export async function createQuote(product: Product, availability: Availability, quantity: number, workload?: WorkloadExecution): Promise<Quote> {
  return observeBusiness("service", () => withProposedSpan("gateway.service.createQuote", "service", async () => {
    if (!availability.available) throw new Error("INSUFFICIENT_STOCK");
    return { product, availability, quantity, subtotal: product.unitPrice * quantity, ...(workload ? { workload } : {}) };
  }));
}

export async function buildQuote(
  catalogUrl: string, inventoryUrl: string, productId: string, quantity: number,
  workloadProfile: WorkloadProfile | undefined, conventionalTrace?: Parameters<typeof fetchProduct>[2]
): Promise<Quote> {
  return observeBusiness("service", () => withProposedSpan("gateway.service.buildQuote", "service", async () => {
    const product = await fetchProduct(catalogUrl, productId, conventionalTrace);
    const availability = await fetchAvailability(inventoryUrl, productId, quantity, conventionalTrace);
    const workload: WorkloadExecution | undefined = workloadProfile ? await executeControlledWorkload(workloadProfile) : undefined;
    return createQuote(product, availability, quantity, workload);
  }));
}
