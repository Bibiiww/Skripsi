import type { Availability, Product, Quote } from "../domain/types.js";
import { executeControlledWorkload, type WorkloadExecution } from "../workload/executor.js";
import type { WorkloadProfile } from "../workload/profiles.js";
import { fetchMatchingData, type ConventionalRequestTrace } from "./http-client.js";
import { withApplicationSpan } from "../tracing/proposed.js";
import { observeBusiness } from "../observability/internal.js";
import { loadPersonalization } from "./repository.js";

export async function createQuote(product: Product, availability: Availability, quantity: number, workload?: WorkloadExecution): Promise<Quote> {
  if (!availability.available) throw new Error("INSUFFICIENT_STOCK");
  return { product, availability, quantity, subtotal: product.unitPrice * quantity, ...(workload ? { workload } : {}) };
}

export async function buildQuote(
  matchingUrl: string, productId: string, quantity: number,
  workloadProfile: WorkloadProfile | undefined, conventionalTrace?: ConventionalRequestTrace
): Promise<Quote> {
  return observeBusiness("service", () => withApplicationSpan("personalization.service.buildQuote", "service", async () => {
    const personalization = await loadPersonalization(productId);
    const { product, availability } = await fetchMatchingData(matchingUrl, productId, quantity, personalization, conventionalTrace);
    const workload: WorkloadExecution | undefined = workloadProfile ? await executeControlledWorkload(workloadProfile) : undefined;
    return createQuote(product, availability, quantity, workload);
  }));
}
