import type { MatchingResult, Personalization } from "../domain/types.js";
import { observeBusiness } from "../observability/internal.js";
import { withApplicationSpan } from "../tracing/proposed.js";
import { findMatchingData } from "./repository.js";

export async function resolveMatchingData(productId: string, quantity: number, _personalization: Personalization): Promise<MatchingResult> {
  return observeBusiness("service", () => withApplicationSpan("matching.service.resolve", "service", async () => {
    const result = await findMatchingData(productId, quantity);
    if (!result) throw new Error("PRODUCT_NOT_FOUND");
    return result;
  }));
}
