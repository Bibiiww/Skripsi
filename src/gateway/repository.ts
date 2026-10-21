import type { Personalization } from "../domain/types.js";
import { observeBusiness } from "../observability/internal.js";
import { withApplicationSpan } from "../tracing/proposed.js";

/** Deterministic local personalization lookup: no personal data is stored. */
export async function loadPersonalization(productId: string): Promise<Personalization> {
  return observeBusiness("repository", () => withApplicationSpan(
    "personalization.repository.loadPersonalization",
    "repository",
    async () => ({ customerSegment: productId === "sku-003" ? "priority" : "standard" })
  ));
}
