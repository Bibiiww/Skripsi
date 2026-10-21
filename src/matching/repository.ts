import type { MatchingResult } from "../domain/types.js";
import { observeBusiness } from "../observability/internal.js";
import { withApplicationSpan } from "../tracing/proposed.js";

const products = {
  "sku-001": { id: "sku-001", name: "Notebook", unitPrice: 25000 },
  "sku-002": { id: "sku-002", name: "Pen", unitPrice: 5000 },
  "sku-003": { id: "sku-003", name: "Backpack", unitPrice: 180000 }
} as const;
const stockByProduct: Record<string, number> = { "sku-001": 1200, "sku-002": 5000, "sku-003": 350 };

export async function findMatchingData(productId: string, quantity: number): Promise<MatchingResult | undefined> {
  return observeBusiness("repository", () => withApplicationSpan("matching.repository.findMatchingData", "repository", async () => {
    const product = products[productId as keyof typeof products];
    const stock = stockByProduct[productId];
    if (!product || stock === undefined) return undefined;
    return { product, availability: { productId, requestedQuantity: quantity, available: stock >= quantity, remaining: Math.max(0, stock - quantity) } };
  }));
}
