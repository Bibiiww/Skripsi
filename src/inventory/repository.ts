import { withProposedSpan } from "../tracing/proposed.js";

const stockByProduct: Record<string, number> = {
  "sku-001": 1200,
  "sku-002": 5000,
  "sku-003": 350
};

export async function readStock(productId: string): Promise<number | undefined> {
  return withProposedSpan("inventory.repository.readStock", "repository", async () => stockByProduct[productId]);
}
