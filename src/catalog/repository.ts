import type { Product } from "../domain/types.js";
import { withProposedSpan } from "../tracing/proposed.js";
import { observeBusiness } from "../observability/internal.js";

// Deliberately deterministic: later workloads can vary compute and structure
// without a database's cache state becoming an uncontrolled variable.
const products: Record<string, Product> = {
  "sku-001": { id: "sku-001", name: "Notebook", unitPrice: 25000 },
  "sku-002": { id: "sku-002", name: "Pen", unitPrice: 5000 },
  "sku-003": { id: "sku-003", name: "Backpack", unitPrice: 180000 }
};

export async function findProduct(id: string): Promise<Product | undefined> {
  return observeBusiness("repository", () => withProposedSpan("catalog.repository.findProduct", "repository", async () => products[id]));
}
