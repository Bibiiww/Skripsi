import { findProduct } from "./repository.js";
import type { Product } from "../domain/types.js";
import { withProposedSpan } from "../tracing/proposed.js";
import { observeBusiness } from "../observability/internal.js";

export async function getProduct(id: string): Promise<Product> {
  return observeBusiness("service", () => withProposedSpan("catalog.service.getProduct", "service", async () => {
    const product = await findProduct(id);
    if (!product) throw new Error("PRODUCT_NOT_FOUND");
    return product;
  }));
}
