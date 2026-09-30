import { findProduct } from "./repository.js";
import type { Product } from "../domain/types.js";
import { withProposedSpan } from "../tracing/proposed.js";

export async function getProduct(id: string): Promise<Product> {
  return withProposedSpan("catalog.service.getProduct", "service", async () => {
    const product = await findProduct(id);
    if (!product) throw new Error("PRODUCT_NOT_FOUND");
    return product;
  });
}
