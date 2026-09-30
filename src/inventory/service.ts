import { readStock } from "./repository.js";
import type { Availability } from "../domain/types.js";
import { withProposedSpan } from "../tracing/proposed.js";

export async function checkAvailability(productId: string, quantity: number): Promise<Availability> {
  return withProposedSpan("inventory.service.checkAvailability", "service", async () => {
    const stock = await readStock(productId);
    if (stock === undefined) throw new Error("PRODUCT_NOT_FOUND");
    return { productId, requestedQuantity: quantity, available: stock >= quantity, remaining: Math.max(0, stock - quantity) };
  });
}
