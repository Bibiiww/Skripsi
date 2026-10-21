export type Product = {
  id: string;
  name: string;
  unitPrice: number;
};

export type Availability = {
  productId: string;
  requestedQuantity: number;
  available: boolean;
  remaining: number;
};

/** Synthetic and non-sensitive input carried from Server A to Server B. */
export type Personalization = { customerSegment: "standard" | "priority" };

export type MatchingResult = { product: Product; availability: Availability };

export type Quote = {
  product: Product;
  availability: Availability;
  quantity: number;
  subtotal: number;
  workload?: {
    profileId: string;
    checksum: number;
    structuralSteps: number;
  };
};
