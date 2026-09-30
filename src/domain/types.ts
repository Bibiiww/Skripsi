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
