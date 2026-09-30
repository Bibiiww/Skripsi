export type ComputationalSignature = "O(1)" | "O(log n)" | "O(n)" | "mixed";
export type StructuralSignature = "shallow" | "moderate" | "complex";

export type WorkloadProfile = {
  id: string;
  computational: { signature: ComputationalSignature; inputSize: number };
  structural: { signature: StructuralSignature; boundaryDepth: number; fanOut: number };
};

const computationalSignatures: ComputationalSignature[] = ["O(1)", "O(log n)", "O(n)", "mixed"];
const structuralSignatures: StructuralSignature[] = ["shallow", "moderate", "complex"];

export const workloadProfiles: Record<string, WorkloadProfile> = {
  "o1-shallow-v1": {
    id: "o1-shallow-v1",
    computational: { signature: "O(1)", inputSize: 1 },
    structural: { signature: "shallow", boundaryDepth: 1, fanOut: 1 }
  },
  "ologn-moderate-v1": {
    id: "ologn-moderate-v1",
    computational: { signature: "O(log n)", inputSize: 1024 },
    structural: { signature: "moderate", boundaryDepth: 2, fanOut: 2 }
  },
  "on-complex-v1": {
    id: "on-complex-v1",
    computational: { signature: "O(n)", inputSize: 4096 },
    structural: { signature: "complex", boundaryDepth: 3, fanOut: 2 }
  },
  "mixed-complex-v1": {
    id: "mixed-complex-v1",
    computational: { signature: "mixed", inputSize: 2048 },
    structural: { signature: "complex", boundaryDepth: 3, fanOut: 3 }
  }
};

export function getWorkloadProfile(id: string): WorkloadProfile | undefined {
  return workloadProfiles[id];
}

export function createWorkloadProfile(computational: ComputationalSignature, structural: StructuralSignature, inputSize: number): WorkloadProfile | undefined {
  if (!computationalSignatures.includes(computational) || !structuralSignatures.includes(structural) || !Number.isInteger(inputSize) || inputSize < 1 || inputSize > 1_000_000) return undefined;
  const shape = structural === "shallow" ? { boundaryDepth: 1, fanOut: 1 } : structural === "moderate" ? { boundaryDepth: 2, fanOut: 2 } : { boundaryDepth: 3, fanOut: 2 };
  const signatureId = computational.replace(/[^a-z0-9]/gi, "").toLowerCase();
  return { id: `custom-${signatureId}-${structural}-n${inputSize}-v1`, computational: { signature: computational, inputSize }, structural: { signature: structural, ...shape } };
}
