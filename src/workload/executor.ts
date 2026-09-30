import type { WorkloadProfile } from "./profiles.js";
import { withProposedSpan } from "../tracing/proposed.js";

export type WorkloadExecution = { profileId: string; checksum: number; structuralSteps: number };

function constantCost(seed: number): number {
  return Math.imul(seed ^ 0x9e3779b9, 31) >>> 0;
}

function logarithmicCost(inputSize: number, seed: number): number {
  let checksum = seed;
  for (let remaining = inputSize; remaining > 1; remaining = Math.floor(remaining / 2)) {
    checksum = constantCost(checksum);
  }
  return checksum;
}

function linearCost(inputSize: number, seed: number): number {
  let checksum = seed;
  for (let index = 0; index < inputSize; index += 1) checksum = constantCost(checksum + index);
  return checksum;
}

function executeCompute(profile: WorkloadProfile): number {
  const { signature, inputSize } = profile.computational;
  if (signature === "O(1)") return constantCost(inputSize);
  if (signature === "O(log n)") return logarithmicCost(inputSize, inputSize);
  if (signature === "O(n)") return linearCost(inputSize, inputSize);
  return linearCost(inputSize, logarithmicCost(inputSize, inputSize));
}

// These named boundaries deliberately create deterministic internal call shapes.
// Future wrapper instrumentation will observe these functions, not utility calls.
function shallowBoundary(checksum: number): number { return constantCost(checksum); }
function moderateBoundary(checksum: number): number { return shallowBoundary(constantCost(checksum)); }
function complexBoundary(checksum: number): number { return moderateBoundary(shallowBoundary(constantCost(checksum))); }

async function executeStructureNode(profile: WorkloadProfile, depth: number, checksum: number): Promise<{ checksum: number; steps: number }> {
  return withProposedSpan(`gateway.service.workload.${profile.structural.signature}.depth${depth}`, "service", async () => {
    const boundary = profile.structural.signature === "shallow" ? shallowBoundary : profile.structural.signature === "moderate" ? moderateBoundary : complexBoundary;
    let result = boundary(checksum);
    let steps = 1;
    if (depth < profile.structural.boundaryDepth) {
      for (let branch = 0; branch < profile.structural.fanOut; branch += 1) {
        const child = await executeStructureNode(profile, depth + 1, result + branch);
        result = child.checksum; steps += child.steps;
      }
    }
    return { checksum: result, steps };
  });
}

export async function executeControlledWorkload(profile: WorkloadProfile): Promise<WorkloadExecution> {
  const computed = await withProposedSpan("gateway.service.workload.compute", "service", async () => executeCompute(profile));
  const structured = await executeStructureNode(profile, 1, computed);
  return { profileId: profile.id, checksum: structured.checksum, structuralSteps: structured.steps };
}
