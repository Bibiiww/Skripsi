#!/usr/bin/env node
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { performance } from "node:perf_hooks";

function option(name, fallback) {
  const index = process.argv.indexOf(name);
  return index >= 0 ? process.argv[index + 1] : fallback;
}

function percentile(sorted, fraction) {
  if (!sorted.length) return null;
  return sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * fraction) - 1)];
}

function metricSummary(samples) {
  const sorted = [...samples].sort((a, b) => a - b);
  return {
    samples: sorted.length,
    min: sorted[0] ?? null,
    mean: sorted.length ? sorted.reduce((total, value) => total + value, 0) / sorted.length : null,
    p50: percentile(sorted, 0.5), p95: percentile(sorted, 0.95), p99: percentile(sorted, 0.99), max: sorted.at(-1) ?? null
  };
}

function errorSignature(error) {
  const primary = error instanceof Error ? error : new Error(String(error));
  const cause = primary.cause instanceof Error ? primary.cause : undefined;
  return [primary.name, primary.message, cause?.name, cause?.message, cause && "code" in cause ? cause.code : undefined].filter(Boolean).join(" | ");
}

function recordTransportError(summary, error) {
  summary.transportErrors += 1;
  const detail = errorSignature(error);
  summary.transportErrorDetails[detail] = (summary.transportErrorDetails[detail] ?? 0) + 1;
}

function assertDescriptor(descriptor) {
  const request = descriptor?.request;
  if (descriptor?.workloadDescriptorVersion !== "1.0.0" || !descriptor.id || request?.method !== "GET" || request.path !== "/api/v1/quote") {
    throw new Error("Invalid workload descriptor. Expected workload descriptor version 1.0.0.");
  }
  for (const key of ["rateRps", "durationSeconds", "warmupSeconds", "maxInFlight"]) {
    if (!Number.isFinite(request[key]) || request[key] <= 0 && key !== "warmupSeconds") throw new Error(`Invalid request.${key}`);
  }
}

function buildUrl(baseUrl, descriptor) {
  const url = new URL(descriptor.request.path, baseUrl);
  for (const [key, value] of Object.entries(descriptor.request.query)) url.searchParams.set(key, String(value));
  return url;
}

const descriptorPath = option("--descriptor");
const baseUrl = option("--base-url", "http://127.0.0.1:8080");
const outputPath = option("--output");
const skipWarmup = process.argv.includes("--skip-warmup");
if (!descriptorPath) throw new Error("Usage: node tools/load-generator.mjs --descriptor workloads/v1/o1-shallow-low.json [--base-url URL] [--output FILE]");

const descriptor = JSON.parse(await readFile(resolve(descriptorPath), "utf8"));
assertDescriptor(descriptor);
const url = buildUrl(baseUrl, descriptor);
const { rateRps, durationSeconds, warmupSeconds, maxInFlight } = descriptor.request;

async function runWarmup() {
  const until = performance.now() + warmupSeconds * 1000;
  const summary = { attempted: 0, completed: 0, successful: 0, transportErrors: 0, transportErrorDetails: {} };
  while (performance.now() < until) {
    summary.attempted += 1;
    try {
      const response = await fetch(url);
      summary.completed += 1;
      if (response.ok) summary.successful += 1;
      await response.arrayBuffer();
    } catch (error) { recordTransportError(summary, error); }
  }
  return summary;
}

console.log(`${skipWarmup ? "Skipping warm-up" : `Warm-up ${warmupSeconds}s`}; executing ${descriptor.id} at ${rateRps} RPS for ${durationSeconds}s.`);
const warmup = skipWarmup ? { attempted: 0, completed: 0, successful: 0, transportErrors: 0, transportErrorDetails: {} } : await runWarmup();

const latenciesMs = [];
const businessProcessingMs = [];
let missingBusinessProcessingMetrics = 0;
const statuses = {};
const transportErrorSummary = { transportErrors: 0, transportErrorDetails: {} };
let generatorDrops = 0;
let inFlight = 0;
const totalRequests = Math.round(rateRps * durationSeconds);
const intervalMs = 1000 / rateRps;
const startedAt = performance.now();
const pending = [];

for (let sequence = 0; sequence < totalRequests; sequence += 1) {
  const scheduledAt = startedAt + sequence * intervalMs;
  const delay = Math.max(0, scheduledAt - performance.now());
  pending.push(new Promise((resolveRequest) => setTimeout(resolveRequest, delay)).then(async () => {
    if (inFlight >= maxInFlight) { generatorDrops += 1; return; }
    inFlight += 1;
    const requestStarted = performance.now();
    try {
      const response = await fetch(url);
      const status = String(response.status);
      statuses[status] = (statuses[status] ?? 0) + 1;
      const businessProcessingHeader = response.headers.get("x-business-processing-ms");
      const businessTime = businessProcessingHeader === null ? Number.NaN : Number(businessProcessingHeader);
      if (response.ok && Number.isFinite(businessTime) && businessTime >= 0) businessProcessingMs.push(businessTime);
      else if (response.ok) missingBusinessProcessingMetrics += 1;
      await response.arrayBuffer();
      latenciesMs.push(performance.now() - requestStarted);
    } catch (error) {
      recordTransportError(transportErrorSummary, error);
    } finally {
      inFlight -= 1;
    }
  }));
}
await Promise.all(pending);
const elapsedSeconds = (performance.now() - startedAt) / 1000;
const summary = {
  schemaVersion: "1.1.0",
  descriptorId: descriptor.id,
  target: url.toString(),
  startedAt: new Date(Date.now() - elapsedSeconds * 1000).toISOString(),
  elapsedSeconds,
  scheduledRequests: totalRequests,
  completedRequests: latenciesMs.length,
  successfulRequests: Object.entries(statuses).filter(([status]) => Number(status) >= 200 && Number(status) < 300).reduce((total, [, count]) => total + count, 0),
  warmup,
  generatorDrops,
  transportErrors: transportErrorSummary.transportErrors,
  transportErrorDetails: transportErrorSummary.transportErrorDetails,
  statuses,
  achievedRps: latenciesMs.length / elapsedSeconds,
  latencyMs: metricSummary(latenciesMs),
  businessProcessingMs: { ...metricSummary(businessProcessingMs), missingSamples: missingBusinessProcessingMetrics }
};
if (outputPath) {
  const resolvedOutput = resolve(outputPath);
  await mkdir(dirname(resolvedOutput), { recursive: true });
  await writeFile(resolvedOutput, `${JSON.stringify(summary, null, 2)}\n`);
}
console.log(JSON.stringify(summary, null, 2));
