#!/usr/bin/env node
import { copyFile, mkdir, readFile, writeFile } from "node:fs/promises";
import { spawn, spawnSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { resolve, join } from "node:path";

function option(name, fallback) {
  const index = process.argv.indexOf(name);
  return index >= 0 ? process.argv[index + 1] : fallback;
}

function run(command, args) {
  const result = spawnSync(command, args, { encoding: "utf8" });
  if (result.status !== 0) throw new Error(`${command} ${args.join(" ")} failed: ${result.stderr.trim() || result.stdout.trim()}`);
  return result.stdout.trim();
}

function containerIds(project, composeFiles) {
  const fileArgs = composeFiles.flatMap((file) => ["-f", file]);
  const ids = run("docker", ["compose", "-p", project, ...fileArgs, "ps", "-q"]).split(/\r?\n/).filter(Boolean);
  if (!ids.length) throw new Error("No Compose containers are running. Start the selected condition with docker compose up -d first.");
  return ids;
}

function dockerStats(ids) {
  const output = run("docker", ["stats", "--no-stream", "--format", "{{json .}}", ...ids]);
  return output.split(/\r?\n/).filter(Boolean).map((line) => JSON.parse(line));
}

function dockerHostCapacity() {
  try {
    const [cpuCores, memoryBytes] = run("docker", ["info", "--format", "{{.NCPU}},{{.MemTotal}}"]).split(",");
    return { cpuCores: Number(cpuCores) || null, memoryBytes: Number(memoryBytes) || null };
  } catch {
    return { cpuCores: null, memoryBytes: null };
  }
}

function waitForChild(child) {
  return new Promise((resolveChild, rejectChild) => {
    child.on("error", rejectChild);
    child.on("exit", (code) => code === 0 ? resolveChild() : rejectChild(new Error(`Load generator exited with code ${code}`)));
  });
}

const sleep = (milliseconds) => new Promise((resolveSleep) => setTimeout(resolveSleep, milliseconds));

async function json(url) {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`Could not fetch ${url}: HTTP ${response.status}`);
  return response.json();
}

async function waitForProposedDrain(queueUrl, reconstructionUrl, timeoutSeconds) {
  const deadline = Date.now() + timeoutSeconds * 1000;
  let emptySamples = 0;
  let queue;
  let reconstruction;
  await sleep(1000); // permits detached producer submissions to reach the queue
  while (Date.now() < deadline) {
    [queue, reconstruction] = await Promise.all([json(queueUrl), json(reconstructionUrl)]);
    emptySamples = queue.queued === 0 ? emptySamples + 1 : 0;
    if (emptySamples >= 2) return { queue, reconstruction, drained: true };
    await sleep(500);
  }
  return { queue, reconstruction, drained: false };
}

function delta(after, before, fields) {
  return Object.fromEntries(fields.map((field) => [field, after[field] - before[field]]));
}

const descriptorPath = option("--descriptor");
const condition = option("--condition");
const baseUrl = option("--base-url", "http://127.0.0.1:8080");
const resultsDir = resolve(option("--results-dir", "results"));
const project = option("--compose-project", condition ? `tracing-${condition}` : undefined);
const queueMetricsUrl = option("--queue-metrics-url", "http://127.0.0.1:16686/v1/metrics");
const reconstructionMetricsUrl = option("--reconstruction-metrics-url", "http://127.0.0.1:16687/v1/metrics");
const traceDrainTimeoutSeconds = Number(option("--trace-drain-timeout-seconds", "30"));
const saturationThresholdPercent = Number(option("--saturation-threshold-percent", "95"));
if (!descriptorPath || !condition) throw new Error("Usage: node tools/run-experiment.mjs --descriptor FILE --condition baseline|conventional|proposed [--base-url URL]");
if (!["baseline", "conventional", "proposed"].includes(condition)) throw new Error("condition must be baseline, conventional, or proposed");
if (!Number.isFinite(saturationThresholdPercent) || saturationThresholdPercent <= 0 || saturationThresholdPercent > 100) throw new Error("--saturation-threshold-percent must be greater than 0 and at most 100.");

const descriptorAbsolutePath = resolve(descriptorPath);
const descriptorText = await readFile(descriptorAbsolutePath, "utf8");
const descriptor = JSON.parse(descriptorText);
if (descriptor.workloadDescriptorVersion !== "1.0.0") throw new Error("Unsupported workload descriptor version.");
const runId = `${new Date().toISOString().replace(/[:.]/g, "-")}-${condition}-${randomUUID().slice(0, 8)}`;
const runDir = join(resultsDir, runId);
await mkdir(runDir, { recursive: true });
await copyFile(descriptorAbsolutePath, join(runDir, "workload.json"));

const composeFiles = condition === "conventional"
  ? ["compose.yaml", "compose.conventional.yaml"]
  : condition === "proposed"
    ? ["compose.yaml", "compose.proposed.yaml"]
    : ["compose.yaml"];
const ids = containerIds(project, composeFiles);
const hostCapacity = dockerHostCapacity();
const manifest = {
  schemaVersion: "1.0.0", runId, condition, status: "running", startedAt: new Date().toISOString(),
  descriptorId: descriptor.id, descriptorSha256: createHash("sha256").update(descriptorText).digest("hex"),
  baseUrl, composeProject: project, composeFiles, containers: ids, hostCapacity, command: process.argv.slice(2)
};
await writeFile(join(runDir, "manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`);

const samples = [];
let samplingError = null;
let sampling = false;
const sample = () => {
  if (sampling) return;
  sampling = true;
  try { samples.push({ capturedAt: new Date().toISOString(), containers: dockerStats(ids) }); }
  catch (error) { samplingError = error instanceof Error ? error.message : String(error); }
  finally { sampling = false; }
};
sample();
const timer = setInterval(sample, 1000);
try {
  // Warm-up is deliberately outside the measurement window. This makes every
  // tracing metric below a per-measurement delta rather than a cumulative value.
  const warmupChild = spawn(process.execPath, ["tools/load-generator.mjs", "--descriptor", descriptorAbsolutePath, "--base-url", baseUrl], { stdio: "inherit" });
  await waitForChild(warmupChild);

  let proposedBefore;
  if (condition === "proposed") {
    const settled = await waitForProposedDrain(queueMetricsUrl, reconstructionMetricsUrl, traceDrainTimeoutSeconds);
    if (!settled.drained) throw new Error("Could not drain proposed tracing events after warm-up; measurement was not started.");
    proposedBefore = { queue: settled.queue, reconstruction: settled.reconstruction };
  }

  const child = spawn(process.execPath, ["tools/load-generator.mjs", "--descriptor", descriptorAbsolutePath, "--base-url", baseUrl, "--skip-warmup", "--output", join(runDir, "http-summary.json")], { stdio: "inherit" });
  await waitForChild(child);
  const http = JSON.parse(await readFile(join(runDir, "http-summary.json"), "utf8"));
  const successRatePercent = http.scheduledRequests ? (http.successfulRequests / http.scheduledRequests) * 100 : 0;
  const generatorDropRatePercent = http.scheduledRequests ? (http.generatorDrops / http.scheduledRequests) * 100 : 0;
  const achievedTargetRatePercent = descriptor.request.rateRps ? (http.achievedRps / descriptor.request.rateRps) * 100 : 0;
  manifest.measurementValidation = {
    successfulRequests: http.successfulRequests,
    completedRequests: http.completedRequests,
    transportErrors: http.transportErrors,
    successRatePercent,
    generatorDropRatePercent,
    achievedTargetRatePercent,
    saturationThresholdPercent
  };
  if (condition === "proposed") {
    const drain = await waitForProposedDrain(queueMetricsUrl, reconstructionMetricsUrl, traceDrainTimeoutSeconds);
    const queue = delta(drain.queue, proposedBefore.queue, ["accepted", "dropped", "queued", "dequeued"]);
    const reconstruction = delta(drain.reconstruction, proposedBefore.reconstruction, ["observedRequests", "completeTraces", "reconstructedSpans", "events", "incompleteSpans"]);
    const expectedRequests = http.successfulRequests;
    const eventsProduced = queue.accepted + queue.dropped;
    const metrics = {
      schemaVersion: "1.1.0", capturedAt: new Date().toISOString(), drained: drain.drained,
      measurementOnly: true, expectedRequests,
      events: { produced: eventsProduced, enqueued: queue.accepted, dropped: queue.dropped, reconstructed: reconstruction.events },
      queue, reconstruction,
      queueDropRatePercent: eventsProduced ? (queue.dropped / eventsProduced) * 100 : null,
      reconstructionSuccessRatePercent: expectedRequests ? (reconstruction.completeTraces / expectedRequests) * 100 : null
    };
    await writeFile(join(runDir, "tracing-metrics.json"), `${JSON.stringify(metrics, null, 2)}\n`);
  }
  const isSaturated = successRatePercent < saturationThresholdPercent || achievedTargetRatePercent < saturationThresholdPercent;
  manifest.measurementValidation.classification = isSaturated ? "saturated" : "completed";
  manifest.status = isSaturated ? "saturated" : "completed";
} catch (error) {
  manifest.status = "failed";
  manifest.error = error instanceof Error ? error.message : String(error);
  throw error;
} finally {
  clearInterval(timer);
  sample();
  manifest.finishedAt = new Date().toISOString();
  manifest.resourceSamplingError = samplingError;
  await writeFile(join(runDir, "container-metrics.json"), `${JSON.stringify({ schemaVersion: "1.0.0", intervalSeconds: 1, samples }, null, 2)}\n`);
  await writeFile(join(runDir, "manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`);
}
console.log(`Experiment complete: ${runDir}`);
