#!/usr/bin/env node
import { mkdir, readdir, readFile, writeFile } from "node:fs/promises";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { basename, join, resolve } from "node:path";

const conditionsAllowed = ["baseline", "conventional", "proposed"];
const composeFiles = {
  baseline: ["compose.yaml"],
  conventional: ["compose.yaml", "compose.conventional.yaml"],
  proposed: ["compose.yaml", "compose.proposed.yaml"]
};

function option(name, fallback) {
  const index = process.argv.indexOf(name);
  return index >= 0 ? process.argv[index + 1] : fallback;
}

function hasFlag(name) {
  return process.argv.includes(name);
}

function csv(value) {
  if (value === null || value === undefined) return "";
  const text = String(value);
  return /[",\r\n]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}

function toNumber(value) {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function average(values) {
  const valid = values.filter((value) => Number.isFinite(value));
  return valid.length ? valid.reduce((total, value) => total + value, 0) / valid.length : null;
}

function max(values) {
  const valid = values.filter((value) => Number.isFinite(value));
  return valid.length ? Math.max(...valid) : null;
}

function bytes(text) {
  const match = /([0-9.]+)\s*(B|KiB|MiB|GiB|TiB)/i.exec(text ?? "");
  if (!match) return null;
  const scale = { b: 1, kib: 1024, mib: 1024 ** 2, gib: 1024 ** 3, tib: 1024 ** 4 };
  return Number(match[1]) * scale[match[2].toLowerCase()];
}

function resourceSummary(metrics, capacity) {
  const samples = metrics?.samples ?? [];
  const cpu = [];
  const memory = [];
  for (const sample of samples) {
    cpu.push((sample.containers ?? []).reduce((total, container) => total + (Number.parseFloat(container.CPUPerc) || 0), 0));
    memory.push((sample.containers ?? []).reduce((total, container) => total + (bytes(container.MemUsage) || 0), 0));
  }
  const cpuCapacityPercent = capacity?.cpuCores ? capacity.cpuCores * 100 : null;
  const memoryCapacityBytes = capacity?.memoryBytes ?? null;
  return {
    samples: samples.length,
    cpuPercent: { mean: average(cpu), max: max(cpu), capacity: cpuCapacityPercent, remainingMean: cpuCapacityPercent === null ? null : Math.max(0, cpuCapacityPercent - average(cpu)), remainingMin: cpuCapacityPercent === null ? null : Math.max(0, cpuCapacityPercent - max(cpu)) },
    memoryBytes: { mean: average(memory), max: max(memory), capacity: memoryCapacityBytes, remainingMean: memoryCapacityBytes === null ? null : Math.max(0, memoryCapacityBytes - average(memory)), remainingMin: memoryCapacityBytes === null ? null : Math.max(0, memoryCapacityBytes - max(memory)) }
  };
}

function execute(command, args, { quiet = false } = {}) {
  return new Promise((resolveCommand, rejectCommand) => {
    const child = spawn(command, args, { stdio: quiet ? ["ignore", "pipe", "pipe"] : "inherit" });
    let output = "";
    if (quiet) {
      child.stdout.on("data", (chunk) => { output += chunk; });
      child.stderr.on("data", (chunk) => { output += chunk; });
    }
    child.on("error", rejectCommand);
    child.on("exit", (code) => code === 0 ? resolveCommand(output) : rejectCommand(new Error(`${command} ${args.join(" ")} failed${output ? `: ${output.trim()}` : ""}`)));
  });
}

async function sleep(milliseconds) {
  await new Promise((resolveSleep) => setTimeout(resolveSleep, milliseconds));
}

async function startStack(composeArguments) {
  let lastError;
  for (let attempt = 1; attempt <= 2; attempt += 1) {
    try {
      await execute("docker", [...composeArguments, "up", "--build", "--detach", "--wait", "--wait-timeout", "120"]);
      return;
    } catch (error) {
      lastError = error;
      try { await execute("docker", [...composeArguments, "down", "--remove-orphans"], { quiet: true }); } catch { /* retry after best-effort cleanup */ }
      if (attempt === 1) await sleep(5000);
    }
  }
  throw lastError;
}

async function workloadDescriptors(directory, requested) {
  const names = requested
    ? requested.split(",").map((name) => basename(name.trim().replace(/[\\/]+$/, ""))).filter(Boolean).map((name) => name.endsWith(".json") ? name : `${name}.json`)
    : (await readdir(directory)).filter((name) => name.endsWith(".json")).sort();
  const descriptors = [];
  for (const name of names) {
    const path = join(directory, name);
    const descriptor = JSON.parse(await readFile(path, "utf8"));
    if (descriptor.workloadDescriptorVersion === "1.0.0" && descriptor.id) descriptors.push({ path, descriptor });
  }
  if (!descriptors.length) throw new Error("No workload descriptors selected.");
  const structureRank = { shallow: 0, moderate: 1, complex: 2 };
  return descriptors.sort((left, right) =>
    left.descriptor.request.rateRps - right.descriptor.request.rateRps ||
    (structureRank[left.descriptor.structural.signature] ?? 99) - (structureRank[right.descriptor.structural.signature] ?? 99) ||
    left.descriptor.id.localeCompare(right.descriptor.id)
  );
}

async function newestRunDirectory(runsDirectory, previousNames) {
  const entries = await readdir(runsDirectory, { withFileTypes: true });
  const candidates = await Promise.all(entries.filter((entry) => entry.isDirectory() && !previousNames.has(entry.name)).map(async (entry) => {
    const manifest = JSON.parse(await readFile(join(runsDirectory, entry.name, "manifest.json"), "utf8"));
    return { name: entry.name, startedAt: manifest.startedAt };
  }));
  return candidates.sort((left, right) => right.startedAt.localeCompare(left.startedAt))[0]?.name;
}

async function summarizeRun(runDirectory) {
  const [manifest, http, tracing, resources] = await Promise.all([
    readFile(join(runDirectory, "manifest.json"), "utf8").then(JSON.parse),
    readFile(join(runDirectory, "http-summary.json"), "utf8").then(JSON.parse),
    readFile(join(runDirectory, "tracing-metrics.json"), "utf8").then(JSON.parse).catch(() => null),
    readFile(join(runDirectory, "container-metrics.json"), "utf8").then(JSON.parse)
  ]);
  return {
    runId: manifest.runId,
    condition: manifest.condition,
    descriptorId: manifest.descriptorId,
    descriptorSha256: manifest.descriptorSha256,
    status: manifest.status,
    measurementValidation: manifest.measurementValidation,
    startedAt: manifest.startedAt,
    finishedAt: manifest.finishedAt,
    http,
    tracing,
    resources: resourceSummary(resources, manifest.hostCapacity),
    runDirectory
  };
}

function flatRecord(record) {
  const http = record.http ?? {};
  const tracing = record.tracing ?? {};
  const resources = record.resources ?? { cpuPercent: {}, memoryBytes: {} };
  return {
    run_id: record.runId,
    condition: record.condition,
    descriptor_id: record.descriptorId,
    descriptor_sha256: record.descriptorSha256,
    status: record.status,
    started_at: record.startedAt,
    finished_at: record.finishedAt,
    achieved_rps: toNumber(http.achievedRps),
    target_rps: toNumber(record.targetRps),
    achieved_target_rate_percent: toNumber(record.measurementValidation?.achievedTargetRatePercent),
    successful_requests: http.successfulRequests ?? null,
    scheduled_requests: http.scheduledRequests ?? null,
    success_rate_percent: toNumber(record.measurementValidation?.successRatePercent),
    generator_drops: http.generatorDrops ?? null,
    generator_drop_rate_percent: toNumber(record.measurementValidation?.generatorDropRatePercent),
    transport_errors: http.transportErrors ?? null,
    request_latency_mean_ms: toNumber(http.latencyMs?.mean),
    request_latency_p50_ms: toNumber(http.latencyMs?.p50),
    request_latency_p95_ms: toNumber(http.latencyMs?.p95),
    request_latency_p99_ms: toNumber(http.latencyMs?.p99),
    business_processing_mean_ms: toNumber(http.businessProcessingMs?.mean),
    business_processing_p50_ms: toNumber(http.businessProcessingMs?.p50),
    business_processing_p95_ms: toNumber(http.businessProcessingMs?.p95),
    business_processing_p99_ms: toNumber(http.businessProcessingMs?.p99),
    business_processing_samples: http.businessProcessingMs?.samples ?? null,
    business_processing_missing_samples: http.businessProcessingMs?.missingSamples ?? null,
    queue_drop_rate_percent: toNumber(tracing.queueDropRatePercent),
    reconstruction_success_rate_percent: toNumber(tracing.reconstructionSuccessRatePercent),
    events_produced: tracing.events?.produced ?? null,
    events_enqueued: tracing.events?.enqueued ?? null,
    events_dropped: tracing.events?.dropped ?? null,
    events_reconstructed: tracing.events?.reconstructed ?? null,
    cpu_percent_mean: resources.cpuPercent.mean ?? null,
    cpu_percent_max: resources.cpuPercent.max ?? null,
    cpu_capacity_percent: resources.cpuPercent.capacity ?? null,
    cpu_remaining_mean_percent: resources.cpuPercent.remainingMean ?? null,
    cpu_remaining_min_percent: resources.cpuPercent.remainingMin ?? null,
    memory_bytes_mean: resources.memoryBytes.mean ?? null,
    memory_bytes_max: resources.memoryBytes.max ?? null,
    memory_capacity_bytes: resources.memoryBytes.capacity ?? null,
    memory_remaining_mean_bytes: resources.memoryBytes.remainingMean ?? null,
    memory_remaining_min_bytes: resources.memoryBytes.remainingMin ?? null,
    run_directory: record.runDirectory,
    error: record.error ?? null
  };
}

const conditions = (option("--conditions", conditionsAllowed.join(","))).split(",").map((value) => value.trim()).filter(Boolean);
if (!conditions.length || conditions.some((condition) => !conditionsAllowed.includes(condition))) throw new Error("--conditions must contain baseline, conventional, and/or proposed.");
const repetitions = Number(option("--repetitions", "1"));
if (!Number.isInteger(repetitions) || repetitions < 1) throw new Error("--repetitions must be a positive integer.");
const cooldownSeconds = Number(option("--cooldown-seconds", "5"));
if (!Number.isFinite(cooldownSeconds) || cooldownSeconds < 0) throw new Error("--cooldown-seconds must be zero or greater.");
const saturationThresholdPercent = Number(option("--saturation-threshold-percent", "95"));
if (!Number.isFinite(saturationThresholdPercent) || saturationThresholdPercent <= 0 || saturationThresholdPercent > 100) throw new Error("--saturation-threshold-percent must be greater than 0 and at most 100.");
const baseUrl = option("--base-url", "http://127.0.0.1:8080");
const resultsRoot = resolve(option("--results-dir", "results"));
const matrixId = `${new Date().toISOString().replace(/[:.]/g, "-")}-matrix-${randomUUID().slice(0, 8)}`;
const matrixDirectory = join(resultsRoot, matrixId);
const runsDirectory = join(matrixDirectory, "runs");
const selected = await workloadDescriptors(resolve(option("--workloads-dir", "workloads/v1")), option("--descriptors"));
const records = [];
const matrix = {
  schemaVersion: "1.0.0", matrixId, status: "running", startedAt: new Date().toISOString(),
  conditions, descriptors: selected.map(({ descriptor }) => ({ id: descriptor.id, rateRps: descriptor.request.rateRps, path: descriptor.request.path })),
  repetitions, cooldownSeconds, saturationThresholdPercent, baseUrl, order: "condition -> descriptor (low to high RPS) -> repetition", stackReset: "before every workload repetition", records
};
await mkdir(runsDirectory, { recursive: true });
await writeFile(join(matrixDirectory, "matrix-manifest.json"), `${JSON.stringify(matrix, null, 2)}\n`);

let failures = 0;
for (const condition of conditions) {
  for (const { path, descriptor } of selected) {
    for (let repetition = 1; repetition <= repetitions; repetition += 1) {
      const project = `tracing-matrix-${condition}`;
      const files = composeFiles[condition];
      const composeArguments = ["compose", "-p", project, ...files.flatMap((file) => ["-f", file])];
      const previousNames = new Set((await readdir(runsDirectory, { withFileTypes: true })).filter((entry) => entry.isDirectory()).map((entry) => entry.name));
      try {
        await startStack(composeArguments);
        try {
          await execute(process.execPath, ["tools/run-experiment.mjs", "--descriptor", path, "--condition", condition, "--compose-project", project, "--results-dir", runsDirectory, "--base-url", baseUrl, "--saturation-threshold-percent", String(saturationThresholdPercent)]);
          const newest = await newestRunDirectory(runsDirectory, previousNames);
          records.push({ ...await summarizeRun(join(runsDirectory, newest)), repetition, targetRps: descriptor.request.rateRps });
        } catch (error) {
          failures += 1;
          const newest = await newestRunDirectory(runsDirectory, previousNames);
          if (newest) {
            try { records.push({ ...await summarizeRun(join(runsDirectory, newest)), repetition, error: error instanceof Error ? error.message : String(error) }); }
            catch { records.push({ condition, descriptorId: descriptor.id, status: "failed", repetition, error: error instanceof Error ? error.message : String(error) }); }
          } else records.push({ condition, descriptorId: descriptor.id, status: "failed", repetition, error: error instanceof Error ? error.message : String(error) });
        }
      } catch (error) {
        failures += 1;
        records.push({ condition, descriptorId: descriptor.id, status: "failed", repetition, error: error instanceof Error ? error.message : String(error) });
      } finally {
        try { await execute("docker", [...composeArguments, "down", "--remove-orphans"], { quiet: true }); }
        catch (error) { failures += 1; records.push({ condition, descriptorId: descriptor.id, status: "cleanup_failed", repetition, error: error instanceof Error ? error.message : String(error) }); }
        await writeFile(join(matrixDirectory, "matrix-manifest.json"), `${JSON.stringify(matrix, null, 2)}\n`);
        if (cooldownSeconds > 0) await sleep(cooldownSeconds * 1000);
      }
    }
  }
}

const flattened = records.map(flatRecord);
const valid = flattened.filter((record) => record.status === "completed");
const saturated = flattened.filter((record) => record.status === "saturated");
const rejected = flattened.filter((record) => record.status !== "completed" && record.status !== "saturated");
const columns = [...new Set(flattened.flatMap((record) => Object.keys(record)))];
const csvText = (rows) => `${columns.join(",")}\n${rows.map((record) => columns.map((column) => csv(record[column])).join(",")).join("\n")}\n`;
await writeFile(join(matrixDirectory, "analysis.json"), `${JSON.stringify({ schemaVersion: "1.2.0", matrixId, generatedAt: new Date().toISOString(), comparisonRuns: valid, stressTestRuns: saturated, failedRuns: rejected }, null, 2)}\n`);
await writeFile(join(matrixDirectory, "analysis.csv"), csvText(valid));
await writeFile(join(matrixDirectory, "stress-test.csv"), csvText(saturated));
await writeFile(join(matrixDirectory, "failed-runs.csv"), csvText(rejected));
try { await execute(process.execPath, ["tools/generate-charts.mjs", "--matrix-dir", matrixDirectory]); }
catch (error) { matrix.chartGenerationError = error instanceof Error ? error.message : String(error); }
matrix.status = failures ? "completed_with_failures" : "completed";
matrix.finishedAt = new Date().toISOString();
matrix.failures = failures;
await writeFile(join(matrixDirectory, "matrix-manifest.json"), `${JSON.stringify(matrix, null, 2)}\n`);
console.log(`Matrix complete: ${matrixDirectory}`);
if (failures) process.exitCode = 1;
