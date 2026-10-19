#!/usr/bin/env node
/**
 * Failure/recovery matrix. It is intentionally separate from run-matrix:
 * primary performance measures a healthy system, whereas this tool measures
 * explicitly injected operational failures and recovery behaviour.
 */
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { join, resolve } from "node:path";

const ROOT = resolve(".");
const allowedConditions = ["conventional-otel", "proposed-memory", "proposed-durable"];
const allowedScenarios = ["central-outage", "processing-outage", "producer-restart"];
const composeFiles = {
  "conventional-otel": ["compose.yaml", "compose.conventional-otel.yaml"],
  "proposed-memory": ["compose.yaml", "compose.proposed-memory.yaml"],
  "proposed-durable": ["compose.yaml", "compose.proposed-durable.yaml"]
};

function option(name, fallback) { const at = process.argv.indexOf(name); return at >= 0 ? process.argv[at + 1] : fallback; }
function list(value) { return String(value).split(",").map((item) => item.trim()).filter(Boolean); }
function csv(value) { if (value === null || value === undefined) return ""; const text = String(value); return /[",\r\n]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text; }
const sleep = (ms) => new Promise((done) => setTimeout(done, ms));

function execute(command, args, { quiet = false, env } = {}) {
  return new Promise((resolveCommand, rejectCommand) => {
    const child = spawn(command, args, { cwd: ROOT, stdio: quiet ? ["ignore", "pipe", "pipe"] : "inherit", env: env ? { ...process.env, ...env } : process.env });
    let output = "";
    if (quiet) { child.stdout.on("data", (part) => { output += part; }); child.stderr.on("data", (part) => { output += part; }); }
    child.on("error", rejectCommand);
    child.on("exit", (code) => code === 0 ? resolveCommand(output) : rejectCommand(new Error(`${command} ${args.join(" ")} failed${output ? `: ${output.trim()}` : ""}`)));
  });
}

async function json(url) { const response = await fetch(url); if (!response.ok) throw new Error(`${url}: HTTP ${response.status}`); return response.json(); }
async function waitForHealth(url, timeoutMs = 45_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try { if ((await fetch(url)).ok) return Date.now(); } catch { /* service is still restarting */ }
    await sleep(250);
  }
  throw new Error(`Timed out waiting for ${url}`);
}

function composeArgs(project, condition) { return ["compose", "-p", project, ...composeFiles[condition].flatMap((file) => ["-f", file])]; }
function endpointForService(service) {
  if (service === "gateway") return "http://127.0.0.1:8080/health";
  if (service === "trace-reconstruction-worker") return "http://127.0.0.1:16687/health";
  return "http://127.0.0.1:16686/health";
}
function faultTarget(condition, scenario) {
  if (scenario === "producer-restart") return "gateway";
  if (condition === "conventional-otel") return "trace-collector";
  return scenario === "central-outage" ? "trace-event-queue" : "trace-reconstruction-worker";
}
function expectedSpans(summary) { return Number(summary?.successful ?? 0) * 5; }

async function collect(condition, loadSummary) {
  if (condition === "conventional-otel") {
    const metrics = await json("http://127.0.0.1:16686/v1/metrics");
    const expected = expectedSpans(loadSummary);
    return {
      tracing_units_produced: expected,
      tracing_units_received: metrics.spansReceived ?? null,
      tracing_units_processed: metrics.spansProcessed ?? null,
      reconstructed_traces: metrics.completeTraces ?? null,
      reconstruction_success_rate_percent: metrics.observedTraces ? 100 * metrics.completeTraces / metrics.observedTraces : null,
      delivery_retention_percent: expected ? 100 * metrics.spansReceived / expected : null,
      queue_drop_rate_percent: null,
      duplicates: null,
      queue_depth: null
    };
  }
  const queue = await json("http://127.0.0.1:16686/v2/metrics");
  const worker = await json("http://127.0.0.1:16687/v2/metrics");
  // v2 is the execution-record pipeline. Its queue and reconstruction store
  // expose their actual counters directly, rather than diagnostic counters.
  const enqueued = queue?.accepted ?? null;
  const dropped = queue?.dropped ?? null;
  const produced = enqueued === null || dropped === null ? null : enqueued + dropped;
  const reconstructed = worker?.completeTraces ?? null;
  const attempts = worker?.observedTraces ?? null;
  return {
    tracing_units_produced: produced,
    tracing_units_received: enqueued,
    tracing_units_processed: worker?.reconstructedSpans ?? null,
    reconstructed_traces: reconstructed,
    reconstruction_success_rate_percent: attempts ? 100 * reconstructed / attempts : null,
    delivery_retention_percent: produced ? 100 * enqueued / produced : null,
    queue_drop_rate_percent: enqueued + dropped ? 100 * dropped / (enqueued + dropped) : null,
    duplicates: queue?.duplicates ?? null,
    queue_depth: queue?.queued ?? null
  };
}

async function drainProposed() {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    try { if ((await json("http://127.0.0.1:16686/v2/metrics"))?.queued === 0) return Date.now(); } catch { /* wait for recovery */ }
    await sleep(250);
  }
  return null;
}

const conditions = list(option("--conditions", allowedConditions.join(",")));
const scenarios = list(option("--scenarios", allowedScenarios.join(",")));
const descriptorPath = resolve(option("--descriptor", "workloads/v1/o1-shallow-low.json"));
const repetitions = Number(option("--repetitions", "1"));
const faultAfterSeconds = Number(option("--fault-after-seconds", "5"));
const outageSeconds = Number(option("--outage-seconds", "5"));
const outputRoot = resolve(option("--results-dir", `results/fault-${new Date().toISOString().replaceAll(":", "-").replaceAll(".", "-")}-${randomUUID().slice(0, 8)}`));
if (conditions.some((value) => !allowedConditions.includes(value))) throw new Error(`conditions must be one of: ${allowedConditions.join(", ")}`);
if (scenarios.some((value) => !allowedScenarios.includes(value))) throw new Error(`scenarios must be one of: ${allowedScenarios.join(", ")}`);
if (!Number.isInteger(repetitions) || repetitions < 1 || !Number.isFinite(faultAfterSeconds) || !Number.isFinite(outageSeconds) || faultAfterSeconds < 0 || outageSeconds <= 0) throw new Error("Invalid repetitions or fault timing.");
const descriptor = JSON.parse(await readFile(descriptorPath, "utf8"));
if (descriptor.request.durationSeconds <= faultAfterSeconds + outageSeconds) throw new Error("Descriptor duration must exceed --fault-after-seconds + --outage-seconds so that recovery is measured during load.");

await mkdir(outputRoot, { recursive: true });
const rows = [];
for (const condition of conditions) for (const scenario of scenarios) for (let repetition = 1; repetition <= repetitions; repetition += 1) {
  const runId = `${new Date().toISOString().replaceAll(":", "-").replaceAll(".", "-")}-${condition}-${scenario}-r${repetition}-${randomUUID().slice(0, 8)}`;
  const runDir = join(outputRoot, "runs", runId); await mkdir(runDir, { recursive: true });
  const project = `fault-${condition}-${randomUUID().slice(0, 8)}`.replaceAll("_", "-");
  const compose = composeArgs(project, condition); const target = faultTarget(condition, scenario);
  const startedAt = new Date().toISOString(); let loadError = null; let faultError = null; let recoveryTimeMs = null; let drainTimeMs = null;
  let loadPromise; const loadOutput = join(runDir, "http-summary.json");
  try {
    await execute("docker", [...compose, "up", "--build", "--detach", "--wait", "--wait-timeout", "120"]);
    // A separate warm-up keeps startup effects out of the injected-fault interval.
    await execute(process.execPath, ["tools/load-generator.mjs", "--descriptor", descriptorPath, "--output", join(runDir, "warmup-summary.json")], { quiet: true });
    loadPromise = execute(process.execPath, ["tools/load-generator.mjs", "--descriptor", descriptorPath, "--skip-warmup", "--output", loadOutput]);
    await sleep(faultAfterSeconds * 1000);
    const recoveryStarted = Date.now();
    await execute("docker", [...compose, "stop", target]);
    await sleep(outageSeconds * 1000);
    await execute("docker", [...compose, "start", target]);
    const healthyAt = await waitForHealth(endpointForService(target));
    recoveryTimeMs = healthyAt - recoveryStarted;
    try { await loadPromise; } catch (error) { loadError = error instanceof Error ? error.message : String(error); }
    await sleep(1000); const drainedAt = condition === "conventional-otel" ? Date.now() : await drainProposed();
    drainTimeMs = drainedAt === null ? null : drainedAt - healthyAt;
  } catch (error) { faultError = error instanceof Error ? error.message : String(error); if (loadPromise) try { await loadPromise; } catch (loadFailure) { loadError ??= loadFailure instanceof Error ? loadFailure.message : String(loadFailure); } }
  let summary = null; try { summary = JSON.parse(await readFile(loadOutput, "utf8")); } catch { /* technical failure is kept in the output row */ }
  let tracing = {}; try { tracing = await collect(condition, summary); } catch (error) { faultError ??= `metric collection: ${error instanceof Error ? error.message : String(error)}`; }
  try { await execute(process.execPath, ["tools/generate-trace-diagram.mjs", "--condition", condition, "--endpoint", condition === "conventional-otel" ? "http://127.0.0.1:16686" : "http://127.0.0.1:16687", "--output", join(runDir, "trace-diagram.svg")], { quiet: true }); } catch { /* fault result stays useful without a diagram */ }
  const scheduled = summary?.scheduled ?? null; const successful = summary?.successful ?? null;
  rows.push({ run_id: runId, condition, scenario, target_service: target, repetition, status: faultError || loadError ? "technical-failure" : "completed", started_at: startedAt, finished_at: new Date().toISOString(), scheduled_requests: scheduled, successful_requests: successful, success_rate_percent: scheduled ? 100 * successful / scheduled : null, successful_throughput_rps: summary?.successfulThroughputRps ?? null, transport_errors: summary?.transportErrors ?? null, recovery_time_ms: recoveryTimeMs, backlog_drain_time_ms: drainTimeMs, ...tracing, error: [faultError, loadError].filter(Boolean).join(" | ") || null, run_directory: runDir });
  try { await execute("docker", [...compose, "down", "--remove-orphans"], { quiet: true }); } catch { /* preserve the collected result, but leave cleanup visible to Docker */ }
}
const headers = [...new Set(rows.flatMap((row) => Object.keys(row)))];
await writeFile(join(outputRoot, "fault-results.json"), `${JSON.stringify({ generatedAt: new Date().toISOString(), descriptor: descriptor.id, faultAfterSeconds, outageSeconds, rows }, null, 2)}\n`);
await writeFile(join(outputRoot, "fault-results.csv"), `${headers.join(",")}\n${rows.map((row) => headers.map((header) => csv(row[header])).join(",")).join("\n")}\n`);
await writeFile(join(outputRoot, "fault-manifest.json"), `${JSON.stringify({ purpose: "Failure and recovery experiment; do not merge with healthy primary performance results.", conditions, scenarios, descriptor: descriptor.id, repetitions, faultAfterSeconds, outageSeconds, generatedAt: new Date().toISOString() }, null, 2)}\n`);
console.log(`Fault matrix completed: ${outputRoot}`);
