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
function containerInternalSnapshots(ids) {
  const snapshots = {};
  for (const id of ids) {
    try {
      const service = run("docker", ["inspect", "--format", "{{index .Config.Labels \"com.docker.compose.service\"}}", id]);
      const output = run("docker", ["exec", id, "node", "-e", "fetch('http://127.0.0.1:3000/v1/internal-observability').then(r=>r.text()).then(console.log)"]);
      snapshots[service || id] = JSON.parse(output);
    } catch { /* a diagnostic endpoint may be unavailable while a container is stopping */ }
  }
  return snapshots;
}
function resetContainerInternalMetrics(ids) {
  for (const id of ids) {
    try { run("docker", ["exec", id, "node", "-e", "fetch('http://127.0.0.1:3000/v1/internal-observability/reset',{method:'POST'}).then(()=>process.exit(0)).catch(()=>process.exit(1))"]); } catch { /* best effort; exposed endpoints are reset separately */ }
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
async function post(url) {
  const response = await fetch(url, { method: "POST" });
  if (!response.ok) throw new Error(`Could not POST ${url}: HTTP ${response.status}`);
  return response.json();
}
async function internalSnapshot(urls) {
  const entries = await Promise.all(Object.entries(urls).map(async ([name, url]) => {
    try { return [name, await json(url)]; } catch { return [name, null]; }
  }));
  return Object.fromEntries(entries);
}
async function resetInternal(urls) { await Promise.all(Object.values(urls).map(async (url) => { try { await post(`${url}/reset`); } catch { /* disabled or unavailable diagnostic endpoint */ } })); }
function counter(metrics, name) { return metrics?.counters?.[name] ?? 0; }
function timing(metrics, name) { return metrics?.timings?.[name] ?? { count: 0, mean: null, min: null, p50: null, p95: null, p99: null, max: null }; }
function sumCounter(metrics, name) { return Object.values(metrics).reduce((total, metric) => total + counter(metric, name), 0); }

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
const internalObservability = option("--internal-observability", process.env.INTERNAL_OBSERVABILITY ?? "false") === "true";
if (!descriptorPath || !condition) throw new Error("Usage: node tools/run-experiment.mjs --descriptor FILE --condition baseline|conventional|proposed|proposed-memory|proposed-durable [--base-url URL]");
if (!["baseline", "conventional", "proposed", "proposed-memory", "proposed-durable"].includes(condition)) throw new Error("condition must be baseline, conventional, proposed, proposed-memory, or proposed-durable");
if (!Number.isFinite(saturationThresholdPercent) || saturationThresholdPercent <= 0 || saturationThresholdPercent > 100) throw new Error("--saturation-threshold-percent must be greater than 0 and at most 100.");
const recordMode = condition === "proposed-memory" || condition === "proposed-durable";
const selectedQueueMetricsUrl = recordMode ? queueMetricsUrl.replace(/\/v1\/metrics$/, "/v2/metrics") : queueMetricsUrl;
const selectedReconstructionMetricsUrl = recordMode ? reconstructionMetricsUrl.replace(/\/v1\/metrics$/, "/v2/metrics") : reconstructionMetricsUrl;

const descriptorAbsolutePath = resolve(descriptorPath);
const descriptorText = await readFile(descriptorAbsolutePath, "utf8");
const descriptor = JSON.parse(descriptorText);
if (descriptor.workloadDescriptorVersion !== "1.0.0") throw new Error("Unsupported workload descriptor version.");
const runId = `${new Date().toISOString().replace(/[:.]/g, "-")}-${condition}-${randomUUID().slice(0, 8)}`;
const runDir = join(resultsDir, runId);
await mkdir(runDir, { recursive: true });
await mkdir(join(runDir, "primary"), { recursive: true });
await mkdir(join(runDir, "observability"), { recursive: true });
await copyFile(descriptorAbsolutePath, join(runDir, "workload.json"));

const composeFiles = condition === "conventional"
  ? ["compose.yaml", "compose.conventional.yaml"]
  : condition === "proposed" ? ["compose.yaml", "compose.proposed.yaml"]
  : condition === "proposed-memory" ? ["compose.yaml", "compose.proposed-memory.yaml"]
  : condition === "proposed-durable" ? ["compose.yaml", "compose.proposed-durable.yaml"]
    : ["compose.yaml"];
const ids = containerIds(project, composeFiles);
const hostCapacity = dockerHostCapacity();
const manifest = {
  schemaVersion: "2.0.0", runId, condition, status: "running", startedAt: new Date().toISOString(),
  descriptorId: descriptor.id, descriptorSha256: createHash("sha256").update(descriptorText).digest("hex"),
  baseUrl, composeProject: project, composeFiles, containers: ids, hostCapacity, internalObservability, command: process.argv.slice(2),
  measurementBoundary: {
    version: "gateway-business-boundary-v1",
    requestLatency: "load-generator schedules request to complete HTTP response receipt",
    businessLatency: "gateway immediately before buildQuote invocation to buildQuote resolution; includes all work that actually executes on this request critical path",
    conventionalExport: "gateway-to-service client export is inside businessLatency; server-span export executes in Fastify onResponse after businessLatency and remains included in external requestLatency",
    proposedInstrumentation: "wrapper context/UUID/timestamp/event construction/JSON serialization/fetch initiation execute on request critical path when reached; detached transport completion, queue, worker, and reconstruction are outside businessLatency",
    asynchronousObservability: "post-submission queue admission/worker/reconstruction, reported separately and never added to businessLatency"
  },
  measurementClassification: {
    primary: ["request_latency", "business_latency", "achieved_rps", "successful_requests", "cpu", "memory"],
    secondary: ["success_rate", "generator_drop_rate", "queue_drop_rate", "reconstruction_success_rate"],
    diagnostic: ["wrapper", "event_capture", "serialization", "submission", "queue", "worker", "reconstruction"]
  }
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
  if (condition === "proposed" || recordMode) {
    const settled = await waitForProposedDrain(selectedQueueMetricsUrl, selectedReconstructionMetricsUrl, traceDrainTimeoutSeconds);
    if (!settled.drained) throw new Error("Could not drain proposed tracing events after warm-up; measurement was not started.");
    proposedBefore = { queue: settled.queue, reconstruction: settled.reconstruction };
  }
  const internalUrls = condition === "proposed" || recordMode
    ? { gateway: `${baseUrl}/v1/internal-observability`, queue: "http://127.0.0.1:16686/v1/internal-observability", worker: "http://127.0.0.1:16687/v1/internal-observability" }
    : condition === "conventional"
      ? { gateway: `${baseUrl}/v1/internal-observability`, collector: "http://127.0.0.1:16686/v1/internal-observability" }
      : { gateway: `${baseUrl}/v1/internal-observability` };
  const gatewayDiagnosticState = await json(internalUrls.gateway);
  if (gatewayDiagnosticState.enabled !== internalObservability) throw new Error(`Diagnostic mode mismatch: runner requested INTERNAL_OBSERVABILITY=${internalObservability}, gateway reports ${gatewayDiagnosticState.enabled}. Restart the selected Compose stack with the same setting.`);
  manifest.diagnosticModeVerified = gatewayDiagnosticState.enabled;
  if (internalObservability) { await resetInternal(internalUrls); resetContainerInternalMetrics(ids); }

  const child = spawn(process.execPath, ["tools/load-generator.mjs", "--descriptor", descriptorAbsolutePath, "--base-url", baseUrl, "--skip-warmup", "--output", join(runDir, "http-summary.json")], { stdio: "inherit" });
  await waitForChild(child);
  const http = JSON.parse(await readFile(join(runDir, "http-summary.json"), "utf8"));
  const primaryPerformance = { schemaVersion: "1.0.0", measurementBoundary: manifest.measurementBoundary.version, condition, workload_id: descriptor.id, run_id: runId, achieved_rps: http.achievedRps, successful_requests: http.successfulRequests, request_latency_ms: http.latencyMs, business_latency_ms: http.businessProcessingMs };
  await writeFile(join(runDir, "latency.json"), `${JSON.stringify(primaryPerformance, null, 2)}\n`);
  await writeFile(join(runDir, "primary", "performance.json"), `${JSON.stringify(primaryPerformance, null, 2)}\n`);
  await writeFile(join(runDir, "primary", "latency.csv"), `condition,workload_id,run_id,achieved_rps,successful_requests,request_latency_mean_ms,request_latency_p50_ms,request_latency_p95_ms,request_latency_p99_ms,business_latency_mean_ms,business_latency_p50_ms,business_latency_p95_ms,business_latency_p99_ms\n${condition},${descriptor.id},${runId},${http.achievedRps},${http.successfulRequests},${http.latencyMs.mean},${http.latencyMs.p50},${http.latencyMs.p95},${http.latencyMs.p99},${http.businessProcessingMs.mean},${http.businessProcessingMs.p50},${http.businessProcessingMs.p95},${http.businessProcessingMs.p99}\n`);
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
  let saturationSignals = [];
  if (condition === "proposed" || recordMode) {
    const drain = await waitForProposedDrain(selectedQueueMetricsUrl, selectedReconstructionMetricsUrl, traceDrainTimeoutSeconds);
    const queue = delta(drain.queue, proposedBefore.queue, recordMode ? ["accepted", "dropped", "queued", "duplicates"] : ["accepted", "dropped", "queued", "dequeued"]);
    const reconstruction = delta(drain.reconstruction, proposedBefore.reconstruction, recordMode ? ["observedTraces", "completeTraces", "reconstructedSpans"] : ["observedRequests", "completeTraces", "reconstructedSpans", "events", "incompleteSpans"]);
    const expectedRequests = http.successfulRequests;
    const eventsProduced = queue.accepted + queue.dropped;
    const metrics = {
      schemaVersion: "1.1.0", capturedAt: new Date().toISOString(), drained: drain.drained,
      measurementOnly: true, expectedRequests,
      events: { produced: eventsProduced, enqueued: queue.accepted, dropped: queue.dropped, reconstructed: recordMode ? reconstruction.reconstructedSpans : reconstruction.events },
      queue, reconstruction,
      queueDropRatePercent: eventsProduced ? (queue.dropped / eventsProduced) * 100 : null,
      reconstructionSuccessRatePercent: expectedRequests ? (reconstruction.completeTraces / expectedRequests) * 100 : null,
      ...(recordMode ? { tracingUnit: "execution_record", duplicateRecords: queue.duplicates } : {})
    };
    await writeFile(join(runDir, "tracing-metrics.json"), `${JSON.stringify(metrics, null, 2)}\n`);
    await writeFile(join(runDir, "observability", "events.json"), `${JSON.stringify(metrics.events, null, 2)}\n`);
    await writeFile(join(runDir, "observability", "queue.json"), `${JSON.stringify({ queue: metrics.queue, queueDropRatePercent: metrics.queueDropRatePercent }, null, 2)}\n`);
    await writeFile(join(runDir, "observability", "reconstruction.json"), `${JSON.stringify({ reconstruction: metrics.reconstruction, reconstructionSuccessRatePercent: metrics.reconstructionSuccessRatePercent }, null, 2)}\n`);
    if (queue.dropped > 0) saturationSignals.push("queue_drops_observed");
    if (!drain.drained) saturationSignals.push("async_pipeline_not_drained");
  }
  if (internalObservability) {
    const exposedServices = await internalSnapshot(internalUrls);
    const services = { ...containerInternalSnapshots(ids), ...exposedServices };
    const gateway = services.gateway;
    const queueService = services.queue;
    const worker = services.worker;
    const collector = services.collector;
    const queueMetrics = condition === "proposed" || recordMode ? await json(selectedQueueMetricsUrl) : null;
    const collectorMetrics = condition === "conventional" ? await json("http://127.0.0.1:16686/v1/metrics") : null;
    const applicationServices = Object.fromEntries(Object.entries(services).filter(([name]) => ["gateway", "catalog", "inventory"].includes(name)));
    const eventsProduced = sumCounter(applicationServices, "EVENT_CAPTURE.events_produced");
    const submitted = sumCounter(applicationServices, "EVENT_SUBMISSION.initiated");
    const enqueued = counter(queueService, "QUEUE.enqueue_success");
    const dropped = counter(queueService, "QUEUE.enqueue_dropped");
    const dequeued = counter(worker, "WORKER.events_dequeued");
    const reconstructed = counter(worker, "RECONSTRUCTION.success");
    const internal = {
      schemaVersion: "1.0.0", diagnosticMode: true, measurementOnly: true, labels: { unit: "ms", scope: "measurement" },
      request: { request_latency: http.latencyMs, critical_path_tracing_overhead: timing(gateway, "WRAPPER.total_overhead_ms"), per_request: gateway?.per_request ?? null },
      business: { duration_ms: http.businessProcessingMs, handler_duration_ms: timing(gateway, "BUSINESS.handler_duration_ms"), service_duration_ms: timing(gateway, "BUSINESS.service_duration_ms"), repository_duration_ms: timing(gateway, "BUSINESS.repository_duration_ms") },
      wrapper: { pre_ms: timing(gateway, "WRAPPER.pre_ms"), post_ms: timing(gateway, "WRAPPER.post_ms"), total_ms: timing(gateway, "WRAPPER.total_overhead_ms"), context_lookup_ms: timing(gateway, "CONTEXT.lookup_ms"), span_id_generation_ms: timing(gateway, "WRAPPER.span_id_generation_ms"), timestamp_generation_ms: timing(gateway, "WRAPPER.timestamp_generation_ms"), async_local_storage_ms: timing(gateway, "WRAPPER.async_local_storage_run_ms") },
      event_capture: { events_produced: eventsProduced, entry_events: sumCounter(applicationServices, "EVENT_CAPTURE.entry_events"), exit_events: sumCounter(applicationServices, "EVENT_CAPTURE.exit_events"), events_per_request: http.successfulRequests ? eventsProduced / http.successfulRequests : null, capture_duration_ms: timing(gateway, "EVENT_CAPTURE.duration_ms") },
      serialization: { count: sumCounter(applicationServices, "SERIALIZATION.duration_ms"), duration_ms: timing(gateway, "SERIALIZATION.duration_ms"), bytes: sumCounter(applicationServices, "SERIALIZATION.bytes"), bytes_per_event: eventsProduced ? sumCounter(applicationServices, "SERIALIZATION.bytes") / eventsProduced : null, bytes_per_request: http.successfulRequests ? sumCounter(applicationServices, "SERIALIZATION.bytes") / http.successfulRequests : null },
      event_submission: { attempts: sumCounter(applicationServices, "EVENT_SUBMISSION.attempts"), initiated: submitted, errors: sumCounter(applicationServices, "EVENT_SUBMISSION.errors") + sumCounter(applicationServices, "EVENT_SUBMISSION.initiation_errors"), init_duration_ms: timing(gateway, "EVENT_SUBMISSION.init_duration_ms"), bytes: sumCounter(applicationServices, "EVENT_SUBMISSION.bytes") },
      queue: queueMetrics ? { capacity: queueMetrics.capacity, depth: queueMetrics.queued, depth_mean: queueMetrics.queueDepthMean, depth_max: queueMetrics.queueDepthMax, utilization: queueMetrics.queueUtilization, utilization_max: queueMetrics.queueUtilizationMax, admission_duration_ms: timing(queueService, "QUEUE.enqueue_duration_ms"), wait_ms: timing(queueService, "QUEUE.wait_ms"), enqueue_attempts: counter(queueService, "QUEUE.enqueue_attempts"), enqueue_success: enqueued, enqueue_dropped: dropped, enqueue_drop_rate: enqueued + dropped ? dropped / (enqueued + dropped) : null } : null,
      worker: worker ? { batches: counter(worker, "WORKER.batches"), events_dequeued: dequeued, events_processed: counter(worker, "WORKER.events_processed"), batch_duration_ms: timing(worker, "WORKER.batch_duration_ms"), processing_duration_ms: timing(worker, "WORKER.processing_duration_ms"), busy_time_ms: timing(worker, "WORKER.processing_duration_ms").total, idle_time_ms: timing(worker, "WORKER.idle_time_ms").total, throughput_events_per_second: timing(worker, "WORKER.processing_duration_ms").total ? (counter(worker, "WORKER.events_processed") * 1000) / timing(worker, "WORKER.processing_duration_ms").total : null } : null,
      reconstruction: condition === "conventional" ? { attempts: collectorMetrics?.observedTraces ?? null, success: collectorMetrics?.completeTraces ?? null, failed: collectorMetrics ? collectorMetrics.observedTraces - collectorMetrics.completeTraces : null, success_rate: collectorMetrics?.observedTraces ? collectorMetrics.completeTraces / collectorMetrics.observedTraces : null, duration_ms: collectorMetrics?.reconstructionDurationMs ?? null } : { attempts: counter(worker, "RECONSTRUCTION.attempts"), success: reconstructed, failed: counter(worker, "RECONSTRUCTION.failed"), success_rate: counter(worker, "RECONSTRUCTION.attempts") ? reconstructed / counter(worker, "RECONSTRUCTION.attempts") : null, duration_ms: timing(worker, "RECONSTRUCTION.duration_ms") },
      event_flow: condition === "conventional" ? { spans_created: sumCounter(applicationServices, "CONVENTIONAL.spans_created"), spans_exported: sumCounter(applicationServices, "CONVENTIONAL.export_success"), spans_received: collectorMetrics?.spansReceived ?? null, spans_processed: collectorMetrics?.spansProcessed ?? null, traces_reconstructed: collectorMetrics?.completeTraces ?? null } : { produced: eventsProduced, submitted, enqueue_success: enqueued, enqueue_dropped: dropped, dequeued, reconstructed, submission_loss_rate: eventsProduced ? 1 - submitted / eventsProduced : null, queue_drop_rate: enqueued + dropped ? dropped / (enqueued + dropped) : null, processing_loss_rate: enqueued ? 1 - dequeued / enqueued : null, overall_trace_event_retention: eventsProduced ? reconstructed / eventsProduced : null },
      system: { container_metrics_file: "container-metrics.json", services },
      correlation: { target_rps: descriptor.request.rateRps, achieved_rps: http.achievedRps, computational_signature: descriptor.computational?.signature ?? null, structural_signature: descriptor.structural?.signature ?? null, input_size: descriptor.inputSize ?? descriptor.computational?.inputSize ?? null, events_per_request: http.successfulRequests ? eventsProduced / http.successfulRequests : null, queue_wait_p95: timing(queueService, "QUEUE.wait_ms").p95, request_p95: http.latencyMs?.p95, request_p99: http.latencyMs?.p99, business_p95: http.businessProcessingMs?.p95 }
    };
    await writeFile(join(runDir, "internal-observability.json"), `${JSON.stringify(internal, null, 2)}\n`);
    await writeFile(join(runDir, "observability", "instrumentation.json"), `${JSON.stringify(internal, null, 2)}\n`);
  }
  if (successRatePercent < saturationThresholdPercent) saturationSignals.push("success_rate_below_threshold");
  if (achievedTargetRatePercent < saturationThresholdPercent) saturationSignals.push("achieved_rps_below_threshold");
  const isSaturated = saturationSignals.length > 0;
  const diagnosticArtifactPresent = Boolean(await readFile(join(runDir, "internal-observability.json"), "utf8").catch(() => null));
  manifest.measurementValidation = {
    ...manifest.measurementValidation,
    measurementBoundaryValid: true,
    workloadConsistent: true,
    conditionValid: true,
    diagnosticModeConsistent: manifest.diagnosticModeVerified === internalObservability,
    primaryMetricsComplete: [http.latencyMs, http.businessProcessingMs].every((metric) => metric?.samples > 0 && Number.isFinite(metric.mean) && Number.isFinite(metric.p95)),
    observabilityMetricsComplete: !internalObservability || diagnosticArtifactPresent,
    diagnosticInstrumentationEnabled: internalObservability,
    diagnosticArtifactsAbsent: internalObservability || !diagnosticArtifactPresent,
    comparisonEligible: !internalObservability,
    saturated: isSaturated,
    saturationSignals,
    valid: null,
    invalidReasons: []
  };
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
  const resourceArtifact = { schemaVersion: "1.0.0", intervalSeconds: 1, samples };
  const primaryMetricsComplete = Boolean(manifest.measurementValidation?.primaryMetricsComplete);
  const resourceMetricsComplete = samples.length >= 2 && !samplingError;
  if (manifest.measurementValidation) {
    manifest.measurementValidation.resourceMetricsComplete = resourceMetricsComplete;
    manifest.measurementValidation.valid = manifest.status !== "failed" && primaryMetricsComplete && resourceMetricsComplete && manifest.measurementValidation.measurementBoundaryValid && manifest.measurementValidation.workloadConsistent && manifest.measurementValidation.conditionValid && manifest.measurementValidation.diagnosticModeConsistent && manifest.measurementValidation.diagnosticArtifactsAbsent;
    manifest.measurementValidation.invalidReasons = [
      ...(primaryMetricsComplete ? [] : ["primary_metrics_missing"]),
      ...(resourceMetricsComplete ? [] : [samplingError ? "resource_sampling_error" : "insufficient_resource_samples"]),
      ...(manifest.measurementValidation.observabilityMetricsComplete === false ? ["diagnostic_artifact_missing"] : []),
      ...(manifest.measurementValidation.diagnosticArtifactsAbsent === false ? ["diagnostic_artifact_present_in_primary_mode"] : [])
    ];
  }
  await writeFile(join(runDir, "container-metrics.json"), `${JSON.stringify(resourceArtifact, null, 2)}\n`);
  await writeFile(join(runDir, "resource.json"), `${JSON.stringify(resourceArtifact, null, 2)}\n`);
  await writeFile(join(runDir, "primary", "resource.json"), `${JSON.stringify(resourceArtifact, null, 2)}\n`);
  await writeFile(join(runDir, "primary", "resource.csv"), `run_id,condition,resource_sample_count,resource_sampling_error\n${runId},${condition},${samples.length},${samplingError ? JSON.stringify(samplingError) : ""}\n`);
  await writeFile(join(runDir, "manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`);
}
console.log(`Experiment complete: ${runDir}`);
