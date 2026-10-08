import { randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";

const ITERATIONS = Number(process.env.PROFILE_ITERATIONS ?? 10_000);
const REPETITIONS = Number(process.env.PROFILE_REPETITIONS ?? 5);
const WARMUP = Number(process.env.PROFILE_WARMUP ?? 2_000);

const RESULT_DIR =
  process.env.PROFILE_RESULT_DIR ??
  "results/tracing-profile-v2";

const ASYNC_BATCH_SIZES = [100, 1_000, 10_000];

/*
 * IMPORTANT
 *
 * This profiling script models the tracing operations based on the
 * implementation supplied for the thesis benchmark.
 *
 * It does NOT claim to execute the production tracing implementation.
 *
 * Both conditions intentionally use the SAME number of logical spans
 * so that the profiling comparison isolates tracing mechanism overhead
 * rather than the number of instrumented operations.
 *
 * The topology is kept explicit so that the topology can be audited
 * independently from the timing results.
 */

/* -------------------------------------------------------------------------- */
/* Topology                                                                   */
/* -------------------------------------------------------------------------- */

/*
 * Shared logical topology
 *
 * Both conventional and proposed conditions model the same 7 logical
 * operations:
 *
 * Gateway handler
 * └── Gateway service.buildQuote
 *     ├── Catalog service
 *     │   └── Catalog repository
 *     │
 *     ├── Inventory service
 *     │   └── Inventory repository
 *     │
 *     └── Gateway service.createQuote
 *
 * = 7 logical spans / distributed request
 *
 * IMPORTANT:
 * The topology represents the same logical execution path for both
 * tracing mechanisms. It does NOT mean that the production conventional
 * implementation necessarily creates these exact 7 spans.
 */

/*
 * Conventional tracing
 *
 * 7 logical spans:
 *
 * Gateway handler
 * └── Gateway service.buildQuote
 *     ├── Catalog service
 *     │   └── Catalog repository
 *     │
 *     ├── Inventory service
 *     │   └── Inventory repository
 *     │
 *     └── Gateway service.createQuote
 *
 * Each logical operation is represented by one conventional span.
 */
const CONVENTIONAL_SPANS = [
  {
    id: "gateway.handler.quote",
    name: "gateway.handler.quote",
    parent: null,
    kind: "server"
  },
  {
    id: "gateway.service.buildQuote",
    name: "gateway.service.buildQuote",
    parent: "gateway.handler.quote",
    kind: "internal"
  },
  {
    id: "catalog.service.getProduct",
    name: "catalog.service.getProduct",
    parent: "gateway.service.buildQuote",
    kind: "internal"
  },
  {
    id: "catalog.repository.findProduct",
    name: "catalog.repository.findProduct",
    parent: "catalog.service.getProduct",
    kind: "internal"
  },
  {
    id: "inventory.service.checkAvailability",
    name: "inventory.service.checkAvailability",
    parent: "gateway.service.buildQuote",
    kind: "internal"
  },
  {
    id: "inventory.repository.readStock",
    name: "inventory.repository.readStock",
    parent: "inventory.service.checkAvailability",
    kind: "internal"
  },
  {
    id: "gateway.service.createQuote",
    name: "gateway.service.createQuote",
    parent: "gateway.service.buildQuote",
    kind: "internal"
  }
];

/*
 * Proposed internal tracing
 *
 * The SAME 7 logical operations are represented.
 *
 * Each logical span produces:
 *   - 1 ENTRY event
 *   - 1 EXIT event
 *
 * Therefore:
 *
 *   7 logical spans
 *   14 tracing events
 *
 * The inter-service HTTP propagation is deliberately NOT added as a
 * separate proposed span because the supplied tracing/proposed.ts
 * does not create HTTP client spans.
 */
const PROPOSED_SPANS = [
  {
    id: "gateway.handler.quote",
    name: "gateway.handler.quote",
    parent: null,
    layer: "handler"
  },
  {
    id: "gateway.service.buildQuote",
    name: "gateway.service.buildQuote",
    parent: "gateway.handler.quote",
    layer: "service"
  },
  {
    id: "catalog.service.getProduct",
    name: "catalog.service.getProduct",
    parent: "gateway.service.buildQuote",
    layer: "service"
  },
  {
    id: "catalog.repository.findProduct",
    name: "catalog.repository.findProduct",
    parent: "catalog.service.getProduct",
    layer: "repository"
  },
  {
    id: "inventory.service.checkAvailability",
    name: "inventory.service.checkAvailability",
    parent: "gateway.service.buildQuote",
    layer: "service"
  },
  {
    id: "inventory.repository.readStock",
    name: "inventory.repository.readStock",
    parent: "inventory.service.checkAvailability",
    layer: "repository"
  },
  {
    id: "gateway.service.createQuote",
    name: "gateway.service.createQuote",
    parent: "gateway.service.buildQuote",
    layer: "service"
  }
];

const PROPOSED_EVENTS_PER_SPAN = 2;

/* -------------------------------------------------------------------------- */
/* Payloads                                                                   */
/* -------------------------------------------------------------------------- */

const PAYLOADS = {
  small: {
    extra: {}
  },

  medium: {
    extra: {
      workload: {
        type: "O(N)",
        inputSize: 512,
        structuralSignature: {
          serviceCount: 2,
          functionCount: 7,
          callDepth: 4
        }
      }
    }
  },

  large: {
    extra: {
      workload: {
        type: "mixed",
        inputSize: 4096,
        structuralSignature: {
          serviceCount: 2,
          functionCount: 7,
          callDepth: 5,
          branches: 4,
          interServiceCalls: 2
        },
        metadata: "x".repeat(1024)
      }
    }
  }
};

/* -------------------------------------------------------------------------- */
/* Business workload                                                          */
/* -------------------------------------------------------------------------- */

/*
 * Deterministic business workload.
 *
 * IMPORTANT:
 * - The exact same workload is executed by conventional and proposed.
 * - Business time is measured separately from tracing operations.
 * - The workload itself does not perform tracing.
 */

/* -------------------------------------------------------------------------- */
/* Statistics                                                                 */
/* -------------------------------------------------------------------------- */

function createMetrics() {
  return {
    // Business
    business_time: [],

    // Request path
    total_request_path: [],

    // Tracing
    context_lookup: [],
    span_id_generation: [],
    timestamp_generation: [],
    event_creation: [],
    serialization: [],
    submission_initiation: [],
    queue_enqueue: [],

    // Async processing
    queue_dequeue: [],
    reconstruction: [],
    async_tail: [],

    // Resource usage
    cpu_user_ms: [],
    cpu_system_ms: [],
    cpu_total_ms: [],
    rss_delta_bytes: [],
    heap_used_delta_bytes: [],
    external_delta_bytes: [],
    array_buffers_delta_bytes: []
  };
}

function percentile(values, p) {
  if (!values.length) {
    return null;
  }

  const sorted = [...values].sort((a, b) => a - b);

  const index = Math.min(
    sorted.length - 1,
    Math.max(0, Math.ceil((p / 100) * sorted.length) - 1)
  );

  return sorted[index];
}

function summarize(values) {
  if (!values.length) {
    return {
      count: 0,
      mean: null,
      p50: null,
      p95: null,
      p99: null,
      min: null,
      max: null
    };
  }

  let sum = 0;
  let min = Infinity;
  let max = -Infinity;

  for (const value of values) {
    sum += value;

    if (value < min) {
      min = value;
    }

    if (value > max) {
      max = value;
    }
  }

  return {
    count: values.length,
    mean: sum / values.length,
    p50: percentile(values, 50),
    p95: percentile(values, 95),
    p99: percentile(values, 99),
    min,
    max
  };
}

/* -------------------------------------------------------------------------- */
/* Business workload                                                          */
/* -------------------------------------------------------------------------- */

function executeBusinessWorkload(payload) {
  const workload = payload.extra?.workload;

  // O(1)
  if (!workload) {
    return 42;
  }

  const inputSize =
    workload.inputSize ?? 512;

  if (workload.type === "O(N)") {
    let result = 0;

    for (
      let i = 0;
      i < inputSize;
      i++
    ) {
      result =
        (result + ((i * 31) % 997)) %
        1_000_003;
    }

    return result;
  }

  if (workload.type === "mixed") {
    const values =
      new Array(inputSize);

    // O(N)
    for (
      let i = 0;
      i < inputSize;
      i++
    ) {
      values[i] =
        ((i * 31) ^ (i >>> 2)) %
        100_003;
    }

    // O(N log N)
    values.sort(
      (a, b) => a - b
    );

    // O(N)
    let result = 0;

    for (
      let i = 0;
      i < values.length;
      i++
    ) {
      if (i % 4 === 0) {
        result += values[i];
      } else {
        result ^= values[i];
      }
    }

    return result;
  }

  return 42;
}

/* -------------------------------------------------------------------------- */
/* Resource measurement                                                      */
/* -------------------------------------------------------------------------- */

function readResourceSnapshot() {
  const memory =
    process.memoryUsage();

  const cpu =
    process.resourceUsage();

  return {
    cpuUserMs:
      cpu.userCPUTime / 1000,

    cpuSystemMs:
      cpu.systemCPUTime / 1000,

    rssBytes:
      memory.rss,

    heapUsedBytes:
      memory.heapUsed,

    externalBytes:
      memory.external,

    arrayBuffersBytes:
      memory.arrayBuffers
  };
}

function recordResourceUsage(
  metrics,
  before,
  after
) {
  metrics.cpu_user_ms.push(
    after.cpuUserMs -
      before.cpuUserMs
  );

  metrics.cpu_system_ms.push(
    after.cpuSystemMs -
      before.cpuSystemMs
  );

  metrics.cpu_total_ms.push(
    (after.cpuUserMs +
      after.cpuSystemMs) -
    (before.cpuUserMs +
      before.cpuSystemMs)
  );

  metrics.rss_delta_bytes.push(
    after.rssBytes -
      before.rssBytes
  );

  metrics.heap_used_delta_bytes.push(
    after.heapUsedBytes -
      before.heapUsedBytes
  );

  metrics.external_delta_bytes.push(
    after.externalBytes -
      before.externalBytes
  );

  metrics.array_buffers_delta_bytes.push(
    after.arrayBuffersBytes -
      before.arrayBuffersBytes
  );
}

/* -------------------------------------------------------------------------- */
/* Primitive tracing operations                                               */
/* -------------------------------------------------------------------------- */

function randomSpanId() {
  return randomUUID();
}

function createSpan({
  traceId,
  spanId,
  parentSpanId,
  operation,
  kind,
  payload
}) {
  return {
    traceId,
    spanId,
    parentSpanId,
    serviceName: "profiling",
    operation,
    kind,
    startedAt: new Date().toISOString(),
    endedAt: new Date().toISOString(),
    ...payload.extra
  };
}

function createEvent({
  requestId,
  spanId,
  parentSpanId,
  operation,
  layer,
  eventType
}) {
  return {
    request_id: requestId,
    span_id: spanId,
    parent_span_id: parentSpanId,
    event_type: eventType,
    function_name: operation,
    layer,
    service_name: "profiling",
    start_timestamp:
      eventType === "ENTRY"
        ? new Date().toISOString()
        : null,
    end_timestamp:
      eventType === "EXIT"
        ? new Date().toISOString()
        : null,
    outcome:
      eventType === "EXIT"
        ? "ok"
        : null
  };
}

/* -------------------------------------------------------------------------- */
/* Conventional                                                               */
/* -------------------------------------------------------------------------- */

function profileConventional(
  iterations,
  payload
) {
  const metrics =
    createMetrics();

  const resourceBefore =
    readResourceSnapshot();

  for (
    let request = 0;
    request < iterations;
    request++
  ) {
    const requestStarted =
      performance.now();

    /*
     * ------------------------------------------------------------
     * BUSINESS
     * ------------------------------------------------------------
     */

    const businessStarted =
      performance.now();

    executeBusinessWorkload(
      payload
    );

    metrics.business_time.push(
      performance.now() -
        businessStarted
    );

    /*
     * ------------------------------------------------------------
     * TRACING
     * ------------------------------------------------------------
     */

    const traceId =
      randomUUID();

    for (
      const operation of
      CONVENTIONAL_SPANS
    ) {
      const spanIdStarted =
        performance.now();

      const spanId =
        randomSpanId();

      metrics.span_id_generation.push(
        performance.now() -
          spanIdStarted
      );

      const timestampStarted =
        performance.now();

      const startedAt =
        new Date().toISOString();

      metrics.timestamp_generation.push(
        performance.now() -
          timestampStarted
      );

      const creationStarted =
        performance.now();

      const span =
        createSpan({
          traceId,
          spanId,
          parentSpanId:
            resolveConventionalParent(
              operation
            ),
          operation:
            operation.name,
          kind:
            operation.kind,
          payload
        });

      span.startedAt =
        startedAt;

      metrics.event_creation.push(
        performance.now() -
          creationStarted
      );

      const serializationStarted =
        performance.now();

      const body =
        JSON.stringify(span);

      metrics.serialization.push(
        performance.now() -
          serializationStarted
      );

      const submissionStarted =
        performance.now();

      /*
       * Local submission placeholder.
       * No actual network export.
       */
      void body.length;

      metrics.submission_initiation.push(
        performance.now() -
          submissionStarted
      );
    }

    /*
     * ------------------------------------------------------------
     * TOTAL REQUEST PATH
     * ------------------------------------------------------------
     */

    metrics.total_request_path.push(
      performance.now() -
        requestStarted
    );
  }

  const resourceAfter =
    readResourceSnapshot();

  recordResourceUsage(
    metrics,
    resourceBefore,
    resourceAfter
  );

  return metrics;
}

function resolveConventionalParent(operation) {
  return operation.parent;
}

/* -------------------------------------------------------------------------- */
/* Proposed                                                                   */
/* -------------------------------------------------------------------------- */

/*
 * We don't retain every event globally.
 *
 * Instead, one synthetic trace is generated per profiling request and
 * immediately fed into the async-tail profiler when required.
 *
 * This prevents the profiling script itself from becoming a memory
 * benchmark.
 */
function generateProposedTrace(payload) {
  const requestId = randomUUID();

  const events = [];
  const spanIds = new Map();

  for (const operation of PROPOSED_SPANS) {
    const contextStarted = performance.now();

    const parentSpanId =
      operation.parent === null
        ? null
        : spanIds.get(operation.parent);

    const contextLookupMs =
      performance.now() - contextStarted;

    const spanIdStarted = performance.now();

    const spanId = randomSpanId();

    const spanIdGenerationMs =
      performance.now() - spanIdStarted;

    spanIds.set(operation.id, spanId);

    const entryCreationStarted = performance.now();

    const entry = createEvent({
      requestId,
      spanId,
      parentSpanId,
      operation: operation.name,
      layer: operation.layer,
      eventType: "ENTRY"
    });

    const entryCreationMs =
      performance.now() - entryCreationStarted;

    const entrySerializationStarted = performance.now();

    const entryBody = JSON.stringify({
      ...entry,
      ...payload.extra
    });

    const entrySerializationMs =
      performance.now() - entrySerializationStarted;

    const entrySubmissionStarted = performance.now();

    void entryBody.length;

    const entrySubmissionMs =
      performance.now() - entrySubmissionStarted;

    const entryEnqueueStarted = performance.now();

    events.push(entry);

    const entryEnqueueMs =
      performance.now() - entryEnqueueStarted;

    /*
     * EXIT event uses the same span ID and parent span ID.
     */
    const exitCreationStarted = performance.now();

    const exit = createEvent({
      requestId,
      spanId,
      parentSpanId,
      operation: operation.name,
      layer: operation.layer,
      eventType: "EXIT"
    });

    const exitCreationMs =
      performance.now() - exitCreationStarted;

    const exitSerializationStarted = performance.now();

    const exitBody = JSON.stringify({
      ...exit,
      ...payload.extra
    });

    const exitSerializationMs =
      performance.now() - exitSerializationStarted;

    const exitSubmissionStarted = performance.now();

    void exitBody.length;

    const exitSubmissionMs =
      performance.now() - exitSubmissionStarted;

    const exitEnqueueStarted = performance.now();

    events.push(exit);

    const exitEnqueueMs =
      performance.now() - exitEnqueueStarted;

    yieldEventMetric(
      metrics,
      "context_lookup",
      contextLookupMs
    );

    yieldEventMetric(
      metrics,
      "span_id_generation",
      spanIdGenerationMs
    );

    yieldEventMetric(
      metrics,
      "event_creation",
      entryCreationMs
    );

    yieldEventMetric(
      metrics,
      "event_creation",
      exitCreationMs
    );

    yieldEventMetric(
      metrics,
      "serialization",
      entrySerializationMs
    );

    yieldEventMetric(
      metrics,
      "serialization",
      exitSerializationMs
    );

    yieldEventMetric(
      metrics,
      "submission_initiation",
      entrySubmissionMs
    );

    yieldEventMetric(
      metrics,
      "submission_initiation",
      exitSubmissionMs
    );

    yieldEventMetric(
      metrics,
      "queue_enqueue",
      entryEnqueueMs
    );

    yieldEventMetric(
      metrics,
      "queue_enqueue",
      exitEnqueueMs
    );
  }

  return events;
}

/*
 * Small helper to avoid coupling trace generation to a specific metrics
 * object while keeping the actual proposed topology generation readable.
 */
function yieldEventMetric() {
  /*
   * Intentionally empty.
   *
   * Actual metric collection is performed by profileProposed().
   */
}

function profileProposed(
  iterations,
  payload
) {
  const metrics =
    createMetrics();

  const traces = [];

  const resourceBefore =
    readResourceSnapshot();

  for (
    let request = 0;
    request < iterations;
    request++
  ) {
    const requestStarted =
      performance.now();

    /*
     * ------------------------------------------------------------
     * BUSINESS
     * ------------------------------------------------------------
     */

    const businessStarted =
      performance.now();

    executeBusinessWorkload(
      payload
    );

    metrics.business_time.push(
      performance.now() -
        businessStarted
    );

    /*
     * ------------------------------------------------------------
     * PROPOSED TRACING
     * ------------------------------------------------------------
     */

    const requestId =
      randomUUID();

    const spanIds =
      new Map();

    const requestEvents = [];

    for (
      const operation of
      PROPOSED_SPANS
    ) {
      /*
       * Context lookup
       */
      const contextStarted =
        performance.now();

      const parentSpanId =
        operation.parent === null
          ? null
          : spanIds.get(
              operation.parent
            );

      metrics.context_lookup.push(
        performance.now() -
          contextStarted
      );

      /*
       * Span ID
       */
      const spanIdStarted =
        performance.now();

      const spanId =
        randomSpanId();

      metrics.span_id_generation.push(
        performance.now() -
          spanIdStarted
      );

      spanIds.set(
        operation.id,
        spanId
      );

      /*
       * ENTRY
       */
      const entryCreationStarted =
        performance.now();

      const entry =
        createEvent({
          requestId,
          spanId,
          parentSpanId,
          operation:
            operation.name,
          layer:
            operation.layer,
          eventType:
            "ENTRY"
        });

      metrics.event_creation.push(
        performance.now() -
          entryCreationStarted
      );

      const entrySerializationStarted =
        performance.now();

      const entryBody =
        JSON.stringify({
          ...entry,
          ...payload.extra
        });

      metrics.serialization.push(
        performance.now() -
          entrySerializationStarted
      );

      const entrySubmissionStarted =
        performance.now();

      void entryBody.length;

      metrics.submission_initiation.push(
        performance.now() -
          entrySubmissionStarted
      );

      const entryEnqueueStarted =
        performance.now();

      requestEvents.push(entry);

      metrics.queue_enqueue.push(
        performance.now() -
          entryEnqueueStarted
      );

      /*
       * EXIT
       */
      const exitCreationStarted =
        performance.now();

      const exit =
        createEvent({
          requestId,
          spanId,
          parentSpanId,
          operation:
            operation.name,
          layer:
            operation.layer,
          eventType:
            "EXIT"
        });

      metrics.event_creation.push(
        performance.now() -
          exitCreationStarted
      );

      const exitSerializationStarted =
        performance.now();

      const exitBody =
        JSON.stringify({
          ...exit,
          ...payload.extra
        });

      metrics.serialization.push(
        performance.now() -
          exitSerializationStarted
      );

      const exitSubmissionStarted =
        performance.now();

      void exitBody.length;

      metrics.submission_initiation.push(
        performance.now() -
          exitSubmissionStarted
      );

      const exitEnqueueStarted =
        performance.now();

      requestEvents.push(exit);

      metrics.queue_enqueue.push(
        performance.now() -
          exitEnqueueStarted
      );
    }

    traces.push(
      requestEvents
    );

    /*
     * TOTAL REQUEST PATH
     */
    metrics.total_request_path.push(
      performance.now() -
        requestStarted
    );
  }

  const resourceAfter =
    readResourceSnapshot();

  recordResourceUsage(
    metrics,
    resourceBefore,
    resourceAfter
  );

  return {
    metrics,
    traces
  };
}

/* -------------------------------------------------------------------------- */
/* Reconstruction                                                             */
/* -------------------------------------------------------------------------- */

function reconstruct(events) {
  const spans = new Map();

  for (const event of events) {
    let span = spans.get(event.span_id);

    if (!span) {
      span = {
        span_id: event.span_id,
        parent_span_id: event.parent_span_id,
        function_name: event.function_name,
        layer: event.layer,
        entry: null,
        exit: null
      };

      spans.set(event.span_id, span);
    }

    if (event.event_type === "ENTRY") {
      span.entry = event;
    } else if (event.event_type === "EXIT") {
      span.exit = event;
    }
  }

  return spans;
}

function validateReconstruction(
  reconstructed,
  expectedSpanCount
) {
  if (reconstructed.size !== expectedSpanCount) {
    return false;
  }

  for (const span of reconstructed.values()) {
    if (!span.entry || !span.exit) {
      return false;
    }

    if (span.entry.span_id !== span.exit.span_id) {
      return false;
    }

    if (
      span.entry.parent_span_id !==
      span.exit.parent_span_id
    ) {
      return false;
    }
  }

  return true;
}

/* -------------------------------------------------------------------------- */
/* Async tail                                                                */
/* -------------------------------------------------------------------------- */

function profileAsyncTail(
  traces,
  batchSize,
  expectedSpanCount
) {
  const metrics = createMetrics();

  let successfulTraces = 0;
  let failedTraces = 0;

  /*
   * Reconstruction is performed on complete traces.
   *
   * batchSize controls how many traces are processed per batch,
   * not how many events from a single trace are arbitrarily split.
   */
  for (
    let offset = 0;
    offset < traces.length;
    offset += batchSize
  ) {
    const batch = traces.slice(
      offset,
      offset + batchSize
    );

    const dequeueStarted =
      performance.now();

    /*
     * In this profiler, the batch has already been materialized.
     *
     * Therefore this measures local batch handling overhead,
     * not an actual network queue dequeue.
     */
    void batch.length;

    metrics.queue_dequeue.push(
      performance.now() -
        dequeueStarted
    );

    const reconstructionStarted =
      performance.now();

    for (const trace of batch) {
      const reconstructed =
        reconstruct(trace);

      if (
        validateReconstruction(
          reconstructed,
          expectedSpanCount
        )
      ) {
        successfulTraces += 1;
      } else {
        failedTraces += 1;
      }
    }

    metrics.reconstruction.push(
      performance.now() -
        reconstructionStarted
    );
  }

  const dequeueTotal =
    metrics.queue_dequeue.reduce(
      (sum, value) => sum + value,
      0
    );

  const reconstructionTotal =
    metrics.reconstruction.reduce(
      (sum, value) => sum + value,
      0
    );

  metrics.async_tail.push(
    dequeueTotal +
      reconstructionTotal
  );

  return {
    metrics,
    successfulTraces,
    failedTraces,
    totalTraces: traces.length,
    reconstructionSuccessRate:
      traces.length
        ? successfulTraces / traces.length
        : 0
  };
}

/* -------------------------------------------------------------------------- */
/* Flatten                                                                    */
/* -------------------------------------------------------------------------- */

const METRIC_UNITS = {
  business_time: "ms",
  total_request_path: "ms",

  context_lookup: "ms",
  span_id_generation: "ms",
  timestamp_generation: "ms",
  event_creation: "ms",
  serialization: "ms",
  submission_initiation: "ms",
  queue_enqueue: "ms",

  queue_dequeue: "ms",
  reconstruction: "ms",
  async_tail: "ms",

  cpu_user_ms: "ms",
  cpu_system_ms: "ms",
  cpu_total_ms: "ms",

  rss_delta_bytes: "bytes",
  heap_used_delta_bytes: "bytes",
  external_delta_bytes: "bytes",
  array_buffers_delta_bytes: "bytes"
};

function flatten({
  condition,
  payload,
  repetition,
  batchSize,
  metrics
}) {
  return Object.entries(metrics).map(
    ([operation, values]) => {
      const stats =
        summarize(values);

      return {
        condition,
        payload,
        repetition,
        batch_size:
          batchSize ?? "",

        operation,

        unit:
          METRIC_UNITS[
            operation
          ] ?? "unknown",

        count:
          stats.count,

        mean:
          stats.mean,

        p50:
          stats.p50,

        p95:
          stats.p95,

        p99:
          stats.p99,

        min:
          stats.min,

        max:
          stats.max
      };
    }
  );
}

/* -------------------------------------------------------------------------- */
/* CSV                                                                        */
/* -------------------------------------------------------------------------- */

function csvEscape(value) {
  return JSON.stringify(
    value ?? ""
  );
}

function toCsv(rows) {
  if (!rows.length) {
    return "";
  }

  const headers =
    Object.keys(rows[0]);

  const lines = [
    headers.join(",")
  ];

  for (const row of rows) {
    lines.push(
      headers
        .map((header) =>
          csvEscape(row[header])
        )
        .join(",")
    );
  }

  return lines.join("\n");
}

/* -------------------------------------------------------------------------- */
/* Validation                                                                 */
/* -------------------------------------------------------------------------- */

function validateTopology() {
  const conventionalIds =
    new Set(
      CONVENTIONAL_SPANS.map(
        (span) => span.id
      )
    );

  for (const span of CONVENTIONAL_SPANS) {
    if (
      span.parent !== null &&
      !conventionalIds.has(span.parent)
    ) {
      throw new Error(
        `Invalid conventional parent: ${span.id} -> ${span.parent}`
      );
    }
  }

  const proposedIds =
    new Set(
      PROPOSED_SPANS.map(
        (span) => span.id
      )
    );

  for (const span of PROPOSED_SPANS) {
    if (
      span.parent !== null &&
      !proposedIds.has(span.parent)
    ) {
      throw new Error(
        `Invalid proposed parent: ${span.id} -> ${span.parent}`
      );
    }
  }

  if (
    CONVENTIONAL_SPANS.filter(
      (span) => span.parent === null
    ).length !== 1
  ) {
    throw new Error(
      "Conventional topology must have exactly one root."
    );
  }

  if (
    PROPOSED_SPANS.filter(
      (span) => span.parent === null
    ).length !== 1
  ) {
    throw new Error(
      "Proposed topology must have exactly one root."
    );
  }
}

/* -------------------------------------------------------------------------- */
/* Main                                                                       */
/* -------------------------------------------------------------------------- */

async function main() {
  validateTopology();

  console.log(
    "========================================"
  );

  console.log(
    "Tracing Profiling V2 - Topology Aware"
  );

  console.log(
    "========================================"
  );

  console.log(
    `iterations : ${ITERATIONS}`
  );

  console.log(
    `repetitions: ${REPETITIONS}`
  );

  console.log(
    `warmup     : ${WARMUP}`
  );

  console.log(
    `conventional spans/request: ${
      CONVENTIONAL_SPANS.length
    }`
  );

  console.log(
    `proposed logical spans/request: ${
      PROPOSED_SPANS.length
    }`
  );

  console.log(
    `proposed events/request: ${
      PROPOSED_SPANS.length *
      PROPOSED_EVENTS_PER_SPAN
    }`
  );

  console.log(
    "\nConventional topology:"
  );

  for (const span of CONVENTIONAL_SPANS) {
    console.log(
      `  ${span.id} <- ${
        span.parent ?? "ROOT"
      }`
    );
  }

  console.log(
    "\nProposed topology:"
  );

  for (const span of PROPOSED_SPANS) {
    console.log(
      `  ${span.id} <- ${
        span.parent ?? "ROOT"
      }`
    );
  }

  console.log(
    "\nWarm-up..."
  );

  for (
    const payload of
    Object.values(PAYLOADS)
  ) {
    profileConventional(
      WARMUP,
      payload
    );

    profileProposed(
      WARMUP,
      payload
    );
  }

  const rows = [];

  for (
    let repetition = 1;
    repetition <= REPETITIONS;
    repetition++
  ) {
    console.log(
      `\n========== Repetition ${repetition}/${REPETITIONS} ==========`
    );

    const conditions =
      repetition % 2 === 1
        ? [
            "conventional",
            "proposed"
          ]
        : [
            "proposed",
            "conventional"
          ];

    for (
      const [
        payloadName,
        payload
      ] of Object.entries(
        PAYLOADS
      )
    ) {
      console.log(
        `\nPayload: ${payloadName}`
      );

      for (
        const condition of
        conditions
      ) {
        console.log(
          `  ${condition}...`
        );

        if (
          condition ===
          "conventional"
        ) {
          const result =
            profileConventional(
              ITERATIONS,
              payload
            );

          rows.push(
            ...flatten({
              condition,
              payload:
                payloadName,
              repetition,
              metrics:
                result
            })
          );

          continue;
        }

        const result =
          profileProposed(
            ITERATIONS,
            payload
          );

        rows.push(
          ...flatten({
            condition,
            payload:
              payloadName,
            repetition,
            metrics:
              result.metrics
          })
        );

        for (
          const batchSize of
          ASYNC_BATCH_SIZES
        ) {
          const tail =
            profileAsyncTail(
              result.traces,
              batchSize,
              PROPOSED_SPANS.length
            );

          rows.push(
            ...flatten({
              condition:
                "proposed_async_tail",
              payload:
                payloadName,
              repetition,
              batchSize,
              metrics: {
                queue_dequeue:
                  tail.metrics
                    .queue_dequeue,

                reconstruction:
                  tail.metrics
                    .reconstruction,

                async_tail:
                  tail.metrics
                    .async_tail
              }
            })
          );

          rows.push({
            condition:
              "proposed_async_tail_summary",
          
            payload:
              payloadName,
          
            repetition,
          
            batch_size:
              batchSize,
          
            operation:
              "reconstruction_success_rate",
          
            unit:
              "ratio",
          
            count:
              tail.totalTraces,
          
            mean:
              tail.reconstructionSuccessRate,
          
            p50: null,
            p95: null,
            p99: null,
            min: null,
            max: null
          });
        }
      }
    }
  }

  await mkdir(
    RESULT_DIR,
    {
      recursive: true
    }
  );

  const timestamp =
    new Date()
      .toISOString()
      .replace(
        /[:.]/g,
        "-"
      );

  const csvPath =
    join(
      RESULT_DIR,
      `profile-v2-topology-${timestamp}.csv`
    );

  const jsonPath =
    join(
      RESULT_DIR,
      `profile-v2-topology-${timestamp}.json`
    );

  await writeFile(
    csvPath,
    toCsv(rows)
  );

  await writeFile(
    jsonPath,
    JSON.stringify(
      {
        schemaVersion:
          "2.2.0",

          methodology: {
            purpose:
              "Topology-aware local tracing microbenchmark with controlled business workload and process-level resource measurement",
          
            importantLimitation:
              "This profiler models tracing operations from the benchmark implementation; it does not execute the production tracing implementation itself.",
          
            businessWorkloadIncluded:
              true,
          
            businessWorkloadDescription:
              "The same synthetic business workload is executed for conventional and proposed conditions before tracing operations.",
          
            resourceMeasurementScope:
              "Per profiling run",
          
            resourceMetrics: [
              "cpu_user_ms",
              "cpu_system_ms",
              "cpu_total_ms",
              "rss_delta_bytes",
              "heap_used_delta_bytes",
              "external_delta_bytes",
              "array_buffers_delta_bytes"
            ],
          
            networkExportIncluded:
              false,
          
            queueNetworkIncluded:
              false,
          
            queueDequeueInterpretation:
              "Local batch handling simulation, not actual network queue dequeue."
          },

        topology: {
          conventional: {
            spansPerRequest:
              CONVENTIONAL_SPANS.length,

            operations:
              CONVENTIONAL_SPANS
          },

          proposed: {
            logicalSpansPerRequest:
              PROPOSED_SPANS.length,

            eventsPerSpan:
              PROPOSED_EVENTS_PER_SPAN,

            eventsPerRequest:
              PROPOSED_SPANS.length *
              PROPOSED_EVENTS_PER_SPAN,

            operations:
              PROPOSED_SPANS
          }
        },

        configuration: {
          iterations:
            ITERATIONS,

          repetitions:
            REPETITIONS,

          warmup:
            WARMUP,

          payloads:
            Object.keys(
              PAYLOADS
            ),

          batchSizes:
            ASYNC_BATCH_SIZES
        },

        results: rows
      },
      null,
      2
    )
  );

  console.log(
    "\n========================================"
  );

  console.log(
    "DONE"
  );

  console.log(
    "========================================"
  );

  console.log(csvPath);
  console.log(jsonPath);
}

main().catch(
  (error) => {
    console.error(error);
    process.exitCode = 1;
  }
);
