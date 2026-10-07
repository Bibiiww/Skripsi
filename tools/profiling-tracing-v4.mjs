import { randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";

const ITERATIONS = Number(process.env.PROFILE_ITERATIONS ?? 10_000);
const REPETITIONS = Number(process.env.PROFILE_REPETITIONS ?? 5);
const WARMUP = Number(process.env.PROFILE_WARMUP ?? 2_000);

const RESULT_DIR =
  process.env.PROFILE_RESULT_DIR ??
  "results/tracing-profile-v4";

const ASYNC_BATCH_SIZES = [100, 1_000, 10_000];

const DEFAULT_QUEUE_CAPACITY =
  Number(process.env.PROFILE_QUEUE_CAPACITY ?? 0);

const RESOURCE_SAMPLE_INTERVAL =
  Number(process.env.PROFILE_RESOURCE_SAMPLE_INTERVAL ?? 100);

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
    conventional_span_creation: [],
    proposed_event_creation: [],
    serialization: [],
    serialization_bytes: [],
    serialization_object_count: [],
    trace_object_creation_ms_per_request: [],
    serialization_ms_per_request: [],
    serialization_bytes_per_request: [],
    queue_enqueue: [],

    // Async processing
    queue_dequeue: [],
    reconstruction: [],
    async_tail: [],

    // Resource usage
    cpu_user_ms: [],
    cpu_system_ms: [],
    cpu_total_ms: [],
    cpu_user_ms_per_request: [],
    cpu_system_ms_per_request: [],
    cpu_total_ms_per_request: [],
    rss_peak_bytes: [],
    rss_peak_delta_bytes: [],
    heap_used_peak_bytes: [],
    heap_used_peak_delta_bytes: [],
    external_peak_bytes: [],
    array_buffers_peak_bytes: []
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
  after,
  iterations,
  peak
) {
  const cpuUserMs =
    after.cpuUserMs - before.cpuUserMs;

  const cpuSystemMs =
    after.cpuSystemMs - before.cpuSystemMs;

  const cpuTotalMs =
    cpuUserMs + cpuSystemMs;

  metrics.cpu_user_ms.push(cpuUserMs);
  metrics.cpu_system_ms.push(cpuSystemMs);
  metrics.cpu_total_ms.push(cpuTotalMs);

  metrics.cpu_user_ms_per_request.push(
    iterations > 0 ? cpuUserMs / iterations : 0
  );

  metrics.cpu_system_ms_per_request.push(
    iterations > 0 ? cpuSystemMs / iterations : 0
  );

  metrics.cpu_total_ms_per_request.push(
    iterations > 0 ? cpuTotalMs / iterations : 0
  );

  metrics.rss_peak_bytes.push(peak.rssBytes);
  metrics.rss_peak_delta_bytes.push(
    peak.rssBytes - before.rssBytes
  );

  metrics.heap_used_peak_bytes.push(
    peak.heapUsedBytes
  );
  metrics.heap_used_peak_delta_bytes.push(
    peak.heapUsedBytes - before.heapUsedBytes
  );

  metrics.external_peak_bytes.push(
    peak.externalBytes
  );

  metrics.array_buffers_peak_bytes.push(
    peak.arrayBuffersBytes
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
/* Local bounded queue                                                        */
/* -------------------------------------------------------------------------- */

/*
 * This queue is intentionally local and in-memory.
 *
 * It replaces the V3 requestEvents.push() placeholder with an actual bounded
 * enqueue/dequeue mechanism. It is NOT a network queue and does not model
 * network transport latency.
 *
 * The default capacity is sized to the complete profiling run so the profiler
 * does not introduce artificial event loss unless PROFILE_QUEUE_CAPACITY is
 * explicitly configured below the produced event count.
 */
class BoundedEventQueue {
  constructor(capacity) {
    this.capacity = capacity;
    this.events = [];
    this.accepted = 0;
    this.dropped = 0;
  }

  enqueue(event) {
    const started = performance.now();

    if (this.events.length >= this.capacity) {
      this.dropped += 1;

      return {
        accepted: false,
        durationMs: performance.now() - started
      };
    }

    this.events.push(event);
    this.accepted += 1;

    return {
      accepted: true,
      durationMs: performance.now() - started
    };
  }

  dequeue(maxEvents) {
    const started = performance.now();
    const count = Math.max(
      0,
      Math.min(maxEvents, this.events.length)
    );

    const batch = this.events.splice(0, count);

    return {
      events: batch,
      durationMs: performance.now() - started
    };
  }

  get size() {
    return this.events.length;
  }

  snapshot() {
    return this.events.slice();
  }
}

/* -------------------------------------------------------------------------- */
/* Conventional                                                               */
/* -------------------------------------------------------------------------- */

function profileConventional(
  iterations,
  payload
) {
  const metrics = createMetrics();

  const resourceBefore =
    readResourceSnapshot();

  let resourcePeak = {
    ...resourceBefore
  };

  let requestCreationTime = 0;
  let requestSerializationTime = 0;
  let requestSerializationBytes = 0;
  let requestSerializationObjects = 0;

  for (
    let request = 0;
    request < iterations;
    request++
  ) {
    const requestStarted =
      performance.now();

    const businessStarted =
      performance.now();

    executeBusinessWorkload(
      payload
    );

    metrics.business_time.push(
      performance.now() -
        businessStarted
    );

    const traceId =
      randomUUID();

    requestCreationTime = 0;
    requestSerializationTime = 0;
    requestSerializationBytes = 0;
    requestSerializationObjects = 0;

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

      const creationMs =
        performance.now() -
        creationStarted;

      metrics.conventional_span_creation.push(
        creationMs
      );

      requestCreationTime +=
        creationMs;

      const serializationStarted =
        performance.now();

      const body =
        JSON.stringify(span);

      const serializationMs =
        performance.now() -
        serializationStarted;

      const bytes =
        Buffer.byteLength(body);

      metrics.serialization.push(
        serializationMs
      );
      metrics.serialization_bytes.push(
        bytes
      );
      metrics.serialization_object_count.push(
        1
      );

      requestSerializationTime +=
        serializationMs;
      requestSerializationBytes +=
        bytes;
      requestSerializationObjects +=
        1;

      /*
       * V4 deliberately does NOT measure the V3
       * submission_initiation placeholder.
       *
       * No network export is executed in this local profiler.
       */
    }

    metrics.trace_object_creation_ms_per_request.push(
      requestCreationTime
    );

    metrics.serialization_ms_per_request.push(
      requestSerializationTime
    );

    metrics.serialization_bytes_per_request.push(
      requestSerializationBytes
    );

    metrics.total_request_path.push(
      performance.now() -
        requestStarted
    );

    if (
      request % RESOURCE_SAMPLE_INTERVAL === 0 ||
      request === iterations - 1
    ) {
      const snapshot =
        readResourceSnapshot();

      resourcePeak = {
        ...resourcePeak,
        rssBytes: Math.max(
          resourcePeak.rssBytes,
          snapshot.rssBytes
        ),
        heapUsedBytes: Math.max(
          resourcePeak.heapUsedBytes,
          snapshot.heapUsedBytes
        ),
        externalBytes: Math.max(
          resourcePeak.externalBytes,
          snapshot.externalBytes
        ),
        arrayBuffersBytes: Math.max(
          resourcePeak.arrayBuffersBytes,
          snapshot.arrayBuffersBytes
        )
      };
    }
  }

  const resourceAfter =
    readResourceSnapshot();

  resourcePeak = {
    ...resourcePeak,
    rssBytes: Math.max(
      resourcePeak.rssBytes,
      resourceAfter.rssBytes
    ),
    heapUsedBytes: Math.max(
      resourcePeak.heapUsedBytes,
      resourceAfter.heapUsedBytes
    ),
    externalBytes: Math.max(
      resourcePeak.externalBytes,
      resourceAfter.externalBytes
    ),
    arrayBuffersBytes: Math.max(
      resourcePeak.arrayBuffersBytes,
      resourceAfter.arrayBuffersBytes
    )
  };

  recordResourceUsage(
    metrics,
    resourceBefore,
    resourceAfter,
    iterations,
    resourcePeak
  );

  return metrics;
}

function resolveConventionalParent(operation) {
  return operation.parent;
}

/* -------------------------------------------------------------------------- */
/* Proposed                                                                   */
/* -------------------------------------------------------------------------- */

function profileProposed(
  iterations,
  payload
) {
  const metrics = createMetrics();

  const eventsPerRequest =
    PROPOSED_SPANS.length *
    PROPOSED_EVENTS_PER_SPAN;

  const queueCapacity =
    DEFAULT_QUEUE_CAPACITY > 0
      ? DEFAULT_QUEUE_CAPACITY
      : Math.max(
          1,
          iterations * eventsPerRequest
        );

  const queue =
    new BoundedEventQueue(
      queueCapacity
    );

  const resourceBefore =
    readResourceSnapshot();

  let resourcePeak = {
    ...resourceBefore
  };

  let requestCreationTime = 0;
  let requestSerializationTime = 0;
  let requestSerializationBytes = 0;

  for (
    let request = 0;
    request < iterations;
    request++
  ) {
    const requestStarted =
      performance.now();

    const businessStarted =
      performance.now();

    executeBusinessWorkload(
      payload
    );

    metrics.business_time.push(
      performance.now() -
        businessStarted
    );

    const requestId =
      randomUUID();

    const spanIds =
      new Map();

    requestCreationTime = 0;
    requestSerializationTime = 0;
    requestSerializationBytes = 0;

    for (
      const operation of
      PROPOSED_SPANS
    ) {
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

      const entryCreationMs =
        performance.now() -
        entryCreationStarted;

      metrics.proposed_event_creation.push(
        entryCreationMs
      );

      requestCreationTime +=
        entryCreationMs;

      const entrySerializationStarted =
        performance.now();

      const entryBody =
        JSON.stringify({
          ...entry,
          ...payload.extra
        });

      const entrySerializationMs =
        performance.now() -
        entrySerializationStarted;

      const entryBytes =
        Buffer.byteLength(
          entryBody
        );

      metrics.serialization.push(
        entrySerializationMs
      );
      metrics.serialization_bytes.push(
        entryBytes
      );
      metrics.serialization_object_count.push(
        1
      );

      requestSerializationTime +=
        entrySerializationMs;
      requestSerializationBytes +=
        entryBytes;

      const entryEnqueue =
        queue.enqueue(entry);

      metrics.queue_enqueue.push(
        entryEnqueue.durationMs
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

      const exitCreationMs =
        performance.now() -
        exitCreationStarted;

      metrics.proposed_event_creation.push(
        exitCreationMs
      );

      requestCreationTime +=
        exitCreationMs;

      const exitSerializationStarted =
        performance.now();

      const exitBody =
        JSON.stringify({
          ...exit,
          ...payload.extra
        });

      const exitSerializationMs =
        performance.now() -
        exitSerializationStarted;

      const exitBytes =
        Buffer.byteLength(
          exitBody
        );

      metrics.serialization.push(
        exitSerializationMs
      );
      metrics.serialization_bytes.push(
        exitBytes
      );
      metrics.serialization_object_count.push(
        1
      );

      requestSerializationTime +=
        exitSerializationMs;
      requestSerializationBytes +=
        exitBytes;

      const exitEnqueue =
        queue.enqueue(exit);

      metrics.queue_enqueue.push(
        exitEnqueue.durationMs
      );
    }

    metrics.trace_object_creation_ms_per_request.push(
      requestCreationTime
    );

    metrics.serialization_ms_per_request.push(
      requestSerializationTime
    );

    metrics.serialization_bytes_per_request.push(
      requestSerializationBytes
    );

    metrics.total_request_path.push(
      performance.now() -
        requestStarted
    );

    if (
      request % RESOURCE_SAMPLE_INTERVAL === 0 ||
      request === iterations - 1
    ) {
      const snapshot =
        readResourceSnapshot();

      resourcePeak = {
        ...resourcePeak,
        rssBytes: Math.max(
          resourcePeak.rssBytes,
          snapshot.rssBytes
        ),
        heapUsedBytes: Math.max(
          resourcePeak.heapUsedBytes,
          snapshot.heapUsedBytes
        ),
        externalBytes: Math.max(
          resourcePeak.externalBytes,
          snapshot.externalBytes
        ),
        arrayBuffersBytes: Math.max(
          resourcePeak.arrayBuffersBytes,
          snapshot.arrayBuffersBytes
        )
      };
    }
  }

  const resourceAfter =
    readResourceSnapshot();

  resourcePeak = {
    ...resourcePeak,
    rssBytes: Math.max(
      resourcePeak.rssBytes,
      resourceAfter.rssBytes
    ),
    heapUsedBytes: Math.max(
      resourcePeak.heapUsedBytes,
      resourceAfter.heapUsedBytes
    ),
    externalBytes: Math.max(
      resourcePeak.externalBytes,
      resourceAfter.externalBytes
    ),
    arrayBuffersBytes: Math.max(
      resourcePeak.arrayBuffersBytes,
      resourceAfter.arrayBuffersBytes
    )
  };

  recordResourceUsage(
    metrics,
    resourceBefore,
    resourceAfter,
    iterations,
    resourcePeak
  );

  return {
    metrics,
    queue,
    queueEvents: queue.snapshot(),
    queueAccepted: queue.accepted,
    queueDropped: queue.dropped
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
  events,
  batchSize,
  expectedSpanCount,
  queueDropped = 0
) {
  const metrics = createMetrics();

  let successfulTraces = 0;
  let failedTraces = 0;

  const eventsPerTrace =
    expectedSpanCount *
    PROPOSED_EVENTS_PER_SPAN;

  /*
   * IMPORTANT:
   * Every batch-size experiment receives the SAME immutable snapshot
   * of accepted queue events. This prevents batch_size=100 from consuming
   * the events before batch_size=1000 and 10000 are measured.
   *
   * A fresh local queue is created for each batch-size run. Events are
   * preloaded without measuring enqueue time because enqueue was already
   * measured on the producer/request path.
   */
  const queue =
    new BoundedEventQueue(
      Math.max(events.length, 1)
    );

  queue.events = events.slice();
  queue.accepted = events.length;
  queue.dropped = queueDropped;

  const totalProducedEvents =
    events.length + queueDropped;

  const totalInputTraces =
    totalProducedEvents > 0
      ? Math.ceil(
          totalProducedEvents /
          eventsPerTrace
        )
      : 0;

  /*
   * The reconstruction worker keeps partial traces across dequeue batches.
   * This is important because a batch boundary may occur between ENTRY and
   * EXIT events belonging to the same request.
   */
  const pendingTraces = new Map();

  while (queue.size > 0) {
    const dequeueStarted =
      performance.now();

    const dequeued =
      queue.dequeue(
        batchSize *
        eventsPerTrace
      );

    metrics.queue_dequeue.push(
      performance.now() -
        dequeueStarted
    );

    const reconstructionStarted =
      performance.now();

    for (
      const event of
      dequeued.events
    ) {
      let trace =
        pendingTraces.get(
          event.request_id
        );

      if (!trace) {
        trace = [];
        pendingTraces.set(
          event.request_id,
          trace
        );
      }

      trace.push(event);

      /*
       * A complete trace contains exactly all expected ENTRY/EXIT events.
       * Reconstruct immediately when the complete event set is available.
       */
      if (
        trace.length >= eventsPerTrace
      ) {
        const reconstructed =
          reconstruct(trace);

        if (
          validateReconstruction(
            reconstructed,
            expectedSpanCount
          ) &&
          trace.length === eventsPerTrace
        ) {
          successfulTraces += 1;
        } else {
          failedTraces += 1;
        }

        pendingTraces.delete(
          event.request_id
        );
      }
    }

    metrics.reconstruction.push(
      performance.now() -
        reconstructionStarted
    );
  }

  /*
   * Any trace left in pendingTraces is incomplete. This can happen when
   * queue capacity caused event drops. Each incomplete request counts as
   * one failed reconstruction.
   */
  for (
    const trace of
    pendingTraces.values()
  ) {
    const reconstructed =
      reconstruct(trace);

    if (
      validateReconstruction(
        reconstructed,
        expectedSpanCount
      ) &&
      trace.length === eventsPerTrace
    ) {
      successfulTraces += 1;
    } else {
      failedTraces += 1;
    }
  }

  const totalReconstructedTraces =
    successfulTraces +
    failedTraces;

  const dequeueTotal =
    metrics.queue_dequeue.reduce(
      (sum, value) =>
        sum + value,
      0
    );

  const reconstructionTotal =
    metrics.reconstruction.reduce(
      (sum, value) =>
        sum + value,
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
    totalTraces:
      totalReconstructedTraces,
    totalInputTraces,
    reconstructionSuccessRate:
      totalReconstructedTraces
        ? successfulTraces /
          totalReconstructedTraces
        : 0,
    queueAccepted:
      events.length,
    queueDropped,
    queueRemaining:
      queue.size
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
  conventional_span_creation: "ms",
  proposed_event_creation: "ms",
  serialization: "ms",
  serialization_bytes: "bytes",
  serialization_object_count: "count",
  trace_object_creation_ms_per_request: "ms/request",
  serialization_ms_per_request: "ms/request",
  serialization_bytes_per_request: "bytes/request",
  queue_enqueue: "ms",

  queue_dequeue: "ms",
  reconstruction: "ms",
  async_tail: "ms",

  cpu_user_ms: "ms",
  cpu_system_ms: "ms",
  cpu_total_ms: "ms",
  cpu_user_ms_per_request: "ms/request",
  cpu_system_ms_per_request: "ms/request",
  cpu_total_ms_per_request: "ms/request",

  rss_peak_bytes: "bytes",
  rss_peak_delta_bytes: "bytes",
  heap_used_peak_bytes: "bytes",
  heap_used_peak_delta_bytes: "bytes",
  external_peak_bytes: "bytes",
  array_buffers_peak_bytes: "bytes"
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
    "Tracing Profiling V4.1 - Topology Aware"
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
          /*
           * batchSize is expressed in complete traces.
           * The local queue stores individual ENTRY/EXIT events.
           */
          const tail =
            profileAsyncTail(
              result.queueEvents,
              batchSize,
              PROPOSED_SPANS.length,
              result.queueDropped
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

          rows.push({
            condition:
              "proposed_async_tail_summary",

            payload:
              payloadName,

            repetition,

            batch_size:
              batchSize,

            operation:
              "queue_dropped",

            unit:
              "count",

            count: 1,

            mean:
              tail.queueDropped,

            p50: null,
            p95: null,
            p99: null,
            min:
              tail.queueDropped,
            max:
              tail.queueDropped
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
      `profile-v4-topology-${timestamp}.csv`
    );

  const jsonPath =
    join(
      RESULT_DIR,
      `profile-v4-topology-${timestamp}.json`
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
          "4.0.0",

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
              "Per profiling run with sampled process-level peak memory observation",
          
            resourceMetrics: [
              "cpu_user_ms",
              "cpu_system_ms",
              "cpu_total_ms",
              "cpu_user_ms_per_request",
              "cpu_system_ms_per_request",
              "cpu_total_ms_per_request",
              "rss_peak_bytes",
              "rss_peak_delta_bytes",
              "heap_used_peak_bytes",
              "heap_used_peak_delta_bytes",
              "external_peak_bytes",
              "array_buffers_peak_bytes"
            ],
          
            networkExportIncluded:
              false,

            submissionInitiationMeasured:
              false,

            serializationMeasurement:
              "Measured per serialized object with serialized byte count; normalized per request.",

            creationMeasurement:
              "Conventional span object creation and proposed ENTRY/EXIT event object creation are reported separately.",

            localBoundedQueueIncluded:
              true,

            localQueueCapacity:
              DEFAULT_QUEUE_CAPACITY > 0
                ? DEFAULT_QUEUE_CAPACITY
                : "iterations * eventsPerRequest",
          
            queueNetworkIncluded:
              false,
          
            queueDequeueInterpretation:
              "Local bounded in-memory queue dequeue; not actual network queue dequeue."
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
