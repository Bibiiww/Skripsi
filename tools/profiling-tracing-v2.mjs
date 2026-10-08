import { randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";

const ITERATIONS = Number(
  process.env.PROFILE_ITERATIONS ?? 10_000
);

const REPETITIONS = Number(
  process.env.PROFILE_REPETITIONS ?? 5
);

const WARMUP = Number(
  process.env.PROFILE_WARMUP ?? 2_000
);

const RESULT_DIR =
  process.env.PROFILE_RESULT_DIR ??
  "results/tracing-profile-v2";

/*
 * Actual logical topology of the benchmark.
 *
 * Conventional:
 *
 * Gateway SERVER
 * ├── Catalog CLIENT
 * │   └── Catalog SERVER
 * │
 * └── Inventory CLIENT
 *     └── Inventory SERVER
 *
 * = 5 spans
 *
 * Proposed:
 *
 * Gateway HANDLER
 * └── Gateway SERVICE buildQuote
 *     ├── Gateway SERVICE createQuote
 *     ├── Catalog SERVICE
 *     │   └── Catalog REPOSITORY
 *     └── Inventory SERVICE
 *         └── Inventory REPOSITORY
 *
 * = 7 logical spans
 * = 14 ENTRY/EXIT events
 */

const CONVENTIONAL_OPERATIONS = [
  {
    name: "gateway.server",
    kind: "server"
  },
  {
    name: "gateway.catalog.client",
    kind: "client"
  },
  {
    name: "catalog.server",
    kind: "server"
  },
  {
    name: "gateway.inventory.client",
    kind: "client"
  },
  {
    name: "inventory.server",
    kind: "server"
  }
];

const PROPOSED_OPERATIONS = [
  {
    name: "gateway.handler.quote",
    layer: "handler"
  },
  {
    name: "gateway.service.buildQuote",
    layer: "service"
  },
  {
    name: "gateway.service.createQuote",
    layer: "service"
  },
  {
    name: "catalog.service.getProduct",
    layer: "service"
  },
  {
    name: "catalog.repository.findProduct",
    layer: "repository"
  },
  {
    name: "inventory.service.checkAvailability",
    layer: "service"
  },
  {
    name: "inventory.repository.readStock",
    layer: "repository"
  }
];

/*
 * Payloads are intentionally small/medium/large.
 *
 * The topology remains identical.
 * Only metadata size changes.
 */
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

const BATCH_SIZES = [
  100,
  1_000,
  10_000
];

/* -------------------------------------------------------------------------- */
/* Helpers                                                                    */
/* -------------------------------------------------------------------------- */

function createMetrics() {
  return {
    total_request_path: [],

    context_lookup: [],

    span_id_generation: [],

    timestamp_generation: [],

    event_creation: [],

    serialization: [],

    submission_initiation: [],

    queue_enqueue: [],

    queue_dequeue: [],

    reconstruction: [],

    async_tail: []
  };
}

function percentile(values, p) {
  if (!values.length) {
    return null;
  }

  const sorted = [...values].sort(
    (a, b) => a - b
  );

  const index = Math.min(
    sorted.length - 1,
    Math.ceil(
      (p / 100) * sorted.length
    ) - 1
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

  const sum = values.reduce(
    (a, b) => a + b,
    0
  );

  return {
    count: values.length,

    mean: sum / values.length,

    p50: percentile(values, 50),

    p95: percentile(values, 95),

    p99: percentile(values, 99),

    min: Math.min(...values),

    max: Math.max(...values)
  };
}

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

    startedAt:
      new Date().toISOString(),

    endedAt:
      new Date().toISOString(),

    ...payload.extra
  };
}

function createEvent({
  requestId,
  spanId,
  parentSpanId,
  operation,
  layer,
  eventType,
  payload
}) {
  return {
    request_id: requestId,

    span_id: spanId,

    parent_span_id:
      parentSpanId,

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
        : null,

    ...payload.extra
  };
}

/* -------------------------------------------------------------------------- */
/* Conventional                                                              */
/* -------------------------------------------------------------------------- */

function profileConventional(
  iterations,
  payload
) {
  const metrics =
    createMetrics();

  for (
    let request = 0;
    request < iterations;
    request++
  ) {
    const requestStarted =
      performance.now();

    const traceId =
      randomUUID();

    /*
     * Five actual logical spans.
     */
    for (
      const operation of
      CONVENTIONAL_OPERATIONS
    ) {
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

      /*
       * Timestamp
       */
      const timestampStarted =
        performance.now();

      const startedAt =
        new Date().toISOString();

      metrics.timestamp_generation.push(
        performance.now() -
        timestampStarted
      );

      /*
       * Span creation
       */
      const creationStarted =
        performance.now();

      const span =
        createSpan({
          traceId,
          spanId,
          parentSpanId: null,
          operation:
            operation.name,
          kind:
            operation.kind,
          payload
        });

      /*
       * Make timestamp relevant
       * to the constructed object.
       */
      span.startedAt =
        startedAt;

      metrics.event_creation.push(
        performance.now() -
        creationStarted
      );

      /*
       * Serialization
       */
      const serializationStarted =
        performance.now();

      const body =
        JSON.stringify(span);

      metrics.serialization.push(
        performance.now() -
        serializationStarted
      );

      /*
       * Synchronous export
       *
       * We intentionally measure only
       * local submission preparation here.
       *
       * Actual network RTT belongs to
       * the application-level benchmark.
       */
      const submissionStarted =
        performance.now();

      void body.length;

      metrics.submission_initiation.push(
        performance.now() -
        submissionStarted
      );
    }

    metrics.total_request_path.push(
      performance.now() -
      requestStarted
    );
  }

  return metrics;
}

/* -------------------------------------------------------------------------- */
/* Proposed                                                                  */
/* -------------------------------------------------------------------------- */

function profileProposed(
  iterations,
  payload
) {
  const metrics =
    createMetrics();

  const events = [];

  for (
    let request = 0;
    request < iterations;
    request++
  ) {
    const requestStarted =
      performance.now();

    const requestId =
      randomUUID();

    let parentSpanId =
      null;

    /*
     * Seven logical spans.
     *
     * Each logical span generates:
     *
     * ENTRY
     * EXIT
     */
    for (
      const operation of
      PROPOSED_OPERATIONS
    ) {
      /*
       * Context lookup
       */
      const contextStarted =
        performance.now();

      void parentSpanId;

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
          eventType: "ENTRY",
          payload
        });

      metrics.event_creation.push(
        performance.now() -
        entryCreationStarted
      );

      /*
       * ENTRY serialization
       */
      const entrySerializationStarted =
        performance.now();

      const entryBody =
        JSON.stringify(entry);

      metrics.serialization.push(
        performance.now() -
        entrySerializationStarted
      );

      /*
       * Async submission initiation
       */
      const entrySubmissionStarted =
        performance.now();

      void entryBody.length;

      metrics.submission_initiation.push(
        performance.now() -
        entrySubmissionStarted
      );

      /*
       * Queue enqueue
       */
      const entryEnqueueStarted =
        performance.now();

      events.push(entry);

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
          eventType: "EXIT",
          payload
        });

      metrics.event_creation.push(
        performance.now() -
        exitCreationStarted
      );

      /*
       * EXIT serialization
       */
      const exitSerializationStarted =
        performance.now();

      const exitBody =
        JSON.stringify(exit);

      metrics.serialization.push(
        performance.now() -
        exitSerializationStarted
      );

      /*
       * EXIT submission
       */
      const exitSubmissionStarted =
        performance.now();

      void exitBody.length;

      metrics.submission_initiation.push(
        performance.now() -
        exitSubmissionStarted
      );

      /*
       * EXIT queue enqueue
       */
      const exitEnqueueStarted =
        performance.now();

      events.push(exit);

      metrics.queue_enqueue.push(
        performance.now() -
        exitEnqueueStarted
      );

      /*
       * Child becomes parent for
       * the next logical operation.
       */
      parentSpanId =
        spanId;
    }

    metrics.total_request_path.push(
      performance.now() -
      requestStarted
    );
  }

  return {
    metrics,
    events
  };
}

/* -------------------------------------------------------------------------- */
/* Reconstruction                                                             */
/* -------------------------------------------------------------------------- */

function reconstruct(events) {
  const spans =
    new Map();

  for (
    const event of events
  ) {
    let span =
      spans.get(event.span_id);

    if (!span) {
      span = {
        span_id:
          event.span_id,

        parent_span_id:
          event.parent_span_id,

        function_name:
          event.function_name,

        entry: null,

        exit: null
      };

      spans.set(
        event.span_id,
        span
      );
    }

    if (
      event.event_type ===
      "ENTRY"
    ) {
      span.entry =
        event;
    }

    if (
      event.event_type ===
      "EXIT"
    ) {
      span.exit =
        event;
    }
  }

  return spans;
}

function profileAsyncTail(
  events,
  batchSize
) {
  const metrics =
    createMetrics();

  let offset = 0;

  while (
    offset <
    events.length
  ) {
    const batch =
      events.slice(
        offset,
        offset + batchSize
      );

    offset +=
      batch.length;

    const dequeueStarted =
      performance.now();

    void batch.length;

    metrics.queue_dequeue.push(
      performance.now() -
      dequeueStarted
    );

    const reconstructionStarted =
      performance.now();

    reconstruct(batch);

    metrics.reconstruction.push(
      performance.now() -
      reconstructionStarted
    );
  }

  metrics.async_tail.push(
    metrics.queue_dequeue.reduce(
      (a, b) => a + b,
      0
    ) +
    metrics.reconstruction.reduce(
      (a, b) => a + b,
      0
    )
  );

  return metrics;
}

/* -------------------------------------------------------------------------- */
/* CSV                                                                        */
/* -------------------------------------------------------------------------- */

function flatten({
  condition,
  payload,
  repetition,
  batchSize,
  metrics
}) {
  return Object.entries(metrics)
    .map(
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

          count:
            stats.count,

          mean_ms:
            stats.mean,

          p50_ms:
            stats.p50,

          p95_ms:
            stats.p95,

          p99_ms:
            stats.p99,

          min_ms:
            stats.min,

          max_ms:
            stats.max
        };
      }
    );
}

function toCsv(rows) {
  if (!rows.length) {
    return "";
  }

  const headers =
    Object.keys(rows[0]);

  return [
    headers.join(","),

    ...rows.map(
      (row) =>
        headers
          .map(
            (header) =>
              JSON.stringify(
                row[header] ?? ""
              )
          )
          .join(",")
    )
  ].join("\n");
}

/* -------------------------------------------------------------------------- */
/* Main                                                                       */
/* -------------------------------------------------------------------------- */

async function main() {
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
      CONVENTIONAL_OPERATIONS.length
    }`
  );

  console.log(
    `proposed logical spans/request: ${
      PROPOSED_OPERATIONS.length
    }`
  );

  console.log(
    `proposed events/request: ${
      PROPOSED_OPERATIONS.length * 2
    }`
  );

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
      `\n========== Repetition ${
        repetition
      }/${REPETITIONS} ==========`
    );

    /*
     * Alternate order to reduce
     * systematic execution-order bias.
     */
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
      ] of Object.entries(PAYLOADS)
    ) {
      console.log(
        `\nPayload: ${payloadName}`
      );

      for (
        const condition of
        conditions
      ) {
        let result;

        console.log(
          `  ${condition}...`
        );

        if (
          condition ===
          "conventional"
        ) {
          result =
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

        result =
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

        /*
         * Async tail is profiled separately.
         */
        for (
          const batchSize of
          BATCH_SIZES
        ) {
          const tail =
            profileAsyncTail(
              result.events,
              batchSize
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
                  tail.queue_dequeue,

                reconstruction:
                  tail.reconstruction,

                async_tail:
                  tail.async_tail
              }
            })
          );
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
          "2.1.0",

        topology: {
          conventional: {
            spansPerRequest:
              CONVENTIONAL_OPERATIONS.length,

            operations:
              CONVENTIONAL_OPERATIONS
          },

          proposed: {
            logicalSpansPerRequest:
              PROPOSED_OPERATIONS.length,

            eventsPerSpan:
              2,

            eventsPerRequest:
              PROPOSED_OPERATIONS.length *
              2,

            operations:
              PROPOSED_OPERATIONS
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
            BATCH_SIZES
        },

        results:
          rows
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

  console.log(
    csvPath
  );

  console.log(
    jsonPath
  );
}

main().catch(
  (error) => {
    console.error(error);

    process.exitCode = 1;
  }
);

