import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";

const ITERATIONS = Number(process.env.PROFILE_ITERATIONS ?? 10_000);
const REPETITIONS = Number(process.env.PROFILE_REPETITIONS ?? 5);
const WARMUP = Number(process.env.PROFILE_WARMUP ?? 2_000);

const RESULT_DIR =
  process.env.PROFILE_RESULT_DIR ?? "results/tracing-profile-v2";

/*
 * Payload profiles.
 *
 * Tujuannya melihat apakah ukuran event mempengaruhi:
 * - event creation
 * - serialization
 * - request-path cost
 */
const PAYLOAD_PROFILES = {
  small: {
    name: "small",
    functionName: "service.checkAvailability",
    extra: {}
  },

  medium: {
    name: "medium",
    functionName: "service.checkAvailability",
    extra: {
      workload: {
        type: "O(N)",
        inputSize: 512,
        structuralSignature: {
          serviceCount: 1,
          functionCount: 3,
          callDepth: 3
        }
      }
    }
  },

  large: {
    name: "large",
    functionName: "service.checkAvailability",
    extra: {
      workload: {
        type: "mixed",
        inputSize: 4096,
        structuralSignature: {
          serviceCount: 3,
          functionCount: 9,
          callDepth: 5,
          branches: 4,
          interServiceCalls: 2
        },
        metadata: "x".repeat(512)
      }
    }
  }
};

const BATCH_SIZES = [
  100,
  1_000,
  10_000
];

const storage = new AsyncLocalStorage();

/* -------------------------------------------------------------------------- */
/* Utilities                                                                  */
/* -------------------------------------------------------------------------- */

function percentile(values, p) {
  if (!values.length) return null;

  const sorted = [...values].sort((a, b) => a - b);

  const index = Math.min(
    sorted.length - 1,
    Math.ceil((p / 100) * sorted.length) - 1
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

  const sum = values.reduce((acc, value) => acc + value, 0);

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

function createMetrics() {
  return {
    total_request_path: [],

    context_lookup: [],
    context_run: [],

    span_id_generation: [],
    timestamp_generation: [],
    span_event_creation: [],
    serialization: [],
    submission_initiation: [],
    queue_enqueue: [],

    queue_dequeue: [],
    reconstruction: [],
    async_tail: []
  };
}

function createEvent({
  requestId,
  spanId,
  parentSpanId,
  eventType,
  payload
}) {
  return {
    request_id: requestId,
    span_id: spanId,
    parent_span_id: parentSpanId,

    event_type: eventType,

    function_name: payload.functionName,
    layer: "service",
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

function runConventional(iterations, payloadProfile) {
  const metrics = createMetrics();

  for (let i = 0; i < iterations; i++) {
    const requestStarted = performance.now();

    /*
     * trace ID
     */
    const traceId = randomUUID();

    /*
     * span ID
     */
    const spanIdStarted = performance.now();

    const spanId = randomUUID();

    metrics.span_id_generation.push(
      performance.now() - spanIdStarted
    );

    /*
     * timestamp
     */
    const timestampStarted = performance.now();

    const startedAt = new Date().toISOString();

    metrics.timestamp_generation.push(
      performance.now() - timestampStarted
    );

    /*
     * span creation
     */
    const creationStarted = performance.now();

    const span = {
      traceId,
      spanId,
      parentSpanId: null,

      serviceName: "profiling",
      operation: payloadProfile.functionName,
      kind: "server",

      startedAt,

      ...payloadProfile.extra
    };

    metrics.span_event_creation.push(
      performance.now() - creationStarted
    );

    /*
     * serialization
     */
    const serializationStarted = performance.now();

    const body = JSON.stringify(span);

    metrics.serialization.push(
      performance.now() - serializationStarted
    );

    /*
     * Conventional tracing melakukan export synchronous.
     *
     * Pada microbenchmark ini kita hanya mengukur preparation/
     * initiation secara lokal. Network round-trip tidak dimasukkan
     * ke local tracing cost.
     */
    const submissionStarted = performance.now();

    void body.length;

    metrics.submission_initiation.push(
      performance.now() - submissionStarted
    );

    /*
     * Total request-path cost.
     *
     * Ini diukur langsung, bukan hasil penjumlahan komponen.
     */
    metrics.total_request_path.push(
      performance.now() - requestStarted
    );
  }

  return metrics;
}

/* -------------------------------------------------------------------------- */
/* Proposed                                                                  */
/* -------------------------------------------------------------------------- */

async function runProposed(iterations, payloadProfile) {
  const metrics = createMetrics();

  const events = [];

  const context = {
    requestId: randomUUID(),
    currentSpanId: null
  };

  await storage.run(context, async () => {
    for (let i = 0; i < iterations; i++) {
      const requestStarted = performance.now();

      /*
       * AsyncLocalStorage context lookup
       */
      const lookupStarted = performance.now();

      const parent = storage.getStore();

      metrics.context_lookup.push(
        performance.now() - lookupStarted
      );

      /*
       * span ID
       */
      const spanIdStarted = performance.now();

      const spanId = randomUUID();

      metrics.span_id_generation.push(
        performance.now() - spanIdStarted
      );

      /*
       * timestamp
       */
      const timestampStarted = performance.now();

      const timestamp = new Date().toISOString();

      metrics.timestamp_generation.push(
        performance.now() - timestampStarted
      );

      /*
       * ENTRY event creation
       */
      const entryCreationStarted = performance.now();

      const entry = createEvent({
        requestId: parent.requestId,
        spanId,
        parentSpanId: parent.currentSpanId,
        eventType: "ENTRY",
        payload: payloadProfile
      });

      entry.start_timestamp = timestamp;

      metrics.span_event_creation.push(
        performance.now() - entryCreationStarted
      );

      /*
       * ENTRY serialization
       */
      const entrySerializationStarted = performance.now();

      const entryBody = JSON.stringify(entry);

      metrics.serialization.push(
        performance.now() - entrySerializationStarted
      );

      /*
       * Async submission initiation.
       *
       * Sama seperti implementasi proposed:
       *
       *   const pending = fetch(...)
       *   void pending.then(...)
       *
       * Promise tidak di-await pada request path.
       */
      const submissionStarted = performance.now();

      const pending = Promise.resolve(entryBody);

      metrics.submission_initiation.push(
        performance.now() - submissionStarted
      );

      /*
       * Queue enqueue ENTRY
       */
      const enqueueStarted = performance.now();

      events.push(entry);

      metrics.queue_enqueue.push(
        performance.now() - enqueueStarted
      );

      /*
       * Update context seperti withProposedSpan().
       */
      const contextRunStarted = performance.now();

      await storage.run(
        {
          requestId: parent.requestId,
          currentSpanId: spanId
        },
        async () => {
          await pending;
        }
      );

      metrics.context_run.push(
        performance.now() - contextRunStarted
      );

      /*
       * EXIT event
       *
       * Implementasi actual proposed juga menghasilkan EXIT.
       */
      const exitCreationStarted = performance.now();

      const exit = createEvent({
        requestId: parent.requestId,
        spanId,
        parentSpanId: parent.currentSpanId,
        eventType: "EXIT",
        payload: payloadProfile
      });

      metrics.span_event_creation.push(
        performance.now() - exitCreationStarted
      );

      /*
       * EXIT serialization
       */
      const exitSerializationStarted = performance.now();

      const exitBody = JSON.stringify(exit);

      metrics.serialization.push(
        performance.now() - exitSerializationStarted
      );

      /*
       * EXIT async submission
       */
      const exitSubmissionStarted = performance.now();

      const exitPending = Promise.resolve(exitBody);

      /*
       * Kita tetap ukur initiation,
       * tetapi completion tidak dimasukkan ke request path.
       */
      metrics.submission_initiation.push(
        performance.now() - exitSubmissionStarted
      );

      /*
       * Queue enqueue EXIT
       */
      const exitEnqueueStarted = performance.now();

      events.push(exit);

      metrics.queue_enqueue.push(
        performance.now() - exitEnqueueStarted
      );

      await exitPending;

      /*
       * Parent context dikembalikan setelah span selesai.
       */
      storage.enterWith(parent);

      /*
       * Request-path total.
       */
      metrics.total_request_path.push(
        performance.now() - requestStarted
      );
    }
  });

  return {
    metrics,
    events
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

function runAsyncTail(events, batchSize) {
  const metrics = createMetrics();

  /*
   * Process queue batch-by-batch.
   *
   * Dengan ini kita tahu secara eksplisit:
   *
   * batchSize = 100
   * batchSize = 1,000
   * batchSize = 10,000
   */
  let offset = 0;

  while (offset < events.length) {
    const batch = events.slice(
      offset,
      offset + batchSize
    );

    offset += batch.length;

    const dequeueStarted = performance.now();

    /*
     * slice di atas merepresentasikan dequeue batch.
     */
    void batch.length;

    metrics.queue_dequeue.push(
      performance.now() - dequeueStarted
    );

    const reconstructionStarted = performance.now();

    reconstruct(batch);

    metrics.reconstruction.push(
      performance.now() - reconstructionStarted
    );
  }

  metrics.async_tail.push(
    metrics.queue_dequeue.reduce((a, b) => a + b, 0) +
    metrics.reconstruction.reduce((a, b) => a + b, 0)
  );

  return metrics;
}

/* -------------------------------------------------------------------------- */
/* Summary                                                                    */
/* -------------------------------------------------------------------------- */

function summarizeMetrics(metrics) {
  return Object.fromEntries(
    Object.entries(metrics).map(([operation, values]) => [
      operation,
      summarize(values)
    ])
  );
}

function flattenSummary({
  condition,
  payload,
  batchSize,
  repetition,
  summary
}) {
  return Object.entries(summary).map(
    ([operation, stats]) => ({
      condition,
      payload,
      batch_size: batchSize,
      repetition,

      operation,

      count: stats.count,

      mean_ms: stats.mean,
      p50_ms: stats.p50,
      p95_ms: stats.p95,
      p99_ms: stats.p99,

      min_ms: stats.min,
      max_ms: stats.max
    })
  );
}

function csv(rows) {
  if (!rows.length) return "";

  const headers = Object.keys(rows[0]);

  return [
    headers.join(","),

    ...rows.map((row) =>
      headers
        .map((header) =>
          JSON.stringify(row[header] ?? "")
        )
        .join(",")
    )
  ].join("\n");
}

/* -------------------------------------------------------------------------- */
/* Main                                                                       */
/* -------------------------------------------------------------------------- */

async function main() {
  console.log("========================================");
  console.log("Tracing Profiling V2");
  console.log("========================================");
  console.log(`iterations : ${ITERATIONS}`);
  console.log(`repetitions: ${REPETITIONS}`);
  console.log(`warmup     : ${WARMUP}`);
  console.log(
    `payloads   : ${Object.keys(PAYLOAD_PROFILES).join(", ")}`
  );
  console.log(
    `batch sizes: ${BATCH_SIZES.join(", ")}`
  );

  /*
   * Warm-up.
   */
  console.log("\nWarm-up...");

  for (const payload of Object.values(PAYLOAD_PROFILES)) {
    runConventional(WARMUP, payload);
    await runProposed(WARMUP, payload);
  }

  const rows = [];
  const rawResults = [];

  for (
    let repetition = 1;
    repetition <= REPETITIONS;
    repetition++
  ) {
    console.log(
      `\n========== Repetition ${repetition}/${REPETITIONS} ==========`
    );

    /*
     * Alternate execution order untuk mengurangi
     * bias karena condition selalu dieksekusi pertama.
     */
    const conditions =
      repetition % 2 === 1
        ? ["conventional", "proposed"]
        : ["proposed", "conventional"];

    for (const payload of Object.values(PAYLOAD_PROFILES)) {
      console.log(
        `\nPayload: ${payload.name}`
      );

      for (const condition of conditions) {
        console.log(
          `  profiling ${condition}...`
        );

        let metrics;
        let events = [];

        if (condition === "conventional") {
          metrics = runConventional(
            ITERATIONS,
            payload
          );
        } else {
          const result = await runProposed(
            ITERATIONS,
            payload
          );

          metrics = result.metrics;
          events = result.events;
        }

        const summary =
          summarizeMetrics(metrics);

        rows.push(
          ...flattenSummary({
            condition,
            payload: payload.name,
            batchSize: null,
            repetition,
            summary
          })
        );

        rawResults.push({
          condition,
          payload: payload.name,
          batchSize: null,
          repetition,
          summary
        });

        /*
         * Async tail hanya relevan untuk proposed.
         *
         * Gunakan event dari request-path yang baru saja
         * dihasilkan.
         */
        if (condition === "proposed") {
          for (const batchSize of BATCH_SIZES) {
            console.log(
              `    reconstruction batch=${batchSize}...`
            );

            const tailMetrics =
              runAsyncTail(
                events,
                batchSize
              );

            const tailSummary =
              summarizeMetrics(tailMetrics);

            rows.push(
              ...flattenSummary({
                condition,
                payload: payload.name,
                batchSize,
                repetition,
                summary: {
                  queue_dequeue:
                    tailSummary.queue_dequeue,

                  reconstruction:
                    tailSummary.reconstruction,

                  async_tail:
                    tailSummary.async_tail
                }
              })
            );

            rawResults.push({
              condition,
              payload: payload.name,
              batchSize,
              repetition,

              summary: {
                queue_dequeue:
                  tailSummary.queue_dequeue,

                reconstruction:
                  tailSummary.reconstruction,

                async_tail:
                  tailSummary.async_tail
              }
            });
          }
        }
      }
    }
  }

  await mkdir(
    RESULT_DIR,
    { recursive: true }
  );

  const timestamp =
    new Date()
      .toISOString()
      .replace(/[:.]/g, "-");

  const jsonPath = join(
    RESULT_DIR,
    `profile-v2-${timestamp}.json`
  );

  const csvPath = join(
    RESULT_DIR,
    `profile-v2-${timestamp}.csv`
  );

  await writeFile(
    jsonPath,
    JSON.stringify(
      {
        schemaVersion: "2.0.0",

        methodology: {
          description:
            "Granular profiling of conventional synchronous and proposed asynchronous tracing costs.",

          requestPath:
            "Measured directly around tracing operations executed on the request path.",

          asyncTail:
            "Measured separately for queue dequeue and trace reconstruction.",

          note:
            "Network round-trip latency is intentionally excluded from local tracing cost."
        },

        configuration: {
          iterations: ITERATIONS,
          repetitions: REPETITIONS,
          warmup: WARMUP,
          payloadProfiles:
            Object.keys(PAYLOAD_PROFILES),
          batchSizes: BATCH_SIZES
        },

        results: rawResults
      },
      null,
      2
    )
  );

  await writeFile(
    csvPath,
    csv(rows)
  );

  /*
   * Console summary:
   * request-path mean.
   */
  console.log("\n========================================");
  console.log("REQUEST-PATH SUMMARY");
  console.log("========================================");

  for (const payload of Object.keys(PAYLOAD_PROFILES)) {
    console.log(`\nPayload: ${payload}`);

    for (const condition of [
      "conventional",
      "proposed"
    ]) {
      const values = rows
        .filter(
          (row) =>
            row.payload === payload &&
            row.condition === condition &&
            row.operation ===
              "total_request_path"
        )
        .map((row) => row.mean_ms);

      if (!values.length) continue;

      const mean =
        values.reduce(
          (sum, value) => sum + value,
          0
        ) / values.length;

      console.log(
        `  ${condition.padEnd(14)} ${mean.toFixed(6)} ms`
      );
    }
  }

  console.log("\n========================================");
  console.log("RESULT FILES");
  console.log("========================================");

  console.log(jsonPath);
  console.log(csvPath);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
