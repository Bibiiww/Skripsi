console.log("[TEST] Script started");
import assert from "node:assert/strict";
import { createServer } from "node:http";
import {
  mkdtemp, readFile, rm, stat, writeFile
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

const root = process.cwd();
const built = resolve(root, "dist/tracing/async-execution.js");
const dir = await mkdtemp(resolve(tmpdir(), "wal-test-"));
const wal = resolve(dir, "execution.wal");
const delivered = [];

const server = createServer((req, res) => {
  let body = "";
  req.setEncoding("utf8");
  req.on("data", chunk => body += chunk);
  req.on("end", () => {
    try {
      assert.equal(req.url, "/v2/records");
      const parsed = JSON.parse(body);
      for (const record of parsed.records ?? []) {
        delivered.push(record.recordId);
      }
      res.writeHead(202).end();
    } catch {
      res.writeHead(400).end();
    }
  });
});

const listen = () => new Promise(resolvePort =>
  server.listen(0, "127.0.0.1", () =>
    resolvePort(server.address().port)
  )
);

function record(recordId, payload = "test") {
  const now = new Date().toISOString();
  return {
    recordId,
    traceId: "wal-test",
    invocationId: `inv-${recordId}`,
    parentInvocationId: null,
    serviceName: "wal-test",
    functionName: "test",
    layer: "handler",
    startedAt: now,
    endedAt: now,
    durationMs: 1,
    outcome: "ok",
    attributes: { payload }
  };
}

let exitCode = 0;

try {
  console.log("[TEST] Starting mock server");
  const port = await listen();
  console.log("[TEST] Mock server listening on port", port);

  process.env.TRACING_MODE = "proposed-durable";
  process.env.SERVICE_ROLE = "wal-test";
  process.env.TRACE_QUEUE_URL = `http://127.0.0.1:${port}`;
  process.env.TRACE_WAL_PATH = wal;
  process.env.TRACE_FLUSH_INTERVAL_MS = "10";
  process.env.TRACE_BATCH_SIZE = "100";
  process.env.TRACE_LOCAL_BUFFER_CAPACITY = "10000";
  process.env.TRACE_WAL_COMPACT_BYTES = String(1024 * 1024);

  const pendingId = "record-still-pending";
  const ackedId = "record-already-acked";
  const lines = [
    JSON.stringify({
      type: "record",
      record: record(pendingId, "pending")
    }),
    JSON.stringify({
      type: "record",
      record: record(ackedId, "already-acked")
    }),
    JSON.stringify({ type: "ack", recordId: ackedId })
  ];

  const largePayload = "wal-test-payload-".repeat(256);

  for (let i = 0; i < 800; i++) {
    const id = `historical-${i}`;
    lines.push(JSON.stringify({
      type: "record",
      record: record(id, largePayload)
    }));
    lines.push(JSON.stringify({ type: "ack", recordId: id }));
  }

  await writeFile(wal, lines.join("\n") + "\n");
  const before = (await stat(wal)).size;

  assert.ok(before > 1024 * 1024, "Seed WAL must exceed 1 MiB");

  console.log("[TEST] Importing built module");
  const url = `${pathToFileURL(built).href}?test=${Date.now()}`;
  const { runExecutionRequest } = await import(url);

  console.log("[TEST] Calling runExecutionRequest");
  await runExecutionRequest({}, "recovery-test", async () => "ok");
  console.log("[TEST] runExecutionRequest completed");

  const after = (await stat(wal)).size;
  console.log(`Original WAL: ${(before / 1048576).toFixed(2)} MiB`);
  console.log(`WAL after recovery: ${(after / 1048576).toFixed(2)} MiB`);

  assert.ok(after < before * 0.75, "WAL compaction did not reduce size enough");
  console.log("PASS: WAL compaction");

  const deadline = Date.now() + 5000;
  while (!delivered.includes(pendingId) && Date.now() < deadline) {
    await new Promise(resolveWait => setTimeout(resolveWait, 25));
  }

  assert.ok(delivered.includes(pendingId), "Pending record was not delivered");
  console.log("PASS: pending record delivered");

  assert.ok(!delivered.includes(ackedId), "ACKed record was redelivered");
  console.log("PASS: ACKed record not redelivered");

  console.log("ALL WAL TESTS PASSED");
} catch (error) {
  exitCode = 1;
  console.error("WAL TEST FAILED:", error);
} finally {
  await new Promise(resolveClose => server.close(resolveClose));
  await rm(dir, { recursive: true, force: true });
}

process.exit(exitCode);
