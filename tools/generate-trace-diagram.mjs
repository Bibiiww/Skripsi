#!/usr/bin/env node
import { mkdir, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";

function option(name, fallback) { const index = process.argv.indexOf(name); return index >= 0 ? process.argv[index + 1] : fallback; }
const condition = option("--condition"); const output = resolve(option("--output", "trace-diagram.svg")); const endpoint = option("--endpoint");
if (!condition || !endpoint) throw new Error("Usage: node tools/generate-trace-diagram.mjs --condition conventional|conventional-otel|proposed|proposed-memory|proposed-durable --endpoint URL --output FILE");
const escape = (value) => String(value).replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;");

async function load() {
  if (condition === "conventional" || condition === "conventional-otel") {
    const body = await fetch(`${endpoint}/v1/spans`).then((response) => response.json());
    const groups = new Map(); for (const span of body.spans ?? []) groups.set(span.traceId, [...(groups.get(span.traceId) ?? []), span]);
    const traces = [...groups].map(([traceId, spans]) => ({ traceId, nodes: spans.map((span) => ({ id: span.spanId, parent: span.parentSpanId, label: `${span.serviceName}: ${span.operation}`, detail: `${span.kind} | ${span.statusCode ?? "?"}` })) }));
    return traces;
  }
  const version = condition === "proposed" ? "v1" : "v2";
  const index = await fetch(`${endpoint}/${version}/traces`).then((response) => response.json());
  const candidates = (index.traces ?? []).sort((left, right) => Number(right.complete) - Number(left.complete) || right.observedRecords - left.observedRecords);
  // One representative diagram is required per run. Fetching every trace can
  // delay teardown by minutes for a high-volume benchmark and risks losing the
  // artifact entirely when the stack is stopped.
  const candidate = candidates[0];
  if (!candidate) return [];
  const traceKey = candidate.traceId ?? candidate.requestId;
  const trace = await fetch(`${endpoint}/${version}/traces/${encodeURIComponent(traceKey)}`).then((response) => response.json());
  const records = trace.records ?? trace.nodes ?? [];
  return [{ traceId: trace.traceId ?? trace.requestId, assessment: trace.assessment ?? { complete: trace.complete, observedRecords: records.length, expectedRecords: null }, nodes: records.map((record) => ({ id: record.invocationId ?? record.spanId, parent: record.parentInvocationId ?? record.parentSpanId, label: record.functionName ? `${record.serviceName}: ${record.functionName}` : `${record.serviceName}: ${record.functionName ?? "span"}`, detail: `${record.layer ?? "span"} | ${record.outcome ?? "ok"}` })) }];
}

let traces = [];
try { traces = await load(); } catch (error) { traces = [{ traceId: "unavailable", error: error instanceof Error ? error.message : String(error), nodes: [] }]; }
const selected = [...traces].sort((left, right) => (Number(right.assessment?.complete ?? false) - Number(left.assessment?.complete ?? false)) || right.nodes.length - left.nodes.length)[0] ?? { traceId: "none", nodes: [] };
const ids = new Set(selected.nodes.map((node) => node.id)); const roots = selected.nodes.filter((node) => !node.parent || !ids.has(node.parent));
const children = new Map(); for (const node of selected.nodes) if (node.parent && ids.has(node.parent)) children.set(node.parent, [...(children.get(node.parent) ?? []), node]);
const order = []; const visit = (node, depth) => { order.push({ node, depth }); for (const child of children.get(node.id) ?? []) visit(child, depth + 1); }; roots.forEach((root) => visit(root, 0));
const maxDepth = order.reduce((current, item) => Math.max(current, item.depth), 0);
const width = Math.max(1060, 290 + maxDepth * 245); const height = Math.max(180, 110 + order.length * 70); const status = selected.assessment ? (selected.assessment.complete ? "COMPLETE" : "INCOMPLETE") : selected.nodes.length ? "STRUCTURAL CHECK NOT AVAILABLE" : "NO TRACE AVAILABLE";
const positions = new Map(order.map(({ node, depth }, index) => [node.id, { x: 50 + depth * 245, y: 70 + index * 70 }]));
const edges = order.filter(({ node }) => node.parent && positions.has(node.parent)).map(({ node }) => { const from = positions.get(node.parent); const to = positions.get(node.id); return `<line x1="${from.x + 190}" y1="${from.y + 24}" x2="${to.x}" y2="${to.y + 24}" stroke="#718096" stroke-width="1.5" marker-end="url(#arrow)"/>`; }).join("");
const boxes = order.map(({ node }) => { const p = positions.get(node.id); return `<rect x="${p.x}" y="${p.y}" width="190" height="48" rx="6" fill="#f8fafc" stroke="#2563eb"/><text x="${p.x + 8}" y="${p.y + 19}" font-size="12" font-weight="600">${escape(node.label).slice(0, 31)}</text><text x="${p.x + 8}" y="${p.y + 36}" font-size="10" fill="#475569">${escape(node.detail ?? "")}</text>`; }).join("");
const svg = `<?xml version="1.0" encoding="UTF-8"?><svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}"><defs><marker id="arrow" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto"><path d="M0,0 L8,4 L0,8 Z" fill="#718096"/></marker></defs><rect width="100%" height="100%" fill="white"/><text x="32" y="28" font-size="18" font-weight="700">Trace diagram: ${escape(condition)}</text><text x="32" y="48" font-size="12" fill="#475569">trace_id: ${escape(selected.traceId)} | ${status}${selected.assessment ? ` | ${selected.assessment.observedRecords}/${selected.assessment.expectedRecords} records` : ""}</text>${edges}${boxes}</svg>`;
await mkdir(dirname(output), { recursive: true }); await writeFile(output, svg); await writeFile(output.replace(/\.svg$/i, ".json"), `${JSON.stringify({ condition, selectedTraceId: selected.traceId, status, assessment: selected.assessment ?? null, nodeCount: selected.nodes.length, sourceTraceCount: traces.length }, null, 2)}\n`);
console.log(`Trace diagram created: ${output}`);
