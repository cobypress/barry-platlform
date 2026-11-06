#!/usr/bin/env node
import { readdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { ALL_MODULES } from "../domains/index.ts";
import { runHealthCheck } from "../engine/run.ts";
import { renderRun } from "../render/render-run.ts";
import type { Transport, TransportRequest, TransportResponse } from "../salesforce/transport.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "./replay.ts";
import { buildScaleOrg, FIVE_HUNDRED_USERS, scaled } from "./scale-org.ts";
import { Simulator } from "./simulator.ts";
import { AS_OF } from "./synthetic-org.ts";

// The scale test (docs/overnight/night-3/scale.md): the whole engine, every
// module, against a generated org the size of our largest first customers,
// through the simulator, which pages as Salesforce does. Nothing calls a real
// org. The run folder goes outside the repository and is never committed.
//
//   node --max-old-space-size=8192 src/testing/scale-run.ts <out dir> [--factor 0.1] [--latency 200] [--per-record 0.3] [--daily 600000] [--no-render]
//
// There's no real waiting: each call's latency is added up, as the engine's
// calls are made one at a time. --latency is per call in ms, and --per-record
// is per row returned, for the time Salesforce takes to send a page.

const args = process.argv.slice(2);
const flag = (name: string, fallback: number): number => {
  const at = args.indexOf(`--${name}`);
  return at === -1 ? fallback : Number(args[at + 1]);
};
const outDir = args.find((a, i) => !a.startsWith("--") && !args[i - 1]?.startsWith("--"));
if (!outDir) throw new Error("Give a folder outside the repository for the run");
const factor = flag("factor", 1);
const latencyMs = flag("latency", 200);
const perRecordMs = flag("per-record", 0.3);
const dailyLimit = flag("daily", 600_000);

const mb = (bytes: number): number => Math.round(bytes / 1024 / 1024);
let peakRss = 0;
let peakHeap = 0;
const sample = (): void => {
  const m = process.memoryUsage();
  peakRss = Math.max(peakRss, m.rss);
  peakHeap = Math.max(peakHeap, m.heapUsed);
};
const sampler = setInterval(sample, 100);

/** Counts what goes over the wire, and the time it would have taken. */
class Measured implements Transport {
  readonly inner: Transport;
  calls = 0;
  records = 0;
  bytes = 0;
  networkMs = 0;
  inFlight = 0;
  maxInFlight = 0;
  biggestPage = 0;
  constructor(inner: Transport) {
    this.inner = inner;
  }
  async send(request: TransportRequest): Promise<TransportResponse> {
    this.inFlight++;
    this.maxInFlight = Math.max(this.maxInFlight, this.inFlight);
    try {
      const res = await this.inner.send(request);
      const rows = /"records":\[/.test(res.text) ? ((JSON.parse(res.text) as { records?: unknown[] }).records?.length ?? 0) : 0;
      this.calls++;
      this.records += rows;
      this.bytes += res.text.length;
      this.biggestPage = Math.max(this.biggestPage, res.text.length);
      this.networkMs += latencyMs + rows * perRecordMs;
      return res;
    } finally {
      this.inFlight--;
    }
  }
}

const size = scaled(FIVE_HUNDRED_USERS, factor);
let t = performance.now();
const records = buildScaleOrg(size);
const generateMs = performance.now() - t;
sample();
const counts = {
  users: records.users.length,
  opportunities: records.pipeline.opportunities.length,
  opportunityHistory: records.pipeline.history.length,
  leads: records.leadFunnel.leads.length,
  leadHistory: records.leadFunnel.statusChanges.length,
  campaignMembers: records.leadFunnel.members.length,
  tasks: records.activity.tasks.length,
  events: records.activity.events.length,
  emails: records.activity.emails.length,
  cases: records.supportCases.cases.length,
  caseHistory: records.supportCases.history.length,
  quotes: records.cpqQuotes.length,
  forecastFacts: records.forecastFacts.length,
  logins: records.loginHistory.length,
  staleLeads: records.staleLeadRows.length,
  contactsNoEmail: records.contactsNoEmail.length,
};
console.log(`Generated in ${Math.round(generateMs / 1000)}s: ${JSON.stringify(counts)}`);

const transport = new Measured(new Simulator({ records, pageSize: 2000, dailyLimit, used: Math.round(dailyLimit * 0.1) }));
const marks: Array<{ step: string; calls: number; networkMs: number; computeMs: number }> = [];
let last = { calls: 0, networkMs: 0, at: performance.now() };
t = performance.now();
const result = await runHealthCheck({
  provider: syntheticProvider,
  calibration: loadCalibration(),
  answers: loadAnswers(),
  modules: ALL_MODULES,
  outDir,
  now: new Date(AS_OF),
  transport,
  log: (line) => {
    const done = /^\s+([\w-]+): (done|failed)/.exec(line);
    if (!done) return;
    const now = performance.now();
    marks.push({ step: done[1]!, calls: transport.calls - last.calls, networkMs: Math.round(transport.networkMs - last.networkMs), computeMs: Math.round(now - last.at) });
    last = { calls: transport.calls, networkMs: transport.networkMs, at: now };
  },
});
const runMs = performance.now() - t;
sample();

function folderBytes(dir: string): number {
  return readdirSync(dir, { withFileTypes: true }).reduce((n, e) => n + (e.isDirectory() ? folderBytes(join(dir, e.name)) : statSync(join(dir, e.name)).size), 0);
}
const runBytes = folderBytes(result.dir);

// Which reads stopped at a cap, as the checkpoints record them.
const truncated: string[] = [];
const checkpoints = join(result.dir, "checkpoints");
for (const file of readdirSync(checkpoints).filter((f) => f.endsWith(".json"))) {
  const text = readFileSync(join(checkpoints, file), "utf8");
  for (const m of text.matchAll(/"query":"([^"]{0,120})[^"]*"[^{}]*?"truncated":true|"truncated":true[^{}]*?"query":"([^"]{0,120})/g)) truncated.push(`${file}: ${m[1] ?? m[2]}`);
}

let render: Record<string, unknown> | null = null;
if (!args.includes("--no-render")) {
  t = performance.now();
  const rendered = await renderRun(result.dir);
  sample();
  render = {
    seconds: Math.round((performance.now() - t) / 1000),
    passed: rendered.passed,
    executivePages: rendered.layout.executive.pages,
    fullSlides: rendered.layout.full.pages,
    workbookBytes: statSync(join(result.dir, "action-lists.xlsx")).size,
    ticketsBytes: statSync(join(result.dir, "tickets.csv")).size,
  };
}
clearInterval(sampler);

const output = result.output!;
const summary = {
  size,
  counts,
  generateSeconds: Math.round(generateMs / 1000),
  runComputeSeconds: Math.round(runMs / 1000),
  latency: { perCallMs: latencyMs, perRecordMs },
  networkMinutes: Math.round(transport.networkMs / 60_000),
  calls: transport.calls,
  budget: result.run.budget,
  recordsReturned: transport.records,
  megabytesReturned: mb(transport.bytes),
  biggestResponseKb: Math.round(transport.biggestPage / 1024),
  maxCallsAtOnce: transport.maxInFlight,
  peakRssMb: mb(peakRss),
  peakHeapMb: mb(peakHeap),
  runFolderMb: Math.round((runBytes / 1024 / 1024) * 10) / 10,
  steps: marks,
  stepCalls: result.run.steps.map((s) => ({ id: s.id, status: s.status, calls: s.calls, error: s.error })),
  truncated,
  findings: { issue: output.findings.filter((f) => f.status === "issue").length, pass: output.findings.filter((f) => f.status === "passed").length, notMeasurable: output.findings.filter((f) => f.status === "not_measurable").map((f) => `${f.ruleId}: ${f.summary.slice(0, 160)}`) },
  render,
};
writeFileSync(join(outDir, `scale-${factor}.json`), `${JSON.stringify(summary, null, 2)}\n`);
console.log(JSON.stringify({ ...summary, steps: undefined, stepCalls: undefined, findings: { ...summary.findings, notMeasurable: summary.findings.notMeasurable.length } }, null, 2));
console.log(`Written to ${join(outDir, `scale-${factor}.json`)}`);
