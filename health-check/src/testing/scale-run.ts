#!/usr/bin/env node
import { fork } from "node:child_process";
import { readdirSync, statSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { ALL_MODULES } from "../domains/index.ts";
import { runHealthCheck } from "../engine/run.ts";
import { renderRun } from "../render/render-run.ts";
import type { Transport, TransportRequest, TransportResponse } from "../salesforce/transport.ts";
import { fingerprint } from "./fingerprint.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "./replay.ts";
import type { ChildReply, ChildRequest, ChildStart } from "./scale-sim-child.ts";
import { FIVE_HUNDRED_USERS, scaled } from "./scale-org.ts";
import { AS_OF } from "./synthetic-org.ts";

// The scale test (docs/overnight/night-3/scale.md): the whole engine, every
// module, against a generated org the size of our largest first customers,
// through the simulator, which pages as Salesforce does. Nothing calls a real
// org. The run folder goes outside the repository and is never committed.
//
//   npm run scale -- <out dir> [--factor 0.1] [--latency 200] [--per-record 0.3] [--daily 600000] [--no-render] [--limit-mb 2500] [--diagnose]
//
// The generated org and the simulator run in a process of their own, so the
// memory measured here is the engine's alone (DECISIONS.md 342). The run fails
// if the engine's resident peak goes over --limit-mb, so a regression shows.
// --diagnose collects garbage at each step's end, to show what each step's
// output keeps; leave it off for the peak a worker would see.
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
const limitMb = flag("limit-mb", 2500);
const diagnose = args.includes("--diagnose");
const gc = (globalThis as { gc?: () => void }).gc;
if (diagnose && !gc) throw new Error("--diagnose needs node --expose-gc");

const mb = (bytes: number): number => Math.round(bytes / 1048576);

// The engine's memory: peak resident and heap overall, and in each step or phase.
let peakRss = 0;
let peakHeap = 0;
let stepRss = 0;
let stepHeap = 0;
const sample = (): void => {
  const m = process.memoryUsage();
  peakRss = Math.max(peakRss, m.rss);
  peakHeap = Math.max(peakHeap, m.heapUsed);
  stepRss = Math.max(stepRss, m.rss);
  stepHeap = Math.max(stepHeap, m.heapUsed);
};
const sampler = setInterval(sample, 50);

type Mark = { step: string; calls: number; networkMs: number; computeMs: number; peakRssMb: number; peakHeapMb: number; heapAfterMb: number };
const marks: Mark[] = [];

/** The simulator in its own process, answering over IPC, with what goes over the wire counted and costed. */
class ChildSimulator implements Transport {
  readonly child = fork(join(import.meta.dirname, "scale-sim-child.ts"), [], { execArgv: ["--max-old-space-size=12288"], serialization: "advanced" });
  #pending = new Map<number, (reply: Extract<ChildReply, { kind: "reply" }>) => void>();
  #next = 0;
  #memory: ((rssMb: number) => void) | null = null;
  calls = 0;
  records = 0;
  bytes = 0;
  networkMs = 0;
  inFlight = 0;
  maxInFlight = 0;

  start(options: ChildStart): Promise<Extract<ChildReply, { kind: "ready" }>> {
    return new Promise((resolve) => {
      this.child.on("message", (message: ChildReply) => {
        if (message.kind === "ready") resolve(message);
        else if (message.kind === "reply") {
          this.#pending.get(message.id)?.(message);
          this.#pending.delete(message.id);
        } else this.#memory?.(message.rssMb);
      });
      this.child.send(options);
    });
  }

  memory(): Promise<number> {
    return new Promise((resolve) => {
      this.#memory = resolve;
      this.child.send({ kind: "memory" });
    });
  }

  async send(request: TransportRequest): Promise<TransportResponse> {
    this.inFlight++;
    this.maxInFlight = Math.max(this.maxInFlight, this.inFlight);
    try {
      const id = ++this.#next;
      const reply = await new Promise<Extract<ChildReply, { kind: "reply" }>>((resolve) => {
        this.#pending.set(id, resolve);
        this.child.send({ kind: "request", id, request: { method: request.method, url: request.url, headers: request.headers, body: request.body } } satisfies ChildRequest);
      });
      const rows = /"records":\[/.test(reply.text) ? ((JSON.parse(reply.text) as { records?: unknown[] }).records?.length ?? 0) : 0;
      this.calls++;
      this.records += rows;
      this.bytes += reply.text.length;
      this.networkMs += latencyMs + rows * perRecordMs;
      const limitInfo = reply.limitInfo;
      return { status: reply.status, text: reply.text, header: (name) => (name.toLowerCase() === "sforce-limit-info" ? limitInfo : null) };
    } finally {
      this.inFlight--;
    }
  }
}

const size = scaled(FIVE_HUNDRED_USERS, factor);
const transport = new ChildSimulator();
const ready = await transport.start({ kind: "start", factor, pageSize: 2000, dailyLimit, used: Math.round(dailyLimit * 0.1) });
console.log(`Generated in ${Math.round(ready.generateMs / 1000)}s, in its own process (${ready.rssMb} MB): ${JSON.stringify(ready.counts)}`);

gc?.();
sample();
const baselineRssMb = mb(process.memoryUsage().rss);
let last = { calls: 0, networkMs: 0, at: performance.now() };
/** Closes a step or phase: what it cost, its peak, and what it left on the heap. */
function mark(step: string): void {
  sample();
  if (diagnose) gc!();
  const now = performance.now();
  marks.push({ step, calls: transport.calls - last.calls, networkMs: Math.round(transport.networkMs - last.networkMs), computeMs: Math.round(now - last.at), peakRssMb: mb(stepRss), peakHeapMb: mb(stepHeap), heapAfterMb: mb(process.memoryUsage().heapUsed) });
  last = { calls: transport.calls, networkMs: transport.networkMs, at: performance.now() };
  stepRss = 0;
  stepHeap = 0;
}

let t = performance.now();
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
    if (done) mark(done[1]!);
  },
});
mark("analysis and writing");
const runMs = performance.now() - t;
const simulatorRssMb = await transport.memory();
transport.child.kill();

const output = result.output!;
const findings = { issue: output.findings.filter((f) => f.status === "issue").length, passed: output.findings.filter((f) => f.status === "passed").length, notMeasurable: output.findings.filter((f) => f.status === "not_measurable").length };
const budget = result.run.budget;
const steps = result.run.steps.map((s) => ({ id: s.id, status: s.status, calls: s.calls, error: s.error }));

let render: Record<string, unknown> | null = null;
if (!args.includes("--no-render")) {
  t = performance.now();
  const rendered = await renderRun(result.dir);
  mark("rendering the documents");
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

function folderBytes(dir: string): number {
  return readdirSync(dir, { withFileTypes: true }).reduce((n, e) => n + (e.isDirectory() ? folderBytes(join(dir, e.name)) : statSync(join(dir, e.name)).size), 0);
}

const summary = {
  size,
  counts: ready.counts,
  diagnose,
  runComputeSeconds: Math.round(runMs / 1000),
  latency: { perCallMs: latencyMs, perRecordMs },
  networkMinutes: Math.round(transport.networkMs / 60_000),
  calls: transport.calls,
  budget,
  recordsReturned: transport.records,
  megabytesReturned: mb(transport.bytes),
  maxCallsAtOnce: transport.maxInFlight,
  engine: { baselineRssMb, peakRssMb: mb(peakRss), peakHeapMb: mb(peakHeap), limitMb },
  simulatorRssMb,
  runFolderMb: Math.round((folderBytes(result.dir) / 1048576) * 10) / 10,
  marks,
  steps,
  findings,
  fingerprint: fingerprint(result.dir),
  render,
};
writeFileSync(join(outDir, `scale-${factor}.json`), `${JSON.stringify(summary, null, 2)}\n`);
console.log(JSON.stringify({ ...summary, marks: undefined, steps: undefined }, null, 2));
console.log(`Written to ${join(outDir, `scale-${factor}.json`)}`);
if (mb(peakRss) > limitMb) {
  console.error(`The engine's resident peak, ${mb(peakRss)} MB, is over the limit of ${limitMb} MB.`);
  process.exitCode = 1;
}
