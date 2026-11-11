import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { technicalHealthModule } from "../../domains/technical-health/index.ts";
import { runHealthCheck, type RunOptions } from "../../engine/run.ts";
import type { Transport, TransportRequest, TransportResponse } from "../../salesforce/transport.ts";
import { tempDir } from "../../testing/harness.ts";
import { loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { APEX_CLASSES, AS_OF, sfId } from "../../testing/synthetic-org.ts";
import { scanApex } from "../apex-scan.ts";
import { APEX_SOURCE_FILE, APEX_SOURCE_MAX_BODY, ApexSource, selectApexSource, type SourceRow } from "../apex-source.ts";

// Decision 491: with the worker's switch on, the Apex step keeps the redacted
// code Claude may summarise, bounded however large the org, and nothing else.

const row = (name: string, body: string | undefined, length: number | null = 100, kind: SourceRow["kind"] = "class"): SourceRow => ({
  kind,
  name,
  length,
  body,
  scan: body && body !== "(hidden)" ? scanApex(body) : null,
});
const SECRET = "sk-synthetic0491notarealkey000000000000";

describe("which classes and triggers are kept", () => {
  test("a test class is left out by the scan's reading of the class itself, not by @isTest anywhere", () => {
    const source = selectApexSource([
      row("RealTest", "@isTest private class RealTest { @isTest static void t() {} }"),
      // The row's own isTest flag would call this a test: it only mentions one.
      row("MentionsTests", "public class MentionsTests { // see the @isTest classes\n public void go() {} }"),
    ]);
    assert.deepEqual(source.items.map((i) => i.name), ["MentionsTests"]);
    assert.equal(source.counts.tests, 1);
  });

  test("a hidden, empty or missing body is unreadable", () => {
    const source = selectApexSource([row("Hidden", "(hidden)"), row("Empty", ""), row("Missing", undefined), row("Fine", "public class Fine {}")]);
    assert.deepEqual(source.items.map((i) => i.name), ["Fine"]);
    assert.equal(source.counts.unreadable, 3);
  });

  test("a body over 100,000 characters is left out whole, and one of exactly 100,000 is kept", () => {
    const body = (n: number) => `public class X {}${" ".repeat(n - 17)}`;
    const source = selectApexSource([row("AtLimit", body(APEX_SOURCE_MAX_BODY)), row("OverLimit", body(APEX_SOURCE_MAX_BODY + 1))]);
    assert.deepEqual(source.items.map((i) => [i.name, i.body.length]), [["AtLimit", APEX_SOURCE_MAX_BODY]]);
    assert.equal(source.counts.tooLarge, 1);
  });

  test("the 200 largest by LengthWithoutComments, classes and triggers together, ties by kind then name", () => {
    const rows: SourceRow[] = [];
    for (let i = 0; i < 230; i++) rows.push(row(`Class${String(i).padStart(3, "0")}`, `public class C${i} {}`, 1000 + i));
    // Ties at the top: a class before a trigger, then by name.
    rows.push(row("Zeta", "trigger Zeta on Account (before insert) {}", 5000, "trigger"));
    rows.push(row("Beta", "public class Beta {}", 5000));
    rows.push(row("Alpha", "public class Alpha {}", 5000));
    const source = selectApexSource(rows.reverse());
    assert.equal(source.items.length, 200);
    assert.deepEqual(source.items.slice(0, 4).map((i) => `${i.kind} ${i.name}`), ["class Alpha", "class Beta", "trigger Zeta", "class Class229"]);
    assert.equal(source.items.at(-1)!.name, "Class033", "the smallest kept is the 200th largest");
    assert.equal(source.counts.overLimit, 33);
    assert.equal(source.counts.read, 233);
  });

  test("the body is kept redacted, and the counts add up to every row read", () => {
    const source = selectApexSource([
      row("Keyed", `public class Keyed { String api_key = '${SECRET}'; }`),
      row("Hidden", "(hidden)"),
      row("SomeTest", "@isTest class SomeTest {}"),
    ]);
    assert.ok(!JSON.stringify(source).includes(SECRET));
    assert.match(source.items[0]!.body, /api_key = '\[REDACTED\]'/);
    const { read, ...left } = source.counts;
    assert.equal(source.items.length + Object.values(left).reduce((a, b) => a + b, 0), read);
    assert.doesNotThrow(() => ApexSource.parse(source));
  });
});

/** Fails the first further page of the classes' read with Salesforce's daily-limit error: a budget stop between two pages. */
class StopBetweenPages implements Transport {
  readonly #inner: Transport;
  #classes = false;
  stopped = false;
  constructor(inner: Transport) {
    this.#inner = inner;
  }
  async send(request: TransportRequest): Promise<TransportResponse> {
    const url = decodeURIComponent(request.url);
    if (url.includes("FROM ApexClass WHERE")) this.#classes = true;
    else if (this.#classes && !this.stopped && /\/query\/[A-Za-z0-9]{15,18}-\d+$/.test(url)) {
      this.stopped = true;
      return { status: 403, header: () => null, text: JSON.stringify([{ errorCode: "REQUEST_LIMIT_EXCEEDED", message: "TotalRequests Limit exceeded." }]) };
    }
    return this.#inner.send(request);
  }
}

/** Looks for the file at every request the run makes, so one that came and went mid-run is seen. */
class Watching implements Transport {
  readonly #inner: Transport;
  readonly #path: string;
  seen = 0;
  requests = 0;
  constructor(inner: Transport, path: string) {
    this.#inner = inner;
    this.#path = path;
  }
  async send(request: TransportRequest): Promise<TransportResponse> {
    this.requests++;
    if (existsSync(this.#path)) this.seen++;
    return this.#inner.send(request);
  }
}

const options = (outDir: string, extra: Partial<RunOptions> = {}): RunOptions => ({
  provider: syntheticProvider,
  calibration: loadCalibration(),
  modules: [technicalHealthModule],
  outDir,
  now: new Date(AS_OF),
  log: () => {},
  ...extra,
});

describe("the Apex step, with the switch on and off", () => {
  test("on: the file is at the top of the run folder, never under checkpoints/, with the synthetic org's non-test classes and triggers", async () => {
    const outDir = tempDir();
    const result = await runHealthCheck(options(outDir, { transport: new Simulator(), keepApexSource: true }));
    const source = ApexSource.parse(JSON.parse(readFileSync(join(result.dir, APEX_SOURCE_FILE), "utf8")));
    assert.equal(source.counts.read, 12);
    assert.equal(source.counts.tests, 3);
    assert.equal(source.items.length, 9);
    assert.deepEqual(source.items.slice(0, 2).map((i) => i.name), ["OpportunityHandler", "LeadRouter"]);
    assert.ok(!(readdirSync(join(result.dir, "checkpoints")) as string[]).some((f) => f.includes("apex-source")));
  });

  test("off: no file ever exists, mid-run included, and the checkpoint is the same as with it on", async () => {
    const offDir = tempDir();
    const runId = "hc-2026-10-31-00000491";
    const watching = new Watching(new Simulator(), join(offDir, runId, APEX_SOURCE_FILE));
    const off = await runHealthCheck(options(offDir, { transport: watching, runId }));
    assert.ok(watching.requests > 10);
    assert.equal(watching.seen, 0);
    assert.ok(!existsSync(join(off.dir, APEX_SOURCE_FILE)));
    const on = await runHealthCheck(options(tempDir(), { transport: new Simulator(), keepApexSource: true }));
    assert.equal(readFileSync(join(off.dir, "checkpoints", "apex.json"), "utf8"), readFileSync(join(on.dir, "checkpoints", "apex.json"), "utf8"));
    assert.deepEqual(off.run.steps.find((s) => s.id === "apex")!.version, on.run.steps.find((s) => s.id === "apex")!.version);
  });

  test("off: a file already there is left alone, neither written nor deleted", async () => {
    const outDir = tempDir();
    const runId = "hc-2026-10-31-00000492";
    mkdirSync(join(outDir, runId), { recursive: true });
    writeFileSync(join(outDir, runId, APEX_SOURCE_FILE), "left as it was\n");
    await runHealthCheck(options(outDir, { transport: new Simulator(), runId }));
    assert.equal(readFileSync(join(outDir, runId, APEX_SOURCE_FILE), "utf8"), "left as it was\n");
  });

  test("stopped on the budget between two pages and resumed, it ends with the same file byte for byte as an unstopped step", async () => {
    const whole = await runHealthCheck(options(tempDir(), { transport: new Simulator({ pageSize: 3 }), keepApexSource: true }));
    const outDir = tempDir();
    const stopping = new StopBetweenPages(new Simulator({ pageSize: 3 }));
    const stopped = await runHealthCheck(options(outDir, { transport: stopping, keepApexSource: true }));
    assert.ok(stopping.stopped);
    assert.equal(stopped.run.status, "stopped_budget");
    assert.equal(stopped.run.steps.find((s) => s.id === "apex")!.status, "pending");
    // The first page's bodies were in memory only, and went with the stopped step.
    assert.ok(!existsSync(join(stopped.dir, APEX_SOURCE_FILE)));
    const resumed = await runHealthCheck(options(outDir, { transport: new Simulator({ pageSize: 3 }), keepApexSource: true, resume: stopped.run.id }));
    assert.notEqual(resumed.run.status, "stopped_budget");
    assert.equal(readFileSync(join(resumed.dir, APEX_SOURCE_FILE), "utf8"), readFileSync(join(whole.dir, APEX_SOURCE_FILE), "utf8"));
  });

  test("a step that starts afresh never inherits a stale file, even when it's stopped before it can write its own", async () => {
    const outDir = tempDir();
    const runId = "hc-2026-10-31-00000493";
    mkdirSync(join(outDir, runId), { recursive: true });
    writeFileSync(join(outDir, runId, APEX_SOURCE_FILE), `${JSON.stringify({ version: 1, counts: { read: 1, tests: 0, tooLarge: 0, overLimit: 0, unreadable: 0 }, items: [{ kind: "class", name: "Stale", body: "stale" }] })}\n`);
    const stopped = await runHealthCheck(options(outDir, { transport: new StopBetweenPages(new Simulator({ pageSize: 3 })), keepApexSource: true, runId }));
    assert.equal(stopped.run.status, "stopped_budget");
    assert.ok(!existsSync(join(outDir, runId, APEX_SOURCE_FILE)));
  });

  test("however many classes the org has, the file holds at most 200 bodies", async () => {
    const many = Array.from({ length: 260 }, (_, i) => ({ ...APEX_CLASSES[0]!, Id: sfId("01p", 100 + i), Name: `Generated${String(i).padStart(3, "0")}`, LengthWithoutComments: 500 + i }));
    const result = await runHealthCheck(options(tempDir(), { transport: new Simulator({ apexClasses: [...APEX_CLASSES, ...many] }), keepApexSource: true }));
    const source = ApexSource.parse(JSON.parse(readFileSync(join(result.dir, APEX_SOURCE_FILE), "utf8")));
    assert.equal(source.items.length, 200);
    assert.equal(source.counts.read, 272);
    assert.equal(source.counts.overLimit, 69);
  });
});
