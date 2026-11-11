import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { cpSync, existsSync, mkdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { APEX_SOURCE_FILE, ApexSource } from "../../extract/apex-source.ts";
import { SUMMARIES_FILE, SUMMARIES_PARTIAL_FILE } from "../../handbook/summaries.ts";
import { APEX_SUMMARY_PROMPT } from "../../narrative/apex-prompt.ts";
import type { NarrativeClient, NarrativeRequest } from "../../narrative/client.ts";
import type { Transport, TransportRequest, TransportResponse } from "../../salesforce/transport.ts";
import { loadAnswers, loadRecording, ReplayTransport, type Recording } from "../../testing/replay.ts";
import { createJobHandler } from "../worker.ts";
import { baseDeps, RUN_ID, runInput, syntheticConnection, temp } from "./helpers.ts";

// The worker's side of decisions 491 and 492: where the summaries run, what a
// job delivered again asks for, and when the kept code is deleted.

const job = { kind: "run", runId: RUN_ID };
const once = { final: false };
const last = { final: true };

/** The recorded synthetic org with one trigger, so five classes and triggers may be summarised. */
function fiveRecording(): Recording {
  const recording = loadRecording();
  for (const exchange of recording.exchanges.filter((e) => decodeURIComponent(e.key).includes("FROM ApexTrigger WHERE"))) {
    const page = JSON.parse(exchange.text) as { totalSize: number; records: unknown[] };
    page.records = page.records.slice(0, 1);
    page.totalSize = 1;
    exchange.text = JSON.stringify(page);
  }
  return recording;
}

/** Once the Apex step has made its last read, fails what follows: with Salesforce's daily-limit error once, or as a dropped network always. */
class AfterApex implements Transport {
  readonly #inner: Transport;
  readonly #how: "budget" | "network";
  #apexDone = false;
  #stopped = false;
  constructor(inner: Transport, how: "budget" | "network") {
    this.#inner = inner;
    this.#how = how;
  }
  async send(request: TransportRequest): Promise<TransportResponse> {
    if (decodeURIComponent(request.url).includes("FROM ApexCodeCoverageAggregate")) {
      this.#apexDone = true;
      return this.#inner.send(request);
    }
    if (this.#apexDone && this.#how === "network") throw new Error("fetch failed: ECONNRESET");
    if (this.#apexDone && !this.#stopped) {
      this.#stopped = true;
      return { status: 403, header: () => null, text: JSON.stringify([{ errorCode: "REQUEST_LIMIT_EXCEEDED", message: "TotalRequests Limit exceeded." }]) };
    }
    return this.#inner.send(request);
  }
}

/** A stub Claude that keeps every request, answers a summary by the item's name and a section with a paragraph, and calls `look` first. */
function claude(look: (request: NarrativeRequest, summaries: number) => void = () => {}): NarrativeClient & { asked: NarrativeRequest[]; summaries: () => number } {
  const asked: NarrativeRequest[] = [];
  const summaries = (): number => asked.filter((r) => r.system === APEX_SUMMARY_PROMPT).length;
  return {
    asked,
    summaries,
    complete: async (request) => {
      look(request, summaries() + (request.system === APEX_SUMMARY_PROMPT ? 1 : 0));
      asked.push(request);
      const text = request.system === APEX_SUMMARY_PROMPT ? JSON.stringify({ summary: `Runs the work of ${request.prompt.match(/Its name: (\w+)/)![1]} when it is called.` }) : JSON.stringify({ paragraphs: [{ text: "We looked at this.", sources: [] }] });
      return { stopReason: "end_turn", model: request.model, text };
    },
  };
}

async function setup(overrides: Parameters<typeof baseDeps>[0] = {}) {
  const deps = baseDeps({ dataTransport: new ReplayTransport(fiveRecording()), ...overrides });
  deps.store.addRun(runInput({ answers: loadAnswers() }));
  await deps.tokens.save(syntheticConnection());
  return { deps, handle: createJobHandler(deps), dir: join(deps.runsDir, RUN_ID) };
}

const has = (dir: string, file: string): boolean => existsSync(join(dir, file));

describe("summaries and a run stopped or failed part way", () => {
  test("(i) an Apex checkpoint made with the switch off, resumed with it on: the step is reused, nothing kept or asked, no summaries file, and the run is done", async () => {
    const client = claude();
    const { deps, handle, dir } = await setup({ dataTransport: new AfterApex(new ReplayTransport(fiveRecording()), "budget"), narrative: client, apexSummaries: false });
    assert.equal((await handle(job, once)).outcome, "stopped_budget");
    assert.ok(has(dir, "checkpoints/apex.json") && !has(dir, APEX_SOURCE_FILE));
    deps.apexSummaries = true;
    deps.dataTransport = new ReplayTransport(fiveRecording());
    assert.equal((await handle(job, once)).outcome, "done");
    assert.ok(deps.lines.includes("  apex: reused from checkpoint"));
    assert.equal(client.summaries(), 0);
    assert.ok(!has(dir, APEX_SOURCE_FILE) && !has(dir, SUMMARIES_FILE) && !has(dir, SUMMARIES_PARTIAL_FILE));
  });

  test("(ii) the switch on at the step and off at the phase: the budget stop keeps the code, and the resumed run asks nothing and deletes it", async () => {
    const client = claude();
    const { deps, handle, dir } = await setup({ dataTransport: new AfterApex(new ReplayTransport(fiveRecording()), "budget"), narrative: client, apexSummaries: true });
    assert.equal((await handle(job, once)).outcome, "stopped_budget");
    assert.equal(ApexSource.parse(JSON.parse(readFileSync(join(dir, APEX_SOURCE_FILE), "utf8"))).items.length, 5, "kept until the run resumes");
    deps.apexSummaries = false;
    deps.dataTransport = new ReplayTransport(fiveRecording());
    assert.equal((await handle(job, once)).outcome, "done");
    assert.equal(client.summaries(), 0);
    assert.ok(!has(dir, APEX_SOURCE_FILE) && !has(dir, SUMMARIES_FILE));
  });

  test("a run that fails and will be delivered again keeps the code; one that fails for good deletes it", async () => {
    const client = claude();
    const { deps, handle, dir } = await setup({ dataTransport: new AfterApex(new ReplayTransport(fiveRecording()), "network"), narrative: client, apexSummaries: true });
    await assert.rejects(handle(job, once), /fetch failed/);
    assert.ok(has(dir, APEX_SOURCE_FILE), "another delivery resumes and uses it");
    writeFileSync(join(dir, SUMMARIES_PARTIAL_FILE), "");
    const outcome = await handle(job, last);
    assert.equal(outcome.outcome, "failed");
    assert.equal(deps.store.runs.get(RUN_ID)!.run.status, "failed");
    assert.ok(!has(dir, APEX_SOURCE_FILE) && !has(dir, SUMMARIES_PARTIAL_FILE));
    assert.equal(client.summaries(), 0);
  });
});

describe("the switch on with no Claude to ask", () => {
  test("a full run keeps no code at any request to Salesforce nor at the end", async () => {
    let dir = "";
    let looked = 0;
    const seen: number[] = [];
    const replay = new ReplayTransport(fiveRecording());
    const watching: Transport = { send: (request) => (looked++, dir && has(dir, APEX_SOURCE_FILE) && seen.push(looked), replay.send(request)) };
    const run = await setup({ dataTransport: watching, apexSummaries: true });
    dir = run.dir;
    assert.equal((await run.handle(job, once)).outcome, "done");
    assert.ok(looked > 10);
    assert.deepEqual(seen, []);
    assert.ok(!has(dir, APEX_SOURCE_FILE) && !has(dir, SUMMARIES_FILE) && !has(dir, SUMMARIES_PARTIAL_FILE));
  });

  test("a budget stop after the Apex step leaves none", async () => {
    const { handle, dir } = await setup({ dataTransport: new AfterApex(new ReplayTransport(fiveRecording()), "budget"), apexSummaries: true });
    assert.equal((await handle(job, once)).outcome, "stopped_budget");
    assert.ok(has(dir, "checkpoints/apex.json"), "the Apex step finished");
    assert.ok(!has(dir, APEX_SOURCE_FILE));
  });
});

describe("a job delivered again", () => {
  /**
   * A single delivery, with the run folder copied as a crash would have left it
   * at two moments: when the fourth summary was asked for, three kept; and when
   * the first section was asked for, the summaries written and the code deleted.
   */
  async function single() {
    const snapshot = join(temp("hc-summaries-snapshot-"), "runs", RUN_ID);
    const written = join(temp("hc-summaries-written-"), "runs", RUN_ID);
    let dir = "";
    const copy = (to: string): void => {
      mkdirSync(dirname(to), { recursive: true });
      cpSync(dir, to, { recursive: true });
    };
    const client = claude((request, n) => {
      if (request.system === APEX_SUMMARY_PROMPT && n === 4) copy(snapshot);
      if (request.system !== APEX_SUMMARY_PROMPT && n === 5 && !existsSync(written)) copy(written);
    });
    const first = await setup({ narrative: client, apexSummaries: true });
    dir = first.dir;
    assert.equal((await first.handle(job, once)).outcome, "done");
    assert.equal(client.summaries(), 5);
    assert.equal(readFileSync(join(snapshot, SUMMARIES_PARTIAL_FILE), "utf8").trim().split("\n").length, 3);
    return { ...first, client, snapshot, written };
  }

  /** The same job, delivered again to the folder a crash left. */
  async function again(snapshot: string) {
    const client = claude();
    const second = await setup({ narrative: client, apexSummaries: true, runsDir: dirname(snapshot) });
    assert.equal((await second.handle(job, once)).outcome, "done");
    return { ...second, client };
  }

  test("(iii) after three of five summaries were kept: two calls, not five, and the same file as a single delivery's", async () => {
    const first = await single();
    const second = await again(first.snapshot);
    assert.equal(second.client.summaries(), 2);
    assert.equal(readFileSync(join(second.dir, SUMMARIES_FILE), "utf8"), readFileSync(join(first.dir, SUMMARIES_FILE), "utf8"));
    assert.ok(!has(second.dir, APEX_SOURCE_FILE) && !has(second.dir, SUMMARIES_PARTIAL_FILE));
  });

  test("(iv) after the summaries file was written and the code deleted: no calls, and the file untouched", async () => {
    const first = await single();
    const path = join(first.written, SUMMARIES_FILE);
    assert.ok(!has(first.written, APEX_SOURCE_FILE) && !has(first.written, SUMMARIES_PARTIAL_FILE));
    const before = { text: readFileSync(path, "utf8"), mtime: statSync(path).mtimeMs };
    assert.equal(before.text, readFileSync(join(first.dir, SUMMARIES_FILE), "utf8"));
    const second = await again(first.written);
    assert.equal(second.client.summaries(), 0);
    assert.deepEqual({ text: readFileSync(path, "utf8"), mtime: statSync(path).mtimeMs }, before);
  });

  test("(v) a class whose body changed between deliveries, because the step ran again, is asked about again", async () => {
    const first = await single();
    const sourcePath = join(first.snapshot, APEX_SOURCE_FILE);
    const source = ApexSource.parse(JSON.parse(readFileSync(sourcePath, "utf8")));
    source.items[0]!.body = `${source.items[0]!.body}\n// changed since`;
    writeFileSync(sourcePath, JSON.stringify(source));
    const second = await again(first.snapshot);
    assert.equal(second.client.summaries(), 3);
    const changed = second.client.asked.filter((r) => r.system === APEX_SUMMARY_PROMPT).map((r) => r.prompt.match(/Its name: (\w+)/)![1]);
    assert.equal(changed[0], source.items[0]!.name);
  });
});
