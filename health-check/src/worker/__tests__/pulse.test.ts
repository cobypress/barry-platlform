import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import type { NarrativeClient, NarrativeRequest } from "../../narrative/client.ts";
import type { Finding, Score } from "../../schemas/index.ts";
import { loadRecording, ReplayTransport } from "../../testing/replay.ts";
import { PostgresWorkerStore, type SqlPool, type SqlResult } from "../postgres-store.ts";
import { createJobHandler } from "../worker.ts";
import { processDocumentsJob } from "../documents-job.ts";
import { baseDeps, CONNECTION, consoleRun, runInput, syntheticConnection } from "./helpers.ts";

// The monthly pulse (DECISIONS.md 134 to 136): a re-check nobody reviews, whose
// result the console shows the customer. It compares against the baseline the
// console names, keeps its folder for the next pulse, and keeps the connection.

const BASELINE = "hc-2026-09-30-00000001";
const PULSE = "hc-2026-10-31-00000002";
const last = { final: true };

/** A narrative client that records any request and refuses it, so a pulse asking Claude for anything shows. */
function refusingClaude() {
  const asked: NarrativeRequest[] = [];
  const narrative = { complete: async (request: NarrativeRequest) => (asked.push(request), Promise.reject(new Error("a pulse never drafts"))) } as unknown as NarrativeClient;
  return { asked, narrative };
}

/** The baseline as it stood a month ago: two issues it didn't have, two it had that are now fixed, and one area lower. */
function ageBaseline(dir: string): { fixedRules: string[]; newRules: string[] } {
  const findings = JSON.parse(readFileSync(join(dir, "findings.json"), "utf8")) as Finding[];
  const issueRules = [...new Set(findings.filter((f) => f.status === "issue").map((f) => f.ruleId))];
  const passedRules = [...new Set(findings.filter((f) => f.status === "passed").map((f) => f.ruleId))].filter((r) => !issueRules.includes(r));
  const newRules = issueRules.slice(0, 2);
  const fixedRules = passedRules.slice(0, 2);
  const aged = findings.map((f): Finding => {
    if (newRules.includes(f.ruleId) && f.status === "issue") return { ...f, status: "passed", value: { kind: "not_sized", reason: "Passed." } };
    if (fixedRules.includes(f.ruleId)) return { ...f, status: "issue", severity: "medium", title: `${f.title} (then)` };
    return f;
  });
  writeFileSync(join(dir, "findings.json"), JSON.stringify(aged));
  const scores = JSON.parse(readFileSync(join(dir, "scores.json"), "utf8")) as Score[];
  writeFileSync(join(dir, "scores.json"), JSON.stringify(scores.map((s, i) => (i === 0 && s.status === "measured" ? { ...s, score: Math.max(0, s.score - 10) } : s))));
  return { fixedRules, newRules };
}

async function pulseRun(options: { baseline: boolean }) {
  const { asked, narrative } = refusingClaude();
  const deps = baseDeps({ dataTransport: new ReplayTransport(loadRecording()), narrative });
  await deps.tokens.save(syntheticConnection());
  const handle = createJobHandler(deps);
  let aged: ReturnType<typeof ageBaseline> | null = null;
  if (options.baseline) {
    deps.store.addRun(runInput({ run: consoleRun({ id: BASELINE }) }));
    await handle({ kind: "run", runId: BASELINE }, last);
    aged = ageBaseline(join(deps.runsDir, BASELINE));
    asked.length = 0;
  }
  // A replay plays through once, so the pulse reads its own, as a month later's run would read the org afresh.
  deps.dataTransport = new ReplayTransport(loadRecording());
  deps.store.addRun(runInput({ run: consoleRun({ id: PULSE, product: "pulse", baselineRunId: options.baseline ? BASELINE : "hc-2026-08-31-00000009" }) }));
  const outcome = await handle({ kind: "run", runId: PULSE }, last);
  return { deps, outcome, asked, aged };
}

describe("a monthly pulse", async () => {
  const { deps, outcome, asked, aged } = await pulseRun({ baseline: true });
  const saved = deps.store.pulseResults.get(PULSE);

  test("finishes, and writes its result row once its findings are saved", () => {
    assert.equal(outcome.outcome, "done");
    assert.ok(saved, "the result row");
    assert.ok(deps.store.runs.get(PULSE)!.output!.findings.length > 0);
    const order = deps.store.writes.map((w) => `${w.method}:${w.id}`);
    assert.ok(order.indexOf(`saveRunOutput:${PULSE}`) < order.indexOf(`savePulseResult:${PULSE}`));
  });

  test("compares against its baseline: score, worth, each area, and what was fixed and what's new", () => {
    const r = saved!.result;
    assert.equal(r.runId, PULSE);
    assert.equal(r.baselineRunId, BASELINE);
    assert.ok(r.baselineAsOf);
    assert.equal(r.counts.fixed, aged!.fixedRules.length);
    assert.equal(r.counts.new, aged!.newRules.length);
    assert.equal(typeof r.counts.stillOpen, "number");
    assert.equal(r.counts.cantTell, 0);
    assert.ok(r.newIssues.length > 0 && r.newIssues.length <= 5);
    assert.ok(r.fixed.length > 0 && r.fixed.length <= 5);
    assert.ok(r.fixed.every((f) => f.title.endsWith("(then)") && f.area.length > 0));
    assert.ok(r.areas.length > 0 && r.areas.some((a) => a.before !== null && a.now !== null && a.before !== a.now), "the area set lower a month ago");
    assert.equal(typeof r.worth.now, "number");
    assert.equal(typeof r.worth.before, "number");
    assert.equal(r.worth.currency, "GBP");
  });

  test("is team-level, with no names or tokens", () => {
    assert.doesNotMatch(JSON.stringify(saved!.result), /\[\[[a-z]+:|@example\.com/);
  });

  test("never asks Claude, and drafts nothing", () => {
    assert.equal(asked.length, 0);
    assert.ok(!existsSync(join(deps.runsDir, PULSE, "narrative-rejections.json")));
  });

  test("never makes documents, even if asked", async () => {
    const { run } = deps.store.runs.get(PULSE)!;
    deps.store.addDocumentsJob({ run: { ...run, status: "released" }, intake: null, sections: [], versions: [], overrides: [], preparedBy: null, state: "queued", driveFolderId: null, files: [] });
    const made = await processDocumentsJob(PULSE, deps, last);
    assert.equal(made.outcome, "skipped");
    assert.match(made.detail, /pulse/);
  });

  test("keeps its connection and its run folder, for next month", () => {
    assert.equal(deps.tokens.has(CONNECTION), true);
    assert.ok(existsSync(join(deps.runsDir, PULSE, "findings.json")));
  });
});

describe("a pulse without a baseline it can read", async () => {
  const { deps, outcome } = await pulseRun({ baseline: false });

  test("still writes its result, with every 'before' null rather than failing", () => {
    assert.equal(outcome.outcome, "done");
    const r = deps.store.pulseResults.get(PULSE)!.result;
    assert.equal(r.baselineRunId, null);
    assert.equal(r.score.before, null);
    assert.equal(r.worth.before, null);
    assert.deepEqual([r.counts.fixed, r.counts.stillOpen, r.counts.new, r.counts.cantTell], [null, null, null, null]);
    assert.ok(r.counts.openNow > 0);
    assert.ok(r.areas.every((a) => a.before === null));
    assert.deepEqual([r.newIssues, r.fixed], [[], []]);
  });
});

describe("the pulse result row in the console's database", () => {
  test("one row a run, replaced on a second delivery", async () => {
    const statements: Array<{ text: string; values: unknown[] }> = [];
    const pool = { async query(text: string, values: unknown[] = []): Promise<SqlResult> { statements.push({ text, values }); return { rows: [], rowCount: 1 }; } } as unknown as SqlPool;
    const result = { version: 1, runId: PULSE } as never;
    await new PostgresWorkerStore(pool).savePulseResult(PULSE, result, "2026-10-31T12:00:00.000Z");
    assert.match(statements[0]!.text, /INSERT INTO "HealthCheckPulseResult" \("runId", "madeAt", "result"\)/);
    assert.match(statements[0]!.text, /ON CONFLICT \("runId"\) DO UPDATE/);
    assert.match(statements[0]!.text, /\$3::jsonb/);
    assert.deepEqual(statements[0]!.values, [PULSE, "2026-10-31T12:00:00.000Z", JSON.stringify(result)]);
  });
});
