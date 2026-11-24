import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { checkedDigest, type CheckedDigest } from "../../digest/checked.ts";
import { buildReport, type ReportInput } from "../../narrative/sections.ts";
import { loadRun } from "../../render/render-run.ts";
import { HELD_FOR_US_MASKS } from "../../render/render-released.ts";
import { MASK } from "../../render/repair.ts";
import { Release, SectionVersion, type Finding, type ReportSection } from "../../schemas/index.ts";
import { AutomaticReplay, automaticSyntheticCalibration } from "../../testing/automatic.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { CONSOLE_CHECKS } from "../console-checks.ts";
import { MemoryWorkerStore } from "../memory-store.ts";
import { RUN, runSql, BUDGET_FLAG_SQL, FAILURE_FLAG_SQL, FLAG_CONFLICT, HELD_REASON_SQL, HOLD_OUTPUT_SQL, HOLD_RUN_SQL, LAST_RELEASED_SCORE_SQL, PostgresWorkerStore, RAISE_FLAG_SQL, RELEASE_ROW_SQL, RELEASE_RUN_SQL, RUN_DIGEST_SQL, type SqlClient, type SqlPool, type SqlResult } from "../postgres-store.ts";
import { checkSections, decideRelease, SCORE_MOVE_POINTS, unmeasuredRules, type HoldReason } from "../release.ts";
import { progressOf, RunClosedError, type RunOutputRecord, type RunOverall } from "../types.ts";
import { consoleRun, RUN_ID, runInput } from "./helpers.ts";

// How an automatic run is released or held, check by check (DECISIONS.md 693),
// over the synthetic org as an automatic run reads it.

const calibration = await automaticSyntheticCalibration();
const made = await runFromRecording({ calibration, reviewMode: "automatic", transport: new AutomaticReplay() });
const loaded = loadRun(made.dir);
const NOW = "2026-11-24T09:00:00.000Z";
const checked = (input: ReportInput = loaded.input): CheckedDigest => checkedDigest({ input, pseudonyms: loaded.pseudonyms, inventory: null, madeAt: NOW });
const sectionsOf = (): ReportSection[] => {
  const report = buildReport(loaded.input);
  checkSections(report, loaded.pseudonyms);
  return Object.values(report.outputs).flatMap((o) => o.sections);
};
const clean = checked();
const base = { runId: RUN_ID, sections: sectionsOf(), checked: clean, findings: loaded.input.findings, overall: { score: 60, grade: "C" as const } as RunOverall | null, previousScore: null as number | null, readsLost: null as { lost: number } | null, now: NOW };

describe("the synthetic org as an automatic run reads it", () => {
  test("its digest traces whole, with every sentence marked, and its sections all pass: it is released", () => {
    assert.deepEqual({ masks: clean.masks, unmatched: clean.unmatched, unmarked: clean.unmarked, held: clean.held }, { masks: 0, unmatched: 0, unmarked: 0, held: null });
    assert.ok(base.sections.every((s) => s.status === "checked" && s.approval === null));
    const d = decideRelease(base);
    assert.equal(d.kind, "release");
    for (const r of d.kind === "release" ? d.releases : []) assert.deepEqual([r.kind, r.releasedBy], ["automatic", null]);
  });
});

describe("rule 10 on the digest: figures trace, an untraceable detail is withheld, org words marked, nothing held for us", () => {
  const PLANTED = 987654;
  const planting = (n: number): ReportInput => {
    const issues = loaded.input.findings.filter((f) => f.status === "issue" && f.orgWords?.summary).slice(0, n).map((f) => f.id);
    return { ...loaded.input, findings: loaded.input.findings.map((f) => (issues.includes(f.id) ? withSummary(f, `${f.summary} It took ${PLANTED} steps.`) : f)) };
  };
  // A finding's own text is in the set by construction (682), so the planted figure is taken out of the set the check reads:
  // what's left is the case the check is for, a figure the digest serves that the run's data doesn't hold.
  const withoutPlanted = (input: ReportInput) => {
    const report = buildReport(input);
    report.dataset = { ...report.dataset, figures: report.dataset.figures.filter((f) => f.value !== PLANTED) };
    return report;
  };
  const checkedWithout = (input: ReportInput): CheckedDigest => checkedDigest({ input, pseudonyms: loaded.pseudonyms, inventory: null, madeAt: NOW, build: withoutPlanted });

  test("a figure the data doesn't hold is withheld where the data writes it, and the digest still traces, its sentence still marked", () => {
    const c = checkedWithout(planting(1));
    assert.deepEqual({ held: c.held, masks: c.masks, unmatched: c.unmatched, unmarked: c.unmarked }, { held: null, masks: 1, unmatched: 0, unmarked: 0 });
    const text = JSON.stringify(c.digest);
    assert.ok(!text.includes(String(PLANTED)));
    assert.ok(text.includes(`It took ${MASK} steps.`));
  });

  test(`more than ${HELD_FOR_US_MASKS} masks is held for us: digest_held`, () => {
    const c = checkedWithout(planting(HELD_FOR_US_MASKS + 1));
    assert.equal(c.masks, HELD_FOR_US_MASKS + 1);
    assert.equal(c.held, "digest_held");
    const d = decideRelease({ ...base, checked: c });
    assert.deepEqual(d.kind === "hold" && [d.reason, d.counts], ["digest_held", { masks: HELD_FOR_US_MASKS + 1, ceiling: HELD_FOR_US_MASKS, unmatched: 0, unmarked: 0 }]);
    // Exactly the ceiling is not past it.
    assert.equal(checkedWithout(planting(HELD_FOR_US_MASKS)).held, null);
  });

  test("a figure no mask can reach is held: digest_held", () => {
    // An area's reason isn't a field the run's data masks, so a figure planted there stays untraced.
    const scores = (loaded.input.scores ?? []).map((sc, i) => (i === 0 ? { status: "not_measurable" as const, domain: sc.domain, ruleSetVersion: sc.ruleSetVersion, reason: `Only ${PLANTED} checks ran.` } : sc));
    const c = checkedWithout({ ...loaded.input, scores });
    assert.ok(c.unmatched > 0);
    assert.equal(c.held, "digest_held");
  });

  test("a sentence whose org words can't be marked is held: digest_held", () => {
    // Unknown marks: the sentence was written outside `said`, as no rule's is.
    const input: ReportInput = { ...loaded.input, findings: loaded.input.findings.map((f, i) => (i === 0 ? { ...f, orgWords: undefined } : f)) as Finding[] };
    const c = checked(input);
    assert.ok(c.unmarked > 0);
    assert.equal(c.held, "digest_held");
  });

  test("a mask through the org's own words leaves the sentence unknown, so held", () => {
    // The figure planted inside a marked span: masking it would cut the org's words.
    const target = loaded.input.findings.find((f) => f.status === "issue" && f.orgWords?.summary && f.orgWords.summary.spans.length > 0)!;
    const span = target.orgWords!.summary!.spans[0]!;
    const summary = `${target.summary.slice(0, span.end)} ${PLANTED}${target.summary.slice(span.end)}`;
    const marked = { ...target, summary, orgWords: { ...target.orgWords, summary: { spans: [{ start: span.start, end: span.end + 1 + String(PLANTED).length }], textSha256: sha(summary) } } };
    const c = checkedWithout({ ...loaded.input, findings: loaded.input.findings.map((f) => (f.id === target.id ? marked : f)) });
    assert.ok(c.masks >= 1);
    assert.ok(c.unmarked >= 1);
    assert.equal(c.held, "digest_held");
  });

  test("no digest at all is held: no_digest", () => {
    const c = checkedDigest({ input: loaded.input, pseudonyms: loaded.pseudonyms, inventory: null, madeAt: NOW, build: () => { throw new Error("no report"); } });
    assert.deepEqual(c, { digest: null, masks: 0, unmatched: 0, unmarked: 0, held: "no_digest" });
    const d = decideRelease({ ...base, checked: c });
    assert.equal(d.kind === "hold" && d.reason, "no_digest");
  });
});

describe("rule 10 on the sections: a draft that fails falls back to our own wording", () => {
  const withDraft = (body: string, breakEngine = false) => {
    const report = buildReport(loaded.input);
    const built = report.outputs.executive_summary!;
    const section = built.sections[0]!;
    const engine = built.versions.find((v) => v.id === section.currentVersionId)!;
    // A draft whose stored check passed, as a scoped check can, but whose printed text holds a figure the data doesn't.
    const draft = SectionVersion.parse({ ...engine, id: `${section.id}:v2`, version: 2, body, source: "claude_draft", changedBy: "claude-model", numberCheck: { passed: true, checkedAt: NOW, unmatched: [] } });
    built.versions.push(draft);
    section.currentVersionId = draft.id;
    if (breakEngine) built.versions[built.versions.indexOf(engine)] = { ...engine, body: `${engine.body} We counted 987654 of them.` };
    const result = checkSections(report, loaded.pseudonyms);
    return { result, section: report.outputs.executive_summary!.sections.find((s) => s.id === section.id)!, engine, sections: Object.values(report.outputs).flatMap((o) => o.sections) };
  };

  test("the draft that fails as printed is replaced by the engine's own text, which passes, so the section is checked", () => {
    const { result, section, engine } = withDraft("We found 987654 issues.");
    assert.deepEqual(result, { fellBack: 1, failed: 0 });
    assert.equal(section.currentVersionId, engine.id);
    assert.equal(section.status, "checked");
  });

  test("when the engine's own text fails too, the section isn't checked and the run is held", () => {
    const { result, section, sections } = withDraft("We found 987654 issues.", true);
    assert.deepEqual(result, { fellBack: 1, failed: 1 });
    assert.equal(section.status, "needs_input");
    const d = decideRelease({ ...base, sections });
    assert.deepEqual(d.kind === "hold" && [d.reason, d.counts], ["section_failed", { sections: sections.length, notChecked: 1 }]);
  });

  test("a draft that passes is kept, checked", () => {
    const { result, section } = withDraft("We looked at your Salesforce set-up.");
    assert.deepEqual(result, { fellBack: 0, failed: 0 });
    assert.equal(section.status, "checked");
    assert.match(section.currentVersionId!, /:v2$/);
  });
});

describe("the run's own checks", () => {
  test("over half the rules that ran not measurable is held: mostly_unmeasured", () => {
    const rule = (id: string, status: Finding["status"]) => ({ ...loaded.input.findings[0]!, id: `f-${id}-${status}`, ruleId: id, status });
    const findings = [rule("a", "not_measurable"), rule("b", "not_measurable"), rule("c", "issue"), rule("c", "not_measurable")] as Finding[];
    assert.deepEqual(unmeasuredRules(findings), { rules: 3, notMeasurable: 2 });
    const d = decideRelease({ ...base, findings });
    assert.deepEqual(d.kind === "hold" && [d.reason, d.counts], ["mostly_unmeasured", { rules: 3, notMeasurable: 2 }]);
    // Exactly half is not over half.
    assert.equal(decideRelease({ ...base, findings: findings.slice(1) }).kind, "release");
  });

  test(`a score ${SCORE_MOVE_POINTS} points or more from the last released run's is held: score_moved; ${SCORE_MOVE_POINTS - 1} isn't`, () => {
    const d = decideRelease({ ...base, previousScore: 60 + SCORE_MOVE_POINTS });
    assert.deepEqual(d.kind === "hold" && [d.reason, d.counts], ["score_moved", { score: 60, previous: 75, moved: 15 }]);
    assert.equal(decideRelease({ ...base, previousScore: 60 - SCORE_MOVE_POINTS + 1 }).kind, "release");
    assert.equal(decideRelease({ ...base, previousScore: null }).kind, "release");
    assert.equal(decideRelease({ ...base, overall: null, previousScore: 0 }).kind, "release");
  });

  test("the checks run in rule 10's order, then the run's: the first that fails is the hold", () => {
    const order: Array<[HoldReason, Partial<typeof base>]> = [
      ["section_failed", { sections: base.sections.map((s, i) => (i === 0 ? { ...s, status: "needs_input" as const } : s)) }],
      ["no_digest", { checked: { ...clean, digest: null, held: "no_digest" } }],
      ["digest_held", { checked: { ...clean, held: "digest_held", masks: 11 } }],
      ["reads_lost", { readsLost: { lost: 3 } }],
      ["mostly_unmeasured", { findings: loaded.input.findings.map((f) => ({ ...f, status: "not_measurable" as const, notMeasurableReason: "x" })) }],
      ["score_moved", { previousScore: 0 }],
    ];
    for (let i = 0; i < order.length; i++) {
      // Applied last first, so where two set the same field the earlier check's value stands.
      const all = Object.assign({}, base, ...order.slice(i).reverse().map(([, o]) => o));
      const d = decideRelease(all);
      assert.equal(d.kind === "hold" && d.reason, order[i]![0]);
      // Every later check that failed is decided too (no digest_held where an earlier check took the digest away), each a flag of its own (DECISIONS.md 715).
      assert.deepEqual(d.kind === "hold" && d.also.map((a) => a.kind), order.slice(i + 1).map(([k]) => k).filter((k) => k !== "digest_held" || i > 1));
    }
  });

  test("lost reads: held reads_lost with the four counts, the digest's own two among them; with no digest, only the reads'", () => {
    const lostFindings = loaded.input.findings.map((f, i) => (i === 0 ? { ...f, status: "not_measurable" as const, notMeasurableReason: "Rule words: Task couldn't be read, because Salesforce didn't answer as it should." } : f));
    const ruleLost = lostFindings[0]!.ruleId;
    const expected = loaded.input.findings.filter((f) => f.ruleId === ruleLost).length === 1 ? 1 : 0;
    const d = decideRelease({ ...base, findings: lostFindings, readsLost: { lost: 4 } });
    assert.deepEqual(d.kind === "hold" && [d.reason, d.counts, d.also], ["reads_lost", { lost: 4, notMeasurable: expected, unmatched: 0, unmarked: 0 }, []]);
    const none = decideRelease({ ...base, checked: { ...clean, digest: null, held: "no_digest" }, readsLost: { lost: 4 } });
    assert.deepEqual(none.kind === "hold" && [none.reason, none.also], ["no_digest", [{ kind: "reads_lost", counts: { lost: 4, notMeasurable: 0 } }]]);
    assert.equal(decideRelease({ ...base, readsLost: null }).kind, "release");
  });

  test("lost reads and an unsafe digest: digest_held, reads_lost beside it with the same digest counts", () => {
    const unsafe = { ...clean, held: "digest_held" as const, unmarked: 2 };
    const d = decideRelease({ ...base, checked: unsafe, readsLost: { lost: 1 } });
    assert.deepEqual(d.kind === "hold" && [d.reason, d.also], ["digest_held", [{ kind: "reads_lost", counts: { lost: 1, notMeasurable: 0, unmatched: 0, unmarked: 2 } }]]);
  });
});

// ─── The stores ──────────────────────────────────────────────────────────────

const PREVIOUS = "hc-2026-09-30-00000001";
const PERSON = Release.parse({ id: "rel-0001", runId: PREVIOUS, output: "executive_summary", sectionIds: ["s-0001"], releasedBy: "consultant@example.com", releasedAt: "2026-10-01T09:00:00.000Z" });

function storeWithRuns(): MemoryWorkerStore {
  const store = new MemoryWorkerStore();
  store.addRun(runInput({ run: consoleRun({ id: PREVIOUS, status: "released" }) }));
  store.releases.set(PREVIOUS, [structuredClone(PERSON)]);
  store.addRun(runInput({ run: consoleRun({ reviewMode: "automatic" }), calibration }));
  return store;
}

const record = (automatic: RunOutputRecord["automatic"], digest = clean.digest): RunOutputRecord => ({
  progress: { ...progressOf(made.run), status: "in_review" },
  runDir: made.dir,
  findings: [],
  metrics: [],
  scores: [],
  manualSteps: [],
  sections: base.sections,
  versions: [],
  facts: null,
  factsFile: null,
  digest,
  ...(automatic ? { automatic } : {}),
});
const holdOf = (reason: string, counts: Record<string, number> = { n: 1 }, also?: Array<{ kind: string; counts: Record<string, number> }>): RunOutputRecord["automatic"] => ({ decision: "hold", reason, counts, ...(also ? { also } : {}), at: NOW });

describe("a hold in the memory store, held to the console's own checks", () => {
  for (const reason of ["section_failed", "no_digest", "digest_held", "mostly_unmeasured", "score_moved"] as const) {
    test(`${reason}: not released, no release row, the reason set, one flag under redelivery, the earlier release untouched`, async () => {
      const store = storeWithRuns();
      await store.saveRunOutput(RUN_ID, record(holdOf(reason), reason === "no_digest" ? null : clean.digest));
      await assert.rejects(store.saveRunOutput(RUN_ID, record(holdOf(reason))), RunClosedError);
      const run = store.runs.get(RUN_ID)!;
      assert.notEqual(run.run.status, "released");
      assert.equal(store.releases.has(RUN_ID), false);
      assert.equal(store.held.get(RUN_ID), reason);
      assert.deepEqual(store.flagsOf(RUN_ID), [reason]);
      assert.deepEqual(store.flags.get(`${RUN_ID}|${reason}`)!.counts, { n: 1 });
      assert.deepEqual(store.releases.get(PREVIOUS), [PERSON]);
      assert.equal(store.runs.get(PREVIOUS)!.run.status, "released");
    });
  }

  test("a hold with other failed checks raises each flag, all at the hold's time; the console's checks hold for every one", async () => {
    const store = storeWithRuns();
    await store.saveRunOutput(RUN_ID, record(holdOf("digest_held", { masks: 0, ceiling: 10, unmatched: 0, unmarked: 2 }, [{ kind: "reads_lost", counts: { lost: 1, notMeasurable: 0, unmatched: 0, unmarked: 2 } }])));
    assert.equal(store.held.get(RUN_ID), "digest_held");
    assert.deepEqual(store.flagsOf(RUN_ID).sort(), ["digest_held", "reads_lost"]);
    assert.equal(store.flags.get(`${RUN_ID}|reads_lost`)!.raisedAt, NOW);
    // A kind the console's CHECK doesn't take refuses the whole save.
    const refused = storeWithRuns();
    await assert.rejects(refused.saveRunOutput(RUN_ID, record(holdOf("digest_held", undefined, [{ kind: "lost_reads", counts: {} }]))), /HealthCheckRunFlag_kind_check/);
    assert.equal(refused.held.has(RUN_ID), false);
  });

  test("a consultant-led run's failure raises no flag", async () => {
    const store = new MemoryWorkerStore();
    store.addRun(runInput());
    await store.recordRunFailure(RUN_ID, "broken", NOW);
    assert.deepEqual(store.flagsOf(RUN_ID), []);
  });

  test("a release with no digest, or naming anyone, or on a consultant-led run, is refused whole", async () => {
    const release = decideRelease(base);
    assert.equal(release.kind, "release");
    const rows = release.kind === "release" ? release.releases : [];
    const refused = async (store: MemoryWorkerStore, r: RunOutputRecord) => {
      await assert.rejects(store.saveRunOutput(RUN_ID, r));
      assert.equal(store.runs.get(RUN_ID)!.output, null);
      assert.equal(store.releases.has(RUN_ID), false);
    };
    await refused(storeWithRuns(), record({ decision: "release", releases: rows }, null));
    const named = rows.map((r) => ({ ...r, releasedBy: "Barometer" }));
    await refused(storeWithRuns(), record({ decision: "release", releases: named }));
    const consultant = new MemoryWorkerStore();
    consultant.addRun(runInput());
    await refused(consultant, record({ decision: "release", releases: rows }));
    const noTable = storeWithRuns();
    noTable.digestTable = false;
    await refused(noTable, record({ decision: "release", releases: rows }));
  });
});

type Handler = (text: string, values: unknown[]) => SqlResult | undefined;
class FakePool implements SqlPool {
  readonly statements: Array<{ text: string; values: unknown[]; client?: true }> = [];
  readonly handlers: Handler[] = [(text) => (text.includes("FOR UPDATE") ? { rows: [{ status: "analysing" }], rowCount: 1 } : undefined)];
  async query(text: string, values: unknown[] = [], client = false): Promise<SqlResult> {
    this.statements.push({ text, values, ...(client ? { client: true as const } : {}) });
    for (const h of this.handlers) {
      const r = h(text, values);
      if (r) return r;
    }
    return { rows: [], rowCount: 1 };
  }
  async connect(): Promise<SqlClient & { release(): void }> {
    return { query: (t, v) => this.query(t, v, true), release: () => {} };
  }
  texts(): string[] {
    return this.statements.map((s) => s.text);
  }
}

describe("the Postgres store's release and hold", () => {
  const release = decideRelease(base);
  const rows = release.kind === "release" ? release.releases : [];

  test("released in the one transaction: the digest, then each release row naming nobody, then the status, then COMMIT", async () => {
    const pool = new FakePool();
    await new PostgresWorkerStore(pool).saveRunOutput(RUN_ID, record({ decision: "release", releases: rows }));
    const texts = pool.texts();
    const [digest, first, status] = [texts.indexOf(RUN_DIGEST_SQL), texts.indexOf(RELEASE_ROW_SQL), texts.indexOf(RELEASE_RUN_SQL)];
    assert.ok(texts[0] === "BEGIN" && digest > 0 && digest < first && first < status && status === texts.length - 2 && texts.at(-1) === "COMMIT", texts.join("\n"));
    // Never in a savepoint: a refused digest refuses the release.
    assert.ok(!texts.includes("SAVEPOINT run_digest"));
    assert.equal(texts.filter((t) => t === RELEASE_ROW_SQL).length, rows.length);
    assert.match(RELEASE_ROW_SQL, /VALUES \(\$1, \$2, \$3, \$4::text\[\], 'automatic', NULL, \$5::timestamp\(3\)\)/);
    for (const s of pool.statements.filter((x) => x.text === RELEASE_ROW_SQL)) assert.equal(s.values.length, 5);
    assert.match(RELEASE_RUN_SQL, /"reviewMode" = 'automatic'/);
    // Every one of them on the transaction's own connection, never the pool's.
    for (const s of pool.statements.filter((x) => [RUN_DIGEST_SQL, RELEASE_ROW_SQL, RELEASE_RUN_SQL].includes(x.text))) assert.equal(s.client, true, s.text);
  });

  test("a digest the database refuses, or a run it doesn't have as automatic, rolls the whole save back", async () => {
    for (const handler of [((t: string) => (t === RUN_DIGEST_SQL ? (() => { throw Object.assign(new Error("refused"), { code: "23514" }); })() : undefined)) as Handler, ((t: string) => (t === RELEASE_RUN_SQL ? { rows: [], rowCount: 0 } : undefined)) as Handler]) {
      const pool = new FakePool();
      pool.handlers.unshift(handler);
      await assert.rejects(new PostgresWorkerStore(pool).saveRunOutput(RUN_ID, record({ decision: "release", releases: rows })));
      assert.equal(pool.texts().at(-1), "ROLLBACK");
      assert.ok(!pool.texts().includes("COMMIT"));
    }
  });

  test("no digest: refused before any release row", async () => {
    const pool = new FakePool();
    await assert.rejects(new PostgresWorkerStore(pool).saveRunOutput(RUN_ID, record({ decision: "release", releases: rows }, null)));
    assert.ok(!pool.texts().includes(RELEASE_ROW_SQL));
    assert.equal(pool.texts().at(-1), "ROLLBACK");
  });

  test("held: the reason, then its flag raised once with its counts, last before COMMIT; no release row", async () => {
    const pool = new FakePool();
    await new PostgresWorkerStore(pool).saveRunOutput(RUN_ID, record(holdOf("score_moved", { score: 60, previous: 80, moved: 20 })));
    const texts = pool.texts();
    assert.deepEqual(texts.slice(-3), [HOLD_OUTPUT_SQL, RAISE_FLAG_SQL, "COMMIT"]);
    assert.deepEqual(pool.statements.find((s) => s.text === RAISE_FLAG_SQL)!.values, [RUN_ID, "score_moved", NOW, JSON.stringify({ score: 60, previous: 80, moved: 20 })]);
    // Raised once while open; a cleared flag is opened again (DECISIONS.md 698).
    assert.ok(RAISE_FLAG_SQL.endsWith(FLAG_CONFLICT));
    assert.ok(!texts.includes(RELEASE_ROW_SQL) && !texts.includes(RELEASE_RUN_SQL));
  });

  test("held with other failed checks: each flag raised after the hold, in order, before COMMIT", async () => {
    const pool = new FakePool();
    await new PostgresWorkerStore(pool).saveRunOutput(RUN_ID, record(holdOf("reads_lost", { lost: 2, notMeasurable: 1, unmatched: 0, unmarked: 0 }, [{ kind: "mostly_unmeasured", counts: { rules: 3, notMeasurable: 2 } }])));
    assert.deepEqual(pool.texts().slice(-4), [HOLD_OUTPUT_SQL, RAISE_FLAG_SQL, RAISE_FLAG_SQL, "COMMIT"]);
    assert.deepEqual(pool.statements.filter((s) => s.text === RAISE_FLAG_SQL).map((s) => s.values.slice(1, 3)), [["reads_lost", NOW], ["mostly_unmeasured", NOW]]);
  });

  test("a budget stop's flag: only an automatic run stopped at its budget, counts {}, raised once while open", async () => {
    const pool = new FakePool();
    await new PostgresWorkerStore(pool).flagBudgetStop(RUN_ID, NOW);
    assert.deepEqual(pool.statements.find((s) => s.text === BUDGET_FLAG_SQL)!.values, [RUN_ID, NOW]);
    assert.match(BUDGET_FLAG_SQL, /SELECT "id", 'stopped_budget', \$2::timestamp\(3\), '\{\}'::jsonb FROM "HealthCheckRun" WHERE "id" = \$1 AND "reviewMode" = 'automatic' AND "status" = 'stopped_budget' /);
    assert.ok(BUDGET_FLAG_SQL.endsWith(FLAG_CONFLICT));
  });

  test("probe A and C on Postgres: the release and both holds only ever touch an unheld run, and the release never clears a hold", () => {
    assert.match(RELEASE_RUN_SQL, /WHERE "id" = \$1 AND "reviewMode" = 'automatic' AND "heldReason" IS NULL AND "status" NOT IN \('released', 'failed'\)$/);
    assert.doesNotMatch(RELEASE_RUN_SQL, /SET [^W]*"heldReason"/);
    assert.match(HOLD_OUTPUT_SQL, /AND "heldReason" IS NULL AND "status" <> 'released'$/);
    assert.match(HOLD_RUN_SQL, /AND "heldReason" IS NULL AND "status" <> 'released' RETURNING "id"/);
  });

  test("probe H on Postgres: an automatic save on a run that failed meanwhile is refused before anything is written", async () => {
    const pool = new FakePool();
    pool.handlers.unshift((t) => (t.includes("FOR UPDATE") ? { rows: [{ status: "failed" }], rowCount: 1 } : undefined));
    await assert.rejects(new PostgresWorkerStore(pool).saveRunOutput(RUN_ID, record({ decision: "release", releases: rows })), RunClosedError);
    assert.ok(!pool.texts().some((t) => t === RELEASE_ROW_SQL || t === RELEASE_RUN_SQL || t.startsWith(`UPDATE "HealthCheckRun" SET "status" = $2`)));
    assert.equal(pool.texts().at(-1), "ROLLBACK");
    // A consultant-led save is as before: a failed run's results are still written by a later delivery.
    const consultant = new FakePool();
    consultant.handlers.unshift((t) => (t.includes("FOR UPDATE") ? { rows: [{ status: "failed" }], rowCount: 1 } : undefined));
    await new PostgresWorkerStore(consultant).saveRunOutput(RUN_ID, record(undefined));
    assert.equal(consultant.texts().at(-1), "COMMIT");
  });

  test("a hold that hits no row (a run already held) rolls the whole save back; the first hold stands", async () => {
    const pool = new FakePool();
    pool.handlers.unshift((t) => (t === HOLD_OUTPUT_SQL ? { rows: [], rowCount: 0 } : undefined));
    await assert.rejects(new PostgresWorkerStore(pool).saveRunOutput(RUN_ID, record(holdOf("score_moved"))), /unheld automatic run/);
    assert.equal(pool.texts().at(-1), "ROLLBACK");
    assert.ok(!pool.texts().includes(RAISE_FLAG_SQL));
  });

  test("an automatic run's hold is read with its job, so the run job can refuse it; a consultant-led run's reads are as before", async () => {
    const pool = new FakePool();
    const { reviewMode: _m, analysisWindow, steps: _s, ...rest } = consoleRun({ reviewMode: "automatic" });
    pool.handlers.unshift((t) => (t === runSql(RUN) ? { rows: [{ ...rest, analysisFrom: analysisWindow.from, analysisTo: analysisWindow.to, connectionName: "synthetic-co", reviewMode: "automatic" }], rowCount: 1 } : t === HELD_REASON_SQL ? { rows: [{ heldReason: "score_moved" }], rowCount: 1 } : undefined));
    const input = await new PostgresWorkerStore(pool).loadRunJob(RUN_ID);
    assert.equal(input?.heldReason, "score_moved");
    const consultant = new FakePool();
    const { reviewMode: _c, analysisWindow: w, steps: _t, ...r2 } = consoleRun();
    consultant.handlers.unshift((t) => (t === runSql(RUN) ? { rows: [{ ...r2, analysisFrom: w.from, analysisTo: w.to, connectionName: "synthetic-co" }], rowCount: 1 } : undefined));
    const plain = await new PostgresWorkerStore(consultant).loadRunJob(RUN_ID);
    assert.equal(plain && "heldReason" in plain, false);
    assert.ok(!consultant.texts().includes(HELD_REASON_SQL));
  });

  test("the score is compared only with the same org's last released run of the same product, never this one", async () => {
    assert.match(LAST_RELEASED_SCORE_SQL, /r\."status" = 'released'/);
    assert.match(LAST_RELEASED_SCORE_SQL, /JOIN "HealthCheckRelease" l ON l\."runId" = r\."id"/);
    assert.match(LAST_RELEASED_SCORE_SQL, /r\."id" <> \$1 AND left\(r\."orgId", 15\) = \$2 AND r\."product" = \$3/);
    const pool = new FakePool();
    pool.handlers.unshift((t) => (t === LAST_RELEASED_SCORE_SQL ? { rows: [{ score: 72 }], rowCount: 1 } : undefined));
    assert.equal(await new PostgresWorkerStore(pool).lastReleasedScore(RUN_ID, "00D000000000001", "barometer"), 72);
    assert.deepEqual(pool.statements.at(-1)!.values, [RUN_ID, "00D000000000001", "barometer"]);
  });

  test("a failed run's flag is raised in its own savepoint; a console without the table still records the failure", async () => {
    const pool = new FakePool();
    await new PostgresWorkerStore(pool).recordRunFailure(RUN_ID, "broken", NOW);
    assert.deepEqual(pool.statements.find((s) => s.text === FAILURE_FLAG_SQL)!.values, [RUN_ID, NOW]);
    assert.match(FAILURE_FLAG_SQL, /->> 'reviewMode' = 'automatic'/);
    const without = new FakePool();
    without.handlers.unshift((t) => (t === FAILURE_FLAG_SQL ? (() => { throw Object.assign(new Error("no table"), { code: "42P01" }); })() : undefined));
    const lines: string[] = [];
    await new PostgresWorkerStore(without, { log: (l) => void lines.push(l) }).recordRunFailure(RUN_ID, "broken", NOW);
    assert.ok(without.texts().includes("ROLLBACK TO SAVEPOINT run_failed_flag"));
    assert.equal(without.texts().at(-1), "COMMIT");
    assert.match(lines[0]!, /SQLSTATE 42P01/);
  });
});

describe("the console's checks are its migrations' own text", () => {
  // Wherever the console's checkout is beside this one; otherwise there is nothing to compare with.
  const roots = [join(process.cwd(), "..", "..", "hc-console-merge", "migrations"), join(process.cwd(), "..", "..", "worktrees", "hc-console-merge", "migrations")];
  const root = roots.find((r) => existsSync(r));
  test("the latest definition of each check is in its migration word for word, and no later migration redefines it", { skip: root ? false : "the console's migrations aren't beside this checkout" }, () => {
    const files = readdirSync(root!).filter((f) => f.endsWith(".sql")).sort();
    for (const check of Object.values(CONSOLE_CHECKS)) {
      // A console migration not merged there yet (20261218 until the console's run-times lane is): only ever the
      // newest, so an older file gone missing or misnamed still fails here.
      if (!existsSync(join(root!, check.migration))) {
        assert.ok(check.migration > files.at(-1)!, `${check.name}: ${check.migration} isn't among the console's migrations, and isn't newer than all of them`);
        continue;
      }
      const text = readFileSync(join(root!, check.migration), "utf8");
      assert.ok(text.includes(`CONSTRAINT "${check.name}" CHECK (${check.sql})`), check.name);
      const later = files.filter((f) => f > check.migration && readFileSync(join(root!, f), "utf8").includes(`ADD CONSTRAINT "${check.name}"`));
      assert.deepEqual(later, [], check.name);
    }
  });
});

/** A finding with a new summary, its marks moved with it: the planted words are ours. */
function withSummary(f: Finding, summary: string): Finding {
  const held = f.orgWords?.summary;
  return { ...f, summary, ...(held ? { orgWords: { ...f.orgWords, summary: { spans: held.spans, textSha256: sha(summary) } } } : {}) };
}

function sha(text: string): string {
  return createHash("sha256").update(text, "utf8").digest("hex");
}
