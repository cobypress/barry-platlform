import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { buildDigest } from "../build.ts";
import { marksOf } from "../checked.ts";
import { SALES_CLOUD_MODULES } from "../../domains/index.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { VERDICT_WORDS, type PillarKey, type ReadinessVerdict } from "../../narrative/readiness.ts";
import { buildReport, readinessOpening, type BuiltReport } from "../../narrative/sections.ts";
import { loadRun } from "../../render/render-run.ts";
import { MASK } from "../../render/repair.ts";
import { RunDigest } from "../../schemas/digest.ts";
import { runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { AS_OF, CONNECTED_USER } from "../../testing/synthetic-org.ts";
import { ACCESS, ACTIONS, assertWellFormed, automatic, check, DATA, ENGINE, KNOWLEDGE, MADE_AT, PASSED_ORG, PLATFORM, readinessTexts, scenario, SCENARIOS, SHAPE, TITLES, withoutReadiness } from "./readiness-scenarios.ts";

// AI readiness in the digest (KAN-50, DECISIONS.md 742): the six pillars the
// full report's slide shows, for the portal and the connector, every text
// marked and every number traced. The lesson of decision 739 is that the
// synthetic org reaches few of the code's paths, so every branch of
// readinessOf that writes a reason is built here from constructed inputs, on an
// automatic run, and each digest block it gives is held to its marks and figures.

const consultant = loadRun((await runFromRecording({ answers: loadAnswers() })).dir);
const consultantReport = buildReport(consultant.input);
const consultantDigest = buildDigest({ report: consultantReport, pseudonyms: consultant.pseudonyms, inventory: null, madeAt: MADE_AT });

describe("the synthetic org's readiness in its digest (DECISIONS.md 742)", () => {
  test("holds the six pillars and verdicts the full report's slide shows, and its opening", () => {
    const r = consultantDigest.readiness!;
    assert.deepEqual(
      r.pillars.map((p) => `${p.label}: ${VERDICT_WORDS[p.verdict as ReadinessVerdict]}`),
      ["Data: Not yet", "Knowledge: Ready", "Platform: Nearly", "Actions: Nearly", "Access: Not yet", "Org shape: Nearly"],
    );
    assert.equal(r.opening, "Not yet: data and access need work before an AI agent goes live in this org.");
    assert.equal(r.overall, "not_yet");
    assert.deepEqual(r.pillars.map((p) => [p.verdict, p.reason, p.findingIds]), consultantReport.readiness.pillars.map((p) => [p.verdict, p.reason, p.findingIds]));
    assertWellFormed(consultantDigest, consultantReport);
  });

  test("the opening, labels and questions are marked as the engine's own", () => {
    const r = consultantDigest.readiness!;
    assert.deepEqual(r.orgWords?.opening, ENGINE);
    for (const p of r.pillars) assert.deepEqual([p.orgWords?.label, p.orgWords?.question], [ENGINE, ENGINE], p.key);
  });

  test("an automatic run on it is still released, with nothing unmatched, unmarked or withheld, and masks as without readiness", () => {
    const c = check(automatic.input);
    const without = withoutReadiness(automatic.input);
    assert.equal(without.digest!.readiness, undefined);
    assert.deepEqual({ unmatched: c.unmatched, unmarked: c.unmarked, held: c.held, readinessWithheld: c.readinessWithheld }, { unmatched: 0, unmarked: 0, held: null, readinessWithheld: 0 });
    assert.equal(c.masks, without.masks);
    for (const t of readinessTexts(c.digest!)) {
      assert.ok(marksOf(c.digest!, t), t.path);
      assert.ok(!t.text.includes(MASK), t.path);
    }
    assertWellFormed(c.digest!, buildReport(automatic.input));
  });

  test("a report without readiness gives a digest with no readiness key, and an older digest parses", () => {
    const { readiness: _r, ...rest } = consultantReport;
    const d = buildDigest({ report: rest as BuiltReport, pseudonyms: consultant.pseudonyms, inventory: null, madeAt: MADE_AT });
    assert.ok(!("readiness" in d));
    const { readiness: _x, ...older } = consultantDigest;
    assert.deepEqual(RunDigest.parse(JSON.parse(JSON.stringify(older))), older);
  });

  test("the schema refuses marks that don't fit a reason, and more than five fixes", () => {
    const r = consultantDigest.readiness!;
    const bad = { ...consultantDigest, readiness: { ...r, pillars: r.pillars.map((p, i) => (i === 0 ? { ...p, orgWords: { reason: { orgWordsMarked: true, orgWords: [{ start: 0, end: p.reason.length + 1 }] } } } : p)) } };
    assert.equal(RunDigest.safeParse(bad).success, false);
    assert.equal(RunDigest.safeParse({ ...consultantDigest, readiness: { ...r, fixIds: ["a", "b", "c", "d", "e", "f"] } }).success, false);
  });
});

describe("each product's digest", () => {
  const run = (overrides: Parameters<typeof runHealthCheck>[0] extends infer O ? Partial<O> : never) =>
    runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), answers: loadAnswers(), modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport: new Simulator(), connectedUser: CONNECTED_USER, log: () => {}, ...overrides });

  test("a monthly pulse computes readiness as a check does, and its digest carries it", async () => {
    const made = await run({ product: "pulse" });
    const loaded = loadRun(made.dir);
    const report = buildReport(loaded.input);
    const d = buildDigest({ report, pseudonyms: loaded.pseudonyms, inventory: null, madeAt: MADE_AT });
    assert.equal(d.product, "pulse");
    assert.equal(d.readiness!.opening, readinessOpening(report.readiness));
    assert.deepEqual(d.readiness!.pillars.map((p) => p.verdict), report.readiness.pillars.map((p) => p.verdict));
    assertWellFormed(d, report);
  });

  test("a set-up review says the data is part of the full check, with no verdict words", async () => {
    const made = await run({ scope: "essentials" });
    const loaded = loadRun(made.dir);
    const report = buildReport(loaded.input);
    const d = buildDigest({ report, pseudonyms: loaded.pseudonyms, inventory: null, madeAt: MADE_AT });
    const data = d.readiness!.pillars[0]!;
    assert.deepEqual([data.verdict, data.reason], ["not_measurable", "A set-up review doesn't read records, so the data itself is part of the full check."]);
    assert.deepEqual(data.orgWords?.reason, ENGINE);
    assertWellFormed(d, report);
  });
});

describe("every branch of readinessOf that writes a reason, served marked and traced (DECISIONS.md 742)", () => {
  const hit = new Set<string>();
  const titled = new Set<string>();

  for (let s = 0; s < SCENARIOS; s++) {
    const sc = scenario(s);
    test(`scenario ${s}: ${sc.name}`, () => {
      const c = check(sc.input);
      const d = c.digest!;
      assert.ok(d.readiness, "readiness is served");
      // A finding whose title the run couldn't mark holds the run by itself, readiness or not (the counts test holds them equal).
      const unmarkedTitle = Object.values(sc.picked).some(({ v, t }) => v.quoted && t.unmarked);
      assert.deepEqual({ unmatched: c.unmatched, unmarked: c.unmarked > 0, held: c.held }, { unmatched: 0, unmarked: unmarkedTitle, held: unmarkedTitle ? "digest_held" : null });
      const report = buildReport(sc.input);
      assertWellFormed(d, report);
      for (const t of readinessTexts(d)) assert.ok(marksOf(d, t), `${t.path} unmarked`);
      assert.deepEqual(d.readiness.orgWords?.opening, ENGINE);
      let withheld = 0;
      d.readiness.pillars.forEach((p, i) => {
        const key = p.key as PillarKey;
        const { v, t } = sc.picked[key];
        const made = report.readiness.pillars[i]!;
        assert.equal(p.verdict, v.verdict, `${key}: ${p.reason}`);
        hit.add(`${key}:${v.name}`);
        const marks = p.orgWords?.reason?.orgWords;
        assert.ok(marks, `${key}: no marks`);
        // A reason quoting a title the run couldn't mark is withheld whole, its figures with it: never served as ours.
        if (v.quoted && t.unmarked) {
          withheld++;
          assert.deepEqual([p.reason, marks, p.figures], [MASK, [], undefined], key);
          return;
        }
        assert.ok(!p.reason.includes(MASK), `${key} withheld: ${made.reason}`);
        assert.match(p.reason, v.reason, key);
        const words = marks.map((m) => p.reason.slice(m.start, m.end));
        if (v.quoted) {
          titled.add(`${key}:${TITLES.indexOf(t)}`);
          assert.ok(made.quoted, key);
          // The title's org spans, moved to where the title sits, after any lower-casing.
          assert.deepEqual(words.map((w) => w.toLowerCase()), t.org ? [t.org.toLowerCase()] : [], `${key}: ${p.reason}`);
          if (t.org) assert.equal(marks[0]!.start, made.quoted!.at + t.text.indexOf(t.org));
        } else if (v.passed) {
          assert.equal(made.passedOn, p.metricIds[0]);
          assert.deepEqual(words, [PASSED_ORG], key);
        } else assert.deepEqual(marks, [], `${key}: all ours`);
        if (p.verdict === "not_measurable") assert.doesNotMatch(p.reason.replace(/Salesforce Knowledge is on/, ""), /\b(ready|nearly|not yet)\b/i, key);
      });
      assert.equal(c.readinessWithheld, withheld);
    });
  }

  test("every variant of every pillar was reached, and each quoting form met a title with the org's words in it", () => {
    const expected = [
      ...DATA.map((v) => `data:${v.name}`),
      ...KNOWLEDGE.map((v) => `knowledge:${v.name}`),
      ...PLATFORM.map((v) => `platform:${v.name}`),
      "platform:passed on",
      ...ACTIONS.map((v) => `actions:${v.name}`),
      ...ACCESS.map((v) => `access:${v.name}`),
      ...SHAPE.map((v) => `shape:${v.name}`),
    ];
    assert.deepEqual([...hit].sort(), [...new Set(expected)].sort());
    for (const key of ["data", "knowledge", "actions", "access", "shape"]) {
      assert.ok([1, 2].some((n) => titled.has(`${key}:${n}`)), `${key} never quoted a title with the org's words`);
    }
  });

  test("the opposite error fails: a quoted title with the org's words served as wholly ours is caught", () => {
    const sc = scenario(1);
    const d = check(sc.input).digest!;
    const p = d.readiness!.pillars.find((x) => x.key === "data")!;
    assert.ok(sc.picked.data.t.org, "the scenario's data title holds an org word");
    assert.notDeepEqual(p.orgWords?.reason, ENGINE);
  });
});
