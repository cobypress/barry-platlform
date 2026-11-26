import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { checkedDigest, digestTextsAt, marksOf, type CheckedDigest } from "../checked.ts";
import { scoreDomain, scoreDomains, SHORT_REASON, type ScoreOptions } from "../../domains/scoring.ts";
import { SALES_CLOUD_MODULES } from "../../domains/index.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { scoreCaveatFor } from "../../narrative/edition-note.ts";
import { buildReport, type ReportInput } from "../../narrative/sections.ts";
import { loadRun } from "../../render/render-run.ts";
import { METADATA_POINTER, refusalSentence } from "../../salesforce/refusal-words.ts";
import { textSha256 } from "../../engine/org-words.ts";
import type { RunDigest } from "../../schemas/digest.ts";
import type { Calibration, Finding } from "../../schemas/index.ts";
import { AutomaticReplay, automaticSyntheticCalibration } from "../../testing/automatic.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadAnswers, syntheticProvider } from "../../testing/replay.ts";
import { Simulator, type SimulatorOptions } from "../../testing/simulator.ts";
import { AS_OF, CONNECTED_USER } from "../../testing/synthetic-org.ts";

// An area we couldn't score says why from its own checks' reasons, behind
// HC_AREA_REASONS (DECISIONS.md 769). Off, every area reads as before. On, the
// reason is the checks' own sentence, said once, or two short ones, or the first
// and a pointer to the rest; "too little" keeps its sentence and adds the reason
// most unmeasured checks give. The edition's reason leads; no reason is made up;
// the digest marks each part as its check's reason is marked, so an automatic
// run is released as before. Constructed cases reproduce a real run's three
// areas on the synthetic org: no real data.

const MADE_AT = "2026-11-26T09:00:00.000Z";
const ON: ScoreOptions = { areaReasons: true };
const NONE = "We couldn't measure this area.";
const TOO_LITTLE = "Too little of this area could be measured to score it fairly.";
const OTHERS = "We couldn't measure other checks in this area either, and they give their own reasons.";
const ANOTHER = "We couldn't measure another check in this area either, and it gives its own reason.";
const FORECAST_OFF = "Forecasting isn't in use, or can't be read, so we don't report a forecast figure. Salesforce holds no forecast rows for this period.";
const THIN = "9 closed deals with stage history is below our minimum of 20.";
const NO_LINE = "No open stage has 20 won deals to compare with.";
const UNSPLIT = "Salesforce's setting can't tell lost from no decision for: Closed Lost, Closed Lost - No Budget, Closed No Decision. If you tell us which each is, your next health check can measure this.";

const calibration = await automaticSyntheticCalibration();
const made = await runFromRecording({ calibration, reviewMode: "automatic", transport: new AutomaticReplay(), log: () => {} });
const loaded = loadRun(made.dir);
const extracts = loadExtracts(made.dir);
const template = loaded.input.findings.find((f) => f.status === "not_measurable")!;

let seq = 0;
/** A finding of the area's, with only what the score reads changed. */
function check(ruleId: string, status: Finding["status"], reason: string | null = null, domain = "historic_pipeline"): Finding {
  seq++;
  const { orgWords: _o, notMeasurableKind: _k, ...rest } = template;
  return { ...clone(rest), id: `f-${seq}`, ruleId, domain, status, notMeasurableReason: status === "not_measurable" ? reason : null };
}
const reasonOf = (findings: Finding[], options: ScoreOptions = ON, domain = "historic_pipeline"): string => {
  const s = scoreDomain(domain, findings, "test", options);
  assert.equal(s.status, "not_measurable");
  return s.status === "not_measurable" ? s.reason : "";
};

describe("an unscored area's reason from its own checks (DECISIONS.md 769)", () => {
  const cases: Array<[string, Finding[], string]> = [
    ["every check gives the same reason: said once", [check("a", "not_measurable", FORECAST_OFF), check("b", "not_measurable", FORECAST_OFF), check("c", "not_measurable", FORECAST_OFF)], FORECAST_OFF],
    ["two short reasons: the commonest first", [check("a", "not_measurable", NO_LINE), check("b", "not_measurable", THIN), check("c", "not_measurable", THIN), check("d", "not_measurable", THIN)], `${THIN} ${NO_LINE}`],
    ["two short reasons as often: the first check's first", [check("a", "not_measurable", NO_LINE), check("b", "not_measurable", THIN)], `${NO_LINE} ${THIN}`],
    ["two reasons, one long: the first and the other check pointed to", [check("a", "not_measurable", THIN), check("b", "not_measurable", THIN), check("c", "not_measurable", UNSPLIT)], `${THIN} ${ANOTHER}`],
    ["two reasons, one long, two other checks", [check("a", "not_measurable", UNSPLIT), check("b", "not_measurable", UNSPLIT), check("c", "not_measurable", UNSPLIT), check("d", "not_measurable", THIN), check("e", "not_measurable", THIN)], `${UNSPLIT} ${OTHERS}`],
    ["three reasons: the commonest and the rest pointed to", [check("a", "not_measurable", NO_LINE), check("b", "not_measurable", THIN), check("c", "not_measurable", THIN), check("d", "not_measurable", FORECAST_OFF)], `${THIN} ${OTHERS}`],
    ["too little measured, most unmeasured checks giving one reason: added", [check("a", "passed"), check("b", "not_measurable", UNSPLIT), check("c", "not_measurable", UNSPLIT), check("d", "not_measurable", THIN)], `${TOO_LITTLE} ${UNSPLIT}`],
    ["too little measured, no reason given by most: as before", [check("a", "passed"), check("b", "not_measurable", UNSPLIT), check("c", "not_measurable", THIN), check("d", "not_measurable", NO_LINE)], TOO_LITTLE],
    ["too little measured, half and half: as before", [check("a", "passed"), check("b", "issue"), check("c", "not_measurable", UNSPLIT), check("d", "not_measurable", UNSPLIT), check("e", "not_measurable", THIN), check("f", "not_measurable", THIN), check("g", "passed")], TOO_LITTLE],
    ["one check measured and nothing unmeasured: no reason made up", [check("a", "passed")], TOO_LITTLE],
    ["a check with several readings counts once", [check("a", "not_measurable", THIN), check("a", "not_measurable", THIN), check("b", "not_measurable", NO_LINE), check("c", "not_measurable", NO_LINE)], `${NO_LINE} ${THIN}`],
    ["one check giving two reasons, one long: no other check, so no pointer", [check("a", "not_measurable", UNSPLIT), check("a", "not_measurable", THIN)], UNSPLIT],
    ["one check giving three reasons and nothing else: the first alone", [check("a", "not_measurable", UNSPLIT), check("a", "not_measurable", THIN), check("a", "not_measurable", NO_LINE)], UNSPLIT],
    ["too little measured, an edition reason from one check and another from two: the one most give (review)", [check("x", "passed"), check("a", "not_measurable", METADATA_POINTER), check("b", "not_measurable", THIN), check("c", "not_measurable", THIN)], `${TOO_LITTLE} ${THIN}`],
    ["a check with a reading measured isn't unmeasured, so its reason isn't said", [check("a", "not_measurable", THIN), check("a", "issue"), check("b", "not_measurable", NO_LINE)], `${TOO_LITTLE} ${NO_LINE}`],
  ];
  for (const [name, findings, expected] of cases) {
    test(name, () => assert.equal(reasonOf(findings), expected));
  }

  test("with the switch off, every case reads exactly as before", () => {
    for (const [name, findings] of cases) {
      const before = reasonOf(findings, {});
      assert.ok(before === NONE || before === TOO_LITTLE, `${name}: ${before}`);
      assert.deepEqual(scoreDomain("historic_pipeline", findings, "test"), scoreDomain("historic_pipeline", findings, "test", { areaReasons: false }), name);
    }
  });

  test("the edition's reason leads in its own words, even when fewer checks give it (710)", () => {
    const findings = [check("a", "not_measurable", THIN), check("b", "not_measurable", THIN), check("c", "not_measurable", METADATA_POINTER)];
    assert.equal(reasonOf(findings), `${METADATA_POINTER} ${THIN}`);
    assert.equal(reasonOf([check("a", "not_measurable", METADATA_POINTER), check("b", "not_measurable", METADATA_POINTER)]), METADATA_POINTER);
    assert.equal(reasonOf([check("x", "passed"), check("a", "not_measurable", METADATA_POINTER), check("b", "not_measurable", METADATA_POINTER), check("c", "not_measurable", THIN)]), `${TOO_LITTLE} ${METADATA_POINTER}`);
  });

  test("where the area's own words already say why, they stand: not in use (627), no checks at all", () => {
    const off = [check("fct.forecasting-in-use", "not_measurable", "No revenue forecast type is active, so Forecasting isn't in use for revenue, and we don't report a forecast figure.", "forecast")];
    off[0]!.notMeasurableKind = "not_in_use";
    assert.equal(reasonOf(off, ON, "forecast"), "You don't use forecasting, so there is nothing to score.");
    assert.equal(reasonOf([], ON), "We couldn't assess this area.");
    const s = scoreDomain("forecast", off, "test", ON);
    assert.equal(s.status === "not_measurable" && s.notMeasurableKind, "not_in_use");
  });

  test("a scored area, and an unscored area's kind, are the same either way", () => {
    const scored = [check("a", "passed"), check("b", "issue"), check("c", "not_measurable", THIN)];
    assert.deepEqual(scoreDomain("historic_pipeline", scored, "test", ON), scoreDomain("historic_pipeline", scored, "test"));
    for (const [, findings] of cases) {
      const [on, off] = [scoreDomain("historic_pipeline", findings, "test", ON), scoreDomain("historic_pipeline", findings, "test")];
      assert.equal(on.status, off.status);
      assert.equal("notMeasurableKind" in on ? on.notMeasurableKind : undefined, "notMeasurableKind" in off ? off.notMeasurableKind : undefined);
    }
  });

  test("the short reasons are short, and the long one isn't", () => {
    for (const r of [THIN, NO_LINE]) assert.ok(r.length <= SHORT_REASON, r);
    assert.ok(UNSPLIT.length > SHORT_REASON);
  });
});

// The real run's areas, on the synthetic org's automatic run: 9 closed deals with stage history, forecasting with no rows.
function realCase(): Map<string, unknown> {
  const ex = clone(extracts) as Map<string, any>;
  const o = ex.get("opportunities");
  const closed = o.opportunities.data.filter((d: { IsClosed: boolean }) => d.IsClosed).map((d: { Id: string }) => d.Id).slice(0, 9);
  const keep = new Set([...o.opportunities.data.filter((d: { IsClosed: boolean }) => !d.IsClosed).map((d: { Id: string }) => d.Id), ...closed]);
  o.history.data = o.history.data.filter((h: { OpportunityId: string }) => keep.has(h.OpportunityId));
  ex.get("forecast").items = { ...ex.get("forecast").items, data: [] };
  return ex;
}
/** The automatic calibration with the open stage it doesn't know put in order, as a customer would answer it. */
const ordered: Calibration = { ...calibration, stages: [...calibration.stages.map((s) => (s.category === "open" && s.order !== undefined ? { ...s, order: s.order + (s.order >= 2 ? 1 : 0) } : s)), { name: "Qualified", category: "open", order: 2 }] };

type Case = { input: ReportInput; off: ReturnType<typeof scoreDomains>; on: ReturnType<typeof scoreDomains> };
function constructed(changed: Map<string, unknown>, cal: Calibration): Case {
  const a = analyseExtracts(changed, { calibration: cal });
  const input: ReportInput = { ...loaded.input, calibration: cal, findings: a.findings, metrics: a.metrics };
  return { input, off: scoreDomains(loaded.input.run.modules, a.findings, loaded.input.run.ruleSetVersion), on: scoreDomains(loaded.input.run.modules, a.findings, loaded.input.run.ruleSetVersion, ON) };
}
const checked = (c: Case, scores: Case["on"]): CheckedDigest => checkedDigest({ input: { ...c.input, scores }, pseudonyms: loaded.pseudonyms, inventory: null, madeAt: MADE_AT });
const unmarkedIn = (d: RunDigest | null): string[] => (d ? digestTextsAt(d).filter((t) => !marksOf(d, t)).map((t) => `${t.path}: ${t.text}`) : []);
function released(c: CheckedDigest, what: string): void {
  assert.deepEqual({ held: c.held, unmarked: c.unmarked, unmatched: c.unmatched, masks: c.masks }, { held: null, unmarked: 0, unmatched: 0, masks: 0 }, `${what}\n${unmarkedIn(c.digest).join("\n")}`);
}
const area = (d: RunDigest, domain: string) => d.areas.find((a) => a.domain === domain)!;
const spansOf = (d: RunDigest, domain: string): string[] => {
  const a = area(d, domain);
  assert.equal(a.status, "not_measurable");
  const marks = a.orgWords && "reason" in a.orgWords ? a.orgWords.reason : undefined;
  assert.ok(marks, `${domain} is marked`);
  return a.status === "not_measurable" ? marks.orgWords.map((s) => a.reason.slice(s.start, s.end)) : [];
};

describe("the real run's three areas, constructed on the synthetic org's automatic run (DECISIONS.md 769)", () => {
  const real = constructed(realCase(), ordered);
  const thin = constructed(extracts, { ...calibration, minSample: 100_000 });
  const plain = constructed(extracts, calibration);

  test("forecasting with no rows and too few closed deals with stage history: each area says its checks' reason", () => {
    const by = (s: Case["on"], d: string) => s.find((x) => x.domain === d)!;
    assert.equal(by(real.off, "forecast").status === "not_measurable" && (by(real.off, "forecast") as { reason: string }).reason, NONE);
    assert.equal((by(real.on, "forecast") as { reason: string }).reason, FORECAST_OFF);
    assert.equal((by(real.off, "historic_pipeline") as { reason: string }).reason, NONE);
    assert.equal((by(real.on, "historic_pipeline") as { reason: string }).reason, `${THIN} ${ANOTHER}`);
  });

  test("activity with too little measured and most of the rest waiting on lost and no decision: the reason added, its stage names marked", () => {
    const off = thin.off.find((s) => s.domain === "activity")!;
    const on = thin.on.find((s) => s.domain === "activity")!;
    assert.equal(off.status === "not_measurable" && off.reason, TOO_LITTLE);
    assert.equal(on.status === "not_measurable" && on.reason, `${TOO_LITTLE} ${UNSPLIT}`);
    const c = checked(thin, thin.on);
    released(c, "thin");
    assert.deepEqual(spansOf(c.digest!, "activity"), ["Closed Lost", "Closed Lost - No Budget", "Closed No Decision"]);
  });

  test("released as before, switch on and off: nothing held, unmarked, untraced or masked; the score's caveat unchanged", () => {
    for (const [name, c] of [["real", real], ["thin", thin], ["plain", plain]] as const) {
      const on = checked(c, c.on);
      const off = checked(c, c.off);
      released(on, `${name} on`);
      released(off, `${name} off`);
      assert.equal((on.digest!.overall as { caveat?: string }).caveat, (off.digest!.overall as { caveat?: string }).caveat, name);
      assert.equal(scoreCaveatFor(buildReport({ ...c.input, scores: c.on })), scoreCaveatFor(buildReport({ ...c.input, scores: c.off })), name);
      // Only the unscored areas' reasons and their marks differ.
      const strip = (d: RunDigest) => ({ ...d, areas: d.areas.map((a) => (a.status === "not_measurable" ? { ...a, reason: "", orgWords: undefined } : a)) });
      assert.deepEqual(strip(on.digest!), strip(off.digest!), name);
    }
  });

  test("a figure an area restates traces as its check's does: the run's own figures, no mask", () => {
    const d = checked(real, real.on).digest!;
    const historic = area(d, "historic_pipeline");
    assert.equal(historic.status === "not_measurable" && historic.reason, `${THIN} ${ANOTHER}`);
    assert.deepEqual(spansOf(d, "historic_pipeline"), []);
    assert.ok(d.findings.some((f) => f.notMeasurableReason === THIN), "the check says the same figures");
  });

  test("the synthetic org's own unscored area, on and off", () => {
    const off = checked(plain, plain.off).digest!;
    const on = checked(plain, plain.on).digest!;
    const before = area(off, "historic_pipeline");
    const after = area(on, "historic_pipeline");
    assert.equal(before.status === "not_measurable" && before.reason, TOO_LITTLE);
    assert.equal(after.status === "not_measurable" && after.reason, `${TOO_LITTLE} You haven't told us where these open stages come in your sales order: Qualified.`);
    assert.deepEqual(spansOf(on, "historic_pipeline"), ["Qualified"]);
  });

  test("an area reason the digest can't match to its checks is never served as marked", () => {
    const c = checked(thin, thin.on.map((s) => (s.domain === "activity" && s.status === "not_measurable" ? { ...s, reason: `${TOO_LITTLE} ${UNSPLIT.replace("Closed Lost,", "Closed Lost and")}` } : s)));
    assert.notEqual(c.held, null);
    assert.ok(c.unmarked > 0);
  });
});

describe("every unscored area on an automatic run, with the switch on (DECISIONS.md 769)", () => {
  const REASONS = ["The org reported no limits.", refusalSentence("Account", "no_permission")];
  const unreadable = (value: unknown, reason: string): unknown => {
    const v = value as Record<string, unknown>;
    const source = (v && "source" in v ? v.source : { kind: "soql", api: "rest", query: "x" }) as unknown;
    if ("status" in v) return { status: "unavailable", reason, source };
    const out = clone(v);
    for (const [k, part] of Object.entries(out)) if (part && typeof part === "object" && "status" in part) out[k] = { status: "unavailable", reason, source: (part as { source?: unknown }).source ?? source };
    return out;
  };
  const ids = [...extracts.keys()];
  test("each read unreadable in turn, and every read at once: released, the areas' reasons marked", () => {
    let changedAreas = 0;
    const runs: Array<[string, Map<string, unknown>]> = [
      ...ids.map((id, i): [string, Map<string, unknown>] => {
        const changed = clone(extracts);
        changed.set(id, unreadable(extracts.get(id), REASONS[i % REASONS.length]!));
        return [id, changed];
      }),
      ...REASONS.map((r): [string, Map<string, unknown>] => [`all, ${r}`, new Map([...extracts].map(([id, v]) => [id, unreadable(v, r)]))]),
    ];
    for (const [name, changed] of runs) {
      const c = constructed(changed, calibration);
      released(checked(c, c.on), name);
      changedAreas += c.on.filter((s, i) => s.status === "not_measurable" && c.off[i]!.status === "not_measurable" && s.reason !== (c.off[i] as { reason: string }).reason).length;
    }
    assert.ok(changedAreas > ids.length / 4, `${changedAreas} areas reworded`);
  });

  test("each read empty in turn: released", () => {
    for (const [id, value] of extracts) {
      const v = value as Record<string, unknown>;
      if (!v || typeof v !== "object" || !("status" in v) || v.status !== "ok" || !Array.isArray(v.data)) continue;
      const changed = clone(extracts);
      changed.set(id, { ...v, data: [] });
      const c = constructed(changed, calibration);
      released(checked(c, c.on), id);
    }
  });
});

describe("the switch through a run, and dormant (DECISIONS.md 769)", () => {
  test("off or absent: the run's files are as before; on: only the unscored areas' reasons change", async () => {
    const { fingerprint } = await import("../../testing/fingerprint.ts");
    const { readFileSync } = await import("node:fs");
    const { join } = await import("node:path");
    const run = async (areaReasons?: boolean) => (await runFromRecording({ calibration, reviewMode: "automatic", transport: new AutomaticReplay(), log: () => {}, ...(areaReasons === undefined ? {} : { areaReasons }) })).dir;
    const [absent, off, on] = [await run(), await run(false), await run(true)];
    assert.deepEqual(fingerprint(off), fingerprint(absent));
    const scores = (dir: string) => JSON.parse(readFileSync(join(dir, "scores.json"), "utf8")) as Array<{ domain: string; reason?: string }>;
    const changed = scores(on).filter((s, i) => s.reason !== scores(absent)[i]!.reason);
    assert.deepEqual(changed.map((s) => [s.domain, s.reason]), [["historic_pipeline", `${TOO_LITTLE} You haven't told us where these open stages come in your sales order: Qualified.`]]);
    assert.ok(readFileSync(join(on, "raw-notes.md"), "utf8").includes("Not measurable. Too little of this area could be measured to score it fairly. You haven't told us"));
    assert.deepEqual(JSON.parse(readFileSync(join(on, "findings.json"), "utf8")), JSON.parse(readFileSync(join(absent, "findings.json"), "utf8")));
  });

  test("a consultant-led run and the simulator's connections: off as absent, on released", async () => {
    const consultant = async (areaReasons?: boolean) => (await runFromRecording({ answers: loadAnswers(), log: () => {}, ...(areaReasons === undefined ? {} : { areaReasons }) })).dir;
    const { fingerprint } = await import("../../testing/fingerprint.ts");
    assert.deepEqual(fingerprint(await consultant(false)), fingerprint(await consultant()));
    const VARIANTS: SimulatorOptions[] = [{ readOnly: true }, { organizationType: "Professional Edition" }, { readOnly: true, organizationType: "Professional Edition" }, { cpq: false, pardot: false }];
    for (const options of VARIANTS) {
      const go = async (areaReasons: boolean, product?: "pulse") =>
        loadRun((await runHealthCheck({ provider: syntheticProvider, calibration, modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport: new Simulator(options), connectedUser: CONNECTED_USER, log: () => {}, reviewMode: "automatic", areaReasons, ...(product ? { product } : {}) })).dir);
      const on = await go(true);
      released(checkedDigest({ input: on.input, pseudonyms: on.pseudonyms, inventory: null, madeAt: MADE_AT }), JSON.stringify(options));
      const off = await go(false);
      assert.deepEqual(on.input.findings, off.input.findings);
      assert.deepEqual((on.input.scores ?? []).map((s) => s.status), (off.input.scores ?? []).map((s) => s.status));
    }
  });
});

describe("a check's reason the name or id guard would change, and checks marking one sentence differently (review of 769)", () => {
  const base = clone(extracts) as Map<string, any>;
  base.get("forecast").items = { ...base.get("forecast").items, data: [] };
  const a = analyseExtracts(base, { calibration });
  /** Every unmeasured forecast check giving this reason, with these words marked as the org's. */
  const withReason = (text: string, marked: (i: number) => string[]): Finding[] => {
    let k = 0;
    return clone(a.findings).map((f) => {
      if (f.domain !== "forecast" || f.status !== "not_measurable") return f;
      const spans = marked(k++).map((w) => ({ start: text.indexOf(w), end: text.indexOf(w) + w.length }));
      return { ...f, notMeasurableReason: text, orgWords: { ...f.orgWords, notMeasurableReason: { spans, textSha256: textSha256(text) } } };
    });
  };
  const digestOf = (findings: Finding[]) => {
    const scores = scoreDomains(loaded.input.run.modules, findings, loaded.input.run.ruleSetVersion, ON);
    return { scores, c: checkedDigest({ input: { ...loaded.input, calibration, findings, metrics: a.metrics, scores }, pseudonyms: loaded.pseudonyms, inventory: null, madeAt: MADE_AT }) };
  };
  for (const [what, name] of [["a person's name", "Priya Shah"], ["a Salesforce id", "00O000000000001AAA"]] as const) {
    test(`${what} in the checks' reason: the area says what it said before, ours, and the run is released`, async () => {
      const text = `The forecast report ${name} couldn't be read.`;
      const findings = withReason(text, () => [name]);
      const { scores, c } = digestOf(findings);
      assert.equal((scores.find((s) => s.domain === "forecast") as { reason: string }).reason, text, "the score keeps the checks' reason");
      released(c, what);
      const f = area(c.digest!, "forecast");
      assert.equal(f.status === "not_measurable" && f.reason, NONE);
      assert.deepEqual(spansOf(c.digest!, "forecast"), []);
      // The documents print the same sentence in its place.
      const { withWholeNotes, listedReason } = await import("../../render/area-notes.ts");
      const { scorecard } = await import("../../narrative/select.ts");
      const report = { input: { ...loaded.input, run: { ...loaded.input.run, areaReasons: true as const }, findings, scores } };
      assert.equal(listedReason(report, "forecast", text, loaded.pseudonyms), NONE);
      const row = withWholeNotes(report, scorecard(findings, scores), loaded.pseudonyms).find((r) => r.domain === "forecast")!;
      assert.ok(row.wholeNotes && !row.wholeNotes.some((n) => n.includes(name)), JSON.stringify(row.wholeNotes));
    });
  }

  test("a name the guard lets through is said, marked as the org's", () => {
    const text = "The forecast report Pipeline Review couldn't be read.";
    const { c } = digestOf(withReason(text, () => ["Pipeline Review"]));
    released(c, "plain");
    assert.deepEqual(spansOf(c.digest!, "forecast"), ["Pipeline Review"]);
  });

  test("checks marking the same sentence differently: the area marks every word any of them marks", () => {
    const text = "The forecast report Pipeline Review couldn't be read.";
    const { c } = digestOf(withReason(text, (i) => (i % 2 ? ["Pipeline"] : ["Review"])));
    assert.deepEqual(spansOf(c.digest!, "forecast"), ["Pipeline", "Review"]);
    const { c: touching } = digestOf(withReason(text, (i) => (i % 2 ? ["Pipeline"] : [" Review"])));
    assert.deepEqual(spansOf(touching.digest!, "forecast"), ["Pipeline Review"]);
  });
});
