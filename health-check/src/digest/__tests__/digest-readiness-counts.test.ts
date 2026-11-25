import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { buildDigest, guardDigest, READINESS_CODE_PATHS, STRUCTURAL_KEYS } from "../build.ts";
import { checkedDigest } from "../checked.ts";
import { AI_ACTIONS, AI_LEGACY_SHARE, AI_PLATFORM } from "../../domains/configuration/ai.ts";
import { SALES_CLOUD_MODULES } from "../../domains/index.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { PILLARS, type PillarKey } from "../../narrative/readiness.ts";
import { buildReport, type BuiltReport, type ReportInput } from "../../narrative/sections.ts";
import { Pseudonymiser } from "../../privacy/pseudonymiser.ts";
import { loadRun } from "../../render/render-run.ts";
import { HELD_FOR_US_MASKS } from "../../render/render-released.ts";
import { MASK } from "../../render/repair.ts";
import { NAMED_PEOPLE_SENTENCE, type RunDigest } from "../../schemas/digest.ts";
import type { Metric } from "../../schemas/index.ts";
import { tempDir } from "../../testing/harness.ts";
import { syntheticProvider } from "../../testing/replay.ts";
import { Simulator, type SimulatorOptions } from "../../testing/simulator.ts";
import { AS_OF, CONNECTED_USER } from "../../testing/synthetic-org.ts";
import { assertSameAsWithout, assertWellFormed, automatic, base, calibration, check, ENGINE, issue, MADE_AT, notMeasurable, scenario, SCENARIOS, stripped, withoutReadiness, type Title } from "./readiness-scenarios.ts";

// AI readiness never moves a count, nor a hold (DECISIONS.md 742, fix round).
// The block restates findings and metrics the counts already read, so a run
// with it is released or held exactly as without it, with the same masks,
// unmatched and unmarked, on every input: a passed-on reason is a copy of a
// metric's reason, and counting each copy of a withheld one again could hold a
// run (the reviewer's case: 9 masks released without readiness, 13 and held
// with it). And a title the guard took out of the digest never comes back in a
// reason, whatever its case.

describe("a run with readiness is released or held exactly as without it (DECISIONS.md 742)", () => {
  const same = (input: ReportInput, what: string): ReturnType<typeof check> => {
    const c = check(input);
    assertSameAsWithout(c, withoutReadiness(input), what);
    if (c.digest?.readiness) assertWellFormed(c.digest, buildReport(input));
    return c;
  };

  test("on every generated scenario", () => {
    for (let s = 0; s < SCENARIOS; s++) same(scenario(s).input, `scenario ${s}: ${scenario(s).name}`);
  });

  test("the reviewer's ceiling case: three AI metrics' reasons unmarked and six other masks stay released, with readiness's copies withheld and not counted", () => {
    let extra = 0;
    const metrics = base.metrics.map((m): Metric => {
      if (m.id === AI_PLATFORM || m.id === AI_ACTIONS || m.id === AI_LEGACY_SHARE) return notMeasurable(m.id, m.id === AI_PLATFORM ? "The org's settings" : "Metadata", false);
      if (extra < 6 && m.status === "measured" && m.assumptions.length && m.orgWords?.assumptions) {
        extra++;
        const marks = structuredClone(m.orgWords);
        marks.assumptions![0] = null;
        return { ...m, orgWords: marks };
      }
      return m;
    });
    const c = same({ ...base, metrics }, "ceiling");
    assert.ok(c.masks <= HELD_FOR_US_MASKS && c.masks + c.readinessWithheld > HELD_FOR_US_MASKS, `${c.masks} masks, ${c.readinessWithheld} withheld in readiness`);
    assert.equal(c.held, null);
    // Knowledge, platform, actions and shape: each a copy of a withheld reason, each withheld.
    assert.deepEqual(c.digest!.readiness!.pillars.filter((p) => p.reason === MASK).map((p) => p.key), ["knowledge", "platform", "actions", "shape"]);
  });

  test("every pillar withheld at once", () => {
    const unmarked: Title = { text: "Zephyr_Batch__c jobs stop part way", org: "Zephyr_Batch__c", unmarked: true };
    const findings = [...stripped(), issue("dq.zephyr-constructed", "data_quality", "high", unmarked), issue("sec.zephyr-constructed", "security", "high", unmarked)];
    const metrics = base.metrics.map((m) => (m.id === AI_PLATFORM || m.id === AI_ACTIONS || m.id === AI_LEGACY_SHARE ? notMeasurable(m.id, "Metadata", false) : m));
    const c = same({ ...base, findings, metrics }, "all withheld");
    assert.equal(c.readinessWithheld, 6);
    assert.ok(c.digest!.readiness!.pillars.every((p) => p.reason === MASK && p.figures === undefined));
  });

  test("a title that grows when lower-cased is withheld, and a title holding the mask's own words is counted once, as without readiness", () => {
    const dotted: Title = { text: "İnvoice_Sync__c has blank owners", org: "İnvoice_Sync__c" };
    const masked: Title = { text: "Zephyr (detail withheld) is blank", org: "Zephyr" };
    const a = same({ ...base, findings: [...stripped(), issue("dq.zephyr-constructed", "data_quality", "high", dotted)] }, "dotted I");
    assert.equal(a.digest!.readiness!.pillars[0]!.reason, MASK);
    const b = same({ ...base, findings: [...stripped(), issue("dq.zephyr-constructed", "data_quality", "high", masked)] }, "mask words");
    assert.equal(b.readinessWithheld, 0);
    assert.match(b.digest!.readiness!.pillars[0]!.reason, /: zephyr \(detail withheld\) is blank\.$/);
  });

  // The engine's own marking path, not hand-made marks: a read-only connection and a Professional Edition org pass real refusals on.
  const runs: Array<[string, SimulatorOptions, Record<string, unknown>]> = [
    ["read-only", { readOnly: true }, {}],
    ["Professional Edition", { organizationType: "Professional Edition" }, {}],
    ["both", { readOnly: true, organizationType: "Professional Edition" }, {}],
    ["a set-up review", {}, { scope: "essentials" }],
  ];
  for (const [name, sim, options] of runs) {
    test(`an automatic run on the simulator, ${name}`, async () => {
      const made = await runHealthCheck({ provider: syntheticProvider, calibration, modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport: new Simulator(sim), connectedUser: CONNECTED_USER, log: () => {}, reviewMode: "automatic", ...options });
      const { input, pseudonyms } = loadRun(made.dir);
      const c = checkedDigest({ input, pseudonyms, inventory: null, madeAt: MADE_AT });
      const w = checkedDigest({ input, pseudonyms, inventory: null, madeAt: MADE_AT, build: (i) => ({ ...buildReport(i), readiness: undefined }) as unknown as BuiltReport });
      assertSameAsWithout(c, w, name);
      const report = buildReport(input);
      assertWellFormed(c.digest!, report);
      // A passed-on reason carries its metric's marks as the run made them, or is withheld as the metric's reason is.
      const d = c.digest!;
      report.readiness.pillars.forEach((made, i) => {
        if (!made.passedOn) return;
        const p = d.readiness!.pillars[i]!;
        const m = d.metrics.find((x) => x.id === made.passedOn);
        assert.equal(m?.status, "not_measurable", made.key);
        if (m?.status !== "not_measurable") return;
        if (m.reason === MASK) assert.equal(p.reason, MASK, made.key);
        else assert.deepEqual([p.reason, p.orgWords?.reason], [m.reason, m.orgWords?.reason], made.key);
      });
      if (name === "read-only") assert.ok(report.readiness.pillars.some((p) => p.passedOn), "a read-only connection passes a refusal on");
    });
  }
});

describe("a title the guard took out never comes back in a reason (DECISIONS.md 742, fix round)", () => {
  const PEOPLE = [["Mark", "Okafor"], ["Will", "Ndlovu"], ["Grant", "Mensah"], ["Rose", "Tanaka"], ["Bill", "Adeyemi"]] as const;
  const people = new Pseudonymiser(structuredClone(automatic.pseudonyms.entries()));
  PEOPLE.forEach(([first, last], i) => people.label("user", `0050000000000009${String(i).padStart(2, "0")}`, `${first} ${last}`));
  const titles = PEOPLE.flatMap(([first, last]) => [`${first} approvals flow fails without telling anyone`, `Approvals owned by ${first} fail without telling anyone`, `${first} ${last} approvals flow fails without telling anyone`].map((text) => ({ first, text })));
  let replacedTitles = 0;

  for (const { first, text } of titles) {
    test(`"${text}" in data, access and knowledge, in a checked and an unchecked digest`, () => {
      const t: Title = { text, org: null };
      const findings = [...stripped(), issue("dq.zephyr-constructed", "data_quality", "high", t), issue("sec.zephyr-constructed", "security", "medium", t), issue("svc.knowledge-current", "service_setup", "medium", t)];
      const input = { ...base, findings };
      const c = checkedDigest({ input, pseudonyms: people, inventory: null, madeAt: MADE_AT });
      const w = checkedDigest({ input, pseudonyms: people, inventory: null, madeAt: MADE_AT, build: (i) => ({ ...buildReport(i), readiness: undefined }) as unknown as BuiltReport });
      assertSameAsWithout(c, w, text);
      // As a pulse's and a consultant-led check's digests are made: built, never checked.
      const plain = buildDigest({ report: buildReport(input), pseudonyms: people, inventory: null, madeAt: MADE_AT });
      for (const d of [c.digest!, plain] as RunDigest[]) {
        const served = d.findings.find((f) => f.title === text || findings.some((x) => x.id === f.id && x.title === text))!;
        const away = served.title !== text;
        if (away && d === plain) replacedTitles++;
        for (const p of d.readiness!.pillars.filter((x) => ["data", "access", "knowledge"].includes(x.key))) {
          const has = p.reason.toLowerCase().includes(first.toLowerCase());
          if (away) {
            assert.equal(has, false, `${p.key}: ${p.reason}`);
            assert.deepEqual([p.reason, p.orgWords?.reason, p.figures], [NAMED_PEOPLE_SENTENCE, ENGINE, undefined], p.key);
          } else assert.ok(has || p.reason === MASK, `${p.key}: quotes the title the digest serves: ${p.reason}`);
        }
      }
    });
  }

  test("the guard took some of these titles out, so the tests above weren't vacuous", () => assert.ok(replacedTitles >= 5, `${replacedTitles}`));

  test("an opening, label or question the guard changes is the fixed sentence, marked ours, and moves no count", () => {
    const guarded = new Pseudonymiser(structuredClone(automatic.pseudonyms.entries()));
    guarded.label("user", "005000000000000990", "Data Access");
    const c = checkedDigest({ input: base, pseudonyms: guarded, inventory: null, madeAt: MADE_AT });
    const w = checkedDigest({ input: base, pseudonyms: guarded, inventory: null, madeAt: MADE_AT, build: (i) => ({ ...buildReport(i), readiness: undefined }) as unknown as BuiltReport });
    assertSameAsWithout(c, w, "Data Access");
    const r = c.digest!.readiness!;
    assert.deepEqual([r.opening, r.orgWords?.opening], [NAMED_PEOPLE_SENTENCE, ENGINE]);
    const changed = r.pillars.filter((p) => p.label !== PILLARS[p.key as PillarKey].label);
    assert.ok(changed.length > 0, "a label was guarded");
    for (const p of changed) assert.deepEqual([p.label, p.orgWords?.label], [NAMED_PEOPLE_SENTENCE, ENGINE], p.key);
    assert.equal(c.readinessWithheld, 0);
  });
});

describe("readiness's codes are skipped by the guard by path, never by key name (DECISIONS.md 742, fix round)", () => {
  test("a free text under a key named as one of them elsewhere still goes through the guard", () => {
    for (const key of ["verdict", "stat", "metricId", "metricIds", "fixIds", "overall"]) assert.equal(STRUCTURAL_KEYS.has(key), false, key);
    const ps = new Pseudonymiser([]);
    ps.label("user", "005000000000000991", "Mark Okafor");
    const named = "Mark Okafor holds these";
    const out = guardDigest({ verdict: named, stat: named, notes: [{ fixIds: [named] }], readiness: { pillars: [{ verdict: "not_yet", reason: named, figures: [{ stat: "articles", metricId: "ai.platform" }] }], fixIds: ["f-1"] } }, ps);
    assert.deepEqual(out, { verdict: NAMED_PEOPLE_SENTENCE, stat: NAMED_PEOPLE_SENTENCE, notes: [{ fixIds: [NAMED_PEOPLE_SENTENCE] }], readiness: { pillars: [{ verdict: "not_yet", reason: NAMED_PEOPLE_SENTENCE, figures: [{ stat: "articles", metricId: "ai.platform" }] }], fixIds: ["f-1"] } });
    assert.ok([...READINESS_CODE_PATHS].every((p) => p.startsWith("readiness.")));
  });
});
