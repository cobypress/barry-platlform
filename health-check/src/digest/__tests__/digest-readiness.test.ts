import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { buildDigest } from "../build.ts";
import { checkedDigest, digestTextsAt, marksOf, type CheckedDigest } from "../checked.ts";
import { AI_ACTIONS, AI_LEGACY_SHARE, AI_PLATFORM } from "../../domains/configuration/ai.ts";
import { SALES_CLOUD_MODULES } from "../../domains/index.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { textSha256 } from "../../engine/org-words.ts";
import { PILLARS, VERDICT_WORDS, type PillarKey, type ReadinessVerdict } from "../../narrative/readiness.ts";
import { buildReport, readinessOpening, type BuiltReport, type ReportInput } from "../../narrative/sections.ts";
import { extractFigures } from "../../numbercheck/extract.ts";
import { loadRun } from "../../render/render-run.ts";
import { MASK } from "../../render/repair.ts";
import { DIGEST_KEYS, RunDigest } from "../../schemas/digest.ts";
import type { Finding, Metric } from "../../schemas/index.ts";
import { AutomaticReplay, automaticSyntheticCalibration } from "../../testing/automatic.ts";
import { runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { AS_OF, CONNECTED_USER } from "../../testing/synthetic-org.ts";

// AI readiness in the digest (KAN-50, DECISIONS.md 742): the six pillars the
// full report's slide shows, for the portal and the connector, every text
// marked and every number traced. The lesson of decision 739 is that the
// synthetic org reaches few of the code's paths, so every branch of
// readinessOf that writes a reason is built here from constructed inputs, on an
// automatic run, and each digest block it gives is held to its marks and figures.

const MADE_AT = "2026-11-25T09:00:00.000Z";
const consultant = loadRun((await runFromRecording({ answers: loadAnswers() })).dir);
const consultantReport = buildReport(consultant.input);
const consultantDigest = buildDigest({ report: consultantReport, pseudonyms: consultant.pseudonyms, inventory: null, madeAt: MADE_AT });

const calibration = await automaticSyntheticCalibration();
const automatic = loadRun((await runFromRecording({ calibration, reviewMode: "automatic", transport: new AutomaticReplay() })).dir);
const check = (input: ReportInput, build?: (i: ReportInput) => BuiltReport): CheckedDigest => checkedDigest({ input, pseudonyms: automatic.pseudonyms, inventory: null, madeAt: MADE_AT, ...(build ? { build } : {}) });

const ENGINE = { orgWordsMarked: true, orgWords: [] };
const readinessTexts = (d: RunDigest) => digestTextsAt(d).filter((t) => t.path.startsWith("readiness."));

/** Every key in a value, at any depth, but the finding ids that key the plan's alsoFor. */
const keysOf = (value: unknown): string[] =>
  Array.isArray(value) ? value.flatMap(keysOf) : value && typeof value === "object" ? Object.entries(value).flatMap(([k, v]) => [k, ...(k === "alsoFor" ? [] : keysOf(v))]) : [];

/** The invariants every digest's readiness holds: ids among the findings, labels from the table, numbers in figures. */
function assertWellFormed(d: RunDigest, report: BuiltReport): void {
  const r = d.readiness!;
  const ids = new Set(d.findings.map((f) => f.id));
  for (const p of r.pillars) for (const id of p.findingIds) assert.ok(ids.has(id), `${p.key}: ${id} is not among the digest's findings`);
  for (const id of r.fixIds) assert.ok(ids.has(id), `fix ${id} is not among the digest's findings`);
  assert.ok(r.fixIds.length <= 5);
  assert.deepEqual(r.fixIds, report.readiness.fixes.map((f) => f.id));
  assert.deepEqual(r.pillars.map((p) => [p.key, p.label, p.question]), (Object.keys(PILLARS) as PillarKey[]).map((k) => [k, PILLARS[k].label, PILLARS[k].question]));
  for (const k of keysOf(d)) assert.ok(DIGEST_KEYS.has(k), `${k} isn't in DIGEST_KEYS`);
  r.pillars.forEach((p, i) => {
    const made = report.readiness.pillars[i]!;
    // Every number the engine put in, in digits, outside a quoted title, is a figure, in order; and each figure is its metric's own.
    // ("too few have one" is a word, not a count the engine put in.)
    const own = made.quoted ? `${p.reason.slice(0, made.quoted.at)}.` : p.reason;
    const numbers = made.passedOn ? [] : extractFigures(own).figures.filter((f) => f.value !== undefined && /\d/.test(f.raw)).map((f) => f.value);
    assert.deepEqual(numbers, (p.figures ?? []).map((f) => f.value), `${p.key}: ${p.reason}`);
    for (const f of p.figures ?? []) {
      assert.ok(p.metricIds.includes(f.metricId), `${p.key}: ${f.metricId}`);
      const m = report.metrics.find((x) => x.id === f.metricId);
      assert.equal(m?.status, "measured");
      if (m?.status !== "measured") continue;
      const held = f.stat === "value" ? m.value : f.stat === "workingLine:ready" ? m.segments.find((s) => s.dimension === "working line" && s.key === "Ready up to")?.value : m.stats[f.stat];
      assert.equal(held, f.value, `${p.key}: ${f.stat}`);
    }
  });
}

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

  test("an automatic run on it is still released, with nothing unmatched or unmarked, and masks no more than without readiness", () => {
    const c = check(automatic.input);
    const without = check(automatic.input, (i) => {
      const { readiness: _r, ...rest } = buildReport(i);
      return rest as BuiltReport;
    });
    assert.equal(without.digest!.readiness, undefined);
    assert.deepEqual({ unmatched: c.unmatched, unmarked: c.unmarked, held: c.held }, { unmatched: 0, unmarked: 0, held: null });
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

// ---- Every branch of readinessOf that writes a reason, on an automatic run ----

/** A title, and the org's words in it. */
type Title = { text: string; org: string | null };
const TITLES: Title[] = [
  { text: "Some flows have no description an agent can read", org: null },
  { text: "Flows on Zephyr_Invoice__c have no description", org: "Zephyr_Invoice__c" },
  { text: "Zephyr sync jobs fail without telling anyone", org: "Zephyr sync" },
  { text: "Salesforce Knowledge articles are out of date", org: null },
  { text: "12 flows have no fault path", org: null },
];
const titleAt = (n: number): Title => TITLES[n % TITLES.length]!;

/** An org word passed on in a metric's not measurable reason, as a refused read names its object. */
const PASSED_ORG = "Zephyr_Setting__mdt";
const passedReason = (head: string): { reason: string; spans: Array<{ start: number; end: number }> } => {
  const reason = `${head} couldn't be read: we can't read ${PASSED_ORG} with this connection.`;
  const start = reason.indexOf(PASSED_ORG);
  return { reason, spans: [{ start, end: start + PASSED_ORG.length }] };
};

const base = automatic.input;
const like = base.findings.find((f) => f.status === "issue" && f.orgWords?.title && f.value.kind === "not_sized") ?? base.findings.find((f) => f.status === "issue" && f.orgWords?.title)!;
let made = 0;
/** A constructed issue, its title marked where the org's words are. */
function issue(ruleId: string, domain: string, severity: Finding["severity"], t: Title): Finding {
  const start = t.org ? t.text.indexOf(t.org) : -1;
  const spans = t.org ? [{ start, end: start + t.org.length }] : [];
  return { ...structuredClone(like), id: `f-00000000r${String(++made).padStart(3, "0")}`, ruleId, domain: domain as Finding["domain"], severity, status: "issue", title: t.text, orgWords: { ...like.orgWords, title: { spans, textSha256: textSha256(t.text) } } };
}

const measured = (id: string): Extract<Metric, { status: "measured" }> => structuredClone(base.metrics.find((m) => m.id === id)) as Extract<Metric, { status: "measured" }>;
function notMeasurable(id: string, head: string): Metric {
  const { value: _v, n: _n, dateRange: _d, segments: _s, segmentExemptReason: _e, series: _se, stats: _st, ...rest } = measured(id);
  const { reason, spans } = passedReason(head);
  return { ...rest, status: "not_measurable", reason, orgWords: { ...rest.orgWords, reason: { spans, textSha256: textSha256(reason) } } } as Metric;
}

/** One variant of one pillar: what it changes, what verdict it gives, and how its reason reads. */
type Variant = { name: string; verdict: ReadinessVerdict; reason: RegExp; findings?: (t: Title) => Finding[]; quoted?: boolean; passed?: boolean };
type Stats = Record<string, number>;

const severityVariants = (domain: string, rule: string, noun: RegExp): Variant[] => [
  { name: "not measured", verdict: "not_measurable", reason: new RegExp(`^None of the checks on ${noun.source} could be measured\\.$`), findings: () => [] },
  { name: "one serious", verdict: "not_yet", reason: new RegExp(`has a critical or high issue: `), findings: (t) => [issue(rule, domain, "high", t), issue(rule, domain, "low", TITLES[0]!)], quoted: true },
  { name: "several serious", verdict: "not_yet", reason: /has critical or high issues, such as: /, findings: (t) => [issue(rule, domain, "critical", t), issue(rule, domain, "high", TITLES[0]!)], quoted: true },
  { name: "one medium", verdict: "nearly", reason: /has nothing critical or high, but a medium issue: /, findings: (t) => [issue(rule, domain, "medium", t)], quoted: true },
  { name: "several medium", verdict: "nearly", reason: /has nothing critical or high, but medium issues, such as: /, findings: (t) => [issue(rule, domain, "medium", t), issue(rule, domain, "medium", TITLES[0]!), issue(rule, domain, "low", TITLES[0]!)], quoted: true },
  { name: "ready", verdict: "ready", reason: /has nothing above low severity\.$/, findings: () => [issue(rule, domain, "low", TITLES[0]!)] },
];

const DATA = severityVariants("data_quality", "dq.zephyr-constructed", /data quality/);
const ACCESS = severityVariants("security", "sec.zephyr-constructed", /security and integration access/);

// Knowledge and platform read one metric: each variant gives its part of the stats, or the metric not measured.
type MetricVariant = Variant & { stats?: Stats; value?: number; off?: boolean };
const KNOWLEDGE: MetricVariant[] = [
  { name: "passed on", verdict: "not_measurable", reason: /couldn't be read: we can't read Zephyr_Setting__mdt/, off: true, passed: true },
  { name: "unknown", verdict: "not_measurable", reason: /^Whether Salesforce Knowledge is on couldn't be read\.$/, stats: {} },
  { name: "off", verdict: "not_yet", reason: /^Salesforce Knowledge isn't switched on, so an agent has no articles/, stats: { knowledge: 0 } },
  { name: "uncounted", verdict: "not_measurable", reason: /^Salesforce Knowledge is on, but its published articles couldn't be counted\.$/, stats: { knowledge: 1 } },
  { name: "no articles", verdict: "not_yet", reason: /^Salesforce Knowledge is on, with no published articles\.$/, stats: { knowledge: 1, articles: 0, articlesCapped: 0, articlesReady: 20 } },
  { name: "one article", verdict: "nearly", reason: /^Salesforce Knowledge is on, with 1 published article: fewer than 20, our working line/, stats: { knowledge: 1, articles: 1, articlesCapped: 0, articlesReady: 20 } },
  { name: "a few", verdict: "nearly", reason: /^Salesforce Knowledge is on, with 7 published articles: fewer than 20/, stats: { knowledge: 1, articles: 7, articlesCapped: 0, articlesReady: 20 } },
  { name: "stale", verdict: "nearly", reason: /^Salesforce Knowledge is on, with 30 published articles, but /, stats: { knowledge: 1, articles: 30, articlesCapped: 0, articlesReady: 20 }, findings: (t) => [issue("svc.knowledge-current", "service_setup", "medium", t)], quoted: true },
  { name: "ready", verdict: "ready", reason: /^Salesforce Knowledge is on, with 30 published articles\.$/, stats: { knowledge: 1, articles: 30, articlesCapped: 0, articlesReady: 20 } },
  { name: "ready, capped", verdict: "ready", reason: /^Salesforce Knowledge is on, with at least 2000 published articles\.$/, stats: { knowledge: 1, articles: 2000, articlesCapped: 1, articlesReady: 20 } },
];
const PLATFORM: MetricVariant[] = [
  { name: "unknown", verdict: "not_measurable", reason: /^Whether Einstein generative AI is on couldn't be read\.$/, stats: {} },
  { name: "off, nothing else", verdict: "not_yet", reason: /^Einstein generative AI isn't switched on, and Agentforce needs it\.$/, stats: { generativeAi: 0 } },
  { name: "off, with both", verdict: "not_yet", reason: /needs it\. Data Cloud is on, and 0 agents set up\.$/, stats: { generativeAi: 0, dataCloud: 1, agents: 0 } },
  { name: "ready, one agent", verdict: "ready", reason: /^Einstein generative AI and Agentforce are both switched on\. Data Cloud is off, and 1 agent set up\.$/, stats: { generativeAi: 1, agentforce: 1, dataCloud: 0, agents: 1 } },
  { name: "no Agentforce", verdict: "nearly", reason: /^Einstein generative AI is on, and Agentforce isn't\. 3 agents set up\.$/, stats: { generativeAi: 1, agentforce: 0, agents: 3 } },
  { name: "Agentforce unread", verdict: "nearly", reason: /^Einstein generative AI is on, and Agentforce couldn't be read\. Data Cloud is on\.$/, stats: { generativeAi: 1, dataCloud: 1 } },
];
const ACTION_RULE = "aut.undocumented-automation";
const ACTIONS: MetricVariant[] = [
  { name: "passed on", verdict: "not_measurable", reason: /couldn't be read: we can't read Zephyr_Setting__mdt/, off: true, passed: true },
  { name: "unknown", verdict: "not_measurable", reason: /^The automation an agent could call couldn't be read\.$/, stats: { autolaunched: 2, invocable: 1 }, value: 3 },
  { name: "nothing to call", verdict: "not_yet", reason: /^There's no active autolaunched flow or invocable Apex method/, stats: { autolaunched: 0, described: 0, invocable: 0 }, value: 0 },
  { name: "too few described", verdict: "nearly", reason: /^4 active autolaunched flows \(1 described\) and 2 invocable Apex methods\. An agent chooses an action by its description/, stats: { autolaunched: 4, described: 1, invocable: 2 }, value: 6 },
  { name: "one finding, singular", verdict: "nearly", reason: /^1 active autolaunched flow \(1 described\) and 1 invocable Apex method\. But /, stats: { autolaunched: 1, described: 1, invocable: 1 }, value: 2, findings: (t) => [issue(ACTION_RULE, "automation", "medium", t)], quoted: true },
  { name: "several findings", verdict: "nearly", reason: /^3 active autolaunched flows \(3 described\) and 0 invocable Apex methods\. But /, stats: { autolaunched: 3, described: 3, invocable: 0 }, value: 3, findings: (t) => [issue("aut.flows-without-fault-paths", "automation", "high", t), issue(ACTION_RULE, "automation", "low", TITLES[0]!)], quoted: true },
  { name: "ready, no flows", verdict: "ready", reason: /^0 active autolaunched flows \(0 described\) and 2 invocable Apex methods\.$/, stats: { autolaunched: 0, described: 0, invocable: 2 }, value: 2 },
];
const SHAPE: MetricVariant[] = [
  { name: "passed on", verdict: "not_measurable", reason: /couldn't be read: we can't read Zephyr_Setting__mdt/, off: true, passed: true },
  { name: "unknown", verdict: "not_measurable", reason: /^How the org's automation is built couldn't be read\.$/, stats: { automation: 10 }, value: 40 },
  { name: "not yet", verdict: "not_yet", reason: /^40% of active automation is still in Workflow Rules and Process Builder, which an agent can't call and Salesforce is retiring\.$/, stats: { legacy: 4, automation: 10 }, value: 40 },
  { name: "past the line", verdict: "nearly", reason: /^25% of active .*retiring\. Our working line is 10%\.$/, stats: { legacy: 1, automation: 4 }, value: 25 },
  { name: "within, a finding", verdict: "nearly", reason: /^5% of active .*retiring, within our working line of 10%, but /, stats: { legacy: 1, automation: 20 }, value: 5, findings: (t) => [issue("aut.legacy-workflow-rules", "automation", "high", t), issue("aut.mixed-automation-on-object", "automation", "medium", TITLES[0]!)], quoted: true },
  { name: "within", verdict: "ready", reason: /^5% of active .*retiring, within our working line of 10%\.$/, stats: { legacy: 1, automation: 20 }, value: 5, findings: () => [issue("aut.mixed-automation-on-object", "automation", "medium", TITLES[0]!)] },
  { name: "none left", verdict: "ready", reason: /^No active automation is left in Workflow Rules or Process Builder\.$/, stats: { legacy: 0, automation: 20 }, value: 0 },
];

const RULES = { knowledge: ["svc.knowledge-current"], actions: ["aut.undocumented-automation", "aut.flows-without-fault-paths"], shape: ["aut.legacy-workflow-rules", "aut.legacy-process-builder", "aut.mixed-automation-on-object", "tech.governor-limit-risk"] };
const ACCESS_RULES = ["int.integration-users-interactive", "sec.personal-integration-accounts", "sec.self-authorised-connected-apps"];

type Scenario = { name: string; input: ReportInput; picked: Record<PillarKey, { v: Variant; t: Title }> };

/** Scenario s: each pillar's variant s, round its list, and a title that moves on at its own pace, so the forms and titles meet in many pairings. */
function scenario(s: number): Scenario {
  const at = <T>(list: T[]): T => list[s % list.length]!;
  const picked = {
    data: { v: at(DATA), t: titleAt(s) },
    knowledge: { v: at(KNOWLEDGE), t: titleAt(s) },
    platform: { v: at(PLATFORM), t: titleAt(s) },
    actions: { v: at(ACTIONS), t: titleAt(s + 2) },
    access: { v: at(ACCESS), t: titleAt(s + 3) },
    shape: { v: at(SHAPE), t: titleAt(s + 4) },
  };
  const gone = (f: Finding): boolean =>
    f.domain === "data_quality" || f.domain === "security" || ACCESS_RULES.includes(f.ruleId) || (f.status === "issue" && [...RULES.knowledge, ...RULES.actions, ...RULES.shape].includes(f.ruleId));
  const findings = [...base.findings.filter((f) => !gone(f)), ...(Object.values(picked) as Array<{ v: Variant; t: Title }>).flatMap(({ v, t }) => v.findings?.(t) ?? [])];
  const k = picked.knowledge.v as MetricVariant;
  const p = picked.platform.v as MetricVariant;
  const a = picked.actions.v as MetricVariant;
  const sh = picked.shape.v as MetricVariant;
  const withStats = (id: string, stats: Stats, value?: number): Metric => ({ ...measured(id), stats, ...(value === undefined ? {} : { value }) });
  const metrics = base.metrics.map((m) => {
    if (m.id === AI_PLATFORM) return k.off ? notMeasurable(AI_PLATFORM, "The org's settings") : withStats(AI_PLATFORM, { ...k.stats, ...p.stats });
    if (m.id === AI_ACTIONS) return a.off ? notMeasurable(AI_ACTIONS, "Metadata") : withStats(AI_ACTIONS, a.stats!, a.value);
    if (m.id === AI_LEGACY_SHARE) return sh.off ? notMeasurable(AI_LEGACY_SHARE, "Apex triggers") : withStats(AI_LEGACY_SHARE, sh.stats!, sh.value);
    return m;
  });
  // With the platform metric not measured, both its pillars pass its reason on.
  if (k.off) picked.platform = { v: { name: "passed on", verdict: "not_measurable", reason: /couldn't be read: we can't read Zephyr_Setting__mdt/, passed: true }, t: picked.platform.t };
  const name = (Object.entries(picked) as Array<[string, { v: Variant; t: Title }]>).map(([key, { v, t }]) => `${key}=${v.name}${v.quoted ? ` "${t.text}"` : ""}`).join("; ");
  return { name, input: { ...base, findings, metrics }, picked };
}

describe("every branch of readinessOf that writes a reason, served marked and traced (DECISIONS.md 742)", () => {
  const all = [DATA, KNOWLEDGE, PLATFORM, ACTIONS, ACCESS, SHAPE].map((l) => l.length);
  // Twice round the longest list, so every variant meets more than one title.
  const count = 2 * Math.max(...all);
  const hit = new Set<string>();
  const titled = new Set<string>();

  for (let s = 0; s < count; s++) {
    const sc = scenario(s);
    test(`scenario ${s}: ${sc.name}`, () => {
      const c = check(sc.input);
      const d = c.digest!;
      assert.ok(d.readiness, "readiness is served");
      assert.deepEqual({ unmatched: c.unmatched, unmarked: c.unmarked, held: c.held }, { unmatched: 0, unmarked: 0, held: null });
      const report = buildReport(sc.input);
      assertWellFormed(d, report);
      for (const t of readinessTexts(d)) {
        assert.ok(marksOf(d, t), `${t.path} unmarked`);
        assert.ok(!t.text.includes(MASK), `${t.path} withheld: ${t.text}`);
      }
      assert.deepEqual(d.readiness.orgWords?.opening, ENGINE);
      d.readiness.pillars.forEach((p, i) => {
        const key = p.key as PillarKey;
        const { v, t } = sc.picked[key];
        const made = report.readiness.pillars[i]!;
        assert.equal(p.verdict, v.verdict, `${key}: ${p.reason}`);
        assert.match(p.reason, v.reason, key);
        hit.add(`${key}:${v.name}`);
        const marks = p.orgWords?.reason?.orgWords;
        assert.ok(marks, `${key}: no marks`);
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
