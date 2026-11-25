import assert from "node:assert/strict";
import { checkedDigest, digestTextsAt, type CheckedDigest } from "../checked.ts";
import { AI_ACTIONS, AI_LEGACY_SHARE, AI_PLATFORM } from "../../domains/configuration/ai.ts";
import { textSha256 } from "../../engine/org-words.ts";
import { PILLARS, type PillarKey, type ReadinessVerdict } from "../../narrative/readiness.ts";
import { extractFigures } from "../../numbercheck/extract.ts";
import { MASK } from "../../render/repair.ts";
import { DIGEST_KEYS, NAMED_PEOPLE_SENTENCE, type RunDigest } from "../../schemas/digest.ts";
import { buildReport, type BuiltReport, type ReportInput } from "../../narrative/sections.ts";
import type { Pseudonymiser } from "../../privacy/pseudonymiser.ts";
import { loadRun } from "../../render/render-run.ts";
import type { Finding, Metric } from "../../schemas/index.ts";
import { AutomaticReplay, automaticSyntheticCalibration } from "../../testing/automatic.ts";
import { runFromRecording } from "../../testing/harness.ts";

// The constructed inputs that reach every branch of readinessOf that writes a
// reason (DECISIONS.md 742), shared by the tests that hold the digest's block to
// its marks and figures and the run's counts to those without it. Built on the
// synthetic org's automatic run, with no real data.

export const MADE_AT = "2026-11-25T09:00:00.000Z";
export const calibration = await automaticSyntheticCalibration();
export const automatic = loadRun((await runFromRecording({ calibration, reviewMode: "automatic", transport: new AutomaticReplay() })).dir);
export const base = automatic.input;

export const check = (input: ReportInput, pseudonyms: Pseudonymiser = automatic.pseudonyms, build?: (i: ReportInput) => BuiltReport): CheckedDigest =>
  checkedDigest({ input, pseudonyms, inventory: null, madeAt: MADE_AT, ...(build ? { build } : {}) });
/** The same run checked as if the report had no readiness: what every count must equal. */
export const withoutReadiness = (input: ReportInput, pseudonyms: Pseudonymiser = automatic.pseudonyms): CheckedDigest =>
  check(input, pseudonyms, (i) => {
    const { readiness: _r, ...rest } = buildReport(i);
    return rest as BuiltReport;
  });

/** A title, the org's words in it, and whether the run marked it at all. */
export type Title = { text: string; org: string | null; unmarked?: boolean };
export const TITLES: Title[] = [
  { text: "Some flows have no description an agent can read", org: null },
  { text: "Flows on Zephyr_Invoice__c have no description", org: "Zephyr_Invoice__c" },
  { text: "Zephyr sync jobs fail without telling anyone", org: "Zephyr sync" },
  { text: "Salesforce Knowledge articles are out of date", org: null },
  { text: "12 flows have no fault path", org: null },
  // A title the run couldn't mark: a reason quoting it can't be vouched for.
  { text: "Zephyr_Batch__c jobs stop part way", org: "Zephyr_Batch__c", unmarked: true },
];
export const titleAt = (n: number): Title => TITLES[n % TITLES.length]!;

/** An org word passed on in a metric's not measurable reason, as a refused read names its object. */
export const PASSED_ORG = "Zephyr_Setting__mdt";
const passedReason = (head: string): { reason: string; spans: Array<{ start: number; end: number }> } => {
  const reason = `${head} couldn't be read: we can't read ${PASSED_ORG} with this connection.`;
  const start = reason.indexOf(PASSED_ORG);
  return { reason, spans: [{ start, end: start + PASSED_ORG.length }] };
};

const like = base.findings.find((f) => f.status === "issue" && f.orgWords?.title && f.value.kind === "not_sized") ?? base.findings.find((f) => f.status === "issue" && f.orgWords?.title)!;
let made = 0;
/** A constructed issue, its title marked where the org's words are, or not marked at all. */
export function issue(ruleId: string, domain: string, severity: Finding["severity"], t: Title): Finding {
  const start = t.org ? t.text.indexOf(t.org) : -1;
  const spans = t.org ? [{ start, end: start + t.org.length }] : [];
  const { title: _t, ...marks } = like.orgWords ?? {};
  return {
    ...structuredClone(like),
    id: `f-00000000r${String(++made).padStart(3, "0")}`,
    ruleId,
    domain: domain as Finding["domain"],
    severity,
    status: "issue",
    title: t.text,
    subjects: [],
    orgWords: t.unmarked ? marks : { ...marks, title: { spans, textSha256: textSha256(t.text) } },
  };
}

export const measured = (id: string): Extract<Metric, { status: "measured" }> => structuredClone(base.metrics.find((m) => m.id === id)) as Extract<Metric, { status: "measured" }>;
/** A metric not measured, its reason marked with the org word in it, or (marked false) unmarked. */
export function notMeasurable(id: string, head: string, marked = true): Metric {
  const { value: _v, n: _n, dateRange: _d, segments: _s, segmentExemptReason: _e, series: _se, stats: _st, ...rest } = measured(id);
  const { reason, spans } = passedReason(head);
  const { reason: _r, ...marks } = rest.orgWords ?? {};
  return { ...rest, status: "not_measurable", reason, orgWords: marked ? { ...marks, reason: { spans, textSha256: textSha256(reason) } } : marks } as Metric;
}

/** One variant of one pillar: what it changes, what verdict it gives, and how its reason reads. */
export type Variant = { name: string; verdict: ReadinessVerdict; reason: RegExp; findings?: (t: Title) => Finding[]; quoted?: boolean; passed?: boolean };
type Stats = Record<string, number>;
export type MetricVariant = Variant & { stats?: Stats; value?: number; off?: boolean };

const severityVariants = (domain: string, rule: string, noun: RegExp): Variant[] => [
  { name: "not measured", verdict: "not_measurable", reason: new RegExp(`^None of the checks on ${noun.source} could be measured\\.$`), findings: () => [] },
  { name: "one serious", verdict: "not_yet", reason: /has a critical or high issue: /, findings: (t) => [issue(rule, domain, "high", t), issue(rule, domain, "low", TITLES[0]!)], quoted: true },
  { name: "several serious", verdict: "not_yet", reason: /has critical or high issues, such as: /, findings: (t) => [issue(rule, domain, "critical", t), issue(rule, domain, "high", TITLES[0]!)], quoted: true },
  { name: "one medium", verdict: "nearly", reason: /has nothing critical or high, but a medium issue: /, findings: (t) => [issue(rule, domain, "medium", t)], quoted: true },
  { name: "several medium", verdict: "nearly", reason: /has nothing critical or high, but medium issues, such as: /, findings: (t) => [issue(rule, domain, "medium", t), issue(rule, domain, "medium", TITLES[0]!), issue(rule, domain, "low", TITLES[0]!)], quoted: true },
  { name: "ready", verdict: "ready", reason: /has nothing above low severity\.$/, findings: () => [issue(rule, domain, "low", TITLES[0]!)] },
];

export const DATA = severityVariants("data_quality", "dq.zephyr-constructed", /data quality/);
export const ACCESS = severityVariants("security", "sec.zephyr-constructed", /security and integration access/);
const PASSED = /couldn't be read: we can't read Zephyr_Setting__mdt/;

export const KNOWLEDGE: MetricVariant[] = [
  { name: "passed on", verdict: "not_measurable", reason: PASSED, off: true, passed: true },
  { name: "unknown", verdict: "not_measurable", reason: /^Whether Salesforce Knowledge is on couldn't be read\.$/, stats: {} },
  { name: "off", verdict: "not_yet", reason: /^Salesforce Knowledge isn't switched on, so an agent has no articles/, stats: { knowledge: 0 } },
  { name: "uncounted", verdict: "not_measurable", reason: /^Salesforce Knowledge is on, but its published articles couldn't be counted\.$/, stats: { knowledge: 1 } },
  { name: "no articles", verdict: "not_yet", reason: /^Salesforce Knowledge is on, with no published articles\.$/, stats: { knowledge: 1, articles: 0, articlesCapped: 0, articlesReady: 20 } },
  { name: "one article", verdict: "nearly", reason: /^Salesforce Knowledge is on, with 1 published article: fewer than 20, our working line/, stats: { knowledge: 1, articles: 1, articlesCapped: 0, articlesReady: 20 } },
  { name: "a few", verdict: "nearly", reason: /^Salesforce Knowledge is on, with 7 published articles: fewer than 20/, stats: { knowledge: 1, articles: 7, articlesCapped: 0, articlesReady: 20 } },
  { name: "stale", verdict: "nearly", reason: /^Salesforce Knowledge is on, with 30 published articles, but /, stats: { knowledge: 1, articles: 30, articlesCapped: 0, articlesReady: 20 }, findings: (t) => [issue("svc.knowledge-current", "service_setup", "medium", t)], quoted: true },
  { name: "ready", verdict: "ready", reason: /^Salesforce Knowledge is on, with 30 published articles\.$/, stats: { knowledge: 1, articles: 30, articlesCapped: 0, articlesReady: 20 } },
  { name: "ready, capped", verdict: "ready", reason: /^Salesforce Knowledge is on, with at least 2000 published articles\.$/, stats: { knowledge: 1, articles: 2000, articlesCapped: 1, articlesReady: 20 } },
  { name: "capped, stale", verdict: "nearly", reason: /^Salesforce Knowledge is on, with at least 2000 published articles, but /, stats: { knowledge: 1, articles: 2000, articlesCapped: 1, articlesReady: 20 }, findings: (t) => [issue("svc.knowledge-current", "service_setup", "medium", t)], quoted: true },
];
export const PLATFORM: MetricVariant[] = [
  { name: "unknown", verdict: "not_measurable", reason: /^Whether Einstein generative AI is on couldn't be read\.$/, stats: {} },
  { name: "off, nothing else", verdict: "not_yet", reason: /^Einstein generative AI isn't switched on, and Agentforce needs it\.$/, stats: { generativeAi: 0 } },
  { name: "off, with both", verdict: "not_yet", reason: /needs it\. Data Cloud is on, and 0 agents set up\.$/, stats: { generativeAi: 0, dataCloud: 1, agents: 0 } },
  { name: "ready, one agent", verdict: "ready", reason: /^Einstein generative AI and Agentforce are both switched on\. Data Cloud is off, and 1 agent set up\.$/, stats: { generativeAi: 1, agentforce: 1, dataCloud: 0, agents: 1 } },
  { name: "ready, nothing else", verdict: "ready", reason: /^Einstein generative AI and Agentforce are both switched on\.$/, stats: { generativeAi: 1, agentforce: 1 } },
  { name: "no Agentforce", verdict: "nearly", reason: /^Einstein generative AI is on, and Agentforce isn't\. 3 agents set up\.$/, stats: { generativeAi: 1, agentforce: 0, agents: 3 } },
  { name: "Agentforce unread", verdict: "nearly", reason: /^Einstein generative AI is on, and Agentforce couldn't be read\. Data Cloud is on\.$/, stats: { generativeAi: 1, dataCloud: 1 } },
];
const ACTION_RULE = "aut.undocumented-automation";
export const ACTIONS: MetricVariant[] = [
  { name: "passed on", verdict: "not_measurable", reason: PASSED, off: true, passed: true },
  { name: "unknown", verdict: "not_measurable", reason: /^The automation an agent could call couldn't be read\.$/, stats: { autolaunched: 2, invocable: 1 }, value: 3 },
  { name: "nothing to call", verdict: "not_yet", reason: /^There's no active autolaunched flow or invocable Apex method/, stats: { autolaunched: 0, described: 0, invocable: 0 }, value: 0 },
  { name: "too few described", verdict: "nearly", reason: /^4 active autolaunched flows \(1 described\) and 2 invocable Apex methods\. An agent chooses an action by its description/, stats: { autolaunched: 4, described: 1, invocable: 2 }, value: 6 },
  { name: "one finding, singular", verdict: "nearly", reason: /^1 active autolaunched flow \(1 described\) and 1 invocable Apex method\. But /, stats: { autolaunched: 1, described: 1, invocable: 1 }, value: 2, findings: (t) => [issue(ACTION_RULE, "automation", "medium", t)], quoted: true },
  { name: "several findings", verdict: "nearly", reason: /^3 active autolaunched flows \(3 described\) and 0 invocable Apex methods\. But /, stats: { autolaunched: 3, described: 3, invocable: 0 }, value: 3, findings: (t) => [issue("aut.flows-without-fault-paths", "automation", "high", t), issue(ACTION_RULE, "automation", "low", TITLES[0]!)], quoted: true },
  { name: "ready, no flows", verdict: "ready", reason: /^0 active autolaunched flows \(0 described\) and 2 invocable Apex methods\.$/, stats: { autolaunched: 0, described: 0, invocable: 2 }, value: 2 },
];
export const SHAPE: MetricVariant[] = [
  { name: "passed on", verdict: "not_measurable", reason: PASSED, off: true, passed: true },
  { name: "unknown", verdict: "not_measurable", reason: /^How the org's automation is built couldn't be read\.$/, stats: { automation: 10 }, value: 40 },
  { name: "not yet", verdict: "not_yet", reason: /^40% of active automation is still in Workflow Rules and Process Builder, which an agent can't call and Salesforce is retiring\.$/, stats: { legacy: 4, automation: 10 }, value: 40 },
  { name: "past the line", verdict: "nearly", reason: /^25% of active .*retiring\. Our working line is 10%\.$/, stats: { legacy: 1, automation: 4 }, value: 25 },
  { name: "within, a finding", verdict: "nearly", reason: /^5% of active .*retiring, within our working line of 10%, but /, stats: { legacy: 1, automation: 20 }, value: 5, findings: (t) => [issue("aut.legacy-workflow-rules", "automation", "high", t), issue("aut.mixed-automation-on-object", "automation", "medium", TITLES[0]!)], quoted: true },
  { name: "within", verdict: "ready", reason: /^5% of active .*retiring, within our working line of 10%\.$/, stats: { legacy: 1, automation: 20 }, value: 5, findings: () => [issue("aut.mixed-automation-on-object", "automation", "medium", TITLES[0]!)] },
  { name: "none left", verdict: "ready", reason: /^No active automation is left in Workflow Rules or Process Builder\.$/, stats: { legacy: 0, automation: 20 }, value: 0 },
  { name: "none left, a finding", verdict: "nearly", reason: /^0% of active .*retiring, within our working line of 10%, but /, stats: { legacy: 0, automation: 20 }, value: 0, findings: (t) => [issue("tech.governor-limit-risk", "technical_health", "critical", t)], quoted: true },
];

const RULES = ["svc.knowledge-current", "aut.undocumented-automation", "aut.flows-without-fault-paths", "aut.legacy-workflow-rules", "aut.legacy-process-builder", "aut.mixed-automation-on-object", "tech.governor-limit-risk"];
export const ACCESS_RULES = ["int.integration-users-interactive", "sec.personal-integration-accounts", "sec.self-authorised-connected-apps"];
/** The synthetic org's findings without any that feed a pillar. */
export const stripped = (): Finding[] => base.findings.filter((f) => !(f.domain === "data_quality" || f.domain === "security" || ACCESS_RULES.includes(f.ruleId) || (f.status === "issue" && RULES.includes(f.ruleId))));

export type Scenario = { name: string; input: ReportInput; picked: Record<PillarKey, { v: Variant; t: Title }> };

/** Scenario s: each pillar's variant s, round its list, and a title that moves on at its own pace, so the forms and titles meet in many pairings. */
export function scenario(s: number): Scenario {
  const at = <T>(list: T[]): T => list[s % list.length]!;
  const picked: Record<PillarKey, { v: Variant; t: Title }> = {
    data: { v: at(DATA), t: titleAt(s) },
    knowledge: { v: at(KNOWLEDGE), t: titleAt(s) },
    platform: { v: at(PLATFORM), t: titleAt(s) },
    actions: { v: at(ACTIONS), t: titleAt(s + 2) },
    access: { v: at(ACCESS), t: titleAt(s + 3) },
    shape: { v: at(SHAPE), t: titleAt(s + 4) },
  };
  const findings = [...stripped(), ...Object.values(picked).flatMap(({ v, t }) => v.findings?.(t) ?? [])];
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
  if (k.off) picked.platform = { v: { name: "passed on", verdict: "not_measurable", reason: PASSED, passed: true }, t: picked.platform.t };
  const name = (Object.entries(picked) as Array<[string, { v: Variant; t: Title }]>).map(([key, { v, t }]) => `${key}=${v.name}${v.quoted ? ` "${t.text}"${t.unmarked ? " (unmarked)" : ""}` : ""}`).join("; ");
  return { name, input: { ...base, findings, metrics }, picked };
}

/** Twice round the longest list, so every variant meets more than one title. */
export const SCENARIOS = 2 * Math.max(...[DATA, KNOWLEDGE, PLATFORM, ACTIONS, ACCESS, SHAPE].map((l) => l.length));

export const ENGINE = { orgWordsMarked: true, orgWords: [] };
export const readinessTexts = (d: RunDigest) => digestTextsAt(d).filter((t) => t.path.startsWith("readiness."));

/** Every key in a value, at any depth, but the finding ids that key the plan's alsoFor. */
const keysOf = (value: unknown): string[] =>
  Array.isArray(value) ? value.flatMap(keysOf) : value && typeof value === "object" ? Object.entries(value).flatMap(([k, v]) => [k, ...(k === "alsoFor" ? [] : keysOf(v))]) : [];

/** A reason the digest doesn't serve as made: withheld, or replaced by the fixed sentence. */
export const replaced = (reason: string): boolean => reason === MASK || reason === NAMED_PEOPLE_SENTENCE;

/** The invariants every digest's readiness holds: ids among the findings, labels from the table, numbers in figures. */
export function assertWellFormed(d: RunDigest, report: BuiltReport): void {
  const r = d.readiness!;
  const ids = new Set(d.findings.map((f) => f.id));
  for (const p of r.pillars) for (const id of p.findingIds) assert.ok(ids.has(id), `${p.key}: ${id} is not among the digest's findings`);
  for (const id of r.fixIds) assert.ok(ids.has(id), `fix ${id} is not among the digest's findings`);
  assert.ok(r.fixIds.length <= 5);
  assert.deepEqual(r.fixIds, report.readiness.fixes.map((f) => f.id));
  assert.deepEqual(r.pillars.map((p) => p.key), Object.keys(PILLARS));
  for (const p of r.pillars) {
    const table = PILLARS[p.key as PillarKey];
    for (const [text, own] of [[p.label, table.label], [p.question, table.question]] as const) assert.ok(text === own || replaced(text), `${p.key}: ${text}`);
  }
  for (const k of keysOf(d)) assert.ok(DIGEST_KEYS.has(k), `${k} isn't in DIGEST_KEYS`);
  r.pillars.forEach((p, i) => {
    const made = report.readiness.pillars[i]!;
    // A reason not served as made carries no figures: the numbers went with it.
    if (replaced(p.reason)) return void assert.equal(p.figures, undefined, `${p.key}: figures beside a reason not served`);
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

/** The counts a hold is decided on, and the digest apart from readiness: what a run with readiness must share with the same run without it. */
export function assertSameAsWithout(c: CheckedDigest, w: CheckedDigest, what: string): void {
  assert.deepEqual({ masks: c.masks, unmatched: c.unmatched, unmarked: c.unmarked, held: c.held }, { masks: w.masks, unmatched: w.unmatched, unmarked: w.unmarked, held: w.held }, what);
  const { readiness: _r, ...rest } = c.digest ?? {};
  assert.deepEqual(c.digest ? rest : null, w.digest, what);
  assert.equal(w.readinessWithheld, 0);
}
