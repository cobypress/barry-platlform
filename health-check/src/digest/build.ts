import { createHash } from "node:crypto";
import { holdsNameOrSecret } from "../narrative/guard.ts";
import type { BuiltReport } from "../narrative/sections.ts";
import { buildPlan, DOMAIN_LABELS, domainLabel, PHASE_TITLES } from "../narrative/select.ts";
import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import { knownPeople } from "../fix/prompt.ts";
import { spansIfCurrent, textSha256, type OrgSpan } from "../engine/org-words.ts";
import { engineTextKey } from "./engine-texts-key.ts";
import { ENGINE_TEXT_KEYS } from "./engine-texts.ts";
import { UNSCORED_RULES } from "../domains/scoring.ts";
import { DIGEST_VERSION, NAMED_PEOPLE_SENTENCE, RunDigest, type DigestArea, type DigestFinding, type DigestMetric, type DigestTotal, type DigestValueModel, type SentenceOrgWords } from "../schemas/digest.ts";
import { FIX_PROMPT_MAX, type Finding, type Metric } from "../schemas/index.ts";
import type { Inventory } from "../schemas/inventory.ts";
import { GROUPS, HEADLINE_KINDS, type ValueKind } from "../value/model.ts";

// The run digest (DECISIONS.md 632): made by code, with no Claude, from the
// built report the documents are made from, so a consultant's corrections are
// already in every figure, and from the run's own pseudonym map, which only the
// worker holds, so text that could name someone is caught here. The console
// can check a token's or an email's shape, never a real name.

/** Any pseudonym token, `[[email]]` included: a name was meant to go back in, which never happens here. */
const TOKEN = /\[\[[a-z]+[:\]]/;
const EMAIL = /@/;
/** A Salesforce id's shape, 15 or 18 letters and digits with at least one digit. */
const SALESFORCE_ID = /\b(?=[a-zA-Z0-9]*\d)[a-zA-Z0-9]{15}(?:[a-zA-Z0-9]{3})?\b/;

/** Words run together in an API name, apart: "ReedExportJob" as "Reed Export Job", so a name inside one is seen. */
const camelApart = (text: string): string => text.replace(/(\p{Ll})(\p{Lu})/gu, "$1 $2").replace(/(\p{Lu})(\p{Lu}\p{Ll})/gu, "$1 $2");

/**
 * Whether text could name someone or hold a secret: a token, an email, an id's
 * shape, or anything the guard in front of Claude would change or refuse, on
 * the text as written and with run-together words apart. That guard is the
 * engine's one name check; this adds only the shapes it leaves to scrub().
 */
export function holdsPersonOrSecret(text: string, pseudonyms: Pseudonymiser): boolean {
  return TOKEN.test(text) || EMAIL.test(text) || SALESFORCE_ID.test(text) || holdsNameOrSecret(text, pseudonyms) || holdsNameOrSecret(camelApart(text), pseudonyms);
}

/** Text as the digest keeps it: as written, or the one fixed sentence. An empty recommendation stays empty. */
export function digestText(text: string, pseudonyms: Pseudonymiser): string {
  return text === "" || !holdsPersonOrSecret(text, pseudonyms) ? text : NAMED_PEOPLE_SENTENCE;
}

const sha256 = (text: string): string => createHash("sha256").update(text, "utf8").digest("hex");

/** Every name an inventory row is shown by: objects, fields, packages, apps, profiles, permission sets and limits. */
export function inventoryNames(inventory: Inventory): string[] {
  const names: Array<string | null> = [];
  if ("rows" in inventory.fillRates) {
    for (const o of inventory.fillRates.rows) {
      names.push(o.object);
      if ("fields" in o) names.push(...o.fields.map((f) => f.field), ...o.skipped.map((s) => s.field));
    }
  }
  if ("rows" in inventory.packages) for (const p of inventory.packages.rows) names.push(p.namespace, p.name, p.versionName);
  if ("rows" in inventory.connectedApps) for (const a of inventory.connectedApps.rows) names.push(a.name);
  if ("rows" in inventory.riskyPermissions) for (const p of inventory.riskyPermissions.rows) names.push(p.name, p.apiName, p.namespace, ...p.grants);
  if ("rows" in inventory.limits) for (const l of inventory.limits.rows) names.push(l.name);
  return names.filter((n): n is string => typeof n === "string" && n.length > 0);
}

/** A name's words as written, split on underscores, spaces, punctuation, digits and case changes. */
const wordsAsWritten = (name: string): string[] =>
  name
    .replace(/(\p{Ll})(\p{Lu})/gu, "$1 $2")
    .replace(/(\p{Lu})(\p{Lu}\p{Ll})/gu, "$1 $2")
    .split(/[^\p{L}\p{M}]+/u)
    .filter(Boolean);

const fold = (word: string): string => word.normalize("NFKD").replace(/\p{M}/gu, "").toLowerCase();

/**
 * Whether an inventory name names a known person in a way no ordinary field name
 * does (DECISIONS.md 636): the first name and surname as whole words side by side
 * in either order ("Sam_Reed_Queue"), or run together as one whole word
 * ("samreed_export", "reedsam"), or initial and surname as one whole word written
 * in one case ("sreed", "SREED"). Never a first name or surname alone, never a
 * part of a longer word, and never a capitalised word ("Amount" is a word, not
 * A. Mount), so Amount__c, Unit_Price__c, Response_Time__c and Days_Open__c stay.
 */
export function inventoryNameNamesPerson(name: string, pseudonyms: Pseudonymiser): boolean {
  const written = wordsAsWritten(name);
  const words = written.map(fold);
  for (const parts of knownPeople(pseudonyms)) {
    if (parts.length < 2) continue;
    const [first, last] = [parts[0]!, parts.at(-1)!];
    for (let i = 0; i < words.length; i++) {
      const w = words[i]!;
      if ((w === first && words[i + 1] === last) || (w === last && words[i + 1] === first)) return true;
      if (w === `${first}${last}` || w === `${last}${first}`) return true;
      const oneCase = written[i] === written[i]!.toLowerCase() || written[i] === written[i]!.toUpperCase();
      if (oneCase && last.length >= 3 && w === `${first[0]}${last}`) return true;
    }
  }
  return false;
}

/** The hashes of the inventory's names that hold a known person's name, sorted, each once. The names themselves never leave. */
export function withheldNames(inventory: Inventory | null, pseudonyms: Pseudonymiser): string[] {
  if (!inventory) return [];
  // And a whole-word match on people's names (DECISIONS.md 636), so a lower-case run-together name ("sreed_sync") is caught.
  return [...new Set(inventoryNames(inventory).filter((n) => holdsPersonOrSecret(n, pseudonyms) || inventoryNameNamesPerson(n, pseudonyms)).map(sha256))].sort();
}

// Each part is put together from the report as it stands; guardDigest then takes every string in it through the guard.

function digestFinding(f: Finding): DigestFinding {
  const v = f.value;
  return {
    id: f.id,
    ruleId: f.ruleId,
    domain: f.domain,
    status: f.status,
    severity: f.severity,
    confidence: f.confidence,
    title: f.title,
    summary: f.summary,
    recommendation: f.recommendation,
    recommendationType: f.recommendationType,
    value: v.kind === "not_sized" ? { kind: "not_sized", reason: v.reason } : { kind: v.kind, amount: v.amount, unit: v.unit, period: v.period, assumptions: [...v.assumptions] },
    effortSize: f.effort.size,
    notMeasurableReason: f.notMeasurableReason,
    ...(f.notMeasurableKind ? { notMeasurableKind: f.notMeasurableKind } : {}),
    ...(f.effortHours ? { effortHours: { low: f.effortHours.low, high: f.effortHours.high, basis: f.effortHours.basis, ...(f.effortHours.partial ? { partial: true as const } : {}) } } : {}),
    ...(f.timeBand ? { timeBand: f.timeBand } : {}),
    ...(f.fixComplexity ? { fixComplexity: f.fixComplexity } : {}),
    ...fixPromptOf(f),
    ...checkerReadOf(f),
    ...orgWordsOf(f),
  };
}

/** What the code checker read, for one of its six groups' issues and passes only (DECISIONS.md 673): the console's re-check needs it. */
function checkerReadOf(f: Finding): { checkerRead?: DigestFinding["checkerRead"] } {
  if (!UNSCORED_RULES.has(f.ruleId) || f.status === "not_measurable") return {};
  const { readClasses, readTriggers, alreadyListed } = f.facts;
  const count = (n: number | undefined): n is number => typeof n === "number" && Number.isInteger(n) && n >= 0;
  if (!count(readClasses) || !count(readTriggers) || !count(alreadyListed)) return {};
  return { checkerRead: { readClasses, readTriggers, alreadyListed } };
}

/**
 * An issue's fix prompt (DECISIONS.md 672), from the report as corrected: a
 * correction that changed a figure has already dropped it (628), so it isn't
 * here. Whole or not at all: one over the limit (the engine makes none) is left
 * out, never cut mid-instruction.
 */
function fixPromptOf(f: Finding): Pick<DigestFinding, "fixPrompt"> {
  return f.status === "issue" && f.fixPrompt && f.fixPrompt.length <= FIX_PROMPT_MAX ? { fixPrompt: f.fixPrompt } : {};
}

const MARKED = ["title", "summary", "recommendation", "notMeasurableReason"] as const;
type Marked = (typeof MARKED)[number];

/**
 * The sentences the engine vouches for (DECISIONS.md 655), each only while it
 * still reads as it did when it was marked: a consultant's corrected figure or
 * the team-level wording changes the text and leaves it unknown.
 */
function orgWordsOf(f: Finding): Pick<DigestFinding, "orgWords"> {
  const out: NonNullable<DigestFinding["orgWords"]> = {};
  for (const key of MARKED) {
    const text = f[key];
    const spans = typeof text === "string" ? spansIfCurrent(text, f.orgWords?.[key]) : null;
    if (spans) out[key] = { orgWordsMarked: true, orgWords: spans };
  }
  // A value's reason, its assumptions all together or none, and the hours' basis (DECISIONS.md 663).
  const marks = f.orgWords;
  const v = f.value;
  if (v.kind === "not_sized") {
    const spans = spansIfCurrent(v.reason, marks?.valueReason);
    if (spans) out.valueReason = { orgWordsMarked: true, orgWords: spans };
  } else if (marks?.valueAssumptions) {
    const held = marks.valueAssumptions;
    const each = v.assumptions.map((a, i) => spansIfCurrent(a, held[i])).filter((s): s is OrgSpan[] => s !== null);
    if (held.length === v.assumptions.length && each.length === v.assumptions.length) out.valueAssumptions = each.map((s) => ({ orgWordsMarked: true, orgWords: s }));
  }
  if (f.effortHours) {
    const spans = spansIfCurrent(f.effortHours.basis, marks?.hoursBasis);
    if (spans) out.hoursBasis = { orgWordsMarked: true, orgWords: spans };
  }
  // The fix prompt's marks, only beside the prompt the digest holds and only while it still hashes to them (DECISIONS.md 672).
  const prompt = fixPromptOf(f).fixPrompt;
  if (prompt) {
    const spans = spansIfCurrent(prompt, marks?.fixPrompt);
    if (spans) out.fixPrompt = { orgWordsMarked: true, textSha256: textSha256(prompt), orgWords: spans };
  }
  return Object.keys(out).length > 0 ? { orgWords: out } : {};
}

/**
 * The marks once the text guard has run: a sentence it replaced with the fixed
 * sentence holds only the engine's words; one it changed any other way (none
 * does today) is unknown.
 */
function afterGuard(before: DigestFinding, after: DigestFinding): DigestFinding {
  const marks: NonNullable<DigestFinding["orgWords"]> = { ...after.orgWords };
  for (const key of MARKED) {
    if (before[key as Marked] === after[key as Marked]) continue;
    if (after[key] === NAMED_PEOPLE_SENTENCE) marks[key] = { orgWordsMarked: true, orgWords: [] };
    else delete marks[key];
  }
  // The same for the value and the hours (DECISIONS.md 663): the fixed sentence is the engine's, any other change unknown.
  const replaced = (was: string | undefined, now: string | undefined, held: SentenceOrgWords | undefined): SentenceOrgWords | undefined =>
    was === now ? held : now === NAMED_PEOPLE_SENTENCE ? { orgWordsMarked: true, orgWords: [] } : undefined;
  const [vb, va] = [before.value, after.value];
  if (vb.kind === "not_sized" && va.kind === "not_sized") marks.valueReason = replaced(vb.reason, va.reason, marks.valueReason);
  if (vb.kind !== "not_sized" && va.kind !== "not_sized" && marks.valueAssumptions) {
    const held = marks.valueAssumptions;
    const each = va.assumptions.map((a, i) => replaced(vb.assumptions[i], a, held[i])).filter((m): m is SentenceOrgWords => m !== undefined);
    marks.valueAssumptions = each.length === va.assumptions.length && va.assumptions.length === vb.assumptions.length ? each : undefined;
  }
  if (before.effortHours && after.effortHours) marks.hoursBasis = replaced(before.effortHours.basis, after.effortHours.basis, marks.hoursBasis);
  for (const key of ["valueReason", "valueAssumptions", "hoursBasis"] as const) if (marks[key] === undefined) delete marks[key];
  // A fix prompt the guard changed in any way is left out whole, with its marks: never the fixed sentence in a prompt's place (DECISIONS.md 672).
  // The marks' hash is the prompt's, which no guard reads as text.
  const promptKept = after.fixPrompt !== undefined && after.fixPrompt === before.fixPrompt;
  if (promptKept && before.orgWords?.fixPrompt) marks.fixPrompt = before.orgWords.fixPrompt;
  else delete marks.fixPrompt;
  const { orgWords: _was, fixPrompt: _prompt, ...rest } = after;
  const kept = promptKept ? { ...rest, fixPrompt: after.fixPrompt } : rest;
  return Object.keys(marks).length > 0 ? { ...kept, orgWords: marks } : kept;
}

/**
 * The hours to fix everything the plan holds (DECISIONS.md 636): its quick wins,
 * the findings in its phases and later list, and the other issues each action
 * also covers, each once. The total adds those with hours; the ones whose hours
 * cover only part, and the ones with none, are listed beside it, so the total is
 * never read as the whole. Null when no finding in the plan has hours.
 */
/** What the plan says when the code checker's findings are left out of its total: a fixed sentence of ours (DECISIONS.md 673). */
export const PLAN_HOURS_APART = "Findings from our code checker are listed with their own hours and are not included in this total yet.";

export function planHours(plan: ReturnType<typeof buildPlan>): { low: number; high: number; findingIds: string[]; partialFindingIds: string[]; withoutHoursFindingIds: string[]; notTotalledFindingIds?: string[]; note?: string } | null {
  const seen = new Map<string, Finding>();
  for (const f of [...plan.quickWins, ...plan.phases.flatMap((p) => p.findings), ...plan.later].flatMap((f) => [f, ...(plan.alsoFor[f.id] ?? [])])) if (!seen.has(f.id)) seen.set(f.id, f);
  // The code checker's groups keep their own hours and stay out of the total for now (DECISIONS.md 673).
  const apart = [...seen.values()].filter((f) => UNSCORED_RULES.has(f.ruleId));
  const counted = [...seen.values()].filter((f) => !UNSCORED_RULES.has(f.ruleId));
  const held = counted.filter((f) => f.effortHours);
  if (held.length === 0 && apart.length === 0) return null;
  return {
    low: held.reduce((s, f) => s + f.effortHours!.low, 0),
    high: held.reduce((s, f) => s + f.effortHours!.high, 0),
    findingIds: held.map((f) => f.id),
    partialFindingIds: held.filter((f) => f.effortHours!.partial).map((f) => f.id),
    withoutHoursFindingIds: counted.filter((f) => !f.effortHours).map((f) => f.id),
    ...(apart.length ? { notTotalledFindingIds: apart.map((f) => f.id), note: PLAN_HOURS_APART } : {}),
  };
}

function digestMetric(m: Metric): DigestMetric {
  const base = { id: m.id, domain: m.domain, label: m.label, definition: m.definition, unit: m.unit };
  if (m.status === "measured") return { status: "measured", ...base, value: m.value, n: m.n, dateRange: m.dateRange, assumptions: [...m.assumptions], exclusions: [...m.exclusions] };
  return { status: "not_measurable", ...base, reason: m.reason, ...(m.notMeasurableKind ? { notMeasurableKind: m.notMeasurableKind } : {}) };
}

/** The value model as the report has it, every total with the lines it adds up. */
function digestValue(report: BuiltReport): DigestValueModel {
  const { value, added } = report;
  // A group with no fixed label is named after its counted finding, by the title as the executive report says it, never as read.
  const teamTitle = new Map(report.teamFindings.map((f) => [f.id, f.title]));
  const labelOf = (g: (typeof value.groups)[number]): string => GROUPS[g.id]?.label ?? teamTitle.get(g.counted.findingId) ?? g.label;
  const lines = value.groups.flatMap((g) => g.lines.map((l) => ({ id: l.findingId, ruleId: l.ruleId, kind: l.kind, group: g.id, annual: l.annual, low: l.low, expected: l.expected, high: l.high, added: added.get(l.findingId) ?? 0 })));
  const totalOf = (kind: ValueKind): DigestTotal => ({ ...value.byKind[kind], lineIds: value.groups.filter((g) => g.kind === kind).map((g) => g.counted.findingId) });
  // As buildValueModel picks it: the first group whose counted line is strictly larger than any before.
  let largest: (typeof value.groups)[number] | undefined;
  for (const g of value.groups) if (g.kind === "exposure" && g.counted.expected > (largest?.counted.expected ?? 0)) largest = g;
  const byKind = { revenue_recoverable: totalOf("revenue_recoverable"), conversion_uplift: totalOf("conversion_uplift"), licence_waste: totalOf("licence_waste"), time_cost: totalOf("time_cost") };
  return {
    currency: value.currency,
    total: { ...value.total, lineIds: HEADLINE_KINDS.flatMap((k) => byKind[k as keyof typeof byKind].lineIds) },
    byKind,
    exposure: { ...value.byKind.exposure, lineIds: largest ? [largest.counted.findingId] : [] },
    groups: value.groups.map((g) => ({ id: g.id, kind: g.kind, label: labelOf(g), realisation: { ...g.realisation }, counted: g.counted.findingId, lineIds: g.lines.map((l) => l.findingId) })),
    lines,
    leftOut: value.left.map((l) => l.findingId),
  };
}

function digestAreas(report: BuiltReport): DigestArea[] {
  return (report.input.scores ?? []).map((s): DigestArea =>
    s.status === "measured"
      ? { status: "measured", domain: s.domain, label: domainLabel(s.domain), score: s.score, findingIds: [...s.findingIds] }
      : { status: "not_measurable", domain: s.domain, label: domainLabel(s.domain), reason: s.reason, ...(s.notMeasurableKind ? { notMeasurableKind: s.notMeasurableKind } : {}) },
  );
}

const GRADE = /^[A-E]$/;

export type DigestFor = {
  /** The built report the documents (or a pulse's result) are made from, corrections applied. */
  report: BuiltReport;
  /** The run's own map, so text naming someone is caught. Only the worker has it. */
  pseudonyms: Pseudonymiser;
  /** The inventory as the console's row holds it, for `withheldNames`. Null with none. */
  inventory: Inventory | null;
  madeAt: string;
};

/**
 * The digest of a pulse or a released check. Parsed before it's returned, so
 * one the console couldn't read is never written. A scan has none.
 */
export function buildDigest({ report, pseudonyms, inventory, madeAt }: DigestFor): RunDigest {
  const { run } = report.input;
  if (run.product === "scan") throw new Error("A scan has no digest");
  const plan = buildPlan(report.teamFindings, report.added);
  const ids = (fs: Finding[]) => fs.map((f) => f.id);
  const overall = report.overall;
  const hoursTotal = planHours(plan);
  // Typed by the parse below, which holds every id and code to its shape.
  const raw = {
    version: DIGEST_VERSION,
    runId: run.id,
    product: run.product,
    // An automatic run's sections passed the machine checks, and no person reviewed them (DECISIONS.md 688).
    reviewed: run.product !== "pulse" && run.reviewMode === "consultant",
    asOf: run.asOf,
    madeAt,
    baselineRunId: run.baselineRunId,
    overall:
      overall.status === "measured" && Number.isInteger(overall.score) && GRADE.test(overall.grade)
        ? { status: "measured", score: overall.score, grade: overall.grade, areaDomains: overall.areas.map((a) => a.domain) }
        : { status: "not_measurable", reason: overall.status === "not_measurable" ? overall.reason : "The overall score couldn't be kept." },
    areas: digestAreas(report),
    findings: report.teamFindings.map(digestFinding),
    metrics: report.metrics.map(digestMetric),
    value: digestValue(report),
    plan: {
      phases: plan.phases.map((p) => ({ key: p.key, title: p.title, from: p.from, to: p.to, findingIds: ids(p.findings) })),
      later: ids(plan.later),
      quickWins: ids(plan.quickWins),
      alsoFor: Object.fromEntries(Object.entries(plan.alsoFor).map(([id, fs]) => [id, ids(fs)])),
      ...(hoursTotal ? { hoursTotal } : {}),
    },
    withheldNames: withheldNames(inventory, pseudonyms),
  };
  const guarded = guardDigest(raw, pseudonyms);
  const findings = guarded.findings.map((f, i) => withValueWords(afterGuard(raw.findings[i]!, f)));
  const merged = { ...guarded, findings, ...fixedWords(guarded, findings) };
  // Typed by the parse below, as the raw digest is.
  return RunDigest.parse({ ...merged, ...moreWords(merged as unknown as RunDigest, report) });
}

const ENGINE_WORDS: SentenceOrgWords = { orgWordsMarked: true, orgWords: [] };

/** Ours where the engine wrote the whole text, or a template of it with only figures put in (DECISIONS.md 693); else unknown. */
const engineWords = (text: string): SentenceOrgWords | undefined => (ENGINE_TEXT_KEYS.has(engineTextKey(text)) ? ENGINE_WORDS : undefined);

/** A sized value's unit and period marked (DECISIONS.md 693): the unit a currency code the org set, whole, or ours; the period the engine's. */
function withValueWords(f: DigestFinding): DigestFinding {
  const v = f.value;
  if (v.kind === "not_sized") return f;
  const unit = /^[A-Z]{3}$/.test(v.unit) ? { orgWordsMarked: true as const, orgWords: [{ start: 0, end: 3 }] } : v.unit === "hours" ? ENGINE_WORDS : undefined;
  const period = engineWords(v.period);
  if (!unit && !period) return f;
  return { ...f, orgWords: { ...f.orgWords, ...(unit ? { valueUnit: unit } : {}), ...(period ? { valuePeriod: period } : {}) } };
}

/**
 * The texts the digest serves beyond a finding's, marked (DECISIONS.md 693):
 * each metric's from the marks its run made (an automatic run's), while its text
 * still reads as marked, or ours where the engine wrote it; an area's and the
 * overall score's reason and the plan's hours note where the engine wrote them.
 * Anything else is left unknown, for the checked digest to withhold or hold.
 */
function moreWords(d: Pick<RunDigest, "metrics" | "areas" | "overall" | "plan">, report: BuiltReport): Pick<RunDigest, "metrics" | "areas" | "overall" | "plan"> {
  const stored = new Map(report.metrics.map((m) => [m.id, m]));
  const mark = (text: string, held: { spans: OrgSpan[]; textSha256: string } | null | undefined): SentenceOrgWords | undefined => {
    const spans = held ? spansIfCurrent(text, held) : null;
    return spans ? { orgWordsMarked: true, orgWords: spans } : engineWords(text);
  };
  const metrics = d.metrics.map((m): DigestMetric => {
    const held = stored.get(m.id)?.orgWords;
    const label = mark(m.label, held?.label);
    const definition = mark(m.definition, held?.definition);
    const list = (texts: string[], marks: Array<{ spans: OrgSpan[]; textSha256: string } | null> | undefined) => texts.map((t, i) => mark(t, marks?.[i]) ?? null);
    const words = m.status === "measured"
      ? { ...(label ? { label } : {}), ...(definition ? { definition } : {}), assumptions: list(m.assumptions, held?.assumptions), exclusions: list(m.exclusions, held?.exclusions) }
      : { ...(label ? { label } : {}), ...(definition ? { definition } : {}), ...(mark(m.reason, held?.reason) ? { reason: mark(m.reason, held?.reason)! } : {}) };
    return { ...m, orgWords: words };
  });
  const areas = d.areas.map((a) => {
    if (a.status !== "not_measurable") return a;
    const reason = engineWords(a.reason);
    return reason ? { ...a, orgWords: { ...a.orgWords, reason } } : a;
  });
  const overallReason = d.overall.status === "not_measurable" ? engineWords(d.overall.reason) : undefined;
  const overall = d.overall.status === "not_measurable" && overallReason ? { ...d.overall, orgWords: { reason: overallReason } } : d.overall;
  const note = d.plan.hoursTotal?.note;
  const noteWords = note ? engineWords(note) : undefined;
  const plan = d.plan.hoursTotal && noteWords ? { ...d.plan, hoursTotal: { ...d.plan.hoursTotal, orgWords: { note: noteWords } } } : d.plan;
  return { metrics, areas, overall, plan };
}

/**
 * The labels and titles the engine takes from its own tables (DECISIONS.md 663),
 * marked as its words only while each, as the digest holds it, is still the
 * table's word for its key. A value group named after its counted finding takes
 * that finding's title marks while the two read the same. Anything else is unknown.
 */
export function fixedWords<P extends { phases: Array<{ key: string; title: string }> }>(d: { areas: DigestArea[]; plan: P; value: DigestValueModel }, findings: DigestFinding[]) {
  const titleOf = (key: string): string | undefined => Object.entries(PHASE_TITLES).find(([k]) => k === key)?.[1];
  const byId = new Map(findings.map((f) => [f.id, f]));
  const groupMarks = (g: DigestValueModel["groups"][number]): SentenceOrgWords | undefined => {
    if (Object.hasOwn(GROUPS, g.id) && g.label === GROUPS[g.id]!.label) return ENGINE_WORDS;
    const counted = byId.get(g.counted);
    return counted && counted.title === g.label ? counted.orgWords?.title : undefined;
  };
  return {
    areas: d.areas.map((a) => (Object.hasOwn(DOMAIN_LABELS, a.domain) && a.label === DOMAIN_LABELS[a.domain] ? { ...a, orgWords: { label: ENGINE_WORDS } } : a)),
    plan: { ...d.plan, phases: d.plan.phases.map((p) => (titleOf(p.key) === p.title ? { ...p, orgWords: { title: ENGINE_WORDS } } : p)) },
    value: {
      ...d.value,
      groups: d.value.groups.map((g) => {
        const label = groupMarks(g);
        return label ? { ...g, orgWords: { label } } : g;
      }),
    },
  };
}

/**
 * The keys whose values are an id, a code from a fixed list, a timestamp, a
 * hash or a currency code, each held to its shape by the schema, so no text
 * can sit there. Everything else is text, and goes through the guard. A key
 * added later is text until it's listed here, so it can't be missed.
 */
export const STRUCTURAL_KEYS: ReadonlySet<string> = new Set([
  "version", "runId", "product", "reviewed", "asOf", "madeAt", "baselineRunId", "status", "domain", "areaDomains", "findingIds", "id", "ruleId",
  "severity", "confidence", "recommendationType", "kind", "effortSize", "notMeasurableKind", "timeBand", "fixComplexity", "from", "to", "currency", "group", "counted",
  "lineIds", "leftOut", "later", "quickWins", "key", "grade", "withheldNames", "partialFindingIds", "withoutHoursFindingIds", "notTotalledFindingIds", "textSha256",
]);
/** Keyed by finding id, each holding finding ids: ids all the way down. */
export const ID_MAPS: ReadonlySet<string> = new Set(["alsoFor"]);

/** Every string in the digest outside STRUCTURAL_KEYS, through digestText: the one route text takes into a digest. */
export function guardDigest<T>(value: T, pseudonyms: Pseudonymiser, key = ""): T {
  if (STRUCTURAL_KEYS.has(key) || ID_MAPS.has(key)) return value;
  if (typeof value === "string") return digestText(value, pseudonyms) as T;
  if (Array.isArray(value)) return value.map((v) => guardDigest(v, pseudonyms, key)) as T;
  if (value && typeof value === "object") return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, guardDigest(v, pseudonyms, k)])) as T;
  return value;
}
