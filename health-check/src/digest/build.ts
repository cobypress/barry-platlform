import { createHash } from "node:crypto";
import { holdsNameOrSecret } from "../narrative/guard.ts";
import type { BuiltReport } from "../narrative/sections.ts";
import { buildPlan, domainLabel } from "../narrative/select.ts";
import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import { DIGEST_VERSION, NAMED_PEOPLE_SENTENCE, RunDigest, type DigestArea, type DigestFinding, type DigestMetric, type DigestTotal, type DigestValueModel } from "../schemas/digest.ts";
import type { Finding, Metric } from "../schemas/index.ts";
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

/** The hashes of the inventory's names that hold a known person's name, sorted, each once. The names themselves never leave. */
export function withheldNames(inventory: Inventory | null, pseudonyms: Pseudonymiser): string[] {
  if (!inventory) return [];
  return [...new Set(inventoryNames(inventory).filter((n) => holdsPersonOrSecret(n, pseudonyms)).map(sha256))].sort();
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
  // Typed by the parse below, which holds every id and code to its shape.
  const raw = {
    version: DIGEST_VERSION,
    runId: run.id,
    product: run.product,
    reviewed: run.product !== "pulse",
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
    },
    withheldNames: withheldNames(inventory, pseudonyms),
  };
  return RunDigest.parse(guardDigest(raw, pseudonyms));
}

/**
 * The keys whose values are an id, a code from a fixed list, a timestamp, a
 * hash or a currency code, each held to its shape by the schema, so no text
 * can sit there. Everything else is text, and goes through the guard. A key
 * added later is text until it's listed here, so it can't be missed.
 */
export const STRUCTURAL_KEYS: ReadonlySet<string> = new Set([
  "version", "runId", "product", "reviewed", "asOf", "madeAt", "baselineRunId", "status", "domain", "areaDomains", "findingIds", "id", "ruleId",
  "severity", "confidence", "recommendationType", "kind", "effortSize", "notMeasurableKind", "from", "to", "currency", "group", "counted",
  "lineIds", "leftOut", "later", "quickWins", "key", "grade", "withheldNames",
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
