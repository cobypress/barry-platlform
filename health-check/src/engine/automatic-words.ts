import { engineTextKey } from "../digest/engine-texts-key.ts";
import { ENGINE_TEXT_KEYS } from "../digest/engine-texts.ts";
import { replaceMarked, type Marked } from "../fix/marked.ts";
import type { Finding, Metric, MetricOrgWords } from "../schemas/index.ts";
import type { MarkedSentence } from "./org-words.ts";
import { orgWordsOf, spansIfCurrent, textSha256 } from "./org-words.ts";

// What a run whose calibration was read from the org's settings says, and how
// its texts are marked (DECISIONS.md 693). An automatic run's customer bought
// online: they have never met a consultant or heard the word "calibration", and
// nobody set one up with them. So every sentence written for a consultant-led
// run that speaks of the calibration, or of what a person agreed, is said here
// for what it is on this run: what we read from their Salesforce settings, or
// what they haven't told us yet. Each rewrite is fixed words of ours, carried
// through a sentence's org-word marks as the fix prompt carries them (672), so
// a rewrite that would cut the org's words leaves the sentence unknown. Nothing
// here runs on a consultant-led run.

/** Each rewrite, in order. Every one is the whole of a sentence or of a clause the engine writes. */
export const AUTOMATIC_REWRITES: ReadonlyArray<readonly [RegExp, string]> = [
  [/The calibration has no cost for it, and there's no list price: /g, "You haven't told us what you pay for it, and there's no list price we can use: "],
  [/The calibration has no cost for: /g, "You haven't told us what you pay for: "],
  [/The calibration has no price for a seat of: /g, "You haven't told us what you pay for a seat of: "],
  [/The calibration has no time per ([a-z ]+) \([a-z0-9-]+\), and we won't guess one\./g, "You haven't told us how long one $1 takes your team, and we won't guess."],
  [/The calibration doesn't say where these open stages come in the sales order: /g, "You haven't told us where these open stages come in your sales order: "],
  [/The calibration doesn't say what these (stages|[a-z ]+) mean: /g, "You haven't told us what these $1 mean: "],
  [/The calibration doesn't say whether these stages are open: /g, "You haven't told us whether these stages are open: "],
  [/The calibration doesn't name a loss reason field that exists on Opportunity, so we can't tell a blank reason from a missing field\./g, "You haven't told us which field on Opportunity holds the loss reason, so we can't tell a blank reason from a missing field."],
  [/No targets were given in the calibration\. Tell us your targets to see pipeline coverage\./g, "You haven't told us your sales targets yet. If you do, your next health check can show pipeline coverage."],
  [/No targets were given in the calibration\./g, "You haven't told us your sales targets yet."],
  [/The calibration needs at least two ordered open stages\./g, "We need at least two open stages in a known order."],
  [/The org's fiscal year couldn't be read, and the calibration doesn't set one\./g, "Your fiscal year couldn't be read from Salesforce, and you haven't told us when it starts."],
  [/, and the calibration marks none as required/g, ", and you haven't told us of any stage every deal must pass through"],
  [/The expected stages are the ones the calibration marks as required, or else those most won deals passed through\./g, "The expected stages are those most won deals passed through."],
  [/Stages the calibration doesn't cover/g, "Stages you haven't told us about"],
  [/No account is calibrated as an integration account, so we can't tell which accounts integrations run on\./g, "You haven't told us which accounts your integrations run on, so we can't tell."],
  [/, unless the calibration gives them a price/g, ""],
  [/Seats paid for times a year's cost per seat: the calibration's figure, or Salesforce's list price where it has none\./g, "Seats paid for times a year's cost per seat, at Salesforce's list price."],
  [/, the calibration's dormant period,/g, ", our standard dormant period,"],
  [/, and the calibration prices user licences/g, ", and the prices we hold are for user licences"],
  [/, and the calibration doesn't classify it\./g, ", and nobody has told us what it is."],
  [/Deals in a stage the calibration calls open, today\./g, "Deals in a stage your Salesforce settings mark as open, today."],
  [/Licence types with no active users and no price in the calibration/g, "Licence types with no active users and no price you've given us"],
  [/the calibrated minimum/g, "our minimum"],
  [/ outside the admin profile and the accounts calibrated as admin or system/g, " outside the admin profile"],
  [/No account is calibrated as an integration account or set to API only, so none holds a licence it doesn't need/g, "No account is set to API only, and you haven't told us of any integration accounts, so none holds a licence it doesn't need"],
  [/Integration accounts: calibrated as one, or set to API only/g, "Integration accounts: set to API only"],
  [/Calibrated dormant period of /g, "Our standard dormant period of "],
  [/Accounts calibrated as integration accounts/g, "Accounts you've told us are integration accounts"],
];

/** Whole texts that are untrue on such a run and are left out: nothing marks any account, so none is excluded as marked. */
export const AUTOMATIC_DROPPED: ReadonlySet<string> = new Set(["Shared, system, integration and test accounts, as the calibration marks them", "Accounts calibrated as shared, system, integration or test"]);

/** The text said for an automatic run, with its marks carried through, or unknown. */
export function rewordMarked(m: Marked): Marked {
  return AUTOMATIC_REWRITES.reduce((out, [pattern, by]) => (new RegExp(pattern.source).test(out.text) ? replaceMarked(out, new RegExp(pattern.source, "g"), (match, ...groups) => match.replace(new RegExp(pattern.source), by.replace(/\$(\d)/g, (_x, n: string) => String(groups[Number(n) - 1] ?? ""))), true) : out), m);
}

export const rewordText = (text: string): string => rewordMarked({ text, spans: [] }).text;

/** Every string in the value, reworded, and every dropped one taken out of the list it's in. Marks and hashes are left for the caller. */
function rewordDeep<T>(value: T, key = ""): T {
  if (key === "orgWords") return value;
  if (typeof value === "string") return rewordText(value) as T;
  if (Array.isArray(value)) return value.filter((v) => typeof v !== "string" || !AUTOMATIC_DROPPED.has(v)).map((v) => rewordDeep(v)) as T;
  if (value && typeof value === "object") return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, rewordDeep(v, k)])) as T;
  return value;
}

/** A sentence's marks after the rewrites, or null when they can't be carried. */
function remark(before: string, after: string, marks: MarkedSentence | undefined | null): MarkedSentence | null {
  const spans = marks ? spansIfCurrent(before, marks) : null;
  if (!spans) return null;
  if (before === after) return marks!;
  const out = rewordMarked({ text: before, spans });
  return out.spans && out.text === after ? { spans: out.spans, textSha256: textSha256(after) } : null;
}

/** A finding as an automatic run says it, its marks carried through each rewrite. */
export function rewordFinding(f: Finding): Finding {
  const out = rewordDeep(f);
  if (!f.orgWords) return out;
  const marks: NonNullable<Finding["orgWords"]> = { ...f.orgWords };
  for (const key of ["title", "summary", "recommendation", "fixPrompt"] as const) {
    const [was, now] = [f[key] ?? undefined, out[key] ?? undefined];
    if (typeof was !== "string" || typeof now !== "string" || !marks[key]) continue;
    const m = remark(was, now, marks[key]);
    if (m) marks[key] = m;
    else delete marks[key];
  }
  if (f.notMeasurableReason && out.notMeasurableReason && marks.notMeasurableReason) {
    const m = remark(f.notMeasurableReason, out.notMeasurableReason, marks.notMeasurableReason);
    if (m) marks.notMeasurableReason = m;
    else delete marks.notMeasurableReason;
  }
  if (f.value.kind === "not_sized" && out.value.kind === "not_sized" && marks.valueReason) {
    const m = remark(f.value.reason, out.value.reason, marks.valueReason);
    if (m) marks.valueReason = m;
    else delete marks.valueReason;
  }
  if (f.value.kind !== "not_sized" && out.value.kind !== "not_sized" && marks.valueAssumptions) {
    const was = f.value.assumptions;
    const now = out.value.assumptions;
    const each = was.length === now.length ? was.map((a, i) => remark(a, now[i]!, marks.valueAssumptions![i])) : [];
    if (each.length > 0 && each.every((m) => m !== null)) marks.valueAssumptions = each as MarkedSentence[];
    else delete marks.valueAssumptions;
  }
  return { ...out, orgWords: marks };
}

/**
 * A metric text's marks: from `said`, where it made the text in this analysis;
 * none of the org's, where the engine wrote the whole text, or a template of it
 * with only figures put in (src/digest/engine-texts.ts); else unknown.
 */
export function metricMarks(text: string): MarkedSentence | null {
  const spans = orgWordsOf(text) ?? (ENGINE_TEXT_KEYS.has(engineTextKey(text)) ? [] : null);
  return spans ? { spans: spans.map((s) => ({ ...s })), textSha256: textSha256(text) } : null;
}

/** A metric's texts marked as built, before anything changes them. */
export function marksOfMetric(m: Metric): MetricOrgWords {
  const one = (t: string): MarkedSentence | undefined => metricMarks(t) ?? undefined;
  return {
    ...(one(m.label) ? { label: one(m.label) } : {}),
    ...(one(m.definition) ? { definition: one(m.definition) } : {}),
    ...(m.status === "not_measurable" && one(m.reason) ? { reason: one(m.reason) } : {}),
    assumptions: m.assumptions.map(metricMarks),
    exclusions: m.exclusions.map(metricMarks),
  };
}

/** A metric as an automatic run says it, its marks carried through each rewrite and a dropped text taken out with its mark. */
export function rewordMetric(m: Metric, marks: MetricOrgWords): Metric {
  const keep = (texts: string[], held: Array<MarkedSentence | null> | undefined) => {
    const pairs = texts.map((t, i) => [t, held?.[i] ?? null] as const).filter(([t]) => !AUTOMATIC_DROPPED.has(t));
    return { texts: pairs.map(([t]) => rewordText(t)), marks: pairs.map(([t, mk]) => remark(t, rewordText(t), mk)) };
  };
  const a = keep(m.assumptions, marks.assumptions);
  const e = keep(m.exclusions, marks.exclusions);
  const label = rewordText(m.label);
  const definition = rewordText(m.definition);
  const orgWords: MetricOrgWords = {
    ...(marks.label && remark(m.label, label, marks.label) ? { label: remark(m.label, label, marks.label)! } : {}),
    ...(marks.definition && remark(m.definition, definition, marks.definition) ? { definition: remark(m.definition, definition, marks.definition)! } : {}),
    assumptions: a.marks,
    exclusions: e.marks,
  };
  if (m.status === "not_measurable") {
    const reason = rewordText(m.reason);
    const r = remark(m.reason, reason, marks.reason);
    return { ...rewordDeep(m), label, definition, assumptions: a.texts, exclusions: e.texts, reason, orgWords: { ...orgWords, ...(r ? { reason: r } : {}) } };
  }
  return { ...rewordDeep(m), label, definition, assumptions: a.texts, exclusions: e.texts, orgWords };
}
