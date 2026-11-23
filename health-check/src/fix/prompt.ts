import { DESTRUCTIVE_RECOMMENDATIONS, FIX_PROMPT_MAX, type Finding, type Subject } from "../schemas/index.ts";
import { holdsNameOrSecret } from "../narrative/guard.ts";
import { teamLevelSteps } from "../narrative/select.ts";
import { spansIfCurrent, type OrgSpan } from "../engine/org-words.ts";
import { cat, joinMarked, ours, replaceMarked, sliceMarked, theirs, unknown, type Marked } from "./marked.ts";
import { ORDINARY, PERSON_KINDS, SHARED_NAME_PREFIX } from "../privacy/names.ts";
import { isSalesforceUserName, tokenCode, UNKNOWN_USER, type Pseudonymiser } from "../privacy/pseudonymiser.ts";
import type { PromptRule } from "./types.ts";

// A fix prompt: plain text a customer pastes into an AI assistant that can read
// their own org (DECISIONS.md 628). Code assembles it from the rule's fixed text
// and the finding's own title, summary and recommendation, in one shape for every
// rule. Claude never writes it (rule 1), and it never names a person or a record
// (rule 7): a prompt that might is left off rather than risked.

/** Subjects that are people or records. Never named in a prompt, even where the finding's text shows them. */
const PERSON_OR_RECORD: ReadonlySet<string> = new Set(["user", "rep", "owner", "account", "contact", "lead", "opportunity", "case", "dashboard", "report", "scheduled job", "price rules", "pricing rule", "product rule", "quote", "contract", "article", "macro"]);

/** Subjects that are set-up the fix works on, so the prompt can list them by name when the finding shows them. */
const LISTABLE: ReadonlySet<string> = new Set(["field", "layout", "object", "picklist", "stage", "record type", "workflow rule", "process", "flow", "apex class", "javascript button", "visualforce page", "profile", "permission set", "connected app", "remote site", "queue", "installed app", "licence", "setting"]);

/** The most items a prompt lists by name. */
const LISTED_ITEMS = 10;

const TOKEN = /\[\[[a-z]+[:\]]/;
const ANY_TOKEN = new RegExp(String.raw`\[\[[a-z]+(?::${tokenCode()})?\]\]`, "g");
const WHOLE_TOKEN = new RegExp(String.raw`^\[\[[a-z]+:${tokenCode()}\]\]$`);
const EMAIL = /@/;
const SALESFORCE_ID = /\b(?=[a-zA-Z0-9]*\d)[a-zA-Z0-9]{15}(?:[a-zA-Z0-9]{3})?\b/;

const escapeRegex = (value: string): string => value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
const wholeWord = (label: string): RegExp => new RegExp(`(?<![\\w])${escapeRegex(label)}(?![\\w])`, "g");

export type PromptFinding = Pick<Finding, "title" | "summary" | "recommendation" | "subjects"> & Partial<Pick<Finding, "recommendationType" | "orgWords">>;

/**
 * A subject the prompt may never name, even where the finding shows it: a person
 * or a record, a token, or a label holding a part of a person's name ("Sam's
 * Escalations"), which the same check that guards Claude's prompts finds.
 */
const unnameable = (s: Subject, pseudonyms?: Pseudonymiser): boolean => PERSON_OR_RECORD.has(s.type) || TOKEN.test(s.label) || (pseudonyms !== undefined && (holdsName(s.label, pseudonyms) || labelHoldsPersonName(s.label, pseudonyms)));

/**
 * The shortest word a set-up label is matched on, in letters. Shorter name parts
 * ("Jo", "Al") would swallow ordinary abbreviations in API names, so they're left
 * to the guard, which sees them only with a capital.
 */
export const MIN_NAME_PART = 3;

const foldLower = (word: string): string => word.normalize("NFKD").replace(/\p{M}/gu, "").toLowerCase();

/** A label's words, split on case changes, digits, underscores, hyphens, spaces and apostrophes of either kind. */
export const labelWords = (label: string): string[] =>
  label
    .replace(/(\p{Ll})(\p{Lu})/gu, "$1 $2")
    .replace(/(\p{Lu})(\p{Lu}\p{Ll})/gu, "$1 $2")
    .split(/[^\p{L}\p{M}]+/u)
    .filter(Boolean)
    .map(foldLower);

/**
 * The people a run knows, each name's words folded to lower case: users, contacts
 * and leads, but not shared or system users, whose names are ordinary words
 * ("Shared Sales Desk", "Integration User").
 */
export function knownPeople(pseudonyms: Pseudonymiser): string[][] {
  const out: string[][] = [];
  for (const entry of pseudonyms.entries()) {
    const kind = entry.token.slice(2, entry.token.indexOf(":"));
    if (!PERSON_KINDS.has(kind) || entry.id.startsWith(SHARED_NAME_PREFIX) || entry.name === UNKNOWN_USER || isSalesforceUserName(entry.name)) continue;
    const parts = entry.name.split(/[^\p{L}\p{M}]+/u).filter(Boolean).map(foldLower);
    if (parts.length > 0 && !parts.every((p) => ORDINARY.has(p))) out.push(parts);
  }
  return out;
}

const FORMS = new WeakMap<Pseudonymiser, { size: number; forms: Set<string> }>();

/**
 * Every form of a known person's name an admin might put in an API name: each
 * first name and surname, with or without a trailing "s", initial and surname
 * ("sreed"), and first name and initial ("samr"). Shared and system users' names
 * ("Shared Sales Desk", "Integration User") are ordinary words, not people's.
 */
function nameForms(pseudonyms: Pseudonymiser): Set<string> {
  const entries = pseudonyms.entries();
  const hit = FORMS.get(pseudonyms);
  if (hit && hit.size === entries.length) return hit.forms;
  const forms = new Set<string>();
  const add = (form: string): void => void (form.length >= MIN_NAME_PART && forms.add(form));
  for (const entry of entries) {
    const kind = entry.token.slice(2, entry.token.indexOf(":"));
    if (!PERSON_KINDS.has(kind) || entry.id.startsWith(SHARED_NAME_PREFIX) || entry.name === UNKNOWN_USER || isSalesforceUserName(entry.name)) continue;
    const parts = entry.name.split(/[^\p{L}\p{M}]+/u).filter(Boolean).map(foldLower);
    for (const part of parts) {
      if (ORDINARY.has(part) || part.length < MIN_NAME_PART) continue;
      add(part);
      add(`${part}s`);
    }
    if (parts.length >= 2) {
      const [first, last] = [parts[0]!, parts.at(-1)!];
      if (!ORDINARY.has(last)) add(`${first[0]}${last}`);
      if (!ORDINARY.has(first)) add(`${first}${last[0]}`);
    }
  }
  FORMS.set(pseudonyms, { size: entries.length, forms });
  return forms;
}

/** True when any word of a set-up label is a form of a known person's name. A false alarm only costs the name. */
export function labelHoldsPersonName(label: string, pseudonyms: Pseudonymiser): boolean {
  const forms = nameForms(pseudonyms);
  return labelWords(label).some((w) => w.length >= MIN_NAME_PART && forms.has(w));
}

/** Words run together in an API name, apart: "ReedExportJob" as "Reed Export Job", so a name inside one is seen. */
const camelApart = (text: string): string => text.replace(/(\p{Ll})(\p{Lu})/gu, "$1 $2").replace(/(\p{Lu})(\p{Lu}\p{Ll})/gu, "$1 $2");

/** The check before anything goes to Claude, on the text as written and with run-together words apart. */
const holdsName = (text: string, pseudonyms: Pseudonymiser): boolean => holdsNameOrSecret(text, pseudonyms) || holdsNameOrSecret(camelApart(text), pseudonyms);

/** A subject's label the prompt may not hold: one it may never name, or set-up the finding's title and summary don't show. */
export function withheldLabels(f: PromptFinding, pseudonyms?: Pseudonymiser): string[] {
  const shown = `${f.title}\n${f.summary}`;
  return f.subjects.filter((s) => unnameable(s, pseudonyms) || !shown.includes(s.label)).map((s) => s.label);
}

/**
 * People as the executive report says them, any other token as a record, and
 * each withheld subject by what it is. Each rewrite says fixed words of ours, so
 * the org's characters it takes out go and its words are the engine's; one that
 * would cut an org value leaves the text unknown (DECISIONS.md 672).
 */
function neutral(text: Marked, withheld: readonly Subject[]): Marked {
  let out = text;
  // Labels first, longest first, so a label holding a token goes whole.
  // A label that is a token alone is left to teamLevelText, which says it as the executive report does.
  for (const s of [...withheld].sort((a, b) => b.label.length - a.label.length)) {
    if (WHOLE_TOKEN.test(s.label)) continue;
    out = replaceMarked(out, wholeWord(s.label), cat(ours("the "), subjectType(s.type), ours(" listed in this finding")), true);
  }
  for (const [pattern, by, fixed] of teamLevelSteps()) out = replaceMarked(out, pattern, by, fixed);
  out = replaceMarked(out, ANY_TOKEN, "a record listed in this finding", true);
  return replaceMarked(out, /\b([Tt])he the\b/g, (_m, t) => `${t}he`, false);
}

/**
 * The subject types the engine writes of its own: the literal types rules set,
 * and the segment dimensions named in their code. A rule sets the type in code
 * today, but nothing in the types holds it there (a dimension such as
 * historic pipeline's "from <stage>" is built from a stage), so any other type
 * is said as the org's words, the safe side (DECISIONS.md 672).
 */
export const ENGINE_SUBJECT_TYPES: ReadonlySet<string> = new Set([
  ...PERSON_OR_RECORD, ...LISTABLE,
  "allowance", "campaign type", "closed quarter", "dashboard", "group", "how it's granted", "installed app", "lead source", "origin", "owner type",
  "priority", "rep", "s-control", "source", "storage", "team", "type",
]);

const subjectType = (type: string): Marked => (ENGINE_SUBJECT_TYPES.has(type) ? ours(type) : theirs(type));

const WORD = /[\p{L}\p{N}_]/u;
/** Whether `at` in `text` is a word's edge or an org span's. */
const edgeAt = (m: Marked, at: number): boolean => at === 0 || at === m.text.length || !(WORD.test(m.text[at - 1]!) && WORD.test(m.text[at]!)) || m.spans!.some((s) => s.start === at || s.end === at);

/**
 * A subject's label as the items line lists it. Every label there is the org's
 * (DECISIONS.md 672), so it is never listed without an org span in it: one
 * span, whole, unless the title or summary shows it with finer marks, a label a
 * rule put together ("Lead Source on Lead", " on " the rule's). Those are used
 * only from a place that starts and ends on a word's edge or a span's and holds
 * at least one org span; where such places disagree, the label is unknown.
 */
function labelMarks(label: string, shown: readonly Marked[]): Marked {
  let found: OrgSpan[] | null = null;
  for (const m of shown) {
    if (!m.spans) return unknown(label);
    for (let at = m.text.indexOf(label); at !== -1; at = m.text.indexOf(label, at + 1)) {
      const spans = sliceMarked(m, at, at + label.length).spans!;
      if (spans.length === 0 || !edgeAt(m, at) || !edgeAt(m, at + label.length)) continue;
      if (found === null) found = spans;
      else if (found.length !== spans.length || found.some((s, i) => s.start !== spans[i]!.start || s.end !== spans[i]!.end)) return unknown(label);
    }
  }
  return found === null ? theirs(label) : { text: label, spans: found };
}

/** Names in a list, "a, b and 3 more": each name as its sentence marks it, the joining words ours. */
const listed = (names: string[], shown: readonly Marked[]): Marked => {
  const shownNames = names.slice(0, LISTED_ITEMS).map((n) => labelMarks(n, shown));
  const more = names.length - shownNames.length;
  if (more > 0) return cat(joinMarked(shownNames, ", "), ours(` and ${more} more`));
  return shownNames.length === 1 ? shownNames[0]! : cat(joinMarked(shownNames.slice(0, -1), ", "), ours(" and "), shownNames.at(-1)!);
};

function itemsLine(f: PromptFinding, items: string, withheld: ReadonlySet<string>, shown: readonly Marked[]): Marked {
  if (f.subjects.length === 0) return ours(`Work on the ${items} this finding describes.`);
  const names = [...new Set(f.subjects.map((s) => s.label))];
  const nameable = f.subjects.every((s) => LISTABLE.has(s.type) && !withheld.has(s.label));
  return nameable ? cat(ours(`Work on the ${items} this finding is about. Named in it: `), listed(names, shown), ours(".")) : ours(`Work on the ${items} listed in this finding.`);
}

/** One of the finding's sentences with the marks `said` made for it, while it still reads as it did; otherwise unknown. */
function sentence(f: PromptFinding, key: "title" | "summary" | "recommendation"): Marked {
  const spans = spansIfCurrent(f[key], f.orgWords?.[key]);
  return spans ? { text: f[key], spans } : unknown(f[key]);
}

/**
 * A summary cut to its first `keep` sentences, joined by one space as before.
 * A joining space that replaces whitespace inside an org value can't be said to
 * be either's, so the summary is then unknown.
 */
function firstSentences(summary: Marked, keep: number): Marked {
  const parts: Marked[] = [];
  let known = summary.spans !== null;
  let at = 0;
  for (const gap of summary.text.matchAll(/(?<=\.)\s+/g)) {
    if (parts.length === keep) break;
    parts.push(sliceMarked(summary, at, gap.index));
    if (parts.length < keep && summary.spans?.some((s) => s.start < gap.index + gap[0].length && s.end > gap.index)) known = false;
    at = gap.index + gap[0].length;
  }
  if (parts.length < keep) parts.push(sliceMarked(summary, at, summary.text.length));
  const joined = joinMarked(parts, " ");
  return known ? joined : unknown(joined.text);
}

/**
 * How the finding's recommendation is introduced, in every prompt: it is the
 * report's advice to the people who own the org, often a record change or an
 * order the steps reverse, and never the assistant's job.
 */
export const FRAMING = "What our report recommends, for the people who own this to decide and carry out. It is not your task: your task is only the steps below.";

/** The task when a rule that can remove, deactivate or merge is held back: a review that ends with the owners. */
const REVIEW_STEPS: readonly string[] = [
  "This task is a review for the people who own these items: not every safety check passed, or the change is theirs to make.",
  "For each item, list everything that depends on it or that it depends on: records, flows, Apex, validation rules, page layouts, reports, list views, sharing, assignment and approval rules, scheduled jobs and integrations.",
  "Note which checks you couldn't complete, and what a person would have to confirm.",
  "Bring the list to the people who own these items, so they can decide what happens to each.",
];
const REVIEW_VERIFY: readonly string[] = ["Each item has a list of what depends on it, and its owner's decision."];

/**
 * Whether a rule that can remove, deactivate or merge gives its review rather
 * than its own steps: whenever its finding isn't the destructive recommendation,
 * because a guard held it back or the rule itself says to investigate.
 */
export const isHeldBack = (f: PromptFinding, rule: PromptRule): boolean => rule.removes === true && (f.recommendationType === undefined || !DESTRUCTIVE_RECOMMENDATIONS.has(f.recommendationType));

/** The prompt's text, before the guard. */
export function assemblePrompt(f: PromptFinding, rule: PromptRule, pseudonyms?: Pseudonymiser): string {
  return assembleMarked(f, rule, pseudonyms).text;
}

/**
 * The prompt with the characters the org chose marked (DECISIONS.md 672), before
 * the guard. The text is assemblePrompt's; the spans are null whenever any part
 * can't be vouched for, so the prompt is then unmarked, never wrongly marked.
 */
export function assembleMarked(f: PromptFinding, rule: PromptRule, pseudonyms?: Pseudonymiser): Marked {
  const withheldSet = new Set(withheldLabels(f, pseudonyms));
  const withheld = f.subjects.filter((s) => withheldSet.has(s.label));
  // The title and summary are shown to the same reader already; only what may never be named comes out of them.
  const never = f.subjects.filter((s) => unnameable(s, pseudonyms));
  const title = neutral(sentence(f, "title"), never);
  const fullSummary = neutral(sentence(f, "summary"), never);
  const good = neutral(sentence(f, "recommendation"), withheld);
  const heldBack = isHeldBack(f, rule);
  const analysis = rule.decision === true || heldBack;
  const steps = heldBack ? (rule.review?.steps ?? REVIEW_STEPS) : rule.steps;
  const verify = heldBack ? (rule.review?.verify ?? REVIEW_VERIFY) : rule.verify;
  const dont = [
    ...(analysis ? ["Change any setting, metadata or record: this task is analysis only."] : []),
    ...(heldBack ? ["Remove, deactivate or merge anything: this task is a review."] : []),
    "Change a record (an owner, a field's value, a user) or delete anything.",
    "Switch anything off before checking what depends on it: flows, Apex, validation rules, layouts, reports, sharing and integrations.",
    "Change production first: use a sandbox, test, then deploy, and only make changes that can be undone.",
    "Take away access, or stop an integration, before its replacement is in place and tested.",
    `Change anything outside the ${rule.items} this finding is about.`,
    "Put a name, email or record detail in your reply.",
  ];
  // Everything but the finding's sentences and the subjects' labels is the engine's: the rule's fixed text.
  const build = (summary: Marked): Marked => joinMarked([
    ours("You are helping a Salesforce administrator act on one finding from a Black Cloud Barometer health check of their org. You can read the org's metadata. Work only from what you can see, and say where you can't confirm something."),
    cat(ours("What we found: "), title, ours(". "), summary),
    // The recommendation is the finding's own, worded for the people who own the org; only the steps are the assistant's.
    cat(ours(`${FRAMING}\nThe report says: `), good),
    itemsLine(f, rule.items, withheldSet, [title, fullSummary]),
    ...(analysis ? [ours("This is for people to decide, not a change to the org. Do not change the org: prepare the analysis, the list and the options so the people who own it can decide.")] : []),
    ours(`Steps:\n${steps.map((s, i) => `${i + 1}. ${s}`).join("\n")}`),
    ours(`Do not:\n${dont.map((d) => `- ${d}`).join("\n")}`),
    ours(`To check it worked:\n${verify.map((v) => `- ${v}`).join("\n")}`),
  ], "\n\n");
  // A summary too long for the limit loses whole sentences from its end, and says so.
  let prompt = build(fullSummary);
  const sentences = fullSummary.text.split(/(?<=\.)\s+/);
  for (let keep = sentences.length - 1; prompt.text.length > FIX_PROMPT_MAX && keep >= 1; keep--) prompt = build(cat(firstSentences(fullSummary, keep), ours(" (The finding says more.)")));
  return prompt;
}

/**
 * Rule 7's guard, at least as strict as the one before anything goes to Claude:
 * true when the text may go out. A token, an email, anything shaped like a
 * Salesforce id, any name, username, email or id the run knows or any part of
 * one, a secret, or a withheld subject's label fails it, as does text over the
 * length limit.
 */
export function promptIsSafe(text: string, f: PromptFinding, pseudonyms: Pseudonymiser): boolean {
  if (text.length > FIX_PROMPT_MAX) return false;
  if (TOKEN.test(text) || EMAIL.test(text) || SALESFORCE_ID.test(text)) return false;
  if (pseudonyms.scrub(text) !== text) return false;
  if (holdsName(text, pseudonyms)) return false;
  return !withheldLabels(f, pseudonyms).some((label) => wholeWord(label).test(text));
}

/** The finding's fix prompt, or null when the guard won't let it out. */
export function fixPrompt(f: PromptFinding, rule: PromptRule, pseudonyms: Pseudonymiser): string | null {
  return markedFixPrompt(f, rule, pseudonyms)?.text ?? null;
}

/** The finding's fix prompt with its org words marked, or null spans when they can't be vouched for (DECISIONS.md 672); null when the guard won't let it out. */
export function markedFixPrompt(f: PromptFinding, rule: PromptRule, pseudonyms: Pseudonymiser): { text: string; spans: OrgSpan[] | null } | null {
  const prompt = assembleMarked(f, rule, pseudonyms);
  return promptIsSafe(prompt.text, f, pseudonyms) ? prompt : null;
}
