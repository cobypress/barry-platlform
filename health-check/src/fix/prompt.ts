import { DESTRUCTIVE_RECOMMENDATIONS, FIX_PROMPT_MAX, type Finding, type Subject } from "../schemas/index.ts";
import { holdsNameOrSecret } from "../narrative/guard.ts";
import { teamLevelText } from "../narrative/select.ts";
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

export type PromptFinding = Pick<Finding, "title" | "summary" | "recommendation" | "subjects"> & Partial<Pick<Finding, "recommendationType">>;

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

/** People as the executive report says them, any other token as a record, and each withheld subject by what it is. */
function neutral(text: string, withheld: readonly Subject[]): string {
  let out = text;
  // Labels first, longest first, so a label holding a token goes whole.
  // A label that is a token alone is left to teamLevelText, which says it as the executive report does.
  for (const s of [...withheld].sort((a, b) => b.label.length - a.label.length)) {
    if (WHOLE_TOKEN.test(s.label)) continue;
    out = out.replace(wholeWord(s.label), `the ${s.type} listed in this finding`);
  }
  return teamLevelText(out).replace(ANY_TOKEN, "a record listed in this finding").replace(/\b([Tt])he the\b/g, "$1he");
}

const listed = (names: string[]): string => {
  const shown = names.slice(0, LISTED_ITEMS);
  const more = names.length - shown.length;
  if (more > 0) return `${shown.join(", ")} and ${more} more`;
  return shown.length === 1 ? shown[0]! : `${shown.slice(0, -1).join(", ")} and ${shown.at(-1)}`;
};

function itemsLine(f: PromptFinding, items: string, withheld: ReadonlySet<string>): string {
  if (f.subjects.length === 0) return `Work on the ${items} this finding describes.`;
  const names = [...new Set(f.subjects.map((s) => s.label))];
  const nameable = f.subjects.every((s) => LISTABLE.has(s.type) && !withheld.has(s.label));
  return nameable ? `Work on the ${items} this finding is about. Named in it: ${listed(names)}.` : `Work on the ${items} listed in this finding.`;
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
  const withheldSet = new Set(withheldLabels(f, pseudonyms));
  const withheld = f.subjects.filter((s) => withheldSet.has(s.label));
  // The title and summary are shown to the same reader already; only what may never be named comes out of them.
  const never = f.subjects.filter((s) => unnameable(s, pseudonyms));
  const title = neutral(f.title, never);
  const fullSummary = neutral(f.summary, never);
  const good = neutral(f.recommendation, withheld);
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
  const build = (summary: string): string => [
    "You are helping a Salesforce administrator act on one finding from a Black Cloud Barometer health check of their org. You can read the org's metadata. Work only from what you can see, and say where you can't confirm something.",
    `What we found: ${title}. ${summary}`,
    // The recommendation is the finding's own, worded for the people who own the org; only the steps are the assistant's.
    `${FRAMING}\nThe report says: ${good}`,
    itemsLine(f, rule.items, withheldSet),
    ...(analysis ? ["This is for people to decide, not a change to the org. Do not change the org: prepare the analysis, the list and the options so the people who own it can decide."] : []),
    `Steps:\n${steps.map((s, i) => `${i + 1}. ${s}`).join("\n")}`,
    `Do not:\n${dont.map((d) => `- ${d}`).join("\n")}`,
    `To check it worked:\n${verify.map((v) => `- ${v}`).join("\n")}`,
  ].join("\n\n");
  // A summary too long for the limit loses whole sentences from its end, and says so.
  let text = build(fullSummary);
  const sentences = fullSummary.split(/(?<=\.)\s+/);
  for (let keep = sentences.length - 1; text.length > FIX_PROMPT_MAX && keep >= 1; keep--) text = build(`${sentences.slice(0, keep).join(" ")} (The finding says more.)`);
  return text;
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
  const text = assemblePrompt(f, rule, pseudonyms);
  return promptIsSafe(text, f, pseudonyms) ? text : null;
}
