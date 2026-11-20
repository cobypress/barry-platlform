import { FIX_PROMPT_MAX, type Finding, type Subject } from "../schemas/index.ts";
import { teamLevelText } from "../narrative/select.ts";
import { tokenCode, type Pseudonymiser } from "../privacy/pseudonymiser.ts";
import type { PromptRule } from "./types.ts";

// A fix prompt: plain text a customer pastes into an AI assistant that can read
// their own org (DECISIONS.md 628). Code assembles it from the rule's fixed text
// and the finding's own title, summary and recommendation, in one shape for every
// rule. Claude never writes it (rule 1), and it never names a person or a record
// (rule 7): a prompt that might is left off rather than risked.

/** Subjects that are people or records. Never named in a prompt, even where the finding's text shows them. */
const PERSON_OR_RECORD: ReadonlySet<string> = new Set(["user", "rep", "owner", "account", "contact", "lead", "opportunity", "case", "dashboard", "report", "scheduled job"]);

/** Subjects that are set-up the fix works on, so the prompt can list them by name when the finding shows them. */
const LISTABLE: ReadonlySet<string> = new Set(["field", "layout", "object", "picklist", "stage", "record type", "workflow rule", "process", "flow", "apex class", "javascript button", "visualforce page", "profile", "permission set", "connected app", "remote site", "queue", "price rules", "pricing rule", "installed app", "licence", "setting"]);

/** The most items a prompt lists by name. */
const LISTED_ITEMS = 10;

const TOKEN = /\[\[[a-z]+[:\]]/;
const ANY_TOKEN = new RegExp(String.raw`\[\[[a-z]+(?::${tokenCode()})?\]\]`, "g");
const EMAIL = /@/;
const SALESFORCE_ID = /\b(?=[a-zA-Z0-9]*\d)[a-zA-Z0-9]{15}(?:[a-zA-Z0-9]{3})?\b/;

const escapeRegex = (value: string): string => value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
const wholeWord = (label: string): RegExp => new RegExp(`(?<![\\w])${escapeRegex(label)}(?![\\w])`, "g");

export type PromptFinding = Pick<Finding, "title" | "summary" | "recommendation" | "subjects">;

/** A subject's label the prompt may not hold: a person or a record, a token, or set-up the finding's title and summary don't show. */
export function withheldLabels(f: PromptFinding): string[] {
  const shown = `${f.title}\n${f.summary}`;
  return f.subjects.filter((s) => PERSON_OR_RECORD.has(s.type) || TOKEN.test(s.label) || !shown.includes(s.label)).map((s) => s.label);
}

/** People as the executive report says them, any other token as a record, and each withheld subject by what it is. */
function neutral(text: string, withheld: readonly Subject[]): string {
  let out = teamLevelText(text).replace(ANY_TOKEN, "a record listed in this finding");
  for (const s of [...withheld].sort((a, b) => b.label.length - a.label.length)) {
    if (TOKEN.test(s.label)) continue;
    out = out.replace(wholeWord(s.label), `the ${s.type} listed in this finding`);
  }
  return out;
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
  return nameable ? `The ${items} involved: ${listed(names)}.` : `Work on the ${items} listed in this finding.`;
}

/** The task when a guard held back a remove, deactivate or merge: a review, never the change. */
const BLOCKED_STEPS: readonly string[] = [
  "Not every safety check passed, so do not remove, deactivate or merge anything: this task is a review.",
  "For each item, list everything that depends on it or that it depends on: records, flows, Apex, validation rules, page layouts, reports, list views, sharing, assignment and approval rules, scheduled jobs and integrations.",
  "Note which checks you couldn't complete, and what a person would have to confirm.",
  "Bring the list to the people who own these items, so they can decide what happens to each.",
];
const BLOCKED_VERIFY: readonly string[] = ["Each item has a list of what depends on it, and its owner's decision."];

export type PromptOptions = {
  /** A guard held back a remove, deactivate or merge, so the finding now says to investigate. */
  blocked?: boolean;
};

/** The prompt's text, before the guard. */
export function assemblePrompt(f: PromptFinding, rule: PromptRule, options: PromptOptions = {}): string {
  const withheldSet = new Set(withheldLabels(f));
  const withheld = f.subjects.filter((s) => withheldSet.has(s.label));
  // The title and summary are shown to the same reader already; only people and records come out of them.
  const personal = f.subjects.filter((s) => PERSON_OR_RECORD.has(s.type));
  const title = neutral(f.title, personal);
  const summary = neutral(f.summary, personal);
  const good = neutral(f.recommendation, withheld);
  // A guard held back the remove, deactivate or merge, so the rule's own steps,
  // which end in one, give way to a review that ends with the owners.
  const steps = options.blocked ? BLOCKED_STEPS : rule.steps;
  const verify = options.blocked ? BLOCKED_VERIFY : rule.verify;
  const dont = [
    ...(rule.decision ? ["Change any setting, metadata or record in the org: this task is analysis only."] : []),
    "Delete or deactivate anything until you have checked what depends on it: flows, Apex, validation rules, page layouts, reports, list views, sharing rules and integrations.",
    "Make a change in production first: work in a sandbox, test it, then deploy.",
    `Change anything outside the ${rule.items} this finding is about.`,
    "Put anyone's name, email or record details in what you write back.",
  ];
  return [
    "You are helping a Salesforce administrator act on one finding from a Black Cloud Barometer health check of their org. You can read the org's metadata. Work only from what you can see, and say where you can't confirm something.",
    `What we found: ${title}. ${summary}`,
    `What good looks like: ${good}`,
    itemsLine(f, rule.items, withheldSet),
    ...(rule.decision ? ["This is a decision for people, not a change to the org. Do not change the org: prepare the analysis and the options so the people who own it can decide."] : []),
    `Steps:\n${steps.map((s, i) => `${i + 1}. ${s}`).join("\n")}`,
    `Do not:\n${dont.map((d) => `- ${d}`).join("\n")}`,
    `To check it worked:\n${verify.map((v) => `- ${v}`).join("\n")}`,
  ].join("\n\n");
}

/**
 * Rule 7's guard, as the scan's fix guard is: true when the text may go out. A
 * token, an email, anything shaped like a Salesforce id, any name, username,
 * email or id the run knows, or a withheld subject's label fails it, as does
 * text over the length limit.
 */
export function promptIsSafe(text: string, f: PromptFinding, pseudonyms: Pseudonymiser): boolean {
  if (text.length > FIX_PROMPT_MAX) return false;
  if (TOKEN.test(text) || EMAIL.test(text) || SALESFORCE_ID.test(text)) return false;
  if (pseudonyms.scrub(text) !== text) return false;
  return !withheldLabels(f).some((label) => wholeWord(label).test(text));
}

/** The finding's fix prompt, or null when the guard won't let it out. */
export function fixPrompt(f: PromptFinding, rule: PromptRule, pseudonyms: Pseudonymiser, options: PromptOptions = {}): string | null {
  const text = assemblePrompt(f, rule, options);
  return promptIsSafe(text, f, pseudonyms) ? text : null;
}
