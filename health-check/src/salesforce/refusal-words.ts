// What a customer reads when Salesforce refused a read (DECISIONS.md 702),
// every sentence in one place, and how the engine knows one again when a rule
// passes it on. Salesforce's own message and code never reach these: they go
// to run.json's readsRefused and the run's log, for us alone.

/**
 * Why a read cost its checks, by the class the client's classifier gives it.
 * `unanswered`: Salesforce didn't answer the read as it should (it timed out,
 * was down, or sent something we couldn't read), which is no refusal, so it
 * is never said as one.
 */
export type RefusalKind = "not_available" | "no_permission" | "refused" | "unanswered";

const BECAUSE: Readonly<Record<RefusalKind, string>> = {
  not_available: "it isn't available in this Salesforce edition or isn't switched on",
  no_permission: "the person who connected Salesforce doesn't have permission to see it",
  refused: "Salesforce refused this read",
  unanswered: "Salesforce didn't answer as it should",
};

/**
 * What a read was of, when it isn't one object: the engine's own words. An
 * object's API name is the other kind, and may be one the org chose.
 */
export const READ_NAMES = ["This information", "The org's set-up from the Metadata API", "The org's record counts", "The same set-up through the Tooling API"] as const;

const API_NAME = /^[A-Za-z][A-Za-z0-9_]{0,79}$/;

/**
 * The sentence for a refused read of `what`: an object's API name, or one of
 * READ_NAMES. An object Salesforce says isn't there (INVALID_TYPE) keeps the
 * engine's sentence from before 702, which is ours and quotes nothing of
 * Salesforce's: the code can mean the object isn't in the org or the user
 * can't see it, and the sentence says both.
 */
export function refusalSentence(what: string, kind: RefusalKind, code = ""): string {
  if (code === "INVALID_TYPE" && API_NAME.test(what)) return `${what} isn't available in this org, or to the connected user.`;
  return `${what} couldn't be read, because ${BECAUSE[kind]}.`;
}

/**
 * A check's reason when the run didn't use the Metadata API because the org's
 * edition only opens it to security-reviewed apps (DECISIONS.md 710): short,
 * because it stands on several checks and the run's note says the rest once.
 */
export const METADATA_POINTER = "Needs the Metadata API, which Professional Edition only opens to security-reviewed apps.";

/** The Apex step's reason when a Professional Edition org answers that it has no Apex of its own (DECISIONS.md 710). */
export const APEX_POINTER = "Needs Apex of the org's own, which Professional Edition doesn't allow.";

/** The Apex step's sentence before 710, still read back from runs that hold it. */
export const NO_APEX_IN_EDITION = "Professional Edition has no Apex, so there's no code to check.";

/** The metadata step's reason when the Tooling route failed too, on an org whose edition keeps the Metadata API from us (DECISIONS.md 710). */
export const TOOLING_AFTER_EDITION = "Professional Edition only opens the Metadata API to security-reviewed apps, and reading the same through the Tooling API didn't work either: ";

/** The run's note about its edition (DECISIONS.md 710), said once for the run and on each page that needs it. */
export const EDITION_NOTE = "This org is on Professional Edition, which only opens the Metadata API to security-reviewed apps, so checks that need it are marked not measured.";

/** What a page that prints a check the edition left unmeasured holds, so the note can be put beside it. */
export const EDITION_MARKS: readonly string[] = [METADATA_POINTER, TOOLING_AFTER_EDITION];

/**
 * Whether a not measurable reason says the org's edition is why (DECISIONS.md
 * 710), wherever in it the sentence sits: a rule may put its own words first.
 */
export function editionIsWhy(reason: string): boolean {
  return [METADATA_POINTER, APEX_POINTER, NO_APEX_IN_EDITION, TOOLING_AFTER_EDITION].some((s) => reason.includes(s));
}

/** Whether a not measurable reason says Salesforce didn't answer a read, wherever a rule puts it (DECISIONS.md 715). */
export function unansweredIsWhy(reason: string): boolean {
  return reason.includes(`because ${BECAUSE.unanswered}.`);
}

/** The org's limits, refused at connect. */
export const LIMITS_NO_PERMISSION = "The org's limits weren't measured, because the person who connected Salesforce can't view Setup.";
export const LIMITS_REFUSED = "The org's limits weren't measured, because Salesforce refused the read.";

/** No API for the org, or for the user who connected: the connect's and the run's final answer. */
export const NO_API_FOR_ORG = "This Salesforce org doesn't allow API access, so we can't read it. Some editions need API access added, and some don't offer it.";
export const NO_API_FOR_USER = "The person who connected doesn't have API access in Salesforce, so we can't read the org. They need the API Enabled permission.";

/** When the deals read can't be used, the history read beside it says so. */
export const DEALS_UNREAD = "Opportunity couldn't be read";

/** Too many rows of one read in a shape we couldn't read (DECISIONS.md 702). */
export function tooManyUnfit(bad: number, total: number, what: "deals" | "won deals" | "lost deals" | "open deals" | "entries in the deals' stage history"): string {
  return `${bad.toLocaleString("en-GB")} of the ${total.toLocaleString("en-GB")} ${what} Salesforce gave us weren't in a shape we could read, more than 1 in 100, so figures from them would mislead.`;
}

/** Names in a list as a sentence says them: "A", "A and B", "A, B and C". */
const listOf = (names: readonly string[]): string => (names.length === 1 ? names[0]! : `${names.slice(0, -1).join(", ")} and ${names.at(-1)}`);

/**
 * An object a read needs that the describe says isn't there, with the describe's
 * own reason after it (DECISIONS.md 743): here, beside its pattern, so a rule
 * passing it on is known by its words, the object marked as the org's.
 */
export function objectAbsent(object: string, reason: string): string {
  return `${object} isn't available in this org, or to the connected user: ${reason}`;
}

/** Fields a read needs that the org doesn't have, so the read wasn't run (DECISIONS.md 743). Each field is marked as the org's. */
export function fieldsNotInOrg(names: readonly string[]): string {
  return `${listOf(names)} ${names.length === 1 ? "isn't" : "aren't"} in this org, and the read needs ${names.length === 1 ? "it" : "them"}, so it wasn't run.`;
}

/** Fields a rule needs that the describe left out (DECISIONS.md 627, 743). Each field is marked as the org's. */
export function fieldsUnseen(gone: readonly string[]): string {
  const one = gone.length === 1;
  return `${listOf(gone)} couldn't be seen in this org: ${one ? "it" : "they"} may not exist, or the connected user may not have access to ${one ? "it" : "them"}, so this can't be measured.`;
}

/** More than one active revenue forecast type and none the standard one (DECISIONS.md 627, 743). */
export function forecastTypesAmbiguous(n: number): string {
  return `${n} revenue forecast types are active, and none is the standard one, so we can't tell which to read.`;
}

/** Whether Account Engagement is installed couldn't be told, with the describe's reason where there is one (DECISIONS.md 743). */
export function pardotUnknown(reason: string | null): string {
  return `We couldn't tell whether Account Engagement is installed: Lead couldn't be described${reason !== null ? `: ${reason}` : ""}.`;
}

/** Too many flows to read one at a time through Tooling (DECISIONS.md 743). */
export function tooManyFlows(n: number, max: number): string {
  return `There are ${n} flows, more than the ${max} we read one at a time.`;
}

/** A flow Tooling gave back without its definition (DECISIONS.md 743). The flow's name is the org's. */
export function flowWithoutDefinition(name: string): string {
  return `Flow ${name} came back without its definition.`;
}

/** Too many Workflow Rules and flows together to read one at a time through Tooling (DECISIONS.md 743). */
export function tooManyRules(rules: number, flows: number, max: number): string {
  return `There are ${rules} Workflow Rules and ${flows} flows, more than the ${max} we read one at a time.`;
}

/** A Workflow Rule Tooling gave back without its definition (DECISIONS.md 743). The rule's name is the org's. */
export function ruleWithoutDefinition(name: string): string {
  return `Workflow Rule ${name} came back without its definition.`;
}

/** Account Engagement's Lead fields this org doesn't have, so its leads can't be read (DECISIONS.md 743). Each field is the org's. */
export function pardotFieldsMissing(fields: readonly string[]): string {
  return `Lead.${fields.join(" and Lead.")} ${fields.length === 1 ? "isn't" : "aren't"} in this org, so Account Engagement's leads can't be read.`;
}

/** ESLint stopped at its time limit (DECISIONS.md 743). */
export function eslintTimeLimit(seconds: number, files: number): string {
  return `ESLint reached its time limit of ${seconds} seconds, so ${files} of the files weren't checked.`;
}

/** The loss reason field the calibration names, when it can't hold a reason. */
export function lossFieldUnusable(field: string): string {
  return `The loss reason field ${field} isn't a text or picklist field, so it can't hold a reason and isn't read.`;
}

const ENDINGS = Object.values(BECAUSE).map((b) => b.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join("|");
const SENTENCE = new RegExp(`^(.+?) couldn't be read, because (?:${ENDINGS})\\.$`);
const NOT_THERE = /^(.+?) isn't available in this org, or to the connected user\.$/;
const UNFIT = /^[\d,]+ of the [\d,]+ (?:(?:won |lost |open )?deals|entries in the deals' stage history) Salesforce gave us weren't in a shape we could read, more than 1 in 100, so figures from them would mislead\.$/;
const LOSS_FIELD = /^The loss reason field (.+?) isn't a text or picklist field, so it can't hold a reason and isn't read\.$/;
const FIELD = "[A-Za-z][A-Za-z0-9_]{0,79}\\.[A-Za-z][A-Za-z0-9_.]{0,159}";
const FIELD_LIST = `(${FIELD}(?:, ${FIELD})*(?: and ${FIELD})?)`;
const NOT_IN_ORG = new RegExp(`^${FIELD_LIST} (?:isn't|aren't) in this org, and the read needs (?:it|them), so it wasn't run\\.$`);
const UNSEEN = new RegExp(`^${FIELD_LIST} couldn't be seen in this org: (?:it|they) may not exist, or the connected user may not have access to (?:it|them), so this can't be measured\\.$`);
const ABSENT = /^([A-Za-z][A-Za-z0-9_]{0,79}) isn't available in this org, or to the connected user: (.+)$/;
const AMBIGUOUS_TYPES = /^\d+ revenue forecast types are active, and none is the standard one, so we can't tell which to read\.$/;
const TOO_MANY_FLOWS = /^There are \d+ flows, more than the \d+ we read one at a time\.$/;
const FLOW_WITHOUT = /^Flow (.+) came back without its definition\.$/;
const TOO_MANY_RULES = /^There are \d+ Workflow Rules and \d+ flows, more than the \d+ we read one at a time\.$/;
const PARDOT_FIELDS = new RegExp(`^(${FIELD}(?: and ${FIELD})*) (?:isn't|aren't) in this org, so Account Engagement's leads can't be read\\.$`);
const RULE_WITHOUT = /^Workflow Rule (.+) came back without its definition\.$/;
const ESLINT_LIMIT = /^ESLint reached its time limit of \d+ seconds, so \d+ of the files weren't checked\.$/;
const PARDOT_UNKNOWN = "We couldn't tell whether Account Engagement is installed: Lead couldn't be described";

/** Each field of a list as a sentence says it, as spans from `at`. */
function fieldSpans(list: string, at: number): Array<{ start: number; end: number }> {
  const spans: Array<{ start: number; end: number }> = [];
  for (const m of list.matchAll(new RegExp(FIELD, "g"))) spans.push({ start: at + m.index, end: at + m.index + m[0].length });
  return spans;
}
const WHOLE: ReadonlySet<string> = new Set([LIMITS_NO_PERMISSION, LIMITS_REFUSED, DEALS_UNREAD, NO_APEX_IN_EDITION, METADATA_POINTER, APEX_POINTER]);

/**
 * The org's words in one of these sentences, as spans, or null when the text
 * isn't one. All of it is ours but the object or field it names, which is
 * marked as the org's whenever it could be a name the org chose.
 */
export function refusalSpans(text: string): Array<{ start: number; end: number }> | null {
  if (WHOLE.has(text) || UNFIT.test(text)) return [];
  // The extractors' own sentences with something put in (DECISIONS.md 743), every name in them marked as the org's;
  // before the refusal's own patterns, whose first part would otherwise take a whole absent object's sentence.
  if (AMBIGUOUS_TYPES.test(text) || TOO_MANY_FLOWS.test(text) || TOO_MANY_RULES.test(text) || ESLINT_LIMIT.test(text)) return [];
  const rule = RULE_WITHOUT.exec(text);
  if (rule) return [{ start: "Workflow Rule ".length, end: "Workflow Rule ".length + rule[1]!.length }];
  const flow = FLOW_WITHOUT.exec(text);
  if (flow) return [{ start: "Flow ".length, end: "Flow ".length + flow[1]!.length }];
  const fields = NOT_IN_ORG.exec(text) ?? UNSEEN.exec(text) ?? PARDOT_FIELDS.exec(text);
  if (fields) return fieldSpans(fields[1]!, 0);
  const absent = ABSENT.exec(text);
  if (absent) {
    const rest = refusalSpans(absent[2]!);
    const at = text.length - absent[2]!.length;
    return rest ? [{ start: 0, end: absent[1]!.length }, ...rest.map((x) => ({ start: x.start + at, end: x.end + at }))] : null;
  }
  if (text === `${PARDOT_UNKNOWN}.`) return [];
  if (text.startsWith(`${PARDOT_UNKNOWN}: `) && text.endsWith(".")) {
    const at = PARDOT_UNKNOWN.length + 2;
    const rest = refusalSpans(text.slice(at, -1));
    return rest ? rest.map((x) => ({ start: x.start + at, end: x.end + at })) : null;
  }
  const sentence = SENTENCE.exec(text) ?? NOT_THERE.exec(text);
  if (sentence) {
    const what = sentence[1]!;
    if ((READ_NAMES as readonly string[]).includes(what)) return [];
    return API_NAME.test(what) ? [{ start: 0, end: what.length }] : null;
  }
  const loss = LOSS_FIELD.exec(text);
  if (loss && API_NAME.test(loss[1]!)) {
    const start = "The loss reason field ".length;
    return [{ start, end: start + loss[1]!.length }];
  }
  return null;
}
