// What a customer reads when Salesforce refused a read (DECISIONS.md 702),
// every sentence in one place, and how the engine knows one again when a rule
// passes it on. Salesforce's own message and code never reach these: they go
// to run.json's readsRefused and the run's log, for us alone.

/** Why a read cost its checks, by the class the client's classifier gives it. */
export type RefusalKind = "not_available" | "no_permission" | "refused";

const BECAUSE: Readonly<Record<RefusalKind, string>> = {
  not_available: "it isn't available in this Salesforce edition or isn't switched on",
  no_permission: "the person who connected Salesforce doesn't have permission to see it",
  refused: "Salesforce refused this read",
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

/** The org's limits, refused at connect. */
export const LIMITS_NO_PERMISSION = "The org's limits weren't measured, because the person who connected Salesforce can't view Setup.";
export const LIMITS_REFUSED = "The org's limits weren't measured, because Salesforce refused the read.";

/** No API for the org, or for the user who connected: the connect's and the run's final answer. */
export const NO_API_FOR_ORG = "This Salesforce org doesn't allow API access, so we can't read it. Some editions need API access added, and some don't offer it.";
export const NO_API_FOR_USER = "The person who connected doesn't have API access in Salesforce, so we can't read the org. They need the API Enabled permission.";

/** When the deals read can't be used, the history read beside it says so. */
export const DEALS_UNREAD = "Opportunity couldn't be read";

/** Too many rows of one read in a shape we couldn't read (DECISIONS.md 702). */
export function tooManyUnfit(bad: number, total: number, what: "deals" | "entries in the deals' stage history"): string {
  return `${bad.toLocaleString("en-GB")} of the ${total.toLocaleString("en-GB")} ${what} Salesforce gave us weren't in a shape we could read, more than 1 in 100, so figures from them would mislead.`;
}

/** The loss reason field the calibration names, when it can't hold a reason. */
export function lossFieldUnusable(field: string): string {
  return `The loss reason field ${field} isn't a text or picklist field, so it can't hold a reason and isn't read.`;
}

const ENDINGS = Object.values(BECAUSE).map((b) => b.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join("|");
const SENTENCE = new RegExp(`^(.+?) couldn't be read, because (?:${ENDINGS})\\.$`);
const NOT_THERE = /^(.+?) isn't available in this org, or to the connected user\.$/;
const UNFIT = /^[\d,]+ of the [\d,]+ (?:deals|entries in the deals' stage history) Salesforce gave us weren't in a shape we could read, more than 1 in 100, so figures from them would mislead\.$/;
const LOSS_FIELD = /^The loss reason field (.+?) isn't a text or picklist field, so it can't hold a reason and isn't read\.$/;
const WHOLE: ReadonlySet<string> = new Set([LIMITS_NO_PERMISSION, LIMITS_REFUSED, DEALS_UNREAD]);

/**
 * The org's words in one of these sentences, as spans, or null when the text
 * isn't one. All of it is ours but the object or field it names, which is
 * marked as the org's whenever it could be a name the org chose.
 */
export function refusalSpans(text: string): Array<{ start: number; end: number }> | null {
  if (WHOLE.has(text) || UNFIT.test(text)) return [];
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
