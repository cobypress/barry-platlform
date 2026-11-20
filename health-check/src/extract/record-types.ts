import { SF_DEFAULTS } from "../config.ts";
import { isUnavailable, SalesforceError } from "../salesforce/client.ts";
import { monthsBefore, readableReason, readSoql, soqlString, soqlUpTo, soqlWindow, type ConnectedUser, type ExtractContext, type Extractor, type Sourced } from "./types.ts";

// Record-type use (DECISIONS.md 497). The step reads its own list of record
// types, because the routes the engine already has disagree on what a type is
// called and none keeps the id the counts are grouped by. All its reads end at
// the run's date where they have one, and run together every time: a list read
// at one moment set against counts read at another would call a type made since
// unused, or count records made since. So there are no partial keys, on purpose.
//
// Some objects are never counted, whatever the user's access (DECISIONS.md 498
// and 499), because a plain count can't see them whole: Salesforce archives
// tasks and events after a year by default (decision 35), shows even a user
// with View All Data only the files they own or share, and returns Knowledge
// article versions by publish status. Counted, a type in use would read as
// never used.
//
// Only objects whose every record the connected user can see are counted
// (DECISIONS.md 498): a count sees only what the user's sharing shows them, and
// nothing in it says it was cut. So the step first reads the user's own access.

export type RecordTypeRow = { Id: string; DeveloperName: string; Name: string; SobjectType: string; IsActive: boolean; NamespacePrefix: string | null; CreatedDate: string };
/** One group of an object's records. A null type is a record with none. */
export type RecordTypeCount = { RecordTypeId: string | null; c: number };

/** What the connected user sees whole, or why we can't tell. Never their id or username. */
export type RecordAccess =
  | { status: "ok"; viewAllData: boolean; viewAllRecords: string[] }
  | { status: "unknown"; reason: string };

export type RecordTypeUse = {
  types: Sourced<RecordTypeRow[]>;
  /** Where "the last 12 months" starts: the run's date less 12 months. */
  recentFrom: string;
  access: RecordAccess;
  /** Objects whose two counts both came back whole: every record made by the run's date, and those made in the 12 months to it. */
  counted: Record<string, { all: RecordTypeCount[]; recent: RecordTypeCount[] }>;
  /** Objects whose count Salesforce refused, with its reason. */
  refused: Array<{ object: string; reason: string }>;
  /** Objects whose count came back cut short. Aggregates can't page, so a part is no proof. */
  truncated: string[];
  /** Objects with an active type of their own that the connected user doesn't see whole, so never counted. */
  notSeenWhole: string[];
  /** Objects with an active type of their own beyond the ones we count. */
  notRead: string[];
  /** Objects a plain count can't see whole (isPartlyHidden) with an active type of their own, which are never counted. */
  partlyHidden: string[];
  /** The two counts' queries, with <object> for each object, and the two access reads', with the user as USER_PLACEHOLDER. */
  queries: { all: string; recent: string; viewAllData: string; viewAllRecords: string };
};

/** How many objects are counted, those with the most active types first. Two counts each, so two composite calls. */
export const RECORD_TYPE_OBJECTS = 25;
export const RECENT_MONTHS = 12;
/**
 * Objects a plain count can't see whole, whatever the user's access. Archived
 * activities are out of a count (queryAll isn't on the allowlist), and files
 * need Query All Files, which we don't read. Article versions are matched by
 * their suffix, below. Not proved complete: QUESTIONS.md asks about the rest.
 */
export const PARTLY_HIDDEN_OBJECTS: ReadonlySet<string> = new Set(["Task", "Event", "ContentVersion", "ContentDocument"]);
/** Whether a plain count can't see every record of `object`: one of the list, or a Knowledge article version object (`Thing__kav`, `ns__Thing__kav`). */
export const isPartlyHidden = (object: string): boolean => PARTLY_HIDDEN_OBJECTS.has(object) || /__kav$/i.test(object);
/** Stands for the user in every query the step keeps, so no id or username reaches the run folder. */
export const USER_PLACEHOLDER = "<the connected user>";

const LIST = (end: string): string => `SELECT Id, DeveloperName, Name, SobjectType, IsActive, NamespacePrefix, CreatedDate FROM RecordType WHERE ${end}`;
const COUNT = (object: string, where: string): string => `SELECT RecordTypeId, COUNT(Id) c FROM ${object} WHERE ${where} GROUP BY RecordTypeId`;
const LOOKUP = (username: string): string => `SELECT Id FROM User WHERE Username = ${soqlString(username)}`;
// A profile's own permission set is among a user's assignments, as are the sets
// in a group, so these two cover everything that gives the user access
// (src/connect/read-only-check.ts). A set that has to be activated in a session
// is left out (DECISIONS.md 499): it shows as assigned while an API session has
// none of its permissions, so a count would run under ordinary sharing. A
// profile's own set never needs activating. `id` is checked against USER_ID first.
const VIEW_ALL_DATA = (id: string): string => `SELECT PermissionSet.PermissionsViewAllData FROM PermissionSetAssignment WHERE AssigneeId = '${id}' AND PermissionSet.HasActivationRequired = false`;
const VIEW_ALL_RECORDS = (id: string): string =>
  `SELECT SobjectType FROM ObjectPermissions WHERE PermissionsViewAllRecords = true AND Parent.HasActivationRequired = false AND ParentId IN (SELECT PermissionSetId FROM PermissionSetAssignment WHERE AssigneeId = '${id}')`;
/** A Salesforce id's shape. Nothing else goes into a query between quotes unescaped. */
const USER_ID = /^[A-Za-z0-9]{15}(?:[A-Za-z0-9]{3})?$/;

export const isPackaged = (type: Pick<RecordTypeRow, "NamespacePrefix">): boolean => Boolean(type.NamespacePrefix);

/** The user's id and username, and the forms a query holds them in, as USER_PLACEHOLDER. */
function hider(user: ConnectedUser | undefined): (text: string) => string {
  const secrets = [user?.id, user?.id?.slice(0, 15), user?.username, user?.username && soqlString(user.username).slice(1, -1)].filter((s): s is string => Boolean(s));
  return (text) => secrets.reduce((out, secret) => out.split(secret).join(USER_PLACEHOLDER), text);
}

/**
 * One access read. Salesforce's reasons can quote the query, so the user is
 * hidden in them; a fault that isn't Salesforce's answer still stops the step,
 * as readSoql has it, with the user hidden in its message too.
 */
async function readHidden<R>(ctx: ExtractContext, soql: string, hide: (text: string) => string): Promise<{ ok: true; rows: R[] } | { ok: false; reason: string }> {
  let read: Sourced<R[]>;
  try {
    read = await readSoql<R>(ctx, soql);
  } catch (err) {
    // Only rebuilt where it quotes the user, so every other error goes up as itself.
    if (err instanceof SalesforceError && hide(err.message) !== err.message) throw new SalesforceError(err.status, err.code, hide(err.message.replace(`${err.code}: `, "")));
    throw err;
  }
  // No row limit is set, so the client pages to the end and a read is never cut short.
  return read.status === "ok" ? { ok: true, rows: read.data } : { ok: false, reason: hide(read.reason) };
}

// These reads have no date: there's no "as at" for a permission. On a resumed
// run they show the user's access at the moment the counts are made, which is
// the pair that must agree. Salesforce sets View All when Modify All is set, so
// keying on View All alone is right (unverified: QUESTIONS.md).
export async function accessOf(ctx: ExtractContext): Promise<RecordAccess> {
  const user = ctx.connectedUser;
  let hide = hider(user);
  let id = user?.id;
  if (!id && user?.username) {
    const found = await readHidden<{ Id: string }>(ctx, LOOKUP(user.username), hide);
    if (!found.ok) return { status: "unknown", reason: `We couldn't look the connected user up by their username: ${found.reason}` };
    if (found.rows.length !== 1) return { status: "unknown", reason: "We couldn't find the connected user by their username." };
    id = found.rows[0]!.Id;
    // The id found is as much the user's as one we're given (DECISIONS.md 499),
    // so the reads that hold it hide it too.
    hide = hider({ ...user, id });
  }
  if (!id) return { status: "unknown", reason: "The run wasn't told which user connected." };
  if (!USER_ID.test(id)) return { status: "unknown", reason: "The connected user's id isn't the shape of a Salesforce id." };
  const sets = await readHidden<{ PermissionSet: { PermissionsViewAllData: boolean } | null }>(ctx, VIEW_ALL_DATA(id), hide);
  if (!sets.ok) return { status: "unknown", reason: `The connected user's permission set assignments couldn't be read: ${sets.reason}` };
  // Every user holds at least their profile's set, which never needs
  // activating, so none at all means we can't see their assignments, not that
  // they have no access.
  if (sets.rows.length === 0) return { status: "unknown", reason: "No permission set assignments were visible for the connected user." };
  const objects = await readHidden<{ SobjectType: string }>(ctx, VIEW_ALL_RECORDS(id), hide);
  if (!objects.ok) return { status: "unknown", reason: `The connected user's object permissions couldn't be read: ${objects.reason}` };
  return {
    status: "ok",
    viewAllData: sets.rows.some((r) => r.PermissionSet?.PermissionsViewAllData === true),
    viewAllRecords: [...new Set(objects.rows.map((r) => r.SobjectType))].sort(),
  };
}

export const recordTypeUseExtractor: Extractor<RecordTypeUse> = {
  id: "record-type-use",
  version: 4,
  // Counts records on every object it reads, so a set-up review never runs it.
  records: true,
  async run(ctx) {
    const end = soqlUpTo("CreatedDate", ctx.now);
    const from = monthsBefore(ctx.now, RECENT_MONTHS);
    const recent = soqlWindow("CreatedDate", from, ctx.now);
    const access = await accessOf(ctx);
    const out: RecordTypeUse = {
      types: await readSoql<RecordTypeRow>(ctx, LIST(end)),
      recentFrom: from.toISOString(),
      access,
      counted: {},
      refused: [],
      truncated: [],
      notSeenWhole: [],
      notRead: [],
      partlyHidden: [],
      queries: { all: COUNT("<object>", end), recent: COUNT("<object>", recent), viewAllData: VIEW_ALL_DATA(USER_PLACEHOLDER), viewAllRecords: VIEW_ALL_RECORDS(USER_PLACEHOLDER) },
    };
    if (out.types.status !== "ok") return out;

    const active = new Map<string, number>();
    for (const type of out.types.data) if (type.IsActive && !isPackaged(type)) active.set(type.SobjectType, (active.get(type.SobjectType) ?? 0) + 1);
    const ordered = [...active].sort(([a, n], [b, m]) => m - n || a.localeCompare(b)).map(([object]) => object);
    out.partlyHidden = ordered.filter(isPartlyHidden);
    // Unknown access counts nothing: a count of none would prove nothing.
    if (access.status !== "ok") return out;
    // The partly hidden first, then access, then the 25, so no object a count
    // can't see whole takes the place of one we can count.
    const ranked = ordered.filter((object) => !isPartlyHidden(object));
    const whole = ranked.filter((object) => access.viewAllData || access.viewAllRecords.includes(object));
    out.notSeenWhole = ranked.filter((object) => !whole.includes(object));
    const objects = whole.slice(0, RECORD_TYPE_OBJECTS);
    out.notRead = whole.slice(RECORD_TYPE_OBJECTS);

    // Each object's two counts side by side, so 25 objects are two calls.
    const queries = objects.flatMap((object) => [COUNT(object, end), COUNT(object, recent)]);
    const results: Array<{ ok: true; records: RecordTypeCount[]; truncated: boolean } | { ok: false; reason: string }> = [];
    for (let i = 0; i < queries.length; i += SF_DEFAULTS.compositeBatchSize) {
      const chunk = queries.slice(i, i + SF_DEFAULTS.compositeBatchSize);
      let answers: Awaited<ReturnType<typeof ctx.client.queryBatch<RecordTypeCount>>>;
      try {
        answers = await ctx.client.queryBatch<RecordTypeCount>(chunk);
      } catch (err) {
        // A whole call refused leaves its objects not judged, but only for the
        // codes the engine takes as Salesforce's answer (DECISIONS.md 498). A 502
        // after the retries, a session still expired after the refresh or a
        // budget stop goes up, as in every other batch read, and the step is
        // read again from its start on resume.
        if (!isUnavailable(err)) throw err;
        results.push(...chunk.map(() => ({ ok: false as const, reason: readableReason(err.message) })));
        continue;
      }
      // Inside a call that came back, one count failing is that object refused,
      // whatever the code, as the ownership counts have it (src/extract/users.ts):
      // it costs that object's types and says so, where stopping would fail a
      // whole health check on one odd object.
      for (const result of answers) results.push(result.ok ? { ok: true, records: result.result.records, truncated: result.result.truncated } : { ok: false, reason: readableReason(result.error.message) });
    }
    objects.forEach((object, i) => {
      const missing = { ok: false as const, reason: "No answer came back for this count." };
      const [all, made] = [results[2 * i] ?? missing, results[2 * i + 1] ?? missing];
      if (!all.ok) out.refused.push({ object, reason: all.reason });
      else if (!made.ok) out.refused.push({ object, reason: made.reason });
      else if (all.truncated || made.truncated) out.truncated.push(object);
      else out.counted[object] = { all: all.records, recent: made.records };
    });
    return out;
  },
};
