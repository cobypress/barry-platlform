import { isDeepStrictEqual } from "node:util";
import { TRIGGER_EVENTS, type Handbook, type HandbookApexClass, type HandbookApexTrigger, type HandbookFlowItem, type HandbookLastChange, type HandbookStep, type HandbookValidationRuleItem, type LastChanges, type ValidationRuleCoverage } from "./schema.ts";

// A re-check's handbook set beside its earlier health check's (DECISIONS.md
// 483): what was added, removed and changed in the flows, validation rules,
// Apex classes and Apex triggers. Worked out from the two files each time the
// Word document is made and kept nowhere. Each side is only what its own file
// holds, and where the two can't be compared fairly nothing is called added,
// removed or changed (rule 5): the result says what was left out, and why, so
// the document can say so.

/**
 * Every detail a change can name, as a fixed key the document turns into words.
 * Never the earlier value: only that it differs.
 */
export const DETAILS = [
  "label",
  "status",
  "type",
  "description",
  "start",
  "schedule",
  "entry",
  "paths",
  "steps",
  "active",
  "formula",
  "message",
  "messageField",
  "apiVersion",
  "isTest",
  "touches",
  "object",
  "events",
  "lastChanged",
] as const;
export type Detail = (typeof DETAILS)[number];

/** What a list names a component by: a flow's label beside its name, a rule's or trigger's object. */
export type Component = { name: string; label: string | null; object: string | null };

/** One in both files whose details differ, with this run's last change for the line that says so. */
export type Changed = Component & { details: Detail[]; lastChanged: HandbookLastChange | null };

/** How each file's metadata was read, told from what its validation rules cover; null when it can't be told. */
export type Route = "retrieve" | "tooling";

/**
 * Why a kind's details weren't compared, from the two files' `readBy` for it
 * (decision 484): the earlier file's reading is older than this one's, the two
 * differ some other way, or the earlier file, this one or both don't say,
 * which a file from before the readings were kept, or a run that reused a
 * step read before then, doesn't.
 */
export type Reading = "older" | "differs" | "unknownBefore" | "unknownNow" | "unknownBoth";

export type Compared = {
  status: "compared";
  added: Component[];
  removed: Component[];
  changed: Changed[];
  /** In both files and compared, changed or not. */
  common: number;
  /** Whether each run read when each was last changed: dates are compared only where both runs hold one. */
  lastChanges: { before: LastChanges; after: LastChanges };
  /** In both, with a detail one of the runs couldn't read, which is left out rather than called a change. */
  unread: number;
  /** Validation rules: objects we can't be sure both runs read the rules on, whose rules are neither added nor removed. */
  leftOutObjects: string[];
  /** Validation rules left out because one run could name their object only by its id. */
  unnamed: number;
  /** Apex: a list cut at its limit in either run, so names are compared only up to `upTo`. `leftOut` counts this run's after it. */
  range: { upTo: string; leftOut: number } | null;
  /** Flows: with steps cut in either run, compared by their count and the steps both kept. */
  stepsCut: number;
  /** Flows: the two runs read them by different routes, or one route can't be told, so only last-changed dates are compared. */
  routes: "differ" | "unknown" | null;
  /** The two files weren't made by the same reading of this kind, or one doesn't say, so only last-changed dates are compared. */
  reading: Reading | null;
};

/** A kind one of the runs couldn't read: "now" is this run, "before" the earlier one. */
export type NotCompared = { status: "notCompared"; unreadIn: "now" | "before" | "both" };

export type KindComparison = Compared | NotCompared;

export type HandbookComparison = { flows: KindComparison; validationRules: KindComparison; apexClasses: KindComparison; apexTriggers: KindComparison };

type Part = { status: "ok" } | { status: "unavailable" };

function notCompared(before: Part, after: Part): NotCompared | null {
  if (before.status === "ok" && after.status === "ok") return null;
  return { status: "notCompared", unreadIn: before.status !== "ok" && after.status !== "ok" ? "both" : before.status !== "ok" ? "before" : "now" };
}

/** What comparing one component in both files found: the details that differ, and whether any couldn't be compared. */
type Difference = { details: Detail[]; unread: boolean };

type Kind<T> = {
  key: (item: T) => string;
  component: (item: T) => Component;
  differ: (before: T, after: T) => Difference;
  /** False for one that can't fairly be called added, removed or changed. */
  inScope: (item: T) => boolean;
};

/** The notes one kind's comparison has, empty for the kinds it doesn't apply to. */
type Notes = Pick<Compared, "leftOutObjects" | "unnamed" | "range" | "stepsCut" | "routes" | "reading">;
const none = (): Notes => ({ leftOutObjects: [], unnamed: 0, range: null, stepsCut: 0, routes: null, reading: null });

type Dated = { lastChanged: HandbookLastChange | null };

/**
 * A component compared only in when it was last changed, where the two files
 * can't be compared detail by detail (decisions 483 and 484): nothing a reading
 * works out, and nothing another reading or route could give differently, so
 * not even its status. Nothing is unread, since nothing else was compared.
 */
const datesOnly = (before: Dated, after: Dated): Difference => ({ details: lastChangedDiffers(before.lastChanged, after.lastChanged) ? ["lastChanged"] : [], unread: false });

/** `full` false compares only last-changed dates: see `datesOnly`. */
function compareKind<T extends Dated>(before: T[], after: T[], kind: Kind<T>, lastChanges: Compared["lastChanges"], full: boolean): Omit<Compared, keyof Notes> {
  const earlier = new Map(before.filter(kind.inScope).map((i) => [kind.key(i), i]));
  const now = new Map(after.filter(kind.inScope).map((i) => [kind.key(i), i]));
  const added = [...now].filter(([k]) => !earlier.has(k)).map(([, i]) => kind.component(i));
  const removed = [...earlier].filter(([k]) => !now.has(k)).map(([, i]) => kind.component(i));
  const changed: Changed[] = [];
  let common = 0;
  let unread = 0;
  for (const [k, item] of now) {
    const was = earlier.get(k);
    if (was === undefined) continue;
    common++;
    const d = full ? kind.differ(was, item) : datesOnly(was, item);
    if (d.unread) unread++;
    if (d.details.length) changed.push({ ...kind.component(item), details: d.details, lastChanged: item.lastChanged });
  }
  return { status: "compared", added, removed, changed, common, lastChanges, unread };
}

/**
 * When it was last changed, only where both files hold a date (decision 483):
 * an earlier file version, a run that couldn't read them or Salesforce's list
 * having no line for it leaves it null, which is never a change. Who changed it
 * is never compared: tokens are numbered afresh in each run.
 */
function lastChangedDiffers(before: HandbookLastChange | null, after: HandbookLastChange | null): boolean {
  if (before === null || after === null) return false;
  const [a, b] = [Date.parse(before.at), Date.parse(after.at)];
  return Number.isFinite(a) && Number.isFinite(b) ? a !== b : before.at !== after.at;
}

const differs = (a: unknown, b: unknown): boolean => !isDeepStrictEqual(a, b);

/**
 * Whether both files were made by the same reading of a kind (decision 484):
 * null when they were, so its details can be compared. A file that doesn't say
 * is never taken to be the same reading as one that does, or as another that
 * doesn't: a parser fix can move a detail of a component nobody touched.
 */
export function readingOf(before: number | null, after: number | null): Reading | null {
  if (before === null || after === null) return before === null && after === null ? "unknownBoth" : before === null ? "unknownBefore" : "unknownNow";
  return before === after ? null : before < after ? "older" : "differs";
}

// ─── Flows ────────────────────────────────────────────────────────────────────

/**
 * The route each file's metadata came by. Both routes fill a flow's detail the
 * same way, but which version of a flow each gives isn't verified (QUESTIONS.md,
 * the handbook's l), so a flow is compared detail by detail only when both runs
 * read it the same way. The file keeps no route of its own: the retrieve says
 * its validation rules cover a list of standard objects and Tooling says all
 * (metadata-parse.ts, metadata-tooling.ts), so that tells it, when they were read.
 */
export function routeOf(h: Handbook): Route | null {
  if (h.validationRules.status !== "ok") return null;
  return h.validationRules.covers.standardObjects === "all" ? "tooling" : "retrieve";
}

const stepsCutIn = (f: HandbookFlowItem): boolean => f.cut || f.stepsTotal > f.steps.length;

/**
 * Steps by name, so steps no path reaches, which each route can list in its own
 * order, never read as a change: what a step leads to is compared on the step.
 * Past a cut in either, only the count and the steps both kept are compared.
 */
function stepsDiffer(before: HandbookFlowItem, after: HandbookFlowItem): boolean {
  if (before.stepsTotal !== after.stepsTotal) return true;
  const earlier = new Map<string, HandbookStep>(before.steps.map((s) => [s.name, s]));
  const now = new Map<string, HandbookStep>(after.steps.map((s) => [s.name, s]));
  for (const [name, step] of now) if (earlier.has(name) && differs(earlier.get(name), step)) return true;
  if (stepsCutIn(before) || stepsCutIn(after)) return false;
  return now.size !== earlier.size || [...now.keys()].some((name) => !earlier.has(name));
}

function flowDiffer(before: HandbookFlowItem, after: HandbookFlowItem): Difference {
  const details: Detail[] = [];
  if (before.label !== after.label) details.push("label");
  if (before.status !== after.status) details.push("status");
  if (before.processType !== after.processType) details.push("type");
  if (before.description !== after.description) details.push("description");
  const start = (f: HandbookFlowItem) => [f.start.object, f.start.triggerType, f.start.recordTriggerType];
  if (differs(start(before), start(after))) details.push("start");
  if (differs(before.start.schedule, after.start.schedule)) details.push("schedule");
  if (differs(before.start.entry, after.start.entry)) details.push("entry");
  const paths = (f: HandbookFlowItem) => [f.start.paths, f.start.pathsTotal, f.start.pathsCut];
  if (differs(paths(before), paths(after))) details.push("paths");
  if (stepsDiffer(before, after)) details.push("steps");
  if (lastChangedDiffers(before.lastChanged, after.lastChanged)) details.push("lastChanged");
  return { details, unread: false };
}

const byLabel = (a: Component, b: Component): number => (a.label || a.name).localeCompare(b.label || b.name, "en-GB") || a.name.localeCompare(b.name, "en-GB");

function compareFlows(before: Handbook, after: Handbook): KindComparison {
  const skip = notCompared(before.flows, after.flows);
  if (skip || before.flows.status !== "ok" || after.flows.status !== "ok") return skip!;
  const [was, now] = [routeOf(before), routeOf(after)];
  const routes = was === null || now === null ? "unknown" : was !== now ? "differ" : null;
  // The same route and the same reading, or only the dates.
  const reading = readingOf(before.readBy.metadata, after.readBy.metadata);
  const full = routes === null && reading === null;
  const kind: Kind<HandbookFlowItem> = { key: (f) => f.name, component: (f) => ({ name: f.name, label: f.label || null, object: null }), differ: flowDiffer, inScope: () => true };
  const result = compareKind(before.flows.items, after.flows.items, kind, { before: before.flows.lastChanges, after: after.flows.lastChanges }, full);
  const earlier = new Map(before.flows.items.map((f) => [f.name, f]));
  const stepsCut = full ? after.flows.items.filter((f) => earlier.has(f.name) && (stepsCutIn(f) || stepsCutIn(earlier.get(f.name)!))).length : 0;
  return { ...result, ...none(), added: result.added.sort(byLabel), removed: result.removed.sort(byLabel), changed: result.changed.sort(byLabel), stepsCut, routes, reading };
}

// ─── Validation rules ─────────────────────────────────────────────────────────

/** A custom object's id, as the Tooling route keeps it when it can't name the object. */
const OBJECT_ID = /^01I[0-9A-Za-z]{12}(?:[0-9A-Za-z]{3})?$/;

/** A custom object of any kind (decision 484): `__c`, `__mdt`, `__e`, `__b`, `__x`, `__kav` and the like, or one known only by its id. */
const isCustom = (object: string): boolean => /__[A-Za-z]+$/.test(object) || OBJECT_ID.test(object);

/**
 * Whether a run read the rules on an object (decisions 455 and 484): every
 * custom object, the standard objects its list names, or all of them on
 * Tooling. One it holds a rule on was read, whatever its list says. Both routes
 * read every custom object, but whether the retrieve's wildcard and Tooling's
 * list bring the same kinds isn't verified (QUESTIONS.md, the handbook's x), so
 * across routes, or where a route can't be told, only `__c` and an id count:
 * a rule on a custom metadata type, say, is left out rather than called added
 * or removed. A file that ever said it read less than every custom object is
 * taken at its word.
 */
function covered(covers: ValidationRuleCoverage, items: HandbookValidationRuleItem[], object: string, sameRoute: boolean): boolean {
  if (isCustom(object)) return covers.customObjects === "all" && (sameRoute || /__c$/.test(object) || OBJECT_ID.test(object));
  if (covers.standardObjects === "all" || covers.standardObjects.includes(object)) return true;
  return items.some((r) => r.object === object);
}

/**
 * The formula, message and where it shows come from the rule's definition,
 * which the Tooling route can fail to read, leaving all three null. A null
 * formula on either side means "not read", never a change.
 */
function ruleDiffer(before: HandbookValidationRuleItem, after: HandbookValidationRuleItem): Difference {
  const details: Detail[] = [];
  if (before.active !== after.active) details.push("active");
  if (before.description !== after.description) details.push("description");
  const unread = before.formula === null || after.formula === null;
  if (!unread) {
    // Cut at 4,000 characters: the kept text and the full length are both in the file, so this is exact as far as it goes.
    if (before.formula !== after.formula || before.formulaLength !== after.formulaLength || before.formulaCut !== after.formulaCut) details.push("formula");
    if (before.message !== after.message) details.push("message");
    if (before.messageField !== after.messageField) details.push("messageField");
  }
  if (lastChangedDiffers(before.lastChanged, after.lastChanged)) details.push("lastChanged");
  return { details, unread };
}

const ruleKey = (r: { object: string; name: string }): string => JSON.stringify([r.object, r.name]);
const byObject = (a: Component, b: Component): number => (a.object ?? "").localeCompare(b.object ?? "", "en-GB") || a.name.localeCompare(b.name, "en-GB");

function compareRules(before: Handbook, after: Handbook): KindComparison {
  const skip = notCompared(before.validationRules, after.validationRules);
  if (skip || before.validationRules.status !== "ok" || after.validationRules.status !== "ok") return skip!;
  const [was, now] = [before.validationRules, after.validationRules];
  // An object one of the runs may not have read: a rule on it is neither added nor removed.
  const route = routeOf(before);
  const sameRoute = route !== null && route === routeOf(after);
  const objects = new Set([...was.items, ...now.items].map((r) => r.object));
  const leftOut = [...objects].filter((o) => !covered(was.covers, was.items, o, sameRoute) || !covered(now.covers, now.items, o, sameRoute)).sort((a, b) => a.localeCompare(b, "en-GB"));
  // A rule whose object one run named only by its id, and the rule of that name
  // the other run named, can't be told the same or different: left out, unless
  // both runs kept it under the same id.
  const [nowKeys, wasKeys] = [new Set(now.items.map(ruleKey)), new Set(was.items.map(ruleKey))];
  const inBoth = (r: HandbookValidationRuleItem): boolean => nowKeys.has(ruleKey(r)) && wasKeys.has(ruleKey(r));
  const unnamedNames = new Set([...was.items, ...now.items].filter((r) => OBJECT_ID.test(r.object) && !inBoth(r)).map((r) => r.name));
  const unnamed = (r: HandbookValidationRuleItem): boolean => unnamedNames.has(r.name) && !inBoth(r);
  const skipped = new Set(leftOut);
  const kind: Kind<HandbookValidationRuleItem> = {
    key: ruleKey,
    component: (r) => ({ name: r.name, label: null, object: r.object }),
    differ: ruleDiffer,
    inScope: (r) => !skipped.has(r.object) && !unnamed(r),
  };
  const reading = readingOf(before.readBy.metadata, after.readBy.metadata);
  const result = compareKind(was.items, now.items, kind, { before: was.lastChanges, after: now.lastChanges }, reading === null);
  // By name: one rule seen under its id in one run and its name in the other is one.
  const unnamedCount = new Set([...was.items, ...now.items].filter((r) => !skipped.has(r.object) && unnamed(r)).map((r) => r.name)).size;
  return { ...result, ...none(), added: result.added.sort(byObject), removed: result.removed.sort(byObject), changed: result.changed.sort(byObject), leftOutObjects: leftOut, unnamed: unnamedCount, reading };
}

// ─── Apex ─────────────────────────────────────────────────────────────────────

type ApexPart<T> = { status: "ok"; items: T[]; total: number; cut: boolean; lastChanges: LastChanges } | { status: "unavailable"; reason: string };

/** In the file's order, which is by name as the file sorts it (build.ts). */
const byName = (a: Component, b: Component): number => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0);

/**
 * Lists are cut at their limit by name (decision 456), so a name past a cut
 * list's last kept one may be there or not. Names are compared only up to the
 * lower of the cut lists' last names, where both lists are whole.
 */
function rangeOf<T extends { name: string }>(before: { items: T[]; total: number; cut: boolean }, after: { items: T[]; total: number; cut: boolean }): string | null {
  const ends = [before, after].filter((p) => p.cut || p.total > p.items.length).map((p) => p.items.at(-1)?.name ?? "");
  if (ends.length === 0) return null;
  return ends.reduce((a, b) => (b < a ? b : a));
}

function apexDiffer(before: HandbookApexClass, after: HandbookApexClass): Difference {
  const details: Detail[] = [];
  if (before.apiVersion !== after.apiVersion) details.push("apiVersion");
  if (before.status !== after.status) details.push("status");
  // Null where the code couldn't be read: not read, never a change.
  const unread = before.isTest === null || after.isTest === null || before.touches === null || after.touches === null;
  if (before.isTest !== null && after.isTest !== null && before.isTest !== after.isTest) details.push("isTest");
  if (before.touches !== null && after.touches !== null && differs(before.touches, after.touches)) details.push("touches");
  if (lastChangedDiffers(before.lastChanged, after.lastChanged)) details.push("lastChanged");
  return { details, unread };
}

const inOrder = (events: HandbookApexTrigger["events"]) => (events === null ? null : [...new Set(events)].sort((a, b) => TRIGGER_EVENTS.indexOf(a) - TRIGGER_EVENTS.indexOf(b)));

function triggerDiffer(before: HandbookApexTrigger, after: HandbookApexTrigger): Difference {
  const apex = apexDiffer(before, after);
  const details: Detail[] = apex.details.filter((d) => d !== "lastChanged");
  // A custom object one run could name and the other gave only as its id isn't a move.
  const objectUnread = before.object !== after.object && (OBJECT_ID.test(before.object) || OBJECT_ID.test(after.object));
  if (before.object !== after.object && !objectUnread) details.push("object");
  const eventsUnread = before.events === null || after.events === null;
  if (!eventsUnread && differs(inOrder(before.events), inOrder(after.events))) details.push("events");
  if (apex.details.includes("lastChanged")) details.push("lastChanged");
  return { details, unread: apex.unread || objectUnread || eventsUnread };
}

function compareApex<T extends HandbookApexClass>(before: ApexPart<T>, after: ApexPart<T>, readBy: [number | null, number | null], differ: (b: T, a: T) => Difference, object: (item: T) => string | null): KindComparison {
  const skip = notCompared(before, after);
  if (skip || before.status !== "ok" || after.status !== "ok") return skip!;
  const upTo = rangeOf(before, after);
  const inRange = (item: T): boolean => upTo === null || item.name <= upTo;
  const kind: Kind<T> = { key: (i) => i.name, component: (i) => ({ name: i.name, label: null, object: object(i) }), differ, inScope: inRange };
  const reading = readingOf(...readBy);
  const result = compareKind(before.items, after.items, kind, { before: before.lastChanges, after: after.lastChanges }, reading === null);
  const range = upTo === null ? null : { upTo, leftOut: Math.max(after.total, after.items.length) - after.items.filter(inRange).length };
  return { ...result, ...none(), added: result.added.sort(byName), removed: result.removed.sort(byName), changed: result.changed.sort(byName), range, reading };
}

/** The two handbooks side by side, each kind on its own. `before` is the earlier health check's. */
export function compareHandbooks(before: Handbook, after: Handbook): HandbookComparison {
  const apex: [number | null, number | null] = [before.readBy.apex, after.readBy.apex];
  return {
    flows: compareFlows(before, after),
    validationRules: compareRules(before, after),
    apexClasses: compareApex<HandbookApexClass>(before.apexClasses, after.apexClasses, apex, apexDiffer, () => null),
    apexTriggers: compareApex<HandbookApexTrigger>(before.apexTriggers, after.apexTriggers, apex, triggerDiffer, (t) => t.object),
  };
}
