import type { Finding, NotMeasurableKind, Run, Score } from "../schemas/index.ts";
import type { OverallScore } from "../domains/scoring.ts";
import type { ValueModel } from "../value/model.ts";
import { apiLimitsExtractor } from "../extract/integrations.ts";
import { UNSCORED_RULES } from "../domains/scoring.ts";
import { foundByStricter, issueUnder, passedOnlyByChange, passedUnder } from "../engine/loosened.ts";
import { said, type Said } from "../engine/org-words.ts";

// The re-check (roadmap Stage 5): what changed since the first health check.
// Checks are lined up by rule, not by finding, because a rule can raise one
// finding per segment and the segments move. A rule that found something then
// is fixed only if it ran now and passed; if it couldn't be measured now, we
// say we can't tell rather than claim it.

/**
 * `notMeasurableKind` is set only on a change we can't tell about, when this
 * run's check couldn't be measured and said why (DECISIONS.md 627): one kind
 * for all its findings, or unreadable where they differ. Optional, so a
 * comparison stored before it reads as before.
 */
export type RuleChange = { ruleId: string; title: string; findingIds: string[]; notMeasurableKind?: NotMeasurableKind };

export type Progress = {
  baselineRunId: string;
  baselineAsOf: string;
  score: { before: number | null; after: number | null };
  /**
   * What fixing the issues is worth, expected, then and now. Absent when the two runs' amounts aren't in the same
   * currency (KAN-41: plain numbers then, a currency the customer named now, with hours priced only now), so no change
   * in money is said that is only a change of unit.
   */
  worth?: { before: number; after: number };
  fixed: RuleChange[];
  remaining: RuleChange[];
  added: RuleChange[];
  unknown: RuleChange[];
  /**
   * Rules with an issue now that the earlier run never checked (no issue or pass
   * there), listed apart so they don't read as the org getting worse
   * (DECISIONS.md 673). Only PMD's groups take this path, and a rule the earlier
   * run couldn't measure for want of which closed stages are lost (756); every
   * other rule compares as before. Optional, so a comparison stored before it reads as before.
   */
  newlyMeasured?: RuleChange[];
  /**
   * Issues the earlier run raised that pass now only because we loosened the rule
   * since (DECISIONS.md 732): out of every count, said once. Optional, so a
   * comparison stored before it reads as before.
   */
  ruleChanged?: RuleChange[];
  /**
   * Rules with an issue now that the earlier run passed, found only because we
   * tightened the rule since (DECISIONS.md 754): open, so in the open count, the
   * score, the plan and the value, but never new; said once. Only when there are any.
   */
  stricter?: RuleChange[];
  /**
   * By rule, the version of the issue each pass in ruleChanged stands apart from,
   * and for a rule this run didn't check that the baseline carried, the baseline's:
   * kept in the run folder, so the next comparison can go on from it (DECISIONS.md
   * 732). Only when there are any.
   */
  carried?: Record<string, number>;
  areas: Array<{ domain: string; before: number | null; after: number | null }>;
  /** Storage then and now (DECISIONS.md 500). Null when this run has no storage finding, so nothing is said. */
  storage: StorageProgress | null;
};

export type StorageReading = { used: number; max: number };
/** One kind of storage, read by both runs. The average is set only for a rise over 30 days or more that comes to 1 MB or more. */
export type StorageChange = { label: string; then: StorageReading; now: StorageReading; days: number; average: number | null };
export type StorageNotCompared = "not_read_then" | "unreadable_now" | "unreadable_then" | "not_alike";
export type StorageProgress = { compared: StorageChange[] } | { notCompared: StorageNotCompared };

/** `limitsReadAt` is when the run's `api-limits` step finished: when its storage was read. */
export type ProgressSide = {
  runId: string;
  asOf: string;
  findings: Finding[];
  scores: Score[];
  overall: OverallScore;
  value: ValueModel;
  limitsReadAt: string | null;
  /** The run's own API calls, for judging a loosened rule's pass against its old line (DECISIONS.md 732). */
  ownCalls?: number;
  /**
   * The baseline's only: by rule, the version of the issue each of its passes
   * still stood apart from (RULE_CHANGES_FILE in its folder, DECISIONS.md 732):
   * passing only because we loosened the rule since that issue.
   */
  carried?: Readonly<Record<string, number>>;
};

const byRule = (findings: Finding[]): Map<string, Finding[]> => {
  const map = new Map<string, Finding[]>();
  for (const f of findings) map.set(f.ruleId, [...(map.get(f.ruleId) ?? []), f]);
  return map;
};

const change = (ruleId: string, findings: Finding[]): RuleChange => ({ ruleId, title: findings[0]!.title, findingIds: findings.map((f) => f.id) });

function kindNow(findings: Finding[]): { notMeasurableKind?: NotMeasurableKind } {
  const kinds = new Set(findings.filter((f) => f.status === "not_measurable").map((f) => f.notMeasurableKind));
  if (kinds.size === 0 || kinds.has(undefined)) return {};
  return { notMeasurableKind: kinds.size === 1 ? [...kinds][0]! : "unreadable" };
}

/** The rules a re-check reads as newly measured when the earlier run didn't check them: PMD's groups (DECISIONS.md 673). */
const MEASURED_LATER = UNSCORED_RULES;

/**
 * A rule the earlier run couldn't measure only because nobody had said which
 * closed stages are lost and which no decision (DECISIONS.md 756): once the
 * customer says, or we read it from the stages' names, what it finds is found
 * with nothing earlier to compare, as the portal says, never new.
 */
const UNSPLIT_THEN = /^Salesforce's setting can't tell lost from no decision for:/;
const unsplitThen = (then: Finding[]): boolean =>
  then.length > 0 && then.every((f) => f.status === "not_measurable") && then.some((f) => UNSPLIT_THEN.test(f.notMeasurableReason ?? ""));

/**
 * Whether a pass for one of PMD's groups can be called fixed (DECISIONS.md 673,
 * fix round 1): only when this run's PMD read at least as many classes and at
 * least as many triggers as the earlier run's did, both as the findings' facts
 * record them, and nothing was left out because our own scan now names that
 * code. Reading less, a fact missing, or findings now under our own check is
 * "can't tell", never fixed.
 */
function fixedAsRead(then: Finding[], now: Finding[]): boolean {
  const pass = now.find((f) => f.status === "passed");
  const before = then[0];
  if (!pass || !before) return false;
  const read = (f: Finding, k: "readClasses" | "readTriggers"): number | undefined => f.facts[k];
  for (const k of ["readClasses", "readTriggers"] as const) {
    const [a, b] = [read(before, k), read(pass, k)];
    if (a === undefined || b === undefined || b < a) return false;
  }
  return (pass.facts.alreadyListed ?? 0) === 0;
}

/**
 * Whether a rule's issues now, after the earlier run passed it, were found only
 * because we tightened the rule since (DECISIONS.md 754): the earlier run passed
 * every reading it made of the rule (a segment it couldn't measure then has no pass
 * to stand on, so its issue now is new), every pass then and every issue now are
 * across a tightening from the pass's version, and no issue's reading is shown
 * raised under it. One issue the old line would also raise is new.
 */
function foundByStricterCheck(then: Finding[], issues: Finding[], ruleId: string, ownCalls: number | undefined): boolean {
  return then.length > 0 && then.every((p) => p.status === "passed" && issues.every((i) => foundByStricter(ruleId, p.ruleVersion, i.ruleVersion, passedUnder(i, ownCalls))));
}

/** The run folder's file that keeps a comparison's `carried` for the next one (DECISIONS.md 732). */
export const RULE_CHANGES_FILE = "rule-changes.json";

/**
 * Whether a rule's pass now passed only because the rule changed since the
 * earlier issue (DECISIONS.md 732, replacing 726's case for int.api-usage):
 * every earlier issue was raised under a version a later one loosened, and this
 * run's reading would still have been an issue under it. Not fixed, still open,
 * new or can't tell: listed apart once.
 */
function passedOnlyByRuleChange(issueVersions: number[], ruleId: string, nowFindings: Finding[], ownCalls: number | undefined): boolean {
  const pass = nowFindings.find((f) => f.status === "passed");
  if (!pass) return false;
  const under = issueUnder(pass, ownCalls);
  return issueVersions.length > 0 && issueVersions.every((v) => passedOnlyByChange(ruleId, v, pass.ruleVersion, under));
}

export function compareWithBaseline(baseline: ProgressSide, current: ProgressSide): Progress {
  const before = byRule(baseline.findings);
  const now = byRule(current.findings);
  const fixed: RuleChange[] = [];
  const remaining: RuleChange[] = [];
  const added: RuleChange[] = [];
  const unknown: RuleChange[] = [];
  const newlyMeasured: RuleChange[] = [];
  const ruleChanged: RuleChange[] = [];
  const stricter: RuleChange[] = [];
  const carried: Record<string, number> = {};
  const carriedThen = baseline.carried ?? {};
  // An issue standing apart in the baseline (DECISIONS.md 732): the baseline passed it only because we loosened the
  // rule. Still apart while every pass is; fixed at a pass the old line would pass too; raised again, still open, not
  // new; not checked, it stays apart.
  for (const [ruleId, version] of Object.entries(carriedThen)) {
    if ((before.get(ruleId) ?? []).some((f) => f.status === "issue")) continue;
    const nowFindings = now.get(ruleId) ?? [];
    const nowIssues = nowFindings.filter((f) => f.status === "issue");
    if (nowIssues.length) remaining.push(change(ruleId, nowIssues));
    else if (!nowFindings.some((f) => f.status === "passed")) carried[ruleId] = version;
    else if (passedOnlyByRuleChange([version], ruleId, nowFindings, current.ownCalls)) {
      ruleChanged.push(change(ruleId, nowFindings));
      carried[ruleId] = version;
    } else fixed.push(change(ruleId, nowFindings));
  }
  for (const [ruleId, then] of before) {
    const thenIssues = then.filter((f) => f.status === "issue");
    if (thenIssues.length === 0) continue;
    const nowFindings = now.get(ruleId) ?? [];
    // PMD's groups exist only on a run that had its step: a pulse, or a run
    // without it, says nothing of them, never "can't tell" (DECISIONS.md 673).
    if (MEASURED_LATER.has(ruleId) && nowFindings.length === 0) continue;
    const nowIssues = nowFindings.filter((f) => f.status === "issue");
    if (!nowIssues.length && passedOnlyByRuleChange(thenIssues.map((f) => f.ruleVersion), ruleId, nowFindings, current.ownCalls)) {
      ruleChanged.push(change(ruleId, thenIssues));
      carried[ruleId] = Math.min(...thenIssues.map((f) => f.ruleVersion));
      continue;
    }
    if (nowIssues.length) remaining.push(change(ruleId, nowIssues));
    else if (nowFindings.some((f) => f.status === "passed") && (!MEASURED_LATER.has(ruleId) || fixedAsRead(thenIssues, nowFindings))) fixed.push(change(ruleId, thenIssues));
    else unknown.push({ ...change(ruleId, thenIssues), ...kindNow(nowFindings) });
  }
  for (const [ruleId, nowFindings] of now) {
    const nowIssues = nowFindings.filter((f) => f.status === "issue");
    if (!nowIssues.length) continue;
    const then = before.get(ruleId) ?? [];
    if (then.some((f) => f.status === "issue") || Object.hasOwn(carriedThen, ruleId)) continue;
    // New only if the earlier run checked the rule, as the portal's alerts decide (DECISIONS.md 661, 673).
    if ((MEASURED_LATER.has(ruleId) && !then.some((f) => f.status === "passed")) || unsplitThen(then)) newlyMeasured.push(change(ruleId, nowIssues));
    // Passed then, raised now only because we tightened the rule since: open, never new (DECISIONS.md 754).
    else if (foundByStricterCheck(then, nowIssues, ruleId, current.ownCalls)) stricter.push(change(ruleId, nowIssues));
    else added.push(change(ruleId, nowIssues));
  }
  const domains = [...new Set([...baseline.scores, ...current.scores].map((s) => s.domain))];
  const scoreOf = (scores: Score[], domain: string): number | null => {
    const s = scores.find((x) => x.domain === domain);
    return s?.status === "measured" ? s.score : null;
  };
  return {
    baselineRunId: baseline.runId,
    baselineAsOf: baseline.asOf,
    score: { before: baseline.overall.status === "measured" ? baseline.overall.score : null, after: current.overall.status === "measured" ? current.overall.score : null },
    ...(baseline.value.currency === current.value.currency ? { worth: { before: baseline.value.total.expected, after: current.value.total.expected } } : {}),
    fixed,
    remaining,
    added,
    unknown,
    // Only when there are some, so a comparison without them is as it was.
    ...(newlyMeasured.length ? { newlyMeasured } : {}),
    ...(ruleChanged.length ? { ruleChanged } : {}),
    ...(stricter.length ? { stricter } : {}),
    ...(Object.keys(carried).length ? { carried } : {}),
    areas: domains.map((domain) => ({ domain, before: scoreOf(baseline.scores, domain), after: scoreOf(current.scores, domain) })).filter((a) => a.before !== null || a.after !== null),
    storage: compareStorage(baseline, current),
  };
}

// Storage growth (DECISIONS.md 500). The org keeps no history of its storage,
// but a re-check holds two readings of ours, so the change between them is
// measured, not estimated. No run-out date: two readings can't tell a steady
// rise from one data load.

export const STORAGE_RULE = "tech.storage-use";
/**
 * The rule's versions that read storage the same way: the same fact keys, used
 * as the allowance less what remains, in MB. Version 2 changed only how the
 * largest objects are named, and version 3 only how a kind past its allowance
 * is worded (DECISIONS.md 502). A reading from a version not on this list may
 * mean something else, so it isn't compared. Whoever raises the rule's version
 * decides whether it belongs here (a test fails until they do).
 */
export const STORAGE_READ_ALIKE: readonly number[] = [1, 2, 3];
const STORAGE_KINDS = [
  ["DataStorageMB", "Data storage"],
  ["FileStorageMB", "File storage"],
] as const;
/** An average is given over this many days, and only when the readings are at least this far apart. */
export const STORAGE_AVERAGE_DAYS = 30;
const DAY_MS = 86_400_000;

/**
 * When the run read its limits: the kept reading's own time where its folder has one
 * (DECISIONS.md 726), since a resumed run's step uses its first sitting's reading;
 * otherwise its `api-limits` step's finish. A reused checkpoint keeps the first one, as it keeps that connection's limits.
 */
export const limitsReadAt = (run: Run, kept?: string | null): string | null => kept ?? run.steps.find((s) => s.id === apiLimitsExtractor.id)?.finishedAt ?? null;

/** A kind's used and allowance figures, when both are whole MB and the allowance is above 0. */
function readingOf(f: Finding, key: string): StorageReading | null {
  const used = f.facts[`${key}Used`];
  const max = f.facts[`${key}Max`];
  if (used === undefined || max === undefined || !Number.isInteger(used) || !Number.isInteger(max) || used < 0 || max <= 0) return null;
  return { used, max };
}

function compareStorage(baseline: ProgressSide, current: ProgressSide): StorageProgress | null {
  const now = current.findings.find((f) => f.ruleId === STORAGE_RULE);
  if (!now) return null;
  const then = baseline.findings.find((f) => f.ruleId === STORAGE_RULE);
  if (!then) return { notCompared: "not_read_then" };
  if (now.status === "not_measurable") return { notCompared: "unreadable_now" };
  if (then.status === "not_measurable") return { notCompared: "unreadable_then" };
  const notAlike: StorageProgress = { notCompared: "not_alike" };
  if (![then, now].every((f) => (f.status === "issue" || f.status === "passed") && STORAGE_READ_ALIKE.includes(f.ruleVersion))) return notAlike;
  const from = Date.parse(baseline.limitsReadAt ?? "");
  const to = Date.parse(current.limitsReadAt ?? "");
  if (!Number.isFinite(from) || !Number.isFinite(to) || to <= from) return notAlike;
  const days = Math.floor((to - from) / DAY_MS);
  // "Up 470 MB in 0 days" isn't true as printed, so readings under a whole day apart aren't set side by side (DECISIONS.md 501).
  if (days < 1) return notAlike;
  const compared = STORAGE_KINDS.flatMap(([key, label]): StorageChange[] => {
    const [a, b] = [readingOf(then, key), readingOf(now, key)];
    if (!a || !b) return [];
    const rise = b.used - a.used;
    const average = rise > 0 && days >= STORAGE_AVERAGE_DAYS ? Math.round((rise / days) * STORAGE_AVERAGE_DAYS) : 0;
    // "An average of 0 MB" says nothing grew, so an average that rounds to nothing isn't given (DECISIONS.md 501).
    return [{ label, then: a, now: b, days, average: average >= 1 ? average : null }];
  });
  return compared.length ? { compared } : notAlike;
}

const NOT_COMPARED: Record<StorageNotCompared, string> = {
  not_read_then: "Your last health check didn't read storage, so we can't say how it has changed.",
  unreadable_now: "Storage couldn't be read this time, so we can't say how it has changed since your last health check.",
  unreadable_then: "Storage couldn't be read last time, so we can't say how it has changed since your last health check.",
  not_alike: "We can't set the two health checks' storage readings side by side, so we don't compare them.",
};

const mb = (n: number): string => n.toLocaleString("en-GB");
const dayCount = (n: number): string => `${n} ${n === 1 ? "day" : "days"}`;
/**
 * One side's reading. Past the allowance it isn't "10,500 of 10,240 MB": that
 * reads as a part larger than its whole, which the number check refuses
 * (DECISIONS.md 501).
 */
const reading = (r: StorageReading, when: "then" | "now"): string =>
  r.used > r.max ? `${mb(r.used)} MB used ${when}, over the ${mb(r.max)} MB allowance` : `${mb(r.used)} of ${mb(r.max)} MB ${when === "then" ? "used then" : "now"}`;

/** What every page about progress says about storage, one sentence a kind, so they can't differ. Empty when there's nothing to say. */
export function storageSentences(p: Progress): string[] {
  const s = p.storage;
  if (!s) return [];
  if ("notCompared" in s) return [NOT_COMPARED[s.notCompared]];
  return s.compared.map((k) => {
    const change = k.now.used - k.then.used;
    const average = k.average === null ? "" : ` (an average of ${mb(k.average)} MB every ${STORAGE_AVERAGE_DAYS} days)`;
    const moved = change === 0 ? `the same as ${dayCount(k.days)} ago` : `${change > 0 ? "up" : "down"} ${mb(Math.abs(change))} MB in ${dayCount(k.days)}${average}`;
    return `${k.label}: ${reading(k.then, "then")}, ${reading(k.now, "now")}, ${moved}.`;
  });
}

const comparedStorage = (p: Progress): StorageChange[] => (p.storage && "compared" in p.storage ? p.storage.compared : []);

/** How many of each list the progress page names. The rest are counted, so the count is a figure too. */
export const PROGRESS_LISTED = 5;
/**
 * How many a deck's progress page names in this report. Its storage sentences
 * take the room of a rule a list, so the full report's slide holds its fullest
 * case (DECISIONS.md 501): what is left out is counted and in the raw notes.
 */
export const progressListed = (p: Progress): number => (p.storage ? PROGRESS_LISTED - 1 : PROGRESS_LISTED);

/** Every figure a page about progress quotes, for the number check. */
export function progressFigures(p: Progress): number[] {
  const pairs = [p.score.before, p.score.after, ...p.areas.flatMap((a) => [a.before, a.after])].filter((n): n is number => n !== null);
  const changes = [
    p.score.before !== null && p.score.after !== null ? Math.abs(p.score.after - p.score.before) : null,
    ...p.areas.map((a) => (a.before !== null && a.after !== null ? Math.abs(a.after - a.before) : null)),
  ].filter((n): n is number => n !== null);
  const listed = progressListed(p);
  const overflow = [p.fixed, p.remaining, p.added].filter((l) => l.length > listed).map((l) => l.length - listed);
  const storage = comparedStorage(p).flatMap((k) => [k.then.used, k.then.max, k.now.used, k.now.max, Math.abs(k.now.used - k.then.used), ...(k.average === null ? [] : [k.average])]);
  // "The two health checks'" reads as a figure, so the sentence that says it carries its 2.
  const notCompared = p.storage && "notCompared" in p.storage && p.storage.notCompared === "not_alike" ? [2] : [];
  const firstTime = p.newlyMeasured?.length ? [p.newlyMeasured.length] : [];
  const changedCheck = p.ruleChanged?.length ? [p.ruleChanged.length] : [];
  const stricterCheck = p.stricter?.length ? [p.stricter.length] : [];
  return [...pairs, ...changes, p.fixed.length, p.remaining.length, p.added.length, p.unknown.length, ...overflow, ...storage, ...notCompared, ...firstTime, ...changedCheck, ...stricterCheck];
}

/** The day counts the storage sentences print ("in 118 days", "every 30 days"). The number check reads them as days, not plain counts. */
export function progressDays(p: Progress): number[] {
  const kinds = comparedStorage(p);
  return [...kinds.map((k) => k.days), ...(kinds.some((k) => k.average !== null) ? [STORAGE_AVERAGE_DAYS] : [])];
}

/**
 * The titles a page about progress lists. A fixed or can't-tell rule's title is
 * the earlier run's, whose findings aren't in this run's data, so the titles go
 * in as text: a figure in one ("90 days") then traces (DECISIONS.md 503).
 */
export const progressTitles = (p: Progress): string[] => [p.fixed, p.remaining, p.added, p.unknown, p.newlyMeasured ?? [], p.stricter ?? []].flatMap((list) => list.map((c) => c.title));

// What the re-check says of issues found by a stricter check (DECISIONS.md 754),
// for the deck's note and the raw notes' list. Our own sentences, made by said so
// an automatic run knows them as ours. No tightening has been declared, so none
// can be printed yet: the owner approves these words before the first one is.

/** The deck's note: one sentence set for one rule, another for several. */
export const stricterNote = (n: number): Said =>
  n === 1
    ? said`We've made one of our checks stricter since your last health check, and it raised an issue it may not have raised before. It's open, but we don't count it as new. The raw notes name it.`
    : said`We've made ${n} of our checks stricter since your last health check, and they raised issues they may not have raised before. They're open, but we don't count them as new. The raw notes name them.`;

/** The raw notes' heading for the list. */
export const stricterHeading = (n: number): Said => said`Found by a stricter check (${n}), so not counted as new:`;

export const worthFigures = (p: Progress): number[] => (p.worth ? [p.worth.before, p.worth.after, Math.abs(p.worth.before - p.worth.after)] : []);
