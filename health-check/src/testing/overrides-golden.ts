import { rmSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { applyOverrides, refusalLines, type OverrideRefusal } from "../narrative/overrides.ts";
import { Finding, Metric, NumberOverride, type Evidence } from "../schemas/index.ts";
import { runFromRecording } from "./harness.ts";
import { loadAnswers } from "./replay.ts";

// The console judges a correction with a copy of src/narrative/overrides.ts
// (DECISIONS.md 532, 533). Both repos run every case in one file, made here
// from the code and the synthetic run, never by hand, so a change to the
// judgement that the copy doesn't follow fails one side or the other.

export const OVERRIDES_GOLDEN = fileURLToPath(new URL("../../fixtures/overrides-golden.json", import.meta.url));
/** How the file is made again, from the health-check folder. */
export const REMAKE = "node src/testing/overrides-golden.ts";

export type GoldenCase = {
  name: string;
  findings: Finding[];
  metrics: Metric[];
  overrides: NumberOverride[];
  expected: { findings: Finding[]; metrics: Metric[]; refused: OverrideRefusal[]; lines: string[] };
};

/** One correction made alone, from the value as read to `newValue`, and the kind of refusal it gets, or null. */
export type GoldenVerdict = { type: "finding" | "metric"; id: string; field: string; newValue: number; refused: null | OverrideRefusal["why"]["kind"] };

export type OverridesGolden = {
  source: string;
  cases: GoldenCase[];
  synthetic: { findings: Finding[]; metrics: Metric[]; verdicts: GoldenVerdict[] };
};

export type Overridable = { type: "finding" | "metric"; id: string; field: string; asRead: number };

/** Every figure the console can correct, in the run's order: each finding's facts and sized amount, each measured metric's value. */
export function overridable(findings: Finding[], metrics: Metric[]): Overridable[] {
  return [
    ...findings.flatMap((f) => [
      ...Object.entries(f.facts).map(([key, asRead]) => ({ type: "finding" as const, id: f.id, field: `facts.${key}`, asRead })),
      ...(f.value.kind === "not_sized" ? [] : [{ type: "finding" as const, id: f.id, field: "value.amount", asRead: f.value.amount }]),
    ]),
    ...metrics.flatMap((m) => (m.status === "measured" ? [{ type: "metric" as const, id: m.id, field: "value", asRead: m.value }] : [])),
  ];
}

const AT = "2026-10-31T13:00:00.000Z";

/** A correction of one figure, as the console stores it. */
export function correction(target: { type: "finding" | "metric"; id: string; field: string }, previousValue: number, newValue: number, n = 1): NumberOverride {
  return { id: `ov-${String(n).padStart(4, "0")}`, runId: "run-0001", target: { type: target.type, id: target.id, field: target.field }, previousValue, newValue, reason: "Checked with the customer.", overriddenBy: "Synthetic Consultant", overriddenAt: AT };
}

/** The synthetic run's verdict for each figure the console can correct, each corrected alone to one more than its value as read. */
export function verdictsOf(findings: Finding[], metrics: Metric[]): GoldenVerdict[] {
  return overridable(findings, metrics).map((x) => {
    const newValue = x.asRead + 1;
    const [refused] = applyOverrides(findings, metrics, [correction(x, x.asRead, newValue)]).refused;
    return { type: x.type, id: x.id, field: x.field, newValue, refused: refused ? refused.why.kind : null };
  });
}

// The hand cases: made-up wording only, ids zero-padded.

const EVIDENCE: Evidence = { kind: "soql", api: "rest", query: "SELECT Id FROM User", count: 11, dateRange: { from: "2024-10-31", to: "2026-10-31" }, exclusions: [], capturedAt: AT };

const findingId = (n: number): string => `f-${String(n).padStart(12, "0")}`;

function finding(n: number, title: string, summary: string, facts: Record<string, number>, edit: Partial<Finding> = {}): Finding {
  return {
    id: findingId(n),
    ruleId: "int.planted",
    ruleVersion: 1,
    domain: "integrations",
    status: "issue",
    title,
    summary,
    recommendation: "Look into it.",
    recommendationType: "change",
    severity: "medium",
    confidence: "high",
    evidence: [EVIDENCE],
    value: { kind: "not_sized", reason: "Not sized." },
    effort: { size: "S" },
    owner: "Salesforce admin",
    dependencies: [],
    guards: [],
    segmentsChecked: [],
    segmentExemptReason: "A single org-wide figure.",
    subjects: [],
    facts,
    notMeasurableReason: null,
    manualStepIds: [],
    ...edit,
  };
}

type Measured = Extract<Metric, { status: "measured" }>;

function measure(n: number, label: string, value: number, edit: Partial<Measured> = {}): Measured {
  return {
    status: "measured",
    id: `planted.measure_${String(n).padStart(2, "0")}`,
    domain: "historic_pipeline",
    label,
    definition: "The share of closed deals that were won.",
    unit: "percent",
    evidence: [EVIDENCE],
    exclusions: [],
    assumptions: [],
    value,
    n: 40,
    dateRange: null,
    segments: [],
    segmentExemptReason: "A single org-wide figure.",
    series: [],
    stats: {},
    ...edit,
  };
}

const onFinding = (f: Finding, field: string, previousValue: number, newValue: number, n = 1): NumberOverride => correction({ type: "finding", id: f.id, field }, previousValue, newValue, n);
const onMetric = (m: Metric, field: string, previousValue: number, newValue: number, n = 1): NumberOverride => correction({ type: "metric", id: m.id, field }, previousValue, newValue, n);

type HandCase = { name: string; findings?: Finding[]; metrics?: Metric[]; overrides: NumberOverride[] };

function handCases(): HandCase[] {
  const cases: HandCase[] = [];
  const add = (c: HandCase): void => void cases.push(c);

  // A finding's refusals, one of each kind.
  const licences = finding(1, "Licences assigned and bought", "8 of 8 licences are assigned.", { assigned: 8, bought: 8 });
  add({ name: "a finding: another figure holds the value as read (shared)", findings: [licences], overrides: [onFinding(licences, "facts.assigned", 8, 7)] });
  const slipped = finding(2, "Deals that slipped", "12 deals slipped, and 12 more are at risk.", { slipped: 12 });
  add({ name: "a finding: the value as read is printed twice (repeated)", findings: [slipped], overrides: [onFinding(slipped, "facts.slipped", 12, 13)] });
  const requests = finding(3, "API requests a day", "About 1.2k requests are made each day.", { requests: 1180 });
  add({ name: "a finding: the value as read is printed rounded (rounded)", findings: [requests], overrides: [onFinding(requests, "facts.requests", 1180, 1250)] });
  const ratio = finding(4, "Admins to users", "Admins to users are 1:40.", { users: 40 });
  add({ name: "a finding: the value as read is only part of a colon form (unsure)", findings: [ratio], overrides: [onFinding(ratio, "facts.users", 40, 44)] });
  const open = finding(5, "Open deals", "30 deals are open.", { open: 30 });
  add({ name: "a finding: the correction was made from another value (stale)", findings: [open], overrides: [onFinding(open, "facts.open", 31, 29)] });

  // Carried, put back, and two at once.
  const pipeline = finding(6, "Pipeline with no next step", "£10,500 of open pipeline has no next step.", { pipeline: 10500 });
  add({ name: "a finding: a grouped figure printed once is rewritten in the form found (carried)", findings: [pipeline], overrides: [onFinding(pipeline, "facts.pipeline", 10500, 9800)] });
  const hidden = finding(7, "Users who never log in", "Some users never log in.", { hidden: 77 });
  add({ name: "a finding: a figure in no text changes and no text does (carried)", findings: [hidden], overrides: [onFinding(hidden, "facts.hidden", 77, 70)] });
  const late = finding(8, "Deals past their close date", "43 deals are past their close date.", { deals: 43 });
  add({ name: "a finding: put back after two rows, nothing changes", findings: [late], overrides: [onFinding(late, "facts.deals", 43, 33, 1), onFinding(late, "facts.deals", 33, 43, 2)] });
  const logins = finding(9, "Users who logged in", "14 of 90 users logged in this month.", { active: 14, users: 90 });
  add({ name: "a finding: two corrections in one finding are carried in one pass", findings: [logins], overrides: [onFinding(logins, "facts.active", 14, 15, 1), onFinding(logins, "facts.users", 90, 95, 2)] });
  const sized = finding(10, "Licences no one uses", "4 licences are unused.", { unused: 4 }, { value: { kind: "licence_cost", amount: 3600, unit: "GBP", period: "per year", assumptions: ["Each licence costs £900 a year."] } });
  add({ name: "a finding: the sized amount in no text changes (carried)", findings: [sized], overrides: [onFinding(sized, "value.amount", 3600, 2700)] });

  // Hours to fix, their time band and a fix prompt (DECISIONS.md 628, 636) are made from the
  // figures as read, so a carried correction drops them, and a refused one or one put back
  // keeps them; the rule's fix complexity stands either way.
  const fix = { effortHours: { low: 2, high: 4, basis: "About 1 to 2 hours for each flow, for 2 flows." }, timeBand: "small" as const, fixComplexity: "moderate" as const, fixPrompt: "What we found: Flows are failing. 2 flows have no fault path." };
  const flows = finding(13, "Flows are failing", "2 flows have no fault path.", { affected: 2 }, fix);
  add({ name: "a finding with hours and a prompt: a carried correction drops both", findings: [flows], overrides: [onFinding(flows, "facts.affected", 2, 5)] });
  const hiddenFix = finding(14, "Flows are failing", "Some flows have no fault path.", { affected: 2 }, fix);
  add({ name: "a finding with hours and a prompt: a carried correction in no text drops both", findings: [hiddenFix], overrides: [onFinding(hiddenFix, "facts.affected", 2, 3)] });
  const twiceFix = finding(15, "Flows are failing", "2 flows have no fault path, and 2 more are slow.", { affected: 2 }, fix);
  add({ name: "a finding with hours and a prompt: a refused correction keeps both", findings: [twiceFix], overrides: [onFinding(twiceFix, "facts.affected", 2, 3)] });
  add({ name: "a finding with hours and a prompt: put back to the value as read keeps both", findings: [flows], overrides: [onFinding(flows, "facts.affected", 2, 5, 1), onFinding(flows, "facts.affected", 5, 2, 2)] });

  // How a refusal's line prints a title.
  const rep = finding(11, "Win rate for rep [[user:U0003]] is well below the overall figure", "Rep [[user:U0003]] won 14% of 43 deals, against 47% for the team.", { deals: 43, segmentValue: 14, overallValue: 47 }, { recommendation: "Look at the 43 deals with the rep's manager." });
  add({ name: "a title holding a person's token prints it as a named person", findings: [rep], overrides: [onFinding(rep, "facts.deals", 43, 44)] });
  const long = finding(12, "A planted finding whose title runs on\nwell past the length a refusal prints, so that the line it gives has to stop short of its end, as this one does", "Seats are open.", { seats: 6 });
  add({ name: "a title over 120 characters with a line break is printed on one line and cut", findings: [long], overrides: [onFinding(long, "facts.seats", 7, 5)] });

  // Rows that change nothing.
  const unmeasured: Metric = { status: "not_measurable", id: "planted.unmeasured", domain: "historic_pipeline", label: "Deals won", definition: "The share of closed deals that were won.", unit: "percent", evidence: [EVIDENCE], exclusions: [], assumptions: [], reason: "Too few closed deals to measure." };
  add({ name: "a correction of a measure that isn't measured is ignored", metrics: [unmeasured], overrides: [onMetric(unmeasured, "value", 30, 35)] });
  const held = finding(13, "Seats held", "Seats are held.", { seats: 9 });
  add({ name: "rows naming a finding or a fact the run doesn't hold are ignored", findings: [held], overrides: [correction({ type: "finding", id: findingId(999), field: "facts.seats" }, 9, 8, 1), onFinding(held, "facts.missing", 9, 8, 2)] });

  // A figure written without a digit 0 to 9 (DECISIONS.md 520).
  const word = finding(14, "Admins who log in", "Five of the 9 admins log in.", { active: 5, admins: 9 });
  add({ name: "a finding: a figure written as a number word is unsure", findings: [word], overrides: [onFinding(word, "facts.active", 5, 6)] });
  const wide = finding(15, "Admins who log in each week", "Only ５ admins log in each week.", { active: 5 });
  add({ name: "a finding: a figure written as a full-width digit is unsure", findings: [wide], overrides: [onFinding(wide, "facts.active", 5, 6)] });

  // A measure's refusals: another of its figures, each where, then its wording, rounded and stale.
  const parts: Array<[string, Measured, string]> = [
    ["value", measure(1, "Deals won", 12, { n: 12 }), "n"],
    ["n", measure(2, "Deals won", 25, { n: 25 }), "value"],
    ["the stat median", measure(3, "Days to close", 38, { stats: { mean: 41, median: 38 } }), "value"],
    ["a segment", measure(4, "Win rate", 55, { segments: [{ dimension: "team", key: "Team A", n: 20, value: 55, notMeasurableReason: null }], segmentExemptReason: null }), "value"],
    ["a point in its series", measure(5, "Win rate", 61, { series: [{ date: "2026-09-30", value: 61, n: 30, notMeasurableReason: null }] }), "value"],
    ["an evidence count", measure(6, "Open deals", 64, { evidence: [{ ...EVIDENCE, count: 64 }] }), "value"],
  ];
  for (const [where, m, field] of parts) {
    const asRead = field === "n" ? m.n : m.value;
    add({ name: `a measure: another of its figures (${where}) holds the value as read (part)`, metrics: [m], overrides: [onMetric(m, field, asRead, asRead + 2)] });
  }
  // Which other figure a refusal names, and the order a finding's refusals are
  // listed in, follow the order of its facts and a measure's stats (DECISIONS.md
  // 532, item 4): the console's copy must keep the order it is given.
  const order = finding(16, "Seats in three places", "8 seats are free.", { zeta: 8, alpha: 8, mid: 8 });
  add({ name: "a finding: two refused figures are named and listed in the order of its facts", findings: [order], overrides: [onFinding(order, "facts.alpha", 8, 9, 1), onFinding(order, "facts.zeta", 8, 7, 2)] });
  const stats = measure(11, "Days to close", 38, { stats: { upper: 38, lower: 38 } });
  add({ name: "a measure: of two stats that hold the value, the first in its stats' order is named", metrics: [stats], overrides: [onMetric(stats, "value", 38, 36)] });
  const worded = measure(7, "People who log in less than weekly", 5, { definition: "Fewer than 5 people log in in a typical week." });
  add({ name: "a measure: its own wording prints the value as read (worded)", metrics: [worded], overrides: [onMetric(worded, "value", 5, 6)] });
  const calls = measure(8, "Calls logged a day", 1180, { definition: "About 1.2k calls are logged each day.", unit: "count" });
  add({ name: "a measure: its own wording prints the value rounded (rounded)", metrics: [calls], overrides: [onMetric(calls, "value", 1180, 1250)] });
  const stale = measure(9, "Win rate", 33.3);
  add({ name: "a measure: the correction was made from another value (stale)", metrics: [stale], overrides: [onMetric(stale, "value", 33, 30)] });
  const carried = measure(10, "Win rate", 72.5);
  add({ name: "a measure: a value no part or text holds changes (carried)", metrics: [carried], overrides: [onMetric(carried, "value", 72.5, 70)] });

  // More than ten: a measure's refusal given first is listed after the findings', and two are counted, not listed.
  const many = Array.from("ABCDEFGHIJK", (team, i) => finding(20 + i, `Seats on team ${team}`, "7 seats are free, and 7 more are held.", { seats: 7 }));
  add({
    name: "twelve refusals: findings' first, ten listed, then how many more",
    findings: many,
    metrics: [worded],
    overrides: [onMetric(worded, "value", 5, 6, 1), ...many.map((f, i) => onFinding(f, "facts.seats", 7, 8, i + 2))],
  });

  // Where the engine isn't beside it, the console's copy of the judgement is
  // held by this file alone (DECISIONS.md 532, 533). Each case from here fails
  // a slip in the copy that the cases above let through: a branch swapped or
  // dropped, a text or part not read, a mask left out.
  for (const c of slipCases()) add(c);
  return cases;
}

/** The cases that hold each branch of the judgement, so a wrong copy fails one by itself. */
function slipCases(): HandCase[] {
  const cases: HandCase[] = [];
  const add = (c: HandCase): void => void cases.push(c);

  // A put-back always saves (DECISIONS.md 533, item 2): the last row decides
  // "unchanged" before the first row is tested for "stale". The measure prints
  // its value, so any other path refuses it.
  const reopened = finding(40, "Open deals", "30 deals are open.", { open: 30 });
  add({ name: "a finding: put back over a stale first row, nothing refused and nothing changes", findings: [reopened], overrides: [onFinding(reopened, "facts.open", 31, 29, 1), onFinding(reopened, "facts.open", 29, 30, 2)] });
  const quiet = measure(12, "People who log in less than weekly", 5, { definition: "Fewer than 5 people log in in a typical week." });
  add({ name: "a measure: put back after two rows from its value as read, nothing refused and nothing changes", metrics: [quiet], overrides: [onMetric(quiet, "value", 5, 6, 1), onMetric(quiet, "value", 6, 5, 2)] });
  add({ name: "a measure: put back over a stale first row, nothing refused and nothing changes", metrics: [quiet], overrides: [onMetric(quiet, "value", 4, 6, 1), onMetric(quiet, "value", 6, 5, 2)] });

  // Corrected twice: the first row is tested against the value as read, the
  // last row's value is carried, and a stale refusal names the first row's
  // value. Rows are judged in the order given, never sorted.
  const bought = finding(41, "Seats bought", "60 seats are bought.", { seats: 60 });
  add({ name: "a finding: corrected twice, the last value is carried", findings: [bought], overrides: [onFinding(bought, "facts.seats", 60, 58, 1), onFinding(bought, "facts.seats", 58, 55, 2)] });
  add({ name: "a finding: corrected twice from a stale first row, the refusal names the first row's value (stale)", findings: [bought], overrides: [onFinding(bought, "facts.seats", 61, 58, 1), onFinding(bought, "facts.seats", 58, 55, 2)] });
  const rate = measure(13, "Win rate", 72.5);
  add({ name: "a measure: corrected twice, the last value is carried", metrics: [rate], overrides: [onMetric(rate, "value", 72.5, 71, 1), onMetric(rate, "value", 71, 70, 2)] });
  add({ name: "a measure: corrected twice from a stale first row, the refusal names the first row's value (stale)", metrics: [rate], overrides: [onMetric(rate, "value", 72, 71, 1), onMetric(rate, "value", 71, 70, 2)] });

  // A measure's texts are its label, definition, assumptions, exclusions, its
  // reason for no segments, and each segment's and point's reason (DECISIONS.md 517).
  const thinTeam = measure(14, "Win rate", 20, { segments: [{ dimension: "team", key: "Team B", n: 3, value: null, notMeasurableReason: "Fewer than 20 closed deals in this team." }], segmentExemptReason: null });
  add({ name: "a measure: its value printed only in a segment's reason (worded)", metrics: [thinTeam], overrides: [onMetric(thinTeam, "value", 20, 22)] });
  const thinMonth = measure(15, "Win rate", 20, { series: [{ date: "2026-09-30", value: null, n: 4, notMeasurableReason: "Fewer than 20 deals closed this month." }] });
  add({ name: "a measure: its value printed only in a series point's reason (worded)", metrics: [thinMonth], overrides: [onMetric(thinMonth, "value", 20, 22)] });
  const texts = [
    measure(16, "Share of the 30 largest deals won", 30),
    measure(17, "Win rate", 30, { segmentExemptReason: "Too few deals in any team: under 30 each." }),
    measure(18, "Win rate", 30, { assumptions: ["A deal counts once it is 30 days old."] }),
    measure(19, "Win rate", 30, { exclusions: ["Deals under 30 days old."] }),
  ];
  add({ name: "a measure: its value printed in its label, its reason for no segments, an assumption or an exclusion (worded, each)", metrics: texts, overrides: texts.map((m, i) => onMetric(m, "value", 30, 31, i + 1)) });
  const fewer = measure(20, "People who never log in", 5, { definition: "Fewer than five people never log in." });
  add({ name: "a measure: its value printed as a number word (worded)", metrics: [fewer], overrides: [onMetric(fewer, "value", 5, 6)] });

  // A measure's n and stats are judged and carried as its value is, and which
  // part a refusal names follows the order: another figure (value, n, then the
  // stats), a segment, a point in its series, an evidence count.
  const days = measure(21, "Days to close", 41.5, { unit: "days", n: 52, stats: { median: 39 } });
  add({ name: "a measure: its n and a stat that no part or text holds are carried", metrics: [days], overrides: [onMetric(days, "n", 52, 50, 1), onMetric(days, "stats.median", 39, 37, 2)] });
  const won = measure(22, "Deals won", 12, { n: 12, stats: { median: 12 } });
  add({ name: "a measure: of n and a stat that hold the value, n is named (part)", metrics: [won], overrides: [onMetric(won, "value", 12, 13)] });
  const held = [
    measure(23, "Win rate", 64, { segments: [{ dimension: "team", key: "Team C", n: 20, value: 64, notMeasurableReason: null }], series: [{ date: "2026-09-30", value: 64, n: 30, notMeasurableReason: null }], evidence: [{ ...EVIDENCE, count: 64 }] }),
    measure(24, "Win rate", 66, { series: [{ date: "2026-09-30", value: 50, n: 66, notMeasurableReason: null }], evidence: [{ ...EVIDENCE, count: 66 }] }),
    measure(25, "Win rate", 68, { stats: { p90: 68 }, segments: [{ dimension: "team", key: "Team D", n: 68, value: 70, notMeasurableReason: null }] }),
  ];
  add({ name: "a measure: of parts that hold the value, the first in order is named (part, each)", metrics: held, overrides: held.map((m, i) => onMetric(m, "value", m.value, m.value + 1, i + 1)) });

  // A year and month is never a figure (DECISIONS.md 514): the month that
  // equals the value as read is masked, so only the figure itself is found
  // and rewritten, and the date stands as written.
  const month = finding(42, "Deals closed in a month", "10 deals closed in 2026-10.", { closed: 10 });
  add({ name: "a finding: its value as read also the month of a year and month, which is masked: carried, the date untouched", findings: [month], overrides: [onFinding(month, "facts.closed", 10, 11)] });
  const counted = finding(43, "Deals counted", "Deals are counted from 2026-12.", { months: 12 });
  add({ name: "a finding: its value as read only the month of a year and month: carried with no text rewritten, the date untouched", findings: [counted], overrides: [onFinding(counted, "facts.months", 12, 13)] });
  // But not one joined to a longer form, before or after: its month is a place.
  const range = finding(44, "Deals that slipped", "Deals from 2025-12-2026-10: 10 slipped.", { slipped: 10 });
  add({ name: "a finding: a year and month joined to a form before it is not masked, so its month is a place (repeated)", findings: [range], overrides: [onFinding(range, "facts.slipped", 10, 11)] });
  const since = finding(45, "Deals that slipped since a day", "Since 2026-10-5, 10 deals slipped.", { slipped: 10 });
  add({ name: "a finding: a year and month joined to a form after it is not masked, so its month is a place (repeated)", findings: [since], overrides: [onFinding(since, "facts.slipped", 10, 11)] });

  // How a found figure is written back: in the form found, grouped from 1,000
  // when it was found grouped or under 1,000, and digits written out in full.
  const owners = finding(46, "Records with no owner", "950 records have no owner.", { records: 950 });
  add({ name: "a finding: a figure under 1,000 corrected to over it is written grouped (carried)", findings: [owners], overrides: [onFinding(owners, "facts.records", 950, 1050)] });
  const read = finding(47, "Records read", "10500 records were read.", { records: 10500 });
  add({ name: "a finding: a figure found ungrouped is written ungrouped (carried)", findings: [read], overrides: [onFinding(read, "facts.records", 10500, 9800)] });
  const rows = finding(48, "Rows counted", "1000000000000000000000 rows were counted.", { rows: 1e21 });
  add({ name: "a finding: a figure of 10^21, which String writes with an exponent, is found and written in full (carried)", findings: [rows], overrides: [onFinding(rows, "facts.rows", 1e21, 2e21)] });

  // Never inside a longer number, and what the 5% test reads.
  const pipeline = finding(49, "Deals with no next step", "£12,500 of pipeline sits in 500 open deals.", { deals: 500 });
  add({ name: "a finding: a value as read inside a grouped number is not a place (carried)", findings: [pipeline], overrides: [onFinding(pipeline, "facts.deals", 500, 480)] });
  const stale = finding(50, "Stale pipeline", "About £12,500 of pipeline is stale.", { pipeline: 12400 });
  add({ name: "a finding: a grouped number near the value as read is read whole (rounded)", findings: [stale], overrides: [onFinding(stale, "facts.pipeline", 12400, 11000)] });
  const apps = finding(51, "Apps on an old version", "Each app runs version 4.5.6.", { apps: 6 });
  add({ name: "a finding: the last digit of a dotted form is not a number of its own (carried)", findings: [apps], overrides: [onFinding(apps, "facts.apps", 6, 7)] });
  const about = finding(52, "Open deals", "About 105 deals are open.", { open: 100 });
  add({ name: "a finding: a number exactly 5% from the value as read may be it rounded (rounded)", findings: [about], overrides: [onFinding(about, "facts.open", 100, 90)] });
  const fell = finding(53, "Change in win rate", "The win rate fell by 12 points this year.", { change: -12 });
  add({ name: "a finding: a negative value as read is near its size printed unsigned (rounded)", findings: [fell], overrides: [onFinding(fell, "facts.change", -12, -10)] });

  // What is masked, never a place: a token's digits, a date with its time, an
  // identifier, and digits joined to a letter in a guessed date.
  const owned = finding(54, "Deals with an owner", "Some deals are owned by [[user:U0003]].", { owners: 3 });
  add({ name: "a finding: a token's digits are not the value as read (carried)", findings: [owned], overrides: [onFinding(owned, "facts.owners", 3, 4)] });
  const at = finding(55, "Open deals at the read", "Read at 2026-10-31T13:00:00Z, 10 deals were open.", { open: 10 });
  add({ name: "a finding: an ISO date with its time is masked whole (carried)", findings: [at], overrides: [onFinding(at, "facts.open", 10, 11)] });
  const field = finding(56, "Deals using a field", "42 deals use the field Score_42__c.", { deals: 42 });
  add({ name: "a finding: an identifier's digits are not a place (carried)", findings: [field], overrides: [onFinding(field, "facts.deals", 42, 43)] });
  const jobs = finding(57, "Jobs that stop", "By 3rd May 2026 the 3 jobs stop.", { jobs: 3 });
  add({ name: "a finding: a guessed date's digits joined to a letter after are masked (carried)", findings: [jobs], overrides: [onFinding(jobs, "facts.jobs", 3, 4)] });
  const teams = finding(58, "Deals that slipped in a quarter", "In Q3 12 deals slipped across the sales teams.", { teams: 3 });
  add({ name: "a finding: a guessed date's digits joined to a letter before are masked (carried)", findings: [teams], overrides: [onFinding(teams, "facts.teams", 3, 4)] });
  const logged = finding(59, "Deals logged in a month", "Deals logged in 2026-02 number 2,026.", { deals: 2026 });
  add({ name: "a finding: a guessed year inside a year and month is not a part (carried)", findings: [logged], overrides: [onFinding(logged, "facts.deals", 2026, 2027)] });

  // What is unsure: a guessed year, a colon form with a grouped or a decimal
  // part, and with a place beside it, both counted.
  const users = finding(60, "Active users", "12 of 2000 active users log in.", { users: 2000 });
  add({ name: "a finding: a bare year guessed from the word before is unsure (unsure)", findings: [users], overrides: [onFinding(users, "facts.users", 2000, 2100)] });
  const thousand = finding(61, "Admins to users", "Admins to users are 1,000:1.", { users: 1000 });
  add({ name: "a finding: a colon form with a grouped part (unsure)", findings: [thousand], overrides: [onFinding(thousand, "facts.users", 1000, 1100)] });
  const half = finding(62, "Admins to users", "Admins to users are 1.5:1.", { ratio: 1.5 });
  add({ name: "a finding: a colon form with a decimal part (unsure)", findings: [half], overrides: [onFinding(half, "facts.ratio", 1.5, 2)] });
  const both = finding(63, "Admins to users", "Admins to users are 1:40, across 40 users.", { users: 40 });
  add({ name: "a finding: a place and an unsure form are counted together (repeated)", findings: [both], overrides: [onFinding(both, "facts.users", 40, 44)] });
  const meetings = finding(64, "Meetings a team holds", "Five teams meet at 5:00 each day.", { meetings: 5.1 });
  add({ name: "a finding: of two unsure forms near the value as read, the first in the text is named (rounded)", findings: [meetings], overrides: [onFinding(meetings, "facts.meetings", 5.1, 6)] });
  // A digit outside the basic plane is two UTF-16 units that normalise to one
  // "5": the text is cut only where each side normalises alone, so the form is named whole.
  const bold = finding(65, "Admins who log in each week", "Only 𝟓 admins log in each week.", { active: 5 });
  add({ name: "a finding: a figure written as a digit outside the basic plane is unsure, and named whole (unsure)", findings: [bold], overrides: [onFinding(bold, "facts.active", 5, 6)] });

  // The rewrite: two places in one text whose lengths change, and a figure in
  // each text a finding has, each judged and rewritten where it stands.
  const active = finding(66, "Users who logged in", "9 of 90 users logged in this month.", { active: 9, users: 90 });
  add({ name: "a finding: two places in one text whose lengths change are rewritten last first (carried)", findings: [active], overrides: [onFinding(active, "facts.active", 9, 10, 1), onFinding(active, "facts.users", 90, 95, 2)] });
  const everywhere = finding(
    67,
    "Integrations that run as 17 users",
    "They make 230 calls a day.",
    { users: 17, calls: 230, jobs: 31, licences: 6, months: 12, days: 3, developers: 2, tickets: 47, archived: 58, allJobs: 81, idle: 96 },
    {
      recommendation: "Move the 31 jobs to their own users.",
      value: { kind: "licence_cost", amount: 4200, unit: "GBP", period: "over 12 months", assumptions: ["Each of the 6 licences costs £700 a year."] },
      effort: { size: "M", note: "About 3 days of work." },
      owner: "Salesforce admin and 2 developers",
      dependencies: ["Close the 47 open tickets first."],
      notMeasurableReason: "The 58 archived jobs could not be read.",
      segmentExemptReason: "One org-wide figure across 81 jobs.",
      guards: [{ guardId: "int.planted.guard", subject: "jobs", status: "passed", detail: "The 96 jobs have not run in the last week.", evidence: [EVIDENCE] }],
    },
  );
  const unsized = finding(68, "Hours spent on manual exports", "Exports are made by hand.", { hours: 14 }, { value: { kind: "not_sized", reason: "Not sized: the 14 hours a week are an estimate." } });
  add({
    name: "a finding: a figure printed once in each text a finding has is rewritten there (carried)",
    findings: [everywhere, unsized],
    overrides: [...Object.entries(everywhere.facts).map(([key, value], i) => onFinding(everywhere, `facts.${key}`, value, value + 100, i + 1)), onFinding(everywhere, "value.amount", 4200, 3600, 20), onFinding(unsized, "facts.hours", 14, 16, 21)],
  });

  // A fact and the sized amount that share a value: each names the other, the fact first.
  const managed = finding(69, "Seats with no manager", "8 seats need a manager.", { seats: 8 }, { value: { kind: "hours", amount: 8, unit: "hours", period: "per week", assumptions: ["Each seat takes time to manage."] } });
  add({ name: "a finding: a fact and the sized amount share a value, the fact's refusal first (shared)", findings: [managed], overrides: [onFinding(managed, "value.amount", 8, 9, 1), onFinding(managed, "facts.seats", 8, 9, 2)] });

  // A row's type is part of what it names.
  const seats = finding(70, "Seats held", "Seats are held.", { seats: 9 });
  const winRate = measure(26, "Win rate", 44);
  add({
    name: "rows naming a finding's id as a measure's, or a measure's as a finding's, are ignored",
    findings: [seats],
    metrics: [winRate],
    overrides: [correction({ type: "metric", id: seats.id, field: "facts.seats" }, 9, 8, 1), correction({ type: "finding", id: winRate.id, field: "value" }, 44, 45, 2)],
  });
  return cases;
}

/** Throws on anything JSON can't keep as it is: undefined, -0, NaN or Infinity. */
function assertJsonSafe(value: unknown, path: string): void {
  if (value === undefined) throw new Error(`${path} is undefined`);
  if (typeof value === "number" && (Object.is(value, -0) || !Number.isFinite(value))) throw new Error(`${path} is ${value}`);
  if (Array.isArray(value)) value.forEach((x, i) => assertJsonSafe(x, `${path}[${i}]`));
  else if (value !== null && typeof value === "object") for (const [key, x] of Object.entries(value)) assertJsonSafe(x, `${path}.${key}`);
}

/** The whole file, from the code and the synthetic run. */
export async function overridesGolden(): Promise<OverridesGolden> {
  const cases = handCases().map(({ name, findings = [], metrics = [], overrides }): GoldenCase => {
    for (const f of findings) Finding.parse(f);
    for (const m of metrics) Metric.parse(m);
    for (const o of overrides) NumberOverride.parse(o);
    const result = applyOverrides(findings, metrics, overrides);
    return { name, findings, metrics, overrides, expected: { ...result, lines: result.refused.length > 0 ? refusalLines(result.refused) : [] } };
  });
  const { output, dir } = await runFromRecording({ answers: loadAnswers() });
  rmSync(dir, { recursive: true, force: true });
  const { findings, metrics } = output!;
  const golden: OverridesGolden = { source: "health-check/src/narrative/overrides.ts", cases, synthetic: { findings, metrics, verdicts: verdictsOf(findings, metrics) } };
  assertJsonSafe(golden, "golden");
  return golden;
}

/** The file's text, one space a level as the recording is. */
export const asText = (golden: OverridesGolden): string => `${JSON.stringify(golden, null, 1)}\n`;

if (import.meta.main) {
  const text = asText(await overridesGolden());
  writeFileSync(OVERRIDES_GOLDEN, text);
  console.log(`Wrote ${OVERRIDES_GOLDEN}: ${text.length} bytes.`);
}
