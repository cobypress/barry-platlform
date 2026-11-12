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
