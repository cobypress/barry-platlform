import { pct, type RuleContext } from "../../engine/rule.ts";
import type { Finding, Metric, MetricSegment } from "../../schemas/index.ts";
import type { Deal } from "./cohort.ts";
import { base, dimensions, fiscalStartOf, groupBy, measured, notMeasurable, pushesOf, quantile, round1 } from "./metrics.ts";
import { backwardMoves, expectedNote, load as loadProcess } from "./process.ts";

// Roadmap 3.3: what separates the customer's own wins from their losses. The
// report's findings are symptoms; this says which of the things we can read for
// every deal went with losing, in deals that have already closed. It compares,
// and never claims a cause: a gap says two things went together, nothing more.

type Ctx = Omit<RuleContext, "metrics">;
type Measured = Extract<Metric, { status: "measured" }>;

const DAY = 86_400_000;

export const WIN_DRIVERS = "historic.win_drivers";
/** A gap in win rate of at least this many points is a driver. Smaller gaps are still shown. */
export const DRIVER_GAP = 10;
/** As cur.close-date-pushes counts a close date that keeps moving. */
const REPEAT_PUSHES = 2;
/** A long cycle is longer than this share of won deals took, the same line as a long wait in a stage. */
const LONG_SHARE = 0.75;

export type FactorKey = "activity" | "pushed" | "skipped" | "backwards" | "long";

type Factor = {
  key: FactorKey;
  label: string;
  /** The side a finding's symptom is on. Deals with no activity, for activity; with the factor for the rest. */
  risk: "with" | "without";
  /** Deals on the risk side, as a sentence names them: "the 40 that ...". */
  riskPhrase: string;
  /** Issue rules about the same thing, which a driver is linked to. */
  ruleIds: string[];
};

export const FACTORS: Factor[] = [
  { key: "activity", label: "Had logged activity", risk: "without", riskPhrase: "had no logged activity", ruleIds: ["dq.open-opps-no-activity"] },
  { key: "pushed", label: "Close date pushed twice or more", risk: "with", riskPhrase: "had their close date pushed twice or more", ruleIds: ["cur.close-date-pushes"] },
  { key: "skipped", label: "Moved past an expected stage without entering it", risk: "with", riskPhrase: "moved past a stage your process expects without entering it", ruleIds: ["pipe.skipped-stages"] },
  { key: "backwards", label: "Went back a stage at least once", risk: "with", riskPhrase: "went back a stage at least once", ruleIds: ["pipe.going-backwards"] },
  { key: "long", label: "Took longer than 75% of won deals", risk: "with", riskPhrase: "took longer from creation to close than 75% of your won deals did", ruleIds: [] },
];

// Short, because the deck's definitions page prints it. The factors are listed in the assumptions.
const DEFINITION =
  "Won and lost deals that closed in the window after being open, compared on things we can read for every deal: for each, the win rate with it and without it, and the gap in points. A gap of 10 points or more is a driver. It shows how often things went together, not that one caused the other. The value is the win rate of all these deals.";
const FACTOR_NOTE = `The factors: ${FACTORS.map((f) => f.label.toLowerCase()).join("; ")}. Win rate is won deals as a share of won and lost deals, as in the headline win rate, so no decision deals are left out`;

const isWon = (d: Deal): boolean => d.category === "won";
const winRate = (deals: Deal[]): number => pct(deals.filter(isWon).length, deals.length);

type Test = { has: (d: Deal) => boolean } | { reason: string };

/**
 * Rule 4 for a gap: the same comparison inside each team, record type, rep and
 * so on, where both sides have enough deals. A gap that only exists because one
 * team both loses more and does the thing more turns round inside the segments.
 */
function withinSegments(ctx: Ctx, deals: Deal[], has: (d: Deal) => boolean, overall: number): { compared: number; held: number } {
  const { minSample } = ctx.calibration;
  let compared = 0;
  let held = 0;
  for (const keyOf of Object.values(dimensions(ctx))) {
    for (const group of groupBy(deals, keyOf).values()) {
      const yes = group.filter(has);
      const no = group.filter((d) => !has(d));
      if (yes.length < minSample || no.length < minSample) continue;
      compared++;
      const gap = winRate(yes) - winRate(no);
      if (gap !== 0 && Math.sign(gap) === Math.sign(overall)) held++;
    }
  }
  return { compared, held };
}

export function driverMetrics(ctx: Ctx): Metric[] {
  const definition = { id: WIN_DRIVERS, label: "What separates your wins from your losses", unit: "percent" as const, definition: DEFINITION };
  const loaded = loadProcess(ctx);
  if (loaded.status !== "ok") return [notMeasurable(base(ctx, loaded.data, definition), loaded.reason)];
  const { data, process: p } = loaded;
  const { minSample } = ctx.calibration;
  // Lost here is lost only, as the headline win rate has it. No decision deals are counted and set aside.
  const deals = p.closed.filter((d) => d.category === "won" || d.category === "lost");
  const noDecision = p.closed.length - deals.length;
  const won = deals.filter(isWon);

  const fiscal = fiscalStartOf(ctx);
  const orderOf = data.stages.orderOf;
  const cycle = (d: Deal): number => Math.max(0, (d.closedAt.getTime() - new Date(d.CreatedDate).getTime()) / DAY);
  const line = won.length >= minSample ? round1(quantile(won.map(cycle), LONG_SHARE)) : null;
  // A skip is a move past an expected stage into a later open one. A close isn't
  // a move past anything: counting a won close would make every won deal skip
  // the stages nobody uses, and no lost deal could.
  const skippedPast = (d: Deal): boolean => {
    const path = p.paths.get(d.Id)!;
    return p.expected.some((stage) => !path.includes(stage) && path.some((s) => (orderOf(s) ?? -1) > (orderOf(stage) ?? Number.MAX_SAFE_INTEGER)));
  };
  const tests: Record<FactorKey, Test> = {
    activity: { has: (d) => d.LastActivityDate !== null },
    pushed: { has: (d) => pushesOf(d, fiscal).length >= REPEAT_PUSHES },
    skipped: p.expected.length ? { has: skippedPast } : { reason: "No open stage was passed through by more than half of won deals, and the calibration marks none as required, so no stage is expected." },
    backwards: { has: (d) => backwardMoves(p.paths.get(d.Id)!, orderOf) > 0 },
    long: line === null ? { reason: `${won.length} won deals is below the calibrated minimum of ${minSample}, too few to say how long a won deal usually takes.` } : { has: (d) => cycle(d) > line },
  };

  const assumptions = [
    FACTOR_NOTE,
    "Logged activity means the deal has a last activity date: at least one task or event was logged against it",
    ...(p.expected.length ? [expectedNote(p)] : []),
    ...(line === null ? [] : [`A long cycle is longer than ${line} days from creation to close, the time 75% of won deals closed within`]),
  ];
  const exclusions = [...data.exclusions, ...(noDecision ? [`${noDecision} no decision deals, which the win rate leaves out`] : [])];
  const b = { ...base(ctx, data, { ...definition, assumptions }), exclusions };
  if (deals.length < minSample) return [notMeasurable(b, `${deals.length} won and lost deals with stage history is below the calibrated minimum of ${minSample}.`)];

  const segments: MetricSegment[] = [];
  const stats: Record<string, number> = { deals: deals.length, won: won.length, lost: deals.length - won.length, noDecision, minGap: DRIVER_GAP, ...(line === null ? {} : { cycleLineDays: line }) };
  const reasons: string[] = [];
  for (const factor of FACTORS) {
    const test = tests[factor.key];
    const yes = "has" in test ? deals.filter(test.has) : [];
    const no = "has" in test ? deals.filter((d) => !test.has(d)) : [];
    const thin = "has" in test ? [yes, no].find((side) => side.length < minSample) : undefined;
    const reason = "reason" in test ? test.reason : thin ? `${thin.length} deals ${thin === yes ? "with" : "without"} it, below the calibrated minimum of ${minSample} on each side.` : null;
    const side = (key: "with" | "without", group: Deal[]): MetricSegment => ({ dimension: factor.label, key, n: group.length, value: reason ? null : winRate(group), notMeasurableReason: reason });
    segments.push(side("with", yes), side("without", no));
    if (reason !== null || !("has" in test)) {
      reasons.push(`${factor.label}: ${reason}`);
      continue;
    }
    // From the rates as printed, so the gap on the page is the difference of the figures beside it.
    const gap = round1(winRate(yes) - winRate(no));
    const { compared, held } = withinSegments(ctx, deals, test.has, gap);
    Object.assign(stats, { [`${factor.key}Gap`]: Math.abs(gap), [`${factor.key}WithWon`]: yes.filter(isWon).length, [`${factor.key}WithoutWon`]: no.filter(isWon).length, [`${factor.key}Compared`]: compared, [`${factor.key}Held`]: held });
  }
  if (reasons.length === FACTORS.length) return [notMeasurable(b, `No factor has enough deals on both sides to compare. ${reasons.join(" ")}`)];
  return [measured(b, { value: winRate(deals), n: deals.length, dateRange: ctx.window, segments, stats })];
}

// ─── Reading it back ─────────────────────────────────────────────────────────

export type Side = { n: number; rate: number | null; won: number | null };

export type Driver = Factor & {
  with: Side;
  without: Side;
  /** Points between the two sides, always positive. Null when not measurable. */
  gap: number | null;
  /** Segments with enough deals on both sides, and how many of them the gap points the same way in. */
  compared: number;
  held: number;
  reason: string | null;
  /** The risk side won less, which is what a finding's symptom would predict. */
  riskLoses: boolean;
  isDriver: boolean;
};

/** The factors from the metric, largest gap first, then the ones that couldn't be compared. */
export function readDrivers(m: Measured): Driver[] {
  const drivers = FACTORS.map((factor): Driver => {
    const at = (key: "with" | "without"): MetricSegment | undefined => m.segments.find((s) => s.dimension === factor.label && s.key === key);
    const stat = (name: string): number | null => m.stats[`${factor.key}${name}`] ?? null;
    const [yes, no] = [at("with"), at("without")];
    const gap = stat("Gap");
    const side = (s: MetricSegment | undefined, won: number | null): Side => ({ n: s?.n ?? 0, rate: s?.value ?? null, won });
    const sides = { with: side(yes, stat("WithWon")), without: side(no, stat("WithoutWon")) };
    const compared = stat("Compared") ?? 0;
    const held = stat("Held") ?? 0;
    const other = factor.risk === "with" ? "without" : "with";
    const riskLoses = gap !== null && sides[factor.risk].rate !== null && sides[other].rate !== null && sides[factor.risk].rate! < sides[other].rate!;
    // A gap that turns round in most of the segments it can be checked in is the mix of deals, not the factor.
    return { ...factor, ...sides, gap, compared, held, reason: yes?.notMeasurableReason ?? null, riskLoses, isDriver: gap !== null && gap >= DRIVER_GAP && held * 2 >= compared };
  });
  return [...drivers.filter((d) => d.gap !== null).sort((a, b) => b.gap! - a.gap!), ...drivers.filter((d) => d.gap === null)];
}

/** One driver as a sentence, built from the metric's own figures. */
export function driverSentence(d: Driver): string {
  const risk = d[d.risk];
  const other = d[d.risk === "with" ? "without" : "with"];
  const within = d.compared
    ? ` The gap points the same way in ${d.held} of the ${d.compared} segments, such as teams and record types, with enough deals on both sides.`
    : " No team, record type or other segment had enough deals on both sides to check it within.";
  return `Measured from your own closed deals: the ${risk.n} that ${d.riskPhrase} won ${risk.rate}% of the time, against ${other.rate}% for the other ${other.n}, a gap of ${d.gap} points.${within} It shows how often the two went together, not proof that one caused the other.`;
}

export type Cause = { factor: FactorKey; findingIds: string[]; sentence: string };

/**
 * Drivers linked to the issues about the same thing. Only where the symptom's
 * side lost more: a gap the other way is on the slide, and isn't attached to a
 * finding it would contradict.
 */
export function causesOf(metrics: Metric[], findings: Finding[]): Cause[] {
  const m = metrics.find((x) => x.id === WIN_DRIVERS);
  if (m?.status !== "measured") return [];
  return readDrivers(m).flatMap((d) => {
    const findingIds = findings.filter((f) => f.status === "issue" && d.ruleIds.includes(f.ruleId)).map((f) => f.id);
    return d.isDriver && d.riskLoses && findingIds.length ? [{ factor: d.key, findingIds, sentence: driverSentence(d) }] : [];
  });
}
