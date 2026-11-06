import type { Finding, Run } from "../schemas/index.ts";

// The value model (roadmap 1.4): one figure for what the issues are worth over
// the next 12 months, with a low and a high case. Code works it all out from
// the findings' own figures; nothing here is estimated where a finding has none.
//
// Three rules keep it honest:
// - Figures that describe the same money share a group, and only the largest in
//   a group counts. Five ways of finding stalled deals are one pot of deals.
// - A finding's figure is what's there if every case were won back. We count a
//   share of it, the realisation rate, set per group and written down below.
//   The expected case is the conservative one.
// - Pipeline at risk, a coverage gap or campaign spend is exposure, not money
//   found, so it's shown beside the total and never added to it.

export type ValueKind = "revenue_recoverable" | "conversion_uplift" | "licence_waste" | "time_cost" | "exposure";

export const KIND_LABELS: Record<ValueKind, string> = {
  revenue_recoverable: "Revenue you can recover",
  conversion_uplift: "Revenue from closing the gap with your best teams",
  licence_waste: "Licences you pay for and don't use",
  time_cost: "Time lost to manual work",
  exposure: "At risk",
};

/** The kinds that make up the headline, in the order they're shown. */
export const HEADLINE_KINDS: ValueKind[] = ["revenue_recoverable", "conversion_uplift", "licence_waste", "time_cost"];

export type Realisation = { low: number; expected: number; high: number };

type GroupSpec = { kind: ValueKind; label: string; realisation: Realisation };

// Our planning assumptions, not measurements. Open deals are already weighted at
// the org's own win rates, so they keep more of their figure than leads or past
// customers, whose figures assume every one is worked.
const OPEN_DEALS: Realisation = { low: 0.25, expected: 0.5, high: 1 };
const LONG_SHOTS: Realisation = { low: 0.1, expected: 0.25, high: 0.5 };
const SEATS: Realisation = { low: 0.5, expected: 0.75, high: 1 };
const HOURS: Realisation = { low: 0.5, expected: 0.75, high: 1 };
const EXPOSED: Realisation = { low: 1, expected: 1, high: 1 };

export const GROUPS: Record<string, GroupSpec> = {
  "stalled-deals": { kind: "revenue_recoverable", label: "Stalled and neglected open deals", realisation: OPEN_DEALS },
  "unworked-leads": { kind: "revenue_recoverable", label: "Leads nobody worked", realisation: LONG_SHOTS },
  "past-customers": { kind: "revenue_recoverable", label: "Past customers and lost deals worth another look", realisation: LONG_SHOTS },
  "segment-gaps": { kind: "conversion_uplift", label: "Teams and segments winning less often than the rest", realisation: LONG_SHOTS },
  "unused-seats": { kind: "licence_waste", label: "Seats held by people who don't log in", realisation: SEATS },
  "unassigned-seats": { kind: "licence_waste", label: "Seats nobody is assigned to", realisation: SEATS },
  "team-time": { kind: "time_cost", label: "Time your team says it loses", realisation: HOURS },
};

/** Which group each rule's figure belongs to. A rule not listed gets a group of its own, by the kind of its figure. */
export const RULE_GROUPS: Record<string, string> = {
  "dq.open-opps-past-close": "stalled-deals",
  "dq.opps-inactive-owner": "stalled-deals",
  "dq.open-opps-no-activity": "stalled-deals",
  "cur.stale-deals": "stalled-deals",
  "rev.stalled-deals": "stalled-deals",
  "dq.stale-leads": "unworked-leads",
  "lead.no-first-response": "unworked-leads",
  // A dormant account and a lost deal can be the same customer, so they're one pot.
  "rev.dormant-accounts": "past-customers",
  "rev.win-backs": "past-customers",
  "pipe.segment-divergence": "segment-gaps",
  "utl.dormant-licensed-users": "unused-seats",
  "utl.never-logged-in": "unused-seats",
  "utl.unassigned-licence-seats": "unassigned-seats",
};

/**
 * What the team survey says time costs: the median hours a week each person
 * loses to admin and re-typing, for every active user, over a working year. It
 * shares a group with the time the findings put a figure on, because they can
 * be the same hours, and only the largest counts (DECISIONS.md 143).
 */
export const TEAM_TIME_GROUP = "team-time";
export const TEAM_TIME_LINE = "survey:team-time";
/** A working year: 52 weeks less the 5.6 weeks of UK statutory annual leave, bank holidays included, rounded down. */
export const WORKING_WEEKS = 46;
export type TeamTime = { hoursPerWeek: number; users: number; usersFrom: string };

/** Exposure: real money, but not money found. */
export const EXPOSURE_RULES = new Set(["cur.coverage", "cur.stuck-in-stage", "cur.close-date-pushes", "cur.account-concentration", "fct.commit-at-risk", "fct.early-stage-commits", "lead.campaign-return"]);

// The fallback hourly cost when the customer gives none. ONS's median full-time
// salary, plus what an employer pays on top, over a standard working year.
export const UK_HOURLY_BENCHMARK = {
  salary: 39039,
  source: "ONS, Annual Survey of Hours and Earnings 2025 (provisional): median gross annual pay for full-time employees",
  url: "https://www.ons.gov.uk/employmentandlabourmarket/peopleinwork/earningsandworkinghours/bulletins/annualsurveyofhoursandearnings/2025",
  checked: "2026-11-05",
  // Employer National Insurance from April 2025: 15% above £5,000 a year.
  employerNiRate: 0.15,
  niThreshold: 5000,
  // The auto-enrolment minimum: 3% from the employer on earnings between £6,240 and £50,270.
  pensionRate: 0.03,
  pensionFrom: 6240,
  pensionTo: 50270,
  hoursPerWeek: 37.5,
  weeksPerYear: 52,
} as const;

export type HourlyCost = { rate: number; source: "customer" | "benchmark"; working: string[]; figures: { ni: number; pension: number; loaded: number; hours: number } | null };

const round2 = (n: number): number => Math.round(n * 100) / 100;

export function hourlyCost(customerRate: number | null | undefined): HourlyCost {
  if (customerRate && customerRate > 0) return { rate: round2(customerRate), source: "customer", working: ["The hourly cost you gave us."], figures: null };
  const b = UK_HOURLY_BENCHMARK;
  const ni = round2(b.employerNiRate * Math.max(0, b.salary - b.niThreshold));
  const pension = round2(b.pensionRate * Math.max(0, Math.min(b.salary, b.pensionTo) - b.pensionFrom));
  const hours = b.hoursPerWeek * b.weeksPerYear;
  const loaded = round2(b.salary + ni + pension);
  return {
    rate: round2(loaded / hours),
    source: "benchmark",
    working: [
      `${b.source}: £${b.salary.toLocaleString("en-GB")}.`,
      `Plus employer National Insurance of £${ni.toLocaleString("en-GB")} and a minimum employer pension of £${pension.toLocaleString("en-GB")}, so £${loaded.toLocaleString("en-GB")} a year.`,
      `Over ${hours.toLocaleString("en-GB")} hours a year, ${b.hoursPerWeek} hours a week.`,
    ],
    figures: { ni, pension, loaded, hours },
  };
}

export type ValueLine = {
  findingId: string;
  ruleId: string;
  title: string;
  kind: ValueKind;
  group: string;
  /** The finding's own figure over 12 months, in the report currency, before the realisation rate. */
  annual: number;
  low: number;
  expected: number;
  high: number;
  /** How the finding's figure became the annual one. */
  basis: string;
};

export type ValueGroup = { id: string; kind: ValueKind; label: string; realisation: Realisation; lines: ValueLine[]; counted: ValueLine };

export type Range = { low: number; expected: number; high: number };

export type ValueModel = {
  currency: string;
  hourly: HourlyCost;
  groups: ValueGroup[];
  /** Over the next 12 months: the headline kinds' groups, each counted once. */
  total: Range;
  byKind: Record<ValueKind, Range>;
  /** Figures with no place in the model, and why. */
  left: Array<{ findingId: string; reason: string }>;
};

const ONE_OFF = /^(potential revenue in deals open today|one-off\b)/i;
const PER_YEAR = /^(per year|a year)$/i;
// A figure measured over the analysis window: the gap in wins, or discount beyond what peers gave (DECISIONS.md 218).
const OVER_WINDOW = /^(in wins the gap may have cost|in discount beyond what peers gave)\b/i;

function yearsIn(run: Run): number {
  const days = (Date.parse(run.analysisWindow.to) - Date.parse(run.analysisWindow.from)) / 86_400_000;
  return days / 365;
}

const zero = (): Range => ({ low: 0, expected: 0, high: 0 });

export function buildValueModel(findings: Finding[], run: Run, currency: string, customerHourly?: number | null, teamTime?: TeamTime | null): ValueModel {
  const hourly = hourlyCost(customerHourly);
  const lines: ValueLine[] = [];
  const team = teamTime && teamTime.hoursPerWeek > 0 && teamTime.users > 0 ? teamTime : null;
  const left: ValueModel["left"] = [];
  for (const f of findings) {
    if (f.status !== "issue" || f.value.kind === "not_sized") continue;
    const v = f.value;
    let annual: number;
    let basis: string;
    if (v.kind === "hours") {
      if (!PER_YEAR.test(v.period)) {
        left.push({ findingId: f.id, reason: `Its hours aren't a yearly figure (${v.period}).` });
        continue;
      }
      annual = round2(v.amount * hourly.rate);
      basis = `${v.amount} hours a year at £${hourly.rate} an hour`;
    } else if (/^[A-Z]{3}$/.test(v.unit) && v.unit !== currency) {
      left.push({ findingId: f.id, reason: `Its figure is in ${v.unit}, not ${currency}.` });
      continue;
    } else if (PER_YEAR.test(v.period)) {
      annual = v.amount;
      basis = "a yearly figure";
    } else if (ONE_OFF.test(v.period) || EXPOSURE_RULES.has(f.ruleId)) {
      annual = v.amount;
      basis = "counted once, within the next 12 months";
    } else if (OVER_WINDOW.test(v.period)) {
      const years = yearsIn(run);
      annual = round2(v.amount / years);
      basis = `over ${round2(years)} years of history, so divided by ${round2(years)} for one year`;
    } else {
      left.push({ findingId: f.id, reason: `We don't yet know how to put its figure (${v.period}) over 12 months.` });
      continue;
    }
    const known = RULE_GROUPS[f.ruleId];
    const kind: ValueKind = EXPOSURE_RULES.has(f.ruleId) ? "exposure" : known ? GROUPS[known]!.kind : v.kind === "licence_cost" ? "licence_waste" : v.kind === "hours" ? "time_cost" : "exposure";
    // With the survey's figure, every time line joins it, so the same hours aren't counted twice.
    const group = EXPOSURE_RULES.has(f.ruleId) ? f.ruleId : (known ?? (team && kind === "time_cost" ? TEAM_TIME_GROUP : f.ruleId));
    const realisation = known ? GROUPS[known]!.realisation : kind === "licence_waste" ? SEATS : kind === "time_cost" ? HOURS : EXPOSED;
    lines.push({ findingId: f.id, ruleId: f.ruleId, title: f.title, kind, group, annual, low: round2(annual * realisation.low), expected: round2(annual * realisation.expected), high: round2(annual * realisation.high), basis });
  }

  if (team) {
    const annual = round2(team.hoursPerWeek * team.users * hourly.rate * WORKING_WEEKS);
    lines.push({
      findingId: TEAM_TIME_LINE,
      ruleId: "survey.hours_lost",
      title: "Time your team says it loses",
      kind: "time_cost",
      group: TEAM_TIME_GROUP,
      annual,
      low: round2(annual * HOURS.low),
      expected: round2(annual * HOURS.expected),
      high: round2(annual * HOURS.high),
      basis: `${team.hoursPerWeek} hours a week, the median your team gives, for ${team.users} active users (${team.usersFrom}), at £${hourly.rate} an hour over ${WORKING_WEEKS} working weeks`,
    });
  }

  const groups: ValueGroup[] = [];
  for (const line of lines) {
    let g = groups.find((x) => x.id === line.group);
    if (!g) {
      const spec = GROUPS[line.group];
      const realisation = spec?.realisation ?? (line.kind === "licence_waste" ? SEATS : line.kind === "time_cost" ? HOURS : EXPOSED);
      g = { id: line.group, kind: line.kind, label: spec?.label ?? line.title, realisation, lines: [], counted: line };
      groups.push(g);
    }
    g.lines.push(line);
    if (line.expected > g.counted.expected) g.counted = line;
  }
  for (const g of groups) g.lines.sort((a, b) => b.expected - a.expected);
  groups.sort((a, b) => HEADLINE_KINDS.indexOf(a.kind) - HEADLINE_KINDS.indexOf(b.kind) || b.counted.expected - a.counted.expected);

  const byKind = Object.fromEntries((Object.keys(KIND_LABELS) as ValueKind[]).map((k) => [k, zero()])) as Record<ValueKind, Range>;
  for (const g of groups) {
    // Exposures overlap (stuck deals are often pushed deals too), so only the largest stands for them.
    if (g.kind === "exposure") {
      if (g.counted.expected > byKind.exposure.expected) byKind.exposure = { low: g.counted.low, expected: g.counted.expected, high: g.counted.high };
      continue;
    }
    const r = byKind[g.kind];
    r.low = round2(r.low + g.counted.low);
    r.expected = round2(r.expected + g.counted.expected);
    r.high = round2(r.high + g.counted.high);
  }
  const total = HEADLINE_KINDS.reduce((sum, k) => ({ low: round2(sum.low + byKind[k].low), expected: round2(sum.expected + byKind[k].expected), high: round2(sum.high + byKind[k].high) }), zero());
  return { currency, hourly, groups, total, byKind, left };
}

/** What a set of findings is worth: within each group, the largest of their figures, added across groups. Exposure is left out, as in the total. */
export function worthOf(model: ValueModel, findingIds: Set<string>): number {
  let sum = 0;
  for (const g of model.groups) {
    if (g.kind === "exposure") continue;
    const own = g.lines.filter((l) => findingIds.has(l.findingId));
    if (own.length) sum += Math.max(...own.map((l) => l.expected));
  }
  return round2(sum);
}
