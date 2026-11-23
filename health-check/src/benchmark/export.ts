import { appendFileSync, chmodSync, existsSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { readCheckpoint } from "../engine/checkpoint.ts";
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { z } from "zod";
import { loadPipeline } from "../domains/historic-pipeline/cohort.ts";
import { closedInWindow } from "../domains/historic-pipeline/metrics.ts";
import { assertOutsideGit } from "../engine/run.ts";
import { Intake } from "../narrative/intake.ts";
import { applyOverrides, refusalSummary } from "../narrative/overrides.ts";
import type { RuleContext } from "../engine/rule.ts";
import type { Extractor } from "../extract/types.ts";
import { usersExtractor } from "../extract/users.ts";
import { PMD_RULES } from "../extract/lint/pmd.ts";
import { LINT_TOTALS_FILE, LintTotals } from "../extract/lint/totals.ts";
import { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import { BenchmarkRow, Calibration, Finding, HeadcountBand, Industry, Metric, Run, SALES_CLOUD_DOMAINS, Score, type NumberOverride } from "../schemas/index.ts";

// Rule 11. One org's figures go into the benchmark set only with the customer's
// consent, and nothing written there leads back to them: no run id, org id, name,
// date or path. The consent itself is recorded in the run folder, which is where
// the customer's identity already lives.

export const BENCHMARK_FILE = "rows.jsonl";
export const CONSENT_FILE = "benchmark-consent.json";
export const defaultBenchmarkDir = (): string => join(homedir(), ".barometer", "benchmarks");

export { Industry };

type Unit = BenchmarkRow["unit"];
type Source =
  | { from: "score"; domain: string }
  | { from: "metric"; id: string }
  // A figure a rule records on a pass and an issue alike. One recorded only on
  // an issue would leave out every org that passed, and skew the set.
  | { from: "fact"; ruleId: string; fact: string }
  // PMD's whole-org counts, from lint-totals.json (DECISIONS.md 673): a rule's, or what it read.
  | { from: "lint"; rule: string; count: "found" | "suppressed" | "components" }
  | { from: "lint"; read: "classes" | "triggers" | "lines" };

export type BenchmarkMetric = { key: string; unit: Unit; source: Source };

/** A PMD rule's name as a key: ApexCRUDViolation as apex_crud_violation. */
export const lintKey = (rule: string): string =>
  rule
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .replace(/([A-Z]+)([A-Z][a-z])/g, "$1_$2")
    .toLowerCase();

/** What a benchmark says of a PMD count when PMD didn't run, or the run had no step: never zero. */
export const LINT_NOT_MEASURED = "PMD didn't run in this health check, so this wasn't measured.";
/** A count of code PMD couldn't read: not measured either. */
export const LINT_KIND_UNREAD = "PMD couldn't read the code this counts in this health check, so this wasn't measured.";

// A fixed list, and only whole-org figures. Segments, series, stats and the
// worst-segment facts are never read, because a per-rep or per-account value is
// a step towards a person. Amounts aren't here, and counts only PMD's (DECISIONS.md
// 673), kept because the owner wants every rule's result to benchmark on: they
// mostly measure the size of the org, so what PMD read is rounded (lines to the
// nearest 1,000, classes and triggers to the nearest 10), and anything published
// from them must apply the minimum group, as every figure here does.
export const BENCHMARK_METRICS: BenchmarkMetric[] = [
  ...SALES_CLOUD_DOMAINS.map((domain): BenchmarkMetric => ({ key: `score.${domain}`, unit: "score", source: { from: "score", domain } })),
  { key: "historic.win_rate", unit: "percent", source: { from: "metric", id: "historic.win_rate" } },
  { key: "historic.win_rate_by_value", unit: "percent", source: { from: "metric", id: "historic.win_rate_by_value" } },
  { key: "historic.win_rate_including_no_decision", unit: "percent", source: { from: "metric", id: "historic.win_rate_including_no_decision" } },
  { key: "historic.no_decision_rate", unit: "percent", source: { from: "metric", id: "historic.no_decision_rate" } },
  { key: "historic.stage_conversion", unit: "percent", source: { from: "metric", id: "historic.stage_conversion" } },
  { key: "historic.sales_cycle_won", unit: "days", source: { from: "metric", id: "historic.sales_cycle_won" } },
  { key: "historic.sales_cycle_lost", unit: "days", source: { from: "metric", id: "historic.sales_cycle_lost" } },
  { key: "historic.slippage_rate", unit: "percent", source: { from: "metric", id: "historic.slippage_rate" } },
  { key: "historic.slippage_quarter_cross_rate", unit: "percent", source: { from: "metric", id: "historic.slippage_quarter_cross_rate" } },
  { key: "historic.slippage_days", unit: "days", source: { from: "metric", id: "historic.slippage_days" } },
  { key: "current.median_days_in_stage", unit: "days", source: { from: "metric", id: "current.median_days_in_stage" } },
  { key: "activity.event_linked_share", unit: "percent", source: { from: "metric", id: "activity.event_linked_share" } },
  { key: "lead.conversion_rate", unit: "percent", source: { from: "metric", id: "lead.conversion_rate" } },
  { key: "dq.open_deals_past_close_rate", unit: "percent", source: { from: "fact", ruleId: "dq.open-opps-past-close", fact: "rate" } },
  { key: "dq.open_deals_inactive_owner_rate", unit: "percent", source: { from: "fact", ruleId: "dq.opps-inactive-owner", fact: "rate" } },
  { key: "dq.open_deals_missing_amount_rate", unit: "percent", source: { from: "fact", ruleId: "dq.open-opps-missing-amount", fact: "rate" } },
  { key: "dq.open_deals_no_activity_rate", unit: "percent", source: { from: "fact", ruleId: "dq.open-opps-no-activity", fact: "rate" } },
  { key: "dq.lost_deals_missing_reason_rate", unit: "percent", source: { from: "fact", ruleId: "dq.loss-reason-missing", fact: "rate" } },
  { key: "dq.open_leads_stale_rate", unit: "percent", source: { from: "fact", ruleId: "dq.stale-leads", fact: "rate" } },
  { key: "dq.contacts_missing_account_rate", unit: "percent", source: { from: "fact", ruleId: "dq.contact-completeness", fact: "accountRate" } },
  { key: "dq.contacts_missing_email_rate", unit: "percent", source: { from: "fact", ruleId: "dq.contact-completeness", fact: "emailRate" } },
  { key: "bp.users_without_role_rate", unit: "percent", source: { from: "fact", ruleId: "bp.users-without-role", fact: "rate" } },
  { key: "utl.unused_reports_rate", unit: "percent", source: { from: "fact", ruleId: "utl.unused-reports", fact: "share" } },
  { key: "sec.health_check_score", unit: "score", source: { from: "fact", ruleId: "sec.health-check-score", fact: "score" } },
  // Every PMD rule's counts, whether or not it fired, and what PMD read, as the
  // owner asked: kept to benchmark on, though most are never shown (DECISIONS.md 673).
  ...Object.keys(PMD_RULES).flatMap((rule) =>
    (["found", "suppressed", "components"] as const).map((count): BenchmarkMetric => ({ key: `code.pmd.${lintKey(rule)}.${count}`, unit: "count", source: { from: "lint", rule, count } })),
  ),
  ...(["classes", "triggers", "lines"] as const).map((read): BenchmarkMetric => ({ key: `code.pmd.${read}`, unit: "count", source: { from: "lint", read } })),
];

export type Figure = { metricKey: string; unit: Unit; value: number };
export type Skipped = { metricKey: string; reason: string };

/** `lintTotals` is the run's lint-totals.json, or null when it has none. */
type Read = { metrics: Metric[]; scores: Score[]; findings: Finding[]; lintTotals?: LintTotals | null; orgSettings?: boolean };

/**
 * On a run whose calibration was read from the org's settings (DECISIONS.md
 * 693), a figure resting on the open stages' unconfirmed order, on several
 * record types read as one, or on a stage's meaning taken from Salesforce's
 * flags would go into the set with no note, which a benchmark row can't carry:
 * it is left out. Every other figure goes in, as the owner's policy is to store broadly.
 */
const UNCONFIRMED: readonly RegExp[] = [/We took the order of your open stages from Salesforce's own stage settings/, /which may be separate sales processes/, /so we used Salesforce's own setting for/];
const ORDER_METRICS: ReadonlySet<string> = new Set(["historic.stage_conversion"]);
export const UNCONFIRMED_SKIP = "This figure rests on a reading of the org's stages that no person has confirmed, so it's left out of the benchmark set.";
const unconfirmed = (texts: readonly string[]): boolean => texts.some((t) => UNCONFIRMED.some((p) => p.test(t)));

/** The run's PMD totals, or null when it had no step. A file that doesn't parse is refused rather than read as not measured. */
export function readLintTotals(dir: string): LintTotals | null {
  const path = join(dir, LINT_TOTALS_FILE);
  return existsSync(path) ? LintTotals.parse(JSON.parse(readFileSync(path, "utf8"))) : null;
}

/** One allowlisted figure as the run holds it, or why there isn't one. */
function figureOf(input: Read, unit: Unit, source: Source): { value: number } | { reason: string } {
  if (source.from === "score") {
    const score = input.scores.find((s) => s.domain === source.domain);
    if (!score) return { reason: "This domain wasn't in the run." };
    if (score.status !== "measured") return { reason: score.reason };
    return { value: score.score };
  }
  if (source.from === "metric") {
    const metric = input.metrics.find((m) => m.id === source.id);
    if (!metric) return { reason: "The run has no such metric." };
    if (metric.status !== "measured") return { reason: metric.reason };
    if (input.orgSettings && (ORDER_METRICS.has(metric.id) || unconfirmed(metric.assumptions))) return { reason: UNCONFIRMED_SKIP };
    // A changed unit would put a figure in the wrong comparison, so it's left out.
    if (metric.unit !== unit) return { reason: `The run measured this in ${metric.unit}, not ${unit}.` };
    return { value: metric.value };
  }
  if (source.from === "lint") {
    const pmd = input.lintTotals?.pmd;
    if (!pmd || pmd.status !== "ran") return { reason: LINT_NOT_MEASURED };
    if ("read" in source) {
      // A kind PMD couldn't read isn't a count of none (fix round 1).
      if (source.read !== "lines" && pmd.unread.includes(source.read === "classes" ? "class" : "trigger")) return { reason: LINT_KIND_UNREAD };
      const step = source.read === "lines" ? 1_000 : 10;
      return { value: Math.round(pmd[source.read] / step) * step };
    }
    const total = pmd.rules[source.rule];
    if (total === null) return { reason: LINT_KIND_UNREAD };
    return total ? { value: total[source.count] } : { reason: "This run's PMD didn't check this rule." };
  }
  const results = input.findings.filter((f) => f.ruleId === source.ruleId);
  const measured = results.filter((f) => f.status !== "not_measurable");
  if (measured.length === 0) return { reason: results[0]?.notMeasurableReason ?? "The run has no result for this check." };
  if (input.orgSettings && unconfirmed(measured.map((f) => f.summary))) return { reason: UNCONFIRMED_SKIP };
  // These rules give one org-wide result. More than one would mean the
  // figure is per subject, which doesn't belong here.
  if (measured.length > 1) return { reason: `The check gave ${measured.length} results, so there's no single org-wide figure.` };
  const fact = measured[0]!.facts[source.fact];
  if (fact === undefined) return { reason: "The check didn't record this figure, usually because there was nothing to check." };
  return { value: fact };
}

/**
 * What a corrected figure in each unit can be. A percentage and a score are out
 * of 100. Days can't be fewer than none, and ten years is far past any sales
 * cycle or time in stage we compare (DECISIONS.md 528): a ceiling on what the
 * set can hold for good, not a guard against a slip of the keyboard.
 */
const RANGE: Record<Unit, { min: number; max: number; what: string }> = {
  percent: { min: 0, max: 100, what: "0 to 100 for a percentage" },
  score: { min: 0, max: 100, what: "0 to 100 for a score" },
  days: { min: 0, max: 3_650, what: "0 to 3,650 for days" },
  // No consultant corrects these (DECISIONS.md 673); the bound is only so every unit has one.
  count: { min: 0, max: 100_000_000, what: "0 to 100,000,000 for a count" },
};

/**
 * The allowlisted figures this run measured, rounded to whole units. Anything
 * not measurable is skipped, never filled in. Given the figures as read beside
 * them, a figure a consultant corrected to one its unit can't hold is skipped
 * too: an override takes any finite number, and once in the set a row can't be
 * taken out (DECISIONS.md 521). A figure as read is taken as it always was.
 */
export function benchmarkFigures(input: Read & { asRead?: Omit<Read, "scores"> }): { figures: Figure[]; skipped: Skipped[] } {
  const figures: Figure[] = [];
  const skipped: Skipped[] = [];
  for (const { key, unit, source } of BENCHMARK_METRICS) {
    const skip = (reason: string): void => void skipped.push({ metricKey: key, reason });
    const found = figureOf(input, unit, source);
    if ("reason" in found) {
      skip(found.reason);
      continue;
    }
    const value = found.value;
    if (!Number.isFinite(value)) {
      skip("The figure isn't a number.");
      continue;
    }
    if (input.asRead) {
      const read = figureOf({ ...input.asRead, scores: input.scores, lintTotals: input.lintTotals }, unit, source);
      const range = RANGE[unit];
      const corrected = !("value" in read) || read.value !== value;
      if (corrected && (value < range.min || value > range.max)) {
        skip(`The figure as corrected is outside ${range.what}, so it's left out.`);
        continue;
      }
    }
    // Whole units, as the row format requires. Math.round(-0.4) is -0, which is still an integer.
    figures.push({ metricKey: key, unit, value: Math.round(value) || 0 });
  }
  return { figures, skipped };
}

export function userBandOf(activeUsers: number): BenchmarkRow["userBand"] | null {
  if (activeUsers < 1) return null;
  return activeUsers <= 50 ? "1-50" : activeUsers <= 200 ? "51-200" : "201+";
}

export function dealSizeBandOf(amount: number): BenchmarkRow["dealSizeBand"] {
  return amount < 5_000 ? "under-5k" : amount < 25_000 ? "5k-25k" : amount < 100_000 ? "25k-100k" : "100k+";
}

/** A calendar quarter, not the org's fiscal one, so every org's rows line up. */
export function quarterOf(day: string): string {
  const [year, month] = day.split("-").map(Number) as [number, number];
  return `${year}-Q${Math.floor((month - 1) / 3) + 1}`;
}

// The minimum group counts orgs, but the store can't tell orgs apart. So an org
// goes in at most once in any window of this many quarters, and the summary only
// counts rows inside the latest window. Together, no org counts twice.
export const BENCHMARK_WINDOW_QUARTERS = 4;

/** Quarters since year 0, so two periods can be compared. */
export function quarterIndex(period: string): number {
  const [year, quarter] = period.split("-Q").map(Number) as [number, number];
  return year * 4 + quarter - 1;
}

/** Another run of the same org, in the same runs folder, already exported within the window. */
function earlierExport(dir: string, orgId: string, period: string): { runId: string; period: string } | null {
  const parent = dirname(dir);
  for (const name of readdirSync(parent)) {
    const other = join(parent, name);
    if (other === dir || !existsSync(join(other, CONSENT_FILE)) || !existsSync(join(other, "run.json"))) continue;
    const run = Run.safeParse(JSON.parse(readFileSync(join(other, "run.json"), "utf8")));
    if (!run.success || run.data.orgId.slice(0, 15) !== orgId.slice(0, 15)) continue;
    const otherPeriod = quarterOf(run.data.analysisWindow.to);
    if (Math.abs(quarterIndex(otherPeriod) - quarterIndex(period)) < BENCHMARK_WINDOW_QUARTERS) return { runId: run.data.id, period: otherPeriod };
  }
  return null;
}

const median = (values: number[]): number => {
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 1 ? sorted[mid]! : (sorted[mid - 1]! + sorted[mid]!) / 2;
};

export type OrgProfile = { activeUsers: number; wonDeals: number; medianWonAmount: number };

/**
 * The two figures the bands come from. Neither is an output of the analysis, so
 * they're read from the raw extracts, through the same loaders the rules use.
 * After a purge they're gone, and the export is refused rather than guessed.
 */
export function orgProfile(dir: string, run: Run, calibration: Calibration): OrgProfile {
  const folder = join(dir, "checkpoints");
  if (!existsSync(folder)) {
    throw new Error("This run's raw extracts have been purged, so its user and deal size bands can't be worked out. Export to the benchmark set before the purge.");
  }
  const data = <T>(extractor: Extractor<T>): T => {
    const path = join(folder, `${extractor.id}.json`);
    if (!existsSync(path)) throw new Error(`The run has no "${extractor.id}" extract, so its bands can't be worked out.`);
    return readCheckpoint<T>(path);
  };
  const now = new Date(run.asOf);
  const ctx: Omit<RuleContext, "metrics"> = {
    calibration,
    now,
    window: run.analysisWindow,
    pseudonyms: new Pseudonymiser(),
    data,
    ev: (source) => ({ ...source, count: null, dateRange: null, exclusions: [], capturedAt: now.toISOString() }),
    manual: () => {
      throw new Error("A benchmark export raises no manual steps.");
    },
    userLabel: () => "a user",
  };

  const users = data(usersExtractor);
  if (users.status !== "ok") throw new Error(`Users couldn't be read in this run (${users.reason}), so the user band can't be worked out.`);
  // People, not accounts: the same exclusions the user rules make.
  const nonPerson = new Set(calibration.users.filter((u) => ["shared", "system", "integration", "test"].includes(u.kind)).map((u) => u.userId.slice(0, 15)));
  const activeUsers = users.data.filter((u) => u.IsActive && !nonPerson.has(u.Id.slice(0, 15))).length;

  const pipeline = loadPipeline(ctx);
  if (pipeline.status !== "ok") throw new Error(`${pipeline.reason} So the deal size band can't be worked out.`);
  // A benchmark row carries no note, so a figure resting on a stage whose
  // meaning came from Salesforce's flags would go in unmarked (DECISIONS.md 600).
  // An automatic run's calibration names every won stage and leaves lost ones to the flags (692), so the won amounts below rest on none of them (693).
  if (pipeline.inferred.length > 0 && calibration.source !== "org_settings") {
    const stages = pipeline.inferred.map((s) => s.stage).join(", ");
    throw new Error(`The calibration doesn't name these stages, so this run took their meaning from Salesforce's own settings: ${stages}. Add them to the calibration and run the health check again before exporting to the benchmark set.`);
  }
  if (pipeline.multiCurrency) throw new Error("The org is multi-currency, so won amounts can't be compared and the deal size band can't be worked out.");
  // The median won amount, not the mean, for the reason in decision 16: one
  // unusually large deal shouldn't move the org into another band. Deals with no
  // amount, or zero, are left out rather than counted as small.
  const amounts = pipeline.deals.filter((d) => d.category === "won" && closedInWindow(d, run.analysisWindow) && d.Amount !== null && d.Amount > 0).map((d) => d.Amount!);
  if (amounts.length < calibration.minSample) {
    throw new Error(`${amounts.length} won deals with an amount is below the calibrated minimum of ${calibration.minSample}, so the deal size band would be a guess.`);
  }
  return { activeUsers, wonDeals: amounts.length, medianWonAmount: median(amounts) };
}

/** Writes rows as JSON lines, private to the owner. Every row is checked against the strict format before anything is written. */
export function appendRows(path: string, rows: unknown[]): BenchmarkRow[] {
  const parsed = rows.map((row) => BenchmarkRow.parse(row));
  const dir = dirname(path);
  mkdirSync(dir, { recursive: true, mode: 0o700 });
  // Modes are only applied on creation, so an existing folder or file is tightened too.
  chmodSync(dir, 0o700);
  if (parsed.length > 0) appendFileSync(path, `${parsed.map((r) => JSON.stringify(r)).join("\n")}\n`, { mode: 0o600 });
  if (existsSync(path)) chmodSync(path, 0o600);
  return parsed;
}

export type BenchmarkOptions = {
  runDir: string;
  /**
   * The run's override rows: the figures are read as corrected (DECISIONS.md
   * 521). The worker passes those it made the documents with; `hc benchmark`
   * the run folder's overrides.json. Default: none, so the figures as read.
   */
  overrides?: NumberOverride[];
  industry: string | undefined;
  headcount: string | undefined;
  consent: string | undefined;
  /** The benchmark store's folder. Default: ~/.barometer/benchmarks */
  storeDir?: string;
  now?: Date;
};

export type BenchmarkResult = { rows: BenchmarkRow[]; skipped: Skipped[]; storePath: string; profile: OrgProfile };

const readJson = (path: string): unknown => JSON.parse(readFileSync(path, "utf8"));

export const ConsentRecord = z.strictObject({
  consent: z.string().min(1),
  recordedAt: z.iso.datetime(),
  rowsWritten: z.number().int().nonnegative(),
  metricKeys: z.array(z.string()),
});
export type ConsentRecord = z.infer<typeof ConsentRecord>;

type Prepared = { dir: string; rows: BenchmarkRow[]; skipped: Skipped[]; profile: OrgProfile; consentPath: string; record: ConsentRecord };

/** Everything up to writing: the checks, the figures and the bands. Throws with the reason when the run can't contribute. */
function prepareBenchmark(options: Omit<BenchmarkOptions, "storeDir">): Prepared {
  const consent = options.consent?.trim() ?? "";
  if (!consent) throw new Error('Refusing to export without --consent "<who agreed and how>". Figures only go into the benchmark set when the customer has opted in.');
  const dir = resolve(options.runDir);
  if (!existsSync(join(dir, "run.json"))) throw new Error(`${dir} isn't a run folder: it has no run.json.`);
  // The intake records both, so the flags are only needed to override it.
  const intake = existsSync(join(dir, "intake.json")) ? Intake.safeParse(readJson(join(dir, "intake.json"))) : null;
  const industry = Industry.safeParse(options.industry ?? (intake?.success ? intake.data.industry : undefined));
  if (!industry.success) throw new Error(`Give --industry, or record it in the intake. One of: ${Industry.options.join(", ")}.`);
  const headcount = HeadcountBand.safeParse(options.headcount ?? (intake?.success ? intake.data.headcountBand : undefined));
  if (!headcount.success) throw new Error(`Give --headcount, or record it in the intake. One of: ${HeadcountBand.options.join(", ")}.`);
  const consentPath = join(dir, CONSENT_FILE);
  if (existsSync(consentPath)) {
    const previous = ConsentRecord.safeParse(readJson(consentPath));
    throw new AlreadyContributed(`This run has already been exported to the benchmark set${previous.success ? ` (${previous.data.recordedAt})` : ""}. A second export would count the org twice.`);
  }
  // The consent record is written into the run folder, so it never belongs in a repository.
  assertOutsideGit(dir);

  const run = Run.parse(readJson(join(dir, "run.json")));
  const earlier = earlierExport(dir, run.orgId, quarterOf(run.analysisWindow.to));
  if (earlier) {
    throw new AlreadyContributed(`This org is already in the benchmark set from run ${earlier.runId} (${earlier.period}). An org goes in once in any ${BENCHMARK_WINDOW_QUARTERS} quarters, so it can't count twice towards a group.`);
  }
  const calibration = Calibration.parse(readJson(join(dir, "calibration.json")));
  const metrics = z.array(Metric).parse(readJson(join(dir, "metrics.json")));
  const findings = z.array(Finding).parse(readJson(join(dir, "findings.json")));
  // As the documents print them. A row we can't carry leaves the documents as
  // read, so neither reading is certain: nothing is added. The count only,
  // since a title or a figure is the customer's.
  const effective = applyOverrides(findings, metrics, options.overrides ?? []);
  if (effective.refused.length > 0) throw new Error(refusalSummary(effective.refused));
  const { figures, skipped } = benchmarkFigures({
    metrics: effective.metrics,
    scores: z.array(Score).parse(readJson(join(dir, "scores.json"))),
    findings: effective.findings,
    asRead: { metrics, findings },
    lintTotals: readLintTotals(dir),
    orgSettings: calibration.source === "org_settings",
  });
  if (figures.length === 0) throw new Error("Nothing in this run was measurable, so there's nothing to export.");

  const profile = orgProfile(dir, run, calibration);
  const userBand = userBandOf(profile.activeUsers);
  if (!userBand) throw new Error("The run found no active users, so the user band can't be worked out.");
  const shared = {
    ruleSetVersion: run.ruleSetVersion,
    industry: industry.data,
    headcountBand: headcount.data,
    userBand,
    dealSizeBand: dealSizeBandOf(profile.medianWonAmount),
    period: quarterOf(run.analysisWindow.to),
  };
  // Built field by field, so nothing from the run can ride along.
  const rows = figures.map((f) => BenchmarkRow.parse({ metricKey: f.metricKey, value: f.value, unit: f.unit, ...shared }));
  const record = ConsentRecord.parse({ consent, recordedAt: (options.now ?? new Date()).toISOString(), rowsWritten: rows.length, metricKeys: rows.map((r) => r.metricKey) });
  return { dir, rows, skipped, profile, consentPath, record };
}

/** The run, or its org within the window, is already in the set. Not a fault: there's nothing more to add. */
export class AlreadyContributed extends Error {
  constructor(message: string) {
    super(message);
    this.name = "AlreadyContributed";
  }
}

/**
 * Consent first, created exclusively, so two exports racing can't both write.
 * If the rows then fail to write, the record is taken back so a retry works.
 */
async function writeWithConsent(prepared: Prepared, write: (rows: BenchmarkRow[]) => void | Promise<void>): Promise<void> {
  writeFileSync(prepared.consentPath, `${JSON.stringify(prepared.record, null, 2)}\n`, { mode: 0o600, flag: "wx" });
  try {
    await write(prepared.rows);
  } catch (err) {
    rmSync(prepared.consentPath, { force: true });
    throw err;
  }
}

/** `hc benchmark`: into the local store, a JSON lines file outside any repository. */
export function exportBenchmark(options: BenchmarkOptions): BenchmarkResult {
  const prepared = prepareBenchmark(options);
  const storeDir = resolve(options.storeDir ?? defaultBenchmarkDir());
  // The store holds customer-derived figures, so it never belongs in a repository.
  assertOutsideGit(storeDir);
  const storePath = join(storeDir, BENCHMARK_FILE);
  // Synchronous here: appendRows is, so the promise has settled by the time this returns.
  writeFileSync(prepared.consentPath, `${JSON.stringify(prepared.record, null, 2)}\n`, { mode: 0o600, flag: "wx" });
  try {
    appendRows(storePath, prepared.rows);
  } catch (err) {
    rmSync(prepared.consentPath, { force: true });
    throw err;
  }
  return { rows: prepared.rows, skipped: prepared.skipped, storePath, profile: prepared.profile };
}

/** The worker: into the console's benchmark table, through whatever writes it. */
export async function contributeBenchmark(
  options: Omit<BenchmarkOptions, "storeDir">,
  write: (rows: BenchmarkRow[]) => Promise<void>,
): Promise<{ rows: BenchmarkRow[]; skipped: Skipped[] }> {
  const prepared = prepareBenchmark(options);
  await writeWithConsent(prepared, write);
  return { rows: prepared.rows, skipped: prepared.skipped };
}
