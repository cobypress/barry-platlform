import { schemaExtractor } from "../../extract/configuration.ts";
import { opportunitiesExtractor, stagesExtractor, wonAccountsExtractor } from "../../extract/opportunities.ts";
import { daysBefore, monthsBefore } from "../../extract/types.ts";
import { usersExtractor } from "../../extract/users.ts";
import { notSized, pct, plural, type NotMeasurableResult, type Rule, type RuleContext, type RuleResult } from "../../engine/rule.ts";
import { enteredCurrentStage } from "../current-pipeline/stages.ts";
import { loadPipeline, unsplitLostReason, type Deal } from "../historic-pipeline/cohort.ts";
import { median } from "../historic-pipeline/metrics.ts";
import { localDay, zoneOf } from "../time-zone.ts";
import { potentialRevenue, SITUATIONS } from "../historic-pipeline/potential.ts";
import { atOwnWinRate } from "../value.ts";
import type { Evidence, Value } from "../../schemas/index.ts";
import type { DomainModule } from "../types.ts";
import { joinSaid, org, said } from "../../engine/org-words.ts";
import { moneySaid } from "../../engine/said-forms.ts";

// Every title, summary, recommendation and reason is written with `said`, every
// stage with `org`, so the digest can say which words are the org's
// (DECISIONS.md 655, 657). A reason passed on from an extract stays plain.

const DOMAIN = "revenue_opportunities";
const STALLED_DAYS = 60;
const WIN_BACK_MIN_MONTHS = 6;
const WIN_BACK_MAX_MONTHS = 18;
const MIN_DEALS = 5;

type Ready = Extract<ReturnType<typeof loadPipeline>, { status: "ok" }>;

function pipelineOr(ctx: RuleContext): Ready | NotMeasurableResult {
  const data = loadPipeline(ctx);
  if (data.status !== "ok") return { outcome: "not_measurable", reason: data.reason, evidence: data.evidence };
  if (data.unmapped.finalStages.length > 0) return { outcome: "not_measurable", reason: said`The calibration doesn't say what these stages mean: ${joinSaid(data.unmapped.finalStages.map((s) => said`${org(s)}`), ", ")}.`, evidence: data.evidence };
  return data;
}

const total = (deals: Deal[]): number => deals.reduce((sum, d) => sum + (d.Amount ?? 0), 0);
const distinct = (values: string[]): number => new Set(values).size;
const dealSegments = (ctx: RuleContext, deals: Deal[]) => [
  { dimension: "team", segments: distinct(deals.map((d) => d.team)) },
  { dimension: "rep", segments: distinct(deals.map((d) => ctx.userLabel(d.OwnerId))) },
  { dimension: "record type", segments: distinct(deals.map((d) => d.RecordType ?? "No record type")) },
];

const stalledDeals: Rule = {
  id: "rev.stalled-deals",
  version: 3,
  domain: DOMAIN,
  title: "Stalled deals worth reviving",
  evaluate(ctx): RuleResult | RuleResult[] {
    const data = pipelineOr(ctx);
    if ("outcome" in data) return data;
    const cutoff = daysBefore(ctx.now, STALLED_DAYS);
    const day = localDay(cutoff, zoneOf(ctx));
    const open = data.deals.filter((d) => d.category === "open");
    const stalled = open.filter((d) => (enteredCurrentStage(d) ?? new Date(d.CreatedDate)) < cutoff && (d.LastActivityDate === null || d.LastActivityDate < day));
    const evidence: Evidence[] = [{ ...data.evidence[0]!, count: stalled.length, dateRange: { from: ctx.window.to, to: ctx.window.to }, exclusions: data.exclusions, note: `Open deals with no stage change and no activity in ${STALLED_DAYS} days` }];
    if (open.length === 0) return { outcome: "pass", summary: said`There are no open deals to check.`, evidence, segmentExemptReason: "There are no deals to segment." };
    const segmentsChecked = dealSegments(ctx, open);
    if (stalled.length < MIN_DEALS) return { outcome: "pass", summary: said`${plural(stalled.length, "open deal")} of ${open.length} ${stalled.length === 1 ? "has" : "have"} stalled for ${STALLED_DAYS} days, under the ${MIN_DEALS} we'd call a pattern.`, evidence, segmentsChecked, facts: { stalled: stalled.length, open: open.length } };
    const amount = total(stalled);
    const situation = SITUATIONS.inStageFor(STALLED_DAYS, data);
    const value = potentialRevenue(ctx, data, stalled, () => situation);
    return {
      outcome: "issue",
      // Titled by its test, as two other checks find quiet deals by theirs (night 3 reading pass, item 16).
      title: said`Open deals with no stage change or activity in ${STALLED_DAYS} days`,
      summary: said`${plural(stalled.length, "open deal")} of ${open.length} (${pct(stalled.length, open.length)}%) haven't changed stage or had activity in ${STALLED_DAYS} days.`,
      recommendation: said`Give each owner a date to re-engage or close these. The ones that come back are the cheapest revenue you have.`,
      recommendationType: "change",
      severity: "medium",
      confidence: "medium",
      evidence,
      value,
      effort: { size: "S" },
      owner: "Head of sales",
      segmentsChecked,
      facts: { stalled: stalled.length, open: open.length, ...(data.multiCurrency ? {} : { amount: Math.round(amount) }), ...(value.kind === "revenue" ? { sized: value.amount } : {}) },
    };
  },
};

const winBacks: Rule = {
  id: "rev.win-backs",
  version: 1,
  domain: DOMAIN,
  title: "Lost deals worth another look",
  evaluate(ctx): RuleResult | RuleResult[] {
    const data = pipelineOr(ctx);
    if ("outcome" in data) return data;
    const newest = monthsBefore(ctx.now, WIN_BACK_MIN_MONTHS);
    const oldest = monthsBefore(ctx.now, WIN_BACK_MAX_MONTHS);
    // An account already buying or being sold to isn't a win-back.
    const active = new Set(data.deals.filter((d) => d.AccountId && (d.category === "open" || (d.category === "won" && d.closedAt >= oldest))).map((d) => d.AccountId!));
    // It reports the no decision deals apart, and leads with them (DECISIONS.md 600).
    const unsplit = unsplitLostReason(data);
    if (unsplit) return { outcome: "not_measurable", reason: unsplit, evidence: data.evidence };
    const closed = data.deals.filter((d) => d.category === "lost" || d.category === "no_decision");
    const noAccount = closed.filter((d) => !d.AccountId).length;
    const candidates = closed.filter((d) => d.AccountId && d.closedAt >= oldest && d.closedAt < newest && !active.has(d.AccountId));
    const exclusions = [...data.exclusions, "Accounts with an open deal, or a win since the deal was lost", ...(noAccount ? [said`${plural(noAccount, "deal")} with no account`] : [])];
    const evidence: Evidence[] = [{ ...data.evidence[0]!, count: candidates.length, dateRange: { from: oldest.toISOString().slice(0, 10), to: newest.toISOString().slice(0, 10) }, exclusions, note: `Lost or no decision between ${WIN_BACK_MIN_MONTHS} and ${WIN_BACK_MAX_MONTHS} months ago` }];
    const segmentsChecked = [...dealSegments(ctx, closed), { dimension: "outcome", segments: distinct(closed.map((d) => d.category ?? "Unknown")) }];
    if (closed.length === 0) return { outcome: "pass", summary: said`No deals have been lost in the window.`, evidence, segmentExemptReason: "There are no lost deals to segment." };
    if (candidates.length < MIN_DEALS) return { outcome: "pass", summary: said`${plural(candidates.length, "lost deal")} from ${WIN_BACK_MIN_MONTHS} to ${WIN_BACK_MAX_MONTHS} months ago could be revisited, under the ${MIN_DEALS} we'd call a pattern.`, evidence, segmentsChecked, facts: { candidates: candidates.length } };
    const amount = total(candidates);
    const noDecision = candidates.filter((d) => d.category === "no_decision").length;
    const value = atOwnWinRate(ctx, amount, {
      multiCurrency: data.multiCurrency,
      period: "one-off, if these deals are reopened",
      basis: said`${plural(candidates.length, "deal")} with a total Amount of ${moneySaid(amount, ctx.calibration.currency)}, at the amount quoted then`,
      caveat: said`Assumes a win-back wins as often as a new deal. We don't have your win-back history, so treat this as a ceiling, and the reason each deal was lost may still stand`,
    });
    return {
      outcome: "issue",
      title: said`You have past deals worth going back to`,
      summary: said`${plural(candidates.length, "deal")} lost or left undecided ${WIN_BACK_MIN_MONTHS} to ${WIN_BACK_MAX_MONTHS} months ago sit on accounts with nothing open and no win since. ${noDecision} of them never reached a decision.`,
      recommendation: said`Start with the no decision deals, where nothing was lost to a competitor, and check why each one stopped.`,
      recommendationType: "investigate",
      severity: "medium",
      confidence: "medium",
      evidence,
      value,
      effort: { size: "M" },
      owner: "Head of sales",
      segmentsChecked,
      facts: { candidates: candidates.length, noDecision, ...(data.multiCurrency ? {} : { amount: Math.round(amount) }), ...(value.kind === "revenue" ? { sized: value.amount } : {}) },
    };
  },
};

const dormantAccounts: Rule = {
  id: "rev.dormant-accounts",
  version: 1,
  domain: DOMAIN,
  title: "Past customers nobody is talking to",
  evaluate(ctx): RuleResult | RuleResult[] {
    const data = pipelineOr(ctx);
    if ("outcome" in data) return data;
    const accounts = ctx.data(wonAccountsExtractor);
    if (accounts.status !== "ok") return { outcome: "not_measurable", reason: `Accounts couldn't be read: ${accounts.reason}`, evidence: [ctx.ev(accounts.source, { count: null })] };
    if (accounts.truncated) return { outcome: "not_measurable", reason: said`There are too many customer accounts to read in full.`, evidence: [ctx.ev(accounts.source, { count: accounts.data.length })] };
    const { dormantDays } = ctx.calibration;
    const cutoff = localDay(daysBefore(ctx.now, dormantDays), zoneOf(ctx));
    const openAccounts = new Set(data.deals.filter((d) => d.category === "open" && d.AccountId).map((d) => d.AccountId!));
    const wonDeals = data.deals.filter((d) => d.category === "won" && d.AccountId);
    const dormant = accounts.data.filter((a) => !openAccounts.has(a.Id) && (a.LastActivityDate === null || a.LastActivityDate < cutoff));
    const users = ctx.data(usersExtractor);
    const roles = new Map(users.status === "ok" ? users.data.map((u) => [u.Id, u.UserRole?.Name ?? "No role"]) : []);
    const inactive = new Set(users.status === "ok" ? users.data.filter((u) => !u.IsActive).map((u) => u.Id) : []);
    const evidence = [ctx.ev(accounts.source, { count: dormant.length, exclusions: ["Accounts with an open deal", `Accounts with activity in the last ${dormantDays} days`], note: `Calibrated dormant period of ${dormantDays} days` })];
    const segmentsChecked = [{ dimension: "team", segments: distinct(accounts.data.map((a) => roles.get(a.OwnerId) ?? "No role")) }, { dimension: "owner", segments: distinct(accounts.data.map((a) => a.OwnerId)) }];
    if (accounts.data.length === 0) return { outcome: "not_measurable", reason: said`No account has a won deal, so there are no past customers to check.`, evidence };
    if (dormant.length < MIN_DEALS) return { outcome: "pass", summary: said`${plural(dormant.length, "past customer")} of ${accounts.data.length} ${dormant.length === 1 ? "has" : "have"} gone quiet, under the ${MIN_DEALS} we'd call a pattern.`, evidence, segmentsChecked, facts: { dormant: dormant.length, accounts: accounts.data.length } };
    const wonAmounts = wonDeals.flatMap((d) => (d.Amount === null ? [] : [d.Amount]));
    const typical = wonAmounts.length >= ctx.calibration.minSample ? median(wonAmounts) : null;
    const value: Value =
      typical === null
        ? notSized(said`${wonAmounts.length} won deals with amounts is below the calibrated minimum of ${ctx.calibration.minSample}, so there's no typical deal to size this from.`)
        : atOwnWinRate(ctx, typical * dormant.length, {
            multiCurrency: data.multiCurrency,
            period: "one-off, if each account buys once more",
            basis: said`${plural(dormant.length, "dormant account")} at your median won deal of ${moneySaid(Math.round(typical), ctx.calibration.currency)}`,
            caveat: said`Assumes each account has one more deal in it, and buys as often as an average deal wins. Some will have closed or moved on, so treat this as a ceiling`,
          });
    const stranded = dormant.filter((a) => inactive.has(a.OwnerId)).length;
    return {
      outcome: "issue",
      title: said`Past customers have gone quiet`,
      summary: said`${plural(dormant.length, "account")} of ${accounts.data.length} that have bought from you have nothing open and no activity in ${dormantDays} days.${stranded ? said` ${stranded} of them belong to inactive users, so nobody owns the relationship.` : ""}`,
      recommendation: said`Agree who owns these accounts, and start with a light check-in rather than a pitch.`,
      recommendationType: "investigate",
      severity: "medium",
      confidence: "medium",
      evidence,
      value,
      effort: { size: "M" },
      owner: "Head of sales",
      segmentsChecked,
      facts: { dormant: dormant.length, accounts: accounts.data.length, stranded, ...(value.kind === "revenue" ? { sized: value.amount } : {}) },
    };
  },
};

export const revenueOpportunitiesModule: DomainModule = {
  id: DOMAIN,
  cloud: "sales",
  label: "Revenue opportunities",
  extractors: [schemaExtractor, usersExtractor, stagesExtractor, opportunitiesExtractor, wonAccountsExtractor],
  rules: [stalledDeals, winBacks, dormantAccounts],
};
