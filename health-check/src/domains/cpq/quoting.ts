import { notSized, pct, plural, type Rule, type RuleContext, type RuleResult } from "../../engine/rule.ts";
import { cpqQuotesExtractor, cpqSetupExtractor, RENEWAL_DAYS, type QuoteRow } from "../../extract/cpq.ts";
import { missingFieldReason } from "../../extract/fields.ts";
import { schemaExtractor } from "../../extract/configuration.ts";
import { opportunitiesExtractor, stagesExtractor } from "../../extract/opportunities.ts";
import { licencesExtractor, usersExtractor } from "../../extract/users.ts";
import type { Value } from "../../schemas/index.ts";
import { loadPipeline, type Deal } from "../historic-pipeline/cohort.ts";
import { median } from "../historic-pipeline/metrics.ts";
import type { DomainModule } from "../types.ts";
import { discountOf, listed, presenceGap } from "./shared.ts";

// How quotes are priced and handed on: discounts against list price and who
// approved them, whether won quotes become orders, and whether contracts ending
// soon have a renewal (DECISIONS.md 218 and 219). Read from records, so a
// set-up review leaves this area to the full check.

const DOMAIN = "cpq_quoting";
// Our judgements, each stated in the finding that uses it.
/** A team or rep discounting this many points deeper than the org's median stands out. */
export const DISCOUNT_GAP = 10;
/** A discount this deep, off list price, should have gone through an approval. */
export const DEEP_DISCOUNT = 20;
const MAX_LISTED = 4;

type Quoted = { deal: Deal; quote: QuoteRow; discount: number };

/** Why a check can't use a read cut off at its row limit. */
const cutOff = (what: string, read: number): string => `The read of ${what} stopped at its row limit after ${read.toLocaleString("en-GB")}, so the rest weren't read and we can't match every won deal to its ${what === "quotes" ? "quote" : "approval"}.`;

/** Closed deals in the window with a primary quote that has a list price, each with its discount. */
function quotedDeals(ctx: RuleContext): { rows: Quoted[]; multiCurrency: boolean; evidence: ReturnType<RuleContext["ev"]>[] } | { reason: string; evidence: ReturnType<RuleContext["ev"]>[] } {
  const { quotes } = ctx.data(cpqQuotesExtractor);
  const data = loadPipeline(ctx);
  if (quotes.status !== "ok") return { reason: `Quotes couldn't be read: ${quotes.reason}`, evidence: [ctx.ev(quotes.source, { count: null })] };
  // A deal whose quote is past the cut-off would look unquoted, and the part read is in id order, not a sample (rule 5).
  if (quotes.truncated) return { reason: cutOff("quotes", quotes.data.length), evidence: [ctx.ev(quotes.source, { count: quotes.data.length })] };
  if (data.status !== "ok") return { reason: data.reason, evidence: data.evidence };
  const byDeal = new Map(quotes.data.filter((q) => q.SBQQ__Opportunity2__c).map((q) => [q.SBQQ__Opportunity2__c!, q]));
  const rows = data.deals.flatMap((deal): Quoted[] => {
    if ((deal.category !== "won" && deal.category !== "lost") || deal.CloseDate < ctx.window.from) return [];
    const quote = byDeal.get(deal.Id);
    const discount = quote ? discountOf(quote) : null;
    return quote && discount !== null ? [{ deal, quote, discount }] : [];
  });
  const evidence = [ctx.ev(quotes.source, { count: quotes.data.length, dateRange: ctx.window, note: "Primary quotes only, matched to their opportunity" }), { ...data.evidence[0]!, count: rows.length, exclusions: [...data.exclusions, "Deals with no primary quote, or one with no list amount", "Open deals"] }];
  return { rows, multiCurrency: data.multiCurrency, evidence };
}

const winRate = (rows: Quoted[]): number => pct(rows.filter((r) => r.deal.category === "won").length, rows.length);

// The money story (DECISIONS.md 218): how far below list price deals are sold,
// by team and rep, set against whether deeper discounts win more often. Every
// figure is measured from the org's own quotes and outcomes.
const discountLeakage: Rule = {
  id: "cpq.discount-leakage",
  version: 1,
  domain: DOMAIN,
  title: "Discounts deeper than your norm that don't win more",
  evaluate(ctx): RuleResult {
    const gap = presenceGap(ctx);
    if (gap) return gap;
    const read = quotedDeals(ctx);
    if ("reason" in read) return { outcome: "not_measurable", reason: read.reason, evidence: read.evidence };
    const { rows, evidence } = read;
    const won = rows.filter((r) => r.deal.category === "won");
    const sample = ctx.calibration.minSample;
    if (won.length < sample) return { outcome: "not_measurable", reason: `Only ${plural(won.length, "won deal")} in the window ${won.length === 1 ? "has" : "have"} a primary quote with a list price, and we need ${sample} to compare discounts fairly.`, evidence };
    const norm = Math.round(median(won.map((r) => r.discount)) * 10) / 10;
    const overall = winRate(rows);
    const rep = (r: Quoted): string => ctx.userLabel(r.deal.OwnerId);
    const medianWon = (rs: Quoted[]): number => Math.round(median(rs.filter((r) => r.deal.category === "won").map((r) => r.discount)) * 10) / 10;
    const enough = (rs: Quoted[]): boolean => rs.filter((r) => r.deal.category === "won").length >= sample;
    // Like with like: a team against the other teams, and a rep against their own
    // team-mates, who sell into the same market. Both sides need the sample.
    const candidates = [
      ...[...new Set(rows.map((r) => r.deal.team))].sort().map((team) => ({ dimension: "team", key: team, members: rows.filter((r) => r.deal.team === team), peers: rows.filter((r) => r.deal.team !== team), against: "the other teams" })),
      ...[...new Set(rows.map((r) => r.deal.OwnerId))].sort().map((owner) => {
        const team = rows.find((r) => r.deal.OwnerId === owner)!.deal.team;
        const members = rows.filter((r) => r.deal.OwnerId === owner);
        return { dimension: "rep", key: rep(members[0]!), members, peers: rows.filter((r) => r.deal.team === team && r.deal.OwnerId !== owner), against: "their team-mates" };
      }),
    ];
    const flagged = candidates.flatMap((c) => {
      if (!enough(c.members) || !enough(c.peers)) return [];
      const m = medianWon(c.members);
      const pm = medianWon(c.peers);
      const rate = winRate(c.members);
      const peerRate = winRate(c.peers);
      return m >= pm + DISCOUNT_GAP && rate <= peerRate ? [{ ...c, median: m, peerMedian: pm, rate, peerRate, rows: c.members }] : [];
    });
    const deeper = rows.filter((r) => r.discount > norm);
    const shallower = rows.filter((r) => r.discount <= norm);
    const compared = deeper.length >= sample && shallower.length >= sample;
    const depth = compared ? ` Across all ${plural(rows.length, "closed deal")} with a quote, those discounted more than the median won ${winRate(deeper)}% of the time and those discounted less ${winRate(shallower)}%, though that mixes teams selling into different markets.` : "";
    const segments = [{ dimension: "team", segments: new Set(rows.map((r) => r.deal.team)).size }, { dimension: "rep", segments: new Set(rows.map((r) => r.deal.OwnerId)).size }];
    const facts: Record<string, number> = { wonQuoted: won.length, closedQuoted: rows.length, medianDiscount: norm, winRate: overall, gap: DISCOUNT_GAP, sample, ...(compared ? { deeperWinRate: winRate(deeper), shallowerWinRate: winRate(shallower) } : {}) };
    flagged.forEach((f, i) => Object.assign(facts, { [`segment${i + 1}Median`]: f.median, [`segment${i + 1}PeerMedian`]: f.peerMedian, [`segment${i + 1}WinRate`]: f.rate, [`segment${i + 1}PeerWinRate`]: f.peerRate }));
    const opening = `On ${plural(won.length, "won deal")} with a quote, the median discount off list price is ${norm}%, and ${overall}% of closed deals with a quote were won.`;
    if (flagged.length === 0) return { outcome: "pass", summary: `${opening} No team discounts ${DISCOUNT_GAP} points deeper than the other teams, and no rep deeper than their team-mates, without winning more often.${depth}`, evidence, segmentsChecked: segments, facts };
    // Each won deal once, at the most it went past its peers' median.
    const excess = new Map<string, { row: Quoted; points: number }>();
    for (const f of flagged) {
      for (const r of f.rows) {
        const points = r.discount - f.peerMedian;
        if (r.deal.category === "won" && points > 0 && points > (excess.get(r.deal.Id)?.points ?? 0)) excess.set(r.deal.Id, { row: r, points });
      }
    }
    const amount = Math.round([...excess.values()].reduce((sum, e) => sum + (e.points / 100) * (e.row.quote.SBQQ__ListAmount__c ?? 0), 0));
    facts.excessDeals = excess.size;
    const value: Value = read.multiCurrency
      ? notSized("The org is multi-currency, so these amounts can't be added.")
      : amount <= 0
        ? notSized("The deals in these segments have no discount beyond the median to count.")
        : {
            kind: "revenue",
            amount,
            unit: ctx.calibration.currency,
            // The value model reads the start of this to put it over 12 months (value/model.ts, OVER_WINDOW).
            period: `in discount beyond what peers gave, ${ctx.window.from} to ${ctx.window.to}, a potential figure`,
            assumptions: [
              `${plural(excess.size, "won deal")} in the teams and reps named, each counted once, at the most it went past its peers' median`,
              "Each deal's discount beyond its peers' median, applied to its quote's list amount",
              "Measured from your own quotes. It's potential: it assumes those deals would still have been won at your median discount, which nobody can know",
            ],
          };
    if (value.kind === "revenue") facts.excess = value.amount;
    return {
      outcome: "issue",
      title: "Some teams and reps discount deeper without winning more",
      summary: `${opening} ${listed(flagged.slice(0, MAX_LISTED).map((f) => `${f.key} discounts ${f.median}% against ${f.peerMedian}% for ${f.against}, and wins ${f.rate}% of closed deals against ${f.peerRate}%`))}${flagged.length > MAX_LISTED ? `, and ${flagged.length - MAX_LISTED} more` : ""}. That's at least ${DISCOUNT_GAP} points deeper, without winning more often.${depth}`,
      recommendation: "Agree a discount policy with limits by deal size, send anything deeper through approval, and review these deals against it in pipeline meetings.",
      recommendationType: "change",
      severity: "high",
      // Discount can follow deal size or product mix, which the quote total doesn't show.
      confidence: "medium",
      evidence,
      value,
      effort: { size: "M" },
      owner: "Head of sales",
      dependencies: ["Agree the policy with finance before enforcing it in CPQ"],
      segmentsChecked: segments,
      subjects: flagged.map((f) => ({ type: f.dimension, label: f.key })),
      facts,
    };
  },
};

const discountApprovals: Rule = {
  id: "cpq.discount-approvals",
  version: 3,
  domain: DOMAIN,
  title: "Deep discounts with no approval",
  evaluate(ctx): RuleResult {
    const gap = presenceGap(ctx);
    if (gap) return gap;
    const { presence } = ctx.data(cpqSetupExtractor);
    const { quotes, approvals } = ctx.data(cpqQuotesExtractor);
    if (quotes.status !== "ok") return { outcome: "not_measurable", reason: `Quotes couldn't be read: ${quotes.reason}`, evidence: [ctx.ev(quotes.source, { count: null })] };
    if (presence.status === "installed" && presence.advancedApprovals) return { outcome: "not_measurable", reason: "Advanced Approvals is installed, and we don't read its approvals yet, so we can't say which discounts were approved.", evidence: [ctx.ev(quotes.source, { count: quotes.data.length })] };
    if (approvals.status !== "ok") return { outcome: "not_measurable", reason: `Approvals couldn't be read: ${approvals.reason}`, evidence: [ctx.ev(approvals.source, { count: null })] };
    // An approval past the cut-off would make its quote look unapproved.
    const cut = quotes.truncated ? ["quotes", quotes] as const : approvals.truncated ? ["approvals", approvals] as const : null;
    if (cut) return { outcome: "not_measurable", reason: cutOff(cut[0], cut[1].data.length), evidence: [ctx.ev(cut[1].source, { count: cut[1].data.length })] };
    // Only a quote on a won deal is a discount given. A draft on a deal still open may not have reached its approval yet.
    const pipeline = loadPipeline(ctx);
    if (pipeline.status !== "ok") return { outcome: "not_measurable", reason: pipeline.reason, evidence: pipeline.evidence };
    const won = new Set(pipeline.deals.filter((d) => d.category === "won").map((d) => d.Id));
    const approved = new Set(approvals.data.map((a) => a.TargetObjectId.slice(0, 15)));
    const deep = quotes.data.filter((q) => q.SBQQ__Opportunity2__c !== null && won.has(q.SBQQ__Opportunity2__c)).flatMap((q) => {
      const d = discountOf(q);
      return d !== null && d >= DEEP_DISCOUNT ? [{ q, d }] : [];
    });
    const without = deep.filter(({ q }) => !approved.has(q.Id.slice(0, 15)));
    // A cut-off read can't show a quote had no approval: it may be among the rows not read (DECISIONS.md 325).
    if (without.length > 0 && approvals.truncated) return { outcome: "not_measurable", reason: `The approvals read stopped at ${approvals.data.length.toLocaleString("en-GB")} rows, so a quote with no approval among them may have one we didn't read.`, evidence: [ctx.ev(approvals.source, { count: approvals.data.length, dateRange: ctx.window, note: "Approval processes started in the window, cut off" })] };
    const anyApproval = quotes.data.some((q) => approved.has(q.Id.slice(0, 15)));
    const evidence = [ctx.ev(quotes.source, { count: deep.length, dateRange: ctx.window, note: `Primary quotes on won deals discounted ${DEEP_DISCOUNT}% or more off list price`, exclusions: ["Quotes on deals still open or lost, whose approval may not have been needed or reached yet"] }), ctx.ev(approvals.source, { count: approvals.data.length, dateRange: ctx.window, note: "Approval processes started in the window, matched to quotes by id" })];
    const share = pct(without.length, deep.length);
    const facts = { deep: deep.length, without: without.length, share, threshold: DEEP_DISCOUNT };
    if (deep.length === 0) return { outcome: "pass", summary: `No primary quote on a won deal in the window was discounted ${DEEP_DISCOUNT}% or more off list price.`, evidence, segmentExemptReason: "There are no deep discounts to segment.", facts };
    const segments = [{ dimension: "approval", segments: 2 }];
    if (without.length === 0) return { outcome: "pass", summary: `All ${plural(deep.length, "primary quote")} on won deals discounted ${DEEP_DISCOUNT}% or more went through an approval process.`, evidence, segmentsChecked: segments, facts };
    return {
      outcome: "issue",
      title: "Deep discounts aren't going through approval",
      summary: `Of ${plural(deep.length, "primary quote")} on won deals in the window discounted ${DEEP_DISCOUNT}% or more off list price, ${without.length} (${share}%) never went through an approval process in Salesforce.${anyApproval ? "" : " No quote went through one at all."} We use ${DEEP_DISCOUNT}% as the line, which is our judgement rather than your policy.`,
      recommendation: "Set the discount levels that need sign-off, build them into an approval process on the quote, and check who agreed the ones listed.",
      recommendationType: "change",
      severity: share >= 50 ? "medium" : "low",
      // Approval can happen outside Salesforce, by email or in a meeting.
      confidence: "medium",
      evidence,
      value: notSized("Unapproved discount is a control gap; the leakage check sizes what discounting costs."),
      effort: { size: "M" },
      owner: "Head of sales",
      dependencies: ["Agree the discount levels with finance first"],
      segmentsChecked: segments,
      facts,
      records: { object: "SBQQ__Quote__c", rows: without.map(({ q, d }) => ({ id: q.Id, problem: `${d}% off list price, with no approval in Salesforce` })), fix: "Check who agreed the discount, and send discounts this deep through approval" },
    };
  },
};

const quoteToOrder: Rule = {
  id: "cpq.quote-to-order",
  version: 1,
  domain: DOMAIN,
  title: "Won quotes that never became orders",
  evaluate(ctx): RuleResult {
    const gap = presenceGap(ctx);
    if (gap) return gap;
    const read = quotedDeals(ctx);
    if ("reason" in read) return { outcome: "not_measurable", reason: read.reason, evidence: read.evidence };
    const { quotes } = ctx.data(cpqQuotesExtractor);
    const missing = missingFieldReason(quotes, "SBQQ__Quote__c.SBQQ__Ordered__c");
    if (missing) return { outcome: "not_measurable", reason: missing, evidence: read.evidence };
    const won = read.rows.filter((r) => r.deal.category === "won");
    const ordered = won.filter((r) => r.quote.SBQQ__Ordered__c === true);
    const unordered = won.filter((r) => r.quote.SBQQ__Ordered__c !== true);
    const facts = { won: won.length, ordered: ordered.length, unordered: unordered.length, share: pct(unordered.length, won.length) };
    if (won.length === 0) return { outcome: "pass", summary: "No deal with a primary quote was won in the window.", evidence: read.evidence, segmentExemptReason: "There are no won deals to segment.", facts };
    // An org that never orders closes some other way, and that's not ours to judge.
    if (ordered.length === 0) return { outcome: "not_measurable", reason: `None of the ${plural(won.length, "won deal")} with a primary quote was ordered in Salesforce, so orders don't look to be part of how you close, and we can't judge the handover.`, evidence: read.evidence };
    const segments = [{ dimension: "team", segments: new Set(won.map((r) => r.deal.team)).size }];
    if (unordered.length === 0) return { outcome: "pass", summary: `All ${plural(won.length, "won deal")} with a primary quote were ordered.`, evidence: read.evidence, segmentsChecked: segments, facts };
    return {
      outcome: "issue",
      title: "Some won quotes never became orders",
      summary: `Of ${plural(won.length, "won deal")} with a primary quote, ${unordered.length} (${facts.share}%) ${unordered.length === 1 ? "hasn't" : "haven't"} been ordered, while ${ordered.length} ${ordered.length === 1 ? "has" : "have"}. If orders are how you bill or deliver, these may not have reached finance or the team that delivers them.`,
      recommendation: "Order each of these quotes, or record why the deal doesn't need an order, and make ordering part of closing a deal.",
      recommendationType: "change",
      severity: "medium",
      confidence: "medium",
      evidence: read.evidence,
      value: notSized("We can't tell from Salesforce whether these deals were billed some other way."),
      effort: { size: "S" },
      owner: "Sales operations",
      segmentsChecked: segments,
      facts,
      records: { object: "Opportunity", rows: unordered.map((r) => ({ id: r.deal.Id, problem: `Won, closing ${r.deal.CloseDate}, and its primary quote isn't ordered` })), fix: "Order the quote, or record why this deal doesn't need an order" },
    };
  },
};

const renewalsNotStarted: Rule = {
  id: "cpq.renewals-not-started",
  version: 1,
  domain: DOMAIN,
  title: "Contracts ending soon with no renewal opportunity",
  evaluate(ctx): RuleResult {
    const gap = presenceGap(ctx);
    if (gap) return gap;
    const { contracts } = ctx.data(cpqQuotesExtractor);
    if (contracts.status !== "ok") return { outcome: "not_measurable", reason: `Contracts couldn't be read: ${contracts.reason}`, evidence: [ctx.ev(contracts.source, { count: null })] };
    const pending = contracts.data.filter((c) => !c.SBQQ__RenewalOpportunity__c).sort((a, b) => a.EndDate.localeCompare(b.EndDate));
    const evidence = [ctx.ev(contracts.source, { count: contracts.data.length, note: `Activated contracts ending in the next ${RENEWAL_DAYS} days` })];
    const facts = { contracts: contracts.data.length, pending: pending.length, days: RENEWAL_DAYS };
    if (contracts.data.length === 0) return { outcome: "pass", summary: `No activated contract ends in the next ${RENEWAL_DAYS} days.`, evidence, segmentExemptReason: "There are no contracts to segment.", facts };
    const segments = [{ dimension: "owner", segments: new Set(contracts.data.map((c) => c.OwnerId)).size }];
    if (pending.length === 0) return { outcome: "pass", summary: `All ${plural(contracts.data.length, "activated contract")} ending in the next ${RENEWAL_DAYS} days have a renewal opportunity.`, evidence, segmentsChecked: segments, facts };
    return {
      outcome: "issue",
      title: "Contracts are ending with no renewal opportunity",
      summary: `${pending.length} of ${plural(contracts.data.length, "activated contract")} ending in the next ${RENEWAL_DAYS} days ${pending.length === 1 ? "has" : "have"} no renewal opportunity, so ${pending.length === 1 ? "it isn't" : "they aren't"} in the pipeline or the forecast. The first ends on ${pending[0]!.EndDate}.`,
      recommendation: "Create each renewal opportunity now, give it an owner, and have CPQ create renewals automatically from here on.",
      recommendationType: "change",
      severity: "high",
      confidence: "high",
      evidence,
      value: notSized("We don't read what each contract is worth, so the renewals at stake aren't sized."),
      effort: { size: "S" },
      owner: "Head of sales",
      segmentsChecked: segments,
      facts,
      records: { object: "Contract", rows: pending.map((c) => ({ id: c.Id, problem: `Ends ${c.EndDate}, with no renewal opportunity` })), fix: "Create the renewal opportunity, and give it an owner" },
    };
  },
};

export const cpqQuotingModule: DomainModule = {
  id: DOMAIN,
  cloud: "cpq",
  label: "Quoting and discounts",
  extractors: [schemaExtractor, usersExtractor, stagesExtractor, opportunitiesExtractor, licencesExtractor, cpqSetupExtractor, cpqQuotesExtractor],
  rules: [discountLeakage, renewalsNotStarted, discountApprovals, quoteToOrder],
};
