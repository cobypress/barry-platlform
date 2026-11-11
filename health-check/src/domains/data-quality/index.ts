import { duplicateRulesExtractor, recordCountsExtractor, schemaExtractor } from "../../extract/configuration.ts";
import { crmHealthExtractor, opportunitiesExtractor, stagesExtractor } from "../../extract/opportunities.ts";
import { leadsExtractor } from "../../extract/leads.ts";
import { actionRecordsExtractor } from "../../extract/action-records.ts";
import { DUPLICATE_GROUPS, duplicateAccountsExtractor } from "../../extract/duplicates.ts";
import { daysBefore } from "../../extract/types.ts";
import { ownershipExtractor, usersExtractor } from "../../extract/users.ts";
import { notSized, pct, plural, segmentRate, type RecordList, type Rule, type RuleContext, type RuleResult } from "../../engine/rule.ts";
import { localDay, todayIn, zoneOf } from "../time-zone.ts";
import { atOwnLeadReturn } from "../value.ts";
import { loadPipeline, type Deal } from "../historic-pipeline/cohort.ts";
import { potentialRevenue, SITUATIONS, type Situation } from "../historic-pipeline/potential.ts";
import type { Value } from "../../schemas/index.ts";
import type { DomainModule } from "../types.ts";

const STALE_ACTIVITY_DAYS = 90;
const MIN_SEGMENT = 10;
const DUPLICATE_OBJECTS = ["Account", "Contact", "Lead"];
const LEAD_STALE_LIMIT = 30;
const CONTACT_GAP_LIMIT = 10;
const OWNER_GAP_LIMIT = 1;

const dealDimensions = (ctx: RuleContext): Record<string, (d: Deal) => string> => ({
  "record type": (d) => d.RecordType ?? "No record type",
  team: (d) => d.team,
  rep: (d) => ctx.userLabel(d.OwnerId),
});

export function exposure(ctx: RuleContext, deals: Deal[], multiCurrency: boolean, note: string): Value {
  if (multiCurrency) return notSized("The org is multi-currency, so these amounts can't be added.");
  const amount = deals.reduce((sum, d) => sum + (d.Amount ?? 0), 0);
  if (amount === 0) return notSized("These deals have no amounts.");
  return { kind: "revenue", amount: Math.round(amount), unit: ctx.calibration.currency, period: "open pipeline today", assumptions: [note, "The total of Amount on affected deals: exposure, not expected loss"] };
}

type DealRuleOptions = {
  id: string;
  /** Defaults to 1. */
  version?: number;
  title: string;
  /** The share of affected deals overall above which this is an issue. */
  totalLimit: number;
  /** The same for one segment, which can trip the rule even when the total is fine. */
  segmentLimit: number;
  severity: "high" | "medium" | "low";
  scope: (d: Deal, ctx: RuleContext) => boolean;
  scopeLabel: string;
  isHit: (d: Deal, ctx: RuleContext) => boolean;
  issueTitle: string;
  sentence: (hits: number, n: number, rate: number) => string;
  recommendation: string;
  effort: "XS" | "S" | "M";
  owner: string;
  /** True sizes the raw exposure. A situation sizes potential revenue at how often comparable deals still won. */
  sized: boolean | ((ctx: RuleContext, data: Extract<ReturnType<typeof loadPipeline>, { status: "ok" }>, deal: Deal) => Situation);
  /** Each affected deal's line in the action list: what's wrong with it, with no name, and what to do. */
  action: { problem: (d: Deal, ctx: RuleContext) => string; fix: string };
};

function dealRule(options: DealRuleOptions): Rule {
  return {
    id: options.id,
    version: options.version ?? 1,
    domain: "data_quality",
    title: options.title,
    evaluate(ctx): RuleResult | RuleResult[] {
      const data = loadPipeline(ctx);
      if (data.status !== "ok") return { outcome: "not_measurable", reason: data.reason, evidence: data.evidence };
      if (data.unmapped.finalStages.length > 0) {
        return { outcome: "not_measurable", reason: `The calibration doesn't say whether these stages are open: ${data.unmapped.finalStages.join(", ")}.`, evidence: data.evidence };
      }
      const deals = data.deals.filter((d) => options.scope(d, ctx));
      const today = { from: ctx.window.to, to: ctx.window.to };
      const evidence = [{ ...data.evidence[0]!, count: deals.length, dateRange: today, exclusions: data.exclusions, note: options.scopeLabel }];
      if (deals.length === 0) {
        return { outcome: "pass", summary: `There are no ${options.scopeLabel} to check.`, evidence, segmentExemptReason: "There are no deals to segment." };
      }
      const hit = (d: Deal): boolean => options.isHit(d, ctx);
      const result = segmentRate(deals, hit, dealDimensions(ctx), { segmentLimit: options.segmentLimit, minSegment: MIN_SEGMENT });
      const overLimit = result.rate > options.totalLimit;
      if (!overLimit && result.hotspots.length === 0) {
        return {
          outcome: "pass",
          summary: `Affected: ${result.hits} of ${result.n} ${options.scopeLabel} (${result.rate}%), with no segment above ${options.segmentLimit}%.`,
          evidence,
          segmentsChecked: result.segmentsChecked,
          facts: { hits: result.hits, n: result.n, rate: result.rate },
        };
      }
      const worst = result.hotspots[0];
      const hidden = !overLimit ? ` The total looks acceptable, but by ${worst!.dimension}, ${worst!.key} is at ${worst!.rate}% (${worst!.hits} of ${worst!.n}).` : worst ? ` Worst by ${worst.dimension}: ${worst.key}, at ${worst.rate}%.` : "";
      return {
        outcome: "issue",
        title: options.issueTitle,
        summary: `${options.sentence(result.hits, result.n, result.rate)}${hidden}`,
        recommendation: options.recommendation,
        recommendationType: "change",
        severity: options.severity,
        confidence: "high",
        evidence,
        value:
          typeof options.sized === "function"
            ? potentialRevenue(ctx, data, deals.filter(hit), (d) => (options.sized as Exclude<DealRuleOptions["sized"], boolean>)(ctx, data, d))
            : options.sized
              ? exposure(ctx, deals.filter(hit), data.multiCurrency, `${result.hits} deals affected`)
              : notSized("This affects how far the pipeline can be trusted, which has no direct figure."),
        effort: { size: options.effort },
        owner: options.owner,
        segmentsChecked: result.segmentsChecked,
        subjects: result.hotspots.slice(0, 5).map((h) => ({ type: h.dimension, label: h.key })),
        facts: { hits: result.hits, n: result.n, rate: result.rate, ...(worst ? { worstSegmentRate: worst.rate } : {}) },
        records: { object: "Opportunity", rows: deals.filter(hit).map((d) => ({ id: d.Id, problem: options.action.problem(d, ctx) })), fix: options.action.fix },
      };
    },
  };
}

const isOpen = (d: Deal): boolean => d.category === "open";

const pastCloseDate = dealRule({
  id: "dq.open-opps-past-close",
  title: "Open deals past their close date",
  totalLimit: 10,
  segmentLimit: 25,
  severity: "high",
  scope: isOpen,
  scopeLabel: "open deals",
  isHit: (d, ctx) => d.CloseDate < todayIn(ctx),
  issueTitle: "Your pipeline includes deals that should already have closed",
  sentence: (hits, n, rate) => `Open deals with a close date in the past: ${hits} of ${n} (${rate}%). The forecast is counting them when it shouldn't.`,
  recommendation: "Have each owner close the deal or give it a real date, then add a weekly check to pipeline reviews.",
  effort: "S",
  owner: "Head of sales",
  sized: (ctx, data) => SITUATIONS.pastClose(ctx, data),
  action: { problem: (d) => `Close date ${d.CloseDate}, still open in ${d.StageName}`, fix: "Close it as won or lost, or give it a real close date" },
});

const inactiveOwner = dealRule({
  id: "dq.opps-inactive-owner",
  title: "Open deals owned by inactive users",
  totalLimit: 0,
  segmentLimit: 100,
  severity: "high",
  scope: isOpen,
  scopeLabel: "open deals",
  isHit: (d, ctx) => {
    const users = ctx.data(usersExtractor);
    return users.status === "ok" && users.data.some((u) => u.Id === d.OwnerId && !u.IsActive);
  },
  issueTitle: "Open deals have no active owner",
  sentence: (hits, n, rate) => `Open deals owned by inactive users: ${hits} of ${n} (${rate}%). Nobody is working them.`,
  recommendation: "Reassign these deals now, and add reassignment to your leaver process.",
  effort: "XS",
  owner: "Sales operations",
  // When an owner left isn't in stage history, so the comparison is deals that reached the same stage.
  sized: (_ctx, _data, d) => SITUATIONS.inStage(d.StageName),
  action: { problem: () => "Its owner is an inactive user", fix: "Give it to an active owner" },
});

const missingAmount = dealRule({
  id: "dq.open-opps-missing-amount",
  title: "Open deals with no amount",
  totalLimit: 10,
  segmentLimit: 30,
  severity: "medium",
  scope: isOpen,
  scopeLabel: "open deals",
  isHit: (d) => d.Amount === null || d.Amount === 0,
  issueTitle: "Open deals are missing an amount",
  sentence: (hits, n, rate) =>
    `Open deals with no amount: ${hits} of ${n} (${rate}%). These deals count for nothing in your pipeline value, so it looks smaller than it is, and you can't see what work is coming or weigh one deal against another. When enough are blank, nobody can tell how much of the pipeline rests on a few accounts or people.`,
  recommendation: "Require an amount, even an estimate, once a price is known, and fill in the gaps.",
  effort: "S",
  owner: "Sales operations",
  sized: false,
  action: { problem: (d) => (d.Amount === 0 ? "Amount is 0" : "Amount is blank"), fix: "Enter the amount, even an estimate" },
});

const noRecentActivity = dealRule({
  id: "dq.open-opps-no-activity",
  version: 2,
  title: "Open deals with no recent activity",
  totalLimit: 25,
  segmentLimit: 50,
  severity: "medium",
  scope: isOpen,
  scopeLabel: "open deals",
  isHit: (d, ctx) => {
    const cutoff = localDay(daysBefore(ctx.now, STALE_ACTIVITY_DAYS), zoneOf(ctx));
    return d.LastActivityDate === null || d.LastActivityDate < cutoff;
  },
  // Titled by its test, as two other checks find quiet deals by theirs (night 3 reading pass, item 16).
  issueTitle: `Open deals with no activity in ${STALE_ACTIVITY_DAYS} days`,
  sentence: (hits, n, rate) => `Open deals with no logged activity in ${STALE_ACTIVITY_DAYS} days: ${hits} of ${n} (${rate}%).`,
  recommendation: "Review each quiet deal: either re-engage it or close it. Check that activity is being captured before treating this as a rep problem.",
  effort: "S",
  owner: "Head of sales",
  // Past activity isn't in stage history. The nearest it holds is a deal nobody changed in as long.
  sized: (_ctx, data) => SITUATIONS.unchangedFor(STALE_ACTIVITY_DAYS, data),
  action: { problem: (d) => (d.LastActivityDate ? `Last activity ${d.LastActivityDate}` : "No activity logged"), fix: "Re-engage the customer and log it, or close the deal" },
});

const noAccount = dealRule({
  id: "dq.opps-without-account",
  title: "Open deals with no account",
  totalLimit: 2,
  segmentLimit: 20,
  severity: "medium",
  scope: isOpen,
  scopeLabel: "open deals",
  isHit: (d) => !d.AccountId,
  issueTitle: "Some open deals aren't linked to a customer",
  sentence: (hits, n, rate) => `Open deals with no account: ${hits} of ${n} (${rate}%). Nobody looking at the customer sees them, and they're left out of every account-level report and figure.`,
  recommendation: "Link each deal to its account, and make the account required on new deals.",
  effort: "XS",
  owner: "Sales operations",
  sized: false,
  action: { problem: () => "No account", fix: "Link it to the customer's account" },
});

// Exact names only (DECISIONS.md 205): "Acme Ltd" and "Acme Limited" aren't
// matched, so this understates duplication rather than overstating it.
const DUPLICATE_SHARE_MEDIUM = 5;

const duplicateAccounts: Rule = {
  id: "dq.duplicate-accounts",
  version: 1,
  records: true,
  domain: "data_quality",
  title: "Accounts that share a name",
  evaluate(ctx): RuleResult | RuleResult[] {
    const { groups, accounts } = ctx.data(duplicateAccountsExtractor);
    if (groups.status !== "ok") return { outcome: "not_measurable", reason: `Accounts couldn't be grouped by name: ${groups.reason}`, evidence: [ctx.ev(groups.source, { count: null })] };
    const counts = ctx.data(recordCountsExtractor);
    const total = counts.status === "ok" ? (counts.data.find((o) => o.name === "Account")?.count ?? null) : null;
    const sizes = new Map(groups.data.map((g) => [g.Name, g.c]));
    const inGroups = groups.data.reduce((sum, g) => sum + g.c, 0);
    const evidence = [ctx.ev(groups.source, { count: groups.data.length, exclusions: groups.data.length >= DUPLICATE_GROUPS ? [`Groups past the ${DUPLICATE_GROUPS} largest`] : [], note: "An exact match on the account name. Differences in spacing, punctuation or spelling aren't caught" })];
    if (counts.status === "ok") evidence.push(ctx.ev(counts.source, { count: total }));
    const share = total ? pct(inGroups, total) : null;
    const facts = { accounts: inGroups, groups: groups.data.length, ...(share !== null && total ? { share, total } : {}) };
    if (groups.data.length === 0) return { outcome: "pass", summary: "No two accounts share an exact name.", evidence, segmentExemptReason: "Every account is compared with every other at once.", facts };
    return {
      outcome: "issue",
      title: "Some accounts look like duplicates",
      summary: `${plural(inGroups, "account")} share their exact name with at least one other, in ${plural(groups.data.length, "group")}${share !== null && total ? `: ${share}% of ${total.toLocaleString("en-GB")} accounts` : ""}. Deals, contacts and activity split across the copies, so no one record shows the whole customer.`,
      recommendation: "Check each group, merge the true duplicates into the record with the most history, then turn on duplicate rules so new ones are caught.",
      // Merging is destructive, and some may be different companies or branches that share a name (rule 3).
      recommendationType: "investigate",
      severity: share !== null && share >= DUPLICATE_SHARE_MEDIUM ? "medium" : "low",
      confidence: "medium",
      evidence,
      value: notSized("We can't say what the split history costs from the names alone."),
      effort: { size: groups.data.length > 50 ? "L" : "M" },
      owner: "Sales operations",
      dependencies: ["Check that integrations don't hold any of these accounts' ids before merging"],
      segmentsChecked: [{ dimension: "name", segments: groups.data.length }],
      facts,
      ...(accounts.status === "ok"
        ? {
            records: {
              object: "Account" as const,
              rows: accounts.data.map((a) => ({ id: a.Id, problem: `Shares its exact name with ${plural((sizes.get(a.Name) ?? 2) - 1, "other account")}` })),
              fix: "Check whether it's the same customer, and merge it into one record if so",
              ...(accounts.truncated ? { part: { total: inGroups, rest: "In Salesforce, an accounts report grouped by account name, showing groups of two or more, lists them all." } } : {}),
            },
          }
        : {}),
    };
  },
};

const lossReasonMissing: Rule = {
  id: "dq.loss-reason-missing",
  version: 1,
  domain: "data_quality",
  title: "Lost deals with no reason recorded",
  evaluate(ctx): RuleResult | RuleResult[] {
    const extract = ctx.data(opportunitiesExtractor);
    const data = loadPipeline(ctx);
    if (data.status !== "ok") return { outcome: "not_measurable", reason: data.reason, evidence: data.evidence };
    if (!extract.lossReasonField) {
      return { outcome: "not_measurable", reason: "The calibration doesn't name a loss reason field that exists on Opportunity, so we can't tell a blank reason from a missing field.", evidence: data.evidence };
    }
    const lost = data.deals.filter((d) => d.category === "lost" && d.CloseDate >= ctx.window.from);
    if (lost.length === 0) return { outcome: "pass", summary: "No deals were lost in the window.", evidence: data.evidence, segmentExemptReason: "There are no lost deals to segment." };
    const result = segmentRate(lost, (d) => !d.LossReason, dealDimensions(ctx), { segmentLimit: 40, minSegment: MIN_SEGMENT });
    const evidence = [{ ...data.evidence[0]!, count: lost.length, exclusions: data.exclusions, note: "Lost deals with a close date in the window" }];
    if (result.rate <= 20 && result.hotspots.length === 0) {
      return { outcome: "pass", summary: `Lost deals with no reason: ${result.hits} of ${result.n} (${result.rate}%), with no segment above 40%.`, evidence, segmentsChecked: result.segmentsChecked, facts: { hits: result.hits, n: result.n, rate: result.rate } };
    }
    const worst = result.hotspots[0];
    return {
      outcome: "issue",
      title: "You can't see why deals are being lost",
      summary: `Lost deals with no loss reason: ${result.hits} of ${result.n} (${result.rate}%).${worst ? ` Worst by ${worst.dimension}: ${worst.key}, at ${worst.rate}%.` : ""}`,
      recommendation: "Make a reason required when a deal is lost, and keep the list short enough to be used honestly.",
      recommendationType: "change",
      severity: "medium",
      confidence: "high",
      evidence,
      value: notSized("Without reasons, the value of fixing losses can't be sized."),
      effort: { size: "XS" },
      owner: "Sales operations",
      segmentsChecked: result.segmentsChecked,
      facts: { hits: result.hits, n: result.n, rate: result.rate },
      records: { object: "Opportunity", rows: lost.filter((d) => !d.LossReason).map((d) => ({ id: d.Id, problem: `Lost, close date ${d.CloseDate}, with no loss reason` })), fix: "Record why it was lost" },
    };
  },
};

const inactiveRecordOwners: Rule = {
  id: "dq.records-inactive-owner",
  version: 1,
  domain: "data_quality",
  title: "Accounts, contacts and leads owned by inactive users",
  evaluate(ctx): RuleResult | RuleResult[] {
    const users = ctx.data(usersExtractor);
    if (users.status !== "ok") return { outcome: "not_measurable", reason: `Users couldn't be read: ${users.reason}`, evidence: [ctx.ev(users.source, { count: null })] };
    const ownership = ctx.data(ownershipExtractor);
    const objects = ["Account", "Contact", "Lead"].filter((o) => ownership.scanned.includes(o));
    const source = { kind: "soql", api: "rest", query: ownership.query } as const;
    if (objects.length === 0) return { outcome: "not_measurable", reason: "Record ownership couldn't be read.", evidence: [ctx.ev(source, { count: null })] };
    const inactive = new Set(users.data.filter((u) => !u.IsActive).map((u) => u.Id));
    const totals = Object.fromEntries(objects.map((o) => [o, 0]));
    const stranded = Object.fromEntries(objects.map((o) => [o, 0]));
    for (const [owner, counts] of Object.entries(ownership.byOwner)) {
      for (const object of objects) {
        totals[object]! += counts[object] ?? 0;
        if (inactive.has(owner)) stranded[object]! += counts[object] ?? 0;
      }
    }
    const total = Object.values(totals).reduce((a, b) => a + b, 0);
    const strandedTotal = Object.values(stranded).reduce((a, b) => a + b, 0);
    const worst = objects.map((o) => ({ object: o, rate: pct(stranded[o]!, totals[o]!) })).sort((a, b) => b.rate - a.rate)[0]!;
    const evidence = [ctx.ev(source, { count: strandedTotal, note: "Owners that are queues aren't counted as inactive" })];
    const segments = [{ dimension: "object", segments: objects.length }];
    if (worst.rate <= OWNER_GAP_LIMIT) {
      return { outcome: "pass", summary: `No object has more than ${OWNER_GAP_LIMIT}% of its records with an inactive owner.`, evidence, segmentsChecked: segments, facts: { strandedTotal, total } };
    }
    return {
      outcome: "issue",
      title: "Customer records belong to people who've left",
      summary: `Accounts, contacts and leads with an inactive owner: ${strandedTotal} of ${total} (${objects.map((o) => `${o}: ${stranded[o]} of ${totals[o]}`).join(", ")}).`,
      recommendation: "Reassign these records in bulk, and make reassignment part of your leaver process.",
      recommendationType: "change",
      severity: "medium",
      confidence: "high",
      evidence,
      value: notSized("We can't say what these records would be worth with an active owner."),
      effort: { size: "S" },
      owner: "Sales operations",
      segmentsChecked: segments,
      facts: { strandedTotal, total, worstObjectRate: worst.rate },
    };
  },
};

const duplicateRulesMissing: Rule = {
  id: "dq.duplicate-rules-missing",
  version: 1,
  domain: "data_quality",
  title: "No active duplicate rule on core objects",
  evaluate(ctx): RuleResult | RuleResult[] {
    const rules = ctx.data(duplicateRulesExtractor);
    if (rules.status !== "ok") return { outcome: "not_measurable", reason: `Duplicate rules couldn't be read: ${rules.reason}`, evidence: [ctx.ev(rules.source, { count: null })] };
    const active = new Set(rules.data.filter((r) => r.IsActive).map((r) => r.SobjectType));
    // A customer review: a missing rule on an object nobody uses isn't worth
    // saying. Lead counts as unused only when we can see no leads were created.
    const { leads } = ctx.data(leadsExtractor);
    const unused = new Set(leads.status === "ok" && !leads.truncated && leads.data.length === 0 ? ["Lead"] : []);
    const missing = DUPLICATE_OBJECTS.filter((o) => !active.has(o) && !unused.has(o));
    const skipped = DUPLICATE_OBJECTS.filter((o) => !active.has(o) && unused.has(o));
    const skippedNote = skipped.length ? ` ${skipped.join(", ")} ${skipped.length === 1 ? "has" : "have"} none either, but no leads were created in the window, so it doesn't matter yet.` : "";
    const evidence = [ctx.ev(rules.source, { count: rules.data.length }), ...(skipped.length ? [ctx.ev(leads.source, { count: 0, dateRange: ctx.window, note: "Leads created in the window" })] : [])];
    const segments = [{ dimension: "object", segments: DUPLICATE_OBJECTS.length }];
    if (missing.length === 0) {
      const summary = skipped.length ? `Every core object in use has an active duplicate rule.${skippedNote}` : "Account, Contact and Lead all have an active duplicate rule.";
      return { outcome: "pass", summary, evidence, segmentsChecked: segments };
    }
    return {
      outcome: "issue",
      title: "Nothing stops duplicates on some core objects",
      summary: `${missing.length > 1 ? `${missing.slice(0, -1).join(", ")} and ${missing.at(-1)}` : missing[0]} ${missing.length === 1 ? "has" : "have"} no active duplicate rule, so the same customer can be entered more than once.${skippedNote}`,
      recommendation: "Turn on matching and duplicate rules, starting in alert mode so you can see the effect.",
      recommendationType: "change",
      severity: "medium",
      confidence: "high",
      evidence,
      value: notSized("We don't yet measure how many duplicates exist."),
      effort: { size: "S" },
      owner: "Salesforce admin",
      segmentsChecked: segments,
      subjects: missing.map((label) => ({ type: "object", label })),
      facts: { missing: missing.length },
    };
  },
};

const staleLeads: Rule = {
  id: "dq.stale-leads",
  version: 2,
  domain: "data_quality",
  title: "Open leads nobody has touched",
  evaluate(ctx): RuleResult | RuleResult[] {
    const health = ctx.data(crmHealthExtractor);
    const open = health.counts.openLeads!;
    const stale = health.counts.staleLeads!;
    if (open.status !== "ok") return { outcome: "not_measurable", reason: `Leads couldn't be counted: ${open.reason}`, evidence: [ctx.ev(open.source, { count: null })] };
    if (stale.status !== "ok") return { outcome: "not_measurable", reason: `Stale leads couldn't be counted: ${stale.reason}`, evidence: [ctx.ev(stale.source, { count: null })] };
    if (health.staleLeadsBySource.status !== "ok" || health.openLeadsBySource.status !== "ok") {
      // A healthy total proves nothing without its segments (rule 4).
      return { outcome: "not_measurable", reason: "Leads couldn't be split by source, so we can't say whether the total hides a problem.", evidence: [ctx.ev(stale.source, { count: stale.data })] };
    }
    const openBySource = new Map(health.openLeadsBySource.data.map((r) => [r.s ?? "No source", r.c]));
    const hotspots = health.staleLeadsBySource.data
      .map((r) => ({ source: r.s ?? "No source", stale: r.c, open: openBySource.get(r.s ?? "No source") ?? 0 }))
      .filter((r) => r.open >= MIN_SEGMENT && pct(r.stale, r.open) > 50)
      .sort((a, b) => pct(b.stale, b.open) - pct(a.stale, a.open) || a.source.localeCompare(b.source));
    const rate = pct(stale.data, open.data);
    const evidence = [ctx.ev(stale.source, { count: stale.data, exclusions: ["Converted leads", `Leads created since ${health.staleBefore}`] }), ctx.ev(open.source, { count: open.data })];
    const segments = [{ dimension: "lead source", segments: openBySource.size }];
    if (rate <= LEAD_STALE_LIMIT && hotspots.length === 0) {
      return { outcome: "pass", summary: `Open leads with no activity since ${health.staleBefore}: ${stale.data} of ${open.data} (${rate}%), with no source above 50%.`, evidence, segmentsChecked: segments, facts: { stale: stale.data, open: open.data, rate } };
    }
    const worst = hotspots[0];
    return {
      outcome: "issue",
      title: "Leads are going unworked",
      summary: `Open leads older than ${STALE_ACTIVITY_DAYS} days with no logged activity: ${stale.data} of ${open.data} (${rate}%).${worst ? ` The worst source is ${worst.source}, at ${pct(worst.stale, worst.open)}%.` : ""}`,
      recommendation: "Agree a follow-up rule by source, and archive what's too old to work.",
      recommendationType: "change",
      severity: "medium",
      confidence: "high",
      evidence,
      value: atOwnLeadReturn(ctx, stale.data, { period: "one-off, if these leads are worked", caveat: "Assumes old, untouched leads return what an average lead does. They usually return less, so treat this as a ceiling" }),
      effort: { size: "S" },
      owner: "Head of marketing",
      segmentsChecked: segments,
      subjects: hotspots.slice(0, 5).map((h) => ({ type: "lead source", label: h.source })),
      facts: { stale: stale.data, open: open.data, rate },
      records: staleLeadRecords(ctx, stale.data),
    };
  },
};

const contactGaps: Rule = {
  id: "dq.contact-completeness",
  version: 1,
  domain: "data_quality",
  title: "Contacts with no account or email",
  evaluate(ctx): RuleResult | RuleResult[] {
    const { counts } = ctx.data(crmHealthExtractor);
    const contacts = counts.contacts!;
    const noAccount = counts.contactsNoAccount!;
    const noEmail = counts.contactsNoEmail!;
    for (const count of [contacts, noAccount, noEmail]) {
      if (count.status !== "ok") return { outcome: "not_measurable", reason: `Contacts couldn't be counted: ${count.reason}`, evidence: [ctx.ev(count.source, { count: null })] };
    }
    if (contacts.status !== "ok" || noAccount.status !== "ok" || noEmail.status !== "ok") throw new Error("unreachable");
    if (contacts.data === 0) return { outcome: "pass", summary: "There are no contacts to check.", evidence: [ctx.ev(contacts.source, { count: 0 })], segmentExemptReason: "There are no contacts to segment." };
    const accountRate = pct(noAccount.data, contacts.data);
    const emailRate = pct(noEmail.data, contacts.data);
    const evidence = [ctx.ev(noAccount.source, { count: noAccount.data }), ctx.ev(noEmail.source, { count: noEmail.data }), ctx.ev(contacts.source, { count: contacts.data })];
    const segments = [{ dimension: "gap type", segments: 2 }];
    if (accountRate <= CONTACT_GAP_LIMIT && emailRate <= CONTACT_GAP_LIMIT) {
      return { outcome: "pass", summary: `${accountRate}% of contacts have no account and ${emailRate}% have no email.`, evidence, segmentsChecked: segments, facts: { accountRate, emailRate } };
    }
    return {
      outcome: "issue",
      title: "Contact records are incomplete",
      summary: `Of ${plural(contacts.data, "contact")}, ${noAccount.data} (${accountRate}%) have no account and ${noEmail.data} (${emailRate}%) have no email address.`,
      recommendation: "Link orphaned contacts to accounts, and decide how missing emails get filled.",
      recommendationType: "change",
      severity: "low",
      confidence: "high",
      evidence,
      value: notSized("We can't say what a complete record would be worth."),
      effort: { size: "M" },
      owner: "Sales operations",
      segmentsChecked: segments,
      facts: { accountRate, emailRate },
      records: contactRecords(ctx, noAccount.data + noEmail.data, { noAccount: noAccount.data, noEmail: noEmail.data }),
    };
  },
};

// The lists behind the two counts, read with the counts' own filters
// (src/extract/action-records.ts). A list that stopped at its ceiling says it's
// a part, with how to see the rest in Salesforce.
function staleLeadRecords(ctx: RuleContext, total: number): RecordList | undefined {
  const { staleLeads } = ctx.data(actionRecordsExtractor);
  if (staleLeads.status !== "ok") return undefined;
  const { staleBefore } = ctx.data(crmHealthExtractor);
  return {
    object: "Lead",
    rows: staleLeads.data.map((l) => ({ id: l.Id, problem: `Created ${l.CreatedDate.slice(0, 10)}${l.LeadSource ? ` from ${l.LeadSource}` : ""}, with no activity logged` })),
    fix: "Work it, or archive it if it's too old to work",
    // A part only when the read stopped at its ceiling: the count was read earlier, and a lead worked since isn't missing.
    ...(staleLeads.truncated ? { part: { total, rest: `In Salesforce, a list view of open leads with no last activity, created before ${staleBefore}, shows them all.` } } : {}),
  };
}

/** One gap's part of a contact list: all of it, or the first rows read of how many there are. */
const gapLabel = (name: string, read: number, truncated: boolean | undefined, count: number): string =>
  truncated ? `${name}: the first ${read.toLocaleString("en-GB")} of ${count.toLocaleString("en-GB")}, in order of Salesforce id.` : `${name}: all ${read.toLocaleString("en-GB")}.`;

function contactRecords(ctx: RuleContext, total: number, counts: { noAccount: number; noEmail: number }): RecordList | undefined {
  const { contactsNoAccount, contactsNoEmail } = ctx.data(actionRecordsExtractor);
  if (contactsNoAccount.status !== "ok" || contactsNoEmail.status !== "ok") return undefined;
  // One line per gap: a contact with both appears twice, once for each fix.
  const rows = [
    ...contactsNoAccount.data.map((c) => ({ id: c.Id, problem: "No account" })),
    ...contactsNoEmail.data.map((c) => ({ id: c.Id, problem: "No email address" })),
  ];
  return {
    object: "Contact",
    rows,
    fix: "Link it to its account, or add the email address, as the line says",
    // Two lists, each in order of id and each with its own ceiling, so the part is said gap by gap.
    ...(contactsNoAccount.truncated || contactsNoEmail.truncated
      ? {
          part: {
            total,
            rest: "In Salesforce, list views of contacts with no account, and with no email, show them all.",
            label: [gapLabel("No account", contactsNoAccount.data.length, contactsNoAccount.truncated, counts.noAccount), gapLabel("No email address", contactsNoEmail.data.length, contactsNoEmail.truncated, counts.noEmail)].join(" "),
          },
        }
      : {}),
  };
}

export const dataQualityModule: DomainModule = {
  id: "data_quality",
  cloud: "sales",
  label: "Data quality",
  // The opportunities read plans its SELECT from the schema and winds deals back
  // with the stages (DECISIONS.md 465), so both go before it.
  extractors: [usersExtractor, recordCountsExtractor, ownershipExtractor, duplicateRulesExtractor, schemaExtractor, stagesExtractor, opportunitiesExtractor, crmHealthExtractor, leadsExtractor, actionRecordsExtractor, duplicateAccountsExtractor],
  rules: [pastCloseDate, inactiveOwner, missingAmount, noRecentActivity, noAccount, lossReasonMissing, inactiveRecordOwners, duplicateRulesMissing, duplicateAccounts, staleLeads, contactGaps],
};
