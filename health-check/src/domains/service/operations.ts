import { notSized, pct, plural, segmentRate, type Rule, type RuleContext, type RuleResult } from "../../engine/rule.ts";
import { orgExtractor } from "../../extract/configuration.ts";
import { serviceActivityExtractor, serviceCasesExtractor, serviceSetupExtractor, SERVICE_MONTHS, type CaseVolumeRow, type ClosedCaseRow, type OpenCaseRow, type ServiceCases } from "../../extract/service.ts";
import { monthsBefore, type Sourced } from "../../extract/types.ts";
import type { Evidence, Metric, MetricSegment, SegmentCheck } from "../../schemas/index.ts";
import type { DomainModule } from "../types.ts";
import { localDay, zoneOf } from "../time-zone.ts";
import { ageDays, caseQueues, HOUR, limitsOf, median, MIN_SEGMENT, originOf, ownerType, priorityOf, quantile, queueOwned, round1, yearRange, yearsCases } from "./shared.ts";

// How cases are handled (roadmap Stage 6): the backlog, how long cases take,
// whether promises are kept, and whether cases say where they came from and
// why. Every rule here reads case records, so a set-up review leaves them out.

const DOMAIN = "service_operations";
const OWNER = "Head of customer service";

/** Months compared for backlog growth: the last six full months. */
const GROWTH_MONTHS = 6;
const GROWTH_LIMIT = 4;
/** A queue should hand a case on within this many days. */
const QUEUE_DAYS = 2;
/** A first reply to an email case within a working day. */
const FIRST_REPLY_HOURS = 24;
const REOPEN_LIMIT = 10;
const MILESTONE_LIMIT = 10;

type Rows<R> = { rows: R[]; evidence: Evidence[]; truncated: boolean } | { reason: string; evidence: Evidence[] };

function rowsOf<R>(ctx: RuleContext, source: Sourced<R[]>, what: string, note?: string): Rows<R> {
  if (source.status !== "ok") return { reason: `${what} couldn't be read: ${source.reason}`, evidence: [ctx.ev(source.source, { count: null })] };
  return { rows: source.data, truncated: source.truncated, evidence: [ctx.ev(source.source, { count: source.data.length, dateRange: yearRange(ctx, SERVICE_MONTHS), ...(note ? { note } : {}), exclusions: source.truncated ? ["Rows past the read limit"] : [] })] };
}

const nm = (reason: string, evidence: Evidence[]): RuleResult => ({ outcome: "not_measurable", reason, evidence });
/** Why a check can't pass on a read that stopped at its row limit. */
const cutOff = (n: number, scope: string): string => `The read stopped at its row limit, so we checked only the first ${n.toLocaleString("en-GB")} ${scope} in order of Salesforce id, and can't say the rest are within the line.`;
const cases = (ctx: RuleContext): ServiceCases => ctx.data(serviceCasesExtractor);

/** A rate rule over cases, with the segments rule 4 asks for. */
function caseRate<R>(
  ctx: RuleContext,
  rows: R[],
  evidence: Evidence[],
  options: {
    hit: (r: R) => boolean;
    dimensions: Record<string, (r: R) => string>;
    totalLimit: number;
    segmentLimit: number;
    scope: string;
    sentence: (hits: number, n: number, rate: number) => string;
    pass: (hits: number, n: number, rate: number) => string;
    title: string;
    recommendation: string;
    severity: "high" | "medium" | "low";
    effort: "XS" | "S" | "M" | "L";
    truncated: boolean;
    facts?: Record<string, number>;
  },
): RuleResult {
  if (rows.length === 0) return { outcome: "pass", summary: `There are no ${options.scope} to check.`, evidence, segmentExemptReason: `There are no ${options.scope}.` };
  const result = segmentRate(rows, options.hit, options.dimensions, { segmentLimit: options.segmentLimit, minSegment: MIN_SEGMENT });
  const facts = { hits: result.hits, n: result.n, rate: result.rate, ...options.facts };
  const over = result.rate > options.totalLimit;
  if (!over && result.hotspots.length === 0) {
    // A read that stopped at its row limit is a part, and a part can't show the rest are within the line (rule 5).
    if (options.truncated) return nm(cutOff(result.n, options.scope), evidence);
    return { outcome: "pass", summary: `${options.pass(result.hits, result.n, result.rate)} No segment is above ${options.segmentLimit}%.`, evidence, segmentsChecked: result.segmentsChecked, facts: { ...facts, segmentLimit: options.segmentLimit } };
  }
  const worst = result.hotspots[0];
  const hidden = !over ? ` The total looks acceptable, but by ${worst!.dimension}, ${worst!.key} is at ${worst!.rate}% (${worst!.hits} of ${worst!.n}).` : worst ? ` Worst by ${worst.dimension}: ${worst.key}, at ${worst.rate}%.` : "";
  return {
    outcome: "issue",
    title: options.title,
    summary: `${options.sentence(result.hits, result.n, result.rate)}${hidden}${options.truncated ? " The read stopped at its row limit, so this is from the cases read." : ""}`,
    recommendation: options.recommendation,
    recommendationType: "change",
    severity: options.severity,
    confidence: options.truncated ? "medium" : "high",
    evidence,
    value: notSized("How cases are handled has no honest figure in money: it shows in customers' experience, not on an invoice."),
    effort: { size: options.effort },
    owner: OWNER,
    segmentsChecked: result.segmentsChecked,
    subjects: result.hotspots.slice(0, 5).map((h) => ({ type: h.dimension, label: h.key })),
    facts: { ...facts, ...(worst ? { worstSegmentRate: worst.rate, worstSegmentHits: worst.hits, worstSegmentN: worst.n } : {}) },
  };
}

/** The last full months, oldest first, as "YYYY-MM" in the org's zone. */
function lastMonths(ctx: RuleContext, count: number): string[] {
  const today = localDay(ctx.now, zoneOf(ctx));
  const [y, m] = today.split("-").map(Number) as [number, number];
  return Array.from({ length: count }, (_, i) => {
    const d = new Date(Date.UTC(y, m - 2 - i, 1));
    return d.toISOString().slice(0, 7);
  }).reverse();
}
const monthKey = (r: CaseVolumeRow): string => `${r.y}-${String(r.m).padStart(2, "0")}`;

const backlogGrowth: Rule = {
  id: "svc.backlog-growth",
  version: 1,
  domain: DOMAIN,
  title: "More cases coming in than closing",
  records: true,
  evaluate(ctx) {
    const { created, closed } = cases(ctx);
    if (created.status !== "ok") return nm(`Case volumes couldn't be read: ${created.reason}`, [ctx.ev(created.source, { count: null })]);
    if (closed.status !== "ok") return nm(`Closed case volumes couldn't be read: ${closed.reason}`, [ctx.ev(closed.source, { count: null })]);
    const months = lastMonths(ctx, GROWTH_MONTHS);
    const range = { from: `${months[0]}-01`, to: localDay(ctx.now, zoneOf(ctx)) };
    const sum = (rows: CaseVolumeRow[], month: string, priority?: string) => rows.filter((r) => monthKey(r) === month && (priority === undefined || priorityOf(r) === priority)).reduce((s, r) => s + r.n, 0);
    const inWindow = (rows: CaseVolumeRow[]) => rows.filter((r) => months.includes(monthKey(r)));
    const opened = inWindow(created.data).reduce((s, r) => s + r.n, 0);
    const shut = inWindow(closed.data).reduce((s, r) => s + r.n, 0);
    const evidence = [ctx.ev(created.source, { count: opened, dateRange: range, note: "Counts by month in the org's time zone" }), ctx.ev(closed.source, { count: shut, dateRange: range })];
    if (opened < ctx.calibration.minSample) return nm(`Only ${plural(opened, "case")} came in over the last ${GROWTH_MONTHS} full months, fewer than the ${ctx.calibration.minSample} we need.`, evidence);
    const growing = months.filter((m) => sum(created.data, m) > sum(closed.data, m)).length;
    const priorities = [...new Set([...created.data, ...closed.data].map(priorityOf))].sort();
    const segmentsChecked: SegmentCheck[] = [{ dimension: "month", segments: months.length }, { dimension: "priority", segments: priorities.length }];
    const byPriority = priorities.map((p) => ({ p, growth: months.reduce((s, m) => s + sum(created.data, m, p) - sum(closed.data, m, p), 0) })).sort((a, b) => b.growth - a.growth);
    const facts = { months: GROWTH_MONTHS, growingMonths: growing, opened, closed: shut, growth: opened - shut, limit: GROWTH_LIMIT };
    if (growing < GROWTH_LIMIT || opened <= shut) {
      return { outcome: "pass", summary: `Over the last ${GROWTH_MONTHS} full months, ${opened} cases came in and ${shut} were closed. More came in than closed in ${growing} of the ${GROWTH_MONTHS} months.`, evidence, segmentsChecked, facts };
    }
    const top = byPriority[0]!;
    return {
      outcome: "issue",
      title: "Your case backlog is growing",
      summary: `More cases came in than were closed in ${growing} of the last ${GROWTH_MONTHS} full months: ${opened} opened and ${shut} closed, so the backlog grew by ${opened - shut}. Most of the growth is at ${top.p} priority (${top.growth}).`,
      recommendation: "Find where the extra cases sit by priority and team, then add capacity there or stop the cause, such as a product fault or a missing help article, before the backlog ages.",
      recommendationType: "change",
      severity: "medium",
      confidence: "high",
      evidence,
      value: notSized("A growing backlog costs customers' patience, which has no honest figure in money."),
      effort: { size: "M" },
      owner: OWNER,
      segmentsChecked,
      subjects: [{ type: "priority", label: top.p }],
      facts: { ...facts, topPriorityGrowth: top.growth },
    };
  },
};

const backlogAge: Rule = {
  id: "svc.backlog-age",
  version: 1,
  domain: DOMAIN,
  title: "Open cases older than the agreed age",
  records: true,
  evaluate(ctx) {
    const read = rowsOf<OpenCaseRow>(ctx, cases(ctx).open, "Open cases", "Every open case");
    if ("reason" in read) return nm(read.reason, read.evidence);
    const days = limitsOf(ctx.calibration).backlogAgeDays;
    return caseRate(ctx, read.rows, read.evidence, {
      hit: (c) => ageDays(c.CreatedDate, ctx.now) > days,
      dimensions: { priority: priorityOf, origin: originOf, "owner type": ownerType },
      totalLimit: 25,
      segmentLimit: 50,
      scope: "open cases",
      sentence: (hits, n, rate) => `Open cases older than ${days} days: ${hits} of ${n} (${rate}%).`,
      pass: (hits, n, rate) => `Open cases older than ${days} days: ${hits} of ${n} (${rate}%).`,
      title: "Too many open cases are old",
      recommendation: "Review the oldest cases each week, close what's resolved or abandoned, and escalate the rest. Ask why they stalled: a missing answer, a waiting customer or a queue nobody watches.",
      severity: "medium",
      effort: "S",
      truncated: read.truncated,
      facts: { days },
    });
  },
};

const HIGH = /^(high|urgent|critical|p1)$/i;

const priorityResolution: Rule = {
  id: "svc.priority-resolution",
  version: 1,
  domain: DOMAIN,
  title: "High-priority cases resolved faster than the rest",
  records: true,
  evaluate(ctx) {
    const read = rowsOf<ClosedCaseRow>(ctx, cases(ctx).closedCases, "Closed cases", "Cases closed in the last year");
    if ("reason" in read) return nm(read.reason, read.evidence);
    const hours = (c: ClosedCaseRow): number => (Date.parse(c.ClosedDate) - Date.parse(c.CreatedDate)) / HOUR;
    const high = read.rows.filter((c) => HIGH.test(c.Priority ?? ""));
    const rest = read.rows.filter((c) => !HIGH.test(c.Priority ?? ""));
    if (high.length < MIN_SEGMENT || rest.length < MIN_SEGMENT) {
      return nm(`Too few cases to compare: ${plural(high.length, "high-priority case")} and ${rest.length} others closed in the last year, and each side needs ${MIN_SEGMENT}.`, read.evidence);
    }
    const highMedian = round1(median(high.map(hours)));
    const restMedian = round1(median(rest.map(hours)));
    const origins = new Set(read.rows.map(originOf)).size;
    const segmentsChecked: SegmentCheck[] = [{ dimension: "priority", segments: new Set(read.rows.map(priorityOf)).size }, { dimension: "origin", segments: origins }];
    const facts = { highMedianHours: highMedian, otherMedianHours: restMedian, high: high.length, other: rest.length };
    // A median from part of the cases, in id order, isn't the cases' median.
    if (read.truncated) return nm(cutOff(read.rows.length, "closed cases"), read.evidence);
    if (highMedian < restMedian) {
      return { outcome: "pass", summary: `High-priority cases took a median ${highMedian} hours to close, against ${restMedian} hours for the rest.`, evidence: read.evidence, segmentsChecked, facts };
    }
    return {
      outcome: "issue",
      title: "High-priority cases aren't resolved any faster",
      summary: `High-priority cases took a median ${highMedian} hours to close, and the rest ${restMedian} hours, so priority isn't changing which cases get worked first. From ${high.length} high-priority and ${rest.length} other cases closed in the last year.`,
      recommendation: "Make priority drive the work: route high-priority cases to their own queue or a higher routing priority, and give them a resolution target the team can see.",
      recommendationType: "change",
      severity: "medium",
      confidence: read.truncated ? "medium" : "high",
      evidence: read.evidence,
      value: notSized("Slow handling of urgent cases costs customers' trust, which has no honest figure in money."),
      effort: { size: "M" },
      owner: OWNER,
      segmentsChecked,
      facts,
    };
  },
};

const milestoneBreaches: Rule = {
  id: "svc.milestone-breaches",
  version: 1,
  domain: DOMAIN,
  title: "Entitlement milestones met",
  records: true,
  evaluate(ctx) {
    const { milestones } = ctx.data(serviceActivityExtractor);
    const read = rowsOf(ctx, milestones, "Case milestones", "Milestones started in the last year");
    if ("reason" in read) return nm(`${read.reason} Entitlement management may not be turned on.`, read.evidence);
    if (read.rows.length === 0) return nm("No case milestone started in the last year, so response and resolution targets aren't set up as entitlements.", read.evidence);
    const setup = ctx.data(serviceSetupExtractor);
    const names = new Map(setup.milestoneTypes.status === "ok" ? setup.milestoneTypes.data.map((t) => [t.Id, t.Name]) : []);
    const limits = limitsOf(ctx.calibration);
    const typeOf = (m: (typeof read.rows)[number]): string => {
      const name = names.get(m.MilestoneTypeId ?? "") ?? "Unknown milestone";
      return limits.firstResponse.includes(name) ? `${name} (first response)` : limits.resolution.includes(name) ? `${name} (resolution)` : name;
    };
    // Only milestones whose outcome is known: met, or past their target.
    const decided = read.rows.filter((m) => m.IsCompleted || Date.parse(m.TargetDate) < ctx.now.getTime());
    return caseRate(ctx, decided, read.evidence, {
      hit: (m) => m.IsViolated,
      dimensions: { "milestone type": typeOf },
      totalLimit: MILESTONE_LIMIT,
      segmentLimit: 25,
      scope: "milestones with a known outcome",
      sentence: (hits, n, rate) => `Milestones missed: ${hits} of ${n} (${rate}%) of those met or past their target in the last year.`,
      pass: (hits, n, rate) => `Milestones missed: ${hits} of ${n} (${rate}%).`,
      title: "Response and resolution targets are being missed",
      recommendation: "Look at the milestone types missed most, and whether the targets, the business hours behind them or the staffing are wrong. Add warning actions so agents see a breach coming.",
      severity: "high",
      effort: "M",
      truncated: read.truncated,
      facts: { limit: MILESTONE_LIMIT },
    });
  },
};

const firstReply: Rule = {
  id: "svc.first-reply-email",
  version: 1,
  domain: DOMAIN,
  title: "Time to first reply on email cases",
  records: true,
  evaluate(ctx) {
    const data = cases(ctx);
    const { replies } = ctx.data(serviceActivityExtractor);
    if (replies.status !== "ok") return nm(`Replies on cases couldn't be read: ${replies.reason}. Email-to-Case or Enhanced Email may not be on.`, [ctx.ev(replies.source, { count: null })]);
    if (data.open.status !== "ok" || data.closedCases.status !== "ok") {
      const bad = data.open.status !== "ok" ? data.open : data.closedCases;
      return nm(`Cases couldn't be read: ${bad.status === "ok" ? "" : bad.reason}`, [ctx.ev(bad.source, { count: null })]);
    }
    const since = monthsBefore(ctx.now, SERVICE_MONTHS);
    const emailCases = yearsCases(data.open.data, data.closedCases.data, since).filter((c) => /^e-?mail$/i.test(c.Origin ?? ""));
    const first = new Map<string, number>();
    for (const r of replies.data) {
      if (!r.MessageDate) continue;
      const t = Date.parse(r.MessageDate);
      if (!first.has(r.ParentId) || t < first.get(r.ParentId)!) first.set(r.ParentId, t);
    }
    const evidence = [ctx.ev(replies.source, { count: replies.data.length, dateRange: yearRange(ctx, SERVICE_MONTHS), note: "The first email sent on each case" }), ctx.ev(data.open.source, { count: emailCases.length, note: "Cases from email, open or closed, created in the last year" })];
    if (emailCases.length < ctx.calibration.minSample) return nm(`Only ${plural(emailCases.length, "case")} came in by email in the last year, fewer than the ${ctx.calibration.minSample} we need.`, evidence);
    const answered = emailCases.filter((c) => first.has(c.Id) && first.get(c.Id)! >= Date.parse(c.CreatedDate));
    if (answered.length === 0) return nm("No email case has a reply sent from Salesforce, so replies may be sent from somewhere else.", evidence);
    // A case whose reply wasn't read would count as unanswered, and a median from part of the cases isn't theirs.
    const cut = [replies, data.open, data.closedCases].find((r) => r.truncated);
    if (cut) return nm(`The read of ${cut === replies ? "replies" : "cases"} stopped at its row limit, so we can't tell how quickly every email case got its first reply.`, evidence);
    const hoursTo = (c: (typeof answered)[number]): number => (first.get(c.Id)! - Date.parse(c.CreatedDate)) / HOUR;
    const med = round1(median(answered.map(hoursTo)));
    const p75 = round1(quantile(answered.map(hoursTo), 0.75));
    const unanswered = emailCases.length - answered.length;
    const result = segmentRate(answered, (c) => hoursTo(c) > FIRST_REPLY_HOURS, { priority: priorityOf, "owner type": ownerType }, { segmentLimit: 50, minSegment: MIN_SEGMENT });
    const facts = { medianHours: med, p75Hours: p75, answered: answered.length, unanswered, cases: emailCases.length, targetHours: FIRST_REPLY_HOURS, late: result.hits, lateRate: result.rate };
    if (med <= FIRST_REPLY_HOURS && result.hotspots.length === 0) {
      return { outcome: "pass", summary: `Email cases got a first reply in a median ${med} hours, within ${FIRST_REPLY_HOURS} hours. ${unanswered} of ${emailCases.length} have no reply from Salesforce yet.`, evidence, segmentsChecked: result.segmentsChecked, facts };
    }
    const worst = result.hotspots[0];
    return {
      outcome: "issue",
      title: "Customers who email wait too long for a first reply",
      summary: `Email cases waited a median ${med} hours for a first reply, and a quarter waited more than ${p75} hours. ${result.hits} of ${answered.length} replied cases (${result.rate}%) took longer than ${FIRST_REPLY_HOURS} hours.${worst ? ` Worst by ${worst.dimension}: ${worst.key}, at ${worst.rate}%.` : ""}`,
      recommendation: "Send an automatic acknowledgement with a realistic reply time, route email cases into a watched queue, and set a first-response milestone so the wait is visible.",
      recommendationType: "change",
      severity: "medium",
      confidence: "medium",
      evidence,
      value: notSized("A slow first reply costs customers' patience, which has no honest figure in money."),
      effort: { size: "S" },
      owner: OWNER,
      segmentsChecked: result.segmentsChecked,
      facts,
    };
  },
};

const reopened: Rule = {
  id: "svc.reopened-cases",
  version: 1,
  domain: DOMAIN,
  title: "Closed cases that are reopened",
  records: true,
  evaluate(ctx) {
    const data = cases(ctx);
    const { statusChanges } = ctx.data(serviceActivityExtractor);
    const { statuses } = ctx.data(serviceSetupExtractor);
    if (statuses.status !== "ok") return nm(`Case statuses couldn't be read: ${statuses.reason}`, [ctx.ev(statuses.source, { count: null })]);
    const history = rowsOf(ctx, statusChanges, "Case history", "Status changes in the last year");
    if ("reason" in history) return nm(history.reason, history.evidence);
    const closedRead = rowsOf<ClosedCaseRow>(ctx, data.closedCases, "Closed cases");
    if ("reason" in closedRead) return nm(closedRead.reason, closedRead.evidence);
    const evidence = [...history.evidence, ...closedRead.evidence];
    if (history.rows.length === 0 && closedRead.rows.length > 0) {
      return nm("Case history holds no status change in the last year, though cases were closed, so status changes may not be tracked.", evidence);
    }
    const closedNames = new Set(statuses.data.filter((s) => s.IsClosed).flatMap((s) => [s.ApiName, s.MasterLabel ?? ""]).filter(Boolean).map((s) => s.toLowerCase()));
    const isClosed = (v: string | null): boolean => v !== null && closedNames.has(v.toLowerCase());
    const reopenedIds = new Set(history.rows.filter((h) => isClosed(h.OldValue) && !isClosed(h.NewValue)).map((h) => h.CaseId));
    return caseRate(ctx, closedRead.rows, evidence, {
      hit: (c) => reopenedIds.has(c.Id),
      dimensions: { priority: priorityOf, origin: originOf },
      totalLimit: REOPEN_LIMIT,
      segmentLimit: 25,
      scope: "cases closed in the last year",
      sentence: (hits, n, rate) => `Cases closed in the last year and then reopened: ${hits} of ${n} (${rate}%). Each one went back to a customer as solved when it wasn't.`,
      pass: (hits, n, rate) => `Cases closed in the last year and then reopened: ${hits} of ${n} (${rate}%).`,
      title: "Cases are being closed before they're solved",
      recommendation: "Ask the customer to confirm before closing, look at which case types reopen most, and fix the cause there: a missing answer, a fix that didn't hold, or a target that rewards closing fast.",
      severity: "medium",
      effort: "S",
      truncated: history.truncated || closedRead.truncated,
      facts: { limit: REOPEN_LIMIT },
    });
  },
};

const unassigned: Rule = {
  id: "svc.unassigned-cases",
  version: 1,
  domain: DOMAIN,
  title: "Open cases waiting in a queue",
  records: true,
  evaluate(ctx) {
    const read = rowsOf<OpenCaseRow>(ctx, cases(ctx).open, "Open cases", "Every open case");
    if ("reason" in read) return nm(read.reason, read.evidence);
    const queues = caseQueues(ctx.data(serviceSetupExtractor)) ?? [];
    const name = new Map(queues.map((q) => [q.Id, q.Name]));
    return caseRate(ctx, read.rows, read.evidence, {
      hit: (c) => queueOwned(c.OwnerId) && ageDays(c.CreatedDate, ctx.now) > QUEUE_DAYS,
      dimensions: { queue: (c) => (queueOwned(c.OwnerId) ? (name.get(c.OwnerId) ?? "A queue") : "Owned by a person"), priority: priorityOf },
      totalLimit: 10,
      segmentLimit: 25,
      scope: "open cases",
      sentence: (hits, n, rate) => `Open cases still owned by a queue more than ${QUEUE_DAYS} days after they came in: ${hits} of ${n} (${rate}%). Nobody has taken them on.`,
      pass: (hits, n, rate) => `Open cases still in a queue after ${QUEUE_DAYS} days: ${hits} of ${n} (${rate}%).`,
      title: "Cases wait in queues with nobody working them",
      recommendation: "Make sure every case queue has members who watch it, and route work from busy queues automatically. Age is from when the case came in, so check the oldest first.",
      severity: "high",
      effort: "S",
      truncated: read.truncated,
      facts: { days: QUEUE_DAYS },
    });
  },
};

const originMissing: Rule = {
  id: "svc.origin-missing",
  version: 1,
  domain: DOMAIN,
  title: "Cases that say where they came from",
  records: true,
  evaluate(ctx) {
    const data = cases(ctx);
    const open = rowsOf<OpenCaseRow>(ctx, data.open, "Open cases");
    if ("reason" in open) return nm(open.reason, open.evidence);
    const closed = rowsOf<ClosedCaseRow>(ctx, data.closedCases, "Closed cases");
    if ("reason" in closed) return nm(closed.reason, closed.evidence);
    const share = limitsOf(ctx.calibration).originBlankShare;
    const rows = yearsCases(open.rows, closed.rows, monthsBefore(ctx.now, SERVICE_MONTHS));
    return caseRate(ctx, rows, [...open.evidence, ...closed.evidence], {
      hit: (c) => !c.Origin,
      dimensions: { priority: priorityOf, "owner type": ownerType },
      totalLimit: share,
      segmentLimit: Math.min(100, share * 2),
      scope: "cases created in the last year",
      sentence: (hits, n, rate) => `Cases with no origin: ${hits} of ${n} (${rate}%) created in the last year. Without it, you can't tell which channels bring the work.`,
      pass: (hits, n, rate) => `Cases with no origin: ${hits} of ${n} (${rate}%), within the ${share}% agreed.`,
      title: "Cases don't say where they came from",
      recommendation: "Set Origin automatically for every channel (Email-to-Case, web forms, messaging and the phone), and make it required when an agent creates a case by hand.",
      severity: "low",
      effort: "S",
      truncated: open.truncated || closed.truncated,
      facts: { limit: share },
    });
  },
};

const classification: Rule = {
  id: "svc.case-classification",
  version: 1,
  domain: DOMAIN,
  title: "Cases record why the customer got in touch",
  records: true,
  evaluate(ctx) {
    const data = cases(ctx);
    const closed = rowsOf<ClosedCaseRow>(ctx, data.closedCases, "Closed cases", "Cases closed in the last year");
    if ("reason" in closed) return nm(closed.reason, closed.evidence);
    const missing = data.closedCases.status === "ok" ? (data.closedCases.missingFields ?? []) : [];
    const hasType = !missing.includes("Case.Type");
    const hasReason = !missing.includes("Case.Reason");
    if (!hasType && !hasReason) return nm("Case.Type and Case.Reason aren't in this org, so there's nothing to say why a case came in.", closed.evidence);
    return caseRate(ctx, closed.rows, closed.evidence, {
      hit: (c) => (!hasType || !c.Type) && (!hasReason || !c.Reason),
      dimensions: { priority: priorityOf, origin: originOf },
      totalLimit: 25,
      segmentLimit: 50,
      scope: "cases closed in the last year",
      sentence: (hits, n, rate) => `Closed cases with no type or reason: ${hits} of ${n} (${rate}%). You can't report on why customers get in touch, or fix the causes that come up most.`,
      pass: (hits, n, rate) => `Closed cases with no type or reason: ${hits} of ${n} (${rate}%).`,
      title: "Cases don't record why the customer got in touch",
      recommendation: "Make Type or Reason required on closing, keep its values short and agreed, and report on them monthly.",
      severity: "low",
      effort: "S",
      truncated: closed.truncated,
    });
  },
};

const customerLinked: Rule = {
  id: "svc.case-customer",
  version: 1,
  domain: DOMAIN,
  title: "Open cases are linked to a customer",
  records: true,
  evaluate(ctx) {
    const open = rowsOf<OpenCaseRow>(ctx, cases(ctx).open, "Open cases", "Every open case");
    if ("reason" in open) return nm(open.reason, open.evidence);
    return caseRate(ctx, open.rows, open.evidence, {
      hit: (c) => !c.ContactId && !c.AccountId,
      dimensions: { origin: originOf, "owner type": ownerType },
      totalLimit: 10,
      segmentLimit: 25,
      scope: "open cases",
      sentence: (hits, n, rate) => `Open cases with neither a contact nor an account: ${hits} of ${n} (${rate}%). Nobody can see the customer's history, or reply from the case.`,
      pass: (hits, n, rate) => `Open cases with neither a contact nor an account: ${hits} of ${n} (${rate}%).`,
      title: "Open cases aren't linked to a customer",
      recommendation: "Match incoming email and web cases to contacts automatically, and ask agents to link the customer before working a case.",
      severity: "low",
      effort: "S",
      truncated: open.truncated,
    });
  },
};

// The figures behind the chapter, for the raw notes and the deck.
function metrics(ctx: Omit<RuleContext, "metrics">): Metric[] {
  const data = ctx.data(serviceCasesExtractor);
  const activity = ctx.data(serviceActivityExtractor);
  const out: Metric[] = [];
  const base = (id: string, label: string, unit: Metric["unit"], definition: string) => ({ id, domain: DOMAIN, label, unit, definition, exclusions: [] as string[], assumptions: [] as string[] });
  const segmentsBy = <R>(rows: R[], keyOf: (r: R) => string, value: (group: R[]) => number, dimension: string): MetricSegment[] =>
    [...new Set(rows.map(keyOf))].sort().map((key) => {
      const group = rows.filter((r) => keyOf(r) === key);
      return { dimension, key, n: group.length, value: group.length >= MIN_SEGMENT ? value(group) : null, notMeasurableReason: group.length >= MIN_SEGMENT ? null : `Fewer than ${MIN_SEGMENT} cases` };
    });

  const openDef = base("service.open_cases", "Open cases", "count", "Cases not closed today, with the median age in days.");
  if (data.open.status === "ok") {
    const ages = data.open.data.map((c) => ageDays(c.CreatedDate, ctx.now));
    out.push({ ...openDef, status: "measured", value: data.open.data.length, n: data.open.data.length, dateRange: null, segments: segmentsBy(data.open.data, priorityOf, (g) => g.length, "priority"), segmentExemptReason: null, series: [], stats: { medianAgeDays: ages.length ? round1(median(ages)) : 0, queueOwned: data.open.data.filter((c) => queueOwned(c.OwnerId)).length }, evidence: [ctx.ev(data.open.source, { count: data.open.data.length })], exclusions: data.open.truncated ? ["Rows past the read limit"] : [] });
  } else out.push({ ...openDef, status: "not_measurable", reason: `Open cases couldn't be read: ${data.open.reason}`, evidence: [ctx.ev(data.open.source, { count: null })] });

  const resolutionDef = base("service.resolution_days", "Time to resolve a case", "days", "Days from a case being created to being closed, for cases closed in the last year: the median, with the upper quartile. Calendar time, not business hours.");
  if (data.closedCases.status === "ok" && data.closedCases.data.length > 0) {
    const days = (c: ClosedCaseRow): number => (Date.parse(c.ClosedDate) - Date.parse(c.CreatedDate)) / (24 * HOUR);
    const rows = data.closedCases.data;
    out.push({ ...resolutionDef, status: "measured", value: round1(median(rows.map(days))), n: rows.length, dateRange: yearRange(ctx, SERVICE_MONTHS), segments: segmentsBy(rows, priorityOf, (g) => round1(median(g.map(days))), "priority"), segmentExemptReason: null, series: [], stats: { p75: round1(quantile(rows.map(days), 0.75)) }, evidence: [ctx.ev(data.closedCases.source, { count: rows.length })] });
  } else out.push({ ...resolutionDef, status: "not_measurable", reason: data.closedCases.status === "ok" ? "No case was closed in the last year." : `Closed cases couldn't be read: ${data.closedCases.reason}`, evidence: [ctx.ev(data.closedCases.source, { count: null })] });

  const createdDef = base("service.cases_created", "Cases created a month", "count", "Cases created in each of the last twelve months, by month in the org's time zone. The value is the year's total.");
  if (data.created.status === "ok") {
    const months = new Map<string, number>();
    for (const r of data.created.data) months.set(monthKey(r), (months.get(monthKey(r)) ?? 0) + r.n);
    const total = [...months.values()].reduce((s, n) => s + n, 0);
    const priorities = new Map<string, number>();
    for (const r of data.created.data) priorities.set(priorityOf(r), (priorities.get(priorityOf(r)) ?? 0) + r.n);
    out.push({
      ...createdDef,
      status: "measured",
      value: total,
      n: total,
      dateRange: yearRange(ctx, SERVICE_MONTHS),
      segments: [...priorities].sort().map(([key, n]) => ({ dimension: "priority", key, n, value: n, notMeasurableReason: null })),
      segmentExemptReason: total ? null : "No case was created in the last year.",
      series: [...months].sort().map(([month, n]) => ({ date: `${month}-01`, value: n, n, notMeasurableReason: null })),
      stats: {},
      evidence: [ctx.ev(data.created.source, { count: total })],
    });
  } else out.push({ ...createdDef, status: "not_measurable", reason: `Case volumes couldn't be read: ${data.created.reason}`, evidence: [ctx.ev(data.created.source, { count: null })] });

  const originDef = base("service.cases_by_origin", "Cases by channel", "percent", "The share of cases created in the last year by Origin, and how many contacts each channel's own records hold. The value is the share with an Origin recorded.");
  if (data.origins.status === "ok") {
    const total = data.origins.data.reduce((s, r) => s + r.n, 0);
    const known = data.origins.data.filter((r) => r.Origin).reduce((s, r) => s + r.n, 0);
    const channels = activity.channels.filter((c) => c.count.status === "ok");
    out.push({
      ...originDef,
      status: "measured",
      value: pct(known, total),
      n: total,
      dateRange: yearRange(ctx, SERVICE_MONTHS),
      segments: [...data.origins.data].sort((a, b) => b.n - a.n).map((r) => ({ dimension: "origin", key: r.Origin ?? "No origin", n: r.n, value: pct(r.n, total), notMeasurableReason: null })),
      segmentExemptReason: total ? null : "No case was created in the last year.",
      series: [],
      stats: Object.fromEntries(channels.map((c) => [`records${c.channel}`, c.count.status === "ok" ? c.count.data : 0])),
      evidence: [ctx.ev(data.origins.source, { count: total }), ...channels.map((c) => ctx.ev(c.count.source, { count: c.count.status === "ok" ? c.count.data : null }))],
      assumptions: activity.channels.filter((c) => c.count.status !== "ok").map((c) => `${c.channel} records weren't readable, so that channel isn't counted`),
    });
  } else out.push({ ...originDef, status: "not_measurable", reason: `Case origins couldn't be read: ${data.origins.reason}`, evidence: [ctx.ev(data.origins.source, { count: null })] });

  const workDef = base("service.routing_declined", "Routed work declined", "percent", "The share of work Omni-Channel offered to agents in the last year that they declined, with the median seconds to answer.");
  if (activity.agentWork.status === "ok" && activity.agentWork.data.length > 0) {
    const rows = activity.agentWork.data;
    const declined = rows.filter((w) => w.DeclineDateTime !== null).length;
    const speeds = rows.flatMap((w) => (w.SpeedToAnswer === null ? [] : [w.SpeedToAnswer]));
    out.push({ ...workDef, status: "measured", value: pct(declined, rows.length), n: rows.length, dateRange: yearRange(ctx, SERVICE_MONTHS), segments: [], segmentExemptReason: "Who the work went to isn't shown, so no agent is singled out", series: [], stats: { declined, ...(speeds.length ? { medianSpeedToAnswerSeconds: round1(median(speeds)) } : {}) }, evidence: [ctx.ev(activity.agentWork.source, { count: rows.length })] });
  } else out.push({ ...workDef, status: "not_measurable", reason: activity.agentWork.status === "ok" ? "Omni-Channel offered no work in the last year." : `Routed work couldn't be read: ${activity.agentWork.reason}. Omni-Channel may not be on.`, evidence: [ctx.ev(activity.agentWork.source, { count: null })] });
  return out;
}

export const serviceOperationsModule: DomainModule = {
  id: DOMAIN,
  cloud: "service",
  label: "Service operations",
  extractors: [orgExtractor, serviceSetupExtractor, serviceCasesExtractor, serviceActivityExtractor],
  metrics: [metrics],
  rules: [backlogGrowth, backlogAge, priorityResolution, milestoneBreaches, firstReply, reopened, unassigned, originMissing, classification, customerLinked],
};

export const SERVICE_OPERATIONS_METRIC_IDS = ["service.open_cases", "service.resolution_days", "service.cases_created", "service.cases_by_origin", "service.routing_declined"];
