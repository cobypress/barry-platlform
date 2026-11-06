import { notSized, pct, plural, segmentRate, type Rule, type RuleContext, type RuleResult } from "../../engine/rule.ts";
import { orgExtractor } from "../../extract/configuration.ts";
import { escalationRulesExtractor, serviceActivityExtractor, serviceCasesExtractor, serviceSetupExtractor, type ArticleRow, type EscalationRule, type ServiceSetup } from "../../extract/service.ts";
import { daysBefore, monthsBefore } from "../../extract/types.ts";
import { loginsExtractor, usersExtractor } from "../../extract/users.ts";
import { costPerSeat, isPriced } from "../../money/seat-cost.ts";
import type { Evidence, SegmentCheck, Value } from "../../schemas/index.ts";
import { lapse } from "../logins.ts";
import type { DomainModule } from "../types.ts";
import { caseQueues, limitsOf, MIN_SEGMENT, originOf, priorityOf } from "./shared.ts";

// How the org is set up to handle cases: escalation, queues, routing,
// licences, knowledge, macros. Most of it reads no case, so a set-up review
// runs it; the knowledge use check compares articles with closed cases, so it
// doesn't.

const DOMAIN = "service_setup";
const OWNER = "Salesforce admin";
/** An agent in more case queues than this is spread too thin to watch them. */
const QUEUES_PER_AGENT = 5;
/** Knowledge is worth checking for use once it has this many articles. */
const MIN_ARTICLES = 20;
const ATTACH_SHARE = 10;
const STALE_SHARE = 50;
const UNUSED_SHARE = 50;
const MIN_MACROS = 5;
/** The Service Cloud user feature licence, as a calibration's licenceCosts names it. */
export const SERVICE_SEAT = "Service Cloud User";

const isAre = (n: number): string => (n === 1 ? "is" : "are");
const hasHave = (n: number): string => (n === 1 ? "has" : "have");
/** "a", "a and b", "a, b and c". */
const listed = (parts: string[]): string => (parts.length <= 1 ? (parts[0] ?? "") : `${parts.slice(0, -1).join(", ")} and ${parts.at(-1)}`);

const nm = (reason: string, evidence: Evidence[], key?: string): RuleResult => ({ outcome: "not_measurable", reason, evidence, ...(key ? { key } : {}) });
const setupOf = (ctx: RuleContext): ServiceSetup => ctx.data(serviceSetupExtractor);
const unavailableEvidence = (ctx: RuleContext, source: Parameters<RuleContext["ev"]>[0]): Evidence[] => [ctx.ev(source, { count: null })];

/** Active users' usernames, and queue developer names with members, to check where escalations go. */
function targets(ctx: RuleContext, setup: ServiceSetup): { activeUsers: Set<string> | null; staffedQueues: Set<string> | null } {
  const users = ctx.data(usersExtractor);
  const activeUsers = users.status === "ok" ? new Set(users.data.filter((u) => u.IsActive).map((u) => u.Username.toLowerCase())) : null;
  if (setup.queues.status !== "ok" || setup.members.status !== "ok") return { activeUsers, staffedQueues: null };
  const staffed = new Set(setup.members.data.map((m) => m.GroupId));
  return { activeUsers, staffedQueues: new Set(setup.queues.data.filter((q) => staffed.has(q.Id)).flatMap((q) => [q.DeveloperName.toLowerCase(), q.Name.toLowerCase()])) };
}

const escalation: Rule = {
  id: "svc.escalation-rules",
  version: 1,
  domain: DOMAIN,
  title: "Cases escalate when they stall",
  evaluate(ctx) {
    const rules = ctx.data(escalationRulesExtractor);
    const sla = setupOf(ctx).slaProcesses;
    const activeSla = sla.status === "ok" ? sla.data.filter((p) => p.IsActive && (p.SObjectType ?? "Case") === "Case") : [];
    const evidence = [ctx.ev(rules.source, { count: rules.status === "ok" ? rules.data.length : null }), ctx.ev(sla.source, { count: sla.status === "ok" ? sla.data.length : null })];
    const single = "The org has one escalation set-up, not groups to compare";
    if (rules.status !== "ok") {
      // Entitlement processes escalate too, so with one active the gap doesn't matter.
      if (activeSla.length) return { outcome: "pass", summary: `Escalation rules couldn't be read (${rules.reason}), but ${plural(activeSla.length, "entitlement process")} for cases ${isAre(activeSla.length)} active, which can escalate cases.`, evidence, segmentExemptReason: single, facts: { processes: activeSla.length } };
      return nm(`Escalation rules couldn't be read: ${rules.reason}`, evidence);
    }
    const active = rules.data.filter((r) => r.active);
    if (active.length || activeSla.length) return { outcome: "pass", summary: `${plural(active.length, "escalation rule")} and ${plural(activeSla.length, "entitlement process")} for cases ${isAre(active.length + activeSla.length)} active.`, evidence, segmentExemptReason: single, facts: { active: active.length, processes: activeSla.length } };
    if (sla.status !== "ok" && !/isn't available/.test(sla.reason)) return nm(`No escalation rule is active, and entitlement processes couldn't be read: ${sla.reason}`, evidence);
    return {
      outcome: "issue",
      title: "Nothing escalates a case that stalls",
      summary: `No case escalation rule is active${rules.data.length ? ` (${plural(rules.data.length, "rule")} ${rules.data.length === 1 ? "exists, inactive" : "exist, all inactive"})` : ""}, and no entitlement process is active for cases. A case can sit untouched for as long as nobody notices.`,
      recommendation: "Add an escalation rule that moves cases open past an agreed time to a team lead's queue and tells them, or set up an entitlement process with milestones.",
      recommendationType: "change",
      severity: "medium",
      confidence: "high",
      evidence,
      value: notSized("A stalled case costs customers' patience, which has no honest figure in money."),
      effort: { size: "S" },
      owner: OWNER,
      facts: { rules: rules.data.length, active: 0 },
    };
  },
};

const escalationTargets: Rule = {
  id: "svc.escalation-targets",
  version: 1,
  domain: DOMAIN,
  title: "Escalations go to someone who can act",
  evaluate(ctx) {
    const rules = ctx.data(escalationRulesExtractor);
    if (rules.status !== "ok") return nm(`Escalation rules couldn't be read: ${rules.reason}`, [ctx.ev(rules.source, { count: null })]);
    const setup = setupOf(ctx);
    const evidence = [ctx.ev(rules.source, { count: rules.data.length }), ctx.ev(setup.queues.source, { count: setup.queues.status === "ok" ? setup.queues.data.length : null })];
    const actions = rules.data.filter((r) => r.active).flatMap((r: EscalationRule) => r.actions.filter((a) => a.assignedTo));
    if (!actions.length) return { outcome: "pass", summary: "No active escalation rule reassigns cases, so there's no target to check.", evidence, segmentExemptReason: "There are no escalation targets" };
    const { activeUsers, staffedQueues } = targets(ctx, setup);
    if (!activeUsers || !staffedQueues) return nm("Users or queues couldn't be read, so we can't tell whether escalation targets can act.", evidence);
    const dead = actions.filter((a) => (a.assignedToType === "Queue" ? !staffedQueues.has(a.assignedTo!.toLowerCase()) : !activeUsers.has(a.assignedTo!.toLowerCase())));
    const segmentsChecked: SegmentCheck[] = [{ dimension: "assigned to", segments: new Set(actions.map((a) => a.assignedToType ?? "User")).size }];
    if (!dead.length) return { outcome: "pass", summary: `All ${actions.length} escalation actions in active rules go to an active user or a queue with members.`, evidence, segmentsChecked, facts: { actions: actions.length } };
    return {
      outcome: "issue",
      title: "Escalations go to people who can't act on them",
      summary: `${dead.length} of ${actions.length} escalation actions in active rules assign cases to an inactive user or a queue with no members, so an escalated case lands where nobody will see it.`,
      recommendation: "Point each escalation at a queue with members, not a named person, and add escalation targets to your leaver checklist.",
      recommendationType: "change",
      severity: "high",
      confidence: "high",
      evidence,
      value: notSized("An escalation nobody sees costs customers' patience, which has no honest figure in money."),
      effort: { size: "XS" },
      owner: OWNER,
      segmentsChecked,
      facts: { dead: dead.length, actions: actions.length },
    };
  },
};

const queueSprawl: Rule = {
  id: "svc.queue-sprawl",
  version: 1,
  domain: DOMAIN,
  title: "Case queues have members, and agents aren't spread too thin",
  evaluate(ctx) {
    const setup = setupOf(ctx);
    const queues = caseQueues(setup);
    for (const source of [setup.queues, setup.queueObjects, setup.members]) {
      if (source.status !== "ok") return nm(`Queues couldn't be read: ${source.reason}`, unavailableEvidence(ctx, source.source));
    }
    if (!queues) return nm("Queues couldn't be read.", unavailableEvidence(ctx, setup.queues.source));
    const evidence = [ctx.ev(setup.queues.source, { count: queues.length, note: "Queues that take cases" }), ctx.ev(setup.members.source, { count: setup.members.status === "ok" ? setup.members.data.length : null })];
    if (queues.length === 0) return { outcome: "pass", summary: "No queue takes cases, so there's nothing to check.", evidence, segmentExemptReason: "There are no case queues" };
    const members = setup.members.status === "ok" ? setup.members.data : [];
    const ids = new Set(queues.map((q) => q.Id));
    const empty = queues.filter((q) => !members.some((m) => m.GroupId === q.Id));
    const perAgent = new Map<string, number>();
    for (const m of members) if (ids.has(m.GroupId) && m.UserOrGroupId.startsWith("005")) perAgent.set(m.UserOrGroupId, (perAgent.get(m.UserOrGroupId) ?? 0) + 1);
    const spread = [...perAgent.values()].filter((n) => n > QUEUES_PER_AGENT).length;
    const segmentsChecked: SegmentCheck[] = [{ dimension: "queue", segments: queues.length }, { dimension: "agent", segments: perAgent.size }];
    const facts = { queues: queues.length, empty: empty.length, agents: perAgent.size, spread, limit: QUEUES_PER_AGENT };
    if (!empty.length && !spread) return { outcome: "pass", summary: `All ${queues.length} case queues have members, and no agent is in more than ${QUEUES_PER_AGENT}.`, evidence, segmentsChecked, facts };
    const parts = [empty.length ? `${empty.length} of ${queues.length} case queues ${hasHave(empty.length)} no members, so cases routed there wait for nobody` : "", spread ? `${plural(spread, "agent")} ${isAre(spread)} in more than ${QUEUES_PER_AGENT} case queues, too many to watch` : ""].filter(Boolean);
    return {
      outcome: "issue",
      title: empty.length ? "Some case queues have nobody in them" : "Agents are spread across too many queues",
      summary: `${listed(parts)}.`,
      recommendation: "Merge or remove queues nobody works, give every remaining queue members, and keep each agent to the few queues they really cover.",
      recommendationType: "change",
      severity: empty.length ? "medium" : "low",
      confidence: "high",
      evidence,
      value: notSized("Queue set-up decides how fast cases are picked up, which has no honest figure in money."),
      effort: { size: "S" },
      owner: OWNER,
      segmentsChecked,
      subjects: empty.slice(0, 5).map((q) => ({ type: "queue", label: q.Name })),
      facts,
    };
  },
};

const omniRouting: Rule = {
  id: "svc.omni-routing",
  version: 1,
  domain: DOMAIN,
  title: "Omni-Channel routes every case queue",
  evaluate(ctx) {
    const setup = setupOf(ctx);
    const { channels, routingConfigs } = setup;
    if (channels.status !== "ok") return nm(`Omni-Channel couldn't be read: ${channels.reason} It may not be turned on.`, unavailableEvidence(ctx, channels.source));
    const evidence = [ctx.ev(channels.source, { count: channels.data.length })];
    const forCases = channels.data.filter((c) => c.RelatedEntity === "Case");
    if (forCases.length === 0) return { outcome: "pass", summary: "Omni-Channel isn't set up to route cases, so there's nothing to check.", evidence, segmentExemptReason: "No service channel takes cases" };
    const queues = caseQueues(setup);
    if (!queues) return nm("Queues couldn't be read.", evidence);
    if (setup.queues.status === "ok" && setup.queues.missingFields?.includes("Group.QueueRoutingConfigId")) return nm("Group.QueueRoutingConfigId isn't in this org, so we can't tell which queues Omni-Channel routes.", evidence);
    const routed = queues.filter((q) => q.QueueRoutingConfigId);
    const unrouted = queues.filter((q) => !q.QueueRoutingConfigId);
    const configs = routingConfigs.status === "ok" ? routingConfigs.data : [];
    const used = new Set(queues.map((q) => q.QueueRoutingConfigId).filter(Boolean));
    const unused = configs.filter((c) => !used.has(c.Id)).length;
    const noTimeout = configs.filter((c) => used.has(c.Id) && !c.PushTimeout).length;
    const facts = { queues: queues.length, routed: routed.length, unrouted: unrouted.length, unusedConfigs: unused, noTimeout };
    const segmentsChecked: SegmentCheck[] = [{ dimension: "queue", segments: queues.length }, { dimension: "routing configuration", segments: configs.length }];
    if (routingConfigs.status === "ok") evidence.push(ctx.ev(routingConfigs.source, { count: configs.length }));
    // Some queues routed and some not: cases in the rest wait to be picked by hand.
    if (routed.length === 0 || unrouted.length === 0) {
      return { outcome: "pass", summary: routed.length ? `All ${routed.length} case queues have a routing configuration.${noTimeout ? ` ${noTimeout} of the configurations in use ${noTimeout === 1 ? "has" : "have"} no push timeout.` : ""}` : `Omni-Channel has a service channel for cases, but routes none of the ${queues.length} case queues: cases are picked by hand.`, evidence, segmentsChecked, facts };
    }
    return {
      outcome: "issue",
      title: "Omni-Channel routes some case queues and not others",
      summary: `${unrouted.length} of ${queues.length} case queues ${hasHave(unrouted.length)} no routing configuration while ${routed.length} ${routed.length === 1 ? "does" : "do"}, so cases in those queues wait to be picked by hand while the rest are pushed to agents.${unused ? ` ${plural(unused, "routing configuration")} ${unused === 1 ? "isn't" : "aren't"} used by any queue.` : ""}`,
      recommendation: "Give every case queue a routing configuration, or take the queues nobody should be pushed work from out of Omni-Channel on purpose. Remove configurations no queue uses.",
      recommendationType: "change",
      severity: "medium",
      confidence: "high",
      evidence,
      value: notSized("How work is routed shows in waiting times, which have no honest figure in money."),
      effort: { size: "S" },
      owner: OWNER,
      segmentsChecked,
      subjects: unrouted.slice(0, 5).map((q) => ({ type: "queue", label: q.Name })),
      facts,
    };
  },
};

const agentLicences: Rule = {
  id: "svc.agent-licences",
  version: 2,
  domain: DOMAIN,
  title: "Service Cloud licences are used",
  evaluate(ctx) {
    const { supportUsers } = setupOf(ctx);
    if (supportUsers.status !== "ok") return nm(`Service Cloud users couldn't be read: ${supportUsers.reason}`, unavailableEvidence(ctx, supportUsers.source));
    const dormantDays = ctx.calibration.dormantDays;
    const cutoff = daysBefore(ctx.now, dormantDays);
    const licenceOf = (u: (typeof supportUsers.data)[number]) => u.Profile?.UserLicense?.Name ?? "Unknown";
    // Integration licences never log in by design.
    const users = supportUsers.data.filter((u) => !/\bintegration\b/i.test(licenceOf(u)));
    const evidence = [ctx.ev(supportUsers.source, { count: users.length, exclusions: ["Users on integration licences"] })];
    if (users.length === 0) return { outcome: "pass", summary: "No active user holds a Service Cloud licence.", evidence, segmentExemptReason: "There are no Service Cloud users" };
    // Judged as the dormant-user count judges them, login log and all, so the two agree about each person (DECISIONS.md 394).
    const logins = ctx.data(loginsExtractor);
    const dormant = users.filter((u) => lapse(u, logins, cutoff) !== null);
    // In a full check, someone who logs in but works no case also holds a seat for nothing.
    let idle: string[] = [];
    if (ctx.scope !== "essentials") {
      const cases = ctx.data(serviceCasesExtractor);
      const work = ctx.data(serviceActivityExtractor);
      if (cases?.open.status === "ok" && cases.closedCases.status === "ok") {
        const since = daysBefore(ctx.now, dormantDays).getTime();
        const worked = new Set([...cases.open.data.map((c) => c.OwnerId), ...cases.closedCases.data.filter((c) => Date.parse(c.ClosedDate) >= since).map((c) => c.OwnerId), ...(work?.agentWork.status === "ok" ? work.agentWork.data.map((w) => w.UserId) : [])]);
        idle = users.filter((u) => !dormant.includes(u) && !worked.has(u.Id)).map((u) => u.Id);
        evidence.push(ctx.ev(cases.open.source, { count: cases.open.data.length, note: "Case owners, to see who works cases" }));
      }
    }
    const wasted = dormant.length + idle.length;
    const segmentsChecked: SegmentCheck[] = [{ dimension: "licence", segments: new Set(users.map(licenceOf)).size }];
    const facts = { users: users.length, dormant: dormant.length, idle: idle.length, days: dormantDays };
    if (!wasted) return { outcome: "pass", summary: `All ${users.length} Service Cloud users logged in within ${dormantDays} days${ctx.scope !== "essentials" ? " and worked a case" : ""}.`, evidence, segmentsChecked, facts };
    const org = ctx.data(orgExtractor);
    const edition = org.status === "ok" ? (org.data[0]?.OrganizationType ?? null) : null;
    const price = costPerSeat(SERVICE_SEAT, { calibration: ctx.calibration, edition });
    const value: Value = isPriced(price)
      ? { kind: "licence_cost", amount: Math.round(price.annual * wasted), unit: price.currency, period: "per year", assumptions: [`${plural(wasted, "Service Cloud seat")} at ${price.note}`] }
      : notSized(`A Service Cloud seat has no price: ${price.reason}`);
    return {
      outcome: "issue",
      title: "Some Service Cloud licences aren't being used",
      summary: `Of ${users.length} Service Cloud users, ${listed([dormant.length ? `${dormant.length} ${hasHave(dormant.length)}n't logged in for ${dormantDays} days` : "", idle.length ? `${idle.length} log${idle.length === 1 ? "s" : ""} in but ${hasHave(idle.length)}n't owned or been routed a case in that time` : ""].filter(Boolean))}. That's ${plural(wasted, "seat")} held for nobody's casework.`,
      recommendation: "Confirm with each user's manager, then remove the Service Cloud licence from those who don't work cases, or move them to a licence that fits what they do.",
      recommendationType: "change",
      severity: "medium",
      confidence: idle.length ? "medium" : "high",
      evidence,
      value,
      effort: { size: "XS" },
      owner: OWNER,
      segmentsChecked,
      facts: { ...facts, wasted, ...(value.kind === "licence_cost" ? { annualSaving: value.amount } : {}) },
    };
  },
};

/** Published articles, or why there's nothing to check. */
function publishedArticles(ctx: RuleContext): { rows: ArticleRow[]; evidence: Evidence[] } | RuleResult {
  const { articles } = setupOf(ctx);
  if (articles.status !== "ok") return nm(`Knowledge articles couldn't be read: ${articles.reason} Knowledge may not be turned on.`, unavailableEvidence(ctx, articles.source));
  const evidence = [ctx.ev(articles.source, { count: articles.data.length, note: "Published articles, latest versions" })];
  if (articles.data.length === 0) return nm("Knowledge is on, but no article is published.", evidence);
  return { rows: articles.data, evidence };
}

const knowledgeUse: Rule = {
  id: "svc.knowledge-use",
  version: 1,
  domain: DOMAIN,
  title: "Agents attach knowledge articles to cases",
  records: true,
  evaluate(ctx) {
    const read = publishedArticles(ctx);
    if ("outcome" in read) return read;
    if (read.rows.length < MIN_ARTICLES) return nm(`Only ${plural(read.rows.length, "article")} ${isAre(read.rows.length)} published, fewer than the ${MIN_ARTICLES} it takes for use to mean something.`, read.evidence);
    const cases = ctx.data(serviceCasesExtractor);
    const { caseArticles } = ctx.data(serviceActivityExtractor);
    if (cases.closedCases.status !== "ok") return nm(`Closed cases couldn't be read: ${cases.closedCases.reason}`, unavailableEvidence(ctx, cases.closedCases.source));
    if (caseArticles.status !== "ok") return nm(`Articles on cases couldn't be read: ${caseArticles.reason}`, unavailableEvidence(ctx, caseArticles.source));
    const withArticle = new Set(caseArticles.data.map((a) => a.CaseId));
    const closed = cases.closedCases.data;
    const evidence = [...read.evidence, ctx.ev(caseArticles.source, { count: caseArticles.data.length })];
    if (closed.length === 0) return { outcome: "pass", summary: "No case was closed in the last year, so there's no use to check.", evidence, segmentExemptReason: "There are no closed cases" };
    const result = segmentRate(closed, (c) => withArticle.has(c.Id), { priority: priorityOf, origin: originOf }, { segmentLimit: 100, minSegment: MIN_SEGMENT });
    const facts = { attached: result.hits, closed: result.n, rate: result.rate, articles: read.rows.length, limit: ATTACH_SHARE };
    if (result.rate >= ATTACH_SHARE) return { outcome: "pass", summary: `${result.hits} of ${result.n} cases closed in the last year (${result.rate}%) have an article attached.`, evidence, segmentsChecked: result.segmentsChecked, facts };
    return {
      outcome: "issue",
      title: "Agents rarely attach knowledge articles to cases",
      summary: `Only ${result.hits} of ${result.n} cases closed in the last year (${result.rate}%) have an article attached, though ${read.rows.length} articles are published. Either the articles don't answer what customers ask, or agents don't use them.`,
      recommendation: "Put the Knowledge component on the case page, ask agents to attach the article that solved each case, and write articles for the case types that come up most.",
      recommendationType: "change",
      severity: "low",
      confidence: "high",
      evidence,
      value: notSized("Articles save agents' time, but there's no agreed figure for how much here."),
      effort: { size: "S" },
      owner: "Knowledge manager",
      segmentsChecked: result.segmentsChecked,
      facts,
    };
  },
};

const knowledgeCurrent: Rule = {
  id: "svc.knowledge-current",
  version: 1,
  domain: DOMAIN,
  title: "Knowledge articles are kept current",
  evaluate(ctx) {
    const read = publishedArticles(ctx);
    if ("outcome" in read) return read;
    const { articleViews } = setupOf(ctx);
    const months = limitsOf(ctx.calibration).stalenessMonths;
    const cutoff = monthsBefore(ctx.now, months).getTime();
    const views = new Map<string, number>();
    // An "AllChannels" row would repeat the per-channel ones, so it's used alone when there is one (QUESTIONS.md).
    if (articleViews.status === "ok") {
      const all = articleViews.data.filter((v) => v.Channel === "AllChannels");
      for (const v of all.length ? all : articleViews.data) views.set(v.ParentId, (views.get(v.ParentId) ?? 0) + v.ViewCount);
    }
    const stale = read.rows.filter((a) => !a.LastPublishedDate || Date.parse(a.LastPublishedDate) < cutoff);
    const unread = articleViews.status === "ok" ? read.rows.filter((a) => !views.get(a.KnowledgeArticleId)).length : null;
    const share = pct(stale.length, read.rows.length);
    const facts = { stale: stale.length, articles: read.rows.length, rate: share, months, limit: STALE_SHARE, ...(unread === null ? {} : { unread }) };
    const evidence = articleViews.status === "ok" ? [...read.evidence, ctx.ev(articleViews.source, { count: articleViews.data.length })] : read.evidence;
    const exempt = "Articles aren't grouped in a way we can read";
    if (share <= STALE_SHARE) return { outcome: "pass", summary: `${stale.length} of ${read.rows.length} published articles (${share}%) haven't been republished in ${months} months.`, evidence, segmentExemptReason: exempt, facts };
    return {
      outcome: "issue",
      title: "Most knowledge articles haven't been reviewed in a year",
      summary: `${stale.length} of ${read.rows.length} published articles (${share}%) haven't been republished in ${months} months.${unread !== null ? ` ${unread} ${hasHave(unread)} never been viewed.` : ""} Out-of-date answers cost agents' and customers' trust in the rest.`,
      recommendation: "Give each article an owner and a review date, archive what nobody reads, and update the articles behind your most common case types first.",
      recommendationType: "change",
      severity: "low",
      confidence: "high",
      evidence,
      value: notSized("Out-of-date articles cost trust, which has no honest figure in money."),
      effort: { size: "M" },
      owner: "Knowledge manager",
      facts,
    };
  },
};

const macros: Rule = {
  id: "svc.macros-and-quick-text",
  version: 1,
  domain: DOMAIN,
  title: "Macros and quick text are used and current",
  evaluate(ctx) {
    const { macros: list, macroUsage, quickText, quickTextUsage } = setupOf(ctx);
    if (list.status !== "ok") return nm(`Macros couldn't be read: ${list.reason}`, unavailableEvidence(ctx, list.source));
    const evidence = [ctx.ev(list.source, { count: list.data.length })];
    if (list.data.length === 0) return { outcome: "pass", summary: "The org has no macros.", evidence, segmentExemptReason: "There are no macros" };
    const months = limitsOf(ctx.calibration).stalenessMonths;
    const classic = list.data.filter((m) => !m.IsLightningSupported).length;
    const usedMacros = macroUsage.status === "ok" ? new Set(macroUsage.data.filter((u) => u.n > 0).map((u) => u.MacroID)) : null;
    const unusedMacros = usedMacros ? list.data.filter((m) => !usedMacros.has(m.Id)).length : null;
    const usedText = quickText.status === "ok" && quickTextUsage.status === "ok" ? new Set(quickTextUsage.data.filter((u) => u.n > 0).map((u) => u.QuickTextID)) : null;
    const unusedText = usedText && quickText.status === "ok" ? quickText.data.filter((q) => !usedText.has(q.Id)).length : null;
    if (macroUsage.status === "ok") evidence.push(ctx.ev(macroUsage.source, { count: macroUsage.data.length, note: `Use in the last ${months} months` }));
    if (quickText.status === "ok") evidence.push(ctx.ev(quickText.source, { count: quickText.data.length }));
    const unusedShare = unusedMacros === null ? null : pct(unusedMacros, list.data.length);
    const facts = { macros: list.data.length, classic, months, ...(unusedMacros === null ? {} : { unusedMacros, unusedShare: unusedShare! }), ...(unusedText === null || quickText.status !== "ok" ? {} : { quickText: quickText.data.length, unusedText }) };
    const segmentsChecked: SegmentCheck[] = [{ dimension: "kind", segments: 1 + (quickText.status === "ok" ? 1 : 0) }];
    const cluttered = unusedShare !== null && list.data.length >= MIN_MACROS && unusedShare > UNUSED_SHARE;
    if (!cluttered && classic === 0) {
      return { outcome: "pass", summary: `${list.data.length} macros, ${unusedMacros === null ? "with use we couldn't read" : `${unusedMacros} unused in ${months} months`}, and none that work only in Classic.`, evidence, segmentsChecked, facts };
    }
    const parts = [
      cluttered ? `${unusedMacros} of ${list.data.length} macros (${unusedShare}%) ${unusedMacros === 1 ? "wasn't" : "weren't"} used in ${months} months` : "",
      classic ? `${classic} ${classic === 1 ? "macro works" : "macros work"} only in Classic` : "",
      unusedText !== null && quickText.status === "ok" && unusedText ? `${unusedText} of ${quickText.data.length} quick text entries ${unusedText === 1 ? "wasn't" : "weren't"} used` : "",
    ].filter(Boolean);
    return {
      outcome: "issue",
      title: "Macros nobody uses clutter the agents' list",
      summary: `${listed(parts)}. A long list of stale shortcuts makes the useful ones hard to find.`,
      recommendation: "Remove macros and quick text nobody has used in a year, rebuild the Classic-only macros for Lightning, and give the rest an owner.",
      recommendationType: "change",
      severity: "low",
      confidence: unusedMacros === null ? "medium" : "high",
      evidence,
      value: notSized("Tidier macros save agents a little time each, with no agreed figure for it here."),
      effort: { size: "S" },
      owner: "Service operations",
      segmentsChecked,
      facts,
    };
  },
};

/**
 * Check 16, which reports under Automation: cases routed by both assignment
 * rules and Omni-Channel. It's part of the service module so it runs only when
 * Service Cloud is in the run.
 */
const caseRoutingOverlap: Rule = {
  id: "aut.case-routing-overlap",
  version: 1,
  domain: "automation",
  title: "Cases routed one way, not two",
  evaluate(ctx) {
    const setup = setupOf(ctx);
    const { assignmentRules, channels } = setup;
    if (assignmentRules.status !== "ok") return nm(`Case assignment rules couldn't be read: ${assignmentRules.reason}`, unavailableEvidence(ctx, assignmentRules.source));
    const evidence = [ctx.ev(assignmentRules.source, { count: assignmentRules.data.length })];
    const active = assignmentRules.data.filter((r) => r.Active);
    if (channels.status !== "ok") return { outcome: "pass", summary: `Omni-Channel couldn't be read (${channels.reason}), so assignment rules are the only routing we can see.`, evidence, segmentExemptReason: "There's one routing set-up to look at" };
    const queues = caseQueues(setup) ?? [];
    const routedQueues = queues.filter((q) => q.QueueRoutingConfigId).length;
    const omni = channels.data.some((c) => c.RelatedEntity === "Case") && routedQueues > 0;
    evidence.push(ctx.ev(channels.source, { count: channels.data.length }));
    const facts = { activeRules: active.length, routedQueues };
    if (!active.length || !omni) return { outcome: "pass", summary: !omni ? "Omni-Channel doesn't route cases, so assignment rules alone decide where they go." : "No case assignment rule is active, so Omni-Channel alone routes cases.", evidence, segmentExemptReason: "There's one routing set-up to look at", facts };
    return {
      outcome: "issue",
      title: "Cases are routed by two systems at once",
      summary: `An active case assignment rule and Omni-Channel routing on ${plural(routedQueues, "case queue")} both decide where cases go. When the two disagree, a case can be moved after an agent has picked it up, or land in a queue Omni-Channel doesn't watch.`,
      recommendation: "Choose one: let assignment rules only put cases into queues, and Omni-Channel push them from those queues to agents. Check no rule assigns cases straight to people.",
      recommendationType: "change",
      severity: "medium",
      confidence: "medium",
      evidence,
      value: notSized("Routing conflicts show in waiting times, which have no honest figure in money."),
      effort: { size: "S" },
      owner: OWNER,
      facts,
    };
  },
};

export const serviceSetupModule: DomainModule = {
  id: DOMAIN,
  cloud: "service",
  label: "Service set-up",
  extractors: [orgExtractor, usersExtractor, loginsExtractor, serviceSetupExtractor, escalationRulesExtractor, serviceCasesExtractor, serviceActivityExtractor],
  rules: [escalation, escalationTargets, queueSprawl, omniRouting, agentLicences, knowledgeUse, knowledgeCurrent, macros, caseRoutingOverlap],
};

