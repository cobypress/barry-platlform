import { notSized, pct, plural, segmentRate, type NotMeasurableResult, type Rule, type RuleContext, type RuleResult } from "../../engine/rule.ts";
import { schemaExtractor } from "../../extract/configuration.ts";
import { pardotLeadsExtractor, pardotSetupExtractor, UNWORKED_DAYS, unworkedBefore, type SyncedLeadRow } from "../../extract/pardot.ts";
import { daysBefore } from "../../extract/types.ts";
import { licencesExtractor, usersExtractor } from "../../extract/users.ts";
import type { DomainModule } from "../types.ts";
import { atOwnLeadReturn } from "../value.ts";

// Marketing Cloud Account Engagement (roadmap Stage 6, DECISIONS.md 261 to 274):
// is marketing handing sales good leads, and is what it hands over worked? One
// area, switched on at intake. Read from the Salesforce side only.

const DOMAIN = "account_engagement";
/** The console's module, and the area's id: one area, so no mapping as for "service" or "cpq". */
export const ACCOUNT_ENGAGEMENT_MODULE = DOMAIN;
// Our judgements, each stated in the finding that uses it.
/** Past this share of synced leads with no grade, or no score, grading or scoring isn't doing its job. */
const UNRATED_SHARE = 50;
/** Past this share of handed-over leads left unworked, the handover is failing. */
const UNWORKED_SHARE = 10;
/** Leads untouched longer than this are dq.stale-leads' business, so they aren't counted twice. */
const STALE_DAYS = 90;
/** A lead source needs this many handed-over leads before its own rate can be called out. */
const MIN_SOURCE = 10;
const OWNER = "Head of marketing";

function presenceGap(ctx: RuleContext): NotMeasurableResult | null {
  const { presence, influence } = ctx.data(pardotSetupExtractor);
  if (presence.status === "installed") return null;
  return { outcome: "not_measurable", reason: presence.reason, evidence: [ctx.ev(influence.source, { count: null })] };
}

const day = (iso: string): string => iso.slice(0, 10);
const queueOwned = (ownerId: string): boolean => ownerId.startsWith("00G");

const scoringGrading: Rule = {
  id: "mkt.scoring-grading",
  version: 1,
  records: true,
  domain: DOMAIN,
  title: "Synced leads with no score or grade",
  evaluate(ctx): RuleResult {
    const gap = presenceGap(ctx);
    if (gap) return gap;
    const { counts } = ctx.data(pardotLeadsExtractor);
    for (const c of Object.values(counts)) if (c.status !== "ok") return { outcome: "not_measurable", reason: `Synced leads couldn't be counted: ${c.reason}`, evidence: [ctx.ev(c.source, { count: null })] };
    const [synced, ungraded, unscored] = [counts.synced, counts.ungraded, counts.unscored].map((c) => (c.status === "ok" ? c.data : 0)) as [number, number, number];
    const evidence = [ctx.ev(counts.synced.source, { count: synced, dateRange: ctx.window, note: "Leads with an Account Engagement prospect (pi__url__c), created in the window" }), ctx.ev(counts.ungraded.source, { count: ungraded }), ctx.ev(counts.unscored.source, { count: unscored, note: "A score of 0 counts as no score" })];
    const facts = { synced, ungraded, unscored, ungradedRate: pct(ungraded, synced), unscoredRate: pct(unscored, synced) };
    if (synced === 0) return { outcome: "pass", summary: "No lead synced from Account Engagement was created in the window.", evidence, segmentExemptReason: "There are no synced leads to segment.", facts };
    const segments = [{ dimension: "rating", segments: 2 }];
    if (facts.ungradedRate < UNRATED_SHARE && facts.unscoredRate < UNRATED_SHARE) return { outcome: "pass", summary: `Of ${plural(synced, "lead")} synced from Account Engagement in the window, ${facts.ungradedRate}% have no grade and ${facts.unscoredRate}% no score.`, evidence, segmentsChecked: segments, facts };
    return {
      outcome: "issue",
      title: "Marketing isn't rating the leads it hands to sales",
      summary: `Of ${plural(synced, "lead")} synced from Account Engagement in the window, ${ungraded} (${facts.ungradedRate}%) have no grade and ${unscored} (${facts.unscoredRate}%) no score. Without both, sales can't tell a good fit who's interested from a poor fit who opened one email, so every lead looks the same.`,
      recommendation: "Grade prospects against your ideal customer profile, check that scoring rewards real buying signals, and hand leads to sales on both.",
      recommendationType: "change",
      severity: "medium",
      confidence: "high",
      evidence,
      value: notSized("The cost is sales time spent on the wrong leads, which the lead records don't show."),
      effort: { size: "M" },
      owner: OWNER,
      dependencies: ["Agree with sales what a good lead looks like before changing the rules"],
      segmentsChecked: segments,
      facts,
    };
  },
};

/** Open synced leads, with the users who own them: or why they can't be read. */
function openLeads(ctx: RuleContext): { rows: SyncedLeadRow[]; truncated: boolean; active: Set<string>; evidence: ReturnType<RuleContext["ev"]>[] } | NotMeasurableResult {
  const { open } = ctx.data(pardotLeadsExtractor);
  const users = ctx.data(usersExtractor);
  if (open.status !== "ok") return { outcome: "not_measurable", reason: `Synced leads couldn't be read: ${open.reason}`, evidence: [ctx.ev(open.source, { count: null })] };
  if (users.status !== "ok") return { outcome: "not_measurable", reason: `Users couldn't be read: ${users.reason}`, evidence: [ctx.ev(users.source, { count: null })] };
  const exclusions = [...(open.truncated ? [`Open synced leads past the first ${open.data.length.toLocaleString("en-GB")}, in order of Salesforce id`] : []), "Converted leads"];
  return { rows: open.data, truncated: open.truncated, active: new Set(users.data.filter((u) => u.IsActive).map((u) => u.Id)), evidence: [ctx.ev(open.source, { count: open.data.length, dateRange: ctx.window, exclusions })] };
}

const unworkedHandovers: Rule = {
  id: "mkt.unworked-handovers",
  version: 2,
  records: true,
  domain: DOMAIN,
  title: "Leads handed to sales and never worked",
  evaluate(ctx): RuleResult {
    const gap = presenceGap(ctx);
    if (gap) return gap;
    const read = openLeads(ctx);
    if ("outcome" in read) return read;
    const handed = read.rows.filter((l) => !queueOwned(l.OwnerId) && read.active.has(l.OwnerId));
    const before = unworkedBefore(ctx.now).toISOString();
    const stale = daysBefore(ctx.now, STALE_DAYS).toISOString();
    // Created 14 to 90 days ago with nothing logged. Older ones are dq.stale-leads', and counted there.
    const isUnworked = (l: SyncedLeadRow): boolean => l.LastActivityDate === null && l.CreatedDate < before && l.CreatedDate >= stale;
    const unworked = handed.filter(isUnworked).sort((a, b) => a.CreatedDate.localeCompare(b.CreatedDate));
    const rate = pct(unworked.length, handed.length);
    const facts = { handed: handed.length, unworked: unworked.length, rate, days: UNWORKED_DAYS, staleDays: STALE_DAYS };
    const evidence = read.evidence;
    if (handed.length === 0) return { outcome: "pass", summary: "No open lead synced from Account Engagement is owned by a salesperson.", evidence, segmentExemptReason: "There are no handed-over leads to segment.", facts };
    // Rule 4: a source nobody works can hide under well-worked ones, so each source is checked before it passes.
    const bySource = segmentRate(handed, isUnworked, { "lead source": (l) => l.LeadSource ?? "No source" }, { segmentLimit: UNWORKED_SHARE, minSegment: MIN_SOURCE });
    const segments = bySource.segmentsChecked;
    const worst = bySource.hotspots[0];
    if (unworked.length === 0 || (rate < UNWORKED_SHARE && !worst)) return { outcome: "pass", summary: `Of ${plural(handed.length, "open lead")} synced from Account Engagement and owned by a salesperson, ${unworked.length} made ${UNWORKED_DAYS} to ${STALE_DAYS} days ago ${unworked.length === 1 ? "has" : "have"} no activity logged (${rate}%).`, evidence, segmentsChecked: segments, facts };
    return {
      outcome: "issue",
      title: "Leads marketing hands over aren't being followed up",
      summary: `Of ${plural(handed.length, "open lead")} synced from Account Engagement and owned by a salesperson, ${read.truncated ? "at least " : ""}${unworked.length} (${rate}%) made ${UNWORKED_DAYS} to ${STALE_DAYS} days ago have no activity logged: marketing handed them over, and nobody followed up while they were warm.${worst ? ` Worst by lead source: ${worst.key}, at ${worst.rate}% of ${worst.n}.` : ""}`,
      recommendation: "Agree how fast sales follows up a marketing lead, make it part of pipeline reviews, and send leads nobody will work back to marketing to nurture.",
      recommendationType: "change",
      severity: rate >= 50 ? "high" : "medium",
      confidence: "high",
      evidence,
      value: atOwnLeadReturn(ctx, unworked.length, { period: "one-off, if these leads are worked", caveat: "Assumes these leads return what your average lead does. Leads left this long often return less, so treat it as a ceiling" }),
      effort: { size: "S" },
      owner: "Head of sales",
      dependencies: ["Check that activity is being logged before treating this as a follow-up problem"],
      segmentsChecked: segments,
      subjects: bySource.hotspots.slice(0, 5).map((h) => ({ type: h.dimension, label: h.key })),
      facts,
      records: { object: "Lead", rows: unworked.map((l) => ({ id: l.Id, problem: `Created ${day(l.CreatedDate)}${l.LeadSource ? ` from ${l.LeadSource}` : ""}, owned by a salesperson, with no activity logged` })), fix: "Call or email them now, or hand them back to marketing to nurture" },
    };
  },
};

const unownedProspects: Rule = {
  id: "mkt.unowned-prospects",
  version: 1,
  records: true,
  domain: DOMAIN,
  title: "Synced leads nobody owns",
  evaluate(ctx): RuleResult {
    const gap = presenceGap(ctx);
    if (gap) return gap;
    const read = openLeads(ctx);
    if ("outcome" in read) return read;
    const before = unworkedBefore(ctx.now).toISOString();
    // A queue can be how leads are shared out, so only one left there, untouched, counts.
    const inQueue = read.rows.filter((l) => queueOwned(l.OwnerId) && l.LastActivityDate === null && l.CreatedDate < before);
    const leftBehind = read.rows.filter((l) => !queueOwned(l.OwnerId) && !read.active.has(l.OwnerId));
    const all = [...inQueue, ...leftBehind];
    const rate = pct(all.length, read.rows.length);
    const facts = { open: read.rows.length, inQueue: inQueue.length, inactiveOwner: leftBehind.length, rate, days: UNWORKED_DAYS };
    if (read.rows.length === 0) return { outcome: "pass", summary: "No lead synced from Account Engagement is open.", evidence: read.evidence, segmentExemptReason: "There are no open synced leads to segment.", facts };
    const segments = [{ dimension: "owner type", segments: 2 }];
    if (all.length === 0) return { outcome: "pass", summary: `All ${plural(read.rows.length, "open lead")} synced from Account Engagement are owned by an active person, or have been in a queue for under ${UNWORKED_DAYS} days.`, evidence: read.evidence, segmentsChecked: segments, facts };
    const parts = [inQueue.length ? `${inQueue.length} ${inQueue.length === 1 ? "has" : "have"} sat untouched in a queue for ${UNWORKED_DAYS} days or more` : "", leftBehind.length ? `${leftBehind.length} ${leftBehind.length === 1 ? "is" : "are"} owned by someone who has left` : ""].filter(Boolean);
    return {
      outcome: "issue",
      title: "Some synced leads have nobody to work them",
      summary: `Of ${plural(read.rows.length, "open lead")} synced from Account Engagement, ${parts.join(", and ")} (${rate}% in all). Nobody is going to call these people.`,
      recommendation: "Assign each to an active salesperson or back to marketing, and check the assignment rules that left them there.",
      recommendationType: "change",
      severity: rate >= UNWORKED_SHARE ? "medium" : "low",
      confidence: "high",
      evidence: read.evidence,
      value: notSized("The leads already worked out in the unworked check are where the value is, so these aren't sized again."),
      effort: { size: "XS" },
      owner: "Sales operations",
      segmentsChecked: segments,
      facts,
      records: { object: "Lead", rows: all.map((l) => ({ id: l.Id, problem: queueOwned(l.OwnerId) ? `In a queue since ${day(l.CreatedDate)}, untouched` : "Owned by someone who has left" })), fix: "Give it to an active salesperson, or back to marketing to nurture" },
    };
  },
};

// Campaign influence is set-up, so a set-up review checks it (DECISIONS.md 265).
const campaignInfluence: Rule = {
  id: "mkt.campaign-influence",
  version: 1,
  domain: DOMAIN,
  title: "Campaign influence set up",
  evaluate(ctx): RuleResult {
    const gap = presenceGap(ctx);
    if (gap) return gap;
    const { influence } = ctx.data(pardotSetupExtractor);
    if (influence.status !== "ok") return { outcome: "not_measurable", reason: `The campaign settings couldn't be read: ${influence.reason}`, evidence: [ctx.ev(influence.source, { count: null })] };
    const { customizable, fromAccountEngagement } = influence.data;
    const evidence = [ctx.ev(influence.source, { count: 1, note: "CampaignSettings: enableCampaignInfluence2 and enableB2bmaCampaignInfluence2" })];
    if (customizable === null) return { outcome: "not_measurable", reason: "Salesforce didn't return whether Customizable Campaign Influence is on.", evidence };
    const exempt = "Campaign influence is one setting for the whole org.";
    if (customizable && fromAccountEngagement !== false) return { outcome: "pass", summary: `Customizable Campaign Influence is on${fromAccountEngagement ? ", and Account Engagement's influence models can be used in Salesforce" : ""}.`, evidence, segmentExemptReason: exempt };
    return {
      outcome: "issue",
      title: customizable ? "Account Engagement's attribution can't be used in Salesforce" : "You can't see which campaigns helped win deals",
      summary: customizable
        ? "Customizable Campaign Influence is on, but the org isn't set to use influence models from Account Engagement, so its attribution doesn't reach the opportunities sales and finance look at."
        : "Customizable Campaign Influence is off, so Salesforce can't show which campaigns played a part in each opportunity, and marketing's contribution to pipeline can't be measured where sales and finance look.",
      recommendation: customizable ? "Allow influence models from Account Engagement in the campaign settings, then choose the model you'll report on." : "Turn on Customizable Campaign Influence, and agree with sales and finance which influence model you'll report on.",
      recommendationType: "change",
      severity: customizable ? "low" : "medium",
      confidence: "high",
      evidence,
      value: notSized("Seeing marketing's influence is a measurement gap, not a figure we can size."),
      effort: { size: "S" },
      owner: OWNER,
      dependencies: ["Check with whoever owns campaign reports before changing the model, because existing reports read the old one"],
      segmentsChecked: [{ dimension: "setting", segments: 2, note: exempt }],
    };
  },
};

export const accountEngagementModule: DomainModule = {
  id: DOMAIN,
  cloud: "pardot",
  label: "Marketing handover",
  extractors: [schemaExtractor, licencesExtractor, usersExtractor, pardotSetupExtractor, pardotLeadsExtractor],
  rules: [unworkedHandovers, scoringGrading, unownedProspects, campaignInfluence],
};
