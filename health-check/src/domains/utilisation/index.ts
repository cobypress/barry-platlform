import { money } from "../../render/format.ts";
import { fieldDescribeExtractor } from "../../extract/fields.ts";
import { apexExtractor, metadataExtractor, scheduledJobsExtractor } from "../../extract/automation.ts";
import { orgExtractor, recordCountsExtractor } from "../../extract/configuration.ts";
import { daysBefore, monthsBefore } from "../../extract/types.ts";
import {
  activityCaptureExtractor,
  licencesExtractor,
  loginsExtractor,
  ownershipExtractor,
  reportsExtractor,
  runAsExtractor,
  usersExtractor,
  type UserRow,
} from "../../extract/users.ts";
import { USER_GUARD_BLOCKERS, userGuards } from "../../guards/user-guards.ts";
import { notSized, pct, plural, type RecordList, type Rule, type RuleContext, type RuleResult } from "../../engine/rule.ts";
import type { Guard, Value } from "../../schemas/index.ts";
import type { DomainModule } from "../types.ts";
import { calibratedHours } from "../value.ts";
import { COMPLIMENTARY_LICENCES } from "../../money/licences.ts";
import { investmentMetrics } from "./investment.ts";

const MIN_SPARE_SEATS = 2;
// API-only licences. Their users never log in by design, so not logging in says nothing.
// A real org showed the Sales Insights integration user as a licence to release.
const isIntegrationLicence = (licence: string): boolean => /\bintegration\b/i.test(licence);
const SPARE_SEAT_SHARE = 10;
const UNUSED_REPORT_MONTHS = 12;
const UNUSED_REPORT_SHARE = 50;
const MIN_REPORTS = 50;
const RECENT_REPORT_DAYS = 90;
const HEALTHY_CONNECTIONS = new Set(["ACTIVE", "PROCESSING", "INITIALIZING"]);

const licenceOf = (user: UserRow): string => user.Profile?.UserLicense?.Name ?? "Unknown";

function seatValue(ctx: RuleContext, users: UserRow[]): Value {
  const costs = ctx.calibration.licenceCosts;
  const unpriced = [...new Set(users.map(licenceOf))].filter((l) => costs[l] === undefined);
  if (unpriced.length > 0) return notSized(`The calibration has no cost for: ${unpriced.sort().join(", ")}.`);
  const byLicence = new Map<string, number>();
  for (const user of users) byLicence.set(licenceOf(user), (byLicence.get(licenceOf(user)) ?? 0) + 1);
  return {
    kind: "licence_cost",
    amount: users.reduce((sum, u) => sum + costs[licenceOf(u)]!, 0),
    unit: ctx.calibration.currency,
    period: "per year",
    assumptions: [...byLicence].sort().map(([licence, n]) => `${plural(n, `${licence} seat`)} at ${costs[licence]} ${ctx.calibration.currency} a year, from the calibration`),
  };
}

// Splits candidates on the guards. The engine would downgrade a mixed finding
// anyway, but this way the clean users keep a real recommendation.
function reclaimFindings(ctx: RuleContext, candidates: UserRow[], describe: { key: string; label: string; basis: string }): RuleResult[] {
  const users = ctx.data(usersExtractor);
  const usersEvidence = ctx.ev(users.source, { count: candidates.length, exclusions: ["Non-standard user types", "Users created inside the dormant window", "Users on integration licences"] });
  const assessed = candidates.map((user) => ({ user, guards: userGuards(ctx, user) }));
  const clear = assessed.filter((a) => a.guards.every((g) => g.status === "passed"));
  const manual = ctx.manual("default-workflow-user");
  // A skipped step makes the finding not measurable. Users with a real blocker
  // are still worth reporting, but one held up only by the skip has no answer.
  const heldBySkip = (a: { guards: Guard[] }): boolean => manual.status === "skipped" && a.guards.filter((g) => g.status !== "passed").every((g) => g.guardId === "not-default-workflow-user");
  const unanswered = assessed.filter((a) => !clear.includes(a) && heldBySkip(a));
  // A set-up review never reads ownership. A user held up by nothing else is one
  // to check by hand, and still worth showing with what the seat costs.
  const unreadOwnership = (g: Guard): boolean => ctx.scope === "essentials" && g.guardId === "owns-no-records" && g.status === "unknown";
  const toCheck = assessed.filter((a) => !clear.includes(a) && !heldBySkip(a) && a.guards.every((g) => g.status === "passed" || unreadOwnership(g)));
  const blocked = assessed.filter((a) => !clear.includes(a) && !heldBySkip(a) && !toCheck.includes(a));
  const segments = [
    { dimension: "licence", segments: new Set(candidates.map(licenceOf)).size },
    { dimension: "role", segments: new Set(candidates.map((u) => u.UserRole?.Name ?? "No role")).size },
  ];
  const manualStepIds = [manual.step.id];
  const label = (a: { user: UserRow }) => ({ type: "user", label: ctx.userLabel(a.user.Id) });
  const lastLogin = (u: UserRow): string => (u.LastLoginDate ? `Last logged in ${u.LastLoginDate.slice(0, 10)}` : "Never logged in");
  const records = (group: Array<{ user: UserRow; guards: Guard[] }>, fix: string, more: (a: { guards: Guard[] }) => string = () => ""): RecordList => ({
    object: "User",
    rows: group.map((a) => ({ id: a.user.Id, problem: `${lastLogin(a.user)}${more(a)}` })),
    fix,
  });
  const results: RuleResult[] = [];

  if (unanswered.length > 0) {
    results.push({
      outcome: "not_measurable",
      key: `${describe.key}-skipped`,
      reason: `Whether ${plural(unanswered.length, "user")} can be released can't be measured, because the Default Workflow User step was skipped: ${manual.answer?.skipReason}`,
      evidence: [manual.evidence],
      manualStepIds,
    });
  }

  if (clear.length > 0) {
    const value = seatValue(ctx, clear.map((a) => a.user));
    results.push({
      outcome: "issue",
      key: `${describe.key}-clear`,
      title: `Licences you can release: ${describe.label}`,
      summary: `Active users who ${describe.basis}: ${clear.length}. Every guard passed: no owned records, no references in automation, and nothing runs as them.`,
      recommendation: `Deactivate ${plural(clear.length, "user")} and release the ${clear.length === 1 ? "licence" : "licences"}.`,
      blockedRecommendation: "Check these users by hand before changing anything.",
      recommendationType: "deactivate",
      severity: "medium",
      confidence: "high",
      evidence: [usersEvidence],
      value,
      effort: { size: "XS" },
      owner: "Salesforce admin",
      dependencies: ["Confirm with each line manager that the person has left or moved role"],
      guards: clear.flatMap((a) => a.guards),
      segmentsChecked: segments,
      subjects: clear.map(label),
      facts: { users: clear.length, ...(value.kind === "licence_cost" ? { annualSaving: value.amount } : {}) },
      records: records(clear, "Confirm with their manager, then deactivate the user and release the licence"),
      manualStepIds,
    });
  }

  if (toCheck.length > 0) {
    const cost = seatValue(ctx, toCheck.map((a) => a.user));
    const priced = cost.kind === "licence_cost" ? ` Their seats cost ${money(cost.amount, ctx.calibration.currency)} a year.` : "";
    results.push({
      outcome: "issue",
      key: `${describe.key}-check-by-hand`,
      title: `Licences to check by hand: ${describe.label}`,
      summary: `Active users who ${describe.basis}: ${toCheck.length}. Every check we could make passed, but a set-up review doesn't read who owns which records.${priced}`,
      recommendation: "Check by hand that each of these users owns no records, then deactivate them and release the licences.",
      recommendationType: "investigate",
      severity: "low",
      confidence: "medium",
      evidence: [usersEvidence],
      // Not a saving until the check is done: an unread guard never counts as passed (rule 3).
      value: notSized(`Not counted as a saving until someone has checked by hand that these users own no records.${priced}`),
      effort: { size: "S" },
      owner: "Salesforce admin",
      dependencies: ["Confirm with each line manager that the person has left or moved role"],
      guards: toCheck.flatMap((a) => a.guards),
      segmentsChecked: segments,
      subjects: toCheck.map(label),
      facts: { users: toCheck.length, ...(cost.kind === "licence_cost" ? { seatCost: cost.amount } : {}) },
      records: records(toCheck, "Check by hand that they own no records, then deactivate the user and release the licence"),
      manualStepIds,
    });
  }

  if (blocked.length > 0) {
    const failed = blocked.filter((a) => a.guards.some((g) => g.status === "failed"));
    const incomplete = blocked.length - failed.length;
    // Only failed checks are named. A check we couldn't finish says nothing about the user, so it's counted instead.
    const blockers = new Map<string, number>();
    for (const a of failed) for (const id of new Set(a.guards.filter((g) => g.status === "failed").map((g) => g.guardId))) blockers.set(id, (blockers.get(id) ?? 0) + 1);
    const named = [...blockers].sort(([a, x], [b, y]) => y - x || a.localeCompare(b)).map(([id, n]) => `${USER_GUARD_BLOCKERS[id as keyof typeof USER_GUARD_BLOCKERS] ?? id} (${n})`);
    results.push({
      outcome: "issue",
      key: `${describe.key}-blocked`,
      title: `Licences that look unused but can't be released yet: ${describe.label}`,
      summary: `Active users who ${describe.basis}: ${blocked.length}. We aren't recommending removal yet.${named.length ? ` What's in the way: ${named.join(", ")}.` : ""}${incomplete ? ` For ${plural(incomplete, "user")}, we couldn't complete every check.` : ""}`,
      recommendation: "Work through each user's blockers. Reassign what they own and finish any open manual step before changing the account.",
      recommendationType: "investigate",
      severity: "low",
      confidence: "high",
      evidence: [usersEvidence],
      value: notSized("These seats can't be counted as savings until their guards pass."),
      effort: { size: "S" },
      owner: "Salesforce admin",
      guards: blocked.flatMap((a) => a.guards),
      segmentsChecked: segments,
      subjects: blocked.map(label),
      records: records(blocked, "Clear what's in the way, then look at the licence again", (a) => {
        const reasons = [...new Set(a.guards.filter((g) => g.status === "failed").map((g) => USER_GUARD_BLOCKERS[g.guardId as keyof typeof USER_GUARD_BLOCKERS] ?? g.guardId))];
        return reasons.length ? `. In the way: ${reasons.join(", ")}` : ". Not every check could be completed";
      }),
      facts: { users: blocked.length, failed: failed.length, incomplete },
      manualStepIds,
    });
  }
  return results;
}

const unassignedSeats: Rule = {
  id: "utl.unassigned-licence-seats",
  version: 1,
  domain: "utilisation",
  title: "Licences paid for but not assigned",
  evaluate(ctx): RuleResult | RuleResult[] {
    const { userLicenses } = ctx.data(licencesExtractor);
    if (userLicenses.status !== "ok") return { outcome: "not_measurable", reason: `Licences couldn't be read: ${userLicenses.reason}`, evidence: [ctx.ev(userLicenses.source, { count: null })] };
    const users = ctx.data(usersExtractor);
    const held = new Set(users.status === "ok" ? users.data.filter((u) => u.IsActive).map(licenceOf) : []);
    // Orgs list many free licence types. Only look at ones in use or given a price.
    // Some come free with the edition, so spare seats of those cost nothing unless
    // the calibration prices them. A real org showed 4 spare Salesforce Integration seats.
    const relevant = userLicenses.data.filter(
      (l) =>
        l.Status === "Active" &&
        l.TotalLicenses > 0 &&
        (ctx.calibration.licenceCosts[l.Name] !== undefined || (held.has(l.Name) && !COMPLIMENTARY_LICENCES.has(l.Name))),
    );
    const spare = relevant.filter((l) => {
      const free = l.TotalLicenses - l.UsedLicenses;
      return free >= MIN_SPARE_SEATS && pct(free, l.TotalLicenses) >= SPARE_SEAT_SHARE;
    });
    const evidence = [ctx.ev(userLicenses.source, { count: relevant.length, exclusions: ["Licence types with no active users and no price in the calibration"] })];
    const segments = [{ dimension: "licence", segments: relevant.length }];
    if (spare.length === 0) return { outcome: "pass", summary: `No licence type has ${SPARE_SEAT_SHARE}% or more of its seats unassigned.`, evidence, segmentsChecked: segments };
    const seats = spare.reduce((sum, l) => sum + l.TotalLicenses - l.UsedLicenses, 0);
    const costs = ctx.calibration.licenceCosts;
    const unpriced = spare.filter((l) => costs[l.Name] === undefined).map((l) => l.Name);
    const value: Value =
      unpriced.length > 0
        ? notSized(`The calibration has no cost for: ${unpriced.join(", ")}.`)
        : {
            kind: "licence_cost",
            amount: spare.reduce((sum, l) => sum + (l.TotalLicenses - l.UsedLicenses) * costs[l.Name]!, 0),
            unit: ctx.calibration.currency,
            period: "per year",
            assumptions: spare.map((l) => `${plural(l.TotalLicenses - l.UsedLicenses, `${l.Name} seat`)} at ${costs[l.Name]} ${ctx.calibration.currency} a year, from the calibration`).concat("Assumes seats can be reduced at renewal"),
          };
    return {
      outcome: "issue",
      title: "You're paying for seats nobody holds",
      summary: `Unassigned seats: ${seats}. ${spare.map((l) => `${l.TotalLicenses - l.UsedLicenses} of ${l.TotalLicenses} ${l.Name}`).join("; ")}.`,
      recommendation: "Reduce these seats at your next renewal, keeping whatever headroom your hiring plan needs.",
      recommendationType: "change",
      severity: "medium",
      confidence: "high",
      evidence,
      value,
      effort: { size: "XS" },
      owner: "Commercial owner of the Salesforce contract",
      dependencies: ["Salesforce contract renewal date"],
      segmentsChecked: segments,
      subjects: spare.map((l) => ({ type: "licence", label: l.Name })),
      facts: { seats, ...(value.kind === "licence_cost" ? { annualSaving: value.amount } : {}) },
    };
  },
};

function dormantRule(id: string, title: string, key: string, label: string, basisFor: (days: number) => string, isCandidate: (u: UserRow, cutoff: Date) => boolean): Rule {
  return {
    id,
    version: 1,
    domain: "utilisation",
    title,
    evaluate(ctx): RuleResult | RuleResult[] {
      const users = ctx.data(usersExtractor);
      if (users.status !== "ok") return { outcome: "not_measurable", reason: `Users couldn't be read: ${users.reason}`, evidence: [ctx.ev(users.source, { count: null })] };
      const days = ctx.calibration.dormantDays;
      const cutoff = daysBefore(ctx.now, days);
      const active = users.data.filter((u) => u.IsActive && new Date(u.CreatedDate) < cutoff && !isIntegrationLicence(licenceOf(u)));
      const candidates = active.filter((u) => isCandidate(u, cutoff));
      if (candidates.length > 0) return reclaimFindings(ctx, candidates, { key, label, basis: basisFor(days) });
      return {
        outcome: "pass",
        summary: `None of the ${active.length} active users ${basisFor(days)}.`,
        evidence: [ctx.ev(users.source, { count: active.length })],
        segmentsChecked: [
          { dimension: "licence", segments: new Set(active.map(licenceOf)).size },
          { dimension: "role", segments: new Set(active.map((u) => u.UserRole?.Name ?? "No role")).size },
        ],
      };
    },
  };
}

const dormantUsers = dormantRule(
  "utl.dormant-licensed-users",
  "Licensed users who've stopped logging in",
  "dormant",
  "dormant users",
  (days) => `haven't logged in for ${days} days`,
  (u, cutoff) => u.LastLoginDate !== null && new Date(u.LastLoginDate) < cutoff,
);

const neverLoggedIn = dormantRule(
  "utl.never-logged-in",
  "Licensed users who've never logged in",
  "never",
  "users who've never logged in",
  () => "have never logged in",
  (u) => u.LastLoginDate === null,
);

const unusedReports: Rule = {
  id: "utl.unused-reports",
  version: 2,
  domain: "utilisation",
  title: "Reports nobody runs",
  evaluate(ctx): RuleResult | RuleResult[] {
    const reports = ctx.data(reportsExtractor);
    if (reports.status !== "ok") return { outcome: "not_measurable", reason: `Reports couldn't be read: ${reports.reason}`, evidence: [ctx.ev(reports.source, { count: null })] };
    const cutoff = monthsBefore(ctx.now, UNUSED_REPORT_MONTHS);
    const settled = reports.data.filter((r) => new Date(r.CreatedDate) < daysBefore(ctx.now, RECENT_REPORT_DAYS));
    const exclusions = [`Reports created in the last ${RECENT_REPORT_DAYS} days`, "Private reports the connected user can't see"];
    if (settled.length < MIN_REPORTS) {
      return { outcome: "not_measurable", reason: `${settled.length} reports is below the ${MIN_REPORTS} needed for a usage rate to mean anything.`, evidence: [ctx.ev(reports.source, { count: settled.length, exclusions })] };
    }
    const isUnused = (r: { LastRunDate: string | null }): boolean => r.LastRunDate === null || new Date(r.LastRunDate) < cutoff;
    const unused = settled.filter(isUnused);
    const folders = new Set(settled.map((r) => r.FolderName ?? "No folder"));
    const share = pct(unused.length, settled.length);
    const range = { from: cutoff.toISOString().slice(0, 10), to: ctx.window.to };
    const evidence = [ctx.ev(reports.source, { count: unused.length, dateRange: range, exclusions })];
    const segments = [{ dimension: "folder", segments: folders.size }];
    if (share < UNUSED_REPORT_SHARE) return { outcome: "pass", summary: `${share}% of reports haven't been run in ${UNUSED_REPORT_MONTHS} months.`, evidence, segmentsChecked: segments, facts: { share } };
    return {
      outcome: "issue",
      title: "Most reports aren't being used",
      summary: `${unused.length} of ${settled.length} reports (${share}%) haven't been run in ${UNUSED_REPORT_MONTHS} months, so people will struggle to find the ones that matter.`,
      recommendation: "Agree a core report set by team and archive the rest to a clearly labelled folder.",
      recommendationType: "investigate",
      severity: "low",
      confidence: "medium",
      evidence,
      value: calibratedHours(ctx, "report-upkeep", unused.length, "unused report"),
      effort: { size: "S" },
      owner: "Sales operations",
      segmentsChecked: segments,
      facts: { unused: unused.length, reports: settled.length, share },
    };
  },
};

const dashboardsOnInactiveUsers: Rule = {
  id: "utl.dashboards-inactive-running-user",
  version: 1,
  domain: "utilisation",
  title: "Dashboards running as inactive users",
  evaluate(ctx): RuleResult | RuleResult[] {
    const { dashboards } = ctx.data(runAsExtractor);
    const users = ctx.data(usersExtractor);
    if (dashboards.status !== "ok") return { outcome: "not_measurable", reason: `Dashboards couldn't be read: ${dashboards.reason}`, evidence: [ctx.ev(dashboards.source, { count: null })] };
    if (users.status !== "ok") return { outcome: "not_measurable", reason: `Users couldn't be read: ${users.reason}`, evidence: [ctx.ev(users.source, { count: null })] };
    const inactive = new Set(users.data.filter((u) => !u.IsActive).map((u) => u.Id));
    const affected = dashboards.data.filter((d) => d.RunningUserId && inactive.has(d.RunningUserId));
    const evidence = [ctx.ev(dashboards.source, { count: affected.length })];
    const segments = [{ dimension: "dashboard type", segments: new Set(dashboards.data.map((d) => d.Type)).size }];
    if (affected.length === 0) return { outcome: "pass", summary: `None of the ${dashboards.data.length} dashboards run as an inactive user.`, evidence, segmentsChecked: segments };
    return {
      outcome: "issue",
      title: "Dashboards are running as people who've left",
      summary: `Dashboards running as an inactive user: ${affected.length} of ${dashboards.data.length}. They may show stale data or stop refreshing.`,
      recommendation: "Give these dashboards a current running user, or a dedicated reporting user.",
      recommendationType: "change",
      severity: "medium",
      confidence: "high",
      evidence,
      value: notSized("The cost is decisions made on stale figures, which we can't size."),
      effort: { size: "XS" },
      owner: "Salesforce admin",
      segmentsChecked: segments,
      subjects: affected.map((d) => ({ type: "dashboard", label: d.Title })),
      facts: { affected: affected.length, dashboards: dashboards.data.length },
    };
  },
};

// Rule 5, in a rule of its own, because the obvious version of this check is
// wrong. Dashboard.LastViewedDate is the connected user's own last view, not
// anyone's, so it would call a dashboard the whole team opens daily "unused".
const dashboardUsage: Rule = {
  id: "utl.dashboard-usage",
  version: 1,
  domain: "utilisation",
  title: "Dashboard use",
  evaluate(ctx): RuleResult | RuleResult[] {
    const { dashboards } = ctx.data(runAsExtractor);
    return {
      outcome: "not_measurable",
      reason: "We can't yet see how often your team opens dashboards. Salesforce only tells us when the person running the check last opened each one, which says nothing about anyone else.",
      evidence: [ctx.ev(dashboards.source, { count: dashboards.status === "ok" ? dashboards.data.length : null })],
    };
  },
};

// Salesforce only has the connection object where the feature is on. A customer
// review asked for this to be one plain line rather than the query error.
const EAC_NOT_ON = "Einstein Activity Capture isn't switched on in this org, or the connected user can't see it, so there's nothing to check.";
const eacNotOn = (rows: { status: string; reason?: string }): boolean => rows.status === "unavailable" && /isn't available in this org/.test(rows.reason ?? "");

const eacConnections: Rule = {
  id: "utl.eac-connections",
  version: 1,
  domain: "utilisation",
  title: "Einstein Activity Capture connection health",
  evaluate(ctx): RuleResult | RuleResult[] {
    const rows = ctx.data(activityCaptureExtractor);
    if (rows.status !== "ok") {
      const reason = eacNotOn(rows) ? EAC_NOT_ON : `Connection status couldn't be read: ${rows.reason}`;
      return { outcome: "not_measurable", reason, evidence: [ctx.ev(rows.source, { count: null })] };
    }
    if (rows.data.length === 0) return { outcome: "pass", summary: "No users are connected to Einstein Activity Capture.", evidence: [ctx.ev(rows.source, { count: 0 })], segmentExemptReason: "There are no connections to segment." };
    const broken = rows.data.filter((r) => !HEALTHY_CONNECTIONS.has(r.ConnectivityStatus ?? ""));
    const statuses = new Set(rows.data.map((r) => r.ConnectivityStatus ?? "Unknown"));
    const evidence = [ctx.ev(rows.source, { count: rows.data.length })];
    const segments = [{ dimension: "connection status", segments: statuses.size }];
    if (broken.length === 0) return { outcome: "pass", summary: `All ${rows.data.length} connected users are healthy.`, evidence, segmentsChecked: segments };
    return {
      outcome: "issue",
      title: "Some users' email and calendar aren't syncing",
      summary: `Connected users in a state that stops activity being captured: ${broken.length} of ${rows.data.length}.`,
      recommendation: "Reconnect these users. Salesforce shows a recommended action against each one.",
      recommendationType: "change",
      severity: "high",
      confidence: "high",
      evidence,
      value: notSized("Lost activity weakens the pipeline view, and we can't tell which deals it belonged to."),
      effort: { size: "XS" },
      owner: "Salesforce admin",
      segmentsChecked: segments,
      subjects: broken.map((r) => ({ type: "user", label: ctx.userLabel(r.UserId) })),
      facts: { broken: broken.length, connected: rows.data.length },
    };
  },
};

const eacAdvancedSync: Rule = {
  id: "utl.eac-advanced-sync",
  version: 1,
  domain: "utilisation",
  title: "Einstein Activity Capture advanced sync settings",
  evaluate(ctx): RuleResult | RuleResult[] {
    const rows = ctx.data(activityCaptureExtractor);
    if (rows.status === "ok" && rows.data.length === 0) {
      return { outcome: "pass", summary: "Nobody is connected to Einstein Activity Capture, so these settings don't apply.", evidence: [ctx.ev(rows.source, { count: 0 })], segmentExemptReason: "The feature isn't in use." };
    }
    if (eacNotOn(rows)) return { outcome: "not_measurable", reason: EAC_NOT_ON, evidence: [ctx.ev(rows.source, { count: null })] };
    const manual = ctx.manual("eac-advanced-sync");
    const manualStepIds = [manual.step.id];
    if (manual.status !== "answered") {
      const reason = manual.status === "skipped" ? `The manual step was skipped: ${manual.answer?.skipReason}` : "This setting can't be read by API, and the manual step hasn't been answered yet.";
      return { outcome: "not_measurable", reason, evidence: [manual.evidence], manualStepIds };
    }
    const relate = manual.answer?.values.relate_synced_events === true;
    if (relate) {
      return { outcome: "pass", summary: "Relate Synced Events is on.", evidence: [manual.evidence], segmentExemptReason: "A single org-level setting, read by a consultant.", manualStepIds };
    }
    return {
      outcome: "issue",
      title: "Synced calendar events may not be reaching your deals",
      summary: "Relate Synced Events is off, so events can sync without being related to the records they belong to. Total event volume can look normal while the link to deals is missing.",
      recommendation: "Turn Relate Synced Events on, then check that new events link to records.",
      recommendationType: "change",
      severity: "high",
      confidence: "high",
      evidence: [manual.evidence],
      value: notSized("Lost activity weakens the pipeline view, and we can't tell which deals it belonged to."),
      effort: { size: "XS" },
      owner: "Salesforce admin",
      segmentsChecked: [{ dimension: "configuration", segments: 1, note: "Read by a consultant, for the configuration most sales users are on" }],
      manualStepIds,
    };
  },
};

export const utilisationModule: DomainModule = {
  id: "utilisation",
  cloud: "sales",
  label: "Utilisation",
  extractors: [
    usersExtractor,
    licencesExtractor,
    fieldDescribeExtractor,
    loginsExtractor,
    recordCountsExtractor,
    ownershipExtractor,
    metadataExtractor,
    apexExtractor,
    scheduledJobsExtractor,
    runAsExtractor,
    reportsExtractor,
    activityCaptureExtractor,
    orgExtractor,
  ],
  metrics: [investmentMetrics],
  rules: [unassignedSeats, dormantUsers, neverLoggedIn, unusedReports, dashboardsOnInactiveUsers, dashboardUsage, eacConnections, eacAdvancedSync],
};
