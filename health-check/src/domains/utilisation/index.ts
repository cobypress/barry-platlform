import { fieldDescribeExtractor } from "../../extract/fields.ts";
import { each, named as valued } from "../../engine/named.ts";
import { apexExtractor, metadataExtractor, scheduledJobsExtractor } from "../../extract/automation.ts";
import { componentChangesExtractor } from "../../extract/component-changes.ts";
import { orgExtractor, recordCountsExtractor, schemaExtractor } from "../../extract/configuration.ts";
import { opportunitiesExtractor, stagesExtractor } from "../../extract/opportunities.ts";
import { daysBefore } from "../../extract/types.ts";
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
import { judged, lapse, lapseByDate, onIntegrationLicence, type Lapse } from "../logins.ts";
import { REPORT_RECENT_DAYS } from "../adoption/index.ts";
import { calibratedHours } from "../value.ts";
import { labelsOf } from "../labels.ts";
import { labelsExtractor } from "../../extract/labels.ts";
import { COMPLIMENTARY_LICENCES } from "../../money/licences.ts";
import { investmentMetrics } from "./investment.ts";
import { WHERE } from "../where.ts";
import { eng, joinSaid, org, said, type Said } from "../../engine/org-words.ts";
import { moneySaid, seatsSaid } from "../../engine/said-forms.ts";

// Every title, summary, recommendation and reason of our own is written with
// `said`, and every value the org chose (a licence, an installed app's name, a
// skip reason someone typed) with `org` (DECISIONS.md 655, 657). A reason passed
// on from an extract quotes Salesforce and stays plain: unknown.

const MIN_SPARE_SEATS = 2;
const SPARE_SEAT_SHARE = 10;
const UNUSED_REPORT_SHARE = 50;
const MIN_REPORTS = 50;
const RECENT_REPORT_DAYS = 90;
const HEALTHY_CONNECTIONS = new Set(["ACTIVE", "PROCESSING", "INITIALIZING"]);

const licenceOf = (user: UserRow): string => user.Profile?.UserLicense?.Name ?? "Unknown";

/** Names in a list, ", " between them, each the org's words. */
const namesSaid = (names: readonly string[]): Said => joinSaid(names.map((n) => said`${org(n)}`), ", ");

function seatValue(ctx: RuleContext, users: UserRow[]): Value {
  const costs = ctx.calibration.licenceCosts;
  const unpriced = [...new Set(users.map(licenceOf))].filter((l) => costs[l] === undefined);
  if (unpriced.length > 0) return notSized(said`The calibration has no cost for: ${namesSaid(unpriced.sort())}.`);
  const byLicence = new Map<string, number>();
  for (const user of users) byLicence.set(licenceOf(user), (byLicence.get(licenceOf(user)) ?? 0) + 1);
  return {
    kind: "licence_cost",
    amount: users.reduce((sum, u) => sum + costs[licenceOf(u)]!, 0),
    unit: ctx.calibration.currency,
    period: "per year",
    assumptions: [...byLicence].sort().map(([licence, n]) => valued(said`${seatsSaid(n, licence)} at ${costs[licence]!} ${org(ctx.calibration.currency)} a year, from the calibration`, { costs: costs[licence]! })),
  };
}

// Splits candidates on the guards. The engine would downgrade a mixed finding
// anyway, but this way the clean users keep a real recommendation.
/** What the reclaim findings say of their users: our own words, and a note ending each issue's summary. */
type Reclaim = { key: string; label: Said; basis: Said; note: Said };

function reclaimFindings(ctx: RuleContext, candidates: UserRow[], describe: Reclaim): RuleResult[] {
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
      // An automatic run asks no one, so there's no person's reason to quote (DECISIONS.md 693).
      reason: manual.answer
        ? valued(said`Whether ${plural(unanswered.length, "user")} can be released can't be measured, because the Default Workflow User step was skipped: ${org(String(manual.answer.skipReason))}`, { unanswered: unanswered.length })
        : valued(said`Whether ${plural(unanswered.length, "user")} can be released can't be measured, because the Default Workflow User can't be read by API, and on an automatic run nobody is asked for it.`, { unanswered: unanswered.length }),
      evidence: [manual.evidence],
      manualStepIds,
    });
  }

  if (clear.length > 0) {
    const value = seatValue(ctx, clear.map((a) => a.user));
    results.push({
      outcome: "issue",
      key: `${describe.key}-clear`,
      title: said`Licences you can release: ${describe.label}`,
      summary: valued(said`Active users who ${describe.basis}: ${clear.length}. Every guard passed: no owned records, no references in automation, and nothing runs as them.${describe.note}`, { clear: clear.length }),
      recommendation: valued(said`Deactivate ${plural(clear.length, "user")} and release the ${clear.length === 1 ? "licence" : "licences"}.`, { clear: clear.length }),
      blockedRecommendation: said`Check these users by hand before changing anything.`,
      recommendationType: "deactivate",
      // The seats' cost when the calibration prices them: with no price and a
      // guard blocking, it's the guard that keeps a figure off (DECISIONS.md 498).
      sizesASaving: true,
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
    const priced = cost.kind === "licence_cost" ? valued(said` Their seats cost ${moneySaid(cost.amount, ctx.calibration.currency)} a year.`, { amount: [cost.amount, "money"] }) : said``;
    results.push({
      outcome: "issue",
      key: `${describe.key}-check-by-hand`,
      title: said`Licences to check by hand: ${describe.label}`,
      summary: valued(said`Active users who ${describe.basis}: ${toCheck.length}. Every check we could make passed, but a set-up review doesn't read who owns which records.${priced}${describe.note}`, { toCheck: toCheck.length }),
      recommendation: said`Check by hand that each of these users owns no records, then deactivate them and release the licences.`,
      recommendationType: "investigate",
      severity: "low",
      confidence: "medium",
      evidence: [usersEvidence],
      // Not a saving until the check is done: an unread guard never counts as passed (rule 3).
      value: notSized(said`Not counted as a saving until someone has checked by hand that these users own no records.${priced}`),
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
    const named = [...blockers].sort(([a, x], [b, y]) => y - x || a.localeCompare(b)).map(([id, n]) => {
      // The guards' own phrases are ours; an id the table doesn't know is printed as the org's words, to be safe.
      // Own keys only, so an id such as "constructor" can't reach the object's prototype and pass as ours.
      const phrase: string | undefined = Object.hasOwn(USER_GUARD_BLOCKERS, id) ? USER_GUARD_BLOCKERS[id as keyof typeof USER_GUARD_BLOCKERS] : undefined;
      return valued(said`${phrase === undefined ? org(id) : eng(phrase)} (${n})`, { n });
    });
    results.push({
      outcome: "issue",
      key: `${describe.key}-blocked`,
      title: said`Licences that look unused but can't be released yet: ${describe.label}`,
      summary: valued(said`Active users who ${describe.basis}: ${blocked.length}. We aren't recommending removal yet.${named.length ? said` What's in the way: ${joinSaid(named, ", ")}.` : ""}${incomplete ? said` For ${plural(incomplete, "user")}, we couldn't complete every check.` : ""}${describe.note}`, { blocked: blocked.length }),
      recommendation: said`Work through each user's blockers. Reassign what they own and finish any open manual step before changing the account.`,
      recommendationType: "investigate",
      severity: "low",
      confidence: "high",
      evidence: [usersEvidence],
      value: notSized(said`These seats can't be counted as savings until their guards pass.`),
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
  where: WHERE.companyInformation,
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
    if (spare.length === 0) return { outcome: "pass", summary: valued(said`No licence type has ${SPARE_SEAT_SHARE}% or more of its seats unassigned.`, { spareSeatShare: [SPARE_SEAT_SHARE, "percent"] }), evidence, segmentsChecked: segments };
    const seats = spare.reduce((sum, l) => sum + l.TotalLicenses - l.UsedLicenses, 0);
    const costs = ctx.calibration.licenceCosts;
    const unpriced = spare.filter((l) => costs[l.Name] === undefined).map((l) => l.Name);
    const value: Value =
      unpriced.length > 0
        ? notSized(said`The calibration has no cost for: ${namesSaid(unpriced)}.`)
        : {
            kind: "licence_cost",
            amount: spare.reduce((sum, l) => sum + (l.TotalLicenses - l.UsedLicenses) * costs[l.Name]!, 0),
            unit: ctx.calibration.currency,
            period: "per year",
            assumptions: spare.map((l) => valued(said`${seatsSaid(l.TotalLicenses - l.UsedLicenses, l.Name)} at ${costs[l.Name]!} ${org(ctx.calibration.currency)} a year, from the calibration`, { costs: [costs[l.Name]!, "money"] })).concat(said`Assumes seats can be reduced at renewal`),
          };
    // Each licence's free seats, worked out once for its sentence and the value it's named with (DECISIONS.md 768).
    const freeSeats = spare.map((l) => ({ l, free: l.TotalLicenses - l.UsedLicenses }));
    return {
      outcome: "issue",
      title: said`You're paying for seats nobody holds`,
      summary: valued(said`Unassigned seats: ${seats}. ${joinSaid(freeSeats.map(({ l, free }) => valued(said`${free} of ${l.TotalLicenses} ${org(l.Name)}`, { free, TotalLicenses: l.TotalLicenses })), "; ")}.`, each(freeSeats, (r) => ({ spare: r.free, of: r.l.TotalLicenses }))),
      recommendation: said`Reduce these seats at your next renewal, keeping whatever headroom your hiring plan needs.`,
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


/** How the calibration prices an installed app's seat: by its namespace, as "package:<namespace>" (DECISIONS.md 203). */
export const packagePriceKey = (namespace: string): string => `package:${namespace}`;

// Seats bought for installed apps (roadmap 3.2). A site licence (-1 seats), a free
// app and a trial aren't seats anyone pays for one by one, so they're left out.
const packageSeats: Rule = {
  id: "utl.package-seats",
  version: 2,
  domain: "utilisation",
  title: "Installed apps' seats nobody holds",
  where: WHERE.installedPackages,
  evaluate(ctx): RuleResult | RuleResult[] {
    const { packageLicenses } = ctx.data(licencesExtractor);
    if (packageLicenses.status !== "ok") return { outcome: "not_measurable", reason: `Installed packages' licences couldn't be read: ${packageLicenses.reason}`, evidence: [ctx.ev(packageLicenses.source, { count: null })] };
    const seated = packageLicenses.data.filter((p) => p.Status === "Active" && p.AllowedLicenses > 0).sort((a, b) => (a.NamespacePrefix ?? "").localeCompare(b.NamespacePrefix ?? ""));
    // By the package's name where it can be read; a licence record holds only the namespace (DECISIONS.md 413).
    const labels = labelsOf(ctx);
    const name = (p: { NamespacePrefix: string | null }): string => (p.NamespacePrefix ? labels.package(p.NamespacePrefix) : "An app with no namespace");
    /** The same name in a sentence: the package's name or namespace is the org's. */
    const named = (p: { NamespacePrefix: string | null }): Said => (p.NamespacePrefix ? said`${org(labels.package(p.NamespacePrefix))}` : said`An app with no namespace`);
    const unused = seated.filter((p) => p.UsedLicenses === 0);
    const spare = seated.filter((p) => p.UsedLicenses > 0 && p.AllowedLicenses - p.UsedLicenses >= MIN_SPARE_SEATS && pct(p.AllowedLicenses - p.UsedLicenses, p.AllowedLicenses) >= SPARE_SEAT_SHARE);
    const evidence = [ctx.ev(packageLicenses.source, { count: seated.length, exclusions: ["Site licences", "Free, trial and expired apps"], note: `Licence records by namespace: ${seated.map((p) => p.NamespacePrefix ?? "none").join(", ")}. Named from the installed packages, where they could be read` })];
    const segments = [{ dimension: "installed app", segments: seated.length }];
    if (unused.length + spare.length === 0) {
      return seated.length
        ? { outcome: "pass", summary: valued(said`All ${plural(seated.length, "installed app")} with seats ${seated.length === 1 ? "has" : "have"} them assigned, with fewer than ${SPARE_SEAT_SHARE}% spare.`, { seated: seated.length, spareSeatShare: [SPARE_SEAT_SHARE, "percent"] }), evidence, segmentsChecked: segments }
        : { outcome: "pass", summary: said`No installed app is licensed by the seat.`, evidence, segmentExemptReason: "There are no seated apps to segment." };
    }
    const idle = (p: (typeof seated)[number]): number => p.AllowedLicenses - p.UsedLicenses;
    const flagged = [...unused, ...spare];
    // A figure only where the calibration prices the app's seat (DECISIONS.md 203).
    const unpriced = flagged.filter((p) => !p.NamespacePrefix || ctx.calibration.licenceCosts[packagePriceKey(p.NamespacePrefix)] === undefined).map(named);
    const price = (p: (typeof seated)[number]): number => ctx.calibration.licenceCosts[packagePriceKey(p.NamespacePrefix!)]!;
    const value: Value = unpriced.length
      ? notSized(said`The calibration has no price for a seat of: ${joinSaid(unpriced, ", ")}.`)
      : {
          kind: "licence_cost",
          amount: flagged.reduce((sum, p) => sum + idle(p) * price(p), 0),
          unit: ctx.calibration.currency,
          period: "per year",
          assumptions: flagged.map((p) => said`${idle(p)} ${named(p)} ${idle(p) === 1 ? "seat" : "seats"} at ${price(p)} ${org(ctx.calibration.currency)} a year, from the calibration`).concat(said`Assumes seats can be reduced, or the app cancelled, at renewal`),
        };
    const idleSeats = spare.map((p) => ({ p, n: idle(p) }));
    const parts = [
      unused.length ? said`${joinSaid(unused.map((p) => valued(said`${named(p)} (${plural(p.AllowedLicenses, "seat")})`, { AllowedLicenses: p.AllowedLicenses })), ", ")} ${unused.length === 1 ? "has" : "have"} nobody assigned at all` : null,
      spare.length ? valued(said`${joinSaid(idleSeats.map(({ p, n }) => valued(said`${named(p)} has ${n} of ${p.AllowedLicenses} seats unassigned`, { n, AllowedLicenses: p.AllowedLicenses })), "; ")}`, each(idleSeats, (r) => ({ idle: r.n, of: r.p.AllowedLicenses }))) : null,
    ].filter((part): part is Said => part !== null);
    return {
      outcome: "issue",
      title: unused.length ? said`Some installed apps aren't used by anyone` : said`You're paying for app seats nobody holds`,
      summary: said`${joinSaid(parts, ". ")}.`,
      recommendation: said`Ask each app's owner whether it's still needed. Cancel the apps nobody uses, and reduce spare seats at renewal.`,
      recommendationType: "change",
      severity: unused.length ? "medium" : "low",
      confidence: "high",
      evidence,
      value,
      effort: { size: "XS" },
      owner: "Commercial owner of the Salesforce contract",
      dependencies: ["Each app's renewal date", "Check whether any automation or integration depends on an app before cancelling it"],
      segmentsChecked: segments,
      subjects: flagged.map((p) => ({ type: "installed app", label: name(p) })),
      facts: { unusedApps: unused.length, spareSeats: spare.reduce((sum, p) => sum + idle(p), 0), apps: seated.length, ...(value.kind === "licence_cost" ? { annualSaving: value.amount } : {}) },
    };
  },
};

function dormantRule(
  id: string,
  title: string,
  key: Lapse,
  label: Said,
  basisFor: (days: number) => Said,
  /** The sentence for users the last-login date counts but the login log doesn't. */
  shownOtherwise: (n: number, days: number) => Said,
): Rule {
  return {
    id,
    version: 2,
    domain: "utilisation",
    title,
    where: WHERE.users,
    evaluate(ctx): RuleResult | RuleResult[] {
      const users = ctx.data(usersExtractor);
      if (users.status !== "ok") return { outcome: "not_measurable", reason: `Users couldn't be read: ${users.reason}`, evidence: [ctx.ev(users.source, { count: null })] };
      const days = ctx.calibration.dormantDays;
      const cutoff = daysBefore(ctx.now, days);
      // The inventory's holders count who has stopped logging in from the same users (DECISIONS.md 735).
      const active = users.data.filter((u) => u.IsActive && judged(u, cutoff) && !onIntegrationLicence(u));
      // The login log is the fuller record: a user it shows logging in inside the
      // period hasn't stopped, whatever LastLoginDate says, so isn't counted (DECISIONS.md 361).
      const logins = ctx.data(loginsExtractor);
      const byDate = active.filter((u) => lapseByDate(u, cutoff) === key);
      const candidates = byDate.filter((u) => lapse(u, logins, cutoff) === key);
      const otherwise = byDate.length - candidates.length;
      const note = otherwise ? said` ${shownOtherwise(otherwise, days)}` : said``;
      if (candidates.length > 0) {
        // Each issue's summary ends with the note, as it's written there.
        return reclaimFindings(ctx, candidates, { key, label, basis: basisFor(days), note });
      }
      return {
        outcome: "pass",
        summary: valued(said`None of the ${active.length} active users ${basisFor(days)}.${note}`, { active: active.length }),
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
  said`dormant users`,
  (days) => valued(said`haven't logged in for ${days} days`, { dormantDays: days }),
  (n, days) => valued(said`${n === 1 ? "One more user's" : said`${n} more users'`} last-login date is over ${days} days old, but the login log shows ${n === 1 ? "them" : "each of them"} logging in since, so they aren't counted.`, { days: [days, "day"] }),
);

const neverLoggedIn = dormantRule(
  "utl.never-logged-in",
  "Licensed users who've never logged in",
  "never",
  said`users who've never logged in`,
  () => said`have never logged in`,
  (n) => said`${n === 1 ? "One more user has" : said`${n} more users have`} no last-login date, but the login log shows ${n === 1 ? "them" : "each of them"} logging in, so they aren't counted.`,
);

const unusedReports: Rule = {
  id: "utl.unused-reports",
  version: 3,
  domain: "utilisation",
  title: "Reports nobody runs",
  evaluate(ctx): RuleResult | RuleResult[] {
    const reports = ctx.data(reportsExtractor);
    if (reports.status !== "ok") return { outcome: "not_measurable", reason: `Reports couldn't be read: ${reports.reason}`, evidence: [ctx.ev(reports.source, { count: null })] };
    // The period the report-use metric defines, so the issue and the definition beside it agree (night 3 reading pass, item 21).
    const cutoff = daysBefore(ctx.now, REPORT_RECENT_DAYS);
    const settled = reports.data.filter((r) => new Date(r.CreatedDate) < daysBefore(ctx.now, RECENT_REPORT_DAYS));
    const exclusions = [`Reports created in the last ${RECENT_REPORT_DAYS} days`, "Private reports the connected user can't see"];
    if (settled.length < MIN_REPORTS) {
      return { outcome: "not_measurable", reason: valued(said`${settled.length} reports is below the ${MIN_REPORTS} needed for a usage rate to mean anything.`, { settled: settled.length, minReports: MIN_REPORTS }), evidence: [ctx.ev(reports.source, { count: settled.length, exclusions })] };
    }
    const isUnused = (r: { LastRunDate: string | null }): boolean => r.LastRunDate === null || new Date(r.LastRunDate) < cutoff;
    const unused = settled.filter(isUnused);
    const folders = new Set(settled.map((r) => r.FolderName ?? "No folder"));
    const share = pct(unused.length, settled.length);
    const range = { from: cutoff.toISOString().slice(0, 10), to: ctx.window.to };
    const evidence = [ctx.ev(reports.source, { count: unused.length, dateRange: range, exclusions })];
    const segments = [{ dimension: "folder", segments: folders.size }];
    if (share < UNUSED_REPORT_SHARE) return { outcome: "pass", summary: valued(said`${share}% of reports haven't been run in ${REPORT_RECENT_DAYS} days.`, { share: [share, "percent"], reportRecentDays: [REPORT_RECENT_DAYS, "day"] }), evidence, segmentsChecked: segments, facts: { share } };
    return {
      outcome: "issue",
      title: said`Most reports aren't being used`,
      summary: valued(said`${unused.length} of ${settled.length} reports (${share}%) haven't been run in ${REPORT_RECENT_DAYS} days, so people will struggle to find the ones that matter.`, { reportRecentDays: REPORT_RECENT_DAYS }),
      recommendation: said`Agree a core report set by team and archive the rest to a clearly labelled folder.`,
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
    if (affected.length === 0) return { outcome: "pass", summary: valued(said`None of the ${dashboards.data.length} dashboards run as an inactive user.`, { data: dashboards.data.length }), evidence, segmentsChecked: segments };
    return {
      outcome: "issue",
      title: said`Dashboards are running as people who've left`,
      summary: valued(said`Dashboards running as an inactive user: ${affected.length} of ${dashboards.data.length}. They may show stale data or stop refreshing.`, { affected: affected.length, data: dashboards.data.length }),
      recommendation: said`Give these dashboards a current running user, or a dedicated reporting user.`,
      recommendationType: "change",
      severity: "medium",
      confidence: "high",
      evidence,
      value: notSized(said`The cost is decisions made on stale figures, which we can't size.`),
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
      reason: said`We can't yet see how often your team opens dashboards. Salesforce only tells us when the person running the check last opened each one, which says nothing about anyone else.`,
      evidence: [ctx.ev(dashboards.source, { count: dashboards.status === "ok" ? dashboards.data.length : null })],
    };
  },
};

// Salesforce only has the connection object where the feature is on. A customer
// review asked for this to be one plain line rather than the query error.
// Made in the analysis that uses it, so the analysis's ledger holds it (DECISIONS.md 655).
const eacNotOnReason = (): Said => said`Einstein Activity Capture isn't switched on in this org, or the connected user can't see it, so there's nothing to check.`;
const eacNotOn = (rows: { status: string; reason?: string }): boolean => rows.status === "unavailable" && /isn't available in this org/.test(rows.reason ?? "");

const eacConnections: Rule = {
  id: "utl.eac-connections",
  version: 1,
  domain: "utilisation",
  title: "Einstein Activity Capture connection health",
  evaluate(ctx): RuleResult | RuleResult[] {
    const rows = ctx.data(activityCaptureExtractor);
    if (rows.status !== "ok") {
      const reason = eacNotOn(rows) ? eacNotOnReason() : `Connection status couldn't be read: ${rows.reason}`;
      return { outcome: "not_measurable", reason, evidence: [ctx.ev(rows.source, { count: null })] };
    }
    if (rows.data.length === 0) return { outcome: "pass", summary: said`No users are connected to Einstein Activity Capture.`, evidence: [ctx.ev(rows.source, { count: 0 })], segmentExemptReason: "There are no connections to segment." };
    const broken = rows.data.filter((r) => !HEALTHY_CONNECTIONS.has(r.ConnectivityStatus ?? ""));
    const statuses = new Set(rows.data.map((r) => r.ConnectivityStatus ?? "Unknown"));
    const evidence = [ctx.ev(rows.source, { count: rows.data.length })];
    const segments = [{ dimension: "connection status", segments: statuses.size }];
    if (broken.length === 0) return { outcome: "pass", summary: valued(said`All ${rows.data.length} connected users are healthy.`, { data: rows.data.length }), evidence, segmentsChecked: segments };
    return {
      outcome: "issue",
      title: said`Some users' email and calendar aren't syncing`,
      summary: valued(said`Connected users in a state that stops activity being captured: ${broken.length} of ${rows.data.length}.`, { broken: broken.length, data: rows.data.length }),
      recommendation: said`Reconnect these users. Salesforce shows a recommended action against each one.`,
      recommendationType: "change",
      severity: "high",
      confidence: "high",
      evidence,
      value: notSized(said`Lost activity weakens the pipeline view, and we can't tell which deals it belonged to.`),
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
      return { outcome: "pass", summary: said`Nobody is connected to Einstein Activity Capture, so these settings don't apply.`, evidence: [ctx.ev(rows.source, { count: 0 })], segmentExemptReason: "The feature isn't in use." };
    }
    if (eacNotOn(rows)) return { outcome: "not_measurable", reason: eacNotOnReason(), evidence: [ctx.ev(rows.source, { count: null })] };
    const manual = ctx.manual("eac-advanced-sync");
    const manualStepIds = [manual.step.id];
    if (manual.status !== "answered") {
      const reason =
        manual.status !== "skipped"
          ? said`This setting can't be read by API, and the manual step hasn't been answered yet.`
          : manual.answer
            ? said`The manual step was skipped: ${org(String(manual.answer.skipReason))}`
            : // An automatic run asks no one (DECISIONS.md 693): the step's own ifSkipped, said as a reason.
              said`This setting can't be read by API, and on an automatic run nobody is asked for it, so activity capture configuration is reported as not measurable.`;
      return { outcome: "not_measurable", reason, evidence: [manual.evidence], manualStepIds };
    }
    const relate = manual.answer?.values.relate_synced_events === true;
    if (relate) {
      return { outcome: "pass", summary: said`Relate Synced Events is on.`, evidence: [manual.evidence], segmentExemptReason: "A single org-level setting, read by a consultant.", manualStepIds };
    }
    return {
      outcome: "issue",
      title: said`Synced calendar events may not be reaching your deals`,
      summary: said`Relate Synced Events is off, so events can sync without being related to the records they belong to. Total event volume can look normal while the link to deals is missing.`,
      recommendation: said`Turn Relate Synced Events on, then check that new events link to records.`,
      recommendationType: "change",
      severity: "high",
      confidence: "high",
      evidence: [manual.evidence],
      value: notSized(said`Lost activity weakens the pipeline view, and we can't tell which deals it belonged to.`),
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
  // The investment metrics gate on the opportunities read (DECISIONS.md 37), which
  // plans its SELECT from the schema and winds deals back with the stages
  // (DECISIONS.md 465), so all three are listed and this module runs alone.
  extractors: [
    labelsExtractor,
    usersExtractor,
    licencesExtractor,
    schemaExtractor,
    stagesExtractor,
    opportunitiesExtractor,
    fieldDescribeExtractor,
    loginsExtractor,
    recordCountsExtractor,
    ownershipExtractor,
    metadataExtractor,
    apexExtractor,
    componentChangesExtractor,
    scheduledJobsExtractor,
    runAsExtractor,
    reportsExtractor,
    activityCaptureExtractor,
    orgExtractor,
  ],
  metrics: [investmentMetrics],
  rules: [unassignedSeats, packageSeats, dormantUsers, neverLoggedIn, unusedReports, dashboardsOnInactiveUsers, dashboardUsage, eacConnections, eacAdvancedSync],
};
