import { loginActivityExtractor, type LoginActivity, type LoginChannel } from "../../extract/adoption.ts";
import { daysBefore } from "../../extract/types.ts";
import { reportsExtractor, usersExtractor, type UserRow } from "../../extract/users.ts";
import { notSized, pct, plural, type Rule, type RuleContext, type RuleResult } from "../../engine/rule.ts";
import type { Evidence, Metric, MetricSegment, SeriesPoint } from "../../schemas/index.ts";
import { opportunitiesGate } from "../gate.ts";
import type { DomainModule } from "../types.ts";

// Roadmap 3.2: whether people use Salesforce, and how. Everything here is read
// from set-up (users, login history, report definitions), never from CRM
// records, so a set-up review runs it too.

const DOMAIN = "adoption";
type Ctx = Omit<RuleContext, "metrics">;

/** A group needs this many people before its own figures are shown or judged. */
export const MIN_GROUP = 5;
/** Monthly active users this far below their peak, three months or more before, is falling adoption. */
export const FALL_SHARE = 20;
export const FALL_SPAN_MONTHS = 3;
/** Fewer login days than this in the last 28 is barely logging in: less than once a week. */
export const BARELY_DAYS = 4;
export const RECENT_DAYS = 28;
/** A group where more than this share barely log in is a problem. */
export const BARELY_SHARE = 50;
export const REPORT_RECENT_DAYS = 90;

const NON_PERSON_KINDS = new Set(["shared", "system", "integration", "test"]);
const profileOf = (u: UserRow): string => u.Profile?.Name ?? "No profile";
const roleOf = (u: UserRow): string => u.UserRole?.Name ?? "No role";

/** The people we measure: active, not a shared, system, integration or test account, and not on an integration licence. */
function people(ctx: Ctx, users: UserRow[]): UserRow[] {
  const nonPerson = new Set(ctx.calibration.users.filter((u) => NON_PERSON_KINDS.has(u.kind)).map((u) => u.userId.slice(0, 15)));
  return users.filter((u) => u.IsActive && !nonPerson.has(u.Id.slice(0, 15)) && !/\bintegration\b/i.test(u.Profile?.UserLicense?.Name ?? ""));
}

const dayMs = 86_400_000;
const isoDay = (d: Date): string => d.toISOString().slice(0, 10);
const monthKey = (day: string): string => day.slice(0, 7);
const addUtcMonths = (month: string, n: number): string => {
  const [y, m] = month.split("-").map(Number) as [number, number];
  const d = new Date(Date.UTC(y, m - 1 + n, 1));
  return d.toISOString().slice(0, 7);
};

/**
 * The whole calendar months the login history covers. Salesforce keeps it for a
 * limited time, and a capped read loses the oldest rows, so the first month is
 * the first one wholly after the oldest login we read, unless that login is
 * within a week of the window's start and nothing was cut off.
 */
export function coveredMonths(activity: LoginActivity, now: Date): string[] {
  if (!activity.earliest) return [];
  const from = new Date(`${activity.from}T00:00:00Z`);
  const earliest = new Date(activity.earliest);
  const fullFromWindow = !activity.truncated && earliest.getTime() - from.getTime() <= 7 * dayMs;
  const start = fullFromWindow ? from : earliest;
  // The first month that starts on or after the start of the data.
  let month = isoDay(start).endsWith("-01") ? monthKey(isoDay(start)) : addUtcMonths(monthKey(isoDay(start)), 1);
  const current = monthKey(isoDay(now));
  const months: string[] = [];
  while (month < current) {
    months.push(month);
    month = addUtcMonths(month, 1);
  }
  return months;
}

type Group = { dimension: string; key: string; users: UserRow[] };

function groupsOf(users: UserRow[]): Group[] {
  const by = (dimension: string, keyOf: (u: UserRow) => string): Group[] => {
    const map = new Map<string, UserRow[]>();
    for (const u of users) map.set(keyOf(u), [...(map.get(keyOf(u)) ?? []), u]);
    return [...map].sort(([a], [b]) => a.localeCompare(b)).map(([key, members]) => ({ dimension, key, users: members }));
  };
  return [...by("profile", profileOf), ...by("role", roleOf)];
}

/** Distinct people with a login on any of these days. */
const activeOn = (activity: LoginActivity, users: UserRow[], days: (day: string) => boolean): number => users.filter((u) => (activity.days[u.Id] ?? []).some(days)).length;

/**
 * QUESTIONS.md 55: without Monitor Login History, the connection sees only its
 * own logins, and everyone else would look inactive. When fewer than half the
 * people Salesforce says logged in during the window have a login in the
 * history, it isn't the whole org's, and nothing is built from it.
 */
export function partialHistory(users: UserRow[], activity: LoginActivity): string | null {
  const from = new Date(`${activity.from}T00:00:00Z`);
  const expected = users.filter((u) => u.LastLoginDate && new Date(u.LastLoginDate.replace(/\+0000$/, "Z")) >= from).length;
  const seen = users.filter((u) => activity.days[u.Id]).length;
  if (expected === 0 || seen * 2 >= expected) return null;
  return `The login history shows logins for ${seen} of the ${expected} people who logged in during the window, so the connection can probably see only its own logins. Give the connected user the Monitor Login History permission, then run the check again.`;
}

type Loaded = { status: "ok"; users: UserRow[]; activity: LoginActivity; evidence: Evidence[] } | { status: "not_measurable"; reason: string; evidence: Evidence[] };

function load(ctx: Ctx): Loaded {
  const users = ctx.data(usersExtractor);
  const logins = ctx.data(loginActivityExtractor);
  const evidence = [ctx.ev(users.source, { count: users.status === "ok" ? users.data.length : null }), ctx.ev(logins.source, { count: logins.status === "ok" ? logins.data.rowsRead : null, ...(logins.status === "ok" ? { dateRange: { from: logins.data.from, to: ctx.window.to } } : {}) })];
  if (users.status !== "ok") return { status: "not_measurable", reason: `Users couldn't be read: ${users.reason}`, evidence };
  if (logins.status !== "ok") return { status: "not_measurable", reason: `Login history couldn't be read: ${logins.reason}`, evidence };
  const who = people(ctx, users.data);
  const partial = partialHistory(who, logins.data);
  if (partial) return { status: "not_measurable", reason: partial, evidence };
  return { status: "ok", users: who, activity: logins.data, evidence };
}

const knowsLightning = (u: UserRow): boolean => typeof u.UserPreferencesLightningExperiencePreferred === "boolean";

// ─── Metrics ─────────────────────────────────────────────────────────────────

type Definition = Pick<Metric, "id" | "label" | "unit" | "definition">;
const DEF = {
  daily: { id: "adoption.daily_active_users", label: "Daily active users", unit: "count", definition: `People who logged in on an average working day, over the last ${RECENT_DAYS} days. API sessions don't count.` },
  weekly: { id: "adoption.weekly_active_users", label: "Weekly active users", unit: "count", definition: `People who logged in in an average week, over the last ${RECENT_DAYS} days.` },
  monthly: { id: "adoption.monthly_active_users", label: "Monthly active users", unit: "count", definition: "People who logged in during the last full month, with each month the login history covers." },
  share: { id: "adoption.monthly_active_share", label: "Users active last month", unit: "percent", definition: "People who logged in during the last full month, as a share of active users." },
  channels: { id: "adoption.mobile_login_share", label: "Logins through the mobile app", unit: "percent", definition: "Successful logins through the Salesforce mobile app, as a share of all successful logins in the window, with every channel's share." },
  lightning: { id: "adoption.lightning_share", label: "Users on Lightning", unit: "percent", definition: "Active users set to use Lightning Experience, as a share of active users." },
  reports: { id: "adoption.reports_run_recently", label: "Reports run recently", unit: "percent", definition: `Reports run in the last ${REPORT_RECENT_DAYS} days, as a share of all reports we can see.` },
} satisfies Record<string, Definition>;

/** Segments of the monthly metric holding the group that fell most, one per month, filed under this and the group's name. */
export const FELL_MOST = "fell most: ";

export const ADOPTION_METRIC_IDS = Object.values(DEF).map((d) => d.id);

const EXCLUSIONS = ["Inactive users", "Shared, system, integration and test accounts, as the calibration marks them", "Users on integration licences", "API sessions", "Failed logins"];

function measured(d: Definition, evidence: Evidence[], value: number, n: number, segments: MetricSegment[], extra: { series?: SeriesPoint[]; stats?: Record<string, number>; assumptions?: string[]; exclusions?: string[]; exempt?: string } = {}): Metric {
  return {
    ...d,
    domain: DOMAIN,
    status: "measured",
    evidence,
    exclusions: extra.exclusions ?? EXCLUSIONS,
    assumptions: extra.assumptions ?? [],
    value,
    n,
    dateRange: null,
    segments,
    segmentExemptReason: segments.length ? null : (extra.exempt ?? "There's nothing to segment."),
    series: extra.series ?? [],
    stats: extra.stats ?? {},
  };
}

const notMeasurable = (d: Definition, evidence: Evidence[], reason: string): Metric => ({ ...d, domain: DOMAIN, status: "not_measurable", reason, evidence, exclusions: [], assumptions: [] });

/** A group's own figure, or why it hasn't one: a small group's figure could single someone out. */
const groupSegment = (g: Group, value: number | null): MetricSegment =>
  g.users.length < MIN_GROUP ? { dimension: g.dimension, key: g.key, n: g.users.length, value: null, notMeasurableReason: `Fewer than ${MIN_GROUP} people` } : { dimension: g.dimension, key: g.key, n: g.users.length, value, notMeasurableReason: null };

const round1 = (n: number): number => Math.round(n * 10) / 10;

/** The last `RECENT_DAYS` whole days before the run, oldest first. */
function recentDays(now: Date): string[] {
  return Array.from({ length: RECENT_DAYS }, (_, i) => isoDay(new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()) - (RECENT_DAYS - i) * dayMs)));
}

function activeMetrics(ctx: Ctx, data: Extract<Loaded, { status: "ok" }>): Metric[] {
  const { users, activity, evidence } = data;
  const groups = groupsOf(users);
  const days = recentDays(ctx.now);
  const working = days.filter((d) => ![0, 6].includes(new Date(`${d}T00:00:00Z`).getUTCDay()));
  const weeks = [0, 1, 2, 3].map((w) => new Set(days.slice(w * 7, w * 7 + 7)));
  const daily = (who: UserRow[]): number => round1(working.reduce((sum, day) => sum + activeOn(activity, who, (d) => d === day), 0) / working.length);
  const weekly = (who: UserRow[]): number => round1(weeks.reduce((sum, week) => sum + activeOn(activity, who, (d) => week.has(d)), 0) / weeks.length);
  const assumptions = ["Days are in UTC", `Working days are Monday to Friday`];
  const out: Metric[] = [
    measured(DEF.daily, evidence, daily(users), users.length, groups.map((g) => groupSegment(g, daily(g.users))), { assumptions, stats: { workingDays: working.length } }),
    measured(DEF.weekly, evidence, weekly(users), users.length, groups.map((g) => groupSegment(g, weekly(g.users))), { assumptions: ["Days are in UTC", "Weeks are the last four runs of 7 days before the run"] }),
  ];

  const months = coveredMonths(activity, ctx.now);
  if (months.length === 0) {
    const reason = activity.earliest ? "The login history doesn't cover a whole month." : "There are no successful logins in the login history.";
    return [...out, notMeasurable(DEF.monthly, evidence, reason), notMeasurable(DEF.share, evidence, reason)];
  }
  const last = months.at(-1)!;
  const inMonth = (month: string) => (d: string) => monthKey(d) === month;
  const series: SeriesPoint[] = months.map((month) => ({ date: `${month}-01`, value: activeOn(activity, users, inMonth(month)), n: users.length, notMeasurableReason: null }));
  const mau = activeOn(activity, users, inMonth(last));
  const coverage = [`The login history covers ${plural(months.length, "whole month")}, from ${months[0]}-01${activity.truncated ? ". The read reached its cap, so older months were left out" : ""}`];
  // The group that fell furthest from its peak, month by month, so a chart can set it beside the org's line.
  const steepest = groups
    .filter((g) => g.users.length >= MIN_GROUP && months.length > 1)
    .map((g) => {
      const counts = months.map((month) => activeOn(activity, g.users, inMonth(month)));
      const peak = Math.max(...counts.slice(0, -1));
      return { g, counts, drop: peak >= MIN_GROUP ? pct(peak - counts.at(-1)!, peak) : 0 };
    })
    .filter((x) => x.drop > 0)
    .sort((a, b) => b.drop - a.drop || a.g.dimension.localeCompare(b.g.dimension) || a.g.key.localeCompare(b.g.key))[0];
  const fell = steepest ? months.map((month, i) => ({ dimension: `${FELL_MOST}${steepest.g.dimension} ${steepest.g.key}`, key: month, n: steepest.g.users.length, value: steepest.counts[i]!, notMeasurableReason: null })) : [];
  out.push(
    measured(DEF.monthly, evidence, mau, users.length, [...groups.map((g) => groupSegment(g, activeOn(activity, g.users, inMonth(last)))), ...fell], { series, assumptions: [...coverage, "Months are calendar months in UTC"], stats: { months: months.length, ...(steepest ? { steepestDrop: steepest.drop } : {}) } }),
    measured(DEF.share, evidence, pct(mau, users.length), users.length, groups.map((g) => groupSegment(g, pct(activeOn(activity, g.users, inMonth(last)), g.users.length))), { assumptions: coverage, stats: { activeUsers: users.length } }),
  );
  return out;
}

function channelMetric(data: Loaded): Metric {
  const { evidence } = data;
  // Someone else's logins only would make the split theirs, not the org's.
  if (data.status !== "ok") return notMeasurable(DEF.channels, evidence, data.reason);
  const { channels, channelFields } = data.activity;
  const total = Object.values(channels).reduce((a, b) => a + b, 0);
  if (total === 0) return notMeasurable(DEF.channels, evidence, "There are no successful logins in the login history.");
  // Without these, a mobile login would read as "other", so the share would look smaller than it is.
  const missing = ["Application", "LoginType", "Platform"].filter((f) => !channelFields.includes(f));
  if (missing.length === 3) return notMeasurable(DEF.channels, evidence, "The login history's Application, LoginType and Platform fields couldn't be read, so we can't tell how people signed in.");
  const order: LoginChannel[] = ["browser", "mobile app", "single sign-on", "api", "other"];
  return measured(
    DEF.channels,
    evidence,
    pct(channels["mobile app"], total),
    total,
    order.map((c) => ({ dimension: "channel", key: c, n: channels[c], value: pct(channels[c], total), notMeasurableReason: null })),
    {
      exclusions: ["Failed logins"],
      assumptions: [
        "The channel is the first that fits: an API session, the Salesforce mobile app, single sign-on, a browser, or other",
        "The mobile app is read from the Application and Platform fields, whose values Salesforce doesn't list",
        ...(missing.length ? [`These fields couldn't be read, so some logins may be counted as other: ${missing.join(", ")}`] : []),
      ],
    },
  );
}

function lightningMetric(ctx: Ctx): Metric {
  const users = ctx.data(usersExtractor);
  const evidence = [ctx.ev(users.source, { count: users.status === "ok" ? users.data.length : null })];
  if (users.status !== "ok") return notMeasurable(DEF.lightning, evidence, `Users couldn't be read: ${users.reason}`);
  if (users.missingFields?.includes("UserPreferencesLightningExperiencePreferred")) return notMeasurable(DEF.lightning, evidence, "The user's Lightning Experience setting couldn't be read in this org.");
  const everyone = people(ctx, users.data);
  if (everyone.length === 0) return notMeasurable(DEF.lightning, evidence, "There are no active users to count.");
  // QUESTIONS.md 56: a setting we can't read is left out, never counted as Classic.
  const who = everyone.filter(knowsLightning);
  if (who.length === 0) return notMeasurable(DEF.lightning, evidence, "The Lightning Experience setting came back empty for every user, so it can't be read for other users with this connection.");
  const on = (list: UserRow[]): number => list.filter((u) => u.UserPreferencesLightningExperiencePreferred === true).length;
  const profiles = groupsOf(who).filter((g) => g.dimension === "profile");
  const unread = everyone.length - who.length;
  const classic = who.length - on(who);
  const stats = { lightning: on(who), classic, classicShare: pct(classic, who.length) };
  return measured(DEF.lightning, evidence, pct(on(who), who.length), who.length, profiles.map((g) => groupSegment(g, pct(on(g.users), g.users.length))), unread ? { stats, exclusions: [...EXCLUSIONS, `${plural(unread, "user")} whose setting couldn't be read`] } : { stats });
}

function reportsMetric(ctx: Ctx): Metric {
  const reports = ctx.data(reportsExtractor);
  const evidence = [ctx.ev(reports.source, { count: reports.status === "ok" ? reports.data.length : null })];
  if (reports.status !== "ok") return notMeasurable(DEF.reports, evidence, `Reports couldn't be read: ${reports.reason}`);
  if (reports.data.length === 0) return notMeasurable(DEF.reports, evidence, "There are no reports we can see.");
  const cutoff = daysBefore(ctx.now, REPORT_RECENT_DAYS);
  const recent = (list: typeof reports.data): number => list.filter((r) => r.LastRunDate !== null && new Date(r.LastRunDate) >= cutoff).length;
  const folders = new Map<string, typeof reports.data>();
  for (const r of reports.data) folders.set(r.FolderName ?? "No folder", [...(folders.get(r.FolderName ?? "No folder") ?? []), r]);
  return measured(
    DEF.reports,
    evidence,
    pct(recent(reports.data), reports.data.length),
    reports.data.length,
    [...folders].sort(([a], [b]) => a.localeCompare(b)).map(([key, list]) => ({ dimension: "folder", key, n: list.length, value: pct(recent(list), list.length), notMeasurableReason: null })),
    { exclusions: ["Other people's private reports, which can't be read"], assumptions: ["Last run is Salesforce's own record of when each report was last run (QUESTIONS.md 3)"] },
  );
}

export function adoptionMetrics(ctx: Ctx): Metric[] {
  const gate = opportunitiesGate(ctx);
  if (gate) return Object.values(DEF).map((d) => notMeasurable(d, [ctx.ev(ctx.data(usersExtractor).source, { count: null })], gate));
  const data = load(ctx);
  const active = data.status === "ok" ? activeMetrics(ctx, data) : [DEF.daily, DEF.weekly, DEF.monthly, DEF.share].map((d) => notMeasurable(d, data.evidence, data.reason));
  return [...active, channelMetric(data), lightningMetric(ctx), reportsMetric(ctx)];
}

// ─── Rules ───────────────────────────────────────────────────────────────────

const RISK = "We don't put a figure on it. The seats of people who've stopped logging in are already counted as licence waste, so a figure here would count them twice.";

const fallingAdoption: Rule = {
  id: "adp.falling-adoption",
  version: 1,
  domain: DOMAIN,
  title: "Falling adoption",
  evaluate(ctx): RuleResult {
    const data = load(ctx);
    if (data.status !== "ok") return { outcome: "not_measurable", reason: data.reason, evidence: data.evidence };
    const { users, activity, evidence } = data;
    const months = coveredMonths(activity, ctx.now);
    if (months.length < FALL_SPAN_MONTHS + 1) {
      return { outcome: "not_measurable", reason: `The login history covers ${plural(months.length, "whole month")}, and a trend needs ${FALL_SPAN_MONTHS + 1}.`, evidence };
    }
    const last = months.at(-1)!;
    const earlier = months.slice(0, -FALL_SPAN_MONTHS);
    const inMonth = (month: string) => (d: string) => monthKey(d) === month;
    type Fall = { label: string; peak: number; peakMonth: string; now: number; drop: number };
    const fallOf = (label: string, who: UserRow[]): Fall | null => {
      const peaks = earlier.map((m) => ({ month: m, active: activeOn(activity, who, inMonth(m)) }));
      const peak = peaks.reduce((best, p) => (p.active > best.active ? p : best), peaks[0]!);
      if (peak.active < MIN_GROUP) return null;
      const now = activeOn(activity, who, inMonth(last));
      const drop = pct(peak.active - now, peak.active);
      return drop >= FALL_SHARE ? { label, peak: peak.active, peakMonth: peak.month, now, drop } : null;
    };
    const groups = groupsOf(users).filter((g) => g.users.length >= MIN_GROUP);
    const org = fallOf("the whole org", users);
    const falls = groups.flatMap((g) => fallOf(`${g.dimension} ${g.key}`, g.users) ?? []);
    const segments = [
      { dimension: "profile", segments: groups.filter((g) => g.dimension === "profile").length },
      { dimension: "role", segments: groups.filter((g) => g.dimension === "role").length },
    ];
    const window = `between ${months[0]} and ${last}`;
    if (!org && falls.length === 0) {
      return { outcome: "pass", summary: `Monthly active users haven't fallen ${FALL_SHARE}% or more from an earlier peak ${window}, in the org or in any profile or role of ${MIN_GROUP} people or more.`, evidence, segmentsChecked: segments, facts: { months: months.length } };
    }
    const line = (f: Fall): string => `${f.label}, from ${f.peak} in ${f.peakMonth} to ${f.now} in ${last} (down ${f.drop}%)`;
    const all = [...(org ? [org] : []), ...falls];
    return {
      outcome: "issue",
      title: org ? "Fewer people are using Salesforce" : "Fewer people are using Salesforce in some teams",
      summary: `Monthly active users have fallen ${FALL_SHARE}% or more from their peak: ${all.map(line).join("; ")}.`,
      recommendation: "Ask the people who've stopped why. Fix what gets in their way, and make Salesforce the place the team's meetings and figures come from.",
      recommendationType: "investigate",
      severity: org ? "high" : "medium",
      // Login history is kept for a limited time, so the earliest months may be thinner than they were.
      confidence: "medium",
      evidence,
      value: notSized(`Fewer people using Salesforce means less of the business is recorded in it. ${RISK}`),
      effort: { size: "M" },
      owner: "Sales leadership",
      segmentsChecked: segments,
      subjects: falls.map((f) => ({ type: "group", label: f.label })),
      facts: { months: months.length, groupsFalling: falls.length, ...(org ? { peak: org.peak, latest: org.now, drop: org.drop } : {}) },
    };
  },
};

const lowEngagement: Rule = {
  id: "adp.low-engagement-group",
  version: 1,
  domain: DOMAIN,
  title: "Teams that barely log in",
  evaluate(ctx): RuleResult {
    const data = load(ctx);
    if (data.status !== "ok") return { outcome: "not_measurable", reason: data.reason, evidence: data.evidence };
    const { users, activity, evidence } = data;
    const days = new Set(recentDays(ctx.now));
    const barely = (u: UserRow): boolean => (activity.days[u.Id] ?? []).filter((d) => days.has(d)).length < BARELY_DAYS;
    const groups = groupsOf(users).filter((g) => g.users.length >= MIN_GROUP);
    const segments = [
      { dimension: "profile", segments: groups.filter((g) => g.dimension === "profile").length },
      { dimension: "role", segments: groups.filter((g) => g.dimension === "role").length },
    ];
    if (groups.length === 0) return { outcome: "pass", summary: `No profile or role has ${MIN_GROUP} people or more to judge.`, evidence, segmentExemptReason: `No group has ${MIN_GROUP} people.` };
    const low = groups.map((g) => ({ g, barely: g.users.filter(barely).length })).filter((x) => pct(x.barely, x.g.users.length) > BARELY_SHARE);
    if (low.length === 0) return { outcome: "pass", summary: `In every profile and role of ${MIN_GROUP} people or more, most log in at least ${BARELY_DAYS} days in ${RECENT_DAYS}.`, evidence, segmentsChecked: segments };
    return {
      outcome: "issue",
      title: "Some teams barely use Salesforce",
      summary: `In these groups, most people logged in on fewer than ${BARELY_DAYS} of the last ${RECENT_DAYS} days: ${low.map((x) => `${x.g.dimension} ${x.g.key}, ${x.barely} of ${x.g.users.length}`).join("; ")}.`,
      recommendation: "Find out what these teams use instead of Salesforce, and bring that work into it: their pipeline, their reports, and their next steps.",
      recommendationType: "investigate",
      severity: "medium",
      confidence: "high",
      evidence,
      value: notSized(`Work done outside Salesforce isn't in your pipeline or your forecast. ${RISK}`),
      effort: { size: "M" },
      owner: "Sales operations",
      segmentsChecked: segments,
      subjects: low.map((x) => ({ type: "group", label: `${x.g.dimension} ${x.g.key}` })),
      facts: { groups: low.length, barelyDays: BARELY_DAYS },
    };
  },
};

const classicUsers: Rule = {
  id: "adp.classic-users",
  version: 1,
  domain: DOMAIN,
  title: "Users still on Classic",
  evaluate(ctx): RuleResult {
    const users = ctx.data(usersExtractor);
    const evidence = [ctx.ev(users.source, { count: users.status === "ok" ? users.data.length : null })];
    if (users.status !== "ok") return { outcome: "not_measurable", reason: `Users couldn't be read: ${users.reason}`, evidence };
    if (users.missingFields?.includes("UserPreferencesLightningExperiencePreferred")) return { outcome: "not_measurable", reason: "The user's Lightning Experience setting couldn't be read in this org.", evidence };
    const everyone = people(ctx, users.data);
    const who = everyone.filter(knowsLightning);
    if (who.length === 0) return { outcome: "not_measurable", reason: "The Lightning Experience setting came back empty for every user, so it can't be read for other users with this connection.", evidence };
    const classic = who.filter((u) => u.UserPreferencesLightningExperiencePreferred === false);
    const profiles = new Set(who.map(profileOf)).size;
    const segments = [{ dimension: "profile", segments: profiles }];
    const unread = everyone.length - who.length;
    if (classic.length === 0) return { outcome: "pass", summary: `All ${plural(who.length, "active user")}${unread ? ` whose setting we could read` : ""} are set to use Lightning Experience.${unread ? ` The setting couldn't be read for ${plural(unread, "user")}.` : ""}`, evidence, segmentsChecked: segments };
    const share = pct(classic.length, who.length);
    const byProfile = [...new Set(classic.map(profileOf))].sort().map((p) => `${p} (${classic.filter((u) => profileOf(u) === p).length})`);
    return {
      outcome: "issue",
      title: "Some people still use Salesforce Classic",
      summary: `${classic.length} of ${who.length} active users (${share}%) aren't set to use Lightning Experience: ${byProfile.join(", ")}.`,
      recommendation: "Move these users to Lightning Experience, and find out first what keeps them on Classic, such as a page, button or tool that only works there.",
      recommendationType: "change",
      severity: share > 25 ? "medium" : "low",
      confidence: "high",
      evidence,
      value: notSized("Salesforce builds new features for Lightning only, so these users miss them. We don't put a figure on that."),
      effort: { size: share > 25 ? "M" : "S" },
      owner: "Salesforce admin",
      segmentsChecked: segments,
      subjects: classic.map((u) => ({ type: "user", label: ctx.userLabel(u.Id) })),
      facts: { users: classic.length, share },
      records: { object: "User", rows: classic.map((u) => ({ id: u.Id, problem: "Set to use Salesforce Classic" })), fix: "Find out what keeps them on Classic, then switch them to Lightning Experience" },
    };
  },
};

export const adoptionModule: DomainModule = {
  id: DOMAIN,
  cloud: "sales",
  label: "Adoption and usage",
  extractors: [usersExtractor, loginActivityExtractor, reportsExtractor],
  metrics: [adoptionMetrics],
  rules: [fallingAdoption, lowEngagement, classicUsers],
};
