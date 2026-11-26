import { apexExtractor } from "../../extract/automation.ts";
import { named as valued } from "../../engine/named.ts";
import { userKinds } from "../user-kinds.ts";
import { customObjectsExtractor, orgExtractor, recordCountsExtractor } from "../../extract/configuration.ts";
import { orgDocumentationExtractor } from "../../extract/documentation.ts";
import { fieldDescribeExtractor, missingFieldReason } from "../../extract/fields.ts";
import { apiLimitsExtractor, apiOnlyExtractor, apiOnlyOf, ASYNC_JOB_DAYS, asyncJobsExtractor } from "../../extract/integrations.ts";
import { licencesExtractor, loginsExtractor, usersExtractor, type UserRow } from "../../extract/users.ts";
import { looksShared } from "../../guards/user-guards.ts";
import { COMPLIMENTARY_LICENCES } from "../../money/licences.ts";
import { costPerSeat, isPriced, type Unpriced } from "../../money/seat-cost.ts";
import { notSized, pct, plural, type Rule, type RuleContext, type RuleResult } from "../../engine/rule.ts";
import type { Value } from "../../schemas/index.ts";
import { apexGap } from "../automation/shared.ts";
import type { DomainModule } from "../types.ts";
import { WHERE } from "../where.ts";
import { joinSaid, org, said, type Said } from "../../engine/org-words.ts";
import { andList, grouped, seatsSaid } from "../../engine/said-forms.ts";

// Integrations (roadmap 3.2): who and what connects to the org, and whether it
// works. The three checks that were in security moved here with their ids, so a
// re-check still lines them up (DECISIONS.md 170). Text says "integration
// account", never "integration user": orgs often name a user that, and the
// scrub would read the words as their name (QUESTIONS.md).
//
// Every title, summary, recommendation and reason of our own is written with
// `said`, and every value the org chose (an app's name, a host, a licence, a
// class, a date read from the log) with `org` (DECISIONS.md 655, 657). A reason
// passed on from an extract quotes Salesforce and stays plain: unknown.

const DOMAIN = "integrations";
const MIN_API_LOGINS = 5;
const NON_PERSON_KINDS = new Set(["shared", "system", "integration", "test"]);
/** The licence Salesforce gives for API-only integration users (Well-Architected, Trust). */
export const INTEGRATION_LICENCE = "Salesforce Integration";
// A daily allowance is read once, when the check first connects, and Salesforce
// counts it over a rolling 24 hours, so one reading can't tell a busy hour from
// a trend. Only one at or near exhaustion is an issue: from 95% used, what is
// left can go in an hour or two of an ordinary day (medium); with none left,
// calls or jobs are being refused as we read it (high) (DECISIONS.md 726).
const ALLOWANCE_NEAR_EXHAUSTION = 95;
// Each allowance's plain name, and the fuller one its figure is first given with.
const ALLOWANCES = {
  DailyApiRequests: { label: "API requests", first: "API requests" },
  DailyAsyncApexExecutions: { label: "background jobs", first: "background (asynchronous Apex) jobs" },
} as const;
/** The highest share printed for an allowance with some left, so it never reads as 100% under "nearly used up". */
const SHARE_BELOW_MAX = 99.9;
// The jobs someone starts. A batch's own chunks (BatchApexWorker) are counted through the batch.
const JOB_TYPES = new Set(["BatchApex", "Queueable", "Future", "ScheduledApex"]);
// Our judgement: one or two failures a week can be a blip, three is a pattern.
const FAILURES_MEDIUM = 3;

const kindOf = (ctx: RuleContext, id: string): string | undefined => userKinds(ctx).find((u) => u.userId.slice(0, 15) === id.slice(0, 15))?.kind;
const licenceOf = (user: UserRow): string => user.Profile?.UserLicense?.Name ?? "Unknown";
const isIntegrationLicence = (licence: string): boolean => /\bintegration\b/i.test(licence);

const personalIntegrations: Rule = {
  id: "sec.personal-integration-accounts",
  version: 4,
  domain: DOMAIN,
  title: "Personal accounts used for integrations",
  evaluate(ctx): RuleResult | RuleResult[] {
    const users = ctx.data(usersExtractor);
    const logins = ctx.data(loginsExtractor);
    if (users.status !== "ok") return { outcome: "not_measurable", reason: `Users couldn't be read: ${users.reason}`, evidence: [ctx.ev(users.source, { count: null })] };
    if (logins.status !== "ok") return { outcome: "not_measurable", reason: `LoginHistory couldn't be read: ${logins.reason}`, evidence: [ctx.ev(logins.source, { count: null })] };
    const gap = missingFieldReason(logins, "LoginHistory.ApiType");
    if (gap) return { outcome: "not_measurable", reason: gap, evidence: [ctx.ev(logins.source, { count: null })] };
    // An API-only account is an integration account, as the licence checks count it.
    const apiOnly = apiOnlyUsers(ctx);
    const isPerson = (u: UserRow): boolean => {
      if (apiOnly?.(u)) return false;
      const kind = kindOf(ctx, u.Id);
      return kind ? !NON_PERSON_KINDS.has(kind) : !looksShared(u);
    };
    const active = users.data.filter((u) => u.IsActive);
    const people = active.filter(isPerson);
    const api = logins.data.apiByUser;
    const hits = people.filter((u) => (api[u.Id]?.logins ?? 0) >= MIN_API_LOGINS);
    const range = { from: logins.data.from, to: ctx.window.to };
    const exclusions = [
      "Accounts calibrated as shared, system, integration or test",
      apiOnly ? `Accounts ${API_ONLY_ACCOUNT}` : "Who is API only couldn't be read, so an account set to API only may be counted as a person's",
      `People with fewer than ${MIN_API_LOGINS} API logins, which is normal for an occasional data load`,
    ];
    const { sets } = ctx.data(apiOnlyExtractor);
    const evidence = [
      ctx.ev(logins.source, { count: hits.length, dateRange: range, exclusions, note: "Salesforce only returns everyone's logins to a user with Manage Users or Monitor Login History" }),
      ctx.ev(sets.source, { count: sets.status === "ok" ? sets.data.length : null, note: API_ONLY_SETS }),
    ];
    const segments = [{ dimension: "user kind", segments: new Set(people.map((u) => kindOf(ctx, u.Id) ?? "Not calibrated")).size }, { dimension: "role", segments: new Set(people.map((u) => u.UserRole?.Name ?? "No role")).size }];
    if (hits.length === 0) {
      if (logins.truncated) return { outcome: "not_measurable", reason: said`The login log was too long to read in full, so we can't say no personal account is used this way.`, evidence };
      return { outcome: "pass", summary: valued(said`None of the ${people.length} personal accounts made ${MIN_API_LOGINS} or more API logins since ${org(logins.data.from)}.`, { people: people.length, minApiLogins: MIN_API_LOGINS }), evidence, segmentsChecked: segments };
    }
    const applications = [...new Set(hits.flatMap((u) => api[u.Id]!.applications))].sort();
    const total = hits.reduce((sum, u) => sum + api[u.Id]!.logins, 0);
    return {
      outcome: "issue",
      title: said`Integrations look to be running on personal logins`,
      // A cut-off log holds only some of the logins, so its counts are floors (DECISIONS.md 325).
      summary: valued(said`${logins.truncated ? "At least " : ""}${plural(hits.length, "personal account")} made ${logins.truncated ? "at least " : ""}${total} API logins since ${org(logins.data.from)}, through ${applications.join(", ") ? joinSaid(applications.map((a) => said`${org(a)}`), ", ") : "an unnamed application"}.${logins.truncated ? " The login log was too long to read in full, so there may be more." : ""} If that person leaves and the account is deactivated, the integration stops.`, { hits: hits.length, total }),
      recommendation: said`Move each integration to its own integration account, with an API-only profile and only the access it needs.`,
      recommendationType: "investigate",
      severity: "medium",
      // API logins are a sign, not proof: someone can run a tool by hand under their own login.
      confidence: "medium",
      evidence,
      value: notSized(said`The cost of an integration failing depends on what it carries, which we can't see from the login log.`),
      effort: { size: "S" },
      owner: "Salesforce admin",
      dependencies: ["Confirm with each person what these logins are"],
      segmentsChecked: segments,
      subjects: hits.map((u) => ({ type: "user", label: ctx.userLabel(u.Id) })),
      facts: { accounts: hits.length, apiLogins: total, applications: applications.length, peopleShare: pct(hits.length, people.length) },
    };
  },
};

// Connected apps and remote sites are read for "Your org, documented"
// (DECISIONS.md 118). These two rules judge what that part only describes.

const selfAuthorisedApps: Rule = {
  id: "sec.self-authorised-connected-apps",
  version: 2,
  domain: DOMAIN,
  title: "Connected apps anyone can authorise",
  where: WHERE.connectedApps,
  evaluate(ctx): RuleResult {
    const { connectedApps } = ctx.data(orgDocumentationExtractor);
    const logins = ctx.data(loginsExtractor);
    const evidence = [ctx.ev(connectedApps.source, { count: connectedApps.status === "ok" ? connectedApps.data.length : null })];
    if (connectedApps.status !== "ok") return { outcome: "not_measurable", reason: `Connected apps couldn't be read: ${connectedApps.reason}`, evidence };
    // A null setting isn't a yes: only a definite false counts.
    const open = connectedApps.data.filter((a) => a.OptionsAllowAdminApprovedUsersOnly === false).sort((a, b) => a.Name.localeCompare(b.Name));
    const unread = connectedApps.data.filter((a) => typeof a.OptionsAllowAdminApprovedUsersOnly !== "boolean").length;
    const segments = [{ dimension: "connected app", segments: connectedApps.data.length }];
    if (open.length === 0) {
      if (unread) return { outcome: "not_measurable", reason: valued(said`Who can authorise ${plural(unread, "connected app")} couldn't be read.`, { unread }), evidence };
      return { outcome: "pass", summary: valued(said`All ${plural(connectedApps.data.length, "connected app")} let only users an admin has approved use them.`, { data: connectedApps.data.length }), evidence, segmentsChecked: segments, segmentExemptReason: connectedApps.data.length ? undefined : "There are no connected apps." };
    }
    // An app with API logins is carrying data now, so opening it to anyone matters more.
    const inUse = new Map<string, number>();
    if (logins.status === "ok") for (const api of Object.values(logins.data.apiByUser)) for (const app of api.applications) inUse.set(app, (inUse.get(app) ?? 0) + api.logins);
    const busy = open.filter((a) => inUse.has(a.Name));
    if (logins.status === "ok") evidence.push(ctx.ev(logins.source, { count: logins.data.rowsRead, dateRange: { from: logins.data.from, to: ctx.window.to }, note: "API logins by application, matched to connected apps by name" }));
    return {
      outcome: "issue",
      title: said`Any user can authorise some connected apps`,
      summary: valued(said`${open.length} of ${connectedApps.data.length} connected apps let users authorise them for themselves, without an admin approving who can: ${joinSaid(open.map((a) => said`${org(a.Name)}`), ", ")}.${busy.length ? said` ${joinSaid(busy.map((a) => said`${org(a.Name)}`), ", ")} ${busy.length === 1 ? "has" : "have"} had API logins since ${logins.status === "ok" ? org(logins.data.from) : ""}, so ${busy.length === 1 ? "it carries" : "they carry"} data now.` : ""}${logins.status !== "ok" ? " We couldn't read the login history, so we can't say which are in use." : ""}`, { open: open.length, data: connectedApps.data.length }),
      recommendation: said`Set each app to admin-approved users only, then give access through a permission set to the people and integration accounts that need it.`,
      recommendationType: "change",
      severity: busy.length ? "high" : "medium",
      confidence: "high",
      evidence,
      value: notSized(said`A security gap is a risk to manage, not a figure we can put in revenue or hours.`),
      effort: { size: "S" },
      owner: "Salesforce admin",
      dependencies: ["Find out who uses each app before changing it, and grant them access first"],
      // Not a block on a change, but the thing to check before making it.
      guards: open.map((a) => ({ guardId: "business-relies-on-app", subject: a.Name, status: "unknown" as const, detail: "This may be an app the business relies on. Check who uses it before restricting it, or their access stops.", evidence })),
      segmentsChecked: segments,
      subjects: open.map((a) => ({ type: "connected app", label: a.Name })),
      facts: { open: open.length, apps: connectedApps.data.length, withApiLogins: busy.length },
    };
  },
};

const insecureRemoteSites: Rule = {
  id: "sec.insecure-remote-sites",
  version: 1,
  domain: DOMAIN,
  title: "Remote sites that allow plain HTTP",
  where: WHERE.remoteSites,
  evaluate(ctx): RuleResult {
    const { remoteSites } = ctx.data(orgDocumentationExtractor);
    const evidence = [ctx.ev(remoteSites.source, { count: remoteSites.status === "ok" ? remoteSites.data.length : null, exclusions: ["Inactive remote sites", "Installed packages' remote sites"], note: "Shown by host only" })];
    if (remoteSites.status !== "ok") return { outcome: "not_measurable", reason: `Remote site settings couldn't be read: ${remoteSites.reason}`, evidence };
    const active = remoteSites.data.filter((r) => r.IsActive && !r.NamespacePrefix);
    const plain = active.filter((r) => !r.secure);
    const mixed = active.filter((r) => r.secure && r.ProtocolMismatch);
    const segments = [{ dimension: "remote site", segments: active.length }];
    if (plain.length + mixed.length === 0) {
      return { outcome: "pass", summary: valued(said`All ${plural(active.length, "active remote site")} use HTTPS and keep to it.`, { active: active.length }), evidence, segmentsChecked: segments, segmentExemptReason: active.length ? undefined : "There are no active remote sites." };
    }
    const hosts = (rows: typeof active): Said => joinSaid([...new Set(rows.map((r) => r.host))].sort().map((h) => said`${org(h)}`), ", ");
    return {
      outcome: "issue",
      title: said`Some remote sites can send data over plain HTTP`,
      summary: joinSaid(
        [
          plain.length ? valued(said`${plural(plain.length, "active remote site")} ${plain.length === 1 ? "uses" : "use"} plain HTTP: ${hosts(plain)}.`, { plain: plain.length }) : null,
          mixed.length ? valued(said`${plural(mixed.length, "active remote site")} ${mixed.length === 1 ? "is" : "are"} set to let code pass data between HTTPS and HTTP: ${hosts(mixed)}.`, { mixed: mixed.length }) : null,
          said`Data sent over HTTP isn't encrypted on the way.`,
        ].filter((part): part is Said => part !== null),
        " ",
      ),
      recommendation: said`Move each site to HTTPS and turn off the setting that lets code mix HTTPS and HTTP. If a site can't do HTTPS, ask whether Salesforce should be calling it at all.`,
      recommendationType: "change",
      severity: "medium",
      confidence: "high",
      evidence,
      value: notSized(said`A security gap is a risk to manage, not a figure we can put in revenue or hours.`),
      effort: { size: "S" },
      owner: "Salesforce admin",
      dependencies: ["Check that the other end accepts HTTPS before changing the address"],
      segmentsChecked: segments,
      subjects: [...plain, ...mixed].map((r) => ({ type: "remote site", label: r.host })),
      facts: { plain: plain.length, mixed: mixed.length, active: active.length },
    };
  },
};

/**
 * How every check describes an API-only account (DECISIONS.md 323). Such an
 * account is an integration's, whatever the calibration says about the rest.
 */
export const API_ONLY_ACCOUNT = "set to API only (API Only User on its profile or a permission set)";
const API_ONLY_SETS = "Profiles and permission sets with API Only User";

type ApiOnly = ((u: UserRow) => boolean) | null;
const apiOnlyRead = new WeakMap<object, ApiOnly>();

/** Who is API only, read once a run from the API-only extract and shared by every check. Null when it couldn't be read. */
function apiOnlyUsers(ctx: RuleContext): ApiOnly {
  const data = ctx.data(apiOnlyExtractor);
  if (apiOnlyRead.has(data)) return apiOnlyRead.get(data)!;
  const apiOnly = apiOnlyOf(data);
  apiOnlyRead.set(data, apiOnly);
  return apiOnly;
}

/** Users the org treats as integrations: calibrated as one, or set to API only. */
function integrationAccounts(ctx: RuleContext, users: UserRow[]): { accounts: UserRow[]; apiOnly: ApiOnly } {
  const apiOnly = apiOnlyUsers(ctx);
  return { accounts: users.filter((u) => u.IsActive && (kindOf(ctx, u.Id) === "integration" || apiOnly?.(u) === true)), apiOnly };
}

const integrationLicences: Rule = {
  id: "int.integration-user-licences",
  version: 1,
  domain: DOMAIN,
  title: "Integration accounts on full licences",
  evaluate(ctx): RuleResult | RuleResult[] {
    const users = ctx.data(usersExtractor);
    const { userLicenses } = ctx.data(licencesExtractor);
    if (users.status !== "ok") return { outcome: "not_measurable", reason: `Users couldn't be read: ${users.reason}`, evidence: [ctx.ev(users.source, { count: null })] };
    if (userLicenses.status !== "ok") return { outcome: "not_measurable", reason: `Licences couldn't be read: ${userLicenses.reason}`, evidence: [ctx.ev(userLicenses.source, { count: null })] };
    const { sets } = ctx.data(apiOnlyExtractor);
    const { accounts, apiOnly } = integrationAccounts(ctx, users.data);
    const evidence = [
      ctx.ev(users.source, { count: accounts.length, exclusions: ["Inactive users"], note: `Integration accounts: calibrated as one, or ${API_ONLY_ACCOUNT}` }),
      ctx.ev(sets.source, { count: sets.status === "ok" ? sets.data.length : null, note: API_ONLY_SETS }),
      ctx.ev(userLicenses.source, { count: userLicenses.data.length }),
    ];
    if (accounts.length === 0) {
      if (apiOnly === null) return { outcome: "not_measurable", reason: said`No account is calibrated as an integration account, and who is API only couldn't be read.`, evidence };
      return { outcome: "pass", summary: said`No account is calibrated as an integration account or set to API only, so none holds a licence it doesn't need.`, evidence, segmentExemptReason: "There are no integration accounts to segment." };
    }
    // A paid licence that isn't an integration one. A priced complimentary licence still costs nothing to move off.
    const onFull = accounts.filter((u) => !isIntegrationLicence(licenceOf(u)) && !COMPLIMENTARY_LICENCES.has(licenceOf(u)));
    const segments = [{ dimension: "licence", segments: new Set(accounts.map(licenceOf)).size }];
    const row = userLicenses.data.find((l) => l.Name === INTEGRATION_LICENCE && l.Status === "Active");
    const spare = row ? Math.max(0, row.TotalLicenses - row.UsedLicenses) : 0;
    if (onFull.length === 0) return { outcome: "pass", summary: valued(said`All ${plural(accounts.length, "integration account")} ${accounts.length === 1 ? "is" : "are"} on an integration or free licence.`, { accounts: accounts.length }), evidence, segmentsChecked: segments, facts: { accounts: accounts.length, onFull: 0 } };
    const movable = Math.min(onFull.length, spare);
    const orgRead = ctx.data(orgExtractor);
    const edition = orgRead.status === "ok" ? (orgRead.data[0]?.OrganizationType ?? null) : null;
    const prices = onFull.slice(0, movable).map((u) => costPerSeat(licenceOf(u), { calibration: ctx.calibration, edition, now: ctx.now }));
    const unpriced = prices.filter((p): p is Unpriced => !isPriced(p));
    const value: Value =
      movable === 0
        ? notSized(said`There's no unassigned ${INTEGRATION_LICENCE} licence to move them to, and we hold no price for buying one.`)
        : unpriced.length > 0
          ? notSized(said`A full seat has no price: ${unpriced[0]!.reason}`)
          : {
              kind: "licence_cost",
              amount: Math.round(prices.reduce((sum, p) => sum + (isPriced(p) ? p.annual : 0), 0)),
              unit: ctx.calibration.currency,
              period: "per year",
              assumptions: [
                ...[...new Map(prices.filter(isPriced).map((p) => [p.licence, p])).values()].map((p) => said`${seatsSaid(prices.filter((x) => isPriced(x) && x.licence === p.licence).length, p.licence)} at ${p.note}`),
                valued(said`${spare} ${INTEGRATION_LICENCE} ${spare === 1 ? "licence" : "licences"} unassigned now, so ${movable} of the accounts can move without buying any`, { spare, movable }),
                said`Assumes the freed seats can be reduced at renewal`,
              ],
            };
    const licences = [...new Set(onFull.map(licenceOf))].sort();
    return {
      outcome: "issue",
      title: said`Integration accounts are using full Salesforce licences`,
      summary: valued(said`${onFull.length} of ${plural(accounts.length, "integration account")} ${onFull.length === 1 ? "holds" : "hold"} a full ${andList(licences.map((l) => said`${org(l)}`))} licence. ${spare ? said`The org has ${plural(spare, `${INTEGRATION_LICENCE} licence`)} unassigned, so ${movable === onFull.length ? (movable === 1 ? "it" : "all of them") : movable} could move now and free the full ${movable === 1 ? "seat" : "seats"}.` : said`The org has no unassigned ${INTEGRATION_LICENCE} licence.`} Salesforce's guidance for new integrations is an API-only profile on the ${INTEGRATION_LICENCE} licence.`, { onFull: onFull.length, accounts: accounts.length }),
      recommendation: said`Move each integration account to the ${INTEGRATION_LICENCE} licence with an API-only profile, give it a permission set with only the access it needs, then reduce the full seats at renewal.`,
      recommendationType: "change",
      severity: movable > 0 ? "medium" : "low",
      confidence: "high",
      evidence,
      value,
      effort: { size: "S" },
      owner: "Salesforce admin",
      dependencies: [`Check in a sandbox that each integration only needs what the ${INTEGRATION_LICENCE} licence allows`],
      segmentsChecked: segments,
      subjects: onFull.map((u) => ({ type: "user", label: ctx.userLabel(u.Id) })),
      facts: { accounts: accounts.length, onFull: onFull.length, spare, movable, ...(value.kind === "licence_cost" ? { annualSaving: value.amount } : {}) },
    };
  },
};

const interactiveIntegrations: Rule = {
  id: "int.integration-users-interactive",
  version: 1,
  domain: DOMAIN,
  title: "Integration accounts that can log in like a person",
  evaluate(ctx): RuleResult | RuleResult[] {
    const users = ctx.data(usersExtractor);
    const { sets, assignments } = ctx.data(apiOnlyExtractor);
    if (users.status !== "ok") return { outcome: "not_measurable", reason: `Users couldn't be read: ${users.reason}`, evidence: [ctx.ev(users.source, { count: null })] };
    if (sets.status !== "ok") return { outcome: "not_measurable", reason: `Who is API only couldn't be read: ${sets.reason}`, evidence: [ctx.ev(sets.source, { count: null })] };
    if (assignments.status !== "ok") return { outcome: "not_measurable", reason: `Who is API only couldn't be read: ${assignments.reason}`, evidence: [ctx.ev(assignments.source, { count: null })] };
    const { apiOnly } = integrationAccounts(ctx, users.data);
    // Only the calibration says an account is an integration's. The integration licence is left out, as it's built for API use (QUESTIONS.md).
    const accounts = users.data.filter((u) => u.IsActive && kindOf(ctx, u.Id) === "integration" && !isIntegrationLicence(licenceOf(u)));
    const evidence = [
      ctx.ev(users.source, { count: accounts.length, exclusions: ["Inactive users", `Users on the ${INTEGRATION_LICENCE} licence`], note: "Accounts calibrated as integration accounts" }),
      ctx.ev(sets.source, { count: sets.data.length, note: API_ONLY_SETS }),
    ];
    if (accounts.length === 0) return { outcome: "not_measurable", reason: said`No account is calibrated as an integration account, so we can't tell which accounts integrations run on.`, evidence };
    const open = accounts.filter((u) => !apiOnly!(u));
    const logins = ctx.data(loginsExtractor);
    const uiLogins = (u: UserRow): number => (logins.status === "ok" ? (logins.data.byUser[u.Id]?.successes ?? 0) - (logins.data.apiByUser[u.Id]?.logins ?? 0) : 0);
    const byHand = open.filter((u) => uiLogins(u) > 0);
    if (logins.status === "ok") evidence.push(ctx.ev(logins.source, { count: byHand.length, dateRange: { from: logins.data.from, to: ctx.window.to }, note: "Logins without an API type, by account" }));
    const segments = [{ dimension: "profile", segments: new Set(accounts.map((u) => u.Profile?.Name ?? "No profile")).size }];
    if (open.length === 0) return { outcome: "pass", summary: valued(said`All ${plural(accounts.length, "integration account")} ${accounts.length === 1 ? "is" : "are"} API only, so nobody can log in to Salesforce with ${accounts.length === 1 ? "it" : "them"}.`, { accounts: accounts.length }), evidence, segmentsChecked: segments, facts: { accounts: accounts.length, open: 0 } };
    return {
      outcome: "issue",
      title: said`Integration accounts can log in like a person`,
      summary: valued(said`${open.length} of ${plural(accounts.length, "integration account")} ${open.length === 1 ? "isn't" : "aren't"} set to API only, so anyone with the password can log in through the browser${byHand.length && logins.status === "ok" ? said`, and ${byHand.length === open.length && open.length === 1 ? "it has" : said`${byHand.length} ${byHand.length === 1 ? "has" : "have"}`} since ${org(logins.data.from)}` : ""}.${logins.status !== "ok" ? " We couldn't read the login history, so we can't say whether anyone has." : ""}`, { open: open.length, accounts: accounts.length }),
      recommendation: said`Give each integration account the API Only User permission, through its profile or a permission set, so it can reach Salesforce only through the API.`,
      recommendationType: "change",
      severity: byHand.length ? "high" : "medium",
      confidence: "high",
      evidence,
      value: notSized(said`A security gap is a risk to manage, not a figure we can put in revenue or hours.`),
      effort: { size: "XS" },
      owner: "Salesforce admin",
      dependencies: ["Check that nobody signs in as these accounts to do anything by hand"],
      segmentsChecked: segments,
      subjects: open.map((u) => ({ type: "user", label: ctx.userLabel(u.Id) })),
      facts: { accounts: accounts.length, open: open.length, loggedInByHand: byHand.length },
    };
  },
};

const apiUsage: Rule = {
  id: "int.api-usage",
  // 3: an issue only at or near exhaustion (95%), not from 80%; a reading past the
  // allowance reads "over the ... allowance"; a reported 0 is no allowance, and more remaining
  // than the maximum can't be read; read before this check's own calls (DECISIONS.md 726).
  // Version 3 loosened the rule from 1 and 2: declared in src/engine/loosened.ts (DECISIONS.md 732),
  // so a pass that only the higher line allows is never called a fix.
  version: 3,
  domain: DOMAIN,
  title: "Daily API and async Apex use against the allowance",
  evaluate(ctx): RuleResult | RuleResult[] {
    const limits = ctx.data(apiLimitsExtractor);
    if (limits.status !== "ok") return { outcome: "not_measurable", reason: `The org's limits couldn't be read: ${limits.reason}`, evidence: [ctx.ev(limits.source, { count: null })] };
    type Named = (typeof ALLOWANCES)[keyof typeof ALLOWANCES];
    type Read = { key: string; label: Named["label"]; first: Named["first"]; used: number; max: number; share: number };
    const read: Read[] = [];
    // Not reported at all; reported with a maximum of 0, so the org has no such allowance;
    // reported with more remaining than the maximum, so how much was used can't be said.
    const absent: Array<Named["label"]> = [];
    const none: Array<Named["label"]> = [];
    const odd: Array<Named["label"]> = [];
    for (const [key, { label, first }] of Object.entries(ALLOWANCES) as Array<[string, Named]>) {
      const limit = limits.data[key];
      if (!limit) absent.push(label);
      else if (limit.Max === 0) none.push(label);
      else if (!(limit.Max > 0) || limit.Remaining > limit.Max) odd.push(label);
      else {
        const used = limit.Max - limit.Remaining;
        read.push({ key, label, first, used, max: limit.Max, share: used < limit.Max ? Math.min(pct(used, limit.Max), SHARE_BELOW_MAX) : pct(used, limit.Max) });
      }
    }
    const listed = (labels: Array<Named["label"]>): Said => andList(labels.map((label) => said`${label}`));
    const evidence = [ctx.ev(limits.source, { count: read.length, note: "Read when this check first connected, before its own calls, over Salesforce's rolling 24 hours." })];
    const segments = [{ dimension: "allowance", segments: read.length }];
    const facts = Object.fromEntries(read.flatMap((r) => [[`${r.key}Used`, r.used], [`${r.key}Max`, r.max], [`${r.key}Share`, r.share]]));
    // Past the allowance, "100,500 of 100,000" would read as a part larger than its whole, which the number check refuses (as storage, 502).
    const amount = (r: Read): Said => (r.used > r.max ? valued(said`${grouped(r.used)}, over the ${grouped(r.max)} allowance`, { used: r.used, max: r.max }) : valued(said`${grouped(r.used)} of ${grouped(r.max)}`, { used: r.used, max: r.max }));
    const text = (r: Read): Said => valued(said`${r.share}% of its daily ${r.first} (${amount(r)})`, { share: [r.share, "percent"] });
    const noAllowance = none.length ? said`This org has no daily allowance for ${listed(none)}, so there's nothing to use up.` : null;
    const oddText = odd.length ? said`Salesforce reported more remaining than the maximum for ${listed(odd)}, so we can't say how much was used.` : null;
    const unreadText = absent.length ? said`The org didn't report its daily allowance for ${listed(absent)}, so we can't say it's within it.` : null;
    const over = read.filter((r) => r.share >= ALLOWANCE_NEAR_EXHAUSTION);
    if (over.length === 0) {
      // What can't be read is said first; with nothing read and every allowance at 0, the check doesn't apply.
      if (oddText) return { outcome: "not_measurable", reason: oddText, evidence };
      if (unreadText) return { outcome: "not_measurable", reason: unreadText, evidence };
      if (read.length === 0) return { outcome: "not_measurable", reason: noAllowance ?? said`The org reported neither daily allowance.`, evidence };
      return { outcome: "pass", summary: said`When this check first connected, the org had used ${andList(read.map(text))}.${noAllowance ? said` ${noAllowance}` : ""}`, evidence, segmentsChecked: segments, facts };
    }
    const usedUp = over.filter((r) => r.used >= r.max).length;
    const title = over.length === 1 ? (usedUp ? said`A daily allowance is used up` : said`A daily allowance is nearly used up`) : usedUp === over.length ? said`Daily allowances are used up` : usedUp ? said`Daily allowances are used up or nearly used up` : said`Daily allowances are nearly used up`;
    const notChecked = [...(oddText ? [said` Salesforce reported more remaining than the maximum for ${listed(odd)}, so that isn't checked.`] : []), ...(absent.length ? [said` The org didn't report its daily allowance for ${listed(absent)}, so that isn't checked.`] : [])];
    return {
      outcome: "issue",
      title,
      summary: said`When this check first connected, the org had used ${andList(over.map(text))}. Past an allowance, Salesforce can refuse further calls or jobs until use falls back, and the integrations that depend on them stop.${joinSaid(notChecked, "")}`,
      recommendation: said`Find which integrations and jobs use the most, then cut repeats: combine calls, and use bulk loads or change events for large volumes.`,
      recommendationType: "investigate",
      // Nearly used up is a warning, since the allowance resets; used up, calls or jobs are refused as we read it.
      severity: usedUp ? "high" : "medium",
      // One reading, at one moment.
      confidence: "medium",
      evidence,
      value: notSized(said`The cost is the integrations that stop when an allowance runs out, which one reading can't size.`),
      effort: { size: "M" },
      owner: "Salesforce developer",
      segmentsChecked: segments,
      subjects: over.map((r) => ({ type: "allowance", label: r.label })),
      facts,
    };
  },
};

const failedJobs: Rule = {
  id: "int.failed-async-jobs",
  version: 2,
  domain: DOMAIN,
  title: "Background jobs that failed",
  where: WHERE.apexJobs,
  evaluate(ctx): RuleResult | RuleResult[] {
    const jobs = ctx.data(asyncJobsExtractor);
    if (jobs.status !== "ok") return { outcome: "not_measurable", reason: `Asynchronous Apex jobs couldn't be read: ${jobs.reason}`, evidence: [ctx.ev(jobs.source, { count: null })] };
    const started = jobs.data.filter((j) => JOB_TYPES.has(j.JobType));
    const failed = started.filter((j) => j.Status === "Failed");
    const withErrors = started.filter((j) => j.JobType === "BatchApex" && j.Status === "Completed" && (j.NumberOfErrors ?? 0) > 0);
    const byClass = new Map<string, number>();
    for (const j of [...failed, ...withErrors]) byClass.set(j.ApexClass?.Name ?? "a class we couldn't name", (byClass.get(j.ApexClass?.Name ?? "a class we couldn't name") ?? 0) + 1);
    const classes = [...byClass].sort(([a, x], [b, y]) => y - x || a.localeCompare(b));
    const range = { from: new Date(ctx.now.getTime() - ASYNC_JOB_DAYS * 86_400_000).toISOString().slice(0, 10), to: ctx.now.toISOString().slice(0, 10) };
    // A cut-off log is a sample of the week, in no order: what it holds is a floor, never the week's totals (rule 5).
    const cutOff = jobs.truncated ? [`The rest of the job log, which was too long to read in full: we read ${jobs.data.length.toLocaleString("en-GB")} rows`] : [];
    const evidence = [ctx.ev(jobs.source, { count: started.length, dateRange: range, exclusions: ["A batch's own chunks, counted through the batch", "Sharing recalculations and other jobs Salesforce starts itself", ...cutOff], note: "Error messages aren't read, because they can quote a record" })];
    const segments = [{ dimension: "job type", segments: new Set(started.map((j) => j.JobType)).size }, { dimension: "class", segments: new Set(started.map((j) => j.ApexClass?.Name ?? "")).size }];
    const facts = { jobs: started.length, failed: failed.length, withErrors: withErrors.length, classes: classes.length, days: ASYNC_JOB_DAYS };
    if (failed.length + withErrors.length === 0) {
      if (jobs.truncated) return { outcome: "not_measurable", reason: said`The job log was too long to read in full, so we can't say none failed.`, evidence };
      if (started.length === 0) return { outcome: "pass", summary: valued(said`No asynchronous Apex ran in the last ${ASYNC_JOB_DAYS} days.`, { asyncJobDays: [ASYNC_JOB_DAYS, "day"] }), evidence, segmentExemptReason: "There are no jobs to segment.", facts };
      return { outcome: "pass", summary: valued(said`None of the ${plural(started.length, "asynchronous Apex job")} in the last ${ASYNC_JOB_DAYS} days failed.`, { started: started.length, asyncJobDays: [ASYNC_JOB_DAYS, "day"] }), evidence, segmentsChecked: segments, facts };
    }
    return {
      outcome: "issue",
      title: said`Background jobs are failing`,
      summary: valued(said`${jobs.truncated ? said`At least ${plural(failed.length, "asynchronous Apex job")}` : said`${failed.length} of ${plural(started.length, "asynchronous Apex job")}`} failed in the last ${ASYNC_JOB_DAYS} days${withErrors.length ? said`, and ${plural(withErrors.length, "batch job")} finished with errors in some of ${withErrors.length === 1 ? "its" : "their"} records` : ""}: ${joinSaid(classes.map(([name, n]) => valued(said`${org(name)} ${n}`, { n })), ", ")}. The work these jobs should have done didn't happen, and failures like these often go unnoticed until someone finds the data wrong.`, { asyncJobDays: [ASYNC_JOB_DAYS, "day"] }),
      recommendation: said`Find out why each job failed and fix the cause, then send failures to someone who will act on them.`,
      recommendationType: "change",
      severity: failed.length + withErrors.length >= FAILURES_MEDIUM ? "medium" : "low",
      confidence: "high",
      evidence,
      value: notSized(said`We can't size the work these jobs left undone from the job log.`),
      effort: { size: "S" },
      owner: "Salesforce developer",
      segmentsChecked: segments,
      subjects: classes.map(([name]) => ({ type: "apex class", label: name })),
      facts,
    };
  },
};

const hardcodedEndpoints: Rule = {
  id: "int.hardcoded-endpoints",
  version: 1,
  domain: DOMAIN,
  title: "Callouts to addresses written into code",
  evaluate(ctx): RuleResult | RuleResult[] {
    const { classes } = ctx.data(apexExtractor);
    if (classes.status !== "ok") return { outcome: "not_measurable", reason: apexGap("classes", classes.reason), evidence: [ctx.ev(classes.source, { count: null })] };
    const { namedCredentials } = ctx.data(orgDocumentationExtractor);
    const code = classes.data.filter((c) => c.Status === "Active" && !c.isTest && c.scan !== null);
    const hidden = classes.data.filter((c) => c.Status === "Active" && c.scan === null).length;
    const callouts = code.filter((c) => c.scan!.literalEndpoints + c.scan!.namedCredentialEndpoints > 0);
    const literal = callouts.filter((c) => c.scan!.literalEndpoints > 0).sort((a, b) => a.Name.localeCompare(b.Name));
    const addresses = literal.reduce((sum, c) => sum + c.scan!.literalEndpoints, 0);
    const evidence = [
      ctx.ev(classes.source, { count: callouts.length, exclusions: ["Test classes", "Inactive classes", "Installed packages' classes", ...(hidden ? [`Classes whose code is hidden: ${hidden}`] : [])], note: "Read from the code: the address each setEndpoint call is given" }),
      ctx.ev(namedCredentials.source, { count: namedCredentials.status === "ok" ? namedCredentials.data.length : null }),
    ];
    const segments = [{ dimension: "class", segments: code.length }];
    const facts = { classes: code.length, callouts: callouts.length, literal: literal.length, addresses };
    if (literal.length === 0) {
      return { outcome: "pass", summary: callouts.length ? valued(said`All ${plural(callouts.length, "class", "classes")} that set a callout address use a named credential.`, { callouts: callouts.length }) : said`None of the code we read sets a callout address itself.`, evidence, segmentsChecked: segments, facts };
    }
    return {
      outcome: "issue",
      title: said`Some code calls other systems at addresses written into it`,
      summary: valued(said`${literal.length} of ${plural(callouts.length, "class", "classes")} that set a callout address ${literal.length === 1 ? "writes" : "write"} it into the code rather than using a named credential, ${plural(addresses, "address", "addresses")} in all. Each change of address then needs a code release, and the login for the other system has to be kept somewhere else. Salesforce's guidance is to manage callouts' authentication centrally through named credentials.`, { literal: literal.length, callouts: callouts.length, addresses }),
      recommendation: said`Move each address into a named credential, with its authentication in an external credential, and call it with callout: in the code.`,
      recommendationType: "change",
      severity: "medium",
      confidence: "medium",
      evidence,
      value: notSized(said`The cost is slower, riskier changes to integrations, which we can't size from the code.`),
      effort: { size: literal.length > 5 ? "M" : "S" },
      owner: "Salesforce developer",
      dependencies: ["Test each callout in a sandbox once it uses the named credential"],
      segmentsChecked: segments,
      subjects: literal.map((c) => ({ type: "apex class", label: c.Name })),
      facts,
    };
  },
};

export const integrationsModule: DomainModule = {
  id: DOMAIN,
  cloud: "sales",
  label: "Integrations",
  // The documentation read lists the objects the record counts name, or the
  // custom objects when the counts can't be read, so both go before it.
  extractors: [orgExtractor, usersExtractor, fieldDescribeExtractor, loginsExtractor, licencesExtractor, recordCountsExtractor, customObjectsExtractor, orgDocumentationExtractor, apexExtractor, apiOnlyExtractor, apiLimitsExtractor, asyncJobsExtractor],
  rules: [personalIntegrations, interactiveIntegrations, integrationLicences, selfAuthorisedApps, insecureRemoteSites, hardcodedEndpoints, apiUsage, failedJobs],
};
