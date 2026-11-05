import { FIELD_SPECS, planFields } from "./fields.ts";
import { readSoql, soqlDateTime, type Extractor, type Sourced } from "./types.ts";

// Who logs in, on which days and how, across the analysis window. Salesforce
// keeps login history for a limited time, so the months it covers are worked
// out from the rows themselves, never assumed (QUESTIONS.md).

export type LoginChannel = "browser" | "mobile app" | "single sign-on" | "api" | "other";

export type LoginActivity = {
  /** The start asked for. */
  from: string;
  /** The oldest successful login read, or null when there were none. */
  earliest: string | null;
  /** The row cap was reached, so the oldest logins weren't read. */
  truncated: boolean;
  rowsRead: number;
  failedOrOther: number;
  /** Days with at least one successful login that isn't an API login, by user. YYYY-MM-DD, in UTC, sorted. */
  days: Record<string, string[]>;
  /** Successful logins by channel. */
  channels: Record<LoginChannel, number>;
  /** Which fields the channel was read from. A missing one can hide a channel. */
  channelFields: string[];
};

const MAX_ROWS = 100_000;

type Row = { UserId: string; LoginTime: string; Status: string; ApiType?: string | null; Application?: string | null; LoginType?: string | null; Platform?: string | null; AuthenticationServiceId?: string | null };

// LoginType's values are documented (Object Reference, API 68.0). Application
// and Platform are free text, so the app's names are our reading of them.
const SSO_TYPES = /^(Saml|Saml2|SamlSite|ThirdPartySso|SAML |Third Party SSO)/i;
const MOBILE_APP = /Salesforce for (iOS|Android)|Salesforce Mobile|SalesforceA|\bChatter for (iOS|Android)/i;
const MOBILE_PLATFORM = /\b(iPhone|iPad|iOS|Android)\b/i;

/** How someone signed in, first match wins: an API session, the mobile app, single sign-on, a browser, or something else. */
export function channelOf(row: Pick<Row, "ApiType" | "Application" | "LoginType" | "Platform" | "AuthenticationServiceId">): LoginChannel {
  if (row.ApiType || row.LoginType === "OtherApi" || row.LoginType === "Other Apex API") return "api";
  if (MOBILE_APP.test(row.Application ?? "") || (MOBILE_PLATFORM.test(row.Platform ?? "") && /Remote Access|Oauth/i.test(row.LoginType ?? ""))) return "mobile app";
  if (SSO_TYPES.test(row.LoginType ?? "") || row.AuthenticationServiceId) return "single sign-on";
  if (row.LoginType === "Application" || /^Browser$/i.test(row.Application ?? "")) return "browser";
  return "other";
}

export const loginActivityExtractor: Extractor<Sourced<LoginActivity>> = {
  id: "login-activity",
  version: 1,
  async run(ctx) {
    const from = ctx.windowStart;
    const plan = planFields(ctx, FIELD_SPECS.loginActivity);
    const where = `WHERE LoginTime >= ${soqlDateTime(from)} ORDER BY LoginTime DESC`;
    if (!plan.ok) return { status: "unavailable", reason: plan.reason, source: { kind: "soql", api: "rest", query: `SELECT ${FIELD_SPECS.loginActivity.required.join(", ")} FROM LoginHistory ${where}` } };
    // Newest first, so a capped read loses the oldest months, which are then left out.
    const rows = await readSoql<Row>(ctx, `SELECT ${plan.select} FROM LoginHistory ${where}`, { maxRecords: MAX_ROWS, missingFields: plan.missing });
    if (rows.status !== "ok") return rows;
    const activity: LoginActivity = {
      from: from.toISOString().slice(0, 10),
      earliest: null,
      truncated: rows.truncated === true,
      rowsRead: rows.data.length,
      failedOrOther: 0,
      days: {},
      channels: { browser: 0, "mobile app": 0, "single sign-on": 0, api: 0, other: 0 },
      channelFields: plan.select.split(", ").filter((f) => ["ApiType", "Application", "LoginType", "Platform", "AuthenticationServiceId"].includes(f)),
    };
    const days = new Map<string, Set<string>>();
    for (const row of rows.data) {
      // Status can't be filtered in SOQL, so failures are split out here.
      if (row.Status !== "Success") {
        activity.failedOrOther++;
        continue;
      }
      const at = new Date(row.LoginTime.replace(/\+0000$/, "Z"));
      if (Number.isNaN(at.getTime()) || at < from) continue;
      const iso = at.toISOString();
      if (!activity.earliest || iso < activity.earliest) activity.earliest = iso;
      const channel = channelOf(row);
      activity.channels[channel]++;
      // An API session is a program working, not a person using Salesforce.
      if (channel === "api") continue;
      const user = days.get(row.UserId) ?? new Set<string>();
      user.add(iso.slice(0, 10));
      days.set(row.UserId, user);
    }
    activity.days = Object.fromEntries([...days].sort(([a], [b]) => a.localeCompare(b)).map(([id, set]) => [id, [...set].sort()]));
    return { ...rows, data: activity };
  },
};
