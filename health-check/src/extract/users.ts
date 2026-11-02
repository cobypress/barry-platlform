import { isUnavailable } from "../salesforce/client.ts";
import { isNoOwnerField, recordCountsExtractor } from "./configuration.ts";
import { daysBefore, readSoql, soqlDateTime, type ExtractContext, type Extractor, type Sourced, type SourceInfo } from "./types.ts";

export type UserRow = {
  Id: string;
  Name: string;
  Username: string;
  Email: string;
  IsActive: boolean;
  UserType: string;
  Profile: { Name: string; UserLicense: { Name: string } | null } | null;
  UserRole: { Name: string } | null;
  LastLoginDate: string | null;
  CreatedDate: string;
  ManagerId: string | null;
  DelegatedApproverId: string | null;
};

export const usersExtractor: Extractor<Sourced<UserRow[]>> = {
  id: "users",
  version: 1,
  run: (ctx) =>
    readSoql<UserRow>(
      ctx,
      "SELECT Id, Name, Username, Email, IsActive, UserType, Profile.Name, Profile.UserLicense.Name, UserRole.Name, " +
        "LastLoginDate, CreatedDate, ManagerId, DelegatedApproverId FROM User WHERE UserType = 'Standard'",
    ),
};

export type UserLicenseRow = { Name: string; TotalLicenses: number; UsedLicenses: number; Status: string };
export type PermissionSetLicenseRow = { MasterLabel: string; TotalLicenses: number; UsedLicenses: number; Status: string; ExpirationDate: string | null };
export type LicenceData = { userLicenses: Sourced<UserLicenseRow[]>; permissionSetLicenses: Sourced<PermissionSetLicenseRow[]> };

export const licencesExtractor: Extractor<LicenceData> = {
  id: "licences",
  version: 1,
  async run(ctx) {
    return {
      userLicenses: await readSoql(ctx, "SELECT Name, TotalLicenses, UsedLicenses, Status FROM UserLicense"),
      permissionSetLicenses: await readSoql(ctx, "SELECT MasterLabel, TotalLicenses, UsedLicenses, Status, ExpirationDate FROM PermissionSetLicense"),
    };
  },
};

export type LoginSummary = { from: string; rowsRead: number; failedOrOther: number; byUser: Record<string, { successes: number; lastSuccess: string }> };

const LOGIN_DAYS = 90;
const LOGIN_MAX_ROWS = 20_000;

export const loginsExtractor: Extractor<Sourced<LoginSummary>> = {
  id: "logins",
  version: 1,
  async run(ctx) {
    const from = daysBefore(ctx.now, LOGIN_DAYS);
    // Status can't be filtered in SOQL, so failures are split out here.
    const rows = await readSoql<{ UserId: string; LoginTime: string; Status: string }>(
      ctx,
      `SELECT UserId, LoginTime, Status FROM LoginHistory WHERE LoginTime >= ${soqlDateTime(from)} ORDER BY LoginTime DESC`,
      { maxRecords: LOGIN_MAX_ROWS },
    );
    if (rows.status !== "ok") return rows;
    const summary: LoginSummary = { from: from.toISOString().slice(0, 10), rowsRead: rows.data.length, failedOrOther: 0, byUser: {} };
    for (const row of rows.data) {
      if (row.Status !== "Success") {
        summary.failedOrOther++;
        continue;
      }
      const user = (summary.byUser[row.UserId] ??= { successes: 0, lastSuccess: row.LoginTime });
      user.successes++;
      if (row.LoginTime > user.lastSuccess) user.lastSuccess = row.LoginTime;
    }
    return { ...rows, data: summary };
  },
};

export type OwnershipScan = {
  byOwner: Record<string, Record<string, number>>;
  scanned: string[];
  /** Objects we tried and failed to read. Non-empty means a clean answer isn't proof. */
  incomplete: string[];
  query: string;
};

const CORE_OWNED = ["Account", "Contact", "Lead", "Opportunity", "Case"];
const MAX_CUSTOM_OBJECTS = 40;
const OWNER_QUERY = (object: string): string => `SELECT OwnerId, COUNT(Id) c FROM ${object} GROUP BY OwnerId`;

// Per object rather than per user, so the cost doesn't grow with the user count.
export const ownershipExtractor: Extractor<OwnershipScan> = {
  id: "ownership",
  version: 1,
  async run(ctx) {
    const counts = ctx.get(recordCountsExtractor);
    const incomplete: string[] = [];
    let custom: string[] = [];
    if (counts.status === "ok") {
      const populated = counts.data.filter((c) => c.name.endsWith("__c") && c.count > 0).sort((a, b) => b.count - a.count);
      custom = populated.slice(0, MAX_CUSTOM_OBJECTS).map((c) => c.name);
      incomplete.push(...populated.slice(MAX_CUSTOM_OBJECTS).map((c) => c.name));
    } else {
      incomplete.push("custom objects");
    }
    const objects = [...CORE_OWNED, ...custom];
    const results = await ctx.client.queryBatch<{ OwnerId: string; c: number }>(objects.map(OWNER_QUERY));
    const scan: OwnershipScan = { byOwner: {}, scanned: [], incomplete, query: OWNER_QUERY("<object>") };
    results.forEach((result, i) => {
      const object = objects[i]!;
      if (!result.ok) {
        if (!isNoOwnerField(result.error)) scan.incomplete.push(object);
        return;
      }
      // Aggregates can't page, so a capped result can't prove a user owns nothing.
      if (result.result.truncated) scan.incomplete.push(object);
      scan.scanned.push(object);
      for (const row of result.result.records) (scan.byOwner[row.OwnerId] ??= {})[object] = row.c;
    });
    return scan;
  },
};

export type RunAsLink = { userId: string; kind: string; name: string };
export type RunAsData = {
  links: RunAsLink[];
  sources: SourceInfo[];
  incomplete: string[];
  dashboards: Sourced<Array<{ Id: string; Title: string; Type: string; RunningUserId: string | null; FolderName: string | null }>>;
};

async function collectRunAs<R>(ctx: ExtractContext, data: RunAsData, label: string, soql: string, toLinks: (row: R) => RunAsLink[], api: "rest" | "tooling" = "rest"): Promise<void> {
  const source: SourceInfo = { kind: "soql", api, query: soql };
  data.sources.push(source);
  try {
    const rows = await ctx.client.queryAll<R>(soql, { api });
    data.links.push(...rows.flatMap(toLinks));
  } catch (err) {
    if (!isUnavailable(err)) throw err;
    // INVALID_TYPE: the feature isn't in this org, so nothing can run through it.
    if (err.code !== "INVALID_TYPE") data.incomplete.push(label);
  }
}

// Things that keep working as a user after they've stopped logging in.
export const runAsExtractor: Extractor<RunAsData> = {
  id: "run-as",
  version: 1,
  async run(ctx) {
    const data: RunAsData = {
      links: [],
      sources: [],
      incomplete: [],
      dashboards: await readSoql(ctx, "SELECT Id, Title, Type, RunningUserId, FolderName FROM Dashboard"),
    };
    if (data.dashboards.status === "ok") {
      data.sources.push(data.dashboards.source);
      for (const d of data.dashboards.data) {
        if (d.RunningUserId) data.links.push({ userId: d.RunningUserId, kind: "dashboard running user", name: d.Title });
      }
    } else {
      data.incomplete.push("dashboards");
    }
    await collectRunAs<{ LocalPart: string; RunAsUserId: string | null }>(ctx, data, "email services", "SELECT LocalPart, RunAsUserId FROM EmailServicesAddress", (r) =>
      r.RunAsUserId ? [{ userId: r.RunAsUserId, kind: "email service", name: r.LocalPart }] : [],
    );
    await collectRunAs<{ DeveloperName: string; ExecutionUserId: string | null }>(ctx, data, "auth providers", "SELECT DeveloperName, ExecutionUserId FROM AuthProvider", (r) =>
      r.ExecutionUserId ? [{ userId: r.ExecutionUserId, kind: "auth provider", name: r.DeveloperName }] : [],
    );
    await collectRunAs<{ Name: string; AdminId: string | null }>(ctx, data, "sites", "SELECT Name, AdminId FROM Site", (r) =>
      r.AdminId ? [{ userId: r.AdminId, kind: "site administrator", name: r.Name }] : [],
    );
    await collectRunAs<{ DeveloperName: string; UserId: string | null }>(
      ctx,
      data,
      "platform event subscribers",
      "SELECT DeveloperName, UserId FROM PlatformEventSubscriberConfig",
      (r) => (r.UserId ? [{ userId: r.UserId, kind: "platform event subscriber", name: r.DeveloperName }] : []),
      "tooling",
    );
    return data;
  },
};

export const reportsExtractor: Extractor<Sourced<Array<{ Id: string; FolderName: string | null; LastRunDate: string | null; CreatedDate: string }>>> = {
  id: "reports",
  version: 1,
  run: (ctx) => readSoql(ctx, "SELECT Id, FolderName, LastRunDate, CreatedDate FROM Report"),
};

// Only the connection state: EmailAddress and UserName are personal, and no rule needs them.
export const activityCaptureExtractor: Extractor<Sourced<Array<{ UserId: string; ConnectivityStatus: string | null }>>> = {
  id: "activity-capture",
  version: 1,
  run: (ctx) => readSoql(ctx, "SELECT UserId, ConnectivityStatus FROM ActivityUsrConnectionStatus"),
};
