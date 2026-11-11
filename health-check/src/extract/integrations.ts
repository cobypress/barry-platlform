import type { OrgLimit } from "../salesforce/client.ts";
import { daysBefore, readSoql, soqlWindow, type Extractor, type Sourced } from "./types.ts";

// What the integrations area reads beyond what security and "Your org,
// documented" already hold: who is API-only, the org's API and async Apex use,
// and the async jobs that ran this week. None of it is a CRM record.

export type ApiOnlySet = { Id: string; IsOwnedByProfile: boolean; Profile: { Name: string } | null };
export type ApiOnlyData = { sets: Sourced<ApiOnlySet[]>; assignments: Sourced<Array<{ AssigneeId: string; PermissionSetId: string }>> };

// Profiles and permission sets apart, as for Modify All Data: a profile's grant
// reaches its users through the profile, a permission set's through assignment.
export const apiOnlyExtractor: Extractor<ApiOnlyData> = {
  id: "api-only",
  version: 1,
  async run(ctx) {
    return {
      sets: await readSoql<ApiOnlySet>(ctx, "SELECT Id, IsOwnedByProfile, Profile.Name FROM PermissionSet WHERE PermissionsApiUserOnly = true"),
      assignments: await readSoql(ctx, "SELECT AssigneeId, PermissionSetId FROM PermissionSetAssignment WHERE PermissionSet.PermissionsApiUserOnly = true AND PermissionSet.IsOwnedByProfile = false AND Assignee.IsActive = true"),
    };
  },
};

/** The org's use of its daily allocations, as the connection first read them. */
export const apiLimitsExtractor: Extractor<Sourced<Record<string, OrgLimit>>> = {
  id: "api-limits",
  version: 1,
  run(ctx) {
    const source = { kind: "rest", path: `/services/data/v${ctx.client.apiVersion}/limits` } as const;
    const limits = ctx.client.limitsAtConnect;
    if (Object.keys(limits).length === 0) return Promise.resolve({ status: "unavailable", reason: "The org reported none of the daily limits we read.", source });
    return Promise.resolve({ status: "ok", data: limits, source, truncated: false });
  },
};

/** How far back jobs are read. We haven't confirmed how long Salesforce keeps them (QUESTIONS.md), so a week. */
export const ASYNC_JOB_DAYS = 7;
const ASYNC_JOB_MAX_ROWS = 10_000;

export type AsyncJobRow = { Id: string; JobType: string; Status: string; NumberOfErrors: number | null; ApexClass: { Name: string } | null; CreatedDate: string };

// The error text (ExtendedStatus) isn't read: it can quote a record.
export const asyncJobsExtractor: Extractor<Sourced<AsyncJobRow[]>> = {
  id: "async-jobs",
  version: 1,
  run: (ctx) =>
    readSoql<AsyncJobRow>(ctx, `SELECT Id, JobType, Status, NumberOfErrors, ApexClass.Name, CreatedDate FROM AsyncApexJob WHERE ${soqlWindow("CreatedDate", daysBefore(ctx.now, ASYNC_JOB_DAYS), ctx.now)}`, { maxRecords: ASYNC_JOB_MAX_ROWS }),
};
