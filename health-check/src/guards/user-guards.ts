import { apexExtractor, metadataExtractor, scheduledJobsExtractor } from "../extract/automation.ts";
import { normaliseUserKey } from "../extract/metadata-parse.ts";
import { loginsExtractor, ownershipExtractor, runAsExtractor, usersExtractor, type UserRow } from "../extract/users.ts";
import type { RuleContext } from "../engine/rule.ts";
import type { Guard, GuardStatus } from "../schemas/index.ts";

// Rule 3. A real check of ours once recommended removing shared users who owned
// most of the accounts, so a user is only recommended for removal once every
// check below passes. Each one fails closed: if it can't finish, it's "unknown",
// and unknown blocks.

export const USER_GUARD_IDS = [
  "not-shared-or-system",
  "owns-no-records",
  "not-referenced-in-automation",
  "runs-no-automation",
  "not-default-workflow-user",
  "not-in-user-hierarchy",
  "no-recent-login",
] as const;

/** What stops a release, in the reader's words. A guard id is ours, not theirs. */
export const USER_GUARD_BLOCKERS: Record<(typeof USER_GUARD_IDS)[number], string> = {
  "not-shared-or-system": "may be a shared or system account",
  "owns-no-records": "still owns records",
  "not-referenced-in-automation": "is named in automation",
  "runs-no-automation": "automation runs as them",
  "not-default-workflow-user": "may be the Default Workflow User",
  "not-in-user-hierarchy": "other users report to them",
  "no-recent-login": "has logged in recently",
};

const SHARED_USERNAME = /^(sales|support|service|info|admin|demo|test|team|shared|integration|api|system|noreply|no-reply|marketing|accounts|hello|office)[.+_-]?\d*@/i;
const NON_PERSON_KINDS = new Set(["shared", "system", "integration", "test"]);

/** Looks like an account people share or a system uses, rather than one person's seat. */
export function looksShared(user: Pick<UserRow, "Name" | "Username">): boolean {
  return SHARED_USERNAME.test(user.Username) || !/\s/.test(user.Name.trim());
}

const guard = (guardId: string, subject: string, status: GuardStatus, detail: string, evidence: Guard["evidence"]): Guard => ({ guardId, subject, status, detail, evidence });

/** Whether the user owns records, from the ownership scan. Every run but a set-up review reads it. */
function ownershipGuard(ctx: RuleContext, user: UserRow, subject: string): Guard {
  const ownership = ctx.data(ownershipExtractor);
  const owned = ownership.byOwner[user.Id] ?? {};
  const ownedTotal = Object.values(owned).reduce((a, b) => a + b, 0);
  const ownershipEvidence = [
    ctx.ev({ kind: "soql", api: "rest", query: ownership.query }, { count: ownedTotal, note: `Objects scanned: ${ownership.scanned.join(", ")}` }),
  ];
  if (ownedTotal > 0) {
    const list = Object.entries(owned).map(([object, count]) => `${count} ${object}`).join(", ");
    return guard("owns-no-records", subject, "failed", `Owns ${ownedTotal} records: ${list}.`, ownershipEvidence);
  }
  if (ownership.incomplete.length > 0) return guard("owns-no-records", subject, "unknown", `The ownership scan couldn't read: ${ownership.incomplete.join(", ")}.`, ownershipEvidence);
  return guard("owns-no-records", subject, "passed", `Owns no records across ${ownership.scanned.length} objects.`, ownershipEvidence);
}

export function userGuards(ctx: RuleContext, user: UserRow): Guard[] {
  const subject = ctx.userLabel(user.Id);
  const users = ctx.data(usersExtractor);
  const metadata = ctx.data(metadataExtractor);
  const apex = ctx.data(apexExtractor);
  const runAs = ctx.data(runAsExtractor);
  const jobs = ctx.data(scheduledJobsExtractor);
  const logins = ctx.data(loginsExtractor);
  const guards: Guard[] = [];

  // Shared or system account: the calibration's word beats our pattern match.
  const calibrated = ctx.calibration.users.find((u) => u.userId.slice(0, 15) === user.Id.slice(0, 15))?.kind;
  const usersEvidence = [ctx.ev(users.source, { count: 1 })];
  if (calibrated && NON_PERSON_KINDS.has(calibrated)) {
    guards.push(guard("not-shared-or-system", subject, "failed", `The calibration marks this user as ${calibrated}.`, usersEvidence));
  } else if (!calibrated && looksShared(user)) {
    guards.push(guard("not-shared-or-system", subject, "failed", "The name or username looks like a shared or system account, and the calibration doesn't classify it.", usersEvidence));
  } else {
    guards.push(guard("not-shared-or-system", subject, "passed", "This looks like one person's account.", usersEvidence));
  }

  // Record ownership, across the core objects and populated custom objects. A
  // set-up review never reads records, so there the guard stays unread, and
  // unread blocks (DECISIONS 97).
  if (ctx.scope === "essentials") {
    guards.push(guard("owns-no-records", subject, "unknown", "A set-up review doesn't read who owns which records, so check by hand that this user owns none.", [{ kind: "engine", path: "owns-no-records", count: null, dateRange: null, exclusions: [], capturedAt: ctx.now.toISOString(), note: "Not read in a set-up review" }]));
  } else guards.push(ownershipGuard(ctx, user, subject));

  // Named in flows, workflow, approvals, assignment rules or Apex.
  const keys = [user.Id, user.Username, user.Email].map(normaliseUserKey);
  const mentions = new Set<string>();
  if (metadata.status === "ok") for (const key of keys) for (const path of metadata.data.references.users[key] ?? []) mentions.add(path);
  for (const key of keys) for (const path of apex.references.users[key] ?? []) mentions.add(path);
  const referenceEvidence = [ctx.ev(metadata.source, { count: mentions.size, note: "Searched flows, workflow rules, approval processes, assignment rules, validation rules and Apex" })];
  if (mentions.size > 0) {
    guards.push(guard("not-referenced-in-automation", subject, "failed", `Referenced in ${[...mentions].sort().join(", ")}.`, referenceEvidence));
  } else if (metadata.status !== "ok" || apex.classes.status !== "ok" || apex.triggers.status !== "ok") {
    guards.push(guard("not-referenced-in-automation", subject, "unknown", "Metadata or Apex couldn't be read, so references couldn't be searched.", referenceEvidence));
  } else if (metadata.data.unsearched?.length) {
    guards.push(guard("not-referenced-in-automation", subject, "unknown", `Not in what we searched, but we couldn't search these: ${metadata.data.unsearched.join(", ")}.`, referenceEvidence));
  } else {
    guards.push(guard("not-referenced-in-automation", subject, "passed", "Not referenced in the metadata or Apex we searched.", referenceEvidence));
  }

  // Runs something as this user: scheduled jobs, dashboards, email services and so on.
  const links = runAs.links.filter((l) => l.userId === user.Id).map((l) => `${l.kind} ${l.name}`);
  if (jobs.status === "ok") {
    for (const job of jobs.data) if (job.OwnerId === user.Id) links.push(`scheduled job ${job.CronJobDetail?.Name ?? job.Id}`);
  }
  const runAsSource = runAs.sources[0] ?? jobs.source;
  const runAsEvidence = [ctx.ev(jobs.source, { count: links.length }), ctx.ev(runAsSource, { count: links.length })];
  if (links.length > 0) {
    guards.push(guard("runs-no-automation", subject, "failed", `Runs ${links.sort().join(", ")}.`, runAsEvidence));
  } else if (runAs.incomplete.length > 0 || jobs.status !== "ok") {
    const gaps = [...runAs.incomplete, ...(jobs.status === "ok" ? [] : ["scheduled jobs"])];
    guards.push(guard("runs-no-automation", subject, "unknown", `Couldn't read: ${gaps.join(", ")}.`, runAsEvidence));
  } else {
    guards.push(guard("runs-no-automation", subject, "passed", "Nothing we can see runs as this user.", runAsEvidence));
  }

  // Default Workflow User, which only a person can read for us.
  const manual = ctx.manual("default-workflow-user");
  if (manual.status === "answered") {
    const answered = String(manual.answer?.values.username ?? "").trim().toLowerCase();
    if (answered === user.Username.toLowerCase()) {
      guards.push(guard("not-default-workflow-user", subject, "failed", "This user is the Default Workflow User.", [manual.evidence]));
    } else {
      guards.push(guard("not-default-workflow-user", subject, "passed", "Another user is the Default Workflow User.", [manual.evidence]));
    }
  } else {
    guards.push(guard("not-default-workflow-user", subject, "unknown", `The manual step is ${manual.status}, so we can't rule this user out.`, [manual.evidence]));
  }

  // Someone's manager or delegated approver. Where the org has no such field,
  // nobody can hold that role, so its absence passes rather than blocks.
  if (users.status === "ok") {
    const dependants = users.data.filter((u) => u.IsActive && u.Id !== user.Id && (u.ManagerId === user.Id || u.DelegatedApproverId === user.Id));
    if (dependants.length > 0) {
      guards.push(guard("not-in-user-hierarchy", subject, "failed", `Manager or delegated approver for ${dependants.length} active users.`, [ctx.ev(users.source, { count: dependants.length })]));
    } else {
      guards.push(guard("not-in-user-hierarchy", subject, "passed", "Nobody active reports to or delegates to this user.", usersEvidence));
    }
  } else {
    guards.push(guard("not-in-user-hierarchy", subject, "unknown", "Users couldn't be read.", usersEvidence));
  }

  // LoginHistory can disagree with LastLoginDate, so check the login log as well.
  if (logins.status !== "ok") {
    guards.push(guard("no-recent-login", subject, "unknown", `LoginHistory couldn't be read: ${logins.reason}`, [ctx.ev(logins.source, { count: null })]));
  } else {
    const seen = logins.data.byUser[user.Id];
    const evidence = [ctx.ev(logins.source, { count: seen?.successes ?? 0, dateRange: { from: logins.data.from, to: ctx.window.to } })];
    if (seen) {
      guards.push(guard("no-recent-login", subject, "failed", `${seen.successes} successful logins since ${logins.data.from}.`, evidence));
    } else if (logins.truncated) {
      guards.push(guard("no-recent-login", subject, "unknown", "The login log was too long to read in full.", evidence));
    } else {
      guards.push(guard("no-recent-login", subject, "passed", `No successful login since ${logins.data.from}.`, evidence));
    }
  }

  return guards;
}
