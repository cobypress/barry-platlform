import { metadataExtractor, scheduledJobsExtractor } from "../../extract/automation.ts";
import { named, aside } from "../../engine/named.ts";
import { userKinds } from "../user-kinds.ts";
import { componentChangesExtractor } from "../../extract/component-changes.ts";
import { ownershipExtractor, usersExtractor, type UserRow } from "../../extract/users.ts";
import { recordCountsExtractor } from "../../extract/configuration.ts";
import { looksShared } from "../../guards/user-guards.ts";
import { notSized, plural, segmentRate, type IssueResult, type Rule, type RuleResult } from "../../engine/rule.ts";

import type { DomainModule } from "../types.ts";
import { labelsOf } from "../labels.ts";
import { labelsExtractor } from "../../extract/labels.ts";
import { WHERE } from "../where.ts";
import { joinSaid, org, said, type Said } from "../../engine/org-words.ts";
import { grouped } from "../../engine/said-forms.ts";

// Rules that compare the org with something Salesforce itself says. The checks
// in other domains are mostly ours: these quote the position, name where it came
// from and say when we last checked it, so a report can tell them apart and a
// change in Salesforce's advice only means bumping one rule's version.
//
// Every title, summary, recommendation and reason is written with `said`, and
// every name the org chose with `org`, so the digest can say which words are the
// org's (DECISIONS.md 655, 657). A reason passed on from an extract quotes
// Salesforce and stays plain: unknown.

export type Practice = {
  /** Our own sentences, made with said where they're declared. */
  position: Said;
  source: Said;
  /** "docs" is Salesforce's own documentation. "secondary" lowers the finding's confidence. */
  basis: "docs" | "secondary";
  checkedOn: Said;
};

const NON_PERSON_KINDS = new Set(["shared", "system", "integration", "test"]);
const ROLE_GAP_LIMIT = 10;
const MIN_SEGMENT = 5;
const OWNER_SKEW_RECORDS = 10_000;

const confidenceOf = (practice: Practice): IssueResult["confidence"] => (practice.basis === "docs" ? "high" : "medium");
const noted = (practice: Practice): Said => named(said`Salesforce's position: ${practice.position} Source: ${practice.source}, checked ${practice.checkedOn}${practice.basis === "secondary" ? ", from secondary sources" : ""}.`, { checkedOn: [practice.checkedOn, "date"] });

export const ROLE_PRACTICE: Practice = {
  position: said`The forecast hierarchy is generated from the role hierarchy, and role-based sharing and reporting rely on it too.`,
  source: said`Salesforce Forecasts documentation`,
  basis: "docs",
  checkedOn: said`2026-11-02`,
};

const usersWithoutRole: Rule = {
  id: "bp.users-without-role",
  version: 1,
  domain: "best_practice",
  title: "People with no role",
  where: WHERE.users,
  evaluate(ctx): RuleResult | RuleResult[] {
    const users = ctx.data(usersExtractor);
    if (users.status !== "ok") return { outcome: "not_measurable", reason: `Users couldn't be read: ${users.reason}`, evidence: [ctx.ev(users.source, { count: null })] };
    const kindOf = (u: UserRow): string | undefined => userKinds(ctx).find((c) => c.userId.slice(0, 15) === u.Id.slice(0, 15))?.kind;
    const inScope = (u: UserRow): boolean => {
      const kind = kindOf(u);
      if (!u.IsActive || u.Profile?.Name === "System Administrator" || kind === "admin") return false;
      return kind ? !NON_PERSON_KINDS.has(kind) : !looksShared(u);
    };
    const people = users.data.filter(inScope);
    const exclusions = ["Inactive users", "Admins", "Accounts calibrated as shared, system, integration or test"];
    const evidence = [ctx.ev(users.source, { count: people.length, exclusions, note: noted(ROLE_PRACTICE) })];
    if (people.length === 0) return { outcome: "pass", summary: said`There are no people in scope to check.`, evidence, segmentExemptReason: "There are no users to segment." };
    const result = segmentRate(people, (u) => u.UserRole === null, { profile: (u) => u.Profile?.Name ?? "No profile", licence: (u) => u.Profile?.UserLicense?.Name ?? "Unknown" }, { segmentLimit: 25, minSegment: MIN_SEGMENT });
    if (result.rate <= ROLE_GAP_LIMIT && result.hotspots.length === 0) {
      return { outcome: "pass", summary: named(said`People with no role: ${result.hits} of ${result.n} (${result.rate}%), with no profile above 25%. ${noted(ROLE_PRACTICE)}`, { hits: result.hits, n: result.n, rate: [result.rate, "percent"] }), evidence, segmentsChecked: result.segmentsChecked, facts: { hits: result.hits, n: result.n, rate: result.rate } };
    }
    return {
      outcome: "issue",
      title: said`Some people sit outside the role hierarchy`,
      summary: named(said`People with no role: ${result.hits} of ${result.n} (${result.rate}%). Their deals won't roll up to a manager's forecast. ${noted(ROLE_PRACTICE)}`, { hits: result.hits, n: result.n, rate: [result.rate, "percent"] }),
      recommendation: said`Give each of these people a role, or confirm with the business that they don't sell or need to see team data.`,
      recommendationType: "change",
      severity: "medium",
      confidence: confidenceOf(ROLE_PRACTICE),
      evidence,
      value: notSized(said`This changes what managers can see, which has no direct figure.`),
      effort: { size: "XS" },
      owner: "Salesforce admin",
      dependencies: ["Check what each new role can see through sharing before assigning it"],
      segmentsChecked: result.segmentsChecked,
      subjects: people.filter((u) => u.UserRole === null).map((u) => ({ type: "user", label: ctx.userLabel(u.Id) })),
      facts: { hits: result.hits, n: result.n, rate: result.rate },
      records: { object: "User", rows: people.filter((u) => u.UserRole === null).map((u) => ({ id: u.Id, problem: "No role" })), fix: "Give them a role, or confirm with the business that they don't need one" },
    };
  },
};

export const SKEW_PRACTICE: Practice = {
  position: said`A single user or queue owning more than 10,000 records of one object is ownership data skew, which can cause long-running sharing recalculations. Spread ownership across more users.`,
  source: said`Designing Record Access for Enterprise Scale`,
  basis: "docs",
  checkedOn: said`2026-11-02`,
};

const ownershipSkew: Rule = {
  id: "bp.ownership-skew",
  version: 2,
  records: true,
  domain: "best_practice",
  // The check's name, which a pass carries: "One" names no count (DECISIONS.md 773).
  title: aside("One owner holding too many records", "One"),
  evaluate(ctx): RuleResult | RuleResult[] {
    const ownership = ctx.data(ownershipExtractor);
    const source = { kind: "soql", api: "rest", query: ownership.query } as const;
    if (ownership.scanned.length === 0) return { outcome: "not_measurable", reason: said`Record ownership couldn't be read.`, evidence: [ctx.ev(source, { count: null })] };
    const skewed = Object.entries(ownership.byOwner).flatMap(([owner, counts]) =>
      Object.entries(counts).filter(([, n]) => n > OWNER_SKEW_RECORDS).map(([object, records]) => ({ owner, object, records })),
    );
    skewed.sort((a, b) => b.records - a.records || a.object.localeCompare(b.object) || a.owner.localeCompare(b.owner));
    const evidence = [ctx.ev(source, { count: skewed.length, exclusions: ownership.incomplete.length ? [`Objects we couldn't read: ${ownership.incomplete.join(", ")}`] : [], note: noted(SKEW_PRACTICE) })];
    const segments = [{ dimension: "object", segments: ownership.scanned.length }, { dimension: "owner", segments: Object.keys(ownership.byOwner).length }];
    if (skewed.length === 0) {
      if (ownership.incomplete.length > 0) return { outcome: "not_measurable", reason: said`No skew in what we could read, but these objects couldn't be read: ${joinSaid(ownership.incomplete.map((o) => said`${org(o)}`), ", ")}.`, evidence };
      return { outcome: "pass", summary: named(said`No owner holds more than ${OWNER_SKEW_RECORDS} records of any scanned object. ${noted(SKEW_PRACTICE)}`, { ownerSkewRecords: OWNER_SKEW_RECORDS }), evidence, segmentsChecked: segments };
    }
    const labelOf = (id: string): string => (id.startsWith("005") ? ctx.userLabel(id) : "A queue");
    return {
      outcome: "issue",
      title: said`A few owners hold enough records to slow sharing changes`,
      summary: named(said`${plural(skewed.length, "owner and object pair")} above ${grouped(OWNER_SKEW_RECORDS)} records. The largest is ${grouped(skewed[0]!.records)} ${org(labelsOf(ctx).object(skewed[0]!.object))} records. ${noted(SKEW_PRACTICE)}`, { skewed: skewed.length, ownerSkewRecords: OWNER_SKEW_RECORDS }),
      recommendation: said`Spread these records across more owners. If one owner has to stay, keep it out of the role hierarchy's lower levels and out of public groups used in sharing rules.`,
      recommendationType: "change",
      severity: "medium",
      confidence: confidenceOf(SKEW_PRACTICE),
      evidence,
      value: notSized(said`The cost is slower sharing changes, which we can't size from the record counts.`),
      effort: { size: "M" },
      owner: "Salesforce admin",
      dependencies: ["Check integrations that set this owner before moving records"],
      segmentsChecked: segments,
      subjects: skewed.map((s) => ({ type: "owner", label: `${labelOf(s.owner)}: ${labelsOf(ctx).object(s.object)}` })),
      facts: { pairs: skewed.length, largest: skewed[0]!.records, limit: OWNER_SKEW_RECORDS },
    };
  },
};

export const SAME_RECORD_PRACTICE: Practice = {
  position: said`Same-record field updates belong in a before-save flow or trigger. They're much faster there, and Salesforce most strongly recommends keeping them out of after-save automation.`,
  source: said`Salesforce Architects, Record-Triggered Automation decision guide`,
  basis: "docs",
  checkedOn: said`2026-11-02`,
};

const afterSaveSameRecord: Rule = {
  id: "bp.after-save-same-record-updates",
  version: 1,
  domain: "best_practice",
  title: "After-save flows updating their own record",
  where: WHERE.flows,
  evaluate(ctx): RuleResult | RuleResult[] {
    const metadata = ctx.data(metadataExtractor);
    if (metadata.status !== "ok") return { outcome: "not_measurable", reason: `Metadata couldn't be retrieved: ${metadata.reason}`, evidence: [ctx.ev(metadata.source, { count: null })] };
    const afterSave = metadata.data.flows.filter((f) => f.status === "Active" && f.triggerType === "RecordAfterSave");
    const hits = afterSave.filter((f) => f.sameRecordUpdates > 0);
    const evidence = [ctx.ev({ kind: "metadata", path: "flows/*.flow" }, { count: afterSave.length, exclusions: ["Inactive flows", "Flows that aren't record-triggered after save"], note: noted(SAME_RECORD_PRACTICE) })];
    const segments = [{ dimension: "object", segments: new Set(afterSave.map((f) => f.object ?? "Unknown")).size }];
    if (afterSave.length === 0) return { outcome: "pass", summary: said`There are no active after-save record-triggered flows.`, evidence, segmentExemptReason: "There are no flows to segment." };
    if (hits.length === 0) return { outcome: "pass", summary: named(said`None of the ${afterSave.length} active after-save flows updates its own record. ${noted(SAME_RECORD_PRACTICE)}`, { afterSave: afterSave.length }), evidence, segmentsChecked: segments };
    return {
      outcome: "issue",
      title: said`Flows are doing same-record updates the slow way`,
      summary: named(said`${plural(hits.length, "active after-save flow")} of ${afterSave.length} ${hits.length === 1 ? "updates" : "update"} the record that triggered ${hits.length === 1 ? "it" : "them"}. ${noted(SAME_RECORD_PRACTICE)}`, { hits: hits.length, afterSave: afterSave.length }),
      recommendation: said`Move the same-record updates to a before-save flow. Keep the rest of the flow where it is if it needs after-save actions.`,
      recommendationType: "change",
      severity: "low",
      confidence: confidenceOf(SAME_RECORD_PRACTICE),
      evidence,
      value: notSized(said`The gain is save speed, which we can't measure from metadata.`),
      effort: { size: "S" },
      owner: "Salesforce admin",
      dependencies: ["Test in a sandbox: a before-save flow runs earlier, so it can change the order things happen in"],
      segmentsChecked: segments,
      subjects: hits.map((f) => ({ type: "flow", label: f.name })),
      facts: { flows: hits.length, afterSaveFlows: afterSave.length },
    };
  },
};

export const JOB_OWNER_PRACTICE: Practice = {
  position: said`A scheduled job runs as the user who scheduled it, so a job left on a deactivated user can fail.`,
  source: said`Salesforce community and vendor support articles`,
  basis: "secondary",
  checkedOn: said`2026-11-02`,
};

const jobsOnInactiveOwners: Rule = {
  id: "bp.jobs-on-inactive-owners",
  version: 1,
  domain: "best_practice",
  title: "Scheduled jobs owned by inactive users",
  where: WHERE.scheduledJobs,
  evaluate(ctx): RuleResult | RuleResult[] {
    const jobs = ctx.data(scheduledJobsExtractor);
    const users = ctx.data(usersExtractor);
    if (jobs.status !== "ok") return { outcome: "not_measurable", reason: `Scheduled jobs couldn't be read: ${jobs.reason}`, evidence: [ctx.ev(jobs.source, { count: null })] };
    if (users.status !== "ok") return { outcome: "not_measurable", reason: `Users couldn't be read: ${users.reason}`, evidence: [ctx.ev(users.source, { count: null })] };
    const inactive = new Set(users.data.filter((u) => !u.IsActive).map((u) => u.Id));
    const hits = jobs.data.filter((j) => inactive.has(j.OwnerId));
    const evidence = [ctx.ev(jobs.source, { count: jobs.data.length, exclusions: ["Deleted and completed jobs"], note: noted(JOB_OWNER_PRACTICE) })];
    const segments = [{ dimension: "job type", segments: new Set(jobs.data.map((j) => j.CronJobDetail?.JobType ?? "Unknown")).size }];
    if (jobs.data.length === 0) return { outcome: "pass", summary: said`There are no scheduled jobs.`, evidence, segmentExemptReason: "There are no jobs to segment." };
    if (hits.length === 0) return { outcome: "pass", summary: named(said`None of the ${jobs.data.length} scheduled jobs is owned by an inactive user. ${noted(JOB_OWNER_PRACTICE)}`, { data: jobs.data.length }), evidence, segmentsChecked: segments };
    return {
      outcome: "issue",
      title: said`Scheduled jobs belong to people who've left`,
      summary: named(said`Scheduled jobs owned by an inactive user: ${hits.length} of ${jobs.data.length}. ${noted(JOB_OWNER_PRACTICE)}`, { hits: hits.length, data: jobs.data.length }),
      recommendation: said`Check each job is still running, then reschedule it under a dedicated account.`,
      recommendationType: "investigate",
      severity: "medium",
      confidence: confidenceOf(JOB_OWNER_PRACTICE),
      evidence,
      value: notSized(said`The cost depends on what each job does, which isn't in the job record.`),
      effort: { size: "XS" },
      owner: "Salesforce admin",
      segmentsChecked: segments,
      subjects: hits.map((j) => ({ type: "scheduled job", label: j.CronJobDetail?.Name ?? j.Id })),
      facts: { jobs: hits.length, total: jobs.data.length },
    };
  },
};

export const PRACTICES: Record<string, Practice> = {
  [usersWithoutRole.id]: ROLE_PRACTICE,
  [ownershipSkew.id]: SKEW_PRACTICE,
  [afterSaveSameRecord.id]: SAME_RECORD_PRACTICE,
  [jobsOnInactiveOwners.id]: JOB_OWNER_PRACTICE,
};

export const bestPracticeModule: DomainModule = {
  id: "best_practice",
  cloud: "sales",
  label: "Best practice",
  extractors: [labelsExtractor, usersExtractor, recordCountsExtractor, ownershipExtractor, metadataExtractor, componentChangesExtractor, scheduledJobsExtractor],
  rules: [usersWithoutRole, ownershipSkew, afterSaveSameRecord, jobsOnInactiveOwners],
};
