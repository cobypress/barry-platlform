import { XMLParser } from "fast-xml-parser";
import { isUnavailable } from "../salesforce/client.ts";
import { describeObject, planFrom, type FieldSpec } from "./fields.ts";
import { monthsBefore, readSoql, soqlDateTime, unavailable, type ExtractContext, type Extractor, type SourceInfo, type Sourced } from "./types.ts";

// Service Cloud's reads (roadmap Stage 6, DECISIONS.md 153). Four steps, so a
// run stopped at the budget picks up where it left off: the set-up, which a
// set-up review reads too; case volumes and the cases themselves; what happened
// to them; and the escalation rules, which only the Metadata API holds.
//
// Every object here is checked against the Object Reference (API 68.0). Feature
// objects (entitlements, Omni-Channel, Knowledge, Messaging, chat, voice, macro
// usage) fail with INVALID_TYPE where the feature is off, which arrives as
// `unavailable`: "not in use", never a zero.

/** How far back activity is read: a year of cases is enough to see a pattern, and bounds the API cost. */
export const SERVICE_MONTHS = 12;
/** Rows read for each list of cases. More is cut short, and the rules say so. */
export const CASE_ROW_CAP = 50_000;

const since = (ctx: ExtractContext): string => soqlDateTime(monthsBefore(ctx.now, SERVICE_MONTHS));

export type CaseStatusRow = { ApiName: string; MasterLabel: string | null; IsClosed: boolean };
export type QueueRow = { Id: string; Name: string; DeveloperName: string; QueueRoutingConfigId?: string | null };
export type QueueObjectRow = { QueueId: string; SobjectType: string };
export type MemberRow = { GroupId: string; UserOrGroupId: string };
export type BusinessHoursRow = { Id: string; Name: string; IsActive: boolean; IsDefault: boolean };
export type SlaProcessRow = { Id: string; Name: string; IsActive: boolean; SObjectType: string | null };
export type MilestoneTypeRow = { Id: string; Name: string };
export type AssignmentRuleRow = { Id: string; Name: string | null; Active: boolean };
export type ServiceChannelRow = { Id: string; DeveloperName: string; RelatedEntity: string };
export type RoutingConfigRow = { Id: string; DeveloperName: string; RoutingModel: string; PushTimeout: number | null };
export type SupportUserRow = { Id: string; LastLoginDate: string | null; Profile: { UserLicense: { Name: string } | null } | null };
export type MacroRow = { Id: string; Name: string; IsLightningSupported: boolean };
export type QuickTextRow = { Id: string; Name: string };
export type MacroUsageRow = { MacroID: string; n: number };
export type QuickTextUsageRow = { QuickTextID: string; n: number };
export type ArticleRow = { Id: string; KnowledgeArticleId: string; LastPublishedDate: string | null };
export type ArticleViewRow = { ParentId: string; ViewCount: number; Channel: string };

export type ServiceSetup = {
  statuses: Sourced<CaseStatusRow[]>;
  queues: Sourced<QueueRow[]>;
  queueObjects: Sourced<QueueObjectRow[]>;
  members: Sourced<MemberRow[]>;
  businessHours: Sourced<BusinessHoursRow[]>;
  slaProcesses: Sourced<SlaProcessRow[]>;
  milestoneTypes: Sourced<MilestoneTypeRow[]>;
  assignmentRules: Sourced<AssignmentRuleRow[]>;
  channels: Sourced<ServiceChannelRow[]>;
  routingConfigs: Sourced<RoutingConfigRow[]>;
  supportUsers: Sourced<SupportUserRow[]>;
  macros: Sourced<MacroRow[]>;
  macroUsage: Sourced<MacroUsageRow[]>;
  quickText: Sourced<QuickTextRow[]>;
  quickTextUsage: Sourced<QuickTextUsageRow[]>;
  articles: Sourced<ArticleRow[]>;
  articleViews: Sourced<ArticleViewRow[]>;
};

// Group's queue routing field comes with Omni-Channel, so it's described first.
const GROUP: FieldSpec = { object: "Group", required: ["Id", "Name", "DeveloperName"], optional: ["QueueRoutingConfigId"] };

/** How the org is set up to handle cases. Reads no case, so a set-up review runs it. */
export const serviceSetupExtractor: Extractor<ServiceSetup> = {
  id: "service-setup",
  version: 1,
  async run(ctx) {
    const group = planFrom(await describeObject(ctx, "Group"), GROUP);
    const queueSoql = `SELECT ${group.ok ? group.select : GROUP.required.join(", ")} FROM Group WHERE Type = 'Queue'`;
    const after = since(ctx);
    return {
      statuses: await readSoql(ctx, "SELECT ApiName, MasterLabel, IsClosed FROM CaseStatus"),
      queues: group.ok ? await readSoql(ctx, queueSoql, { missingFields: group.missing }) : { status: "unavailable", reason: group.reason, source: { kind: "soql", api: "rest", query: queueSoql } },
      queueObjects: await readSoql(ctx, "SELECT QueueId, SobjectType FROM QueueSobject WHERE SobjectType = 'Case'"),
      members: await readSoql(ctx, "SELECT GroupId, UserOrGroupId FROM GroupMember WHERE GroupId IN (SELECT Id FROM Group WHERE Type = 'Queue')"),
      businessHours: await readSoql(ctx, "SELECT Id, Name, IsActive, IsDefault FROM BusinessHours"),
      slaProcesses: await readSoql(ctx, "SELECT Id, Name, IsActive, SObjectType FROM SlaProcess"),
      milestoneTypes: await readSoql(ctx, "SELECT Id, Name FROM MilestoneType"),
      assignmentRules: await readSoql(ctx, "SELECT Id, Name, Active FROM AssignmentRule WHERE SobjectType = 'Case'"),
      channels: await readSoql(ctx, "SELECT Id, DeveloperName, RelatedEntity FROM ServiceChannel"),
      routingConfigs: await readSoql(ctx, "SELECT Id, DeveloperName, RoutingModel, PushTimeout FROM QueueRoutingConfig"),
      supportUsers: await readSoql(ctx, "SELECT Id, LastLoginDate, Profile.UserLicense.Name FROM User WHERE IsActive = true AND UserPermissionsSupportUser = true"),
      macros: await readSoql(ctx, "SELECT Id, Name, IsLightningSupported FROM Macro"),
      macroUsage: await readSoql(ctx, `SELECT MacroID, COUNT(Id) n FROM MacroUsage WHERE CreatedDate >= ${after} GROUP BY MacroID`),
      quickText: await readSoql(ctx, "SELECT Id, Name FROM QuickText"),
      quickTextUsage: await readSoql(ctx, `SELECT QuickTextID, COUNT(Id) n FROM QuickTextUsage WHERE LoggedTime >= ${after} GROUP BY QuickTextID`),
      // The page says article queries must name PublishStatus or Id.
      articles: await readSoql(ctx, "SELECT Id, KnowledgeArticleId, LastPublishedDate FROM KnowledgeArticleVersion WHERE PublishStatus = 'Online' AND IsLatestVersion = true"),
      articleViews: await readSoql(ctx, "SELECT ParentId, ViewCount, Channel FROM KnowledgeArticleViewStat"),
    };
  },
};

export type CaseVolumeRow = { y: number; m: number; Priority: string | null; n: number };
export type OriginRow = { Origin: string | null; n: number };
export type OpenCaseRow = { Id: string; Status: string; Priority: string | null; Origin: string | null; OwnerId: string; CreatedDate: string; ContactId: string | null; AccountId: string | null };
export type ClosedCaseRow = { Id: string; CreatedDate: string; ClosedDate: string; Priority: string | null; Origin: string | null; OwnerId: string; Type?: string | null; Reason?: string | null };

export type ServiceCases = {
  created: Sourced<CaseVolumeRow[]>;
  closed: Sourced<CaseVolumeRow[]>;
  origins: Sourced<OriginRow[]>;
  open: Sourced<OpenCaseRow[]>;
  closedCases: Sourced<ClosedCaseRow[]>;
};

// Type and Reason are standard, but an org can take them off; the rest every org has.
const CLOSED_CASE: FieldSpec = { object: "Case", required: ["Id", "CreatedDate", "ClosedDate", "Priority", "Origin", "OwnerId"], optional: ["Type", "Reason"] };

/** Case volumes, as counts by month in the org's time zone, and the cases open now or closed in the last year. */
export const serviceCasesExtractor: Extractor<ServiceCases> = {
  id: "service-cases",
  version: 1,
  records: true,
  async run(ctx) {
    const after = since(ctx);
    const plan = planFrom(await describeObject(ctx, "Case"), CLOSED_CASE);
    const closedSoql = `SELECT ${plan.ok ? plan.select : CLOSED_CASE.required.join(", ")} FROM Case WHERE IsClosed = true AND ClosedDate >= ${after}`;
    const month = (field: string) => `CALENDAR_YEAR(convertTimezone(${field})) y, CALENDAR_MONTH(convertTimezone(${field})) m, Priority, COUNT(Id) n`;
    const by = (field: string) => `GROUP BY CALENDAR_YEAR(convertTimezone(${field})), CALENDAR_MONTH(convertTimezone(${field})), Priority`;
    return {
      created: await readSoql(ctx, `SELECT ${month("CreatedDate")} FROM Case WHERE CreatedDate >= ${after} ${by("CreatedDate")}`),
      closed: await readSoql(ctx, `SELECT ${month("ClosedDate")} FROM Case WHERE IsClosed = true AND ClosedDate >= ${after} ${by("ClosedDate")}`),
      origins: await readSoql(ctx, `SELECT Origin, COUNT(Id) n FROM Case WHERE CreatedDate >= ${after} GROUP BY Origin`),
      open: await readSoql(ctx, "SELECT Id, Status, Priority, Origin, OwnerId, CreatedDate, ContactId, AccountId FROM Case WHERE IsClosed = false", { maxRecords: CASE_ROW_CAP }),
      closedCases: plan.ok ? await readSoql(ctx, closedSoql, { maxRecords: CASE_ROW_CAP, missingFields: plan.missing }) : { status: "unavailable", reason: plan.reason, source: { kind: "soql", api: "rest", query: closedSoql } },
    };
  },
};

export type StatusChangeRow = { CaseId: string; OldValue: string | null; NewValue: string | null; CreatedDate: string };
export type MilestoneRow = { CaseId: string; MilestoneTypeId: string | null; IsCompleted: boolean; IsViolated: boolean; StartDate: string | null; TargetDate: string; CompletionDate: string | null };
export type ReplyRow = { ParentId: string; MessageDate: string | null };
export type AgentWorkRow = { UserId: string; Status: string; RequestDateTime: string | null; AcceptDateTime: string | null; DeclineDateTime: string | null; SpeedToAnswer: number | null };
export type CaseArticleRow = { CaseId: string };
export type ChannelCount = { channel: string; count: Sourced<number> };

export type ServiceActivity = {
  statusChanges: Sourced<StatusChangeRow[]>;
  milestones: Sourced<MilestoneRow[]>;
  replies: Sourced<ReplyRow[]>;
  agentWork: Sourced<AgentWorkRow[]>;
  caseArticles: Sourced<CaseArticleRow[]>;
  /** Contacts by channel beyond the case's own Origin. Unavailable means the channel isn't in use. */
  channels: ChannelCount[];
};

async function countOf(ctx: ExtractContext, soql: string): Promise<Sourced<number>> {
  const source: SourceInfo = { kind: "soql", api: "rest", query: soql };
  try {
    return { status: "ok", data: (await ctx.client.query(soql)).totalSize, source, truncated: false };
  } catch (err) {
    return unavailable(source, err);
  }
}

/** What happened to the year's cases: status changes, milestones, first replies, routing, articles. */
export const serviceActivityExtractor: Extractor<ServiceActivity> = {
  id: "service-activity",
  version: 1,
  records: true,
  async run(ctx) {
    const after = since(ctx);
    const recentCases = `SELECT Id FROM Case WHERE CreatedDate >= ${after}`;
    return {
      statusChanges: await readSoql(ctx, `SELECT CaseId, OldValue, NewValue, CreatedDate FROM CaseHistory WHERE Field = 'Status' AND CreatedDate >= ${after}`, { maxRecords: CASE_ROW_CAP }),
      milestones: await readSoql(ctx, `SELECT CaseId, MilestoneTypeId, IsCompleted, IsViolated, StartDate, TargetDate, CompletionDate FROM CaseMilestone WHERE StartDate >= ${after}`, { maxRecords: CASE_ROW_CAP }),
      replies: await readSoql(ctx, `SELECT ParentId, MessageDate FROM EmailMessage WHERE Incoming = false AND ParentId IN (${recentCases})`, { maxRecords: CASE_ROW_CAP }),
      agentWork: await readSoql(ctx, `SELECT UserId, Status, RequestDateTime, AcceptDateTime, DeclineDateTime, SpeedToAnswer FROM AgentWork WHERE RequestDateTime >= ${after}`, { maxRecords: CASE_ROW_CAP }),
      caseArticles: await readSoql(ctx, `SELECT CaseId FROM CaseArticle WHERE CaseId IN (SELECT Id FROM Case WHERE IsClosed = true AND ClosedDate >= ${after})`, { maxRecords: CASE_ROW_CAP }),
      channels: [
        { channel: "Email", count: await countOf(ctx, `SELECT COUNT() FROM EmailMessage WHERE Incoming = true AND ParentId IN (${recentCases})`) },
        { channel: "Messaging", count: await countOf(ctx, `SELECT COUNT() FROM MessagingSession WHERE StartTime >= ${after}`) },
        { channel: "Chat", count: await countOf(ctx, `SELECT COUNT() FROM LiveChatTranscript WHERE StartTime >= ${after}`) },
        { channel: "Voice", count: await countOf(ctx, `SELECT COUNT() FROM VoiceCall WHERE CallStartDateTime >= ${after}`) },
      ],
    };
  },
};

export type EscalationAction = { assignedTo: string | null; assignedToType: string | null; minutes: number | null; notifyTo: string | null };
export type EscalationRule = { name: string; active: boolean; entries: number; actions: EscalationAction[] };

const parser = new XMLParser({ ignoreAttributes: true, parseTagValue: false, isArray: (name) => ["escalationRule", "ruleEntry", "escalationAction"].includes(name) });

/** Case escalation rules from their Metadata API file. The format is in the Metadata API guide, "EscalationRules". */
export function parseEscalationRules(xml: string): EscalationRule[] {
  const root = (parser.parse(xml) as { EscalationRules?: { escalationRule?: Array<Record<string, unknown>> } }).EscalationRules;
  return (root?.escalationRule ?? []).map((rule) => {
    const entries = (rule.ruleEntry as Array<Record<string, unknown>> | undefined) ?? [];
    const actions = entries.flatMap((e) => ((e.escalationAction as Array<Record<string, unknown>> | undefined) ?? []).map((a) => ({
      assignedTo: typeof a.assignedTo === "string" ? a.assignedTo : null,
      assignedToType: typeof a.assignedToType === "string" ? a.assignedToType : null,
      minutes: a.minutesToEscalation === undefined ? null : Number(a.minutesToEscalation),
      notifyTo: typeof a.notifyTo === "string" ? a.notifyTo : null,
    })));
    return { name: String(rule.fullName ?? ""), active: String(rule.active) === "true", entries: entries.length, actions };
  });
}

export const ESCALATION_SOURCE: SourceInfo = { kind: "metadata", path: "EscalationRules: Case" };

/**
 * Its own step and its own retrieve, so Sales Cloud's bundle is unchanged. A
 * connection that can't use the Metadata API gets unavailable: we know of no
 * Tooling route for escalation rules (QUESTIONS.md).
 */
export const escalationRulesExtractor: Extractor<Sourced<EscalationRule[]>> = {
  id: "service-escalation-rules",
  version: 1,
  async run(ctx) {
    try {
      const files = await ctx.client.retrieveMetadata([{ name: "EscalationRules", members: ["Case"] }]);
      const file = Object.entries(files).find(([path]) => /(^|\/)escalationRules\/Case\.escalationRules$/.test(path));
      return { status: "ok", data: file ? parseEscalationRules(file[1]) : [], source: ESCALATION_SOURCE, truncated: false };
    } catch (err) {
      if (isUnavailable(err)) return unavailable(ESCALATION_SOURCE, err);
      throw err;
    }
  },
};
