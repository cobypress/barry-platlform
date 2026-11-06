// The Salesforce facts the engine assumes but couldn't check offline (DECISIONS.md
// 287 to 295). Each names the QUESTIONS.md item it settles, by id, and the checks
// that rest on it. A run's facts step answers every one from describes alone:
// names, booleans and field types, never a record or a value.

/** What it means when the name isn't there: the assumption was wrong, or the feature may just be off. */
export type IfMissing = "contradicts" | "open";

type Base = {
  id: string;
  /** The QUESTIONS.md item, as `section.item` (see src/facts/questions.ts). */
  question: string;
  /** Whether the check answers all of that item, or only the names in it. */
  covers: "whole" | "part";
  /** What we assume, in a sentence for the raw notes and the questions report. */
  says: string;
  /** The checks that say not measurable when it's wrong. */
  affects: string[];
};

export type Assumption = Base &
  (
    | { kind: "object"; object: string; ifMissing: IfMissing }
    | { kind: "objects"; objects: string[]; ifMissing: IfMissing }
    /** Fields on an object. With a namespace, only checked where that package is installed. */
    | { kind: "fields"; object: string; fields: string[]; namespace?: string }
    /** Where a package's names are in the org, it has a PackageLicense row. */
    | { kind: "licence"; namespace: string }
    | { kind: "limit"; name: string }
    /** A Settings member a module retrieves: checked only when that module ran. */
    | { kind: "settings"; members: string[]; ifMissing: IfMissing }
  );

const CPQ_QUOTING = ["cpq.discount-leakage", "cpq.discount-approvals", "cpq.quote-to-order"];

export const ASSUMPTIONS: readonly Assumption[] = [
  {
    id: "cpq.quote-fields",
    question: "cpq.a",
    covers: "part",
    kind: "fields",
    object: "SBQQ__Quote__c",
    fields: ["SBQQ__Primary__c", "SBQQ__Opportunity2__c", "SBQQ__ListAmount__c", "SBQQ__NetAmount__c", "SBQQ__Ordered__c"],
    namespace: "SBQQ",
    says: "A CPQ quote has the primary, opportunity, list amount, net amount and ordered fields we read.",
    affects: CPQ_QUOTING,
  },
  {
    id: "cpq.price-rule-fields",
    question: "cpq.b",
    covers: "part",
    kind: "fields",
    object: "SBQQ__PriceRule__c",
    fields: ["SBQQ__Active__c", "SBQQ__EvaluationEvent__c", "SBQQ__EvaluationOrder__c", "SBQQ__TargetObject__c"],
    namespace: "SBQQ",
    says: "A CPQ price rule has the active, evaluation event, evaluation order and target object fields we read.",
    affects: ["cpq.price-rule-conflicts", "cpq.inactive-rules"],
  },
  {
    id: "cpq.price-action-fields",
    question: "cpq.b",
    covers: "part",
    kind: "fields",
    object: "SBQQ__PriceAction__c",
    fields: ["SBQQ__Rule__c", "SBQQ__Field__c", "SBQQ__TargetObject__c"],
    namespace: "SBQQ",
    says: "A CPQ price action has the rule, field and target object fields we read.",
    affects: ["cpq.price-rule-conflicts"],
  },
  {
    id: "cpq.product-rule-fields",
    question: "cpq.b",
    covers: "part",
    kind: "fields",
    object: "SBQQ__ProductRule__c",
    fields: ["SBQQ__Active__c", "SBQQ__Type__c"],
    namespace: "SBQQ",
    says: "A CPQ product rule has the active and type fields we read.",
    affects: ["cpq.inactive-rules"],
  },
  {
    id: "cpq.renewal-field",
    question: "cpq.b",
    covers: "part",
    kind: "fields",
    object: "Contract",
    fields: ["SBQQ__RenewalOpportunity__c"],
    namespace: "SBQQ",
    says: "With CPQ, a contract has the renewal opportunity field.",
    affects: ["cpq.renewals-not-started"],
  },
  {
    id: "cpq.licence",
    question: "cpq.d",
    covers: "whole",
    kind: "licence",
    namespace: "SBQQ",
    says: "Where CPQ is installed, PackageLicense has a row for namespace SBQQ.",
    // The describe finds CPQ without it, at one more call.
    affects: [],
  },
  {
    id: "ae.licence",
    question: "ae.a",
    covers: "whole",
    kind: "licence",
    namespace: "pi",
    says: "Where Account Engagement is installed, PackageLicense has a row for namespace pi.",
    // Lead.pi__url__c finds it without the licence row.
    affects: [],
  },
  {
    id: "ae.lead-fields",
    question: "ae.b",
    covers: "part",
    kind: "fields",
    object: "Lead",
    fields: ["pi__url__c", "pi__score__c", "pi__grade__c"],
    namespace: "pi",
    says: "With Account Engagement, a lead has the pi__url__c, pi__score__c and pi__grade__c fields.",
    affects: ["mkt.scoring-grading", "mkt.unworked-handovers", "mkt.unowned-prospects"],
  },
  {
    id: "permission.api-user-only",
    question: "integrations.b",
    covers: "part",
    kind: "fields",
    object: "PermissionSet",
    fields: ["PermissionsApiUserOnly"],
    says: "A permission set has a PermissionsApiUserOnly field for the API Only User permission.",
    affects: ["int.integration-users-interactive"],
  },
  {
    id: "permission.view-all-data",
    question: "fields.a",
    covers: "whole",
    kind: "fields",
    object: "PermissionSet",
    fields: ["PermissionsViewAllData", "PermissionsModifyAllData"],
    says: "A permission set has PermissionsViewAllData and PermissionsModifyAllData fields.",
    affects: ["sec.modify-all-data", "sec.view-all-data"],
  },
  {
    id: "user.lightning-preference",
    question: "adoption.56",
    covers: "part",
    kind: "fields",
    object: "User",
    fields: ["UserPreferencesLightningExperiencePreferred"],
    says: "A user has the Switch to Lightning Experience preference field, and the connection can see it.",
    affects: ["adp.classic-users"],
  },
  {
    id: "object.scontrol",
    question: "integrations.i",
    covers: "part",
    kind: "object",
    object: "Scontrol",
    ifMissing: "contradicts",
    says: "Scontrol can be queried in any org, including one that never had s-controls.",
    affects: ["tech.classic-leftovers"],
  },
  {
    id: "object.package-licence",
    question: "pay.49",
    covers: "whole",
    kind: "object",
    object: "PackageLicense",
    ifMissing: "contradicts",
    says: "The connection can query PackageLicense.",
    affects: ["utl.package-seats"],
  },
  {
    id: "object.auth-provider",
    question: "real-org.36",
    covers: "whole",
    kind: "object",
    object: "AuthProvider",
    ifMissing: "contradicts",
    says: "AuthProvider is in every org, so a connection that can't see it lacks the permission.",
    affects: ["utl.dormant-licensed-users"],
  },
  {
    id: "object.documented-setup",
    question: "documented.59",
    covers: "part",
    kind: "objects",
    objects: ["ConnectedApplication", "NamedCredential"],
    ifMissing: "contradicts",
    says: "The connection can query ConnectedApplication and NamedCredential.",
    affects: ["sec.self-authorised-connected-apps"],
  },
  {
    id: "object.bot-definition",
    question: "ai.c",
    covers: "part",
    kind: "object",
    object: "BotDefinition",
    // Only readable once agents are enabled, so its absence settles nothing.
    ifMissing: "open",
    says: "BotDefinition can be read where agents are enabled.",
    affects: [],
  },
  {
    id: "object.knowledge-article-version",
    question: "service.d",
    covers: "part",
    kind: "object",
    object: "KnowledgeArticleVersion",
    ifMissing: "open",
    says: "KnowledgeArticleVersion can be read where Knowledge is on.",
    affects: [],
  },
  {
    id: "object.service-setup",
    question: "service.e",
    covers: "part",
    kind: "objects",
    objects: ["MilestoneType", "SlaProcess", "ServiceChannel", "QueueRoutingConfig", "MacroUsage", "QuickTextUsage"],
    ifMissing: "open",
    says: "The connection can read the service set-up objects where their features are on.",
    affects: [],
  },
  {
    id: "limit.async-apex",
    question: "integrations.h",
    covers: "part",
    kind: "limit",
    name: "DailyAsyncApexExecutions",
    says: "/limits reports the async Apex allowance as DailyAsyncApexExecutions.",
    affects: ["int.api-usage"],
  },
  {
    id: "settings.ai",
    question: "ai.a",
    covers: "whole",
    kind: "settings",
    members: ["Knowledge", "EinsteinGpt", "AgentPlatform", "CustomerDataPlatform"],
    // A member whose feature isn't licensed may return nothing, so its absence settles nothing.
    ifMissing: "open",
    says: "The Settings members Knowledge, EinsteinGpt, AgentPlatform and CustomerDataPlatform each return their file.",
    affects: [],
  },
  {
    id: "settings.campaign",
    question: "ae.d",
    covers: "whole",
    kind: "settings",
    members: ["Campaign"],
    ifMissing: "open",
    says: "The Settings member Campaign returns the campaign settings.",
    affects: [],
  },
];

/** Every object the facts step describes for its fields. */
export const DESCRIBED_OBJECTS: readonly string[] = [...new Set(ASSUMPTIONS.flatMap((a) => (a.kind === "fields" ? [a.object] : [])))];
