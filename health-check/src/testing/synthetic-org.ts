// An invented company, built from a seeded random generator so every run gives
// the same org. Rule 8 forbids customer data in the repo, so nothing here comes
// from a real org. Ids are zero-padded and emails use example.com, and a test
// fails on anything else.
//
// The data holds deliberate traps, each matching a real way a check can go wrong.
// The numbered users in USERS say which trap each one is.

export const AS_OF = "2026-10-31T12:00:00.000Z";
export const API_VERSION = "66.0";

export const sfId = (prefix: string, n: number): string => `${prefix}${String(n).padStart(12, "0")}AAA`;
export const userId = (n: number): string => sfId("005", n);
export const ORG_ID = sfId("00D", 1);

const DAY = 86_400_000;
const asOf = new Date(AS_OF).getTime();
const daysAgo = (days: number): string => new Date(asOf - days * DAY).toISOString().replace("Z", "+0000");
const dateOnly = (ms: number): string => new Date(ms).toISOString().slice(0, 10);

/** Mulberry32: small, seeded and identical on every platform. */
function seeded(seed: number): () => number {
  let a = seed;
  return () => {
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

type UserSeed = { n: number; name: string; login?: string; lastLogin: number | null; active?: boolean; role?: string; created?: number; manager?: number; licence?: string; profile?: string };

export const USERS: UserSeed[] = [
  { n: 1, name: "Alex Morgan", lastLogin: 2, role: "Sales UK" },
  { n: 2, name: "Priya Shah", lastLogin: 1, role: "Sales UK" },
  { n: 3, name: "Tom Baker", lastLogin: 3, role: "Sales EU", manager: 12 },
  { n: 4, name: "Jo Ellis", lastLogin: 1, profile: "System Administrator" },
  // Trap: the one clean dormant user. Every guard should pass once the manual step is answered.
  { n: 5, name: "Sam Reed", lastLogin: 200, role: "Sales UK" },
  // Trap: looks dormant but owns most of the accounts, and has a shared username.
  { n: 6, name: "Shared Sales Desk", login: "sales@example.com", lastLogin: 300 },
  // Trap: dormant, and hardcoded by id in an active flow.
  { n: 7, name: "Dana Cole", lastLogin: 250, role: "Sales UK" },
  // Trap: dormant, and named as the Default Workflow User in the manual answer.
  { n: 8, name: "Lee Park", lastLogin: 180 },
  // Trap: already inactive, and still owns open deals, accounts and a dashboard.
  { n: 9, name: "Kim Fox", lastLogin: 500, active: false, role: "Sales EU" },
  // Trap: never logged in, and otherwise clean.
  { n: 10, name: "Nina Hart", lastLogin: null, role: "Sales UK" },
  { n: 11, name: "Integration User", login: "integration@example.com", lastLogin: 1, profile: "API Only" },
  // Trap: dormant, and an active user's manager.
  { n: 12, name: "Max Stone", lastLogin: 400, role: "Sales EU" },
  // Trap: dormant, and the owner of a scheduled job.
  { n: 13, name: "Ola Nowak", lastLogin: 220 },
  { n: 14, name: "Ren Ito", lastLogin: 4, role: "Sales EU" },
  // Trap: LastLoginDate says dormant, but LoginHistory shows recent logins.
  { n: 15, name: "Ben Cross", lastLogin: 150, role: "Sales UK" },
];

export const usersTable = USERS.map((u) => {
  const username = u.login ?? `${u.name.toLowerCase().replace(/\s+/g, ".")}@example.com`;
  return {
    Id: userId(u.n),
    Name: u.name,
    Username: username,
    Email: username,
    IsActive: u.active ?? true,
    UserType: "Standard",
    Profile: { Name: u.profile ?? "Standard User", UserLicense: { Name: u.licence ?? "Salesforce" } },
    UserRole: u.role ? { Name: u.role } : null,
    LastLoginDate: u.lastLogin === null ? null : daysAgo(u.lastLogin),
    CreatedDate: daysAgo(u.created ?? 900),
    ManagerId: u.manager ? userId(u.manager) : null,
    DelegatedApproverId: null,
  };
});

export const STAGES = [
  { ApiName: "Prospecting", MasterLabel: "Prospecting", IsActive: true, IsClosed: false, IsWon: false, SortOrder: 1 },
  { ApiName: "Discovery", MasterLabel: "Discovery", IsActive: true, IsClosed: false, IsWon: false, SortOrder: 2 },
  { ApiName: "Proposal", MasterLabel: "Proposal", IsActive: true, IsClosed: false, IsWon: false, SortOrder: 3 },
  { ApiName: "Negotiation", MasterLabel: "Negotiation", IsActive: true, IsClosed: false, IsWon: false, SortOrder: 4 },
  // Trap: active, and nobody uses it.
  { ApiName: "Legal Review", MasterLabel: "Legal Review", IsActive: true, IsClosed: false, IsWon: false, SortOrder: 5 },
  { ApiName: "Closed Won", MasterLabel: "Closed Won", IsActive: true, IsClosed: true, IsWon: true, SortOrder: 6 },
  { ApiName: "Closed Lost", MasterLabel: "Closed Lost", IsActive: true, IsClosed: true, IsWon: false, SortOrder: 7 },
  // Trap: looks like a loss in Salesforce, and shouldn't count as one.
  { ApiName: "Closed No Decision", MasterLabel: "Closed No Decision", IsActive: true, IsClosed: true, IsWon: false, SortOrder: 8 },
];

const OPEN_PATH = ["Prospecting", "Discovery", "Proposal", "Negotiation"];
const SOURCES = ["Web", "Referral", "Partner", "Event"];

export type SyntheticOpportunity = {
  Id: string;
  OwnerId: string;
  StageName: string;
  Amount: number | null;
  CloseDate: string;
  CreatedDate: string;
  IsClosed: boolean;
  IsWon: boolean;
  LastActivityDate: string | null;
  Type: string;
  LeadSource: string;
  RecordType: { DeveloperName: string };
  Loss_Reason__c: string | null;
};

export type SyntheticHistory = { Id: string; OpportunityId: string; StageName: string; Amount: number | null; CloseDate: string; CreatedDate: string };

function buildPipeline(): { opportunities: SyntheticOpportunity[]; history: SyntheticHistory[] } {
  const random = seeded(20261031);
  const between = (low: number, high: number): number => Math.floor(low + random() * (high - low + 1));
  const opportunities: SyntheticOpportunity[] = [];
  const history: SyntheticHistory[] = [];
  const closedOwners = [1, 2, 3, 14];
  let historyCount = 0;

  const addHistory = (oppId: string, stage: string, amount: number | null, close: number, at: number): void => {
    historyCount++;
    history.push({ Id: sfId("008", historyCount), OpportunityId: oppId, StageName: stage, Amount: amount, CloseDate: dateOnly(close), CreatedDate: new Date(at).toISOString().replace("Z", "+0000") });
  };

  // Closed deals. Trap: the EU team wins far less than the UK team, and the
  // overall win rate on its own looks ordinary.
  for (let i = 1; i <= 220; i++) {
    const id = sfId("006", i);
    const owner = closedOwners[i % closedOwners.length]!;
    const eu = owner === 3 || owner === 14;
    const created = asOf - between(120, 700) * DAY;
    const amount = between(5, 80) * 1000;
    const roll = random();
    const noDecisionShare = 0.2;
    const winShare = eu ? 0.12 : 0.55;
    const final = roll < noDecisionShare ? "Closed No Decision" : roll < noDecisionShare + winShare * (1 - noDecisionShare) ? "Closed Won" : "Closed Lost";
    let close = created + between(30, 60) * DAY;
    let at = created;
    const depth = final === "Closed Won" ? OPEN_PATH.length : between(1, OPEN_PATH.length);
    // Trap: a tenth of deals jump a stage, which conversion has to handle.
    const skip = random() < 0.1 ? 1 : -1;
    for (let s = 0; s < depth; s++) {
      if (s === skip) continue;
      addHistory(id, OPEN_PATH[s]!, amount, close, at);
      at += between(5, 25) * DAY;
      if (random() < 0.3) {
        close += between(10, 75) * DAY;
        addHistory(id, OPEN_PATH[s]!, amount, close, at - DAY);
      }
    }
    addHistory(id, final, amount, Math.max(close, at), at);
    opportunities.push({
      Id: id,
      OwnerId: userId(owner),
      StageName: final,
      Amount: amount,
      CloseDate: dateOnly(Math.max(close, at)),
      CreatedDate: new Date(created).toISOString().replace("Z", "+0000"),
      IsClosed: true,
      IsWon: final === "Closed Won",
      LastActivityDate: dateOnly(at),
      Type: random() < 0.33 ? "Existing Business" : "New Business",
      LeadSource: SOURCES[between(0, SOURCES.length - 1)]!,
      RecordType: { DeveloperName: random() < 0.2 ? "Renewal" : "New_Business" },
      Loss_Reason__c: final === "Closed Lost" && random() < 0.5 ? "Price" : null,
    });
  }

  // Trap: three migrated closed deals with no history at all.
  for (let i = 221; i <= 223; i++) {
    opportunities.push({
      Id: sfId("006", i),
      OwnerId: userId(1),
      StageName: "Closed Won",
      Amount: 10_000,
      CloseDate: dateOnly(asOf - 200 * DAY),
      CreatedDate: new Date(asOf - 300 * DAY).toISOString().replace("Z", "+0000"),
      IsClosed: true,
      IsWon: true,
      LastActivityDate: null,
      Type: "New Business",
      LeadSource: "Web",
      RecordType: { DeveloperName: "New_Business" },
      Loss_Reason__c: null,
    });
  }

  // Open deals. Trap: amounts are fine in total, but the Renewal record type is
  // mostly blank, so a total-only check would pass.
  for (let i = 224; i <= 293; i++) {
    const id = sfId("006", i);
    const renewal = i % 6 === 0;
    const owner = i % 11 === 0 ? 9 : [1, 2, 3, 14][i % 4]!;
    const created = asOf - between(10, 200) * DAY;
    const pastDue = random() < 0.15;
    const close = asOf + (pastDue ? -between(5, 90) : between(5, 120)) * DAY;
    const amount = renewal && i % 12 !== 0 ? null : between(5, 80) * 1000;
    const stage = OPEN_PATH[between(0, OPEN_PATH.length - 1)]!;
    let at = created;
    for (const s of OPEN_PATH.slice(0, OPEN_PATH.indexOf(stage) + 1)) {
      addHistory(id, s, amount, close, at);
      at += between(2, 8) * DAY;
    }
    opportunities.push({
      Id: id,
      OwnerId: userId(owner),
      StageName: stage,
      Amount: amount,
      CloseDate: dateOnly(close),
      CreatedDate: new Date(created).toISOString().replace("Z", "+0000"),
      IsClosed: false,
      IsWon: false,
      LastActivityDate: random() < 0.2 ? dateOnly(asOf - 120 * DAY) : dateOnly(asOf - between(1, 30) * DAY),
      Type: "New Business",
      LeadSource: SOURCES[between(0, SOURCES.length - 1)]!,
      RecordType: { DeveloperName: renewal ? "Renewal" : "New_Business" },
      Loss_Reason__c: null,
    });
  }
  history.sort((a, b) => a.Id.localeCompare(b.Id));
  return { opportunities, history };
}

export const pipeline = buildPipeline();

const standardField = (name: string, type = "string", extra: Record<string, unknown> = {}) => ({
  name,
  label: name,
  type,
  custom: false,
  calculated: false,
  aggregatable: true,
  filterable: true,
  nillable: true,
  unique: false,
  externalId: false,
  ...extra,
});

const customField = (name: string, extra: Record<string, unknown> = {}) => standardField(name, "string", { custom: true, ...extra });
const picklist = (name: string, count: number) => standardField(name, "picklist", { picklistValues: Array.from({ length: count }, (_, i) => ({ value: `${name} ${i + 1}`, active: true })) });

const paddingFields = (object: string, count: number) => Array.from({ length: count }, (_, i) => customField(`${object}_Attr_${String(i + 1).padStart(3, "0")}__c`));

export const DESCRIBES: Record<string, { fields: unknown[]; recordTypeInfos: unknown[] }> = {
  Account: {
    fields: [
      standardField("Id", "id"),
      standardField("Name"),
      picklist("Industry", 12),
      // Trap: sprawl on Account.
      ...paddingFields("Account", 155),
      customField("Old_Sic_Code__c"),
    ],
    recordTypeInfos: [{ developerName: "Master", name: "Master", active: true, master: true }],
  },
  Contact: { fields: [standardField("Id", "id"), standardField("Email", "email")], recordTypeInfos: [{ developerName: "Master", name: "Master", active: true, master: true }] },
  Lead: {
    fields: [standardField("Id", "id"), picklist("LeadSource", 30), customField("Lead_Score__c"), customField("Old_Campaign_Ref__c", { externalId: true })],
    recordTypeInfos: [{ developerName: "Master", name: "Master", active: true, master: true }],
  },
  Opportunity: {
    fields: [
      standardField("Id", "id"),
      picklist("Type", 3),
      standardField("LeadSource", "picklist"),
      standardField("RecordTypeId", "reference"),
      customField("Competitor__c"),
      customField("Loss_Reason__c"),
      customField("Legacy_Code__c"),
      customField("Old_Region__c"),
      customField("Ai_Score__c", { calculated: true }),
    ],
    recordTypeInfos: [
      { developerName: "New_Business", name: "New Business", active: true, master: false },
      { developerName: "Renewal", name: "Renewal", active: true, master: false },
      { developerName: "Master", name: "Master", active: true, master: true },
    ],
  },
};

// Total and recent counts for custom field usage, as COUNT(field) would give them.
export const RECORD_TOTALS: Record<string, { total: number; recent: number }> = {
  Account: { total: 500, recent: 120 },
  Contact: { total: 500, recent: 90 },
  Lead: { total: 500, recent: 300 },
  Opportunity: { total: pipeline.opportunities.length, recent: 150 },
};

export const FILLED: Record<string, number | [number, number]> = {
  Old_Sic_Code__c: 2,
  // Trap: nearly empty overall, but filling up on new records. Not unused.
  Lead_Score__c: [12, 12],
  Old_Campaign_Ref__c: 1,
  Competitor__c: 150,
  Loss_Reason__c: 60,
  Legacy_Code__c: 1,
  Old_Region__c: 0,
};

export const fieldFilled = (object: string, field: string, recent: boolean): number => {
  const planted = FILLED[field];
  const totals = RECORD_TOTALS[object]!;
  if (Array.isArray(planted)) return planted[recent ? 1 : 0];
  if (planted !== undefined) return recent ? Math.min(planted, 1) : planted;
  return Math.round((recent ? totals.recent : totals.total) * 0.6);
};

export const OWNERSHIP: Record<string, Array<[number, number]>> = {
  // Trap: the shared desk owns most accounts, which is what a licence check once missed.
  Account: [[6, 310], [1, 80], [2, 60], [9, 50]],
  Contact: [[1, 200], [2, 200], [9, 100]],
  Lead: [[1, 200], [2, 260], [9, 40]],
  Case: [],
  Project__c: [[2, 20]],
};

export const RECORD_COUNTS = [
  { name: "Account", count: 500 },
  { name: "Project__c", count: 20 },
  // Trap: a detail object has no OwnerId, which isn't a gap in the scan.
  { name: "Project_Line__c", count: 40 },
  { name: "Empty_Object__c", count: 0 },
];

export const USER_LICENSES = [
  { Name: "Salesforce", TotalLicenses: 30, UsedLicenses: 15, Status: "Active" },
  { Name: "Salesforce Platform", TotalLicenses: 10, UsedLicenses: 10, Status: "Active" },
  { Name: "Chatter Free", TotalLicenses: 5000, UsedLicenses: 0, Status: "Active" },
];

export const PERMISSION_SET_LICENSES = [{ MasterLabel: "Sales Console", TotalLicenses: 15, UsedLicenses: 3, Status: "Active", ExpirationDate: null }];

export const LAYOUTS = [
  { Id: sfId("00h", 1), Name: "Account Layout", TableEnumOrId: "Account" },
  { Id: sfId("00h", 2), Name: "Opportunity Layout", TableEnumOrId: "Opportunity" },
  { Id: sfId("00h", 3), Name: "Lead Layout", TableEnumOrId: "Lead" },
  { Id: sfId("00h", 4), Name: "Old Opportunity Layout", TableEnumOrId: "Opportunity" },
  { Id: sfId("00h", 5), Name: "Contact Layout", TableEnumOrId: "Contact" },
  { Id: sfId("00h", 6), Name: "Partner Lead Layout", TableEnumOrId: "Lead" },
];

export const PROFILE_LAYOUTS = [1, 2, 3, 5].map((n) => ({ LayoutId: sfId("00h", n) }));

export const DUPLICATE_RULES = [
  { DeveloperName: "Lead_Rule", SobjectType: "Lead", IsActive: true },
  { DeveloperName: "Contact_Rule", SobjectType: "Contact", IsActive: false },
];

export const SETUP_AUDIT = [
  ...Array.from({ length: 27 }, (_, i) => ({ Section: "Flows", CreatedDate: daysAgo(i * 5 + 1), CreatedById: userId(4) })),
  ...Array.from({ length: 3 }, (_, i) => ({ Section: "Customize Opportunities", CreatedDate: daysAgo(i * 7 + 2), CreatedById: userId(9) })),
  // Trap: user admin isn't configuration work, and Section can't be filtered in SOQL.
  ...Array.from({ length: 8 }, (_, i) => ({ Section: "Manage Users", CreatedDate: daysAgo(i * 4 + 1), CreatedById: userId(1) })),
];

const loginRows = (user: number, count: number, status = "Success") => Array.from({ length: count }, (_, i) => ({ UserId: userId(user), LoginTime: daysAgo(i * 2 + 1), Status: status }));

export const LOGIN_HISTORY = [
  ...loginRows(1, 10),
  ...loginRows(2, 12),
  ...loginRows(3, 8),
  ...loginRows(4, 15),
  ...loginRows(11, 30),
  ...loginRows(14, 7),
  // Trap: disagrees with LastLoginDate.
  ...loginRows(15, 3),
  // Trap: failures aren't logins, and Status can't be filtered in SOQL.
  ...loginRows(5, 4, "Invalid Password"),
];

export const CRON_TRIGGERS = [
  { Id: sfId("08e", 1), OwnerId: userId(13), State: "WAITING", CronJobDetail: { Name: "Nightly Renewal Sync", JobType: "7" } },
  { Id: sfId("08e", 2), OwnerId: userId(4), State: "WAITING", CronJobDetail: { Name: "Weekly Cleanup", JobType: "7" } },
];

export const DASHBOARDS = [
  { Id: sfId("01Z", 1), Title: "Sales Overview", Type: "SpecifiedUser", RunningUserId: userId(4), FolderName: "Sales" },
  { Id: sfId("01Z", 2), Title: "EU Pipeline", Type: "SpecifiedUser", RunningUserId: userId(9), FolderName: "Sales" },
  { Id: sfId("01Z", 3), Title: "My Deals", Type: "LoggedInUser", RunningUserId: null, FolderName: "Sales" },
];

export const EMAIL_SERVICES = [{ LocalPart: "cases", RunAsUserId: userId(4) }];
export const AUTH_PROVIDERS = [{ DeveloperName: "Google_SSO", ExecutionUserId: userId(4) }];

const reportFolders = ["Sales", "Marketing", "Service"];
export const REPORTS = Array.from({ length: 80 }, (_, i) => ({
  Id: sfId("00O", i + 1),
  FolderName: reportFolders[i % reportFolders.length]!,
  // Trap: 60 of 80 not run in a year, plus one too new to judge.
  LastRunDate: i % 4 === 0 ? daysAgo(10) : i % 4 === 1 ? null : daysAgo(500),
  CreatedDate: i === 79 ? daysAgo(10) : daysAgo(600),
}));

export const ACTIVITY_CONNECTIONS = [
  { UserId: userId(1), ConnectivityStatus: "ACTIVE" },
  { UserId: userId(2), ConnectivityStatus: "ACTIVE" },
  { UserId: userId(3), ConnectivityStatus: "NEEDSATTENTION" },
  { UserId: userId(14), ConnectivityStatus: "ACTIVE" },
];

export const LEADS = {
  open: 500,
  stale: 110,
  openBySource: [{ s: "Web", c: 300 }, { s: "Event", c: 120 }, { s: "Partner", c: 60 }, { s: null, c: 20 }],
  // Trap: the total is under the limit, and Partner leads are nearly all untouched.
  staleBySource: [{ s: "Web", c: 30 }, { s: "Event", c: 25 }, { s: "Partner", c: 50 }, { s: null, c: 5 }],
};

export const CONTACTS = { total: 500, noAccount: 20, noEmail: 80 };

export const APEX_CLASSES = [
  { Id: sfId("01p", 1), Name: "OpportunityHandler", ApiVersion: 60, Status: "Active", LengthWithoutComments: 1200, Body: "public class OpportunityHandler { /* uses Competitor__c */ }" },
  { Id: sfId("01p", 2), Name: "LeadRouter", ApiVersion: 62, Status: "Active", LengthWithoutComments: 800, Body: "public class LeadRouter { }" },
  { Id: sfId("01p", 3), Name: "LegacySync", ApiVersion: 38, Status: "Active", LengthWithoutComments: 400, Body: "public class LegacySync { }" },
  { Id: sfId("01p", 4), Name: "OpportunityHandlerTest", ApiVersion: 60, Status: "Active", LengthWithoutComments: 300, Body: "@isTest class OpportunityHandlerTest { }" },
];

export const APEX_TRIGGERS = [
  { Id: sfId("01q", 1), Name: "OpportunityTrigger", TableEnumOrId: "Opportunity", ApiVersion: 60, Status: "Active", Body: "trigger OpportunityTrigger on Opportunity (before insert) { }" },
  { Id: sfId("01q", 2), Name: "OpportunitySplitTrigger", TableEnumOrId: "Opportunity", ApiVersion: 60, Status: "Active", Body: "trigger OpportunitySplitTrigger on Opportunity (after update) { }" },
  { Id: sfId("01q", 3), Name: "AccountTrigger", TableEnumOrId: "Account", ApiVersion: 60, Status: "Active", Body: "trigger AccountTrigger on Account (before update) { }" },
];

export const ORG_COVERAGE = [{ PercentCovered: 82 }];

export const CLASS_COVERAGE = [
  { ApexClassOrTrigger: { Name: "OpportunityHandler" }, NumLinesCovered: 180, NumLinesUncovered: 20 },
  { ApexClassOrTrigger: { Name: "LeadRouter" }, NumLinesCovered: 90, NumLinesUncovered: 10 },
  // Trap: a healthy org-wide figure hiding a weak class.
  { ApexClassOrTrigger: { Name: "LegacySync" }, NumLinesCovered: 15, NumLinesUncovered: 45 },
];

const METADATA_NS = 'xmlns="http://soap.sforce.com/2006/04/metadata"';

export const METADATA_FILES: Record<string, string> = {
  "package.xml": `<?xml version="1.0"?><Package ${METADATA_NS}><version>${API_VERSION}</version></Package>`,
  "flows/Opportunity_Route_Deal.flow": `<?xml version="1.0"?><Flow ${METADATA_NS}>
  <apiVersion>62.0</apiVersion><label>Route Deal</label><processType>AutoLaunchedFlow</processType><status>Active</status>
  <start><object>Opportunity</object><triggerType>RecordAfterSave</triggerType></start>
  <constants><name>Approver</name><value><stringValue>${userId(7)}</stringValue></value></constants>
  <recordUpdates><name>Update_Owner</name><object>Opportunity</object><inputAssignments><field>Legacy_Code__c</field></inputAssignments></recordUpdates>
</Flow>`,
  "flows/Lead_Assign.flow": `<?xml version="1.0"?><Flow ${METADATA_NS}>
  <apiVersion>62.0</apiVersion><label>Assign Lead</label><processType>AutoLaunchedFlow</processType><status>Active</status>
  <start><object>Lead</object><triggerType>RecordBeforeSave</triggerType></start>
  <recordUpdates><name>Set_Region</name><object>Lead</object><faultConnector><targetReference>Log</targetReference></faultConnector></recordUpdates>
</Flow>`,
  "flows/Renewal_Notice.flow": `<?xml version="1.0"?><Flow ${METADATA_NS}>
  <apiVersion>40.0</apiVersion><label>Renewal Notice</label><processType>AutoLaunchedFlow</processType><status>Active</status>
  <constants><name>Sender</name><value><stringValue>renewals@example.com</stringValue></value></constants>
  <actionCalls><name>Send</name><actionName>emailSimple</actionName><actionType>emailSimple</actionType></actionCalls>
</Flow>`,
  "flows/Opportunity_Old_Process.flow": `<?xml version="1.0"?><Flow ${METADATA_NS}>
  <apiVersion>50.0</apiVersion><label>Old Process</label><processType>Workflow</processType><status>Active</status>
  <processMetadataValues><name>ObjectType</name><value><stringValue>Opportunity</stringValue></value></processMetadataValues>
  <recordUpdates><name>Touch</name><object>Opportunity</object></recordUpdates>
</Flow>`,
  "flows/Draft_Idea.flow": `<?xml version="1.0"?><Flow ${METADATA_NS}>
  <apiVersion>62.0</apiVersion><label>Draft</label><processType>Flow</processType><status>Draft</status>
  <constants><name>Key</name><value><stringValue>${sfId("005", 99)}</stringValue></value></constants>
</Flow>`,
  "workflows/Opportunity.workflow": `<?xml version="1.0"?><Workflow ${METADATA_NS}>
  <rules><fullName>Notify_Large_Deal</fullName><active>true</active><actions><name>Alert</name><type>Alert</type></actions></rules>
  <rules><fullName>Old_Rule</fullName><active>false</active></rules>
</Workflow>`,
  "workflows/Lead.workflow": `<?xml version="1.0"?><Workflow ${METADATA_NS}>
  <rules><fullName>Lead_Alert</fullName><active>true</active><actions><name>Alert</name><type>Alert</type></actions></rules>
</Workflow>`,
  "objects/Opportunity.object": `<?xml version="1.0"?><CustomObject ${METADATA_NS}>
  <fields><fullName>Old_Region__c</fullName><type>Text</type></fields>
  <validationRules><fullName>Needs_Competitor</fullName><active>true</active><errorConditionFormula>ISBLANK(Competitor__c)</errorConditionFormula></validationRules>
</CustomObject>`,
  "approvalProcesses/Opportunity.Discount_Approval.approvalProcess": `<?xml version="1.0"?><ApprovalProcess ${METADATA_NS}><active>true</active></ApprovalProcess>`,
};
