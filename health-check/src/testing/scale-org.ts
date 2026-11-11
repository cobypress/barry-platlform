import type { OrgRecords } from "./simulator.ts";
import * as org from "./synthetic-org.ts";

// A big synthetic org, made on the fly for the scale test (docs/overnight/
// night-3/scale.md): as many users and records as our first customers have, or
// a fraction of that for a quick test. It keeps the synthetic org's set-up (its
// metadata, rules, licences and its first 15 users, so the calibration still
// fits) and replaces everything that grows with an org's size. Nothing is
// planted: the point is volume, not traps. Seeded, so every run makes the same
// org. All names are made up and every address is example.com (rule 8).

export type ScaleSize = {
  users: number;
  accounts: number;
  contacts: number;
  opportunities: number;
  /** Stage changes per opportunity, on average. */
  historyPerOpportunity: number;
  leads: number;
  tasks: number;
  events: number;
  emails: number;
  cases: number;
  /** Logins a week per active user, over the past year. */
  loginsPerWeek: number;
};

/** A 500-user org, as Brief AQ describes our largest first customers. */
export const FIVE_HUNDRED_USERS: ScaleSize = {
  users: 500,
  accounts: 60_000,
  contacts: 250_000,
  opportunities: 200_000,
  historyPerOpportunity: 2.5,
  leads: 300_000,
  tasks: 700_000,
  events: 300_000,
  emails: 60_000,
  cases: 50_000,
  loginsPerWeek: 3,
};

/** The same org shrunk by `factor`, at least the synthetic org's 15 users. */
export function scaled(size: ScaleSize, factor: number): ScaleSize {
  const by = (n: number): number => Math.max(1, Math.round(n * factor));
  return { ...size, users: Math.max(16, by(size.users)), accounts: by(size.accounts), contacts: by(size.contacts), opportunities: by(size.opportunities), leads: by(size.leads), tasks: by(size.tasks), events: by(size.events), emails: by(size.emails), cases: by(size.cases) };
}

const DAY = 86_400_000;
const asOf = Date.parse(org.AS_OF);
const stamp = (ms: number): string => new Date(ms).toISOString().replace("Z", "+0000");
const dateOnly = (ms: number): string => new Date(ms).toISOString().slice(0, 10);

function seeded(seed: number): () => number {
  let a = seed;
  return () => {
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// Ids start well above the synthetic org's, so none collide.
const BASE = 1_000_000;

export function buildScaleOrg(size: ScaleSize, seed = 20261107): OrgRecords {
  const random = seeded(seed);
  const between = (low: number, high: number): number => Math.floor(low + random() * (high - low + 1));
  const pick = <T>(items: readonly T[]): T => items[Math.floor(random() * items.length)]!;
  const daysAgo = (low: number, high: number): number => asOf - between(low, high) * DAY - between(0, 86_399) * 1000;

  // Users: the synthetic org's 15, then the rest. About one in ten is dormant and one in twenty inactive.
  const users = [...org.usersTable];
  const lastLogin = new Map<string, number | null>(org.USERS.map((u) => [org.userId(u.n), u.lastLogin]));
  for (let n = org.USERS.length + 1; n <= size.users; n++) {
    const roll = random();
    const days = roll < 0.02 ? null : roll < 0.12 ? between(91, 400) : between(0, 14);
    const username = `user${n}@example.com`;
    lastLogin.set(org.userId(n), days);
    users.push({
      Id: org.userId(n),
      Name: `Synthetic User ${n}`,
      Username: username,
      Email: username,
      IsActive: random() >= 0.05,
      UserType: "Standard",
      Profile: { Name: n % 50 === 0 ? "System Administrator" : "Standard User", UserLicense: { Name: "Salesforce" } },
      UserRole: { Name: n % 3 === 0 ? "Sales EU" : "Sales UK" },
      LastLoginDate: days === null ? null : stamp(asOf - days * DAY),
      CreatedDate: stamp(asOf - between(60, 2000) * DAY),
      ManagerId: n > 20 ? org.userId(16 + (n % 5)) : null,
      DelegatedApproverId: null,
      UserPreferencesLightningExperiencePreferred: random() >= 0.02,
    });
  }
  const active = users.filter((u) => u.IsActive);
  const reps = active.filter((u) => u.Profile.Name === "Standard User").map((u) => u.Id);
  const rep = (): string => pick(reps);

  // A year of logins, a few a week for each active user who's logged in lately.
  const loginHistory: OrgRecords["loginHistory"] = [];
  for (const u of active) {
    const last = lastLogin.get(u.Id);
    if (last === null || last === undefined) continue;
    for (let day = last; day < 365; day += Math.max(1, Math.round(7 / size.loginsPerWeek))) {
      loginHistory.push({ UserId: u.Id, LoginTime: stamp(asOf - day * DAY - between(28_800, 64_800) * 1000), Status: "Success", ApiType: null, Application: "Browser", LoginType: "Application", Platform: "Mac OSX" } as (typeof loginHistory)[number]);
    }
  }

  const permissionAssignments = [...org.PERMISSION_ASSIGNMENTS, ...active.filter((u) => u.Profile.Name === "System Administrator" && !org.usersTable.some((x) => x.Id === u.Id)).map((u) => ({ AssigneeId: u.Id, PermissionSetId: org.sfId("0PS", 1) }))];
  const supportUsers = active.slice(0, Math.max(5, Math.round(size.users * 0.15))).map((u) => ({ Id: u.Id, LastLoginDate: u.LastLoginDate, Profile: { UserLicense: { Name: u.Profile.UserLicense.Name } } }));

  const account = (): string => org.sfId("001", BASE + between(1, size.accounts));
  const contact = (): string => org.sfId("003", BASE + between(1, size.contacts));

  // Opportunities over two years, about seven in ten closed, each with a few stage changes.
  const open = ["Prospecting", "Discovery", "Proposal", "Negotiation"];
  const opportunities: org.SyntheticOpportunity[] = [];
  const history: org.SyntheticHistory[] = [];
  for (let i = 1; i <= size.opportunities; i++) {
    const Id = org.sfId("006", BASE + i);
    const created = daysAgo(1, 730);
    const closed = random() < 0.7 && created < asOf - 20 * DAY;
    const won = closed && random() < 0.3;
    const stage = closed ? (won ? "Closed Won" : "Closed Lost") : pick(open);
    const amount = random() < 0.05 ? null : between(2, 400) * 250;
    const closeAt = closed ? Math.min(asOf - DAY, created + between(10, 200) * DAY) : asOf + between(5, 120) * DAY;
    opportunities.push({
      Id,
      OwnerId: rep(),
      StageName: stage,
      Amount: amount,
      CloseDate: dateOnly(closeAt),
      CreatedDate: stamp(created),
      IsClosed: closed,
      IsWon: won,
      LastActivityDate: random() < 0.2 ? null : dateOnly(Math.min(asOf, created + between(0, 120) * DAY)),
      Type: pick(["New Business", "Existing Business", null] as const),
      LeadSource: pick(["Web", "Referral", "Partner", "Event", null] as const),
      RecordType: { DeveloperName: "Standard" },
      Loss_Reason__c: closed && !won ? pick(["Price", "Competitor", "No Decision", null] as const) : null,
      AccountId: random() < 0.03 ? null : account(),
      NextStep: !closed && random() < 0.6 ? "Follow up" : null,
    });
    const changes = Math.max(1, Math.round(size.historyPerOpportunity + (random() - 0.5) * 2));
    for (let k = 0; k < changes; k++) {
      const last = k === changes - 1;
      history.push({ Id: org.sfId("008", BASE + history.length + 1), OpportunityId: Id, StageName: last ? stage : open[Math.min(k, open.length - 1)]!, Amount: amount, CloseDate: dateOnly(closeAt), CreatedDate: stamp(Math.min(asOf, created + k * between(1, 30) * DAY)) });
    }
  }
  history.sort((a, b) => a.Id.localeCompare(b.Id));
  const wins = opportunities.filter((o) => o.IsWon);
  const priorWins = Array.from({ length: Math.round(wins.length / 10) }, (_, k) => ({ Id: org.sfId("006", BASE + size.opportunities + k + 1), AccountId: account(), CloseDate: dateOnly(asOf - between(760, 1100) * DAY), IsWon: true, StageName: "Closed Won" }));
  const wonAccounts = [...new Set(wins.flatMap((o) => (o.AccountId ? [o.AccountId] : [])))].sort().map((Id) => ({ Id, OwnerId: rep(), LastActivityDate: random() < 0.3 ? null : dateOnly(daysAgo(1, 400)) }));

  // Activity over the past year: tasks and events against deals, leads and contacts, and some emails.
  const deal = (): string => opportunities[between(0, opportunities.length - 1)]!.Id;
  const tasks: org.SyntheticTask[] = [];
  for (let i = 1; i <= size.tasks; i++) {
    const roll = random();
    tasks.push({ Id: org.sfId("00T", BASE + i), OwnerId: rep(), WhoId: roll < 0.4 ? contact() : null, WhatId: roll >= 0.25 ? deal() : null, CreatedDate: stamp(daysAgo(0, 364)), TaskSubtype: pick(["Task", "Call", "Email"] as const) });
  }
  const events: org.SyntheticEvent[] = [];
  for (let i = 1; i <= size.events; i++) events.push({ Id: org.sfId("00U", BASE + i), OwnerId: rep(), WhoId: random() < 0.3 ? contact() : null, WhatId: random() < 0.6 ? deal() : null, StartDateTime: stamp(daysAgo(0, 364)) });
  const emails: org.SyntheticEmail[] = [];
  for (let i = 1; i <= size.emails; i++) emails.push({ Id: org.sfId("02s", BASE + i), RelatedToId: deal(), ActivityId: random() < 0.5 ? tasks[between(0, tasks.length - 1)]!.Id : null, MessageDate: stamp(daysAgo(0, 364)), CreatedById: rep() });

  // Leads over two years, about one in four converted.
  const sources = ["Web", "Web", "Event", "Referral", "Partner", null] as const;
  const leads: org.SyntheticLead[] = [];
  const statusChanges: OrgRecords["leadFunnel"]["statusChanges"] = [];
  const members: OrgRecords["leadFunnel"]["members"] = [];
  const staleLeadRows: OrgRecords["staleLeadRows"] = [];
  const openBy = new Map<string | null, number>();
  const staleBy = new Map<string | null, number>();
  for (let i = 1; i <= size.leads; i++) {
    const Id = org.sfId("00Q", BASE + i);
    const created = daysAgo(1, 730);
    const converted = random() < 0.25;
    const source = pick(sources);
    const OwnerId = rep();
    leads.push({ Id, OwnerId, LeadSource: source, CreatedDate: stamp(created), IsConverted: converted, ConvertedDate: converted ? dateOnly(created + between(1, 60) * DAY) : null, ConvertedOpportunityId: converted && random() < 0.6 ? deal() : null, ConvertedContactId: converted ? contact() : null });
    if (random() < 0.3) statusChanges.push({ Id: org.sfId("017", BASE + i), LeadId: Id, CreatedDate: stamp(Math.min(asOf, created + between(1, 72) * 3_600_000)) });
    if (random() < 0.3) members.push({ Id: org.sfId("00v", BASE + i), CampaignId: org.sfId("701", between(1, 3)), LeadId: Id });
    if (!converted) {
      openBy.set(source, (openBy.get(source) ?? 0) + 1);
      // Untouched for 90 days or more: what dq.stale-leads counts, and its action list lists.
      if (created < asOf - 90 * DAY && random() < 0.35) {
        staleBy.set(source, (staleBy.get(source) ?? 0) + 1);
        staleLeadRows.push({ Id, Name: `Synthetic lead ${i}`, OwnerId, LeadSource: source, CreatedDate: stamp(created) });
      }
    }
  }
  const bySource = (m: Map<string | null, number>) => [...m].map(([s, c]) => ({ s, c }));
  const openLeads = [...openBy.values()].reduce((a, b) => a + b, 0);

  // Contacts: counts, and the rows behind the two action lists.
  const noAccount = Math.round(size.contacts * 0.02);
  const noEmail = Math.round(size.contacts * 0.08);
  const contactRow = (n: number) => ({ Id: org.sfId("003", BASE + n), Name: `Synthetic contact ${n}`, OwnerId: rep() });

  // A year of cases, with their history, milestones, replies and routed work.
  const supportCases: OrgRecords["supportCases"] = { cases: [], history: [], milestones: [], replies: [], work: [], articles: [] };
  const agents = supportUsers.map((u) => u.Id);
  for (let i = 1; i <= size.cases; i++) {
    const Id = org.sfId("500", BASE + i);
    const created = daysAgo(0, 364);
    const closedAt = created + between(2, 200) * 3_600_000;
    const isOpen = closedAt > asOf || random() < 0.1;
    const origin = pick(["Email", "Email", "Web", "Phone", null] as const);
    const owner = pick(agents);
    supportCases.cases.push({ Id, Status: isOpen ? pick(["New", "Working", "Escalated"] as const) : "Closed", IsClosed: !isOpen, Priority: pick(["High", "Medium", "Low"] as const), Origin: origin, OwnerId: owner, CreatedDate: stamp(created), ClosedDate: isOpen ? null : stamp(closedAt), Type: pick(["Problem", "Question", null] as const), Reason: pick(["Installation", "Performance", null] as const), ContactId: random() < 0.05 ? null : contact(), AccountId: random() < 0.4 ? null : account() });
    if (!isOpen) {
      supportCases.history.push({ Id: org.sfId("017", 50 * BASE + supportCases.history.length + 1), CaseId: Id, Field: "Status", OldValue: "Working", NewValue: "Closed", CreatedDate: stamp(closedAt) });
      if (random() < 0.05) supportCases.articles.push({ Id: org.sfId("0Ar", BASE + supportCases.articles.length + 1), CaseId: Id, CreatedDate: stamp(closedAt) });
    }
    supportCases.milestones.push({ CaseId: Id, MilestoneTypeId: org.sfId("557", 1), IsCompleted: true, IsViolated: random() < 0.05, StartDate: stamp(created), TargetDate: stamp(created + 8 * 3_600_000), CompletionDate: stamp(created + 4 * 3_600_000) });
    if (!isOpen) supportCases.milestones.push({ CaseId: Id, MilestoneTypeId: org.sfId("557", 2), IsCompleted: true, IsViolated: random() < 0.15, StartDate: stamp(created), TargetDate: stamp(created + 5 * DAY), CompletionDate: stamp(closedAt) });
    if (origin === "Email") {
      supportCases.replies.push({ Id: org.sfId("02s", 20 * BASE + supportCases.replies.length + 1), ParentId: Id, MessageDate: stamp(created), Incoming: true });
      if (random() < 0.9) supportCases.replies.push({ Id: org.sfId("02s", 20 * BASE + supportCases.replies.length + 1), ParentId: Id, MessageDate: stamp(created + between(1, 30) * 3_600_000), Incoming: false });
    }
    supportCases.work.push({ Id: org.sfId("0Bz", BASE + i), UserId: owner, Status: "Closed", RequestDateTime: stamp(created), AcceptDateTime: stamp(created + 60_000), DeclineDateTime: null, SpeedToAnswer: between(20, 200) });
  }

  // A primary quote on each closed deal with an amount, a few approvals, and contracts.
  const cpqQuotes = opportunities
    .filter((o) => o.IsClosed && o.Amount !== null && o.Amount > 0)
    .map((o, i) => {
      const discount = between(0, 30);
      return { Id: org.sfId("a1Q", BASE + i + 1), Name: `Q-${String(i + 1).padStart(7, "0")}`, OwnerId: o.OwnerId, SBQQ__Primary__c: true, SBQQ__Opportunity2__c: o.Id, SBQQ__ListAmount__c: Math.round((o.Amount! / (1 - discount / 100)) * 100) / 100, SBQQ__NetAmount__c: o.Amount, SBQQ__Ordered__c: o.IsWon && random() < 0.9, CreatedDate: stamp(Date.parse(`${o.CloseDate}T12:00:00Z`) - 10 * DAY) };
    });
  const cpqApprovals = cpqQuotes.filter(() => random() < 0.1).map((q, i) => ({ Id: org.sfId("04g", BASE + i + 1), TargetObjectId: q.Id, Status: "Approved", CreatedDate: q.CreatedDate }));
  const cpqContracts = Array.from({ length: Math.round(size.accounts / 12) }, (_, i) => ({ Id: org.sfId("800", BASE + i + 1), ContractNumber: String(i + 1).padStart(8, "0"), OwnerId: rep(), EndDate: dateOnly(asOf + between(-30, 400) * DAY), Status: "Activated", SBQQ__RenewalOpportunity__c: random() < 0.5 ? deal() : null }));

  // Account Engagement: most leads synced, and the open ones handed over in the last 90 days.
  const synced = Math.round(size.leads * 0.7);
  const pardotOpenLeads = leads.filter((l) => !l.IsConverted && Date.parse(l.CreatedDate.replace("+0000", "Z")) >= asOf - 90 * DAY).map((l) => ({ Id: l.Id, Name: `Synthetic prospect ${Number(l.Id.slice(3, 15))}`, OwnerId: l.OwnerId, CreatedDate: l.CreatedDate, LastActivityDate: random() < 0.5 ? null : dateOnly(daysAgo(0, 30)), LeadSource: l.LeadSource ?? "Web" }));

  // Collaborative Forecasts for the quarter holding today: a row for each rep and category, and a fact for each open deal closing in it.
  const period = org.PERIODS[0]!;
  const forecastFacts = opportunities
    .filter((o) => !o.IsClosed && o.CloseDate >= period.StartDate && o.CloseDate <= period.EndDate)
    .map((o) => ({ OpportunityId: o.Id, PeriodId: period.Id, ForecastCategoryName: o.StageName === "Negotiation" ? "Commit" : o.StageName === "Proposal" ? "Best Case" : "Pipeline" }));
  const forecastItems = reps.flatMap((owner) =>
    ["Commit", "Best Case", "Pipeline"].map((category) => {
      const before = between(1, 40) * 10_000;
      const after = random() < 0.1 ? before + between(1, 10) * 10_000 : before;
      return { OwnerId: owner, PeriodId: period.Id, ForecastingTypeId: org.sfId("0Db", 1), ForecastCategoryName: category, ForecastAmount: after, AmountWithoutAdjustments: before, HasAdjustment: before !== after };
    }),
  );

  const share = (total: number) => Array.from({ length: Math.min(reps.length, 200) }, (_, k) => [Number(reps[k]!.slice(3, 15)), Math.round(total / Math.min(reps.length, 200))] as [number, number]);
  return {
    users,
    supportUsers,
    loginHistory,
    permissionAssignments,
    pipeline: { opportunities, history },
    priorWins,
    activity: { tasks, events, emails },
    wonAccounts,
    leadFunnel: { leads, statusChanges, members },
    leads: { open: openLeads, stale: staleLeadRows.length, openBySource: bySource(openBy), staleBySource: bySource(staleBy) },
    staleLeadRows,
    contacts: { total: size.contacts, noAccount, noEmail },
    contactsNoAccount: Array.from({ length: noAccount }, (_, i) => contactRow(i + 1)),
    contactsNoEmail: Array.from({ length: noEmail }, (_, i) => contactRow(noAccount + i + 1)),
    supportCases,
    cpqQuotes,
    cpqApprovals,
    cpqContracts,
    pardotCounts: { synced, ungraded: Math.round(synced * 0.4), unscored: Math.round(synced * 0.1) },
    pardotOpenLeads,
    recordTotals: {
      Account: { total: size.accounts, recent: Math.round(size.accounts * 0.2) },
      Contact: { total: size.contacts, recent: Math.round(size.contacts * 0.2) },
      Lead: { total: size.leads, recent: Math.round(size.leads * 0.4) },
      Opportunity: { total: size.opportunities, recent: Math.round(size.opportunities * 0.35) },
    },
    recordCounts: [
      { name: "Account", count: size.accounts },
      { name: "Project__c", count: Math.round(size.accounts / 10) },
      { name: "Invoice__c", count: size.accounts * 8 },
      { name: "Project_Line__c", count: Math.round(size.accounts / 2) },
      { name: "Empty_Object__c", count: 0 },
    ],
    ownership: {
      Account: share(size.accounts),
      Contact: share(size.contacts),
      Lead: share(size.leads),
      Case: [],
      Project__c: share(Math.round(size.accounts / 10)),
      Invoice__c: share(size.accounts * 8),
    },
    forecastItems,
    forecastFacts,
  };
}
