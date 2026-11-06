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

type UserSeed = { n: number; name: string; login?: string; lastLogin: number | null; active?: boolean; role?: string; created?: number; manager?: number; licence?: string; profile?: string; classic?: boolean };

export const USERS: UserSeed[] = [
  { n: 1, name: "Alex Morgan", lastLogin: 2, role: "Sales UK" },
  { n: 2, name: "Priya Shah", lastLogin: 1, role: "Sales UK" },
  // Trap: still on Classic.
  { n: 3, name: "Tom Baker", lastLogin: 3, role: "Sales EU", manager: 12, classic: true },
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
  // Trap: still on Classic.
  { n: 14, name: "Ren Ito", lastLogin: 4, role: "Sales EU", classic: true },
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
    UserPreferencesLightningExperiencePreferred: !u.classic,
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
  // A second way to lose, so lost deals can be split by how they closed.
  { ApiName: "Closed Lost - No Budget", MasterLabel: "Closed Lost - No Budget", IsActive: true, IsClosed: true, IsWon: false, SortOrder: 9 },
  // Trap: retired when the process was renamed, and still holding open deals with
  // too little history to weight them by.
  { ApiName: "Qualified", MasterLabel: "Qualified", IsActive: false, IsClosed: false, IsWon: false, SortOrder: 10 },
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
  Type: string | null;
  LeadSource: string | null;
  RecordType: { DeveloperName: string };
  Loss_Reason__c: string | null;
  AccountId: string;
  NextStep: string | null;
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
      AccountId: sfId("001", (i % 120) + 1),
      NextStep: null,
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
      AccountId: sfId("001", 1),
      NextStep: null,
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
    // Trap: the same close date moved again and again, which only history shows.
    if (i % 5 === 0) {
      for (let push = 3; push >= 1; push--) addHistory(id, stage, amount, close - push * 20 * DAY, at + (3 - push) * DAY);
      addHistory(id, stage, amount, close, at + 3 * DAY);
    }
    // Drawn before the trap is applied, so planting it doesn't move the seeded sequence.
    const touched = random() < 0.2 ? 120 : between(1, 30);
    // Trap: one rep's deals have gone quiet, under a total that looks fine.
    opportunities.push({
      Id: id,
      OwnerId: userId(owner),
      StageName: stage,
      Amount: amount,
      CloseDate: dateOnly(close),
      CreatedDate: new Date(created).toISOString().replace("Z", "+0000"),
      IsClosed: false,
      IsWon: false,
      LastActivityDate: dateOnly(asOf - (owner === 3 && i % 8 === 2 ? 120 : owner === 2 ? 10 : touched) * DAY),
      Type: "New Business",
      LeadSource: SOURCES[between(0, SOURCES.length - 1)]!,
      RecordType: { DeveloperName: renewal ? "Renewal" : "New_Business" },
      Loss_Reason__c: null,
      // Trap: a quarter of open deals sit on one customer, so a loss there hurts.
      AccountId: sfId("001", i % 4 === 0 ? 900 : (i % 120) + 1),
      // Trap: the total looks fine, and one rep leaves next steps empty on nearly everything.
      NextStep: owner === 14 ? (i % 8 === 0 ? "Call the buyer on Friday" : null) : i % 9 === 0 ? null : "Call the buyer on Friday",
    });
  }

  // Trap: the EU team moves deals it goes on to lose back a stage, and forward
  // again an hour later. Planted after the seeded draws, so nothing else in the
  // data moves.
  const eu = new Set([userId(3), userId(14)]);
  for (const o of opportunities.filter((x) => x.IsClosed && !x.IsWon && eu.has(x.OwnerId))) {
    const rows = history.filter((h) => h.OpportunityId === o.Id && OPEN_PATH.includes(h.StageName));
    const next = rows.find((h) => h.StageName !== rows[0]!.StageName);
    if (!next) continue;
    const at = new Date(next.CreatedDate.replace("+0000", "Z")).getTime();
    for (const [hours, stage] of [[1, rows[0]!.StageName], [2, next.StageName]] as const) {
      history.push({ Id: "", OpportunityId: o.Id, StageName: stage, Amount: next.Amount, CloseDate: next.CloseDate, CreatedDate: new Date(at + hours * 3_600_000).toISOString().replace("Z", "+0000") });
    }
  }
  // Ids follow each deal's rows in time order, as Salesforce's do, so rows
  // written in the same second still sort the way they happened.
  const first = new Map<string, number>();
  history.forEach((h, i) => first.has(h.OpportunityId) || first.set(h.OpportunityId, i));
  const position = new Map(history.map((h, i) => [h, i]));
  history.sort((a, b) => first.get(a.OpportunityId)! - first.get(b.OpportunityId)! || a.CreatedDate.localeCompare(b.CreatedDate) || position.get(a)! - position.get(b)!);
  history.forEach((h, i) => (h.Id = sfId("008", i + 1)));
  return { opportunities, history };
}

const SERVICES_SEED = 20261104;
const CLIENTS = 30;
const client = (k: number): string => sfId("001", 300 + k);
const stampOf = (ms: number): string => new Date(ms).toISOString().replace("Z", "+0000");
const dayMs = (day: string): number => new Date(`${day}T12:00:00Z`).getTime();

type PriorWin = { Id: string; AccountId: string; CloseDate: string; IsWon: boolean; StageName: string };

/**
 * A services side to the business: account managers selling to retained
 * clients, mostly by word of mouth, with no lead source and a lot of repeat work.
 * It's added after the original deals so their seeded data doesn't move, and
 * every deal here was created over a year ago, so the activity checks, which
 * only read the last year, see exactly what they did before.
 */
function buildServices(firstId: number): { opportunities: SyntheticOpportunity[]; history: SyntheticHistory[]; priorWins: PriorWin[] } {
  const random = seeded(SERVICES_SEED);
  const between = (low: number, high: number): number => Math.floor(low + random() * (high - low + 1));
  const opportunities: SyntheticOpportunity[] = [];
  const history: SyntheticHistory[] = [];
  let next = firstId;
  const addHistory = (oppId: string, stage: string, amount: number | null, close: string, at: number): void => {
    history.push({ Id: sfId("008", 500_000 + history.length), OpportunityId: oppId, StageName: stage, Amount: amount, CloseDate: close, CreatedDate: stampOf(at) });
  };
  const earliest = asOf - 725 * DAY;
  const latest = asOf - 366 * DAY;

  /** One closed deal along `path`, with its time split across the stages. */
  const closedDeal = (path: string[], final: string, created: number, closed: number): void => {
    const id = sfId("006", next++);
    const amount = random() < 0.07 ? null : between(5, 60) * 1000;
    const closeDay = dateOnly(closed);
    // Trap: the first close date is often too early, so won deals land late against it.
    const first = dateOnly(closed - between(-15, 75) * DAY);
    const weights = path.map(() => between(5, 25));
    const span = closed - created;
    // A long deal spends most of its time in one stage, so no stage's usual pace moves.
    const long = span > 150 * DAY ? between(0, path.length - 1) : -1;
    const share = weights.reduce((a, b) => a + b, 0);
    const others = weights.reduce((sum, w, i) => sum + (i === long ? 0 : w), 0) * DAY;
    let at = created;
    path.forEach((stage, s) => {
      addHistory(id, stage, amount, first, at);
      at += long === -1 ? (span * weights[s]!) / share : s === long ? span - others : weights[s]! * DAY;
    });
    if (first !== closeDay) addHistory(id, path.at(-1)!, amount, closeDay, closed - DAY);
    addHistory(id, final, amount, closeDay, closed);
    opportunities.push({
      Id: id,
      OwnerId: userId(next % 2 === 0 ? 1 : 2),
      StageName: final,
      Amount: amount,
      CloseDate: closeDay,
      CreatedDate: stampOf(created),
      IsClosed: true,
      IsWon: final === "Closed Won",
      LastActivityDate: closeDay,
      Type: random() < 0.7 ? "Existing Business" : "New Business",
      LeadSource: random() < 0.35 ? null : SOURCES[between(0, SOURCES.length - 1)]!,
      RecordType: { DeveloperName: random() < 0.2 ? "Renewal" : "New_Business" },
      Loss_Reason__c: final.startsWith("Closed Lost") && random() < 0.5 ? "Price" : null,
      AccountId: client(between(1, CLIENTS)),
      NextStep: null,
    });
  };

  // Enough wins in each quarter for quarterly medians, at a win rate not far from the rest of the org's.
  const quarters: Array<[string, string, number, number, number]> = [
    ["2025-01-01", "2025-03-31", 22, 9, 3],
    ["2025-04-01", "2025-06-30", 11, 10, 3],
    ["2025-07-01", "2025-09-30", 12, 10, 3],
    ["2025-10-01", "2025-12-31", 19, 10, 3],
    ["2026-01-01", "2026-03-31", 10, 9, 3],
    ["2026-04-01", "2026-06-30", 12, 10, 3],
    ["2026-07-01", "2026-09-30", 14, 10, 3],
  ];
  for (const [from, to, won, lost, noDecision] of quarters) {
    const outcomes = [...Array<string>(won).fill("Closed Won"), ...Array<string>(lost).fill("lost"), ...Array<string>(noDecision).fill("Closed No Decision")];
    for (const outcome of outcomes) {
      const closed = dayMs(from) + between(0, Math.round((dayMs(to) - dayMs(from)) / DAY)) * DAY;
      const usual = closed - between(40, 120) * DAY;
      // A deal closing within the year was created before it, at some point over the months before.
      const created = Math.max(earliest, usual <= latest ? usual : latest - between(0, 300) * DAY);
      const final = outcome === "lost" ? (random() < 0.4 ? "Closed Lost - No Budget" : "Closed Lost") : outcome;
      const depth = final === "Closed Won" ? OPEN_PATH.length : between(1, OPEN_PATH.length);
      closedDeal(OPEN_PATH.slice(0, depth), final, created, closed);
    }
  }

  // Trap: the old process had a Qualified stage. Too few deals went through it
  // for it to have a win rate of its own.
  const OLD_PATH = ["Prospecting", "Qualified", "Discovery", "Proposal", "Negotiation"];
  const oldOutcomes = ["Closed Won", "Closed Lost", "Closed Won", "Closed No Decision", "Closed Lost", "Closed Won", "Closed Lost", "Closed Won", "Closed Lost", "Closed No Decision", "Closed Won", "Closed Lost"];
  oldOutcomes.forEach((final, i) => {
    const created = asOf - between(700, 720) * DAY;
    closedDeal(OLD_PATH.slice(0, final === "Closed Won" ? OLD_PATH.length : 2 + (i % 3)), final, created, created + between(70, 130) * DAY);
  });

  // Trap: open deals left in the retired stage, past their date, owned by
  // someone who has left. Two have no amount. Someone in delivery still logs
  // activity on them, so they aren't stale.
  [30_000, null, 45_000, 20_000, null, 60_000].forEach((amount, i) => {
    const id = sfId("006", next++);
    const created = asOf - between(600, 700) * DAY;
    const close = dateOnly(created + 90 * DAY);
    addHistory(id, "Prospecting", amount, close, created);
    addHistory(id, "Qualified", amount, close, created + between(10, 20) * DAY);
    opportunities.push({
      Id: id,
      OwnerId: userId(9),
      StageName: "Qualified",
      Amount: amount,
      CloseDate: close,
      CreatedDate: stampOf(created),
      IsClosed: false,
      IsWon: false,
      LastActivityDate: dateOnly(asOf - between(3, 20) * DAY),
      Type: "Existing Business",
      LeadSource: null,
      RecordType: { DeveloperName: "New_Business" },
      Loss_Reason__c: null,
      AccountId: client(i + 1),
      NextStep: "Call the buyer on Friday",
    });
  });

  // Trap: from the last quarter of 2025, deals that were going to be lost went
  // through their stages in a day, then back to Prospecting, where they sat
  // until someone closed them. Every deal is still in the same stages, so no
  // stage's win rate moves, and it's done after the seeded draws.
  const deadEnds = opportunities.filter((o) => !o.IsWon && o.IsClosed && o.CloseDate >= "2025-10-01");
  for (const o of deadEnds) {
    const rows = history.filter((h) => h.OpportunityId === o.Id && OPEN_PATH.includes(h.StageName)).sort((a, b) => a.CreatedDate.localeCompare(b.CreatedDate));
    if (new Set(rows.map((h) => h.StageName)).size < 2) continue;
    const start = new Date(rows[0]!.CreatedDate.replace("+0000", "Z")).getTime();
    rows.forEach((h, k) => (h.CreatedDate = stampOf(start + k * 3_600_000)));
    addHistory(o.Id, "Prospecting", o.Amount, rows.at(-1)!.CloseDate, start + rows.length * 3_600_000);
  }
  // Ids follow each deal's rows in time order, as Salesforce's do.
  const first = new Map<string, number>();
  history.forEach((h, i) => first.has(h.OpportunityId) || first.set(h.OpportunityId, i));
  const position = new Map(history.map((h, i) => [h, i]));
  history.sort((a, b) => first.get(a.OpportunityId)! - first.get(b.OpportunityId)! || a.CreatedDate.localeCompare(b.CreatedDate) || position.get(a)! - position.get(b)!);
  history.forEach((h, i) => (h.Id = sfId("008", 500_000 + i)));

  // Half the retained clients had already bought before the analysis window.
  const priorWins = Array.from({ length: CLIENTS / 2 }, (_, k) => ({ Id: sfId("006", 900_000 + k), AccountId: client(k + 1), CloseDate: dateOnly(asOf - between(760, 1100) * DAY), IsWon: true, StageName: "Closed Won" }));
  return { opportunities, history, priorWins };
}

/** The original deals. Activity, leads and record totals are built from these alone, so other domains' data doesn't move. */
export const basePipeline = buildPipeline();
const services = buildServices(basePipeline.opportunities.length + 1);
export const pipeline = { opportunities: [...basePipeline.opportunities, ...services.opportunities], history: [...basePipeline.history, ...services.history].sort((a, b) => a.Id.localeCompare(b.Id)) };
/** Wins before the analysis window, which only the prior wins read returns. */
export const PRIOR_WINS = services.priorWins;

export type SyntheticTask = { Id: string; OwnerId: string; WhoId: string | null; WhatId: string | null; CreatedDate: string; TaskSubtype: string | null };
export type SyntheticEvent = { Id: string; OwnerId: string; WhoId: string | null; WhatId: string | null; StartDateTime: string };
export type SyntheticEmail = { Id: string; RelatedToId: string | null; ActivityId: string | null; MessageDate: string; CreatedById: string };

const stamp = (ms: number): string => new Date(ms).toISOString().replace("Z", "+0000");

function buildActivity(): { tasks: SyntheticTask[]; events: SyntheticEvent[]; emails: SyntheticEmail[] } {
  const random = seeded(20261102);
  const between = (low: number, high: number): number => Math.floor(low + random() * (high - low + 1));
  const reps = [1, 2, 3, 14];
  const tasks: SyntheticTask[] = [];
  const events: SyntheticEvent[] = [];
  const emails: SyntheticEmail[] = [];
  const oneYearAgo = asOf - 365 * DAY;
  const addTask = (owner: string, whatId: string | null, at: number, subtype: string | null = "Task"): string => {
    const Id = sfId("00T", tasks.length + 1);
    tasks.push({ Id, OwnerId: owner, WhoId: null, WhatId: whatId, CreatedDate: stamp(at), TaskSubtype: subtype });
    return Id;
  };
  const dealIds = basePipeline.opportunities.filter((o) => o.OwnerId === userId(1) || o.OwnerId === userId(2)).map((o) => o.Id);

  // Trap, built first: a real check once missed this. Total events are flat all
  // year. From July, almost none of them are linked to a record, which is what
  // happens when synced events stop being related. A monthly total shows nothing.
  const COLLAPSE = Date.UTC(2026, 6, 1);
  for (let month = 0; month < 12; month++) {
    const first = Date.UTC(2025, 10 + month, 1);
    for (let i = 0; i < 60; i++) {
      const at = first + between(0, 26) * DAY + between(8, 17) * 3_600_000;
      if (at > asOf) continue;
      const linked = random() < (at >= COLLAPSE ? 0.05 : 0.75);
      events.push({ Id: sfId("00U", events.length + 1), OwnerId: userId(reps[i % 3]!), WhoId: null, WhatId: linked ? dealIds[between(0, dealIds.length - 1)]! : null, StartDateTime: stamp(at) });
    }
  }

  // Closed deals. Won deals get more touches than lost ones. Trap: most of the EU
  // team's closed deals have nothing logged at all, inside a total that looks fine.
  for (const deal of basePipeline.opportunities.filter((o) => o.IsClosed && new Date(o.CreatedDate.replace("+0000", "Z")).getTime() >= oneYearAgo)) {
    const eu = deal.OwnerId === userId(3) || deal.OwnerId === userId(14);
    const roll = random();
    const count = eu && roll < 0.7 ? 0 : deal.IsWon ? between(5, 9) : between(1, 4);
    for (let i = 0; i < count; i++) addTask(deal.OwnerId, deal.Id, new Date(deal.CreatedDate.replace("+0000", "Z")).getTime() + (i + 1) * DAY);
  }

  // Open deals in the last 90 days. Trap: one rep has logged almost nothing.
  for (const deal of basePipeline.opportunities.filter((o) => !o.IsClosed)) {
    const count = deal.OwnerId === userId(14) ? (random() < 0.1 ? 1 : 0) : between(3, 6);
    for (let i = 0; i < count; i++) addTask(deal.OwnerId, deal.Id, asOf - between(1, 85) * DAY);
  }

  // Trap: one person logs tasks against nothing, so they vanish from every deal.
  for (let i = 0; i < 40; i++) addTask(userId(15), i % 5 === 0 ? dealIds[0]! : null, asOf - between(1, 200) * DAY);

  // Trap: an enhanced email also makes a task, so counting both would double it.
  for (let i = 0; i < 30; i++) {
    const deal = basePipeline.opportunities[224 + i]!;
    const at = asOf - between(1, 60) * DAY;
    const logged = i % 2 === 0 ? addTask(deal.OwnerId, deal.Id, at, "Email") : null;
    emails.push({ Id: sfId("02s", i + 1), RelatedToId: deal.Id, ActivityId: logged, MessageDate: stamp(at), CreatedById: deal.OwnerId });
  }
  return { tasks, events, emails };
}

export const activity = buildActivity();

export type SyntheticLead = { Id: string; OwnerId: string; LeadSource: string | null; CreatedDate: string; IsConverted: boolean; ConvertedDate: string | null; ConvertedOpportunityId: string | null; ConvertedContactId: string | null };

function buildLeads() {
  const random = seeded(20261103);
  const between = (low: number, high: number): number => Math.floor(low + random() * (high - low + 1));
  const leads: SyntheticLead[] = [];
  const statusChanges: Array<{ Id: string; LeadId: string; CreatedDate: string }> = [];
  const members: Array<{ Id: string; CampaignId: string; LeadId: string }> = [];
  const won = basePipeline.opportunities.filter((o) => o.IsWon).map((o) => o.Id);
  const lost = basePipeline.opportunities.filter((o) => o.StageName === "Closed Lost").map((o) => o.Id);
  const sources = ["Web", "Web", "Web", "Event", "Referral", "Partner"];
  const owners = [1, 2, 3];
  let tasks = activity.tasks.length;
  for (let i = 1; i <= 360; i++) {
    const source = i % 40 === 0 ? null : sources[i % sources.length]!;
    const partner = source === "Partner";
    const created = asOf - between(35, 600) * DAY;
    const convertRoll = random();
    // Trap: Partner leads barely convert, and what does never wins.
    const converted = convertRoll < (partner ? 0.04 : 0.3);
    // Trap: Referral leads are converted to tidy them up, with no deal behind most of them.
    const noOpportunity = converted && random() < (source === "Referral" ? 0.7 : 0.1);
    const Id = sfId("00Q", i);
    const contact = converted ? sfId("003", i) : null;
    const opportunity = !converted || noOpportunity ? null : partner ? lost[i % lost.length]! : won[i % won.length]!;
    leads.push({
      Id,
      OwnerId: userId(owners[i % owners.length]!),
      LeadSource: source,
      CreatedDate: stamp(created),
      IsConverted: converted,
      ConvertedDate: converted ? dateOnly(created + 20 * DAY) : null,
      ConvertedOpportunityId: opportunity,
      ConvertedContactId: contact,
    });
    // Trap: Partner leads sit for days, and Event leads often get no answer at all.
    const hours = partner ? between(100, 200) : between(1, 20);
    const touched = random() >= (source === "Event" ? 0.45 : 0.04);
    const at = created + hours * 3_600_000;
    if (touched && at >= asOf - 365 * DAY) {
      // Trap: a converted lead's activity moves to the contact, so a lead id alone would miss it.
      tasks++;
      activity.tasks.push({ Id: sfId("00T", tasks), OwnerId: userId(owners[i % owners.length]!), WhoId: contact ?? Id, WhatId: null, CreatedDate: stamp(at), TaskSubtype: "Call" });
    }
    // Trap: some first responses show only as a status change, with no task logged.
    if (!touched && i % 2 === 0) statusChanges.push({ Id: sfId("017", i), LeadId: Id, CreatedDate: stamp(at) });
    if (i % 3 === 0) members.push({ Id: sfId("00v", i), CampaignId: sfId("701", (i % 2) + 1), LeadId: Id });
  }
  return { leads, statusChanges, members };
}

export const leadFunnel = buildLeads();

// Past customers. Trap: an account with an open deal, or recent activity, isn't dormant.
export const WON_ACCOUNTS = [...new Set(pipeline.opportunities.filter((o) => o.IsWon).map((o) => o.AccountId))].sort().map((Id, i) => ({
  Id,
  OwnerId: userId([1, 2, 3, 9][i % 4]!),
  LastActivityDate: i % 3 === 0 ? dateOnly(asOf - 200 * DAY) : i % 3 === 1 ? null : dateOnly(asOf - 20 * DAY),
}));

export const CAMPAIGNS = [
  { Id: sfId("701", 1), Type: "Webinar", ActualCost: 2_000 },
  // Trap: all the spend, and a return well under it.
  { Id: sfId("701", 2), Type: "Trade Show", ActualCost: 900_000 },
  // Trap: no cost recorded, so it can't be given a return.
  { Id: sfId("701", 3), Type: "Email", ActualCost: null },
];

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

// Objects the field-describe step reads before querying them. Kept apart from
// DESCRIBES, which the field sprawl and usage rules scan.
const lookup = (name: string, relationshipName: string) => standardField(name, "reference", { relationshipName });
export const FIELD_DESCRIBES: Record<string, { fields: unknown[] }> = {
  LoginHistory: { fields: ["Id", "LoginTime", "Status", "ApiType", "ApiVersion", "Application", "LoginType", "Platform", "SourceIp"].map((f) => standardField(f)).concat(lookup("UserId", "User"), lookup("AuthenticationServiceId", "AuthenticationService")) },
  ForecastingItem: {
    fields: ["Id", "ForecastCategoryName", "ForecastAmount", "AmountWithoutAdjustments", "AmountWithoutManagerAdjustment", "HasAdjustment", "OwnerOnlyAmount"]
      .map((f) => standardField(f))
      .concat(lookup("OwnerId", "Owner"), lookup("PeriodId", "Period"), lookup("ForecastingTypeId", "ForecastingType")),
  },
  Campaign: { fields: ["Id", "Name", "Type", "Status", "ActualCost", "BudgetedCost", "IsActive"].map((f) => standardField(f)) },
  User: {
    fields: ["Id", "Name", "Username", "Email", "IsActive", "UserType", "LastLoginDate", "CreatedDate", "UserPreferencesLightningExperiencePreferred"]
      .map((f) => standardField(f))
      .concat(lookup("ProfileId", "Profile"), lookup("UserRoleId", "UserRole"), lookup("ManagerId", "Manager"), lookup("DelegatedApproverId", "DelegatedApprover")),
  },
  // Service Cloud's, described before they're read.
  Case: {
    fields: ["Id", "Status", "IsClosed", "Priority", "Origin", "CreatedDate", "ClosedDate", "Type", "Reason"].map((f) => standardField(f)).concat(lookup("OwnerId", "Owner"), lookup("ContactId", "Contact"), lookup("AccountId", "Account")),
  },
  Group: { fields: ["Id", "Name", "DeveloperName", "Type"].map((f) => standardField(f)).concat(lookup("QueueRoutingConfigId", "QueueRoutingConfig")) },
};

// Total and recent counts for custom field usage, as COUNT(field) would give them.
export const RECORD_TOTALS: Record<string, { total: number; recent: number }> = {
  Account: { total: 500, recent: 120 },
  Contact: { total: 500, recent: 90 },
  Lead: { total: 500, recent: 300 },
  // The original deals only, so field fill rates don't move with the services block.
  Opportunity: { total: basePipeline.opportunities.length, recent: 150 },
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
  // Trap: one account over Salesforce's ownership skew line, on a custom object.
  Invoice__c: [[11, 12_000]],
};

export const RECORD_COUNTS = [
  { name: "Account", count: 500 },
  { name: "Project__c", count: 20 },
  { name: "Invoice__c", count: 12_000 },
  // Trap: a detail object has no OwnerId, which isn't a gap in the scan.
  { name: "Project_Line__c", count: 40 },
  { name: "Empty_Object__c", count: 0 },
];

export const USER_LICENSES = [
  { Name: "Salesforce", TotalLicenses: 30, UsedLicenses: 15, Status: "Active" },
  { Name: "Salesforce Platform", TotalLicenses: 10, UsedLicenses: 10, Status: "Active" },
  { Name: "Chatter Free", TotalLicenses: 5000, UsedLicenses: 0, Status: "Active" },
  // Trap: free integration seats nobody holds, while the integration account takes a full one.
  { Name: "Salesforce Integration", TotalLicenses: 5, UsedLicenses: 0, Status: "Active" },
];

// Traps: a seat-limited package with spare seats; a site licence, a free and an
// expired package, none of which are seats anyone pays for.
export const PACKAGE_LICENSES = [
  { NamespacePrefix: "acmecpq", AllowedLicenses: 20, UsedLicenses: 12, Status: "Active", ExpirationDate: null },
  { NamespacePrefix: "sitewide", AllowedLicenses: -1, UsedLicenses: 40, Status: "Active", ExpirationDate: null },
  { NamespacePrefix: "freebie", AllowedLicenses: 100, UsedLicenses: 3, Status: "Free", ExpirationDate: null },
  { NamespacePrefix: "oldapp", AllowedLicenses: 10, UsedLicenses: 0, Status: "Expired", ExpirationDate: "2025-01-31T00:00:00.000Z" },
];

export const PERMISSION_SET_LICENSES = [{ MasterLabel: "Sales Console", TotalLicenses: 15, UsedLicenses: 3, Status: "Active", ExpirationDate: null }];

export const LAYOUTS = [
  { Id: sfId("00h", 1), Name: "Account Layout", EntityDefinitionId: "Account" },
  { Id: sfId("00h", 2), Name: "Opportunity Layout", EntityDefinitionId: "Opportunity" },
  { Id: sfId("00h", 3), Name: "Lead Layout", EntityDefinitionId: "Lead" },
  { Id: sfId("00h", 4), Name: "Old Opportunity Layout", EntityDefinitionId: "Opportunity" },
  { Id: sfId("00h", 5), Name: "Contact Layout", EntityDefinitionId: "Contact" },
  { Id: sfId("00h", 6), Name: "Partner Lead Layout", EntityDefinitionId: "Lead" },
  // Trap: a custom object's layouts name it by id, once in the 15-character form.
  { Id: sfId("00h", 7), Name: "Invoice Layout", EntityDefinitionId: sfId("01I", 1).slice(0, 15) },
  { Id: sfId("00h", 8), Name: "Credit Note Layout", EntityDefinitionId: sfId("01I", 1) },
  { Id: sfId("00h", 9), Name: "Project Layout", EntityDefinitionId: sfId("01I", 2) },
];

export const PROFILE_LAYOUTS = [1, 2, 3, 5, 7, 8, 9].map((n) => ({ LayoutId: sfId("00h", n) }));

// Tooling's CustomObject, which links those ids back to an API name.
export const CUSTOM_OBJECTS = [
  { Id: sfId("01I", 1), DeveloperName: "Invoice", NamespacePrefix: null },
  { Id: sfId("01I", 2), DeveloperName: "Project", NamespacePrefix: null },
  { Id: sfId("01I", 3), DeveloperName: "Project_Line", NamespacePrefix: null },
  { Id: sfId("01I", 4), DeveloperName: "Empty_Object", NamespacePrefix: null },
];

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

const loginRows = (user: number, count: number, status = "Success", api: { ApiType: string; Application: string } | null = null) =>
  Array.from({ length: count }, (_, i) => ({ UserId: userId(user), LoginTime: daysAgo(i * 2 + 1), Status: status, ApiType: api?.ApiType ?? null, Application: api?.Application ?? "Browser" }));

export const FORECAST_TYPES = [
  { Id: sfId("0Db", 1), DeveloperName: "OpportunityRevenue", IsActive: true, IsAmount: true },
  { Id: sfId("0Db", 2), DeveloperName: "OpportunityQuantity", IsActive: true, IsAmount: false },
];

const QUARTER_START = "2026-10-01";

// Trap: each rep's quota is real, and the manager's quota above them would double
// count if every row were added up. It's held by a user who owns no deals.
export const QUOTAS = [
  // Trap: rep 14 owns open deals and has no quota at all.
  ...[1, 2, 3].map((n) => ({ QuotaOwnerId: userId(n), QuotaAmount: 400_000, StartDate: QUARTER_START, ForecastingTypeId: sfId("0Db", 1) })),
  { QuotaOwnerId: userId(12), QuotaAmount: 1_600_000, StartDate: QUARTER_START, ForecastingTypeId: sfId("0Db", 1) },
  // Trap: a quantity quota isn't money.
  { QuotaOwnerId: userId(1), QuotaAmount: 50, StartDate: QUARTER_START, ForecastingTypeId: sfId("0Db", 2) },
];

const PERIOD_ID = sfId("026", 2);

export const PERIODS = [{ Id: PERIOD_ID, Type: "Quarter", StartDate: QUARTER_START, EndDate: "2026-12-31" }];

const forecastItem = (n: number, category: string, before: number, after: number) => ({
  OwnerId: userId(n),
  PeriodId: PERIOD_ID,
  ForecastingTypeId: sfId("0Db", 1),
  ForecastCategoryName: category,
  ForecastAmount: after,
  AmountWithoutAdjustments: before,
  HasAdjustment: before !== after,
});

export const FORECAST_ITEMS = [
  forecastItem(1, "Commit", 200_000, 200_000),
  forecastItem(2, "Commit", 200_000, 200_000),
  // Trap: most of this forecast is a manager's hand adjustment, with no deal behind it.
  forecastItem(3, "Commit", 100_000, 400_000),
  forecastItem(14, "Commit", 100_000, 100_000),
  forecastItem(1, "Best Case", 300_000, 300_000),
  { ...forecastItem(2, "Pipeline", 500_000, 500_000), ForecastingTypeId: sfId("0Db", 2) },
];

// Commit follows the late stages, with a planted handful of early-stage deals.
export const FORECAST_FACTS = pipeline.opportunities
  .filter((o) => !o.IsClosed && o.CloseDate >= QUARTER_START && o.CloseDate <= "2026-12-31")
  .map((o, i) => ({
    OpportunityId: o.Id,
    PeriodId: PERIOD_ID,
    ForecastCategoryName: o.StageName === "Negotiation" || i % 7 === 0 ? "Commit" : o.StageName === "Proposal" ? "Best Case" : "Pipeline",
  }));

/** Weekly browser logins for one user, every `step` days from `from` to `to` days ago. */
const loginsBetween = (user: number, from: number, to: number, step = 7) =>
  Array.from({ length: Math.floor((to - from) / step) + 1 }, (_, i) => ({ UserId: userId(user), LoginTime: daysAgo(from + i * step), Status: "Success", ApiType: null, Application: "Browser", LoginType: "Application", Platform: "Mac OSX" }));

// Months of history for adoption. Each user's logins stop before their LastLoginDate.
function ADOPTION_HISTORY() {
  return [
    // The steady core, logging in through every month of the window.
    ...[1, 2, 3, 4, 14].flatMap((user) => loginsBetween(user, 35, 245)),
    // Trap: falling adoption. Four people who used Salesforce in the spring have stopped.
    ...loginsBetween(5, 200, 260, 6),
    ...loginsBetween(7, 250, 300, 6),
    ...loginsBetween(8, 180, 230, 6),
    ...loginsBetween(13, 220, 270, 6),
    // Trap: mobile use, through the Salesforce app.
    ...[1, 2].flatMap((user) => [2, 6, 10].map((d) => ({ UserId: userId(user), LoginTime: daysAgo(d), Status: "Success", ApiType: null, Application: "Salesforce for iOS", LoginType: "Remote Access 2.0", Platform: "iPhone" }))),
  ];
}

export const LOGIN_HISTORY = [
  ...loginRows(1, 10),
  ...loginRows(2, 12),
  ...loginRows(3, 8),
  ...loginRows(4, 15),
  ...loginRows(11, 30, "Success", { ApiType: "SOAP Enterprise", Application: "Finance Sync" }),
  ...loginRows(14, 7),
  // Trap: disagrees with LastLoginDate.
  ...loginRows(15, 3),
  // Trap: failures aren't logins, and Status can't be filtered in SOQL.
  ...loginRows(5, 4, "Invalid Password"),
  // Trap: an integration quietly running on a salesperson's own login.
  ...loginRows(14, 6, "Success", { ApiType: "SOAP Enterprise", Application: "Partner Portal Sync" }),
  // Trap: one data load by an admin is normal, and shouldn't read as an integration.
  ...loginRows(4, 1, "Success", { ApiType: "SOAP Partner", Application: "Data Loader" }),
  ...ADOPTION_HISTORY(),
];

export const HEALTH_SCORE = "62";

export const HEALTH_RISKS = [
  { RiskType: "HIGH_RISK", Setting: "Minimum password length", SettingGroup: "Password Policies", OrgValue: "8 characters", StandardValue: "12 characters" },
  { RiskType: "HIGH_RISK", Setting: "Session timeout", SettingGroup: "Session Settings", OrgValue: "12 hours", StandardValue: "2 hours" },
  { RiskType: "MEDIUM_RISK", Setting: "Maximum invalid login attempts", SettingGroup: "Password Policies", OrgValue: "10", StandardValue: "5" },
  { RiskType: "MEETS_STANDARD", Setting: "Enable clickjack protection", SettingGroup: "Session Settings", OrgValue: "Enabled", StandardValue: "Enabled" },
  { RiskType: "MEETS_STANDARD", Setting: "Password complexity", SettingGroup: "Password Policies", OrgValue: "Alpha and numeric", StandardValue: "Alpha and numeric" },
];

const permissionSet = (n: number, Label: string, profile: string | null, modify: boolean, view: boolean, apiOnly = false) => ({
  Id: sfId("0PS", n),
  Label,
  IsOwnedByProfile: profile !== null,
  Profile: profile ? { Name: profile } : null,
  PermissionsModifyAllData: modify,
  PermissionsViewAllData: view,
  PermissionsApiUserOnly: apiOnly,
});

export const PERMISSION_SETS = [
  permissionSet(1, "00eSystemAdmin", "System Administrator", true, true),
  // Trap: the profile is called API Only, and doesn't have the API Only User permission.
  permissionSet(2, "00eApiOnly", "API Only", true, true),
  // Trap: the profile list looks clean. The grant sits in a permission set on a rep.
  permissionSet(3, "Data Fix Temporary", null, true, true),
  permissionSet(4, "Reporting Everything", null, false, true),
];

export const PERMISSION_ASSIGNMENTS = [
  { AssigneeId: userId(4), PermissionSetId: sfId("0PS", 1) },
  { AssigneeId: userId(11), PermissionSetId: sfId("0PS", 2) },
  { AssigneeId: userId(2), PermissionSetId: sfId("0PS", 3) },
  // Trap: inactive, so it can't log in and shouldn't count.
  { AssigneeId: userId(9), PermissionSetId: sfId("0PS", 3) },
  { AssigneeId: userId(1), PermissionSetId: sfId("0PS", 4) },
];

// "Your org, documented": the set-up only that part reads.

// Lookups and a master-detail between the documented objects, as Tooling's
// FieldDefinition gives them. Traps: a lookup to User, which isn't a documented
// object, and a field that isn't a relationship at all.
const relationship = (object: string, field: string, dataType: string, to: string[] | null) => ({ EntityDefinition: { QualifiedApiName: object }, QualifiedApiName: field, DataType: dataType, ReferenceTo: to ? { referenceTo: to } : null });
export const RELATIONSHIP_FIELDS = [
  relationship("Account", "ParentId", "Hierarchy", ["Account"]),
  relationship("Account", "OwnerId", "Lookup(User)", ["User"]),
  relationship("Account", "Name", "Name", null),
  relationship("Contact", "AccountId", "Lookup(Account)", ["Account"]),
  relationship("Opportunity", "AccountId", "Lookup(Account)", ["Account"]),
  relationship("Lead", "ConvertedAccountId", "Lookup(Account)", ["Account"]),
  relationship("Lead", "ConvertedContactId", "Lookup(Contact)", ["Contact"]),
  relationship("Lead", "ConvertedOpportunityId", "Lookup(Opportunity)", ["Opportunity"]),
  relationship("Invoice__c", "Account__c", "Lookup(Account)", ["Account"]),
  relationship("Invoice__c", "Opportunity__c", "Lookup(Opportunity)", ["Opportunity"]),
  relationship("Project__c", "Account__c", "Lookup(Account)", ["Account"]),
  relationship("Project_Line__c", "Project__c", "Master-Detail(Project)", ["Project__c"]),
];

export const SHARING_MODELS: Record<string, [string, string]> = {
  Account: ["Read", "None"],
  Contact: ["ControlledByParent", "ControlledByParent"],
  Opportunity: ["Edit", "None"],
  Lead: ["ReadEditTransfer", "None"],
  Invoice__c: ["None", "None"],
  Project__c: ["Read", "None"],
  Project_Line__c: ["ControlledByParent", "ControlledByParent"],
};

// Three levels: the users' two teams report to a director, who reports to the top.
export const ROLES = [
  { Id: sfId("00E", 1), Name: "Managing Director", ParentRoleId: null },
  { Id: sfId("00E", 2), Name: "Sales Director", ParentRoleId: sfId("00E", 1) },
  { Id: sfId("00E", 3), Name: "Sales UK", ParentRoleId: sfId("00E", 2) },
  { Id: sfId("00E", 4), Name: "Sales EU", ParentRoleId: sfId("00E", 2) },
];

// Every permission set nobody's profile owns. Trap: one from a package.
export const DOCUMENTED_PERMISSION_SETS = [
  { Id: sfId("0PS", 3), Name: "Data_Fix_Temporary", Label: "Data Fix Temporary", IsCustom: true, NamespacePrefix: null },
  { Id: sfId("0PS", 4), Name: "Reporting_Everything", Label: "Reporting Everything", IsCustom: true, NamespacePrefix: null },
  { Id: sfId("0PS", 5), Name: "Sales_Console_User", Label: "Sales Console User", IsCustom: true, NamespacePrefix: null },
  { Id: sfId("0PS", 6), Name: "CPQ_User", Label: "CPQ User", IsCustom: false, NamespacePrefix: "acmecpq" },
];
// Who holds them, active users only, as the query asks. User 9 is inactive.
export const DOCUMENTED_ASSIGNMENTS = ([[3, 2], [3, 9], [4, 1], [5, 1], [5, 2], [5, 3], [5, 5], [5, 7], [6, 1], [6, 2]] as const)
  .filter(([, user]) => USERS.find((u) => u.n === user)?.active !== false)
  .map(([set]) => ({ PermissionSetId: sfId("0PS", set) }));

export const CONNECTED_APPS = [
  { Name: "Data Loader", OptionsAllowAdminApprovedUsersOnly: false },
  { Name: "Finance Sync", OptionsAllowAdminApprovedUsersOnly: true },
];
export const NAMED_CREDENTIALS = [{ DeveloperName: "Finance_API", MasterLabel: "Finance API", NamespacePrefix: null }];
// Traps: a key in the query string, which must never be kept; an https site
// that lets code mix in HTTP; and a plain http site, inactive, so it's no risk.
export const REMOTE_SITES = [
  { EndpointUrl: "https://api.finance.example.com/v2?key=not-a-real-key", IsActive: true, ProtocolMismatch: true, NamespacePrefix: null },
  { EndpointUrl: "http://legacy.example.com", IsActive: false, ProtocolMismatch: false, NamespacePrefix: null },
];

// Technical health: what Lightning Experience left behind. Traps: a JavaScript
// button, and a Visualforce page not marked for Lightning. A package's own
// JavaScript button is left out by the query, and a URL link is fine.
export const WEB_LINKS = [
  { Name: "Send_To_ERP", PageOrSobjectType: "Opportunity", LinkType: "javascript", NamespacePrefix: null },
  { Name: "Company_Website", PageOrSobjectType: "Account", LinkType: "url", NamespacePrefix: null },
  { Name: "Quote_Wizard", PageOrSobjectType: "Opportunity", LinkType: "javascript", NamespacePrefix: "acmecpq" },
];
export const SCONTROLS: Array<{ Name: string; NamespacePrefix: string | null }> = [];
export const APEX_PAGES = [
  { Name: "InvoicePrint", ApiVersion: 45, IsAvailableInTouch: false, NamespacePrefix: null },
  { Name: "AccountMap", ApiVersion: 62, IsAvailableInTouch: true, NamespacePrefix: null },
];

// Integrations: the async Apex that ran this week. Traps: LegacySync failing
// three times, and a batch that finished with errors. A batch's chunks and a
// failure from before the week are left out.
const asyncJob = (n: number, JobType: string, Status: string, className: string, days: number, NumberOfErrors = 0) => ({ Id: sfId("707", n), JobType, Status, NumberOfErrors, ApexClass: { Name: className }, CreatedDate: daysAgo(days) });
export const ASYNC_JOBS = [
  asyncJob(1, "Queueable", "Failed", "LegacySync", 1),
  asyncJob(2, "Queueable", "Failed", "LegacySync", 2),
  asyncJob(3, "Queueable", "Failed", "LegacySync", 4),
  asyncJob(4, "Queueable", "Completed", "LegacySync", 3),
  asyncJob(5, "BatchApex", "Completed", "FinanceSync", 2, 4),
  asyncJob(6, "BatchApexWorker", "Failed", "FinanceSync", 2, 4),
  asyncJob(7, "BatchApex", "Completed", "FinanceSync", 5),
  asyncJob(8, "ScheduledApex", "Completed", "FinanceSync", 1),
  asyncJob(9, "Queueable", "Failed", "LeadRouter", 20),
];

// Trap: async Apex nearly used up for the day, while API requests are fine.
export const ASYNC_APEX_LIMIT = { Max: 250_000, Remaining: 30_000 };

export const SHARING_DEFAULTS = { DefaultAccountAccess: "Read", DefaultContactAccess: "ControlledByParent", DefaultOpportunityAccess: "Edit", DefaultLeadAccess: "ReadEditTransfer" };

export const CRON_TRIGGERS = [
  { Id: sfId("08e", 1), OwnerId: userId(13), State: "WAITING", CronJobDetail: { Name: "Nightly Renewal Sync", JobType: "7" } },
  { Id: sfId("08e", 2), OwnerId: userId(4), State: "WAITING", CronJobDetail: { Name: "Weekly Cleanup", JobType: "7" } },
  // Trap: still scheduled under someone who's left.
  { Id: sfId("08e", 3), OwnerId: userId(9), State: "WAITING", CronJobDetail: { Name: "Quarterly Export", JobType: "7" } },
];

export const DASHBOARDS = [
  { Id: sfId("01Z", 1), Title: "Sales Overview", Type: "SpecifiedUser", RunningUserId: userId(4), FolderName: "Sales" },
  { Id: sfId("01Z", 2), Title: "EU Pipeline", Type: "SpecifiedUser", RunningUserId: userId(9), FolderName: "Sales" },
  { Id: sfId("01Z", 3), Title: "My Deals", Type: "LoggedInUser", RunningUserId: null, FolderName: "Sales" },
];

// Lead and Case assignment rules. None in the retrieve, so none here.
export const ASSIGNMENT_RULES: Array<{ Id: string }> = [];

export const EMAIL_SERVICES = [{ LocalPart: "cases", RunAsUserId: userId(4) }];
export const AUTH_PROVIDERS = [{ DeveloperName: "Google_SSO", ExecutionUserId: userId(4) }];

const reportFolders = ["Sales", "Marketing", "Service"];
export const REPORT_FOLDERS = reportFolders.map((name, i) => ({ Id: sfId("00l", i + 1), DeveloperName: name, ParentId: null }));

export const REPORTS = Array.from({ length: 80 }, (_, i) => ({
  Id: sfId("00O", i + 1),
  DeveloperName: `Report_${i + 1}`,
  OwnerId: sfId("00l", (i % reportFolders.length) + 1),
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

// The records behind those counts, for the action lists. Synthetic names only.
/** A synthetic deal's name, from its id. */
export const dealName = (id: string): string => `Synthetic deal ${Number(id.slice(3, 15))}`;
export const STALE_LEAD_ROWS = LEADS.staleBySource.flatMap(({ s, c }, k) =>
  Array.from({ length: c }, (_, i) => {
    const n = k * 1000 + i + 1;
    return { Id: sfId("00Q", n), Name: `Synthetic lead ${n}`, OwnerId: userId(i % 2 ? 1 : 2), LeadSource: s, CreatedDate: daysAgo(120 + i) };
  }),
);
// Traps: ten contacts have neither, so they're on both lists.
const contactRow = (n: number) => ({ Id: sfId("003", n), Name: `Synthetic contact ${n}`, OwnerId: userId(n % 3 === 0 ? 3 : 14) });
export const CONTACTS_NO_ACCOUNT = Array.from({ length: CONTACTS.noAccount }, (_, i) => contactRow(i + 1));
export const CONTACTS_NO_EMAIL = Array.from({ length: CONTACTS.noEmail }, (_, i) => contactRow(i + 11));

export const APEX_CLASSES = [
  // Trap: a SOQL for loop queries once, so it isn't a query in a loop.
  { Id: sfId("01p", 1), Name: "OpportunityHandler", ApiVersion: 60, Status: "Active", LengthWithoutComments: 1200, Body: "public class OpportunityHandler { /* uses Competitor__c */ public void close() { List<Opportunity> opps = new List<Opportunity>(); for (Opportunity o : [SELECT Id FROM Opportunity WHERE IsClosed = false]) { opps.add(o); } update opps; } }" },
  // Trap: a query and a save inside a loop.
  { Id: sfId("01p", 2), Name: "LeadRouter", ApiVersion: 62, Status: "Active", LengthWithoutComments: 800, Body: "public class LeadRouter { public void route(List<Lead> leads) { for (Lead l : leads) { Account a = [SELECT Id FROM Account WHERE Name = :l.Company LIMIT 1]; l.Description = a.Id; update l; } } }" },
  // Trap: a callout address written into the code.
  { Id: sfId("01p", 3), Name: "LegacySync", ApiVersion: 38, Status: "Active", LengthWithoutComments: 400, Body: "public class LegacySync { public void send() { HttpRequest req = new HttpRequest(); req.setEndpoint('https://legacy.example.com/api'); req.setMethod('POST'); new Http().send(req); } }" },
  // Trap: a test that runs the code and checks nothing.
  { Id: sfId("01p", 4), Name: "OpportunityHandlerTest", ApiVersion: 60, Status: "Active", LengthWithoutComments: 300, Body: "@isTest class OpportunityHandlerTest { @isTest static void closes() { new OpportunityHandler().close(); } }" },
  // Clean: a callout through a named credential. The old address is in a comment, so it doesn't count.
  { Id: sfId("01p", 5), Name: "FinanceSync", ApiVersion: 62, Status: "Active", LengthWithoutComments: 350, Body: "// Sends invoices to the finance system. Was req.setEndpoint('https://old.example.com').\npublic class FinanceSync { public void send() { HttpRequest req = new HttpRequest(); req.setEndpoint('callout:Finance_API/invoices'); new Http().send(req); } }" },
  // Trap: reads the org's live data, and does check its result.
  { Id: sfId("01p", 6), Name: "LeadRouterTest", ApiVersion: 62, Status: "Active", LengthWithoutComments: 250, Body: "@isTest(SeeAllData=true) private class LeadRouterTest { @isTest static void routes() { new LeadRouter().route(new List<Lead>()); System.assertEquals(0, Limits.getDmlStatements()); } }" },
  // Trap: a test data factory has no test methods, so it has nothing to assert.
  { Id: sfId("01p", 7), Name: "TestDataFactory", ApiVersion: 62, Status: "Active", LengthWithoutComments: 150, Body: "@isTest public class TestDataFactory { public static Lead lead() { return new Lead(LastName = 'Test', Company = 'Test'); } }" },
];

const TRIGGER_EVENTS = ["BeforeInsert", "BeforeUpdate", "BeforeDelete", "AfterInsert", "AfterUpdate", "AfterDelete", "AfterUndelete"] as const;
const usage = (...on: Array<(typeof TRIGGER_EVENTS)[number]>): Record<string, boolean> => Object.fromEntries(TRIGGER_EVENTS.map((e) => [`Usage${e}`, on.includes(e)]));

export const APEX_TRIGGERS = [
  { Id: sfId("01q", 1), Name: "OpportunityTrigger", TableEnumOrId: "Opportunity", ApiVersion: 60, LengthWithoutComments: 180, Status: "Active", ...usage("BeforeInsert"), Body: "trigger OpportunityTrigger on Opportunity (before insert) { }" },
  { Id: sfId("01q", 2), Name: "OpportunitySplitTrigger", TableEnumOrId: "Opportunity", ApiVersion: 60, LengthWithoutComments: 220, Status: "Active", ...usage("AfterUpdate"), Body: "trigger OpportunitySplitTrigger on Opportunity (after update) { }" },
  { Id: sfId("01q", 3), Name: "AccountTrigger", TableEnumOrId: "Account", ApiVersion: 60, LengthWithoutComments: 150, Status: "Active", ...usage("BeforeUpdate"), Body: "trigger AccountTrigger on Account (before update) { }" },
  // Trap: a trigger on a custom object names it by id, not by name.
  { Id: sfId("01q", 4), Name: "InvoiceTrigger", TableEnumOrId: sfId("01I", 1), ApiVersion: 62, LengthWithoutComments: 260, Status: "Active", ...usage("BeforeInsert", "BeforeUpdate"), Body: "// Totals each invoice from its lines.\ntrigger InvoiceTrigger on Invoice__c (before insert, before update) { }" },
  // Trap: a second trigger on the same custom object, by its 15-character id. With
  // the flow and Workflow Rule on Invoice__c, that's three tools and two triggers
  // on one object, which raw ids would split across three objects.
  { Id: sfId("01q", 5), Name: "InvoiceAudit", TableEnumOrId: sfId("01I", 1).slice(0, 15), ApiVersion: 62, LengthWithoutComments: 140, Status: "Active", ...usage("AfterUpdate"), Body: "// Records who changed an invoice's total.\ntrigger InvoiceAudit on Invoice__c (after update) { }" },
];

export const ORG_COVERAGE = [{ PercentCovered: 82 }];

export const CLASS_COVERAGE = [
  { ApexClassOrTrigger: { Name: "OpportunityHandler" }, NumLinesCovered: 180, NumLinesUncovered: 20 },
  { ApexClassOrTrigger: { Name: "LeadRouter" }, NumLinesCovered: 90, NumLinesUncovered: 10 },
  // Trap: a healthy org-wide figure hiding a weak class.
  { ApexClassOrTrigger: { Name: "LegacySync" }, NumLinesCovered: 15, NumLinesUncovered: 45 },
];

// Trap: other people's private reports can't be read, so a clean scan isn't proof.
export const PRIVATE_REPORTS = 4;



export const REPORT_FILES: Record<string, string> = Object.fromEntries(
  REPORTS.map((report, i) => [
    `reports/${report.FolderName}/${report.DeveloperName}.report`,
    // Trap: a field nothing else uses is a column in one report. The filter value
    // stands in for customer data, which must not survive into a checkpoint.
    `<?xml version="1.0"?><Report xmlns="http://soap.sforce.com/2006/04/metadata"><columns><field>ACCOUNT.NAME</field></columns>${i === 7 ? "<columns><field>Opportunity.Old_Region__c</field></columns>" : ""}<filter><criteriaItems><column>ACCOUNT.NAME</column><value>Confidential Buyer Ltd</value></criteriaItems></filter><name>Report ${i + 1}</name></Report>`,
  ]),
);

const METADATA_NS = 'xmlns="http://soap.sforce.com/2006/04/metadata"';

// Trap: most automation has no description or comment, and most custom fields
// have no help text. One flow, one validation rule and one field are documented.
export const METADATA_FILES: Record<string, string> = {
  "package.xml": `<?xml version="1.0"?><Package ${METADATA_NS}><version>${API_VERSION}</version></Package>`,
  "flows/Opportunity_Route_Deal.flow": `<?xml version="1.0"?><Flow ${METADATA_NS}>
  <apiVersion>62.0</apiVersion><label>Route Deal</label><processType>AutoLaunchedFlow</processType><status>Active</status>
  <start><object>Opportunity</object><triggerType>RecordAfterSave</triggerType></start>
  <constants><name>Approver</name><value><stringValue>${userId(7)}</stringValue></value></constants>
  <recordUpdates><name>Update_Owner</name><object>Opportunity</object><inputAssignments><field>Legacy_Code__c</field></inputAssignments></recordUpdates>
</Flow>`,
  "flows/Lead_Assign.flow": `<?xml version="1.0"?><Flow ${METADATA_NS}>
  <apiVersion>62.0</apiVersion><label>Assign Lead</label><description>Sets the region on new leads so routing can pick an owner.</description><processType>AutoLaunchedFlow</processType><status>Active</status>
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
  <fields><fullName>Legacy_Code__c</fullName><type>Text</type></fields>
  <fields><fullName>Deal_Score__c</fullName><type>Number</type></fields>
  <fields><fullName>Competitor__c</fullName><inlineHelpText>Who else the customer is talking to.</inlineHelpText><type>Text</type></fields>
  <fields><fullName>Amount</fullName><type>Currency</type></fields>
  <validationRules><fullName>Needs_Competitor</fullName><description>A competitor is needed to report on losses.</description><active>true</active><errorConditionFormula>ISBLANK(Competitor__c)</errorConditionFormula></validationRules>
</CustomObject>`,
  // Trap for the inventory: a busy custom object, with automation on it and an
  // inactive record type and rule that mustn't be counted.
  "objects/Invoice__c.object": `<?xml version="1.0"?><CustomObject ${METADATA_NS}>
  <fields><fullName>Invoice_Total__c</fullName><description>Sum of the invoice lines, set by InvoiceTrigger.</description><type>Currency</type></fields>
  <fields><fullName>Due_Date__c</fullName><type>Date</type></fields>
  <fields><fullName>Paid__c</fullName><type>Checkbox</type></fields>
  <recordTypes><fullName>Standard</fullName><active>true</active><label>Standard</label></recordTypes>
  <recordTypes><fullName>Credit_Note</fullName><active>true</active><label>Credit Note</label></recordTypes>
  <recordTypes><fullName>Old_Format</fullName><active>false</active><label>Old Format</label></recordTypes>
  <validationRules><fullName>Due_Date_Required</fullName><description>Finance can't chase an invoice with no due date.</description><active>true</active><errorConditionFormula>ISBLANK(Due_Date__c)</errorConditionFormula></validationRules>
  <validationRules><fullName>Old_Check</fullName><active>false</active><errorConditionFormula>Paid__c</errorConditionFormula></validationRules>
</CustomObject>`,
  "workflows/Invoice__c.workflow": `<?xml version="1.0"?><Workflow ${METADATA_NS}>
  <rules><fullName>Notify_Overdue</fullName><active>true</active><description>Tells finance when an invoice passes its due date.</description><actions><name>Alert</name><type>Alert</type></actions></rules>
</Workflow>`,
  "objects/Project__c.object": `<?xml version="1.0"?><CustomObject ${METADATA_NS}>
  <fields><fullName>Project_Code__c</fullName><type>Text</type></fields>
</CustomObject>`,
  "objects/Project_Line__c.object": `<?xml version="1.0"?><CustomObject ${METADATA_NS}></CustomObject>`,
  // Trap: an object with no records stays out of the inventory, however much it holds.
  "objects/Empty_Object__c.object": `<?xml version="1.0"?><CustomObject ${METADATA_NS}>
  <recordTypes><fullName>Default</fullName><active>true</active><label>Default</label></recordTypes>
</CustomObject>`,
  "flows/Invoice_Set_Status.flow": `<?xml version="1.0"?><Flow ${METADATA_NS}>
  <apiVersion>62.0</apiVersion><label>Set Invoice Status</label><processType>AutoLaunchedFlow</processType><status>Active</status>
  <start><object>Invoice__c</object><triggerType>RecordBeforeSave</triggerType></start>
  <assignments><name>Mark_Paid</name></assignments>
</Flow>`,
  "approvalProcesses/Opportunity.Discount_Approval.approvalProcess": `<?xml version="1.0"?><ApprovalProcess ${METADATA_NS}><active>true</active></ApprovalProcess>`,
};

// ---------------------------------------------------------------------------
// Service Cloud (roadmap Stage 6). A support team of four, a year of cases and
// the set-up around them. Each trap is named where it's planted.

const queueId = (n: number): string => sfId("00G", 100 + n);
const routingId = (n: number): string => sfId("0K3", n);

export const CASE_QUEUES = [
  { Id: queueId(1), Name: "Support Tier 1", DeveloperName: "Support_Tier_1", QueueRoutingConfigId: routingId(1) },
  // Trap: Omni-Channel routes Tier 1 but not Tier 2, so Tier 2's cases wait to be picked by hand.
  { Id: queueId(2), Name: "Support Tier 2", DeveloperName: "Support_Tier_2", QueueRoutingConfigId: null },
  // Trap: a case queue with no members, and the target of an escalation rule.
  { Id: queueId(3), Name: "Old Returns", DeveloperName: "Old_Returns", QueueRoutingConfigId: null },
];
// A lead queue isn't a case queue, and must be left out of every case check.
export const OTHER_QUEUES = [{ Id: queueId(9), Name: "Inbound Leads", DeveloperName: "Inbound_Leads", QueueRoutingConfigId: null }];
export const QUEUE_SOBJECTS = [...CASE_QUEUES.map((q) => ({ QueueId: q.Id, SobjectType: "Case" })), { QueueId: queueId(9), SobjectType: "Lead" }];
export const QUEUE_MEMBERS = [
  { GroupId: queueId(1), UserOrGroupId: userId(1) },
  { GroupId: queueId(1), UserOrGroupId: userId(2) },
  { GroupId: queueId(2), UserOrGroupId: userId(2) },
  { GroupId: queueId(2), UserOrGroupId: userId(4) },
  { GroupId: queueId(9), UserOrGroupId: userId(3) },
];

export const CASE_STATUSES = [
  { ApiName: "New", MasterLabel: "New", IsClosed: false },
  { ApiName: "Working", MasterLabel: "Working", IsClosed: false },
  { ApiName: "Escalated", MasterLabel: "Escalated", IsClosed: false },
  { ApiName: "Closed", MasterLabel: "Closed", IsClosed: true },
];
export const BUSINESS_HOURS = [{ Id: sfId("01m", 1), Name: "Default", IsActive: true, IsDefault: true }];
export const SLA_PROCESSES = [{ Id: sfId("552", 1), Name: "Standard Support", IsActive: true, SObjectType: "Case" }];
export const MILESTONE_TYPES = [
  { Id: sfId("557", 1), Name: "First Response" },
  { Id: sfId("557", 2), Name: "Resolution" },
];
// Trap: an active case assignment rule, while Omni-Channel routes the same queues.
export const CASE_ASSIGNMENT_RULES = [{ Id: sfId("01Q", 1), Name: "Standard Case Assignment", Active: true }];
export const SERVICE_CHANNELS = [{ Id: sfId("0N9", 1), DeveloperName: "Cases", RelatedEntity: "Case" }];
export const ROUTING_CONFIGS = [
  { Id: routingId(1), DeveloperName: "Tier_1", RoutingModel: "MostAvailable", PushTimeout: 30 },
  // Trap: a routing configuration no queue uses.
  { Id: routingId(2), DeveloperName: "Old_Pilot", RoutingModel: "LeastActive", PushTimeout: 0 },
];

// Service Cloud users: the four agents, plus the traps among the Sales users.
// Trap: Sam Reed (5) hasn't logged in for 200 days. Trap: Nina Hart (10) never has.
export const SUPPORT_USER_IDS = [1, 2, 4, 5, 10];
export const supportUsers = SUPPORT_USER_IDS.map((n) => {
  const u = usersTable.find((x) => x.Id === userId(n))!;
  return { Id: u.Id, LastLoginDate: u.LastLoginDate, Profile: { UserLicense: { Name: u.Profile.UserLicense.Name } } };
});

// Trap: most macros are unused, and one still works only in Classic.
export const MACROS = Array.from({ length: 8 }, (_, i) => ({ Id: sfId("0JZ", i + 1), Name: `Macro ${String(i + 1).padStart(2, "0")}`, IsLightningSupported: i !== 7 }));
export const MACRO_USAGE = [
  { MacroID: sfId("0JZ", 1), n: 140 },
  { MacroID: sfId("0JZ", 2), n: 35 },
];
export const QUICK_TEXT = Array.from({ length: 4 }, (_, i) => ({ Id: sfId("574", i + 1), Name: `Greeting ${i + 1}` }));
export const QUICK_TEXT_USAGE = [1, 2, 3].map((i) => ({ QuickTextID: sfId("574", i), n: 20 * i }));

// Trap: most articles haven't been republished in a year, and many were never read.
export const ARTICLES = Array.from({ length: 30 }, (_, i) => ({ Id: sfId("ka0", i + 1), KnowledgeArticleId: sfId("kA0", i + 1), LastPublishedDate: daysAgo(i < 20 ? 400 + i * 10 : 30 + i) }));
export const ARTICLE_VIEWS = ARTICLES.slice(0, 10).map((a, i) => ({ ParentId: a.KnowledgeArticleId, ViewCount: 50 - i * 3, Channel: "AllChannels" }));

// Trap: escalations to a queue with no members, and to a user who's left (Kim Fox, 9).
export const ESCALATION_FILES: Record<string, string> = {
  "package.xml": `<?xml version="1.0"?><Package ${'xmlns="http://soap.sforce.com/2006/04/metadata"'}><version>${API_VERSION}</version></Package>`,
  "escalationRules/Case.escalationRules": `<?xml version="1.0"?><EscalationRules xmlns="http://soap.sforce.com/2006/04/metadata">
  <escalationRule><fullName>Standard</fullName><active>true</active>
    <ruleEntry><escalationAction><assignedTo>Old_Returns</assignedTo><assignedToType>Queue</assignedToType><minutesToEscalation>1440</minutesToEscalation><notifyCaseOwner>false</notifyCaseOwner></escalationAction>
      <escalationAction><assignedTo>kim.fox@example.com</assignedTo><assignedToType>User</assignedToType><minutesToEscalation>2880</minutesToEscalation><notifyCaseOwner>true</notifyCaseOwner></escalationAction>
      <escalationStartTime>CaseCreation</escalationStartTime></ruleEntry>
  </escalationRule>
  <escalationRule><fullName>Old_Rule</fullName><active>false</active></escalationRule>
</EscalationRules>`,
};

export type SyntheticCase = {
  Id: string;
  Status: string;
  IsClosed: boolean;
  Priority: string;
  Origin: string | null;
  OwnerId: string;
  CreatedDate: string;
  ClosedDate: string | null;
  Type: string | null;
  Reason: string | null;
  ContactId: string | null;
  AccountId: string | null;
};

const SERVICE_SEED = 20261105;
const CASE_AGENTS = [1, 2, 4];

function buildCases() {
  const random = seeded(SERVICE_SEED);
  const pick = <T>(items: readonly T[]): T => items[Math.floor(random() * items.length)]!;
  const cases: SyntheticCase[] = [];
  const history: Array<{ Id: string; CaseId: string; Field: string; OldValue: string | null; NewValue: string | null; CreatedDate: string }> = [];
  const milestones: Array<{ CaseId: string; MilestoneTypeId: string; IsCompleted: boolean; IsViolated: boolean; StartDate: string; TargetDate: string; CompletionDate: string | null }> = [];
  const replies: Array<{ Id: string; ParentId: string; MessageDate: string; Incoming: boolean }> = [];
  const work: Array<{ Id: string; UserId: string; Status: string; RequestDateTime: string; AcceptDateTime: string | null; DeclineDateTime: string | null; SpeedToAnswer: number | null }> = [];
  const articles: Array<{ Id: string; CaseId: string }> = [];
  let n = 0;
  // A year of cases, more arriving in recent months. Trap: from the sixth month
  // back, more cases come in each month than are closed.
  for (let daysBack = 364; daysBack >= 0; daysBack--) {
    const perDay = daysBack < 180 ? 3 : 2;
    for (let k = 0; k < perDay; k++) {
      n++;
      const created = asOf - daysBack * DAY - Math.floor(random() * 8) * 3_600_000;
      const priority = random() < 0.2 ? "High" : random() < 0.6 ? "Medium" : "Low";
      // Trap: about one in seven has no origin, above the agreed one in ten.
      const origin = random() < 0.14 ? null : pick(["Email", "Email", "Web", "Phone"] as const);
      // Trap: high-priority cases take longer than the rest.
      const hours = (priority === "High" ? 60 : 30) + random() * (priority === "High" ? 140 : 90);
      const closedAt = created + hours * 3_600_000;
      // Recent cases stay open more often, so the backlog grows; a few old ones are left open too.
      const stillOpen = daysBack < 180 ? random() < 0.35 * (1 - daysBack / 180) + (daysBack > 30 ? 0.05 : 0) : random() < 0.02;
      const open = stillOpen || closedAt > asOf;
      const id = sfId("500", n);
      // Trap: some open cases sit in a queue long after they came in.
      const ownerId = open && random() < 0.3 ? queueId(random() < 0.5 ? 1 : 2) : userId(pick(CASE_AGENTS));
      cases.push({
        Id: id,
        Status: open ? pick(["New", "Working", "Escalated"] as const) : "Closed",
        IsClosed: !open,
        Priority: priority,
        Origin: origin,
        OwnerId: ownerId,
        CreatedDate: new Date(created).toISOString().replace("Z", "+0000"),
        ClosedDate: open ? null : new Date(closedAt).toISOString().replace("Z", "+0000"),
        Type: random() < 0.08 ? null : pick(["Problem", "Question", "Feature Request"] as const),
        Reason: random() < 0.5 ? null : pick(["Installation", "Performance", "Breakdown"] as const),
        ContactId: random() < 0.04 ? null : sfId("003", 1 + (n % 400)),
        AccountId: random() < 0.5 ? null : sfId("001", 1 + (n % 200)),
      });
      const iso = (ms: number): string => new Date(ms).toISOString().replace("Z", "+0000");
      if (!open) {
        history.push({ Id: sfId("017", history.length + 1), CaseId: id, Field: "Status", OldValue: "Working", NewValue: "Closed", CreatedDate: iso(closedAt) });
        // Trap: about one closed case in six was reopened, above the one in ten allowed.
        if (random() < 0.16) {
          const span = closedAt - created;
          history.push({ Id: sfId("017", history.length + 1), CaseId: id, Field: "Status", OldValue: "Working", NewValue: "Closed", CreatedDate: iso(created + span * 0.3) });
          history.push({ Id: sfId("017", history.length + 1), CaseId: id, Field: "Status", OldValue: "Closed", NewValue: "Working", CreatedDate: iso(created + span * 0.5) });
        }
        // Trap: few closed cases have an article attached.
        if (random() < 0.03) articles.push({ Id: sfId("0Ar", articles.length + 1), CaseId: id });
      }
      // Entitlements: a first response and a resolution milestone on each case. Trap: one in five resolutions is missed.
      milestones.push({ CaseId: id, MilestoneTypeId: sfId("557", 1), IsCompleted: true, IsViolated: random() < 0.04, StartDate: iso(created), TargetDate: iso(created + 8 * 3_600_000), CompletionDate: iso(created + 4 * 3_600_000) });
      if (!open) milestones.push({ CaseId: id, MilestoneTypeId: sfId("557", 2), IsCompleted: true, IsViolated: random() < 0.2, StartDate: iso(created), TargetDate: iso(created + 5 * DAY), CompletionDate: iso(closedAt) });
      // A first reply on email cases, mostly within a working day.
      if (origin === "Email") {
        replies.push({ Id: sfId("02s", replies.length + 1), ParentId: id, MessageDate: iso(created), Incoming: true });
        if (random() < 0.92) replies.push({ Id: sfId("02s", replies.length + 1), ParentId: id, MessageDate: iso(created + (2 + random() * 26) * 3_600_000), Incoming: false });
      }
      if (ownerId.startsWith("005") || !open) {
        const declined = random() < 0.08;
        work.push({ Id: sfId("0Bz", work.length + 1), UserId: userId(pick(CASE_AGENTS)), Status: declined ? "Declined" : "Closed", RequestDateTime: iso(created), AcceptDateTime: declined ? null : iso(created + 60_000), DeclineDateTime: declined ? iso(created + 30_000) : null, SpeedToAnswer: declined ? null : 30 + Math.floor(random() * 90) });
      }
    }
  }
  // Older cases still open, from before the year.
  for (let i = 0; i < 12; i++) {
    n++;
    cases.push({ Id: sfId("500", n), Status: "Working", IsClosed: false, Priority: "Low", Origin: "Web", OwnerId: userId(CASE_AGENTS[i % 3]!), CreatedDate: daysAgo(400 + i * 20), ClosedDate: null, Type: "Question", Reason: null, ContactId: sfId("003", 1 + i), AccountId: null });
  }
  return { cases, history, milestones, replies, work, articles };
}

export const supportCases = buildCases();
