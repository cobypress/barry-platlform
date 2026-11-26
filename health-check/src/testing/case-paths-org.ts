import { serviceActivityExtractor, serviceCasesExtractor, serviceSetupExtractor } from "../extract/service.ts";
import { AS_OF, sfId, userId, type SyntheticCase } from "./synthetic-org.ts";
import { SYNTHETIC_RECORDS, type OrgRecords } from "./simulator.ts";

// Constructed case sets for the case status paths' tests (DECISIONS.md 776):
// cases, their Status changes and the org's case statuses, put in place of the
// synthetic org's own, so a test sets every case's day, path and close. No real
// data: ids are zero-padded from 900,000, so none meets the synthetic org's.

const DAY = 86_400_000;
const NOW = Date.parse(AS_OF);
const FIRST = 900_000;

export type CaseStatus = { ApiName: string; MasterLabel: string | null; IsClosed: boolean };

/** The constructed org's statuses: three open and two closed. */
export const CASE_PATH_STATUSES: CaseStatus[] = [
  { ApiName: "New", MasterLabel: "New", IsClosed: false },
  { ApiName: "Working", MasterLabel: "Working", IsClosed: false },
  { ApiName: "Waiting", MasterLabel: "Waiting on Customer", IsClosed: false },
  { ApiName: "Closed", MasterLabel: "Closed", IsClosed: true },
  { ApiName: "Duplicate", MasterLabel: "Closed as Duplicate", IsClosed: true },
];

/** One case: how many days before the run it was made, and each change of Status after that. */
export type CaseSpec = {
  /** Days before the run the case was made, at noon UTC unless `at` says another time of day. */
  made: number;
  /** "HH:MM" UTC on that day. */
  at?: string;
  /** The status it was made in. */
  start?: string;
  /** Each change: the new status and the days after the case was made it happened, in order. */
  steps: Array<[string, number]>;
  priority?: string | null;
  origin?: string | null;
  /** The case's Status as read now, when a test wants it apart from where its changes end. */
  statusNow?: string;
  /** Leave its changes out of the history read. */
  noHistory?: boolean;
};

export type CaseRecords = { cases: SyntheticCase[]; history: Array<{ Id: string; CaseId: string; Field: string; OldValue: string | null; NewValue: string | null; CreatedDate: string }>; statuses: CaseStatus[] };

const stamp = (ms: number): string => new Date(ms).toISOString().replace("Z", "+0000");
const dayOf = (ms: number): string => new Date(ms).toISOString().slice(0, 10);

/** The cases and their history. A case is closed when its last status is a closed one, on the day of that change. */
export function caseRecords(specs: CaseSpec[], statuses: CaseStatus[] = CASE_PATH_STATUSES): CaseRecords {
  const closedNames = new Set(statuses.filter((s) => s.IsClosed).flatMap((s) => [s.ApiName, s.MasterLabel ?? ""]));
  const cases: SyntheticCase[] = [];
  const history: CaseRecords["history"] = [];
  specs.forEach((s, i) => {
    const n = FIRST + i;
    const id = sfId("500", n);
    const madeMs = Date.parse(`${dayOf(NOW - s.made * DAY)}T${s.at ?? "12:00"}:00.000Z`);
    let status = s.start ?? "New";
    let lastAt = madeMs;
    s.steps.forEach(([to, after], k) => {
      lastAt = madeMs + after * DAY;
      if (!s.noHistory) history.push({ Id: sfId("017", n * 20 + k), CaseId: id, Field: "Status", OldValue: status, NewValue: to, CreatedDate: stamp(lastAt) });
      status = to;
    });
    const closed = closedNames.has(status);
    cases.push({
      Id: id,
      Status: s.statusNow ?? status,
      IsClosed: closed,
      Priority: s.priority === undefined ? "Medium" : (s.priority as string),
      Origin: s.origin === undefined ? "Email" : s.origin,
      OwnerId: userId(1 + (i % 3)),
      CreatedDate: stamp(madeMs),
      ClosedDate: closed ? stamp(lastAt) : null,
      Type: "Question",
      Reason: null,
      ContactId: sfId("003", 1 + (i % 50)),
      AccountId: null,
    });
  });
  return { cases, history, statuses };
}

/** The synthetic org's records with these cases in place of its own: for the simulator. */
export function caseOrg(r: CaseRecords): OrgRecords {
  return { ...SYNTHETIC_RECORDS, supportCases: { ...SYNTHETIC_RECORDS.supportCases, cases: r.cases, history: r.history }, caseStatuses: r.statuses };
}

type Read = { status: string; data: unknown[]; truncated?: boolean };

/** A run's checkpoints with these cases in place of its own: for analyseExtracts. */
export function withCases(extracts: Map<string, unknown>, r: CaseRecords): Map<string, unknown> {
  const changed = structuredClone(extracts);
  const setup = changed.get(serviceSetupExtractor.id) as { statuses: Read };
  setup.statuses = { ...setup.statuses, status: "ok", data: r.statuses };
  const cases = changed.get(serviceCasesExtractor.id) as { open: Read; closedCases: Read };
  const yearStart = new Date(NOW);
  yearStart.setUTCMonth(yearStart.getUTCMonth() - 12);
  cases.open.data = r.cases.filter((c) => !c.IsClosed).map(({ Id, Status, Priority, Origin, OwnerId, CreatedDate, ContactId, AccountId }) => ({ Id, Status, Priority, Origin, OwnerId, CreatedDate, ContactId, AccountId }));
  cases.closedCases.data = r.cases
    .filter((c) => c.IsClosed && Date.parse(c.ClosedDate!.replace("+0000", "Z")) >= yearStart.getTime())
    .map(({ Id, CreatedDate, ClosedDate, Priority, Origin, OwnerId, Type, Reason }) => ({ Id, CreatedDate, ClosedDate, Priority, Origin, OwnerId, Type, Reason }));
  const activity = changed.get(serviceActivityExtractor.id) as { statusChanges: Read };
  activity.statusChanges.data = r.history.filter((h) => Date.parse(h.CreatedDate.replace("+0000", "Z")) >= yearStart.getTime()).map(({ Id: _id, Field: _f, ...h }) => h);
  return changed;
}

/** A small seeded random, so a constructed org is the same on every run (mulberry32). */
export function seededRandom(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/**
 * A case made at the very start of the year read, changed at once and closed
 * the next day, so the Status history found starts on the year's first full
 * day: without it, the constructed history would start with its earliest case,
 * and the cases made in its first days would be left out as made before it.
 * Made on the year's first, part-read day, it is itself left out and said.
 */
export const ANCHOR: CaseSpec = { made: 365, steps: [["Working", 0.5], ["Closed", 1]] };

/**
 * A rich org. 300 cases made 40 to 339 days before the run, all closed: most go
 * New, Working, Closed in 1 to 9 days; one in five waits on the customer for 2
 * to 5 days on the way; one in ten is closed straight from New; one in eight is
 * reopened, with longer after its first close than before it; one in ten is
 * sent back from Waiting to Working. Then 40 open cases made 1 to 12 days before
 * the run, too recent for the period: half waiting on the customer since their
 * first day, half in Working. Priorities and origins in turn.
 */
export function richCases(seed = 20261127): CaseSpec[] {
  const random = seededRandom(seed);
  const specs: CaseSpec[] = [ANCHOR];
  const priorities = ["High", "Medium", "Low"];
  const origins = ["Email", "Web", "Phone"];
  for (let i = 0; i < 300; i++) {
    const made = 40 + i;
    const priority = priorities[i % 3]!;
    const origin = origins[Math.floor(i / 3) % 3]!;
    const w = 1 + Math.floor(random() * 4);
    let steps: Array<[string, number]>;
    if (i % 10 === 0) steps = [["Closed", 0.5 + random()]];
    else if (i % 8 === 1) {
      const first = w + 1;
      steps = [["Working", 0.5], ["Closed", first], ["Working", first + 3], ["Closed", first + 3 + first + 2 + random() * 3]];
    } else if (i % 10 === 3) steps = [["Working", 0.5], ["Waiting", w], ["Working", w + 2], ["Waiting", w + 3], ["Closed", w + 5 + random() * 2]];
    else if (i % 5 === 2) steps = [["Working", 0.5], ["Waiting", w], ["Closed", w + 2 + random() * 3]];
    else steps = [["Working", 0.5], ["Closed", w + random() * 5]];
    specs.push({ made, priority, origin, steps });
  }
  for (let i = 0; i < 40; i++) {
    const made = 1 + (i % 12);
    const priority = priorities[i % 3]!;
    const origin = origins[Math.floor(i / 3) % 3]!;
    specs.push({ made, priority, origin, steps: i % 2 === 0 ? [["Working", 0.25], ["Waiting", 0.5]] : [["Working", 0.25]] });
  }
  return specs;
}

/**
 * Many closed straight: 200 cases New, Working, Closed in two days and 100
 * closed as duplicates straight from New, made 40 to 339 days before the run,
 * and one open case.
 */
export function straightCases(): CaseSpec[] {
  const priorities = ["High", "Medium", "Low"];
  const origins = ["Email", "Web", "Phone"];
  return [
    ANCHOR,
    ...Array.from({ length: 300 }, (_, i): CaseSpec => ({ made: 40 + i, priority: priorities[i % 3]!, origin: origins[Math.floor(i / 3) % 3]!, steps: i % 3 === 1 ? [["Duplicate", 0.2]] : [["Working", 0.5], ["Closed", 2]] })),
    { made: 3, steps: [["Working", 0.5]] },
  ];
}
