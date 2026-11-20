import { COMPLEXITY_BANDS, COMPLEXITY_VERSION, Complexity, type ComplexityComponent } from "../schemas/complexity.ts";

// The complexity score (DECISIONS.md 614): how big and tangled the org's build
// is, beside the health score and never part of it. A short published table, so
// a consultant can say exactly why an org scored what it did. Each count earns
// its points in proportion up to its top and nothing past it, so one enormous
// figure can't take more than its own share. The tops and weights are our
// judgement: changing either is a new FORMULA, and DECISIONS.md says why.

export const FORMULA = 1;

type CountDef = { id: string; label: string; top: number; points: number };
type ComponentDef = { id: string; label: string; weight: number; counts: CountDef[] };

export const COMPONENTS: ComponentDef[] = [
  {
    id: "dataModel",
    label: "Data model",
    weight: 20,
    counts: [
      { id: "customObjects", label: "Custom objects of your own", top: 100, points: 8 },
      { id: "customFields", label: "Custom fields of your own on custom objects, accounts, contacts, leads and opportunities", top: 1500, points: 8 },
      { id: "recordTypes", label: "Active record types on the same objects", top: 50, points: 4 },
    ],
  },
  {
    id: "declarativeAutomation",
    label: "Declarative automation",
    weight: 15,
    counts: [
      { id: "recordTriggeredFlows", label: "Active record-triggered flows", top: 100, points: 6 },
      { id: "otherFlows", label: "Other active flows", top: 100, points: 4 },
      { id: "validationRules", label: "Active validation rules", top: 200, points: 5 },
    ],
  },
  {
    id: "legacyAutomation",
    label: "Legacy automation",
    weight: 10,
    counts: [
      { id: "workflowRules", label: "Active Workflow Rules", top: 100, points: 5 },
      { id: "processBuilders", label: "Active Process Builders", top: 50, points: 5 },
    ],
  },
  {
    id: "code",
    label: "Code",
    weight: 20,
    counts: [
      { id: "apexClasses", label: "Active Apex classes not marked @isTest", top: 500, points: 8 },
      { id: "apexTriggers", label: "Active Apex triggers", top: 50, points: 4 },
      // Half the 6,000,000 Salesforce allows an org, counted as the code-size check counts it.
      { id: "apexCharacters", label: "Characters of that code, without comments", top: 3_000_000, points: 8 },
    ],
  },
  {
    id: "automationOverlap",
    label: "Automation overlap on objects",
    weight: 10,
    counts: [{ id: "objectsWithSeveralKinds", label: "Objects running more than one kind of active automation", top: 20, points: 10 }],
  },
  {
    id: "integrations",
    label: "Integrations and packages",
    weight: 15,
    counts: [
      { id: "connectedApps", label: "Connected apps", top: 30, points: 5 },
      { id: "endpoints", label: "Named credentials of your own and active remote sites", top: 30, points: 5 },
      { id: "packages", label: "Installed packages with a namespace", top: 30, points: 5 },
    ],
  },
  {
    id: "accessModel",
    label: "Access model",
    weight: 10,
    counts: [
      { id: "customProfiles", label: "Custom profiles", top: 50, points: 4 },
      { id: "customPermissionSets", label: "Custom permission sets of your own", top: 150, points: 4 },
      { id: "roles", label: "Roles", top: 200, points: 2 },
    ],
  },
];

/** Bands by their lowest score. Words of size, never of quality. */
export const BANDS: Array<{ min: number; band: (typeof COMPLEXITY_BANDS)[number] }> = [
  { min: 75, band: "Very complex" },
  { min: 50, band: "Complex" },
  { min: 25, band: "Moderate" },
  { min: 0, band: "Simple" },
];

/** Fewer components or less weight read than this, and there's no score. */
export const MIN_COMPONENTS_READ = 5;
export const MIN_WEIGHT_READ = 70;

export const bandOf = (score: number): (typeof COMPLEXITY_BANDS)[number] => BANDS.find((b) => score >= b.min)!.band;

const round2 = (x: number): number => Math.round(x * 100) / 100;

/** What a count earns: its points in proportion to its top, and no more once it's there. */
export const countPoints = (count: number, top: number, points: number): number => round2((points * Math.min(count, top)) / top);

/** A component's counts by id, or why they couldn't all be read. */
export type ComponentInput = { counts: Record<string, number>; notes?: string[] } | { reason: string };

export function scoreComplexity(inputs: Record<string, ComponentInput>, madeAt: Date): Complexity {
  const built = COMPONENTS.map((def): ComplexityComponent => {
    const input = inputs[def.id] ?? { reason: "It wasn't read." };
    if ("reason" in input) return { status: "not_read", id: def.id, label: def.label, weight: def.weight, reason: input.reason };
    const counts = def.counts.map((c) => {
      const count = input.counts[c.id];
      if (count === undefined || !Number.isInteger(count) || count < 0) throw new Error(`The complexity count ${c.id} is missing`);
      return { id: c.id, label: c.label, count, top: c.top, maxPoints: c.points, points: countPoints(count, c.top, c.points) };
    });
    return { status: "measured", id: def.id, label: def.label, weight: def.weight, counts, points: round2(counts.reduce((s, c) => s + c.points, 0)), shareOfScore: 0, notes: input.notes ?? [] };
  });
  const read = built.filter((c) => c.status === "measured");
  const earned = round2(read.reduce((s, c) => s + c.points, 0));
  for (const c of read) c.shareOfScore = earned > 0 ? Math.round((c.points / earned) * 100) : 0;
  const leftOut = built.filter((c) => c.status === "not_read");
  const weightRead = read.reduce((s, c) => s + c.weight, 0);
  const common = { version: COMPLEXITY_VERSION, formula: FORMULA, madeAt: madeAt.toISOString(), components: built, leftOut: leftOut.map((c) => c.id) };
  const named = leftOut.map((c) => `${c.label} (${c.reason})`).join("; ");
  // Rule 5: too little read to stand for the org, so no score rather than a partial one.
  if (read.length < MIN_COMPONENTS_READ || weightRead < MIN_WEIGHT_READ) return Complexity.parse({ status: "not_measurable", ...common, reason: `Too little of the org's build could be read to score its complexity. Not read: ${named}.` });
  const score = Math.round((earned * 100) / weightRead);
  return Complexity.parse({ status: "measured", ...common, score, band: bandOf(score), weightRead });
}
