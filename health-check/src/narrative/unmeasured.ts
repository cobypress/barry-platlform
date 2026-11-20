import type { Finding, NotMeasurableKind } from "../schemas/index.ts";

// What couldn't be measured, grouped by kind in the documents' words (DECISIONS.md
// 627), so a reader can tell "you don't use this" and "we need something from
// you" from "we couldn't read it". Presentation only: no count, score or verdict
// depends on it. A finding written before kinds has none and reads as unreadable
// in a group. A set where every check is one we couldn't read, or that has no
// kinds at all, is printed as it was before: "couldn't be measured" already says
// that, and one heading over the whole list would add nothing.

export const KIND_ORDER: readonly NotMeasurableKind[] = ["not_in_use", "needs_input", "too_little_data", "unreadable"];

/** Each kind's heading wherever what couldn't be measured is listed. */
export const KIND_HEADINGS: Record<NotMeasurableKind, string> = {
  not_in_use: "Doesn't apply to your org",
  needs_input: "Needs something from you",
  too_little_data: "Not enough data to judge fairly",
  unreadable: "We couldn't read this",
};

/** Each kind as a count in a sentence: "2 don't apply to your org". */
const COUNTED: Record<NotMeasurableKind, (n: number) => string> = {
  not_in_use: (n) => `${n} ${n === 1 ? "doesn't" : "don't"} apply to your org`,
  needs_input: (n) => `${n} ${n === 1 ? "needs" : "need"} something from you`,
  too_little_data: (n) => `${n} ${n === 1 ? "has" : "have"} too little data to judge fairly`,
  unreadable: (n) => `${n} we couldn't read`,
};

export const kindOf = (f: Finding): NotMeasurableKind => f.notMeasurableKind ?? "unreadable";

const open = (findings: Finding[]): Finding[] => findings.filter((f) => f.status === "not_measurable");

/** Whether the documents group these by kind: some not measurable finding has a kind other than unreadable. */
export const hasKinds = (findings: Finding[]): boolean => open(findings).some((f) => f.notMeasurableKind !== undefined && f.notMeasurableKind !== "unreadable");

/** The not measurable findings by kind, in the order the documents give them, leaving out kinds with none. */
export function byKind(findings: Finding[]): Array<{ kind: NotMeasurableKind; heading: string; findings: Finding[] }> {
  const all = open(findings);
  return KIND_ORDER.map((kind) => ({ kind, heading: KIND_HEADINGS[kind], findings: all.filter((f) => kindOf(f) === kind) })).filter((g) => g.findings.length > 0);
}

/** How many of each kind, for the number check: the same counts `kindCounts` words. */
export const kindTallies = (findings: Finding[]): number[] => byKind(findings).map((g) => g.findings.length);

const listed = (items: string[]): string => (items.length <= 1 ? (items[0] ?? "") : `${items.slice(0, -1).join(", ")} and ${items.at(-1)}`);

/** "11 don't apply to your org, 6 need something from you and 3 we couldn't read". Empty without kinds. */
export function kindCounts(findings: Finding[]): string {
  if (!hasKinds(findings)) return "";
  return listed(byKind(findings).map((g) => COUNTED[g.kind](g.findings.length)));
}

/** The counts for a short note beside an area, or null where the note stays as it was. */
export const noteCounts = (findings: Finding[]): string | null => (hasKinds(findings) ? kindCounts(findings) : null);
