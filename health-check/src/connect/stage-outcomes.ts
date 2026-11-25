import type { CalibratedStage, Calibration } from "../schemas/calibration.ts";
import type { OrgStage } from "./org-stages.ts";
import { outcomeFromName } from "./stage-names.ts";

// "Answer to unlock" for an automatic run's closed stages (DECISIONS.md 756).
// Salesforce's flags say a stage is closed and not won, never whether that is
// lost or no decision, so an automatic calibration leaves such stages out and
// every check that splits the two is not measurable (692, 600). The customer
// can tell us in the portal, which the console keeps in
// HealthCheckCustomerAnswer as one JSON object: stage API name to "lost" or
// "no_decision". This file reads that answer and puts it into a calibration,
// on a stage the org has that is closed and not won, and nowhere else: never a
// won or open stage, never a stage a person or Salesforce already named.
// Nothing here reads Salesforce: the stages are the ones connect already read.
// Every count it gives is a number only: stage names are the org's words.

/** The question's name in the console's HealthCheckCustomerAnswer. */
export const STAGE_ANSWER_QUESTION = "closed_stages";

export type StageOutcome = "lost" | "no_decision";
/** A customer's answer as read: what it says of each stage it names, and how many entries in it weren't an answer at all. */
export type StageAnswer = { outcomes: ReadonlyMap<string, StageOutcome>; ignored: number };

/** No org has this many stages: an answer naming more is refused whole, so what it sets never depends on its order. */
const MAX_ENTRIES = 2_000;
/** A picklist value's API name is at most 255 characters. */
const MAX_NAME = 255;

/**
 * The stored answer, or "malformed" when it isn't a JSON object (a string
 * holding one, or the object itself) or names more entries than any org has
 * stages. Within an object, only a non-empty name
 * of a stage's length with exactly "lost" or "no_decision" is an answer;
 * anything else in it is counted as ignored and never read further.
 */
export function parseStageAnswer(value: unknown): StageAnswer | "malformed" {
  let read = value;
  if (typeof value === "string") {
    try {
      read = JSON.parse(value);
    } catch {
      return "malformed";
    }
  }
  if (read === null || typeof read !== "object" || Array.isArray(read)) return "malformed";
  const entries = Object.entries(read as Record<string, unknown>);
  if (entries.length > MAX_ENTRIES) return "malformed";
  const outcomes = new Map<string, StageOutcome>();
  let ignored = 0;
  for (const [name, outcome] of entries) {
    if (name.length === 0 || name.length > MAX_NAME || (outcome !== "lost" && outcome !== "no_decision")) ignored++;
    else outcomes.set(name, outcome);
  }
  return { outcomes, ignored };
}

/** Where a machine-set stage's meaning came from (DECISIONS.md 756): the customer's answer, or its name by our rule. */
export type StageFrom = NonNullable<CalibratedStage["from"]>;

/** A stage the customer's answer set: lost or no decision, marked as theirs. */
export const fromCustomer = (s: Pick<CalibratedStage, "category" | "from">): boolean => s.from === "customer" && (s.category === "lost" || s.category === "no_decision");
/** A stage read from its name by our rule: lost or no decision, marked so. */
export const fromName = (s: Pick<CalibratedStage, "category" | "from">): boolean => s.from === "name" && (s.category === "lost" || s.category === "no_decision");
/** Set by the machine from the customer's answer or the name rule, so ours to set again. Anything else is a person's word or Salesforce's. */
export const machineSet = (s: Pick<CalibratedStage, "category" | "from">): boolean => fromCustomer(s) || fromName(s);

/** A calibration's own stages, without those the machine set: a person's word or Salesforce's, which neither an answer nor a name changes. */
export const namedByOthers = (stages: readonly CalibratedStage[]): CalibratedStage[] => stages.filter((s) => !machineSet(s));

/**
 * The closed, not-won stages the machine sets, in the org's order, each one the
 * org has that `named` (a person's or Salesforce's) doesn't hold: the
 * customer's answer first, then, only with `byName`, the name rule for a stage
 * the answer leaves out. `ignored` counts the answer's entries that set
 * nothing: not an answer, or a stage the org doesn't have, isn't closed, is won,
 * or that a person or Salesforce already named.
 */
export function machineStages(org: readonly OrgStage[], named: ReadonlySet<string>, answer: StageAnswer | null, byName: boolean): { stages: CalibratedStage[]; ignored: number; answered: number } {
  const stages: CalibratedStage[] = [];
  let answered = 0;
  for (const s of org) {
    if (!s.isClosed || s.isWon || named.has(s.apiName) || stages.some((t) => t.name === s.apiName)) continue;
    const said = answer?.outcomes.get(s.apiName);
    if (said) {
      stages.push({ name: s.apiName, category: said, from: "customer" });
      answered++;
      continue;
    }
    const read = byName ? outcomeFromName(s.label, s.apiName) : null;
    if (read) stages.push({ name: s.apiName, category: read, from: "name" });
  }
  return { stages, answered, ignored: answer ? answer.ignored + answer.outcomes.size - answered : 0 };
}

/**
 * A saved calibration's stages with the machine's part set again: every stage a
 * person or Salesforce named kept as it is and where it is, then the customer's
 * newest answer and (with `byName`) the name rule, in the org's order. A later
 * answer replaces an earlier one whole and replaces a name's reading; a stage
 * no longer answered goes back to its name's reading or to undecided; with the
 * rule off, no name's reading is kept. `changed` false: exactly as they were.
 */
export function withMachineStages(calibration: Calibration, org: readonly OrgStage[], answer: StageAnswer | null, byName: boolean): { stages: CalibratedStage[]; answered: number; byName: number; ignored: number; changed: boolean } {
  const kept = namedByOthers(calibration.stages);
  const set = machineStages(org, new Set(kept.map((s) => s.name)), answer, byName);
  const stages = [...kept, ...set.stages];
  return { stages, answered: set.answered, byName: set.stages.length - set.answered, ignored: set.ignored, changed: JSON.stringify(stages) !== JSON.stringify(calibration.stages) };
}
