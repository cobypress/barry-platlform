import { z } from "zod";

// The org handbook's data (DECISIONS.md 451 to 454): what each flow and
// validation rule does, written to handbook.json in the run folder. It's the
// customer's own configuration, so it goes nowhere but the documents made for
// them: not to the benchmark, Claude, the worker's store or the console (452).

export const HANDBOOK_VERSION = 1;

// The most a flow, step, entry and text keep (decision 453). Anything past
// these is cut, marked as cut, and carries its full count.
export const MAX_STEPS = 500;
export const MAX_FIELDS = 50;
export const MAX_CONDITIONS = 50;
export const MAX_TEXT = 4_000;

const Count = z.number().int().nonnegative();
const Text = z.string().max(MAX_TEXT);

/** One entry condition. `valueLength` is the value's full length, so a cut value says how much it had. */
export const HandbookCondition = z.strictObject({
  field: z.string(),
  operator: z.string().nullable(),
  value: Text.nullable(),
  valueCut: z.boolean(),
  valueLength: Count.nullable(),
});
export type HandbookCondition = z.infer<typeof HandbookCondition>;

/** Where one path out of a step goes: a decision's or wait's outcome, or a loop's two ways out. */
export const HandbookOutcome = z.strictObject({ label: z.string(), next: z.string().nullable() });
export type HandbookOutcome = z.infer<typeof HandbookOutcome>;

/** One element of a flow, under Salesforce's own name for its kind. */
export const HandbookStep = z.strictObject({
  kind: z.string().min(1),
  name: z.string(),
  label: z.string().nullable(),
  object: z.string().nullable(),
  /** Fields it filters on, reads, sets or assigns, in that order. */
  fields: z.array(z.string()).max(MAX_FIELDS),
  fieldsTotal: Count,
  /** The action and its type, the subflow, the loop's collection or the record variable. */
  detail: z.string().nullable(),
  outcomes: z.array(HandbookOutcome),
  next: z.string().nullable(),
  fault: z.string().nullable(),
  /** False when no path from the start reaches it. Those come after the rest. */
  reached: z.boolean(),
});
export type HandbookStep = z.infer<typeof HandbookStep>;

export const HandbookFlow = z.strictObject({
  name: z.string(),
  label: z.string(),
  status: z.string(),
  processType: z.string(),
  description: z.string().nullable(),
  start: z.strictObject({
    object: z.string().nullable(),
    triggerType: z.string().nullable(),
    recordTriggerType: z.string().nullable(),
    schedule: z.strictObject({ frequency: z.string().nullable(), startDate: z.string().nullable(), startTime: z.string().nullable() }).nullable(),
    entry: z.strictObject({
      logic: z.string().nullable(),
      formula: Text.nullable(),
      formulaCut: z.boolean(),
      formulaLength: Count.nullable(),
      conditions: z.array(HandbookCondition).max(MAX_CONDITIONS),
      conditionsTotal: Count,
      cut: z.boolean(),
    }),
  }),
  /** In the order the flow runs them, from the start, then any no path reaches. */
  steps: z.array(HandbookStep).max(MAX_STEPS),
  stepsTotal: Count,
  cut: z.boolean(),
});
export type HandbookFlow = z.infer<typeof HandbookFlow>;

export const HandbookValidationRule = z.strictObject({
  object: z.string(),
  name: z.string(),
  active: z.boolean(),
  description: z.string().nullable(),
  /** Null when the formula couldn't be read, which only happens on the Tooling route. */
  formula: Text.nullable(),
  formulaCut: z.boolean(),
  formulaLength: Count.nullable(),
  message: z.string().nullable(),
  /** The field the message shows on, or null when it shows at the top of the page. */
  messageField: z.string().nullable(),
});
export type HandbookValidationRule = z.infer<typeof HandbookValidationRule>;

// "We couldn't read it" and "there's none" must never look alike (decision 454, rule 5).
const part = <T extends z.ZodType>(item: T) =>
  z.discriminatedUnion("status", [z.strictObject({ status: z.literal("ok"), items: z.array(item) }), z.strictObject({ status: z.literal("unavailable"), reason: z.string().min(1) })]);

export const Handbook = z.strictObject({
  version: z.literal(HANDBOOK_VERSION),
  flows: part(HandbookFlow),
  validationRules: part(HandbookValidationRule),
});
export type Handbook = z.infer<typeof Handbook>;
