import { z } from "zod";
import { CHANGES, MAX_QUERIED_OBJECTS } from "../extract/apex-touches.ts";

// The org handbook's data (DECISIONS.md 451 to 457): what each flow, validation
// rule, Apex class and trigger does, written to handbook.json in the run folder. It's the
// customer's own configuration, so it goes nowhere but the documents made for
// them: not to the benchmark, Claude, the worker's store or the console (452).

export const HANDBOOK_VERSION = 2;

// The most a flow, step, entry and text keep (decision 453). Anything past
// these is cut, marked as cut, and carries its full count.
export const MAX_STEPS = 500;
export const MAX_FIELDS = 50;
export const MAX_CONDITIONS = 50;
export const MAX_TEXT = 4_000;
// And the most Apex classes and triggers, by name (decision 456).
export const MAX_APEX_CLASSES = 5_000;
export const MAX_APEX_TRIGGERS = 1_000;

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

/**
 * Which objects' validation rules were read (decision 455). Tooling lists every
 * rule. The retrieve asks for every custom object and names a few standard ones,
 * and its wildcard brings no standard object, so it holds those few and no more.
 */
export const ValidationRuleCoverage = z.strictObject({
  customObjects: z.literal("all"),
  standardObjects: z.union([z.literal("all"), z.array(z.string().min(1))]),
});
export type ValidationRuleCoverage = z.infer<typeof ValidationRuleCoverage>;

/**
 * What a class or trigger touches, read from its code with comments and strings
 * left out (decision 457). Identifiers only: no code, comment or string is kept.
 */
export const HandbookApexTouches = z.strictObject({
  /** Objects after FROM in its inline queries, sorted. */
  queries: z.array(z.string().regex(/^[A-Za-z]\w*$/)).max(MAX_QUERIED_OBJECTS),
  queriesTotal: Count,
  changes: z.array(z.enum(CHANGES)),
  /** It builds a query at run time, so what that query reads isn't known. */
  dynamicQueries: z.boolean(),
});
export type HandbookApexTouches = z.infer<typeof HandbookApexTouches>;

const apexItem = {
  name: z.string(),
  apiVersion: z.number(),
  status: z.string(),
  /** Null when the code couldn't be read, so it can't be told. */
  isTest: z.boolean().nullable(),
  /** Null when the code couldn't be read, or the run didn't read what it touches. */
  touches: HandbookApexTouches.nullable(),
};

export const HandbookApexClass = z.strictObject(apexItem);
export type HandbookApexClass = z.infer<typeof HandbookApexClass>;

/** Trigger events in a fixed order, by the names Apex gives them. */
export const TRIGGER_EVENTS = ["before insert", "before update", "before delete", "after insert", "after update", "after delete", "after undelete"] as const;

export const HandbookApexTrigger = z.strictObject({
  ...apexItem,
  /** TableEnumOrId as Salesforce gives it. */
  object: z.string().min(1),
  /** Null when the run didn't read them, never an empty list. */
  events: z.array(z.enum(TRIGGER_EVENTS)).min(1).nullable(),
});
export type HandbookApexTrigger = z.infer<typeof HandbookApexTrigger>;

// "We couldn't read it" and "there's none" must never look alike (decision 454, rule 5).
const Unavailable = z.strictObject({ status: z.literal("unavailable"), reason: z.string().min(1) });

/** A list cut at `max` by name, with the full count beside it (decision 453). */
const capped = <T extends z.ZodType>(item: T, max: number) =>
  z.discriminatedUnion("status", [z.strictObject({ status: z.literal("ok"), items: z.array(item).max(max), total: Count, cut: z.boolean() }), Unavailable]);

export const Handbook = z.strictObject({
  version: z.literal(HANDBOOK_VERSION),
  flows: z.discriminatedUnion("status", [z.strictObject({ status: z.literal("ok"), items: z.array(HandbookFlow) }), Unavailable]),
  // Nor may "no rule on Case" and "we didn't read Case": the list says what it covers.
  validationRules: z.discriminatedUnion("status", [z.strictObject({ status: z.literal("ok"), covers: ValidationRuleCoverage, items: z.array(HandbookValidationRule) }), Unavailable]),
  apexClasses: capped(HandbookApexClass, MAX_APEX_CLASSES),
  apexTriggers: capped(HandbookApexTrigger, MAX_APEX_TRIGGERS),
});
export type Handbook = z.infer<typeof Handbook>;
