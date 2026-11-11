import { z } from "zod";
import { CHANGES, MAX_QUERIED_OBJECTS } from "../extract/apex-touches.ts";
import { tokenSource } from "../privacy/pseudonymiser.ts";

// The org handbook's data (DECISIONS.md 451 to 457): what each flow, validation
// rule, Apex class and trigger does, written to handbook.json in the run folder.
// It's the customer's own configuration, so it goes nowhere but the documents
// made for them: not to the benchmark, Claude, the worker's store or the
// console (452).

// Still 2 with each flow's paths added (decision 446): no file of version 2 had
// left an engine before them, so none in a run folder lacks them. 3 adds the
// recent changes (decision 461), 4 when each class and trigger was last changed
// and by whom (decision 473), and 5 the same for each flow and validation rule
// (decision 482). Files of versions 2, 3 and 4 are still read.
export const HANDBOOK_VERSION = 5;

// The most a flow, step, entry, start and text keep (decision 453). Anything
// past these is cut, marked as cut, and carries its full count.
export const MAX_STEPS = 500;
export const MAX_FIELDS = 50;
export const MAX_CONDITIONS = 50;
export const MAX_PATHS = 50;
export const MAX_TEXT = 4_000;
// And the most Apex classes and triggers, by name (decision 456).
export const MAX_APEX_CLASSES = 5_000;
export const MAX_APEX_TRIGGERS = 1_000;
// And the most setup changes: the trail's own read stops at as many (decision 461).
export const MAX_CHANGES = 2_000;

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

/**
 * One way a flow starts running (decision 446): the start's own connector, or
 * one of its scheduled paths. `kind` is "immediate", "scheduled" (a path with
 * no pathType) or "afterCommit" (pathType AsyncAfterCommit), and any other
 * pathType under Salesforce's own name for it, as a step's kind is (453).
 */
export const HandbookPath = z.strictObject({
  kind: z.string().min(1),
  /** As the admin wrote it, redacted like other free text. */
  label: z.string().nullable(),
  /** When a later path runs, as Salesforce records it, with nothing worked out. Null for the immediate path. */
  when: z
    .strictObject({
      /** Flow Builder saves "before" as a negative number (QUESTIONS.md, handbook). */
      offsetNumber: z.number().nullable(),
      offsetUnit: z.string().nullable(),
      /** "RecordField" or "RecordTriggerEvent", as Salesforce names them. */
      timeSource: z.string().nullable(),
      recordField: z.string().nullable(),
    })
    .nullable(),
  next: z.string().nullable(),
});
export type HandbookPath = z.infer<typeof HandbookPath>;

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
    /** Every way it starts, the immediate path first when the start has one. */
    paths: z.array(HandbookPath).max(MAX_PATHS),
    pathsTotal: Count,
    pathsCut: z.boolean(),
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
  /** It searches. What a search reads isn't kept. */
  searches: z.boolean(),
});
export type HandbookApexTouches = z.infer<typeof HandbookApexTouches>;

/** A pseudonymiser's token for a user: a name only when the Word document is made (decision 421). */
export const UserToken = z.string().regex(new RegExp(`^${tokenSource("user", "U")}$`));

/** When a class, trigger, flow or validation rule was last changed, and by whom (decisions 473 and 482). */
export const HandbookLastChange = z.strictObject({
  /** LastModifiedDate, as Salesforce gave it when the step read: it can be after the run's date. */
  at: z.string().min(1),
  /** Null when Salesforce names no user. */
  by: UserToken.nullable(),
});
export type HandbookLastChange = z.infer<typeof HandbookLastChange>;

/** A class or trigger as a file of version 3 or before holds it. */
const olderApexItem = {
  name: z.string(),
  apiVersion: z.number(),
  status: z.string(),
  /** Null when the code couldn't be read, so it can't be told. */
  isTest: z.boolean().nullable(),
  /** Null when the code couldn't be read, or the run didn't read what it touches. */
  touches: HandbookApexTouches.nullable(),
};

const apexItem = {
  ...olderApexItem,
  /** Null when it wasn't read, which the part says, or Salesforce gave no date. */
  lastChanged: HandbookLastChange.nullable(),
};

export const HandbookApexClass = z.strictObject(apexItem);
export type HandbookApexClass = z.infer<typeof HandbookApexClass>;

/** Trigger events in a fixed order, by the names Apex gives them. */
export const TRIGGER_EVENTS = ["before insert", "before update", "before delete", "after insert", "after update", "after delete", "after undelete"] as const;

const triggerItem = {
  /** TableEnumOrId, a custom object's id given as its API name when the run could name it, and as Salesforce gives it when not. */
  object: z.string().min(1),
  /** Null when the run didn't read them, never an empty list. */
  events: z.array(z.enum(TRIGGER_EVENTS)).min(1).nullable(),
};

export const HandbookApexTrigger = z.strictObject({ ...apexItem, ...triggerItem });
export type HandbookApexTrigger = z.infer<typeof HandbookApexTrigger>;

/**
 * A flow as the file lists it: what the metadata step read, and when it was
 * last changed and by whom (decision 482). Null when that wasn't read, which
 * the part says, or Salesforce's list of changes had no line for it.
 */
export const HandbookFlowItem = z.strictObject({ ...HandbookFlow.shape, lastChanged: HandbookLastChange.nullable() });
export type HandbookFlowItem = z.infer<typeof HandbookFlowItem>;

/** A validation rule as the file lists it, with its last change as a flow has it. */
export const HandbookValidationRuleItem = z.strictObject({ ...HandbookValidationRule.shape, lastChanged: HandbookLastChange.nullable() });
export type HandbookValidationRuleItem = z.infer<typeof HandbookValidationRuleItem>;

// "We couldn't read it" and "there's none" must never look alike (decision 454, rule 5).
const Unavailable = z.strictObject({ status: z.literal("unavailable"), reason: z.string().min(1) });

/** A list cut at `max` by name, with the full count beside it (decision 453). */
const capped = <T extends z.ZodType>(item: T, max: number) =>
  z.discriminatedUnion("status", [z.strictObject({ status: z.literal("ok"), items: z.array(item).max(max), total: Count, cut: z.boolean() }), Unavailable]);

/**
 * Whether this run read when each was last changed: "refused" when Salesforce
 * refused the read (for Apex, the fields, and the list was read without them),
 * "not_kept" when the run was read before they were kept (decisions 473 and
 * 482). Salesforce's reason is never here: it stays in the run folder.
 */
export const LAST_CHANGES = ["read", "refused", "not_kept"] as const;
export type LastChanges = (typeof LAST_CHANGES)[number];

/** An Apex list as `capped` makes it, saying whether the last changes were read. */
const cappedApex = <T extends z.ZodType>(item: T, max: number) =>
  z.discriminatedUnion("status", [z.strictObject({ status: z.literal("ok"), items: z.array(item).max(max), total: Count, cut: z.boolean(), lastChanges: z.enum(LAST_CHANGES) }), Unavailable]);

/** One setup change, as the Setup Audit Trail records it (decision 461). */
export const HandbookChange = z.strictObject({
  /** CreatedDate, as Salesforce gives it. */
  at: z.string().min(1),
  section: z.string().nullable(),
  action: z.string().nullable(),
  /** Salesforce's sentence for it, redacted, as written, and cut like a formula. */
  what: Text.nullable(),
  whatCut: z.boolean(),
  whatLength: Count.nullable(),
  /** Null when the trail names no user. */
  by: UserToken.nullable(),
  /** The username someone was logged in as, as the trail has it. */
  loggedInAs: z.string().nullable(),
});
export type HandbookChange = z.infer<typeof HandbookChange>;

export const HandbookRecentChanges = z.discriminatedUnion("status", [
  z.strictObject({
    status: z.literal("ok"),
    /** Newest first. */
    items: z.array(HandbookChange).max(MAX_CHANGES),
    total: Count,
    /** The trail had more rows than the read takes, so the oldest in the window aren't here. */
    cut: z.boolean(),
    /** User administration ("Manage Users") rows, counted and not kept. */
    leftOutUserAdmin: Count,
    /** How many days back the trail was read. */
    days: Count,
  }),
  Unavailable,
]);
export type HandbookRecentChanges = z.infer<typeof HandbookRecentChanges>;

/** The flows and validation rules as a file of version 4 or before has them: neither says when it was last changed. */
const olderMetadataParts = {
  flows: z.discriminatedUnion("status", [z.strictObject({ status: z.literal("ok"), items: z.array(HandbookFlow) }), Unavailable]),
  // Nor may "no rule on Case" and "we didn't read Case": the list says what it covers.
  validationRules: z.discriminatedUnion("status", [z.strictObject({ status: z.literal("ok"), covers: ValidationRuleCoverage, items: z.array(HandbookValidationRule) }), Unavailable]),
};

export const Handbook = z.strictObject({
  version: z.literal(HANDBOOK_VERSION),
  // Each saying whether the last changes were read, as an Apex list does (decision 482).
  flows: z.discriminatedUnion("status", [z.strictObject({ status: z.literal("ok"), items: z.array(HandbookFlowItem), lastChanges: z.enum(LAST_CHANGES) }), Unavailable]),
  validationRules: z.discriminatedUnion("status", [z.strictObject({ status: z.literal("ok"), covers: ValidationRuleCoverage, items: z.array(HandbookValidationRuleItem), lastChanges: z.enum(LAST_CHANGES) }), Unavailable]),
  apexClasses: cappedApex(HandbookApexClass, MAX_APEX_CLASSES),
  apexTriggers: cappedApex(HandbookApexTrigger, MAX_APEX_TRIGGERS),
  recentChanges: HandbookRecentChanges,
});
export type Handbook = z.infer<typeof Handbook>;

/** A file from before the flows' and validation rules' last changes were kept. It's read as a handbook that lists them as not kept, and says nothing of them (decision 482). */
export const HandbookVersion4 = z.strictObject({
  version: z.literal(4),
  ...olderMetadataParts,
  apexClasses: cappedApex(HandbookApexClass, MAX_APEX_CLASSES),
  apexTriggers: cappedApex(HandbookApexTrigger, MAX_APEX_TRIGGERS),
  recentChanges: HandbookRecentChanges,
});

/** The four parts as a file of version 3 or before has them: no class or trigger says when it was last changed. */
const olderParts = {
  ...olderMetadataParts,
  apexClasses: capped(z.strictObject(olderApexItem), MAX_APEX_CLASSES),
  apexTriggers: capped(z.strictObject({ ...olderApexItem, ...triggerItem }), MAX_APEX_TRIGGERS),
};

/** A file from before the last changes were kept. It's read as a handbook whose Apex lists them as not kept, and says nothing of them (decision 473). */
export const HandbookVersion3 = z.strictObject({ version: z.literal(3), ...olderParts, recentChanges: HandbookRecentChanges });

/**
 * A file from before the recent changes were kept: the same four parts, and
 * no fifth. It's read as a handbook whose fifth part says so (decision 462).
 */
export const HandbookVersion2 = z.strictObject({ version: z.literal(2), ...olderParts });

/** Why a handbook has no recent changes when its run kept none: the part says only this. */
export const CHANGES_NOT_KEPT = "This run was read before recent changes were kept.";
