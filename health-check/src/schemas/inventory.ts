import { z } from "zod";
import { CalendarDate, Timestamp } from "./common.ts";

// The org's inventory (DECISIONS.md 603 to 605): inventory.json in the run
// folder, and the console's HealthCheckInventory row. Names Salesforce gives
// set-up, and counts: never a record, a field's value, or a person.

export const INVENTORY_VERSION = 1;

const Count = z.number().int().nonnegative();
/** The longest name kept. A longer one is cut to it and the section's notes say so (DECISIONS.md 618). */
export const NAME_MAX = 255;
const Name = z.string().min(1).max(NAME_MAX);
const Unavailable = z.object({ unavailable: z.string().min(1) }).strict();

/** A section holds its rows, with notes on anything capped, skipped or not read, or says why it couldn't be read. */
const section = <T extends z.ZodTypeAny>(row: T) => z.union([z.object({ rows: z.array(row), notes: z.array(z.string()) }).strict(), Unavailable]);

export const FillRateField = z.object({ field: Name, filled: Count, share: z.number().min(0).max(1).nullable() }).strict();
export const FillRateObject = z.union([
  z
    .object({
      object: Name,
      records: Count,
      fields: z.array(FillRateField),
      /** Fields not counted, each with the reason. */
      skipped: z.array(z.object({ field: Name, reason: z.string().min(1) }).strict()),
    })
    .strict(),
  z.object({ object: Name, unavailable: z.string().min(1) }).strict(),
]);

export const PackageRow = z
  .object({
    /** SubscriberPackageId: what an installed package is listed by. Null for a namespace with only a licence row. */
    packageId: z.string().max(18).nullable(),
    namespace: z.string().max(255).nullable(),
    name: z.string().max(255).nullable(),
    /** Major.minor.patch.build, as Salesforce numbers the installed version. */
    version: z.string().max(100).nullable(),
    versionName: z.string().max(255).nullable(),
    /** Listed by InstalledSubscriberPackage. A namespace with only a licence row is false. */
    installed: z.boolean(),
    /** Null where Salesforce didn't say. */
    deprecated: z.boolean().nullable(),
    managed: z.boolean().nullable(),
    licence: z
      .object({
        /** Null where Salesforce gave none (DECISIONS.md 618). */
        status: z.string().max(100).nullable(),
        /** Seats allowed, or null on a site licence, which Salesforce gives as -1. */
        allowed: Count.nullable(),
        /** True only where Salesforce gives -1 seats allowed. */
        siteWide: z.boolean(),
        /** Seats in use, or null where Salesforce gave no count (DECISIONS.md 618). */
        used: Count.nullable(),
        expiresOn: CalendarDate.nullable(),
      })
      .strict()
      .nullable(),
  })
  .strict();

export const ConnectedAppRow = z
  .object({
    name: Name,
    /** Listed among the org's connected apps. False: only users authorised it. Null: the list couldn't be read. */
    installed: z.boolean().nullable(),
    /** Whether only users an admin approved may use it. Null where it couldn't be read. */
    adminApprovedOnly: z.boolean().nullable(),
    firstAuthorised: Timestamp.nullable(),
    lastUsed: Timestamp.nullable(),
    /** People holding a token for it: a count, never who. Null when the tokens couldn't be read. */
    users: Count.nullable(),
    uses: Count.nullable(),
  })
  .strict();

export const RiskyPermissionRow = z
  .object({
    kind: z.enum(["profile", "permission set"]),
    /** The profile's name, or the permission set's label. */
    name: Name,
    apiName: Name,
    namespace: z.string().max(255).nullable(),
    /** The permissions it grants, by Salesforce's label, from the list checked. */
    grants: z.array(Name).min(1),
    /** Active users holding it, directly or through a permission set group. Null when assignments couldn't be read. */
    activeUsers: Count.nullable(),
  })
  .strict();

/** The check a holders row links to, by its id. */
const RuleId = z.string().min(1).max(100);

/**
 * One permission from the list checked, by Salesforce's label, and how many
 * active people hold it, each counted once (DECISIONS.md 735). Counts only,
 * never who. No verdict: where a check judges the permission, `rule` names it
 * and this count is that check's.
 */
export const RiskyPermissionHoldersRow = z.union([
  z
    .object({
      permission: Name,
      /** The check that judges this permission, where one does: its finding is the verdict, and its count is this one. */
      rule: RuleId.nullable(),
      /** Active users of the standard type holding it, each once, however many profiles, permission sets or groups grant it. */
      holders: Count,
      /** Of them, those holding it through their profile. */
      byProfile: Count,
      /** Of them, those assigned a permission set that grants it. Null where sets and groups couldn't be told apart. */
      byPermissionSet: Count.nullable(),
      /** Of them, those holding it through a permission set group. Null where sets and groups couldn't be told apart. */
      byGroup: Count.nullable(),
      /** Of them, integration accounts: calibrated as one, on an integration licence, or set to API only. Null when who is API only couldn't be read. */
      integration: Count.nullable(),
      /**
       * Of them, those who haven't logged in for the dormant period, or never have, as the
       * dormant-user checks tell it: anyone set up within the period, and anyone on an
       * integration licence (counted under `integration`), is left out, as those checks leave them.
       */
      notLoggedIn: Count,
      /**
       * Active users not counted who are assigned a permission set group holding a set
       * that grants it. A group can mute a permission and mutes aren't read, so we
       * can't say they hold it. Null when the groups, or who is assigned one, couldn't be read.
       */
      unconfirmedThroughGroups: Count.nullable(),
    })
    .strict(),
  z.object({ permission: Name, rule: RuleId.nullable(), unavailable: z.string().min(1) }).strict(),
]);

/**
 * As Salesforce reports it: `used` is the maximum less what remains, so it is
 * below zero where Salesforce reports more remaining than the maximum, and
 * either can be a fraction. The section's notes name any such limit (DECISIONS.md 618).
 */
export const LimitRow = z.object({ name: Name, max: z.number(), used: z.number() }).strict();

export const Inventory = z
  .object({
    version: z.literal(INVENTORY_VERSION),
    madeAt: Timestamp,
    scope: z.enum(["complete", "essentials"]),
    fillRates: section(FillRateObject),
    packages: section(PackageRow),
    connectedApps: section(ConnectedAppRow),
    riskyPermissions: z.union([
      z.object({ rows: z.array(RiskyPermissionRow), notes: z.array(z.string()), checked: z.array(Name), notInOrg: z.array(Name) }).strict(),
      Unavailable,
    ]),
    limits: section(LimitRow),
    /**
     * Added after version 1 was first written, so an older file has none
     * (DECISIONS.md 735). `dormantDays` is the period `notLoggedIn` counts.
     */
    riskyPermissionHolders: z
      .union([z.object({ rows: z.array(RiskyPermissionHoldersRow), notes: z.array(z.string()), dormantDays: z.number().int().positive() }).strict(), Unavailable])
      .optional(),
  })
  .strict();

export type Inventory = z.infer<typeof Inventory>;
export type FillRateObject = z.infer<typeof FillRateObject>;
export type PackageRow = z.infer<typeof PackageRow>;
export type ConnectedAppRow = z.infer<typeof ConnectedAppRow>;
export type RiskyPermissionRow = z.infer<typeof RiskyPermissionRow>;
export type LimitRow = z.infer<typeof LimitRow>;
export type RiskyPermissionHoldersRow = z.infer<typeof RiskyPermissionHoldersRow>;
