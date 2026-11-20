import { z } from "zod";
import { CalendarDate, Timestamp } from "./common.ts";

// The org's inventory (DECISIONS.md 603 to 605): inventory.json in the run
// folder, and the console's HealthCheckInventory row. Names Salesforce gives
// set-up, and counts: never a record, a field's value, or a person.

export const INVENTORY_VERSION = 1;

const Count = z.number().int().nonnegative();
const Name = z.string().min(1).max(255);
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
        status: z.string().max(100),
        /** Seats allowed, or null on a site licence, which Salesforce gives as -1. */
        allowed: Count.nullable(),
        siteWide: z.boolean(),
        used: Count,
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

export const LimitRow = z.object({ name: Name, max: z.number().int(), used: z.number().int() }).strict();

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
  })
  .strict();

export type Inventory = z.infer<typeof Inventory>;
export type FillRateObject = z.infer<typeof FillRateObject>;
export type PackageRow = z.infer<typeof PackageRow>;
export type ConnectedAppRow = z.infer<typeof ConnectedAppRow>;
export type RiskyPermissionRow = z.infer<typeof RiskyPermissionRow>;
export type LimitRow = z.infer<typeof LimitRow>;
