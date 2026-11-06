import { z } from "zod";
import type { FindingRecords } from "../engine/evaluate.ts";
import { actionRecordsExtractor, type ActionRecords } from "../extract/action-records.ts";
import { opportunitiesExtractor, type OpportunityExtract } from "../extract/opportunities.ts";
import { duplicateAccountsExtractor, type DuplicateAccounts } from "../extract/duplicates.ts";
import { cpqQuotesExtractor, type CpqQuotes } from "../extract/cpq.ts";
import { usersExtractor, type UserRow } from "../extract/users.ts";
import type { Sourced } from "../extract/types.ts";

// The action lists' data (roadmap Stage 4): each issue's records, by name. It's
// built in the worker from what the rules handed over (ids and what's wrong)
// and the extracts (names and owners), and kept in the run folder beside the
// pseudonym map, outside checkpoints/: the raw extracts are deleted 30 days
// after release, and the lists have to outlive them as long as the documents
// do (decision 16, DECISIONS.md 196). Nothing here goes to Claude or the console.

export const ACTION_LISTS_FILE = "action-lists.json";

export const ACTION_OBJECTS = ["Opportunity", "Lead", "Contact", "Account", "SBQQ__Quote__c", "Contract", "User"] as const;

/** How a sheet names each object. */
export const OBJECT_LABELS: Record<(typeof ACTION_OBJECTS)[number], string> = { Opportunity: "Opportunity", Lead: "Lead", Contact: "Contact", Account: "Account", SBQQ__Quote__c: "Quote", Contract: "Contract", User: "User" };

export const ActionRow = z.object({
  id: z.string().min(1),
  /** Null when the extract didn't name the record. */
  name: z.string().nullable(),
  /** The record's owner. A user record has none: it's the person. */
  ownerId: z.string().nullable(),
  problem: z.string().min(1),
});
export type ActionRow = z.infer<typeof ActionRow>;

export const ActionList = z.object({
  findingId: z.string().min(1),
  ruleId: z.string().min(1),
  object: z.enum(ACTION_OBJECTS),
  fix: z.string().min(1),
  part: z.object({ total: z.number().int().nonnegative(), rest: z.string().min(1) }).optional(),
  rows: z.array(ActionRow),
});
export type ActionList = z.infer<typeof ActionList>;

/** Who a user is, so a sheet can name them with consent and give their role without. */
export const ActionUser = z.object({ name: z.string(), role: z.string().nullable(), profile: z.string().nullable() });
export type ActionUser = z.infer<typeof ActionUser>;

export const ActionLists = z.object({
  /** The org's own domain, for links to each record. */
  instanceUrl: z.url(),
  lists: z.array(ActionList),
  users: z.record(z.string(), ActionUser),
});
export type ActionLists = z.infer<typeof ActionLists>;

const ok = <T>(s: Sourced<T> | undefined): T | null => (s?.status === "ok" ? s.data : null);

/** Every record's name and owner that an extract holds, by object. */
function directory(extracts: Map<string, unknown>): Record<(typeof ACTION_OBJECTS)[number], Map<string, { name: string | null; ownerId: string | null }>> {
  const opportunities = extracts.get(opportunitiesExtractor.id) as OpportunityExtract | undefined;
  const records = extracts.get(actionRecordsExtractor.id) as ActionRecords | undefined;
  const duplicates = extracts.get(duplicateAccountsExtractor.id) as DuplicateAccounts | undefined;
  const cpq = extracts.get(cpqQuotesExtractor.id) as CpqQuotes | undefined;
  const users = ok(extracts.get(usersExtractor.id) as Sourced<UserRow[]> | undefined) ?? [];
  const names = opportunities?.names ?? {};
  const named = (rows: Array<{ Id: string; Name: string; OwnerId: string | null }>) => rows.map((r): [string, { name: string | null; ownerId: string | null }] => [r.Id, { name: r.Name, ownerId: r.OwnerId }]);
  return {
    Opportunity: new Map((ok(opportunities?.opportunities) ?? []).map((o) => [o.Id, { name: names[o.Id] ?? null, ownerId: o.OwnerId }])),
    Lead: new Map(named(ok(records?.staleLeads) ?? [])),
    Contact: new Map([...named(ok(records?.contactsNoAccount) ?? []), ...named(ok(records?.contactsNoEmail) ?? [])]),
    Account: new Map(named(ok(duplicates?.accounts) ?? [])),
    SBQQ__Quote__c: new Map(named(ok(cpq?.quotes) ?? [])),
    Contract: new Map((ok(cpq?.contracts) ?? []).map((c) => [c.Id, { name: c.ContractNumber, ownerId: c.OwnerId }])),
    User: new Map(users.map((u) => [u.Id, { name: u.Name, ownerId: null }])),
  };
}

export function buildActionLists(records: FindingRecords[], extracts: Map<string, unknown>, instanceUrl: string): ActionLists {
  const known = directory(extracts);
  const lists = records.map(({ findingId, ruleId, object, fix, part, rows }): ActionList => ({
    findingId,
    ruleId,
    object,
    fix,
    ...(part ? { part } : {}),
    rows: rows.map((r) => ({ id: r.id, name: known[object].get(r.id)?.name ?? null, ownerId: known[object].get(r.id)?.ownerId ?? null, problem: r.problem })),
  }));
  const wanted = new Set(lists.flatMap((l) => l.rows.flatMap((r) => (l.object === "User" ? [r.id, r.ownerId] : [r.ownerId]))).filter((id): id is string => Boolean(id)));
  const users = ok(extracts.get(usersExtractor.id) as Sourced<UserRow[]> | undefined) ?? [];
  return ActionLists.parse({
    instanceUrl,
    lists,
    users: Object.fromEntries(users.filter((u) => wanted.has(u.Id)).map((u) => [u.Id, { name: u.Name, role: u.UserRole?.Name ?? null, profile: u.Profile?.Name ?? null }])),
  });
}
