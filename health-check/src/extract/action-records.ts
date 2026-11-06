import { readKeyset, staleLeadFilter } from "./opportunities.ts";
import { unavailable, type ExtractContext, type Extractor, type Sourced, type SourceInfo } from "./types.ts";

// The records behind the checks that only count (stale leads, incomplete
// contacts), listed for the action lists (roadmap Stage 4, DECISIONS.md 193).
// Each list uses the filter its count does, reads at most ACTION_ROWS rows a
// page at a time, resumably, and stops there: past it, the sheet says it's the
// first part and how to see the rest. Emails and other free text are never
// selected, only whether they're there.

/** The most rows one list reads. A larger list is a part, and says so. */
export const ACTION_ROWS = 5_000;

export type NamedRow = { Id: string; Name: string; OwnerId: string | null };
export type LeadActionRow = NamedRow & { LeadSource: string | null; CreatedDate: string };
export type ActionRecords = {
  staleLeads: Sourced<LeadActionRow[]>;
  contactsNoAccount: Sourced<NamedRow[]>;
  contactsNoEmail: Sourced<NamedRow[]>;
};

async function readList<R extends { Id: string }>(ctx: ExtractContext, key: string, select: string, from: string, where: string): Promise<Sourced<R[]>> {
  const source: SourceInfo = { kind: "soql", api: "rest", query: `SELECT ${select} FROM ${from} WHERE ${where}` };
  try {
    const rows = await readKeyset<R>(ctx, key, select, from, where, ACTION_ROWS);
    return { status: "ok", data: rows.slice(0, ACTION_ROWS), source, truncated: rows.length >= ACTION_ROWS };
  } catch (err) {
    return unavailable(source, err);
  }
}

export const actionRecordsExtractor: Extractor<ActionRecords> = {
  id: "action-records",
  version: 1,
  records: true,
  partialKeys: ["action-stale-leads", "action-contacts-no-account", "action-contacts-no-email"],
  async run(ctx) {
    return {
      staleLeads: await readList<LeadActionRow>(ctx, "action-stale-leads", "Id, Name, OwnerId, LeadSource, CreatedDate", "Lead", staleLeadFilter(ctx.now).where),
      contactsNoAccount: await readList<NamedRow>(ctx, "action-contacts-no-account", "Id, Name, OwnerId", "Contact", "AccountId = null"),
      contactsNoEmail: await readList<NamedRow>(ctx, "action-contacts-no-email", "Id, Name, OwnerId", "Contact", "Email = null"),
    };
  },
};
