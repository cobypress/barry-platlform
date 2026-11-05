import { z } from "zod";
import { schemaExtractor } from "./configuration.ts";
import { daysBefore, readSoql, soqlDate, soqlDateTime, unavailable, type ExtractContext, type Extractor, type Sourced, type SourceInfo } from "./types.ts";

export type StageRow = { ApiName: string; MasterLabel: string; IsActive: boolean; IsClosed: boolean; IsWon: boolean; SortOrder: number };

export const stagesExtractor: Extractor<Sourced<StageRow[]>> = {
  id: "opportunity-stages",
  version: 1,
  run: (ctx) => readSoql<StageRow>(ctx, "SELECT ApiName, MasterLabel, IsActive, IsClosed, IsWon, SortOrder FROM OpportunityStage"),
};

// Every pipeline number comes from these two rows, so they're validated on the
// way in. A shape we don't expect should stop the run, not skew a win rate.
export const OpportunityRow = z.object({
  Id: z.string(),
  OwnerId: z.string(),
  StageName: z.string(),
  Amount: z.number().nullable(),
  CloseDate: z.string(),
  CreatedDate: z.string(),
  IsClosed: z.boolean(),
  IsWon: z.boolean(),
  LastActivityDate: z.string().nullable(),
  Type: z.string().nullable(),
  LeadSource: z.string().nullable(),
  RecordType: z.string().nullable(),
  CurrencyIsoCode: z.string().nullable(),
  LossReason: z.string().nullable(),
  AccountId: z.string().nullable().optional(),
});
export type OpportunityRow = z.infer<typeof OpportunityRow>;

export const HistoryRow = z.object({
  Id: z.string(),
  OpportunityId: z.string(),
  StageName: z.string(),
  Amount: z.number().nullable(),
  CloseDate: z.string(),
  CreatedDate: z.string(),
});
export type HistoryRow = z.infer<typeof HistoryRow>;

/** Reads `ORDER BY Id` a page at a time, checkpointing each page, so a stopped run resumes at the last Id. Stops at `maxRows` if given. */
export async function readKeyset<R extends { Id: string }>(ctx: ExtractContext, key: string, select: string, from: string, where: string, maxRows = Infinity): Promise<R[]> {
  const rows = ctx.partial.read<R>(key);
  for (;;) {
    const after = rows.at(-1)?.Id;
    const conditions = [where, after ? `Id > '${after}'` : ""].filter(Boolean).join(" AND ");
    const soql = `SELECT ${select} FROM ${from}${conditions ? ` WHERE ${conditions}` : ""} ORDER BY Id LIMIT ${ctx.pageSize}`;
    const page = await ctx.client.queryAll<R>(soql);
    rows.push(...page);
    ctx.partial.append(key, page);
    if (page.length < ctx.pageSize || rows.length >= maxRows) return rows;
  }
}

const MAX_ROWS = 200_000;

/** A keyset read with a ceiling on rows. `truncated` means a part, and keyset order means a biased part. */
export async function readCapped<R extends { Id: string }>(ctx: ExtractContext, key: string, select: string, from: string, where: string): Promise<Sourced<R[]>> {
  const source: SourceInfo = { kind: "soql", api: "rest", query: `SELECT ${select} FROM ${from} WHERE ${where}` };
  try {
    const rows = await readKeyset<R>(ctx, key, select, from, where, MAX_ROWS);
    return { status: "ok", data: rows, source, truncated: rows.length >= MAX_ROWS };
  } catch (err) {
    return unavailable(source, err);
  }
}

export type OpportunityExtract = {
  opportunities: Sourced<OpportunityRow[]>;
  history: Sourced<HistoryRow[]>;
  multiCurrency: boolean;
  lossReasonField: string | null;
  windowFrom: string;
};

function calibratedOpenStages(ctx: ExtractContext): string {
  const open = ctx.calibration.stages.filter((s) => s.category === "open").map((s) => `'${s.name.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`);
  return open.length > 0 ? ` OR StageName IN (${open.join(", ")})` : "";
}

function scopeClause(ctx: ExtractContext): string {
  const created = soqlDateTime(ctx.windowStart);
  const closed = soqlDate(ctx.windowStart);
  // LastModifiedDate catches a deal closed in the window with an old CloseDate,
  // such as a bulk tidy-up. The stage list keeps "open" in the calibration's
  // hands, because IsClosed is Salesforce's opinion, not the customer's.
  return `(CreatedDate >= ${created} OR CloseDate >= ${closed} OR LastModifiedDate >= ${created} OR IsClosed = false${calibratedOpenStages(ctx)})`;
}

export const opportunitiesExtractor: Extractor<OpportunityExtract> = {
  id: "opportunities",
  version: 3,
  records: true,
  partialKeys: ["opportunities", "opportunity-history"],
  async run(ctx) {
    const schema = ctx.get(schemaExtractor).Opportunity!;
    const windowFrom = soqlDate(ctx.windowStart);
    const scope = scopeClause(ctx);
    const historyFields = "Id, OpportunityId, StageName, Amount, CloseDate, CreatedDate";
    const historyWhere = `OpportunityId IN (SELECT Id FROM Opportunity WHERE ${scope})`;
    const historySource: SourceInfo = { kind: "soql", api: "rest", query: `SELECT ${historyFields} FROM OpportunityHistory WHERE ${historyWhere}` };
    if (schema.status !== "ok") {
      const source: SourceInfo = { kind: "soql", api: "rest", query: `SELECT Id FROM Opportunity WHERE ${scope}` };
      const reason = `Opportunity couldn't be described: ${schema.reason}`;
      return {
        opportunities: { status: "unavailable", reason, source },
        history: { status: "unavailable", reason, source: historySource },
        multiCurrency: false,
        lossReasonField: null,
        windowFrom,
      };
    }

    // Some of these fields only exist in some orgs, so ask only for what's there.
    const available = new Set(schema.data.fields.map((f) => f.name));
    const lossField = ctx.calibration.fields.lossReason;
    const lossReasonField = lossField && available.has(lossField) ? lossField : null;
    const fields = [
      "Id, AccountId, OwnerId, StageName, Amount, CloseDate, CreatedDate, IsClosed, IsWon, LastActivityDate",
      ...["Type", "LeadSource", "CurrencyIsoCode"].filter((f) => available.has(f)),
      ...(available.has("RecordTypeId") ? ["RecordType.DeveloperName"] : []),
      ...(lossReasonField ? [lossReasonField] : []),
    ].join(", ");

    const opportunitySource: SourceInfo = { kind: "soql", api: "rest", query: `SELECT ${fields} FROM Opportunity WHERE ${scope}` };
    const base = { multiCurrency: available.has("CurrencyIsoCode"), lossReasonField, windowFrom };
    let opportunities: OpportunityRow[];
    try {
      const raw = await readKeyset<{ Id: string } & Record<string, any>>(ctx, "opportunities", fields, "Opportunity", scope);
      opportunities = raw.map((r) =>
        OpportunityRow.parse({
          Id: r.Id,
          AccountId: r.AccountId ?? null,
          OwnerId: r.OwnerId,
          StageName: r.StageName,
          Amount: r.Amount ?? null,
          CloseDate: r.CloseDate,
          CreatedDate: r.CreatedDate,
          IsClosed: r.IsClosed,
          IsWon: r.IsWon,
          LastActivityDate: r.LastActivityDate ?? null,
          Type: r.Type ?? null,
          LeadSource: r.LeadSource ?? null,
          RecordType: r.RecordType?.DeveloperName ?? null,
          CurrencyIsoCode: r.CurrencyIsoCode ?? null,
          LossReason: lossReasonField ? (r[lossReasonField] ?? null) : null,
        }),
      );
    } catch (err) {
      const failed = unavailable<OpportunityRow[]>(opportunitySource, err);
      return { ...base, opportunities: failed, history: { status: "unavailable", reason: "Opportunity couldn't be read", source: historySource } };
    }

    const ok = { status: "ok", source: opportunitySource, truncated: false, data: opportunities } as const;
    try {
      const history = await readKeyset<HistoryRow>(
        ctx,
        "opportunity-history",
        historyFields,
        "OpportunityHistory",
        historyWhere,
      );
      return { ...base, opportunities: ok, history: { status: "ok", data: history.map((h) => HistoryRow.parse(h)), source: historySource, truncated: false } };
    } catch (err) {
      return { ...base, opportunities: ok, history: unavailable(historySource, err) };
    }
  },
};

// NextStep is free text, so the rule only ever needs to know whether it's filled.
// Filtering means the text is never selected, so it can't reach a checkpoint.
export const nextStepsExtractor: Extractor<Sourced<string[]>> = {
  id: "next-steps",
  version: 1,
  partialKeys: ["next-steps"],
  async run(ctx) {
    const where = `(IsClosed = false${calibratedOpenStages(ctx)}) AND NextStep != null`;
    const source: SourceInfo = { kind: "soql", api: "rest", query: `SELECT Id FROM Opportunity WHERE ${where}` };
    try {
      const rows = await readKeyset<{ Id: string }>(ctx, "next-steps", "Id", "Opportunity", where);
      return { status: "ok", data: rows.map((r) => r.Id), source, truncated: false };
    } catch (err) {
      return unavailable(source, err);
    }
  },
};

export type CrmHealth = {
  counts: Record<string, Sourced<number>>;
  staleLeadsBySource: Sourced<Array<{ s: string | null; c: number }>>;
  openLeadsBySource: Sourced<Array<{ s: string | null; c: number }>>;
  staleBefore: string;
};

const STALE_LEAD_DAYS = 90;

export const crmHealthExtractor: Extractor<CrmHealth> = {
  id: "crm-health",
  version: 1,
  async run(ctx) {
    const staleBefore = daysBefore(ctx.now, STALE_LEAD_DAYS);
    const stale = `IsConverted = false AND LastActivityDate = null AND CreatedDate < ${soqlDateTime(staleBefore)}`;
    const queries: Record<string, string> = {
      openLeads: "SELECT COUNT() FROM Lead WHERE IsConverted = false",
      staleLeads: `SELECT COUNT() FROM Lead WHERE ${stale}`,
      contacts: "SELECT COUNT() FROM Contact",
      contactsNoAccount: "SELECT COUNT() FROM Contact WHERE AccountId = null",
      contactsNoEmail: "SELECT COUNT() FROM Contact WHERE Email = null",
    };
    const keys = Object.keys(queries);
    const results = await ctx.client.queryBatch(keys.map((k) => queries[k]!));
    const counts: Record<string, Sourced<number>> = {};
    keys.forEach((key, i) => {
      const result = results[i]!;
      const source: SourceInfo = { kind: "soql", api: "rest", query: queries[key]! };
      counts[key] = result.ok
        ? { status: "ok", data: result.result.totalSize, source, truncated: false }
        : { status: "unavailable", reason: result.error.message, source };
    });
    return {
      counts,
      staleLeadsBySource: await readSoql(ctx, `SELECT LeadSource s, COUNT(Id) c FROM Lead WHERE ${stale} GROUP BY LeadSource`),
      openLeadsBySource: await readSoql(ctx, "SELECT LeadSource s, COUNT(Id) c FROM Lead WHERE IsConverted = false GROUP BY LeadSource"),
      staleBefore: soqlDate(staleBefore),
    };
  },
};

export type WonAccountRow = { Id: string; OwnerId: string; LastActivityDate: string | null };

// Only accounts that have bought. The name isn't read, and no rule needs it.
export const wonAccountsExtractor: Extractor<Sourced<WonAccountRow[]>> = {
  id: "won-accounts",
  version: 1,
  partialKeys: ["won-accounts"],
  run: (ctx) => readCapped<WonAccountRow>(ctx, "won-accounts", "Id, OwnerId, LastActivityDate", "Account", "Id IN (SELECT AccountId FROM Opportunity WHERE IsWon = true)"),
};
