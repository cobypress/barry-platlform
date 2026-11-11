import { z } from "zod";
import { schemaExtractor } from "./configuration.ts";
import { daysBefore, readSoql, soqlDate, soqlDateTime, soqlUpTo, unavailable, upToItsSecond, type ExtractContext, type Extractor, type Sourced, type SourceInfo } from "./types.ts";

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

/**
 * The most rows a capped read takes: activity, leads and their history,
 * campaign members, won accounts and prior wins. A 500-user org has about
 * 700,000 tasks in a year and 300,000 leads, so it's read in full; an org
 * with more says it's a part (DECISIONS.md 381). At 1,000,000 rows, the run
 * still fits the worker's 1,536 MB heap (decision 347).
 */
export const RECORD_READ_CAP = 1_000_000;

/** A keyset read with a ceiling on rows. `truncated` means a part, and keyset order means a biased part. */
export async function readCapped<R extends { Id: string }>(ctx: ExtractContext, key: string, select: string, from: string, where: string): Promise<Sourced<R[]>> {
  const source: SourceInfo = { kind: "soql", api: "rest", query: `SELECT ${select} FROM ${from} WHERE ${where}` };
  try {
    const rows = await readKeyset<R>(ctx, key, select, from, where, RECORD_READ_CAP);
    return { status: "ok", data: rows, source, truncated: rows.length >= RECORD_READ_CAP };
  } catch (err) {
    return unavailable(source, err);
  }
}

export type OpportunityExtract = {
  opportunities: Sourced<OpportunityRow[]>;
  /**
   * Each deal's name, by id, for the action lists only (roadmap Stage 4). Kept
   * apart from the rows, so no rule reads it: a deal's name often holds the
   * customer's, and rule 7 keeps names out of findings. Undefined in a
   * checkpoint from before it was read.
   */
  names?: Record<string, string>;
  history: Sourced<HistoryRow[]>;
  multiCurrency: boolean;
  lossReasonField: string | null;
  windowFrom: string;
  /**
   * What happened after the run's date (DECISIONS.md 465). `madeSince` is how
   * many deals made since were on pages kept from a read without the end, and
   * left out. `woundBack` and `asRead` are the deals with a history row made
   * since: how many were wound back to how they stood at the run's date, and
   * how many couldn't be and are as read. Those two are undefined where the
   * history couldn't be read, so nothing is known, and any of them in a
   * checkpoint from before it was counted.
   */
  sinceTheRun?: { woundBack?: number; asRead?: number; madeSince?: number };
  /**
   * Deals wound back to open that are closed as read, so the next-steps read,
   * which lists open deals as they stand, can't have listed them: their next
   * step at the run's date can't be read (DECISIONS.md 465). Ids only.
   * Undefined in a checkpoint from before it was kept.
   */
  closedSince?: string[];
};

/** By time, then id, as the pipeline sorts each deal's history (historic-pipeline/cohort.ts). */
function byTimeThenId(a: HistoryRow, b: HistoryRow): number {
  const [x, y] = [`${a.CreatedDate}|${a.Id}`, `${b.CreatedDate}|${b.Id}`];
  return x < y ? -1 : x > y ? 1 : 0;
}

export type AsAtTheRun = {
  opportunities: OpportunityRow[];
  history: HistoryRow[];
  names?: Record<string, string>;
  woundBack: number;
  asRead: number;
  madeSince: number;
  closedSince: string[];
};

/**
 * Each deal as it stood at the run's date (DECISIONS.md 465). A deal made
 * since is dropped, with its history and its name: the scope leaves it out,
 * but a page kept from a read without the end can hold one. A deal changed
 * since is read as it is now, beside a history that shows the change. A
 * history row made after the end is dropped, and a deal that had one takes its
 * stage, amount and close date from its last row at or before the end, and
 * IsClosed and IsWon from that stage. A deal with no later row is left exactly
 * as read, so a run where nothing has happened since gives what it always did.
 * One that can't be wound back (no row by the end, or a stage then that the
 * stages read doesn't hold, or no stages read) is kept as read, with all its
 * history, and counted. `openStages` are the calibration's, which the
 * next-steps read lists as open beside IsClosed false.
 */
export function asAtTheRun(
  opportunities: OpportunityRow[],
  history: HistoryRow[],
  stages: Sourced<StageRow[]>,
  now: Date,
  options: { names?: Record<string, string>; openStages?: ReadonlySet<string> } = {},
): AsAtTheRun {
  const end = upToItsSecond(now).getTime();
  // A CreatedDate that doesn't parse compares false, so the deal is kept as read.
  const made = new Set(opportunities.filter((o) => Date.parse(o.CreatedDate) > end).map((o) => o.Id));
  const cut = made.size === 0
    ? { opportunities, history, names: options.names }
    : {
        opportunities: opportunities.filter((o) => !made.has(o.Id)),
        history: history.filter((h) => !made.has(h.OpportunityId)),
        names: options.names && Object.fromEntries(Object.entries(options.names).filter(([id]) => !made.has(id))),
      };
  const later = (h: HistoryRow): boolean => Date.parse(h.CreatedDate) > end;
  const changed = new Set(cut.history.filter(later).map((h) => h.OpportunityId));
  if (changed.size === 0) return { ...cut, woundBack: 0, asRead: 0, madeSince: made.size, closedSince: [] };

  const lastByTheEnd = new Map<string, HistoryRow>();
  for (const h of cut.history) {
    if (!changed.has(h.OpportunityId) || later(h)) continue;
    const last = lastByTheEnd.get(h.OpportunityId);
    if (!last || byTimeThenId(last, h) < 0) lastByTheEnd.set(h.OpportunityId, h);
  }
  const byStage = new Map(stages.status === "ok" ? stages.data.map((s) => [s.ApiName, s]) : []);
  // The next-steps read's own test of open, so a deal it lists now is never in closedSince.
  const listedAsOpen = (stageName: string, isClosed: boolean): boolean => !isClosed || (options.openStages?.has(stageName) ?? false);
  const asRead = new Set<string>();
  const closedSince: string[] = [];
  let woundBack = 0;
  const deals = cut.opportunities.map((o) => {
    if (!changed.has(o.Id)) return o;
    const last = lastByTheEnd.get(o.Id);
    const stage = last ? byStage.get(last.StageName) : undefined;
    if (!last || !stage) {
      asRead.add(o.Id);
      return o;
    }
    woundBack++;
    if (listedAsOpen(last.StageName, stage.IsClosed) && !listedAsOpen(o.StageName, o.IsClosed)) closedSince.push(o.Id);
    return { ...o, StageName: last.StageName, Amount: last.Amount, CloseDate: last.CloseDate, IsClosed: stage.IsClosed, IsWon: stage.IsWon };
  });
  return { ...cut, opportunities: deals, history: cut.history.filter((h) => !later(h) || asRead.has(h.OpportunityId)), woundBack, asRead: asRead.size, madeSince: made.size, closedSince };
}

const openStageNames = (ctx: ExtractContext): string[] => ctx.calibration.stages.filter((s) => s.category === "open").map((s) => s.name);

function calibratedOpenStages(ctx: ExtractContext): string {
  const open = openStageNames(ctx).map((name) => `'${name.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`);
  return open.length > 0 ? ` OR StageName IN (${open.join(", ")})` : "";
}

function scopeClause(ctx: ExtractContext): string {
  const created = soqlDateTime(ctx.windowStart);
  const closed = soqlDate(ctx.windowStart);
  // LastModifiedDate catches a deal closed in the window with an old CloseDate,
  // such as a bulk tidy-up. The stage list keeps "open" in the calibration's
  // hands, because IsClosed is Salesforce's opinion, not the customer's. The
  // end leaves out a deal made after the run's date (DECISIONS.md 465); one
  // changed since is wound back from its history (asAtTheRun).
  return `(CreatedDate >= ${created} OR CloseDate >= ${closed} OR LastModifiedDate >= ${created} OR IsClosed = false${calibratedOpenStages(ctx)}) AND ${soqlUpTo("CreatedDate", ctx.now)}`;
}

export const opportunitiesExtractor: Extractor<OpportunityExtract> = {
  id: "opportunities",
  // 5: a checkpoint from 4 may hold deals read late as they are now, so a run
  // in flight reads the step again, as it stood at the run's date (DECISIONS.md
  // 465). Pages kept part-way through a read are resumed from, and asAtTheRun
  // cuts any deal made since from them.
  version: 5,
  records: true,
  partialKeys: ["opportunities", "opportunity-history"],
  async run(ctx) {
    const schema = ctx.get(schemaExtractor).Opportunity!;
    // What a stage wound back to means for IsClosed and IsWon. Asked for on
    // every run, so a module that doesn't list it first fails on clean data too.
    const stages = ctx.get(stagesExtractor);
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
      "Id, Name, AccountId, OwnerId, StageName, Amount, CloseDate, CreatedDate, IsClosed, IsWon, LastActivityDate",
      ...["Type", "LeadSource", "CurrencyIsoCode"].filter((f) => available.has(f)),
      ...(available.has("RecordTypeId") ? ["RecordType.DeveloperName"] : []),
      ...(lossReasonField ? [lossReasonField] : []),
    ].join(", ");

    const opportunitySource: SourceInfo = { kind: "soql", api: "rest", query: `SELECT ${fields} FROM Opportunity WHERE ${scope}` };
    const base = { multiCurrency: available.has("CurrencyIsoCode"), lossReasonField, windowFrom };
    let opportunities: OpportunityRow[];
    const names: Record<string, string> = {};
    try {
      const raw = await readKeyset<{ Id: string } & Record<string, any>>(ctx, "opportunities", fields, "Opportunity", scope);
      for (const r of raw) if (typeof r.Name === "string") names[r.Id] = r.Name;
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

    const openStages = new Set(openStageNames(ctx));
    let history: HistoryRow[];
    try {
      const raw = await readKeyset<HistoryRow>(
        ctx,
        "opportunity-history",
        historyFields,
        "OpportunityHistory",
        historyWhere,
      );
      history = raw.map((h) => HistoryRow.parse(h));
    } catch (err) {
      // With no history, nothing can be wound back, so every deal is as read.
      // A deal made since is still left out: it needs only its own date.
      const cut = asAtTheRun(opportunities, [], stages, ctx.now, { names, openStages });
      return {
        ...base,
        names: cut.names,
        opportunities: { status: "ok", source: opportunitySource, truncated: false, data: cut.opportunities },
        history: unavailable(historySource, err),
        sinceTheRun: { madeSince: cut.madeSince },
      };
    }
    // The history read keeps no end of its own: a row made since is how a deal
    // that changed since is found, and it's dropped here (DECISIONS.md 465).
    // A deal made since can still be on a page kept from before the read had
    // its end, so it's dropped here too.
    const atTheRun = asAtTheRun(opportunities, history, stages, ctx.now, { names, openStages });
    return {
      ...base,
      names: atTheRun.names,
      opportunities: { status: "ok", source: opportunitySource, truncated: false, data: atTheRun.opportunities },
      history: { status: "ok", data: atTheRun.history, source: historySource, truncated: false },
      sinceTheRun: { woundBack: atTheRun.woundBack, asRead: atTheRun.asRead, madeSince: atTheRun.madeSince },
      closedSince: atTheRun.closedSince,
    };
  },
};

// NextStep is free text, so the rule only ever needs to know whether it's filled.
// Filtering means the text is never selected, so it can't reach a checkpoint.
// It ends where the deals do (DECISIONS.md 465), as cur.missing-next-steps sets
// the two against each other. It reads next steps as they stand: a deal closed
// since isn't listed, though the deals read winds it back to open, so the deals
// read names it in closedSince and the rule leaves it out.
export const nextStepsExtractor: Extractor<Sourced<string[]>> = {
  id: "next-steps",
  version: 1,
  partialKeys: ["next-steps"],
  async run(ctx) {
    const where = `(IsClosed = false${calibratedOpenStages(ctx)}) AND NextStep != null AND ${soqlUpTo("CreatedDate", ctx.now)}`;
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

/** Open leads nobody has touched: counted for `dq.stale-leads`, and listed for its action list with the same filter. */
export function staleLeadFilter(now: Date): { where: string; before: Date } {
  const before = daysBefore(now, STALE_LEAD_DAYS);
  return { where: `IsConverted = false AND LastActivityDate = null AND CreatedDate < ${soqlDateTime(before)}`, before };
}

export const crmHealthExtractor: Extractor<CrmHealth> = {
  id: "crm-health",
  version: 1,
  async run(ctx) {
    const { where: stale, before: staleBefore } = staleLeadFilter(ctx.now);
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
  version: 2,
  partialKeys: ["won-accounts"],
  run: (ctx) => readCapped<WonAccountRow>(ctx, "won-accounts", "Id, OwnerId, LastActivityDate", "Account", "Id IN (SELECT AccountId FROM Opportunity WHERE IsWon = true)"),
};
