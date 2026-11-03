import { readKeyset } from "./opportunities.ts";
import { soqlDate, unavailable, type Extractor, type Sourced, type SourceInfo } from "./types.ts";

export type PriorWinRow = { Id: string; AccountId: string; CloseDate: string };

// Wins that closed before the analysis window, so a first win inside it can be
// told apart from a repeat. Ids, accounts and dates only. Won means Salesforce's
// flag or a stage the calibration calls won, so either opinion counts.
export const priorWinsExtractor: Extractor<Sourced<PriorWinRow[]>> = {
  id: "prior-wins",
  version: 1,
  partialKeys: ["prior-wins"],
  async run(ctx) {
    const won = ctx.calibration.stages.filter((s) => s.category === "won").map((s) => `'${s.name.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`);
    const where = `AccountId != null AND (IsWon = true${won.length ? ` OR StageName IN (${won.join(", ")})` : ""}) AND CloseDate < ${soqlDate(ctx.windowStart)}`;
    const source: SourceInfo = { kind: "soql", api: "rest", query: `SELECT Id, AccountId, CloseDate FROM Opportunity WHERE ${where}` };
    try {
      const rows = await readKeyset<PriorWinRow>(ctx, "prior-wins", "Id, AccountId, CloseDate", "Opportunity", where);
      return { status: "ok", data: rows.map((r) => ({ Id: r.Id, AccountId: r.AccountId, CloseDate: r.CloseDate })), source, truncated: false };
    } catch (err) {
      return unavailable(source, err);
    }
  },
};
