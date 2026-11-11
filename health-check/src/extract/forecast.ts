import { FIELD_SPECS, planFields } from "./fields.ts";
import { readKeyset } from "./opportunities.ts";
import { monthsBefore, readSoql, soqlDate, soqlDateTime, soqlUpTo, unavailable, type Extractor, type SourceInfo, type Sourced } from "./types.ts";

export type ForecastTypeRow = { Id: string; DeveloperName: string; IsActive: boolean; IsAmount: boolean };
export type QuotaRow = { QuotaOwnerId: string; QuotaAmount: number | null; StartDate: string; ForecastingTypeId: string | null };
export type QuotaData = { types: Sourced<ForecastTypeRow[]>; quotas: Sourced<QuotaRow[]> };

const QUOTA_MONTHS_BACK = 15;

// INVALID_TYPE here means Forecasting isn't turned on, which arrives as
// `unavailable` and becomes not measurable. It's never read as a zero.
export const quotasExtractor: Extractor<QuotaData> = {
  id: "forecast-quotas",
  version: 1,
  async run(ctx) {
    return {
      types: await readSoql(ctx, "SELECT Id, DeveloperName, IsActive, IsAmount FROM ForecastingType"),
      quotas: await readSoql(ctx, `SELECT QuotaOwnerId, QuotaAmount, StartDate, ForecastingTypeId FROM ForecastingQuota WHERE StartDate >= ${soqlDate(monthsBefore(ctx.now, QUOTA_MONTHS_BACK))}`),
    };
  },
};

/** The one revenue forecast type to read, or why there isn't one. Adding two types together would double count. */
export function revenueForecastType(types: ForecastTypeRow[]): { id: string } | { reason: string } {
  const amount = types.filter((t) => t.IsActive && t.IsAmount);
  if (amount.length === 1) return { id: amount[0]!.Id };
  if (amount.length === 0) return { reason: "No revenue forecast type is active, so Forecasting isn't in use for revenue." };
  const standard = amount.find((t) => t.DeveloperName === "OpportunityRevenue");
  return standard ? { id: standard.Id } : { reason: `${amount.length} revenue forecast types are active, and none is the standard one, so we can't tell which to read.` };
}

export type PeriodRow = { Id: string; Type: string; StartDate: string; EndDate: string };
export type ForecastItemRow = {
  OwnerId: string;
  PeriodId: string;
  ForecastingTypeId: string;
  ForecastCategoryName: string | null;
  ForecastAmount: number | null;
  /** Absent when the org doesn't have the field: see `missingFields` on the source. */
  AmountWithoutAdjustments?: number | null;
  HasAdjustment?: boolean;
};
export type ForecastFactRow = { OpportunityId: string; ForecastCategoryName: string | null; PeriodId: string };
export type ForecastData = {
  periods: Sourced<PeriodRow[]>;
  items: Sourced<ForecastItemRow[]>;
  facts: Sourced<ForecastFactRow[]>;
  /**
   * Deals closed now and changed since the run's start, read after the facts
   * (DECISIONS.md 466). A deal the deals read holds as open that is here closed
   * after the run's date, so its fact gives the category it has now, not the
   * one it had then. Ids only.
   */
  closedSinceTheRun: Sourced<string[]>;
};

/**
 * The most forecast rows read: the periods holding today only, never history.
 * A 500-user org's quarter holds about 31,000 open deals, so it's read in full
 * (DECISIONS.md 383). More is a part, and the checks say so.
 */
export const FORECAST_MAX_ROWS = 100_000;

// Only the periods holding today: these objects show the forecast as it stands,
// so an older period says nothing about what was forecast back then.
export const forecastExtractor: Extractor<ForecastData> = {
  id: "forecast",
  // 4: the deals closed since the run's date are read beside the facts, so the
  // Commit figures can leave out a deal whose category then can't be read
  // (DECISIONS.md 466). A version 3 checkpoint has no such list, and its facts
  // may have been read after a deal open at the run's date had closed.
  version: 4,
  partialKeys: ["closed-since-the-run"],
  async run(ctx) {
    const today = soqlDate(ctx.now);
    const periods = await readSoql<PeriodRow>(ctx, `SELECT Id, Type, StartDate, EndDate FROM Period WHERE Type IN ('Month', 'Quarter') AND StartDate <= ${today} AND EndDate >= ${today}`);
    const ids = periods.status === "ok" ? periods.data.map((p) => `'${p.Id}'`).join(", ") : "";
    const inPeriods = ids ? `PeriodId IN (${ids})` : "";
    const plan = planFields(ctx, FIELD_SPECS.forecastingItem);
    const itemSoql = `SELECT ${plan.ok ? plan.select : FIELD_SPECS.forecastingItem.required.join(", ")} FROM ForecastingItem`;
    const factSoql = "SELECT OpportunityId, ForecastCategoryName, PeriodId FROM ForecastingFact";
    // Closing a deal changes it, so one closed after the run's date, whenever,
    // is closed now and changed since its start. Read after the facts, so a deal
    // closed while they were read is here too.
    const closedWhere = `IsClosed = true AND LastModifiedDate >= ${soqlDateTime(ctx.now)} AND ${soqlUpTo("CreatedDate", ctx.now)}`;
    const closedSource: SourceInfo = { kind: "soql", api: "rest", query: `SELECT Id FROM Opportunity WHERE ${closedWhere}` };
    if (!ids) {
      const reason = periods.status === "ok" ? "Salesforce returned no period holding today." : `Periods couldn't be read: ${periods.reason}`;
      return {
        periods,
        items: { status: "unavailable", reason, source: { kind: "soql", api: "rest", query: itemSoql } },
        facts: { status: "unavailable", reason, source: { kind: "soql", api: "rest", query: factSoql } },
        closedSinceTheRun: { status: "unavailable", reason, source: closedSource },
      };
    }
    const items: Sourced<ForecastItemRow[]> = plan.ok
      ? await readSoql(ctx, `${itemSoql} WHERE ${inPeriods}`, { maxRecords: FORECAST_MAX_ROWS, missingFields: plan.missing })
      : { status: "unavailable", reason: plan.reason, source: { kind: "soql", api: "rest", query: `${itemSoql} WHERE ${inPeriods}` } };
    const facts = await readSoql<ForecastFactRow>(ctx, `${factSoql} WHERE ${inPeriods}`, { maxRecords: FORECAST_MAX_ROWS });
    let closedSinceTheRun: Sourced<string[]>;
    try {
      const rows = await readKeyset<{ Id: string }>(ctx, "closed-since-the-run", "Id", "Opportunity", closedWhere);
      closedSinceTheRun = { status: "ok", data: rows.map((r) => r.Id), source: closedSource, truncated: false };
    } catch (err) {
      closedSinceTheRun = unavailable(closedSource, err);
    }
    return { periods, items, facts, closedSinceTheRun };
  },
};
