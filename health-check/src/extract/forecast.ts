import { monthsBefore, readSoql, soqlDate, type Extractor, type Sourced } from "./types.ts";

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
  AmountWithoutAdjustments: number | null;
  HasAdjustment: boolean;
};
export type ForecastFactRow = { OpportunityId: string; ForecastCategoryName: string | null; PeriodId: string };
export type ForecastData = { periods: Sourced<PeriodRow[]>; items: Sourced<ForecastItemRow[]>; facts: Sourced<ForecastFactRow[]> };

const FORECAST_MAX_ROWS = 20_000;

// Only the periods holding today: these objects show the forecast as it stands,
// so an older period says nothing about what was forecast back then.
export const forecastExtractor: Extractor<ForecastData> = {
  id: "forecast",
  version: 1,
  async run(ctx) {
    const today = soqlDate(ctx.now);
    const periods = await readSoql<PeriodRow>(ctx, `SELECT Id, Type, StartDate, EndDate FROM Period WHERE Type IN ('Month', 'Quarter') AND StartDate <= ${today} AND EndDate >= ${today}`);
    const ids = periods.status === "ok" ? periods.data.map((p) => `'${p.Id}'`).join(", ") : "";
    const inPeriods = ids ? `PeriodId IN (${ids})` : "";
    const itemSoql = `SELECT OwnerId, PeriodId, ForecastingTypeId, ForecastCategoryName, ForecastAmount, AmountWithoutAdjustments, HasAdjustment FROM ForecastingItem`;
    const factSoql = "SELECT OpportunityId, ForecastCategoryName, PeriodId FROM ForecastingFact";
    if (!ids) {
      const reason = periods.status === "ok" ? "Salesforce returned no period holding today." : `Periods couldn't be read: ${periods.reason}`;
      return { periods, items: { status: "unavailable", reason, source: { kind: "soql", api: "rest", query: itemSoql } }, facts: { status: "unavailable", reason, source: { kind: "soql", api: "rest", query: factSoql } } };
    }
    return {
      periods,
      items: await readSoql(ctx, `${itemSoql} WHERE ${inPeriods}`, { maxRecords: FORECAST_MAX_ROWS }),
      facts: await readSoql(ctx, `${factSoql} WHERE ${inPeriods}`, { maxRecords: FORECAST_MAX_ROWS }),
    };
  },
};
