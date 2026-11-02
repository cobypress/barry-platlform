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
