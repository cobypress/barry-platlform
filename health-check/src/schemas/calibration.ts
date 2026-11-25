import { z } from "zod";
import { Timestamp } from "./common.ts";

// What a stage means to this customer. "no_decision" is separate from "lost"
// because deals that fizzle out would distort a win rate either way.
export const StageCategory = z.enum(["open", "won", "lost", "no_decision", "excluded"]);
export type StageCategory = z.infer<typeof StageCategory>;

export const CalibratedStage = z.object({
  name: z.string().min(1),
  category: StageCategory,
  // Sales order of open stages. Conversion can't be measured without it.
  order: z.number().int().nonnegative().optional(),
  // The customer says every won deal passes through this open stage (DECISIONS.md 113).
  required: z.boolean().optional(),
  // Where a closed stage's lost or no decision came from on an automatic calibration (DECISIONS.md 756): "customer",
  // they said so in the portal; "name", our rule read it from the stage's name, said as an assumption. Absent: a
  // person's or Salesforce's word. Never checked here, so a stored row always parses: the engine reads it only on a
  // lost or no decision stage of a calibration read from the org's settings.
  from: z.enum(["customer", "name"]).optional(),
});
export type CalibratedStage = z.infer<typeof CalibratedStage>;

export const UserKind = z.enum(["sales_rep", "manager", "admin", "shared", "system", "integration", "test", "other"]);
export type UserKind = z.infer<typeof UserKind>;

/**
 * The currencies a customer may name (KAN-41): real, current ISO 4217 codes,
 * fixed here so the console and the worker agree whatever Node each runs.
 * Source: the hand-checked country map in src/connect/locale-currency.ts
 * (135 codes), plus the real currencies it leaves out on purpose or has no
 * country for: BTN CUP FKP HTG KHR LBP LRD LSL NAD PAB SHP SLE SOS SSP SZL
 * VES XCG ZWG. Left out: withdrawn or replaced codes (ANG, BGN, CUC, HRK,
 * SLL, SVC, ZWL), units of account (XDR, XSU), metals, funds, "no currency"
 * (XXX) and the testing code (XTS). Made 25 November 2026, with no network:
 * a code added later goes here, in the engine, and is copied to the console.
 */
export const CURRENCY_CODES: ReadonlySet<string> = new Set([
  "AED", "AFN", "ALL", "AMD", "AOA", "ARS", "AUD", "AWG", "AZN", "BAM", "BBD", "BDT", "BHD", "BIF", "BMD", "BND",
  "BOB", "BRL", "BSD", "BTN", "BWP", "BYN", "BZD", "CAD", "CDF", "CHF", "CLP", "CNY", "COP", "CRC", "CUP", "CVE",
  "CZK", "DJF", "DKK", "DOP", "DZD", "EGP", "ERN", "ETB", "EUR", "FJD", "FKP", "GBP", "GEL", "GHS", "GIP", "GMD",
  "GNF", "GTQ", "GYD", "HKD", "HNL", "HTG", "HUF", "IDR", "ILS", "INR", "IQD", "IRR", "ISK", "JMD", "JOD", "JPY",
  "KES", "KGS", "KHR", "KMF", "KPW", "KRW", "KWD", "KYD", "KZT", "LAK", "LBP", "LKR", "LRD", "LSL", "LYD", "MAD",
  "MDL", "MGA", "MKD", "MMK", "MNT", "MOP", "MRU", "MUR", "MVR", "MWK", "MXN", "MYR", "MZN", "NAD", "NGN", "NIO",
  "NOK", "NPR", "NZD", "OMR", "PAB", "PEN", "PGK", "PHP", "PKR", "PLN", "PYG", "QAR", "RON", "RSD", "RUB", "RWF",
  "SAR", "SBD", "SCR", "SDG", "SEK", "SGD", "SHP", "SLE", "SOS", "SRD", "SSP", "STN", "SYP", "SZL", "THB", "TJS",
  "TMT", "TND", "TOP", "TRY", "TTD", "TWD", "TZS", "UAH", "UGX", "USD", "UYU", "UZS", "VES", "VND", "VUV", "WST",
  "XAF", "XCD", "XCG", "XOF", "XPF", "YER", "ZAR", "ZMW", "ZWG",
]);
/** One of the list above: where an answer comes in, and where one is read. */
export const isCurrencyCode = (code: unknown): code is string => typeof code === "string" && CURRENCY_CODES.has(code);
/** The shape a stored code must have: three capitals, never "no currency" or the testing code. A code off the list still parses, so a stored answer never breaks a run. */
export const currencyShaped = (code: unknown): code is string => typeof code === "string" && /^[A-Z]{3}$/.test(code) && code !== "XXX" && code !== "XTS";

export const Calibration = z
  .object({
    version: z.literal(1),
    // Optional, but set it: the run refuses a file written for another org.
    orgId: z.string().min(15).max(18).optional(),
    confirmedBy: z.string().min(1),
    confirmedAt: Timestamp,
    // Confirmed: a person agreed it, and confirmedBy names them. Org settings: read
    // from Salesforce's own flags with no person's confirmation, and confirmedBy
    // only says who set it up, so nothing prints it as a confirmation (DECISIONS.md 688).
    source: z.enum(["confirmed", "org_settings"]).default("confirmed"),
    stages: z.array(CalibratedStage).min(1),
    scope: z.object({
      recordTypes: z.union([z.literal("all"), z.array(z.string().min(1)).min(1)]),
      businessLines: z.array(z.string()).default([]),
    }),
    users: z.array(z.object({ userId: z.string().min(15).max(18), kind: UserKind })).default([]),
    fields: z.object({ lossReason: z.string().min(1).optional() }).default({}),
    targets: z.object({
      source: z.enum(["forecasting_quota", "custom_object", "spreadsheet", "none"]),
      note: z.string().optional(),
    }),
    salesCycleAssumptionDays: z.number().positive().optional(),
    // Leave out to use the org's own setting. A silent default of January would
    // give an April-start org the wrong quarters.
    fiscalYearStartMonth: z.number().int().min(1).max(12).optional(),
    analysisWindowMonths: z.number().int().positive().default(24),
    minSample: z.number().int().positive().default(20),
    dormantDays: z.number().int().positive().default(90),
    currency: z.string().length(3).default("GBP"),
    // What an automatic run knows of its currency (DECISIONS.md 700), read only
    // with source org_settings. "org": one currency, not known by name ("XXX",
    // ISO 4217's code for none), its amounts shown as plain numbers. "unread":
    // connect couldn't tell; the run decides from its opportunities. Absent: the
    // code is the org's own, as before 700, or a person's. "customer": the org
    // has one currency, and its customer named it in the portal (KAN-41): a
    // real code, printed as the org's own would be. "connect_api": the org has
    // one currency, and its settings named it through the Connect REST API
    // (DECISIONS.md 740): a real code, printed as the org's own would be.
    currencyFrom: z.enum(["org", "unread", "customer", "connect_api"]).optional(),
    // The currency the org's default locale suggests, for the question the
    // portal asks. Never printed as the currency and never formats an amount.
    currencySuggested: z.string().regex(/^[A-Z]{3}$/).optional(),
    // Annual cost per seat by Salesforce licence name. No entry means no sizing, never a guess.
    licenceCosts: z.record(z.string(), z.number().nonnegative()).default({}),
    // Minutes a year per item, by key, as agreed with the customer. Salesforce has
    // no such figure, so no entry means an hours value isn't sized, never a guess.
    effortMinutes: z.record(z.string(), z.number().positive()).default({}),
    // Service Cloud's limits. Left out, the defaults in SERVICE_DEFAULTS apply,
    // and a Sales Cloud calibration stays as it was (DECISIONS.md 156).
    service: z
      .object({
        backlogAgeDays: z.number().int().positive().optional(),
        originBlankShare: z.number().min(0).max(100).optional(),
        stalenessMonths: z.number().int().positive().optional(),
        // Milestone type names, as the org has them. Their names are the org's own, so we can't tell otherwise.
        firstResponseMilestones: z.array(z.string().min(1)).optional(),
        resolutionMilestones: z.array(z.string().min(1)).optional(),
      })
      .optional(),
  })
  .superRefine((c, ctx) => {
    // "No currency" and the testing code belong only to a calibration read from the org's settings (DECISIONS.md 700):
    // a person's always names its currency, so its amounts are never plain numbers with no line to say why.
    if ((c.currency === "XXX" || c.currency === "XTS") && c.source !== "org_settings") {
      ctx.addIssue({ code: "custom", path: ["currency"], message: "A person's calibration names its currency" });
    }
    // A currency the customer named (KAN-41) is a code's shape, on a calibration read from the org's settings only. Its
    // place on the list is checked where an answer comes in and where one is read, never here: a stored row must parse.
    if (c.currencyFrom === "customer" && (c.source !== "org_settings" || !currencyShaped(c.currency))) {
      ctx.addIssue({ code: "custom", path: ["currencyFrom"], message: "A currency the customer named is a currency code, on a calibration read from the org's settings" });
    }
    // A currency the org's settings named (DECISIONS.md 740): the same, and checked against the list where it is read.
    if (c.currencyFrom === "connect_api" && (c.source !== "org_settings" || !currencyShaped(c.currency))) {
      ctx.addIssue({ code: "custom", path: ["currencyFrom"], message: "A currency the org's settings named is a currency code, on a calibration read from the org's settings" });
    }
    const seen = new Set<string>();
    for (const stage of c.stages) {
      if (seen.has(stage.name)) ctx.addIssue({ code: "custom", path: ["stages"], message: `Stage "${stage.name}" is listed twice` });
      seen.add(stage.name);
    }
    const open = c.stages.filter((s) => s.category === "open");
    if (open.some((s) => s.order === undefined)) {
      ctx.addIssue({ code: "custom", path: ["stages"], message: "Every open stage needs an order" });
    }
    if (new Set(open.map((s) => s.order)).size !== open.length) {
      ctx.addIssue({ code: "custom", path: ["stages"], message: "Open stage orders must be unique" });
    }
  });
export type Calibration = z.infer<typeof Calibration>;

/**
 * Who stands behind the calibration, as the notes and reports print it. Only a
 * confirmed one names a person: one read from the org's settings says so.
 */
export function calibrationConfirmation(c: Pick<Calibration, "source" | "confirmedBy" | "confirmedAt">): string {
  return c.source === "org_settings"
    ? `Calibration read from the org's own settings on ${c.confirmedAt.slice(0, 10)}, with no person's confirmation`
    : `Calibration confirmed by ${c.confirmedBy} on ${c.confirmedAt.slice(0, 10)}`;
}
