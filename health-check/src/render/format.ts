// How figures are written in reports. Each form here has to get past the number
// check, so a format change that loses accuracy shows up as a blocked section.

const SYMBOL: Record<string, string> = { GBP: "£", USD: "$", EUR: "€" };

export const moneySymbol = (currency: string): string => SYMBOL[currency] ?? `${currency} `;

export function money(amount: number, currency: string): string {
  return `${moneySymbol(currency)}${Math.round(amount).toLocaleString("en-GB")}`;
}

/** Keeps three significant figures, so the rounding stays within what the number check allows. */
export function compactMoney(amount: number, currency: string): string {
  const symbol = moneySymbol(currency);
  if (amount >= 1e6) return `${symbol}${threeFigures(amount / 1e6)}m`;
  if (amount >= 1e3) return `${symbol}${threeFigures(amount / 1e3)}k`;
  return `${symbol}${Math.round(amount)}`;
}

/** Half up, as the number check rounds. toPrecision works on the binary value, so 3.255 became 3.25. */
function threeFigures(value: number): number {
  const decimals = Math.max(0, 3 - (Math.floor(Math.log10(value)) + 1));
  const scale = 10 ** decimals;
  return Math.round(value * scale + 1e-9) / scale;
}

export const percent = (value: number): string => `${value}%`;

const MONTH_SHORT = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

export function monthYear(iso: string): string {
  const [year, month] = iso.split("-");
  return `${MONTH_SHORT[Number(month) - 1]} ${year}`;
}

export function longDate(iso: string): string {
  const [year, month, day] = iso.slice(0, 10).split("-");
  return `${Number(day)} ${MONTH_SHORT[Number(month) - 1]} ${year}`;
}

// The whole token only, so a run id such as hc-2026-10-31-ab12cd34 is left alone.
const ISO_DATE = /(?<![\w-])(\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01]))(?:T[\d:.]+(?:Z|[+-]\d{2}:?\d{2})?)?(?![\w-])/g;

/** Engine text holds ISO dates. A reader gets a UK date. Queries must keep theirs, so don't pass them here. */
export const ukDates = (text: string): string => text.replace(ISO_DATE, (_m, iso: string) => longDate(iso));

export function escapeXml(text: string): string {
  return text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#39;");
}

// The checks explain themselves in the words we use to build and test them:
// "the calibrated minimum", "the connected user", "rules", an object's API
// name. A customer reads a consultant's report, so what's printed says the same
// things the way we'd say them to the customer. The figures are untouched.
const PLAIN: Array<[RegExp, string | ((...groups: string[]) => string)]> = [
  [/\b0 ([^.]+?) is below the calibrated minimum of (\d+)/g, "There are no $1 yet, and we need at least $2 for a reliable figure"],
  [/ is below the calibrated minimum of (\d+)/g, " is fewer than the $1 we need for a reliable figure"],
  [/,? under the calibrated minimum of (\d+)/g, ", fewer than the $1 we need"],
  [/Sample below the calibrated minimum(?: \((\d+) of (\d+)\))?/g, "Too few to measure reliably"],
  [/the calibrated minimum of (\d+)/g, "the $1 we need"],
  [/the calibrated minimum/g, "the minimum we need for a reliable figure"],
  [/calibrated as/g, "you told us are"],
  [/the calibration calls/g, "you told us is"],
  [/Stages the calibration doesn't cover/g, "Stages we haven't agreed with you"],
  [/The calibration covers all (\d+) stages seen in the org\./g, "We've agreed with you what all $1 stages in your org mean."],
  [/The calibration disagrees with/g, "Your stage meanings disagree with"],
  [/The calibration agrees with/g, "Your stage meanings agree with"],
  [/The calibration doesn't say what these stages mean/g, "We haven't yet agreed with you what these stages mean"],
  [/The calibration doesn't say whether these stages are open/g, "We haven't yet agreed with you whether these stages are open"],
  [/The calibration doesn't name a loss reason field that exists on Opportunity/g, "We haven't yet agreed with you which field holds the loss reason"],
  [/and the calibration doesn't set one/g, "and we haven't yet agreed one with you"],
  // Engine words a customer would stumble on (night 3 reading pass).
  [/Every guard passed:/g, "Every check passed:"],
  [/until their guards pass/g, "until every check on them passes"],
  [/not every guard passed/g, "not every check on it passed"],
  [/Your Health Check score is (\d+)\./g, "Salesforce's own security tool, Health Check, scores your settings at $1."],
  [/The Health Check score is (\d+),/g, "Salesforce's own security tool, Health Check, scores your settings at $1,"],
  [/\b1 deals affected\b/g, "1 deal affected"],
  [/\bat (\d+(?:\.\d+)?) ([A-Z]{3}) a year\b/g, (_m: string, n: string, code: string) => `at ${money(Number(n), code)} a year`],
  [/\b([Ww])e (don't|never) impute one\b/g, "$1e $2 estimate one"],
  [/ campaign spend to date\b/g, " more spent than won back, to date"],
  [/Quota comes from Collaborative Forecasts, for people who own deals/g, "Quota is the target set in Salesforce's forecasting, for people who own deals"],
  [/Open pipeline in the quarter is taken at full Amount, with no stage weighting/g, "Open deals in the quarter count at their full value, not weighted by stage"],
  // A month as the engine writes it, 2026-02, as a reader writes it.
  [/(?<![\w-])(\d{4})-(0[1-9]|1[0-2])(?![\w-])/g, (_m: string, year: string, month: string) => `${MONTH_SHORT[Number(month) - 1]} ${year}`],
  // Sentences the generic swap below would leave broken (night 3 reading pass).
  [/the ones the calibration marks as required/g, "the ones you told us are required"],
  [/the calibration marks none as required/g, "you told us none is required"],
  [/,? as the calibration marks them/g, ", as you told us"],
  [/(^|,? )agreed in the calibration/g, (_m: string, lead: string) => `${lead === "" ? "" : ", "}as agreed with you`],
  [/The calibration has no price for a seat of: /g, "We have no agreed price for a seat of "],
  [/Marked as an integration in the calibration/g, "Marked by you as integrations"],
  [/no price in the calibration/g, "no price agreed with you"],
  // Anything about the calibration not caught above: it's what we agreed with the customer.
  [/\bThe calibration\b/g, "What we agreed with you"],
  [/\bthe calibration\b/g, "what we agreed with you"],
  [/\bcalibrated /g, ""],
  [/,? or (?:to )?the connected user(?: can't see it)?/g, ""],
  [/(Private reports) the connected user can't see/g, "$1 kept by individual users"],
  [/ForecastingType isn't available in this org\./g, "Forecasting isn't switched on in this org."],
  [/OpportunityHistory has no rows\./g, "There's no stage history for your deals yet."],
  [/Every check in this domain was not measurable \(\d+ rules\)\./g, "We couldn't measure this area."],
  [/No check in this domain produced a result\./g, "We couldn't assess this area."],
  // Wording approved before these were reworded at the source (narrative/sections.ts, domains/scoring.ts).
  [/A domain with no score says why\. We never show a gap as a zero\./g, "Each area of your Salesforce, scored out of 100. Where an area can't be scored fairly, we say why rather than guess."],
  [/across (\d+) domains?\b/g, "across $1 areas of your Salesforce"],
  [/: 0 critical and 0 high\./g, ", none of them critical or high."],
  [/: 0 critical and (\d+) high\./g, ": $1 high, none critical."],
  [/: (\d+) critical and 0 high\./g, ": $1 critical, none high."],
  [/, of which (\d+) (?:are|is) critical and (\d+) (?:are|is) high\./g, (_m, c, h) => (c === "0" && h === "0" ? ", none of them critical or high." : c === "0" ? `: ${h} high, none critical.` : h === "0" ? `: ${c} critical, none high.` : `: ${c} critical and ${h} high.`)],
  [/We didn't assess this domain in this run/g, "We didn't assess this area"],
  [/Too few checks here could be measured to give a fair score\./g, "Too little of this area could be measured to score it fairly."],
  [/admins or calibrated system accounts/g, "admins or system accounts you told us about"],
  // A count of nothing reads as a template filled in. Say there are none instead.
  [/None of the 0 ([^.]+?) (?:is|are|was|were|has|have|hasn't|haven't|run|runs|made|use|uses|contain|contains|updates|update)\b[^.]*\./g, "There are no $1."],
  [/All 0 ([^.]+?) (?:is|are|have|has)\b[^.]*\./g, "There are no $1."],
];

/** Report text as a customer should read it. */
export const plainWords = (text: string): string => PLAIN.reduce((t, [pattern, plain]) => t.replace(pattern, plain as (...groups: string[]) => string), text);
