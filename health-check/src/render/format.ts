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
  if (amount >= 1e6) return `${symbol}${Number((amount / 1e6).toPrecision(3))}m`;
  if (amount >= 1e3) return `${symbol}${Number((amount / 1e3).toPrecision(3))}k`;
  return `${symbol}${Math.round(amount)}`;
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
