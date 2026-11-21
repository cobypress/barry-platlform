import { eng, joinSaid, org, said, type Said } from "./org-words.ts";

// The forms rules put figures and lists in, written for `said` (DECISIONS.md
// 657), so a sentence that uses one is still marked. Each prints exactly what
// the plain form it stands beside prints: a test holds each pair equal.

/** A number as en-GB writes it, "17,880.5": digits, commas and a point, worked out by the engine. */
export const grouped = (n: number): Said => eng(n.toLocaleString("en-GB"));

/**
 * Sentences in a list, "a, b and c", keeping their marks. Every part must be
 * one `said` made in this analysis, as `joinSaid` needs, or the list is unknown.
 */
// Every part goes through joinSaid, the last too, so each must be in the ledger.
export const andList = (parts: readonly Said[]): Said => (parts.length <= 1 ? (parts[0] ?? said``) : joinSaid([joinSaid(parts.slice(0, -1), ", "), said` and `, parts.at(-1)!], ""));

/**
 * An amount as `money` in src/render/format.ts writes it. The currency is the
 * calibration's, so one without a symbol of ours is printed as the org's words.
 */
export function moneySaid(amount: number, currency: string): Said {
  const whole = grouped(Math.round(amount));
  if (currency === "GBP" || currency === "USD" || currency === "EUR") return said`${currency === "GBP" ? "£" : currency === "USD" ? "$" : "€"}${whole}`;
  return said`${org(currency)} ${whole}`;
}

/** Seats of a licence or an app, "3 Sales Cloud seats", as `plural(n, \`${name} seat\`)` writes it, with the name the org's words (DECISIONS.md 663). */
export const seatsSaid = (n: number, name: string): Said => said`${n} ${org(name)} ${n === 1 ? "seat" : "seats"}`;
