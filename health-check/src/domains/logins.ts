import type { LoginSummary } from "../extract/users.ts";
import type { Sourced } from "../extract/types.ts";

// Who has stopped logging in, said once for every check that counts them, so the
// dormant users and the Service Cloud seats can't disagree about a person
// (DECISIONS.md 361, 394). LastLoginDate is where a user starts; the login log is
// the fuller record, and a login it shows inside the period clears them.

/** How a user's last-login date reads against the start of the dormant period. */
export type Lapse = "dormant" | "never";

/** A login log time, as Salesforce writes it ("+0000"), as a date. */
const loggedAt = (time: string): Date => new Date(time.replace(/\+0000$/, "Z"));

/** By LastLoginDate alone: never logged in, not since `cutoff`, or null if they have. */
export function lapseByDate(user: { LastLoginDate: string | null }, cutoff: Date): Lapse | null {
  if (user.LastLoginDate === null) return "never";
  return new Date(user.LastLoginDate) < cutoff ? "dormant" : null;
}

/**
 * The same, unless the login log shows otherwise: a login inside the period clears
 * a dormant user, and any login it holds clears one with no last-login date. A log
 * that couldn't be read clears nobody.
 */
export function lapse(user: { Id: string; LastLoginDate: string | null }, logins: Sourced<LoginSummary> | undefined, cutoff: Date): Lapse | null {
  const byDate = lapseByDate(user, cutoff);
  const last = logins?.status === "ok" ? logins.data.byUser[user.Id]?.lastSuccess : undefined;
  if (byDate === null || last === undefined) return byDate;
  return byDate === "never" || loggedAt(last) >= cutoff ? null : byDate;
}
