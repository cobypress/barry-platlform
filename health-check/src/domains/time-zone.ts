import { orgExtractor } from "../extract/configuration.ts";
import type { RuleContext } from "../engine/rule.ts";

// A close at 02:00 UTC on 1 April is 31 March in Los Angeles, and belongs in that
// quarter. So instants become calendar days in the org's own zone. Date-only
// fields such as CloseDate have no zone: they're already the day, and converting
// one would move it.

const QUARTER_HOUR = 900_000;

type Zoned = { format: Intl.DateTimeFormat; days: Map<number, string> };
const zones = new Map<string, Zoned | null>();

function zoned(zone: string): Zoned | null {
  if (zones.has(zone)) return zones.get(zone)!;
  let entry: Zoned | null = null;
  try {
    entry = { format: new Intl.DateTimeFormat("en-CA", { timeZone: zone, year: "numeric", month: "2-digit", day: "2-digit" }), days: new Map() };
  } catch {
    // A zone name Salesforce knows and Intl doesn't. The caller falls back to UTC and says so.
  }
  zones.set(zone, entry);
  return entry;
}

/** The org's time zone, or null if it can't be read or isn't one we can apply. */
export function zoneOf(ctx: Pick<RuleContext, "data">): string | null {
  const org = ctx.data(orgExtractor);
  const zone = org.status === "ok" ? org.data[0]?.TimeZoneSidKey : undefined;
  return zone && zoned(zone) ? zone : null;
}

/** The calendar day an instant falls on. With no zone, the UTC day. */
export function localDay(instant: Date | string, zone: string | null): string {
  const date = typeof instant === "string" ? new Date(instant) : instant;
  const entry = zone ? zoned(zone) : null;
  if (!entry) return date.toISOString().slice(0, 10);
  // Every zone's offset is a whole number of quarter hours, so days can be cached
  // by the quarter hour. A large org would otherwise make millions of format calls.
  const bucket = Math.floor(date.getTime() / QUARTER_HOUR);
  const known = entry.days.get(bucket);
  if (known) return known;
  const parts = Object.fromEntries(entry.format.formatToParts(date).map((p) => [p.type, p.value]));
  const day = `${parts.year}-${parts.month}-${parts.day}`;
  entry.days.set(bucket, day);
  return day;
}

/** Today in the org. */
export const todayIn = (ctx: Pick<RuleContext, "data" | "now">): string => localDay(ctx.now, zoneOf(ctx));

/** For a metric's assumptions, so a reader knows which calendar the figures are on. */
export function zoneNote(ctx: Pick<RuleContext, "data">): string {
  const zone = zoneOf(ctx);
  return zone ? `Days, months and quarters are in the org's time zone, ${zone}` : "Days, months and quarters are in UTC, because the org's time zone couldn't be read";
}
