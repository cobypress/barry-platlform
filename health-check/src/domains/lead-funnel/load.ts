import { activityExtractor } from "../../extract/activity.ts";
import { leadsExtractor, type LeadRow } from "../../extract/leads.ts";
import { daysBefore } from "../../extract/types.ts";
import { usersExtractor } from "../../extract/users.ts";
import type { RuleContext } from "../../engine/rule.ts";
import type { Evidence } from "../../schemas/index.ts";
import { localDay, zoneOf } from "../time-zone.ts";

export type Lead = LeadRow & {
  source: string;
  team: string;
  /** Null if nothing shows a response. */
  firstTouch: Date | null;
  /** False if the lead is older than the readable activity, so a blank proves nothing. */
  touchKnowable: boolean;
};

/**
 * Whether Salesforce kept the leads' Status history, read from what the history
 * holds (HC_LEAD_JOURNEY, DECISIONS.md 770), as the stage history's absence is
 * read for deals (paths/variants.ts): no new read. `complete`: nothing says
 * otherwise. `untracked`: the read gave no Status change at all while leads were
 * converted in the window, `converted` of them. `from`: the earliest change kept
 * is later than a conversion, so the history starts on `day`, the earliest
 * change's local day, and a lead made before it can't show a status change.
 * Both rest on conversion writing a Status change where the field is tracked
 * (unverified, docs/QUESTIONS.md).
 */
export type StatusHistory = { kind: "complete" } | { kind: "untracked"; converted: number } | { kind: "from"; day: string };

export type LeadData =
  | { status: "unavailable"; reason: string; evidence: Evidence[] }
  | { status: "ok"; leads: Lead[]; evidence: Evidence[]; exclusions: string[]; activityReadable: boolean; statusReadable: boolean; statusHistory: StatusHistory };

const stamp = (value: string): Date => new Date(value.replace(/\+0000$/, "Z"));

/**
 * The leads, each with its first touch. With `inferHistory` (the lead journey's
 * switch on, DECISIONS.md 770), whether the Status history was kept is read too,
 * and a lead made before the history starts can't show a first touch by status;
 * off, every lead is read exactly as before and the history counts as complete.
 */
export function loadLeads(ctx: Omit<RuleContext, "metrics">, inferHistory = false): LeadData {
  const { leads, statusChanges } = ctx.data(leadsExtractor);
  const activity = ctx.data(activityExtractor);
  const evidence = [ctx.ev(leads.source, { count: leads.status === "ok" ? leads.data.length : null, dateRange: ctx.window })];
  if (leads.status !== "ok") return { status: "unavailable", reason: `Leads couldn't be read: ${leads.reason}`, evidence };
  if (leads.truncated) return { status: "unavailable", reason: "There are too many leads to read in full, and a part would be a biased sample.", evidence };
  if (leads.data.length === 0) return { status: "unavailable", reason: "No leads were created in the window, so there's no funnel to measure.", evidence };

  const users = ctx.data(usersExtractor);
  const roles = new Map(users.status === "ok" ? users.data.map((u) => [u.Id, u.UserRole?.Name ?? "No role"]) : []);
  const earliest = new Map<string, Date>();
  const note = (id: string | null, at: string): void => {
    if (!id) return;
    const date = stamp(at);
    const seen = earliest.get(id);
    if (!seen || date < seen) earliest.set(id, date);
  };
  const activityReadable = activity.tasks.status === "ok" && activity.events.status === "ok" && !activity.tasks.truncated && !activity.events.truncated;
  if (activity.tasks.status === "ok") for (const task of activity.tasks.data) note(task.WhoId, task.CreatedDate);
  if (activity.events.status === "ok") for (const event of activity.events.data) note(event.WhoId, event.StartDateTime);
  const statusReadable = statusChanges.status === "ok" && !statusChanges.truncated;
  if (statusChanges.status === "ok") for (const change of statusChanges.data) note(change.LeadId, change.CreatedDate);
  const statusHistory: StatusHistory = inferHistory && statusReadable ? historyOf(ctx, leads.data, statusChanges.data) : { kind: "complete" };
  const zone = statusHistory.kind === "from" ? zoneOf(ctx) : null;

  const readableFrom = daysBefore(ctx.now, 365);
  const rows = leads.data.map((lead): Lead => {
    const created = stamp(lead.CreatedDate);
    // Conversion moves a lead's activity to the new contact, so both ids count.
    const touches = [earliest.get(lead.Id), lead.ConvertedContactId ? earliest.get(lead.ConvertedContactId) : undefined].filter((d): d is Date => d !== undefined && d >= created);
    // Made before the Status history starts: a blank can't say the lead wasn't answered by a change of status.
    const beforeHistory = statusHistory.kind === "from" && localDay(created, zone) < statusHistory.day;
    return {
      ...lead,
      source: lead.LeadSource ?? "No source",
      team: roles.get(lead.OwnerId) ?? "No role",
      firstTouch: touches.length ? new Date(Math.min(...touches.map((d) => d.getTime()))) : null,
      touchKnowable: activityReadable && created >= readableFrom && !beforeHistory,
    };
  });
  const exclusions = [...(statusReadable ? [] : ["Lead status changes, which couldn't be read in full"])];
  return { status: "ok", leads: rows, evidence, exclusions, activityReadable, statusReadable, statusHistory };
}

/** What the Status history read says of itself (see StatusHistory). Only from a read that came back whole. */
function historyOf(ctx: Omit<RuleContext, "metrics">, leads: LeadRow[], changes: Array<{ CreatedDate: string }>): StatusHistory {
  const converted = leads.filter((l) => l.IsConverted && l.ConvertedDate);
  if (changes.length === 0) return converted.length > 0 ? { kind: "untracked", converted: converted.length } : { kind: "complete" };
  const zone = zoneOf(ctx);
  const first = changes.reduce((min, c) => (c.CreatedDate < min ? c.CreatedDate : min), changes[0]!.CreatedDate);
  const day = localDay(stamp(first), zone);
  // ConvertedDate is a day with no time: a conversion on a day before the first change kept was made while nothing was kept.
  return converted.some((l) => l.ConvertedDate! < day) ? { kind: "from", day } : { kind: "complete" };
}
