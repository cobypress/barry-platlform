import { activityExtractor } from "../../extract/activity.ts";
import { leadsExtractor, type LeadRow } from "../../extract/leads.ts";
import { daysBefore } from "../../extract/types.ts";
import { usersExtractor } from "../../extract/users.ts";
import type { RuleContext } from "../../engine/rule.ts";
import type { Evidence } from "../../schemas/index.ts";

export type Lead = LeadRow & {
  source: string;
  team: string;
  /** Null if nothing shows a response. */
  firstTouch: Date | null;
  /** False if the lead is older than the readable activity, so a blank proves nothing. */
  touchKnowable: boolean;
};

export type LeadData =
  | { status: "unavailable"; reason: string; evidence: Evidence[] }
  | { status: "ok"; leads: Lead[]; evidence: Evidence[]; exclusions: string[]; activityReadable: boolean; statusReadable: boolean };

const stamp = (value: string): Date => new Date(value.replace(/\+0000$/, "Z"));

export function loadLeads(ctx: Omit<RuleContext, "metrics">): LeadData {
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

  const readableFrom = daysBefore(ctx.now, 365);
  const rows = leads.data.map((lead): Lead => {
    const created = stamp(lead.CreatedDate);
    // Conversion moves a lead's activity to the new contact, so both ids count.
    const touches = [earliest.get(lead.Id), lead.ConvertedContactId ? earliest.get(lead.ConvertedContactId) : undefined].filter((d): d is Date => d !== undefined && d >= created);
    return {
      ...lead,
      source: lead.LeadSource ?? "No source",
      team: roles.get(lead.OwnerId) ?? "No role",
      firstTouch: touches.length ? new Date(Math.min(...touches.map((d) => d.getTime()))) : null,
      touchKnowable: activityReadable && created >= readableFrom,
    };
  });
  const exclusions = [...(statusReadable ? [] : ["Lead status changes, which couldn't be read in full"])];
  return { status: "ok", leads: rows, evidence, exclusions, activityReadable, statusReadable };
}
