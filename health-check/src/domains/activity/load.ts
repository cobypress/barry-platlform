import { activityExtractor, ACTIVITY_DAYS, type EventRow, type TaskRow } from "../../extract/activity.ts";
import { activityCaptureExtractor } from "../../extract/users.ts";
import type { RuleContext } from "../../engine/rule.ts";
import type { Evidence } from "../../schemas/index.ts";

export type Touch = { at: string; ownerId: string; dealId: string | null; kind: "task" | "event" | "email" };

export type ActivityData =
  | { status: "unavailable"; reason: string; evidence: Evidence[] }
  | { status: "ok"; touches: Touch[]; tasks: TaskRow[]; events: EventRow[]; from: string; evidence: Evidence[]; exclusions: string[]; assumptions: string[]; captureInUse: boolean };

const isOpportunity = (id: string | null): id is string => id !== null && id.startsWith("006");

// Emails captured by Einstein Activity Capture are widely reported to live
// outside these objects. Where it's in use, or we can't tell, a low count might
// be a capture gap rather than a quiet rep, so findings drop to medium confidence.
export const CAPTURE_NOTE = "Einstein Activity Capture may hold email that isn't in these objects, so counts here are a floor";

export function loadActivity(ctx: Omit<RuleContext, "metrics">): ActivityData {
  const { tasks, events, emails, from } = ctx.data(activityExtractor);
  const range = { from, to: ctx.window.to };
  const evidence = [
    ctx.ev(tasks.source, { count: tasks.status === "ok" ? tasks.data.length : null, dateRange: range }),
    ctx.ev(events.source, { count: events.status === "ok" ? events.data.length : null, dateRange: range }),
    ctx.ev(emails.source, { count: emails.status === "ok" ? emails.data.length : null, dateRange: range }),
  ];
  if (tasks.status !== "ok") return { status: "unavailable", reason: `Tasks couldn't be read: ${tasks.reason}`, evidence };
  if (events.status !== "ok") return { status: "unavailable", reason: `Events couldn't be read: ${events.reason}`, evidence };
  if (tasks.truncated || events.truncated) return { status: "unavailable", reason: "There are too many activities to read in full, and a part would be a biased sample.", evidence };

  const taskIds = new Set(tasks.data.map((t) => t.Id));
  const touches: Touch[] = [
    ...tasks.data.map((t): Touch => ({ at: t.CreatedDate, ownerId: t.OwnerId, dealId: isOpportunity(t.WhatId) ? t.WhatId : null, kind: "task" })),
    ...events.data.map((e): Touch => ({ at: e.StartDateTime, ownerId: e.OwnerId, dealId: isOpportunity(e.WhatId) ? e.WhatId : null, kind: "event" })),
  ];
  const exclusions = [`Activities older than ${ACTIVITY_DAYS} days, which Salesforce archives out of normal queries`, "Each invitee's copy of an event"];
  if (emails.status === "ok" && !emails.truncated) {
    // Enhanced Email also writes a task, so the email only counts if that task isn't there.
    for (const email of emails.data) {
      if (email.ActivityId && taskIds.has(email.ActivityId)) continue;
      touches.push({ at: email.MessageDate, ownerId: email.CreatedById, dealId: isOpportunity(email.RelatedToId) ? email.RelatedToId : null, kind: "email" });
    }
  } else {
    exclusions.push(emails.status === "ok" ? "EmailMessage, which was too long to read in full" : "EmailMessage, which couldn't be read");
  }
  const capture = ctx.data(activityCaptureExtractor);
  const captureInUse = capture.status !== "ok" || capture.data.length > 0;
  return { status: "ok", touches, tasks: tasks.data, events: events.data, from, evidence, exclusions, assumptions: captureInUse ? [CAPTURE_NOTE] : [], captureInUse };
}
