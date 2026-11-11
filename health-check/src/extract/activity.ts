import { readCapped } from "./opportunities.ts";
import { daysBefore, soqlWindow, type Extractor, type Sourced } from "./types.ts";

// Ids and dates only. Subject, Description and message bodies are customer
// conversations, and no rule needs them.
export type TaskRow = { Id: string; OwnerId: string; WhoId: string | null; WhatId: string | null; CreatedDate: string; TaskSubtype: string | null };
export type EventRow = { Id: string; OwnerId: string; WhoId: string | null; WhatId: string | null; StartDateTime: string };
export type EmailRow = { Id: string; RelatedToId: string | null; ActivityId: string | null; MessageDate: string; CreatedById: string };
export type ActivityExtract = { tasks: Sourced<TaskRow[]>; events: Sourced<EventRow[]>; emails: Sourced<EmailRow[]>; from: string };

// A plain query doesn't return activities archived after 365 days, and queryAll
// isn't on the allowlist. Reading only what's unarchived means the limit is
// stated up front, rather than history quietly thinning out.
export const ACTIVITY_DAYS = 365;
export const activityExtractor: Extractor<ActivityExtract> = {
  id: "activity",
  version: 2,
  partialKeys: ["activity-tasks", "activity-events", "activity-emails"],
  async run(ctx) {
    const from = daysBefore(ctx.now, ACTIVITY_DAYS);
    // Each read stops at the run's own date (DECISIONS.md 480), so a resumed run reads what it would have at the start.
    const window = (field: string): string => soqlWindow(field, from, ctx.now);
    return {
      tasks: await readCapped<TaskRow>(ctx, "activity-tasks", "Id, OwnerId, WhoId, WhatId, CreatedDate, TaskSubtype", "Task", window("CreatedDate")),
      // IsChild leaves out each invitee's copy, which would count one meeting many times.
      events: await readCapped<EventRow>(ctx, "activity-events", "Id, OwnerId, WhoId, WhatId, StartDateTime", "Event", `IsChild = false AND ${window("StartDateTime")}`),
      emails: await readCapped<EmailRow>(ctx, "activity-emails", "Id, RelatedToId, ActivityId, MessageDate, CreatedById", "EmailMessage", window("MessageDate")),
      from: from.toISOString().slice(0, 10),
    };
  },
};
