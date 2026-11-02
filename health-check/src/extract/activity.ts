import { readKeyset } from "./opportunities.ts";
import { daysBefore, soqlDateTime, unavailable, type ExtractContext, type Extractor, type Sourced, type SourceInfo } from "./types.ts";

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
const MAX_ROWS = 200_000;

async function readCapped<R extends { Id: string }>(ctx: ExtractContext, key: string, select: string, from: string, where: string): Promise<Sourced<R[]>> {
  const source: SourceInfo = { kind: "soql", api: "rest", query: `SELECT ${select} FROM ${from} WHERE ${where}` };
  try {
    const rows = await readKeyset<R>(ctx, key, select, from, where, MAX_ROWS);
    return { status: "ok", data: rows, source, truncated: rows.length >= MAX_ROWS };
  } catch (err) {
    return unavailable(source, err);
  }
}

export const activityExtractor: Extractor<ActivityExtract> = {
  id: "activity",
  version: 1,
  partialKeys: ["activity-tasks", "activity-events", "activity-emails"],
  async run(ctx) {
    const from = daysBefore(ctx.now, ACTIVITY_DAYS);
    const start = soqlDateTime(from);
    const end = soqlDateTime(ctx.now);
    return {
      tasks: await readCapped<TaskRow>(ctx, "activity-tasks", "Id, OwnerId, WhoId, WhatId, CreatedDate, TaskSubtype", "Task", `CreatedDate >= ${start}`),
      // IsChild leaves out each invitee's copy, which would count one meeting many times.
      events: await readCapped<EventRow>(ctx, "activity-events", "Id, OwnerId, WhoId, WhatId, StartDateTime", "Event", `IsChild = false AND StartDateTime >= ${start} AND StartDateTime <= ${end}`),
      emails: await readCapped<EmailRow>(ctx, "activity-emails", "Id, RelatedToId, ActivityId, MessageDate, CreatedById", "EmailMessage", `MessageDate >= ${start}`),
      from: from.toISOString().slice(0, 10),
    };
  },
};
