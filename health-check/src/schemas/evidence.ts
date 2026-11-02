import { z } from "zod";
import { DateRange, Timestamp } from "./common.ts";

// The fields every piece of evidence carries, whatever its source. `count` and
// `dateRange` are nullable but never optional: a rule has to state "no count"
// on purpose rather than leave it out.
const base = {
  count: z.number().int().nonnegative().nullable(),
  dateRange: DateRange.nullable(),
  exclusions: z.array(z.string()),
  capturedAt: Timestamp,
  note: z.string().optional(),
};

export const Evidence = z
  .discriminatedUnion("kind", [
    z.object({ kind: z.literal("soql"), api: z.enum(["rest", "tooling"]), query: z.string().min(1), ...base }),
    z.object({ kind: z.literal("rest"), path: z.string().min(1), ...base }),
    z.object({ kind: z.literal("metadata"), path: z.string().min(1), ...base }),
    z.object({
      kind: z.literal("manual"),
      manualStepId: z.string().min(1),
      status: z.enum(["pending", "answered", "skipped"]),
      answeredBy: z.string().nullable(),
      answeredAt: Timestamp.nullable(),
      ...base,
    }),
    // Used only when a rule or guard could not produce real evidence at all,
    // so the failure is recorded in the finding rather than lost in a log.
    z.object({ kind: z.literal("engine"), path: z.string().min(1), ...base }),
  ])
  .superRefine((e, ctx) => {
    if (e.kind === "manual" && e.status === "answered" && (!e.answeredBy || !e.answeredAt)) {
      ctx.addIssue({ code: "custom", message: "An answered manual step must record who answered it and when" });
    }
  });
export type Evidence = z.infer<typeof Evidence>;
