import { z } from "zod";

// What the customer told us before the run. There's no shared schema for this
// yet, so it lives here until the console's intake page settles it. It's
// consultant-entered text, so it can hold names and never goes to Claude.
export const StatedProblem = z.object({
  id: z.string().min(1),
  statement: z.string().min(1),
  raisedBy: z.string().min(1).optional(),
  // Set by the consultant. Stated vs actual is a judgement, so code doesn't guess the link.
  findingIds: z.array(z.string()).default([]),
  assessment: z.string().min(1).optional(),
});
export type StatedProblem = z.infer<typeof StatedProblem>;

export const Intake = z.object({
  customerName: z.string().min(1),
  summary: z.string().min(1),
  statedProblems: z.array(StatedProblem),
  capturedBy: z.string().min(1),
  capturedAt: z.iso.datetime(),
});
export type Intake = z.infer<typeof Intake>;
