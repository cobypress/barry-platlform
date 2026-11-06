import { z } from "zod";

// Stakeholder interviews (roadmap Stage 4): four or five structured 30-minute
// conversations a consultant holds on a Complete run, recorded in the console
// as notes and, where asked, a short set of answers. The console renders
// exactly these questions and stores what the consultant typed. A change to a
// question is a new version, because answers to different questions can't be
// set against the data the same way.
//
// People are recorded by role and never by name: there's no field for one, and
// the report speaks of "your sales director", never a person.

export const INTERVIEW_VERSION = 1;

export const INTERVIEW_ROLES = [
  { id: "sales_leader", label: "Sales director" },
  { id: "sales_manager", label: "Sales manager" },
  { id: "sales_ops", label: "Sales operations" },
  { id: "admin", label: "Salesforce admin" },
  { id: "sales_rep", label: "Sales rep" },
  { id: "finance", label: "Finance" },
  { id: "marketing", label: "Marketing" },
  { id: "other", label: "Another role" },
] as const;
export type InterviewRole = (typeof INTERVIEW_ROLES)[number]["id"];
export const InterviewRoleId = z.enum(INTERVIEW_ROLES.map((r) => r.id) as [InterviewRole, ...InterviewRole[]]);

export const roleLabel = (role: InterviewRole): string => INTERVIEW_ROLES.find((r) => r.id === role)?.label ?? "Another role";

/** The longest notes the console takes for one interview. */
export const MAX_INTERVIEW_NOTES = 20_000;
export const MAX_ANSWER_TEXT = 1_000;

export type InterviewQuestionKind = "scale" | "percent" | "days" | "hours" | "count" | "text";

export type InterviewQuestion = {
  id: string;
  kind: InterviewQuestionKind;
  /** What the consultant asks, in their words to the person. */
  text: string;
  /** The words at each end of a 1 to 5 scale. */
  minLabel?: string;
  maxLabel?: string;
  /** The highest answer the console takes. */
  max?: number;
};

/** Every question, in the order the console shows them. Each role asks a few (ROLE_QUESTIONS). */
export const INTERVIEW_QUESTIONS: InterviewQuestion[] = [
  { id: "trust", kind: "scale", text: "How much do you trust the pipeline figures in Salesforce?", minLabel: "Not at all", maxLabel: "Completely" },
  { id: "ease", kind: "scale", text: "How easy is Salesforce to use, as it's set up for your team?", minLabel: "Very hard", maxLabel: "Very easy" },
  { id: "stages", kind: "scale", text: "How closely do deals follow the stages as they're written down?", minLabel: "Not at all", maxLabel: "Exactly" },
  { id: "automation", kind: "scale", text: "How confident are you changing Salesforce's automation without breaking something?", minLabel: "Not at all", maxLabel: "Completely" },
  { id: "win_rate", kind: "percent", text: "Of the deals that reach a decision, roughly what share do you win?", max: 100 },
  { id: "forecast_hit", kind: "percent", text: "Roughly what share of won deals close in the quarter they were first forecast for?", max: 100 },
  { id: "sales_cycle", kind: "days", text: "How many days does a typical won deal take, from when it's created in Salesforce to when it closes?", max: 1_000 },
  { id: "rekeying", kind: "hours", text: "In a typical week, how many hours go on typing data in again, or keeping a spreadsheet alongside Salesforce?", max: 40 },
  { id: "admin_hours", kind: "hours", text: "In a typical week, how many hours go on Salesforce admin that doesn't help anyone sell?", max: 40 },
  { id: "weekly_users", kind: "count", text: "Roughly how many people use Salesforce in a typical week?", max: 100_000 },
  { id: "integrations", kind: "count", text: "How many other systems send data to Salesforce or read from it?", max: 1_000 },
  { id: "fix_first", kind: "text", text: "If you could fix one thing about Salesforce first, what would it be?" },
];

/** Kept short: a 30-minute conversation is mostly listening. Notes carry the rest. */
export const ROLE_QUESTIONS: Record<InterviewRole, string[]> = {
  sales_leader: ["trust", "win_rate", "forecast_hit", "sales_cycle", "fix_first"],
  sales_manager: ["trust", "stages", "forecast_hit", "fix_first"],
  sales_ops: ["trust", "stages", "rekeying", "integrations", "fix_first"],
  admin: ["ease", "automation", "weekly_users", "integrations", "fix_first"],
  sales_rep: ["ease", "stages", "rekeying", "admin_hours", "fix_first"],
  finance: ["trust", "forecast_hit", "fix_first"],
  marketing: ["trust", "ease", "fix_first"],
  other: ["ease", "trust", "fix_first"],
};

export const questionFor = (id: string): InterviewQuestion | undefined => INTERVIEW_QUESTIONS.find((q) => q.id === id);

/** The questions a role is asked, in order. */
export const questionsFor = (role: InterviewRole): InterviewQuestion[] => ROLE_QUESTIONS[role].map((id) => questionFor(id)!).filter(Boolean);

/** One answer's shape. Hours go in half hours; a share is a whole percentage. */
export function answerSchema(q: InterviewQuestion): z.ZodType<number | string> {
  switch (q.kind) {
    case "scale":
      return z.number().int().min(1).max(5);
    case "percent":
      return z.number().int().min(0).max(q.max ?? 100);
    case "hours":
      return z.number().min(0).max(q.max ?? 40).refine((n) => Number.isInteger(n * 2), "Hours go in half hours");
    case "days":
    case "count":
      return z.number().int().min(0).max(q.max ?? 100_000);
    case "text":
      return z.string().trim().min(1).max(MAX_ANSWER_TEXT);
  }
}

/** One interview as the console stores it. The interviewer stays in the console. */
export const Interview = z.object({
  id: z.string().min(1),
  role: InterviewRoleId,
  heldOn: z.iso.date(),
  notes: z.string().max(MAX_INTERVIEW_NOTES),
  /** Answers to the role's questions, by question id. Any may be left out. */
  answers: z.record(z.string(), z.unknown()).default({}),
  /** The question set it was asked from. Missing is version 1. */
  version: z.number().int().positive().optional(),
});
export type Interview = z.infer<typeof Interview>;

export const Interviews = z.object({ version: z.number().int().positive(), interviews: z.array(Interview) });
export type Interviews = z.infer<typeof Interviews>;

/**
 * The answers that can be read: only the role's own questions, each in its
 * shape, from this version. Anything else is dropped rather than guessed at.
 */
export function readInterviewAnswers(interview: Interview): Record<string, number | string> {
  if ((interview.version ?? INTERVIEW_VERSION) !== INTERVIEW_VERSION) return {};
  const out: Record<string, number | string> = {};
  for (const q of questionsFor(interview.role)) {
    const parsed = answerSchema(q).safeParse(interview.answers[q.id]);
    if (parsed.success) out[q.id] = parsed.data;
  }
  return out;
}
