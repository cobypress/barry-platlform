import { z } from "zod";

// The team survey (roadmap Stage 4): about five minutes, anonymous, and no name
// is ever asked. The console renders exactly these questions, and stores each
// answer set as it is. A change to a question is a new version, because answers
// to different questions can't be counted together.

export const SURVEY_VERSION = 1;

export const SURVEY_INTRO =
  "Five minutes, and anonymous: we never ask your name, and nobody at your company sees your answers on their own. We set what your team tells us beside what we find in Salesforce.";

export const ROLE_BANDS = [
  { id: "sales_rep", label: "Sales rep" },
  { id: "sales_manager", label: "Sales manager" },
  { id: "ops_admin", label: "Sales ops or Salesforce admin" },
  { id: "leadership", label: "Leadership" },
  { id: "other", label: "Something else" },
] as const;
export type RoleBand = (typeof ROLE_BANDS)[number]["id"];

export const BLOCKERS = [
  { id: "too_many_fields", label: "Too many fields or screens to fill in" },
  { id: "slow", label: "It's slow, or takes too many clicks" },
  { id: "hard_to_find", label: "It's hard to find what I need" },
  { id: "data_wrong", label: "The data in it is wrong or out of date" },
  { id: "double_entry", label: "Typing the same thing in more than one place" },
  { id: "reports", label: "Reports and dashboards don't show what I need" },
  { id: "mobile", label: "It doesn't work well on my phone" },
  { id: "training", label: "Nobody showed me how to use it properly" },
  { id: "other_tools", label: "The real work happens in other tools" },
  { id: "nothing", label: "Nothing gets in my way" },
] as const;
export type Blocker = (typeof BLOCKERS)[number]["id"];
export const MAX_BLOCKERS = 3;
export const MAX_HOURS = 40;
export const MAX_COMMENT = 1000;

export type Question =
  | { id: "role"; kind: "single"; text: string; options: ReadonlyArray<{ id: string; label: string }>; required: true }
  | { id: "ease" | "trust" | "recommend"; kind: "scale"; text: string; min: number; max: number; minLabel: string; maxLabel: string; required: true }
  | { id: "admin_hours" | "rekeying_hours"; kind: "hours"; text: string; min: 0; max: number; step: number; required: true }
  | { id: "blockers"; kind: "multi"; text: string; options: ReadonlyArray<{ id: string; label: string }>; max: number; required: true }
  | { id: "comment"; kind: "text"; text: string; maxLength: number; required: false };

/** In the order the console shows them. */
export const QUESTIONS: Question[] = [
  { id: "role", kind: "single", text: "Which best describes your role?", options: ROLE_BANDS, required: true },
  { id: "ease", kind: "scale", text: "How easy is Salesforce to use, as it's set up for you?", min: 1, max: 5, minLabel: "Very hard", maxLabel: "Very easy", required: true },
  { id: "trust", kind: "scale", text: "How much do you trust the pipeline figures in Salesforce?", min: 1, max: 5, minLabel: "Not at all", maxLabel: "Completely", required: true },
  { id: "admin_hours", kind: "hours", text: "In a typical week, how many hours do you spend on Salesforce admin that doesn't help you sell?", min: 0, max: MAX_HOURS, step: 0.5, required: true },
  { id: "rekeying_hours", kind: "hours", text: "In a typical week, how many hours do you spend typing data in again, or keeping a spreadsheet alongside Salesforce?", min: 0, max: MAX_HOURS, step: 0.5, required: true },
  { id: "blockers", kind: "multi", text: `What gets in your way most? Choose up to ${MAX_BLOCKERS}.`, options: BLOCKERS, max: MAX_BLOCKERS, required: true },
  { id: "recommend", kind: "scale", text: "How likely are you to recommend Salesforce, as it's set up here, to a colleague?", min: 0, max: 10, minLabel: "Not at all likely", maxLabel: "Extremely likely", required: true },
  { id: "comment", kind: "text", text: "Anything else you'd like us to know? Please don't name anyone.", maxLength: MAX_COMMENT, required: false },
];

const halfHours = z.number().min(0).max(MAX_HOURS).refine((n) => Number.isInteger(n * 2), "Hours go in half hours");

/** One person's answers to version 1. */
export const SurveyAnswers = z.object({
  role: z.enum(ROLE_BANDS.map((r) => r.id) as [RoleBand, ...RoleBand[]]),
  ease: z.number().int().min(1).max(5),
  trust: z.number().int().min(1).max(5),
  admin_hours: halfHours,
  rekeying_hours: halfHours,
  blockers: z.array(z.enum(BLOCKERS.map((b) => b.id) as [Blocker, ...Blocker[]])).max(MAX_BLOCKERS).refine((list) => new Set(list).size === list.length, "A blocker is chosen once"),
  recommend: z.number().int().min(0).max(10),
  comment: z.string().max(MAX_COMMENT).nullable().optional(),
});
export type SurveyAnswers = z.infer<typeof SurveyAnswers>;

/** One stored response, as the console's HealthCheckSurveyResponse row holds it. */
export const SurveyResponse = z.object({
  id: z.string().min(1),
  submittedAt: z.string().min(1),
  /** The version this person answered. Missing is the survey's own (DECISIONS.md 150). */
  version: z.number().int().optional(),
  answers: z.unknown(),
});
export type SurveyResponse = z.infer<typeof SurveyResponse>;

/**
 * A run's survey: the question set's version and every response. Answers that
 * don't fit the version are dropped and counted. A survey is one that was sent,
 * so one with no responses still says fewer than five answered (DECISIONS.md 149).
 */
export const Survey = z.object({ version: z.number().int().positive(), responses: z.array(SurveyResponse) });
export type Survey = z.infer<typeof Survey>;

/** What a comment becomes in the run's folder: whether there was one, never what it said. */
export const COMMENT_LEFT_OUT = "Left out by the worker";

/**
 * The survey as the run's folder keeps it, with every comment replaced, so no
 * free text reaches the engine's files or anything printed from them.
 */
export function withoutComments(survey: Survey): Survey {
  return {
    version: survey.version,
    responses: survey.responses.map((r) => {
      const kept = { id: r.id, submittedAt: r.submittedAt, ...(r.version === undefined ? {} : { version: r.version }) };
      if (typeof r.answers !== "object" || r.answers === null || !("comment" in r.answers)) return { ...kept, answers: r.answers };
      const { comment, ...rest } = r.answers as Record<string, unknown>;
      return { ...kept, answers: typeof comment === "string" && comment.trim() ? { ...rest, comment: COMMENT_LEFT_OUT } : rest };
    }),
  };
}

/**
 * The answers that can be counted. A free-text comment never leaves here: only
 * whether there was one, so a document can say how many left one without
 * quoting any (DECISIONS.md 142).
 */
export function readAnswers(survey: Survey): { answers: Array<Omit<SurveyAnswers, "comment"> & { commented: boolean }>; dropped: number } {
  if (survey.version !== SURVEY_VERSION) return { answers: [], dropped: survey.responses.length };
  const answers: Array<Omit<SurveyAnswers, "comment"> & { commented: boolean }> = [];
  let dropped = 0;
  for (const r of survey.responses) {
    // Answers to another version are to other questions, so they can't be counted with these.
    const parsed = (r.version ?? survey.version) === SURVEY_VERSION ? SurveyAnswers.safeParse(r.answers) : { success: false as const };
    if (!parsed.success) {
      dropped++;
      continue;
    }
    const { comment, ...rest } = parsed.data;
    answers.push({ ...rest, commented: typeof comment === "string" && comment.trim().length > 0 });
  }
  return { answers, dropped };
}
