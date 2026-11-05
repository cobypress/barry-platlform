import { SURVEY_VERSION, type Survey, type SurveyAnswers } from "../survey/questions.ts";

// A made-up team survey for tests. Every answer is invented and every id is
// zero-padded, as the fixtures rule says.

/** The comment every synthetic person who comments leaves, so a test can look for it anywhere. */
export const SYNTHETIC_COMMENT = "Synthetic comment that must never be printed 7f3a";

const ROLES = ["sales_rep", "sales_rep", "sales_manager", "ops_admin", "leadership"] as const;
const BLOCKER_SETS: Array<SurveyAnswers["blockers"]> = [["too_many_fields", "double_entry"], ["data_wrong"], ["slow", "hard_to_find", "double_entry"], ["nothing"], ["reports", "data_wrong"]];

/** `n` people, varied but fixed, with every third leaving a comment. `change` adjusts each one. */
export function syntheticSurvey(n: number, change: (answers: SurveyAnswers, i: number) => Partial<SurveyAnswers> = () => ({})): Survey {
  return {
    version: SURVEY_VERSION,
    responses: Array.from({ length: n }, (_, i) => {
      const answers: SurveyAnswers = {
        role: ROLES[i % ROLES.length]!,
        ease: 1 + (i % 5),
        trust: 1 + ((i * 2) % 5),
        admin_hours: (i % 4) * 1.5,
        rekeying_hours: (i % 3) * 1,
        blockers: BLOCKER_SETS[i % BLOCKER_SETS.length]!,
        recommend: (i * 3) % 11,
        comment: i % 3 === 0 ? SYNTHETIC_COMMENT : null,
      };
      return { id: `sr-${String(i + 1).padStart(10, "0")}`, submittedAt: `2026-11-0${1 + (i % 4)}T09:${String(i % 60).padStart(2, "0")}:00.000Z`, answers: { ...answers, ...change(answers, i) } };
    }),
  };
}
