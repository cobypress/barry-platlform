import { describe, test } from "node:test";
import assert from "node:assert/strict";
import type { Metric } from "../../schemas/index.ts";
import { SURVEY_VERSION, type Survey, type SurveyAnswers } from "../questions.ts";
import { MIN_RESPONSES, SURVEY, surveyMetrics } from "../summary.ts";

// Decision 648, exhaustively: for every survey of 5 to 14 answers over up to
// three roles, and every way a yes or no answer can fall between them, no
// stored figure (what the documents, the digest and the console print from) is
// about a group of one to four people a reader can name. Two readers:
//  - one who knows only what's printed: every group they can name (everyone,
//    a shown role, everyone outside the shown roles, by subtraction) holds none
//    or five or more, and every count they can read or work out is of such a group;
//  - one who also knows exactly how many of each role answered: a role of one
//    to four has its answer pinned down only when a group of five or more
//    holding it all answered the same, as any survey's unanimous figure does.

const AS_OF = "2026-11-01T09:00:00.000Z";
const ROLES = ["sales_rep", "sales_manager", "leadership"] as const;
const LABELS: Record<string, string> = { sales_rep: "Sales rep", sales_manager: "Sales manager", leadership: "Leadership" };

/** A yes or no answer, and how a person gives each. */
type Question = { name: string; give: (yes: boolean) => Partial<SurveyAnswers> };
const QUESTIONS: Question[] = [
  { name: "answers 9 or 10 to recommend", give: (yes) => ({ recommend: yes ? 10 : 3 }) },
  { name: "names a blocker", give: (yes) => ({ blockers: yes ? ["data_wrong"] : ["nothing"] }) },
  { name: "leaves a comment", give: (yes) => ({ comment: yes ? "Synthetic." : null }) },
];

/** The first `yes[r]` of each role's `sizes[r]` people answer yes. Everything else is the same for all. */
function survey(sizes: number[], yes: number[], q: Question): Survey {
  const responses = sizes.flatMap((size, r) =>
    Array.from({ length: size }, (_, i) => ({ role: ROLES[r]!, ease: 3, trust: 3, admin_hours: 1, rekeying_hours: 1, blockers: ["slow"], recommend: 7, comment: null, ...q.give(i < yes[r]!) })),
  );
  return { version: SURVEY_VERSION, responses: responses.map((answers, i) => ({ id: `sr-${String(i + 1).padStart(10, "0")}`, submittedAt: AS_OF, answers })) };
}

/** Every split of n into three, each part 0 or more. */
const splits = (n: number): number[][] => Array.from({ length: n + 1 }, (_, a) => Array.from({ length: n - a + 1 }, (_, b) => [a, b, n - a - b])).flat();
/** Every count of yes in each part. */
const cells = (sizes: number[]): number[][] => sizes.reduce<number[][]>((acc, size) => acc.flatMap((c) => Array.from({ length: size + 1 }, (_, k) => [...c, k])), [[]]);

const find = (metrics: Metric[], id: string) => metrics.find((m) => m.id === id);
const share = (value: number, of: number): number => Math.round((value * of) / 100);

/**
 * Every count a reader can read off the stored figures, or work out from them,
 * with the group it's out of: the roles shown and whoever is outside them, and
 * each answer's count overall, in a shown role and outside the shown roles.
 */
function readable(metrics: Metric[], q: Question): { groups: number[]; counts: Array<{ k: number; of: number; what: string }> } {
  const responses = find(metrics, SURVEY.responses)!;
  if (responses.status !== "measured") return { groups: [], counts: [] };
  const n = responses.value;
  const roles = responses.segments.filter((s) => s.dimension === "role");
  const inRoles = roles.reduce((sum, s) => sum + s.n, 0);
  const groups = [...roles.map((s) => s.n), n - inRoles];
  const counts: Array<{ k: number; of: number; what: string }> = [];
  if (q.name.startsWith("answers")) {
    const m = find(metrics, SURVEY.recommend)!;
    if (m.status === "measured") {
      const total = share(m.value, n);
      counts.push({ k: total, of: n, what: "9 or 10 overall" });
      for (const s of m.segments.filter((x) => x.dimension === "answer")) counts.push({ k: s.n, of: n, what: s.key });
      const byRole = m.segments.filter((x) => x.dimension === "role");
      const known = byRole.map((s) => ({ k: share(s.value!, roles.find((r) => r.key === s.key)!.n), of: roles.find((r) => r.key === s.key)!.n, what: `9 or 10, ${s.key}` }));
      counts.push(...known);
      if (byRole.length && n - inRoles > 0) counts.push({ k: total - known.reduce((sum, c) => sum + c.k, 0), of: n - inRoles, what: "9 or 10, outside the roles shown" });
    }
  } else if (q.name.startsWith("names")) {
    const m = find(metrics, SURVEY.blockers)!;
    if (m.status === "measured") {
      counts.push({ k: share(m.value, n), of: n, what: "held back" });
      for (const s of m.segments) counts.push({ k: s.n, of: n, what: s.key });
    }
  } else if (responses.stats.comments !== undefined) counts.push({ k: responses.stats.comments, of: n, what: "comments" });
  return { groups, counts };
}

describe("no stored survey figure lets a reader work out fewer than five people's answers (decision 648)", () => {
  for (const q of QUESTIONS) {
    test(`whoever ${q.name}: every survey of 5 to 14 answers over up to three roles, and every way the answer falls between them`, () => {
      let surveys = 0;
      for (let n = MIN_RESPONSES; n <= 14; n++) {
        for (const sizes of splits(n)) {
          // What each reader sees, for every way the answer can fall.
          const seen = new Map<string, number[][]>();
          for (const yes of cells(sizes)) {
            surveys++;
            const metrics = surveyMetrics(survey(sizes, yes, q), AS_OF);
            const where = `${sizes.join("/")} answering, ${yes.join("/")} yes`;
            // A reader who knows only what's printed: every group named holds none or five or more, and every count is of one.
            const { groups, counts } = readable(metrics, q);
            for (const g of groups) assert.ok(g === 0 || g >= MIN_RESPONSES, `${where}: a group of ${g} can be named`);
            for (const c of counts) assert.ok(c.of >= MIN_RESPONSES && c.k >= 0 && c.k <= c.of, `${where}: ${c.what} counts ${c.k} of a group of ${c.of}`);
            const key = JSON.stringify(metrics);
            seen.set(key, [...(seen.get(key) ?? []), yes]);
          }
          // A reader who also knows how many of each role answered: a role of one to four people has its answer pinned
          // down only when a group of five or more holding it all gave the same answer.
          for (const [key, alike] of seen) {
            const metrics = JSON.parse(key) as Metric[];
            const { groups, counts } = readable(metrics, q);
            sizes.forEach((size, r) => {
              if (size === 0 || size >= MIN_RESPONSES || new Set(alike.map((y) => y[r])).size > 1) return;
              const yes = alike[0]![r]!;
              const unanimous = counts.some((c) => c.of >= MIN_RESPONSES && c.of >= size && (c.k === 0 || c.k === c.of) && (c.of === groups.at(-1) || c.of === sizes.reduce((a, b) => a + b, 0)));
              assert.ok(unanimous && (yes === 0 || yes === size), `${sizes.join("/")} answering: the ${size} in ${LABELS[ROLES[r]!]} are pinned to ${yes} yes by ${key.length} characters of figures that give ${alike.length} way(s)`);
            });
          }
        }
      }
      assert.ok(surveys > 10_000, `${surveys} surveys`);
    });
  }

  test("the review's probe: 5 sales reps and a manager show no role split, so nothing is about the manager, and every whole-survey figure", () => {
    const answers = (i: number): Partial<SurveyAnswers> => (i < 5 ? { role: "sales_rep", blockers: ["slow"], recommend: 3 } : { role: "sales_manager", blockers: ["data_wrong"], recommend: 10 });
    const s: Survey = { version: SURVEY_VERSION, responses: Array.from({ length: 6 }, (_, i) => ({ id: `sr-${String(i + 1).padStart(10, "0")}`, submittedAt: AS_OF, answers: { role: "sales_rep", ease: 3, trust: 3, admin_hours: 1, rekeying_hours: 1, blockers: ["slow"], recommend: 7, comment: null, ...answers(i) } })) };
    const metrics = surveyMetrics(s, AS_OF);
    assert.ok(metrics.every((m) => m.status === "measured" && m.segments.every((x) => x.dimension !== "role")), "every figure shown, none by role");
    assert.doesNotMatch(JSON.stringify(metrics), /Sales rep|Sales manager/);
    const recommend = find(metrics, SURVEY.recommend)!;
    assert.equal(recommend.status === "measured" && recommend.value, 16.7, "one of six said 9 or 10: who, the figures don't say");
  });
});
