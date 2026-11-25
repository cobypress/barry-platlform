import { PATH_RELATED_RULES } from "../domains/scoring.ts";
import { said, spansIfCurrent, type MarkedSentence } from "../engine/org-words.ts";
import type { BuiltReport } from "../narrative/sections.ts";
import { extractFigures } from "../numbercheck/extract.ts";
import { NAMED_PEOPLE_SENTENCE, PATHS_NOT_STATED, type DigestPathInsight, type DigestPaths, type PATH_FIGURES, type SentenceOrgWords } from "../schemas/digest.ts";
import type { Finding } from "../schemas/index.ts";

// The digest's `paths` block (DECISIONS.md 758): the path insights the engine
// raised (703, 712), served for the first time, and nothing else of the paths
// analysis. Never the table of path variants: set side by side, its rows invite
// a variant read against the usual path, the comparison 703 forbids. Every text
// is the finding's own sentence as the engine wrote it, or its reason, each with
// the marks the run made for it; the one sentence written here, what would let
// the engine say more, holds only numbers the reason already says. Built from
// the stored findings, which nothing else built from the report reads, so the
// block moves no finding, score, plan or document. The checked digest settles
// its texts apart, as AI readiness's (742), so it never moves a count or a hold.

type Figure = { name: (typeof PATH_FIGURES)[number]; value: number };
type NotStatedKind = (typeof PATHS_NOT_STATED)[number];

const ENGINE_WORDS: SentenceOrgWords = { orgWordsMarked: true, orgWords: [] };
const marked = (text: string, held: MarkedSentence | undefined): SentenceOrgWords | undefined => {
  const spans = spansIfCurrent(text, held);
  return spans ? { orgWordsMarked: true, orgWords: spans } : undefined;
};
/** A stage is the org's word whole. */
const stageWords = (stage: string): SentenceOrgWords | null => (stage.length > 0 ? { orgWordsMarked: true, orgWords: [{ start: 0, end: stage.length }] } : null);

/** The engine's order of importance: the surer first, then the bigger gap in win rate, then more deals compared, then the rule's own order (skips along the usual path, then send-backs by name). */
const CONFIDENCE_RANK: Record<string, number> = { high: 0, medium: 1, low: 2 };
const gapOf = (f: Finding): number => Math.abs((f.facts.winRate ?? 0) - (f.facts.otherWinRate ?? 0));
export function insightOrder(insights: Finding[]): Finding[] {
  return insights
    .map((f, i) => ({ f, i }))
    .sort((a, b) => (CONFIDENCE_RANK[a.f.confidence]! - CONFIDENCE_RANK[b.f.confidence]!) || gapOf(b.f) - gapOf(a.f) || (b.f.facts.reached ?? 0) - (a.f.facts.reached ?? 0) || a.i - b.i)
    .map(({ f }) => f);
}

/**
 * The numbers a summary says that the reader counts, in order: every figure but a
 * date's, and none inside the org's words (a stage named "Stage 2 Review" is a
 * name, not a count), which are set aside first by the summary's marks.
 */
export const saidNumbers = (text: string, orgWords: ReadonlyArray<{ start: number; end: number }> = []): number[] => {
  let ours = text;
  for (const s of orgWords) ours = `${ours.slice(0, s.start)}${"x".repeat(s.end - s.start)}${ours.slice(s.end)}`;
  return extractFigures(ours).figures.filter((f) => f.kind !== "date" && f.value !== undefined && /\d/.test(f.raw)).map((f) => f.value!);
};

/** The insight's own sentences end here; what follows is a note the engine adds to every pipeline finding. */
const NOT_A_CAUSE = " not that one is the reason for the other.";

/**
 * Each number in an insight's summary, by what it is, from the finding's facts
 * and the first count the sentence says (every deal made in the period that
 * reached the point, which the facts don't hold apart from the closed and the
 * open). Only when they are exactly the numbers the summary says, in its order;
 * otherwise none, never a guess.
 */
export function figuresOf(f: Finding): Figure[] | undefined {
  const x = f.facts;
  const first = /^Of the (\d+) /.exec(f.summary);
  if (!first || x.reached === undefined || x.deals === undefined || x.otherDeals === undefined || x.winRate === undefined || x.otherWinRate === undefined) return undefined;
  const considered = Number(first[1]);
  const open = x.open ?? 0;
  const closedOut = considered - x.reached - open;
  const list: Figure[] = [{ name: "considered", value: considered }];
  if (closedOut > 0) list.push({ name: "closedOut", value: closedOut });
  if (open > 0) list.push({ name: "open", value: open });
  if (closedOut > 0 || open > 0) list.push({ name: "closed", value: x.reached });
  // A send-back's sentence says the rest's win rate, not how many they were.
  const sentBack = f.subjects.filter((s) => s.type === "stage").length === 2;
  list.push({ name: "groupDeals", value: x.deals }, { name: "groupWinRate", value: x.winRate }, { name: "otherWinRate", value: x.otherWinRate }, ...(sentBack ? [] : [{ name: "otherDeals" as const, value: x.otherDeals }]));
  if (x.wonMedianDays !== undefined && x.otherWonMedianDays !== undefined) list.push({ name: "groupWonMedianDays", value: x.wonMedianDays }, { name: "otherWonMedianDays", value: x.otherWonMedianDays });
  if (x.held !== undefined && x.compared !== undefined) list.push({ name: "segmentsHeld", value: x.held }, { name: "segmentsCompared", value: x.compared });
  // A note after the insight's own sentences (how many record types the deals use, say) can put a count in too.
  const spans = spansIfCurrent(f.summary, f.orgWords?.summary);
  if (!spans) return undefined;
  const end = f.summary.indexOf(NOT_A_CAUSE);
  if (end < 0) return undefined;
  const cut = end + NOT_A_CAUSE.length;
  const own = saidNumbers(f.summary.slice(0, cut), spans.filter((s) => s.end <= cut));
  const note = saidNumbers(f.summary.slice(cut), spans.filter((s) => s.start >= cut).map((s) => ({ start: s.start - cut, end: s.end - cut })));
  const all = [...list, ...note.map((value) => ({ name: "inNote" as const, value }))];
  return own.length === list.length && own.every((n, i) => n === list[i]!.value) ? all : undefined;
}

/** Why nothing is said, read from the engine's own sentence (insights.ts and variants.ts): its kind, or `other`. */
export function notStatedKind(text: string): NotStatedKind {
  if (/^We couldn't tell whether skipping a stage or being sent back went with winning more or less often\./.test(text)) return "no_gap_found";
  if (/^No deal skipped a stage of the usual path or was sent back to an earlier stage/.test(text)) return "nothing_to_compare";
  if (/^No skipped stage or move back had enough won and lost deals on both sides/.test(text)) return "too_few_won_and_lost";
  if (/^We couldn't compare any skipped stage or move back\./.test(text)) {
    // Each reason said once, the slowest first, as the rule writes them.
    if (/ took so long to close that none made since .+ has had time to, so none could be counted yet\./.test(text)) return "too_slow_to_close";
    if (/, too many are still open\./.test(text)) return "too_many_open";
    return "too_few_won_and_lost";
  }
  if (/^(Opportunities|OpportunityHistory) couldn't be read: /.test(text)) return "deals_not_read";
  // An automatic run's words for the same reasons (automatic-words.ts) are read alike.
  if (/^(The calibration doesn't say|You haven't told us) what these stages mean: /.test(text)) return "stages_not_mapped";
  if (/^Stage history isn't tracked for these deals: /.test(text)) return "history_not_tracked";
  if (/^\d+ closed deals with stage history is below (?:the calibrated|our) minimum of \d+\./.test(text)) return "too_few_closed";
  if (/^\d+ won deals with stage history is below (?:the calibrated|our) minimum of \d+\./.test(text)) return "too_few_won";
  if (/^Each of the \d+ closed deals took a different path through your stages/.test(text)) return "every_path_different";
  if (/^Each of the \d+ won deals took a different path through your stages/.test(text)) return "no_usual_path";
  return "other";
}

/**
 * What would let the engine say more, only where it is plainly true and every
 * number in it is one the reason already says: the minimum of closed or won
 * deals with stage history, against how many there were. Ours whole: only
 * numbers are put in. None for any other kind.
 */
export function unlockOf(kind: NotStatedKind, reason: string): string | undefined {
  const m = /^(\d+) (?:closed|won) deals with stage history is below (?:the calibrated|our) minimum of (\d+)\./.exec(reason);
  if (!m) return undefined;
  const [found, needed] = [Number(m[1]), Number(m[2])];
  if (kind === "too_few_closed") return said`We need at least ${needed} closed deals with stage history to compare the paths deals take, and found ${found}.`;
  if (kind === "too_few_won") return said`We need at least ${needed} won deals with stage history to compare the paths deals take, and found ${found}.`;
  return undefined;
}

/** A text and its marks as the engine made them, before the guard. */
type Made = { text: string; marks: SentenceOrgWords | null | undefined };
type MadeInsight = { insight: Omit<DigestPathInsight, "orgWords">; words: { stages: Made[]; title: Made; summary: Made; recommendation: Made } };
type MadeBlock = { insights: MadeInsight[]; notStated?: { kind: NotStatedKind; reason: Made; unlock?: Made }; relatedFindingIds: string[] };

function made(report: BuiltReport): MadeBlock | null {
  const all = report.pathInsights ?? [];
  if (all.length === 0) return null;
  const raised = insightOrder(all.filter((f) => f.status === "issue"));
  const insights = raised.map((f): MadeInsight => {
    const stages = f.subjects.filter((s) => s.type === "stage").map((s) => s.label);
    const figures = figuresOf(f);
    return {
      insight: { id: f.id, kind: stages.length === 2 ? "sent_back" : "skipped", stages, title: f.title, summary: f.summary, recommendation: f.recommendation, confidence: f.confidence, ...(figures ? { figures } : {}) },
      words: {
        stages: stages.map((s) => ({ text: s, marks: stageWords(s) })),
        title: { text: f.title, marks: marked(f.title, f.orgWords?.title) },
        summary: { text: f.summary, marks: marked(f.summary, f.orgWords?.summary) },
        recommendation: { text: f.recommendation, marks: marked(f.recommendation, f.orgWords?.recommendation) },
      },
    };
  });
  const issues = new Set(report.teamFindings.filter((f) => f.status === "issue" && PATH_RELATED_RULES.has(f.ruleId)).map((f) => f.id));
  const relatedFindingIds = report.teamFindings.filter((f) => issues.has(f.id)).map((f) => f.id);
  if (insights.length > 0) return { insights, relatedFindingIds };
  const f = all[0]!;
  const nm = f.status === "not_measurable" && f.notMeasurableReason;
  const reason: Made = nm ? { text: f.notMeasurableReason!, marks: marked(f.notMeasurableReason!, f.orgWords?.notMeasurableReason) } : { text: f.summary, marks: marked(f.summary, f.orgWords?.summary) };
  const kind = notStatedKind(reason.text);
  const unlock = unlockOf(kind, reason.text);
  return { insights, notStated: { kind, reason, ...(unlock ? { unlock: { text: unlock, marks: ENGINE_WORDS } } : {}) }, relatedFindingIds };
}

/** The block before the guard: codes, ids and the texts as the findings have them. Null when the run made no path analysis. */
export function digestPaths(report: BuiltReport): DigestPaths | null {
  const m = made(report);
  if (!m) return null;
  const top = m.insights[0]?.insight;
  return {
    insights: m.insights.map((i) => i.insight),
    ...(top ? { headline: { insightId: top.id, text: top.title } } : {}),
    ...(m.notStated ? { notStated: { kind: m.notStated.kind, reason: m.notStated.reason.text, ...(m.notStated.unlock ? { unlock: m.notStated.unlock.text } : {}) } } : {}),
    relatedFindingIds: m.relatedFindingIds,
  };
}

/**
 * The block's texts marked, each only while it reads as the engine made it: as
 * the finding's marks have it, a stage as the org's word whole, the headline as
 * its insight's title. One the guard replaced with the fixed sentence is ours,
 * and an insight whose summary went takes its figures with it; anything else
 * the guard changed is left unknown, for the checked digest to withhold.
 */
export function pathsWords(after: DigestPaths, report: BuiltReport): DigestPaths {
  const m = made(report)!;
  const words = (was: Made | undefined, now: string): SentenceOrgWords | undefined =>
    was && was.text === now ? (was.marks ?? undefined) : now === NAMED_PEOPLE_SENTENCE ? ENGINE_WORDS : undefined;
  const insights = after.insights.map((p, i): DigestPathInsight => {
    const w = m.insights[i]!.words;
    const stages = p.stages.map((s, k) => words(w.stages[k], s) ?? null);
    const title = words(w.title, p.title);
    const summary = words(w.summary, p.summary);
    const recommendation = words(w.recommendation, p.recommendation);
    const { figures: _f, ...bare } = p;
    const kept = p.summary === w.summary.text ? p : bare;
    return { ...kept, orgWords: { stages, ...(title ? { title } : {}), ...(summary ? { summary } : {}), ...(recommendation ? { recommendation } : {}) } };
  });
  const headline = after.headline && (() => {
    const text = words(m.insights[0]!.words.title, after.headline.text);
    return { ...after.headline, ...(text ? { orgWords: { text } } : {}) };
  })();
  const notStated = after.notStated && m.notStated && (() => {
    const reason = words(m.notStated.reason, after.notStated.reason);
    const unlock = after.notStated.unlock === undefined ? undefined : words(m.notStated.unlock, after.notStated.unlock);
    const o = { ...(reason ? { reason } : {}), ...(unlock ? { unlock } : {}) };
    return { ...after.notStated, ...(Object.keys(o).length ? { orgWords: o } : {}) };
  })();
  return { ...after, insights, ...(headline ? { headline } : {}), ...(notStated ? { notStated } : {}) };
}

/** The block's ids and codes by full path, never by a key's name (as readiness's, 742): the guard and the checker skip them. */
export const PATHS_CODE_PATHS: ReadonlySet<string> = new Set([
  "paths.insights[].id", "paths.insights[].kind", "paths.insights[].confidence", "paths.insights[].figures[].name",
  "paths.headline.insightId", "paths.notStated.kind", "paths.relatedFindingIds",
]);
