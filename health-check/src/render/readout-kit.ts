import { AlignmentType, Document, Footer, HeadingLevel, Packer, PageNumber, Paragraph, TextRun } from "docx";
import { comparisonId, differenceWords, formatFigure } from "../benchmark/compare.ts";
import { colour, font } from "../design/tokens.ts";
import { INVESTMENT } from "../money/investment.ts";
import type { BuiltReport } from "../narrative/sections.ts";
import { areasOf, bySeverity, buildPlan, domainLabel, isSetupReview, scorecard, topIssues, verdictOf } from "../narrative/select.ts";
import { TOP_ISSUE_COUNT } from "../narrative/sections.ts";
import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import { areaWeight } from "../domains/scoring.ts";
import { compactMoney, longDate, plainWords, ukDates } from "./format.ts";
import { returnOnSpend } from "./money.ts";
import { noteSections, referenceMap, valueItemId } from "./references.ts";

// The readout kit (roadmap Stage 5): talking points for the consultant's live
// readout, never for the customer. For each part of the executive summary, the
// points to make, the questions the customer is likely to ask with where the
// answer is, and what not to promise. All engine text from the report's own
// figures and wording: no Claude yet.

/** Where an answer is: a finding, metric, value group or comparison by its id, or a raw notes section as "section:<key>". */
export type KitQuestion = { question: string; answers: string[] };
export type KitPart = { key: string; title: string; points: string[]; questions: KitQuestion[]; notPromise: string[] };

const section = (key: string): string => `section:${key}`;
const firstSentence = (text: string): string => /^.*?[.!?](?=\s|$)/.exec(text)?.[0] ?? text;
const plural = (n: number, one: string, many = `${one}s`): string => `${n} ${n === 1 ? one : many}`;

// What the money never is, wherever a figure in money is said out loud (DECISIONS.md 50 and 88).
const POTENTIAL = "Every figure in money is potential, worked out from their own data: what's there if every case were won back, of which we count only a share. Don't present it as a forecast, a guarantee or an uplift we'll deliver.";

/** The kit's parts, in the order the executive summary runs. A part the summary doesn't have isn't here either. */
export function readoutKit(report: BuiltReport): KitPart[] {
  const { run } = report.input;
  const team = report.teamFindings;
  const m = (n: number): string => compactMoney(n, report.value.currency);
  const verdict = verdictOf(team);
  const top = topIssues(team, TOP_ISSUE_COUNT);
  const o = report.overall;
  const parts: KitPart[] = [];

  parts.push({
    key: "verdict",
    title: "Verdict",
    points: [
      `${verdict.headline}.`,
      o.status === "measured" ? `Barometer score ${o.score} out of 100, grade ${o.grade}${o.capped ? ": a critical issue holds it there, however well the rest does" : ""}.` : `There's no single score this time: ${o.reason}`,
      `${plural(verdict.issues, "issue")}: ${verdict.critical} critical and ${verdict.high} high.${top[0] ? ` The one that matters most: ${top[0].title}.` : ""}`,
    ],
    questions: [
      { question: "How is the score worked out?", answers: [section("barometer_score")] },
      { question: "Which issues matter most, and why?", answers: top.slice(0, 3).map((f) => f.id) },
    ],
    notPromise: ["The score is our measure, from their own org, not an industry ranking. Don't say where it puts them against others: that's the comparison page's job, with its sources."],
  });

  const p = report.progress;
  if (p) {
    parts.push({
      key: "progress",
      title: "Since the last health check",
      points: [
        p.score.before !== null && p.score.after !== null ? `The Barometer score has gone from ${p.score.before} to ${p.score.after}.` : "We couldn't score both checks, so the change is told by rule rather than by score.",
        `${plural(p.fixed.length, "rule")} fixed, ${p.added.length} new, ${p.remaining.length} still open${p.unknown.length ? `, and ${p.unknown.length} we couldn't measure this time` : ""}.`,
        `What the open issues are worth has gone from ${m(p.worth.before)} to ${m(p.worth.after)}.`,
      ],
      questions: [
        { question: "What did we fix?", answers: p.fixed.slice(0, 3).flatMap((c) => c.findingIds.slice(0, 1)) },
        { question: "What's new since then?", answers: p.added.slice(0, 3).flatMap((c) => c.findingIds.slice(0, 1)) },
      ],
      notPromise: ["Some of the change in money can come from the business itself, not only from fixes. Don't credit all of it to the work done."],
    });
  }

  const value = report.value;
  if (value.total.expected > 0) {
    const groups = value.groups.filter((g) => g.kind !== "exposure").sort((a, b) => b.counted.expected - a.counted.expected);
    const exposure = value.groups.filter((g) => g.kind === "exposure").sort((a, b) => b.counted.annual - a.counted.annual)[0];
    parts.push({
      key: "money",
      title: "What fixing this is worth",
      points: [
        `${m(value.total.expected)} over the next 12 months, expected, between ${m(value.total.low)} and ${m(value.total.high)}.`,
        groups[0] ? `The largest part: ${groups[0].label}, ${m(groups[0].counted.expected)} expected.` : "",
        exposure ? `Kept apart and never added: ${exposure.counted.title.replace(/\.$/, "")} (${m(exposure.counted.annual)}). That's exposure, not money found.` : "",
      ].filter(Boolean),
      questions: [
        { question: "Where does the figure come from?", answers: groups.slice(0, 3).map((g) => valueItemId(g.id)) },
        { question: "Why only a share of each figure?", answers: [section("value_model")] },
      ],
      notPromise: [POTENTIAL, "Exposure is money at risk, not money found. Never add it to the total, even to make a point."],
    });
  }

  const paid = report.metrics.find((x) => x.id === INVESTMENT.seatsPaid);
  if (paid?.status === "measured") {
    const cost = report.investment.annualCost;
    const ratio = returnOnSpend(report);
    parts.push({
      key: "investment",
      title: "Your investment",
      points: [
        `They pay for ${paid.value} seats.${cost.status === "measured" ? ` That's ${m(cost.value)} a year: ${cost.label.toLowerCase()}.` : " We don't have a figure for what they pay a year."}`,
        ratio !== null ? `What fixing the issues is worth is ${ratio} times what they pay for Salesforce a year.` : "",
      ].filter(Boolean),
      questions: [
        { question: "How many of our seats are used?", answers: [INVESTMENT.seatsUsed, INVESTMENT.seatsUnused] },
        { question: "How did you price our seats?", answers: [INVESTMENT.seatCost] },
      ],
      notPromise: ["The multiple sets a 12-month potential beside a year's cost. It isn't a return on investment, and it isn't what they'll get back."],
    });
  }

  const rows = scorecard(team, report.input.scores ?? [], areasOf(run, team)).filter((r) => r.score !== null);
  const weakest = [...rows].sort((a, b) => areaWeight(b.domain) * (100 - b.score!) - areaWeight(a.domain) * (100 - a.score!))[0];
  parts.push({
    key: "scorecard",
    title: "How each area stands",
    points: [
      weakest ? `${weakest.label} holds the overall score back most for its weight: ${weakest.score} out of 100, with ${plural(weakest.issues, "issue")}.` : "No area could be scored this time.",
      "Each area's score counts towards the overall score by how directly it affects revenue.",
    ],
    questions: [
      { question: "Why do some areas count for more?", answers: [section("barometer_score")] },
      { question: `What's behind the ${weakest?.label ?? "weakest"} score?`, answers: weakest ? team.filter((f) => f.domain === weakest.domain && f.status === "issue").sort(bySeverity).slice(0, 2).map((f) => f.id) : [section("barometer_score")] },
    ],
    notPromise: [],
  });

  const compared = isSetupReview(run) ? [] : report.comparisons.filter((c) => c.value !== null && (c.published.length > 0 || c.own.status === "measured"));
  if (compared.length) {
    parts.push({
      key: "compare",
      title: "How you compare",
      points: compared.slice(0, 3).map((c) => {
        const pub = c.published[0];
        const yours = formatFigure(c.value!, c.metric.unit);
        return pub ? `${c.metric.label}: theirs is ${yours}, ${differenceWords(c, pub)} the published ${formatFigure(pub.figure.value, c.metric.unit)} (${pub.figure.source.publisher}, ${pub.figure.source.year}).` : `${c.metric.label}: theirs is ${yours}, ${c.own.status === "measured" ? c.own.position : "with no view yet"} among Black Cloud customers.`;
      }),
      questions: [{ question: "Who are we being compared with?", answers: compared.slice(0, 3).map((c) => comparisonId(c.metric.key)) }],
      notPromise: ["Published research measures other companies in its own way, and we never combine it with our own customers' figures. Don't say they're above or below \"the market\" as one figure."],
    });
  }

  if (!isSetupReview(run)) {
    const metric = (id: string) => report.metrics.find((x) => x.id === id);
    const open = metric("current.open_deals");
    const weighted = metric("current.weighted_pipeline");
    parts.push({
      key: "pipeline",
      title: "Your pipeline today",
      points: [
        open?.status === "measured" ? `${plural(open.value, "open deal")}${metric("current.open_pipeline")?.status === "measured" ? `, ${m((metric("current.open_pipeline") as { value: number }).value)} in all` : ""}.` : "We couldn't measure the open pipeline this time.",
        weighted?.status === "measured" ? `Weighted by how often deals at each stage have won before: ${m(weighted.value)}.` : "",
      ].filter(Boolean),
      questions: [
        { question: "How is the weighted figure worked out?", answers: weighted ? ["current.weighted_pipeline"] : [section("pipeline_metrics")] },
        { question: "Which deals are at risk?", answers: team.filter((f) => f.status === "issue" && f.domain === "current_pipeline").sort(bySeverity).slice(0, 2).map((f) => f.id) },
      ],
      notPromise: ["The weighted pipeline and the delivery outlook come from their own past win rates. They're not a forecast to commit to a board."],
    });
  }

  parts.push({
    key: "top_issues",
    title: `Top ${TOP_ISSUE_COUNT} issues`,
    points: top.slice(0, 3).map((f) => `${f.title}: ${firstSentence(f.recommendation)} Owner: ${f.owner}.`),
    questions: top.slice(0, 3).map((f) => ({ question: `Why does "${f.title}" matter to us?`, answers: [f.id] })),
    notPromise: top.some((f) => f.value.kind !== "not_sized") ? [POTENTIAL] : [],
  });

  const risks = team.filter((f) => f.status === "issue" && f.value.kind === "not_sized" && (f.severity === "critical" || f.severity === "high")).sort(bySeverity);
  if (risks.length) {
    parts.push({
      key: "risk",
      title: "What could hurt you",
      points: risks.slice(0, 2).map((f) => `${f.title} (${f.severity}, ${domainLabel(f.domain)}).`),
      questions: [{ question: "How likely is this to go wrong?", answers: risks.slice(0, 2).map((f) => f.id) }],
      notPromise: ["We don't put a figure on these risks. Don't size them in the room: the cost depends on what goes wrong and when."],
    });
  }

  const phases = buildPlan(team).phases.filter((ph) => ph.findings.length > 0);
  parts.push({
    key: "plan",
    title: "The 90-day plan",
    points: phases.length
      ? phases.slice(0, 2).map((ph) => `Days ${ph.from} to ${ph.to}, ${ph.title.toLowerCase()}: ${plural(ph.findings.length, "action")}, starting with ${ph.findings[0]!.title.replace(/\.$/, "")}.`)
      : ["There's nothing on the plan: say what we'd watch instead."],
    questions: phases[0] ? [{ question: "Who should own the first actions?", answers: phases[0].findings.slice(0, 3).map((f) => f.id) }] : [],
    notPromise: ["The plan's timing is what one team can usually take on. Don't commit Black Cloud to dates or a scope before it's agreed."],
  });

  parts.push({
    key: "next",
    title: "What happens next",
    points: [
      "Agree what matters most to them, and who owns each action in the first month.",
      "We'll look at their Salesforce again after 90 days, so they can see what has improved, in money as well as in the score.",
      "Black Cloud can deliver the plan alongside their team.",
    ],
    questions: [{ question: "What will the re-check measure?", answers: [section("barometer_score"), section("value_model")] }],
    notPromise: ["Don't promise the score will reach a number by the re-check: it moves with what's fixed and with the business."],
  });
  return parts;
}

/** Every answer's raw notes reference, or null where one doesn't resolve. */
export function kitReferences(report: BuiltReport): Map<string, string | null> {
  const sections = noteSections(report);
  const refs = referenceMap(sections);
  const out = new Map<string, string | null>();
  for (const part of readoutKit(report)) {
    for (const q of part.questions) for (const id of q.answers) out.set(id, id.startsWith("section:") ? (sections.find((s) => s.key === id.slice("section:".length))?.ref ?? null) : (refs.get(id) ?? null));
  }
  return out;
}

const hex = (value: string): string => value.replace("#", "");

/** The kit as a Word document, so a consultant can add to it before the readout. Names come back here, as in the raw notes. */
export async function renderReadoutKitDocx(report: BuiltReport, pseudonyms: Pseudonymiser): Promise<Buffer> {
  const say = (text: string): string => plainWords(ukDates(pseudonyms.restore(text)));
  const run = (text: string, options: { bold?: boolean; colour?: string; size?: number } = {}): TextRun => new TextRun({ text: say(text), bold: options.bold, font: font.family, size: options.size ?? 21, color: options.colour ?? hex(colour.carbon) });
  const heading = (text: string, level: 1 | 2 | 3): Paragraph =>
    new Paragraph({ heading: { 1: HeadingLevel.HEADING_1, 2: HeadingLevel.HEADING_2, 3: HeadingLevel.HEADING_3 }[level], keepNext: true, spacing: { before: level === 3 ? 160 : 320, after: 80 }, children: [run(text, { bold: true, size: { 1: 32, 2: 26, 3: 21 }[level] })] });
  const bullet = (children: TextRun[]): Paragraph => new Paragraph({ bullet: { level: 0 }, spacing: { after: 60 }, children });
  const refs = kitReferences(report);
  const { run: r, intake } = report.input;
  const customer = intake?.customerName ?? r.customerRef ?? "Customer";
  const blocks: Paragraph[] = [
    heading(`Readout kit: ${customer}`, 1),
    new Paragraph({ spacing: { after: 120 }, children: [run("Black Cloud only. Never send this to the customer or put it in their folder.", { bold: true, colour: hex(colour.alert) })] }),
    new Paragraph({ spacing: { after: 200 }, children: [run(`For the readout of the health check as of ${longDate(r.asOf)}. Each part follows the executive summary: the points to make, the questions they're likely to ask with where the answer is in the raw notes, and what not to promise.`)] }),
  ];
  for (const part of readoutKit(report)) {
    blocks.push(heading(part.title, 2));
    blocks.push(heading("Points to make", 3), ...part.points.map((pt) => bullet([run(pt)])));
    if (part.questions.length) {
      blocks.push(heading("Questions they may ask", 3));
      for (const q of part.questions) {
        const where = [...new Set(q.answers.map((id) => refs.get(id)).filter((x): x is string => Boolean(x)))];
        blocks.push(bullet([run(q.question), run(where.length ? `  Answer: ${where.join(", ")}` : "  Answer: the full report", { colour: hex(colour.muted) })]));
      }
    }
    if (part.notPromise.length) blocks.push(heading("What not to promise", 3), ...part.notPromise.map((n) => bullet([run(n)])));
  }
  const doc = new Document({
    title: "Readout kit",
    creator: "Black Cloud",
    styles: { default: { document: { run: { font: font.family, size: 21, color: hex(colour.carbon) } } } },
    sections: [
      {
        properties: { page: { size: { width: 11906, height: 16838 }, margin: { top: 1200, bottom: 1200, left: 1300, right: 1300 } } },
        footers: { default: new Footer({ children: [new Paragraph({ alignment: AlignmentType.RIGHT, children: [new TextRun({ text: "Black Cloud only. Page ", font: font.family, size: 16, color: hex(colour.muted) }), new TextRun({ children: [PageNumber.CURRENT], font: font.family, size: 16, color: hex(colour.muted) })] })] }) },
        children: blocks,
      },
    ],
  });
  return Buffer.from(await Packer.toBuffer(doc));
}
