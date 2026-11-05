import { AlignmentType, Document, Footer, HeadingLevel, Packer, PageNumber, Paragraph, TextRun } from "docx";
import { colour, font } from "../design/tokens.ts";
import type { BuiltReport } from "../narrative/sections.ts";
import { domainLabel } from "../narrative/select.ts";
import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import { compactMoney, longDate, plainWords, ukDates } from "./format.ts";

// A case study draft (roadmap Stage 5), for a re-check whose customer agreed at
// intake to be a case study. It's a starting point for the consultant, in Black
// Cloud's own folder: the customer approves the words and every figure before
// anything is published, and their quote is theirs to give.

export type CaseStudy = {
  customer: string;
  headline: string;
  challenge: string[];
  found: string[];
  changed: string[];
  fixed: string[];
  beforePublishing: string[];
};

const FIXED_LISTED = 6;

/** Null unless the run re-checks an earlier one and the customer agreed to be a case study. */
export function caseStudyDraft(report: BuiltReport): CaseStudy | null {
  const p = report.progress;
  const intake = report.input.intake;
  if (!p || intake?.caseStudyConsent !== true) return null;
  const m = (n: number): string => compactMoney(n, report.value.currency);
  const customer = intake.customerName ?? report.input.run.customerRef ?? "The customer";
  const scored = p.score.before !== null && p.score.after !== null;
  const saved = p.worth.before - p.worth.after;
  const headline = scored && p.score.after! > p.score.before! ? `From a Barometer score of ${p.score.before} to ${p.score.after}` : saved > 0 ? `${m(saved)} a year no longer being missed` : "What changed after their health check";
  const moved = p.areas.filter((a) => a.before !== null && a.after !== null && a.after > a.before).sort((a, b) => b.after! - b.before! - (a.after! - a.before!));
  return {
    customer,
    headline,
    challenge: intake.statedProblems.length ? intake.statedProblems.map((s) => s.statement) : [intake.summary],
    found: [
      ...(p.score.before !== null ? [`A Barometer score of ${p.score.before} out of 100.`] : []),
      `Issues worth about ${m(p.worth.before)} over 12 months.`,
      `${p.fixed.length + p.remaining.length + p.unknown.length} kinds of issue across their Salesforce.`,
    ],
    changed: [
      ...(scored ? [`The Barometer score went from ${p.score.before} to ${p.score.after}.`] : []),
      saved > 0 ? `What the remaining issues are worth fell from ${m(p.worth.before)} to ${m(p.worth.after)}: ${m(saved)} no longer being missed. Some of that can come from the business itself, not only from fixes.` : `What the remaining issues are worth is ${m(p.worth.after)}, from ${m(p.worth.before)}.`,
      `${p.fixed.length} kinds of issue fixed${p.added.length ? `, and ${p.added.length} new ones found and planned` : ""}.`,
      ...(moved.length ? [`Biggest improvements: ${moved.slice(0, 3).map((a) => `${domainLabel(a.domain)} (${a.before} to ${a.after})`).join(", ")}.`] : []),
    ],
    fixed: p.fixed.slice(0, FIXED_LISTED).map((c) => c.title),
    beforePublishing: [
      "Send the draft to the customer and get their written approval of the words and every figure.",
      "Ask for their quote. Never write one for them.",
      "Take out anything they'd rather not share, including their name if they want to stay anonymous.",
      "Check each figure against this re-check's executive summary, and keep the line about the business's own part in the change.",
    ],
  };
}

const hex = (value: string): string => value.replace("#", "");

/** The draft as Word, for the consultant to edit. Null when there's no case study to draft. */
export async function renderCaseStudyDocx(report: BuiltReport, pseudonyms: Pseudonymiser): Promise<Buffer | null> {
  const study = caseStudyDraft(report);
  if (!study) return null;
  const say = (text: string): string => plainWords(ukDates(pseudonyms.restore(text)));
  const run = (text: string, options: { bold?: boolean; colour?: string; size?: number; italics?: boolean } = {}): TextRun =>
    new TextRun({ text: say(text), bold: options.bold, italics: options.italics, font: font.family, size: options.size ?? 22, color: options.colour ?? hex(colour.carbon) });
  const heading = (text: string, level: 1 | 2): Paragraph =>
    new Paragraph({ heading: level === 1 ? HeadingLevel.HEADING_1 : HeadingLevel.HEADING_2, keepNext: true, spacing: { before: level === 1 ? 0 : 280, after: 100 }, children: [run(text, { bold: true, size: level === 1 ? 34 : 26 })] });
  const bullet = (text: string): Paragraph => new Paragraph({ bullet: { level: 0 }, spacing: { after: 60 }, children: [run(text)] });
  const blocks: Paragraph[] = [
    new Paragraph({ spacing: { after: 160 }, children: [run("Draft. Black Cloud only until the customer has approved it.", { bold: true, colour: hex(colour.alert) })] }),
    heading(`${study.customer}: ${study.headline}`, 1),
    new Paragraph({ spacing: { after: 200 }, children: [run(`From their Black Cloud Barometer health check and its re-check on ${longDate(report.input.run.asOf)}.`, { colour: hex(colour.muted) })] }),
    heading("The challenge", 2),
    ...study.challenge.map(bullet),
    heading("What we found", 2),
    ...study.found.map(bullet),
    heading("What changed", 2),
    ...study.changed.map(bullet),
    ...(study.fixed.length ? [heading("What they fixed", 2), ...study.fixed.map(bullet)] : []),
    heading("In their words", 2),
    new Paragraph({ spacing: { after: 120 }, children: [run("[Their quote, in their words, once they've given it.]", { italics: true, colour: hex(colour.muted) })] }),
    heading("Before publishing", 2),
    ...study.beforePublishing.map(bullet),
  ];
  const doc = new Document({
    title: "Case study draft",
    creator: "Black Cloud",
    styles: { default: { document: { run: { font: font.family, size: 22, color: hex(colour.carbon) } } } },
    sections: [
      {
        properties: { page: { size: { width: 11906, height: 16838 }, margin: { top: 1200, bottom: 1200, left: 1300, right: 1300 } } },
        footers: { default: new Footer({ children: [new Paragraph({ alignment: AlignmentType.RIGHT, children: [new TextRun({ text: "Draft, not approved by the customer. Page ", font: font.family, size: 16, color: hex(colour.muted) }), new TextRun({ children: [PageNumber.CURRENT], font: font.family, size: 16, color: hex(colour.muted) })] })] }) },
        children: blocks,
      },
    ],
  });
  return Buffer.from(await Packer.toBuffer(doc));
}
