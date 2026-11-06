import { ASSUMPTIONS } from "./assumptions.ts";
import type { FactsFile } from "./summary.ts";
import type { z } from "zod";

// QUESTIONS.md against the facts runs found (DECISIONS.md 293). Each item's id is
// its section's short name and its number or letter, such as `cpq.a` or `pay.49`:
// numbers repeat across sections, so the section is part of the id.

/** Each section's short name, by its heading without the bracketed reference. A new section needs one. */
export const SECTION_IDS: Record<string, string> = {
  "For Coby": "coby",
  "To confirm against a real org": "real-org",
  "The connection": "connection",
  "Published research": "research",
  "What they pay": "pay",
  Adoption: "adoption",
  "Your org, documented": "documented",
  "Field names checked against Salesforce's docs": "fields",
  "The team survey and the run": "survey",
  "Service Cloud": "service",
  "Integrations and technical health": "integrations",
  "Action lists": "action-lists",
  "Roadmap 3.2's remaining checks": "checks-3-2",
  "Salesforce CPQ": "cpq",
  "AI readiness": "ai",
  "Account Engagement": "ae",
  "Sharing the Drive folder": "drive",
};

export type QuestionItem = { id: string; section: string; text: string };

const heading = (line: string): string => line.replace(/^##\s+/, "").replace(/\s*\(.*\)\s*$/, "").trim();
const firstSentence = (text: string): string => {
  const plain = text.replace(/`/g, "").trim();
  const end = plain.search(/[.?](\s|$)/);
  const sentence = end > 0 ? plain.slice(0, end + 1) : plain;
  return sentence.length > 160 ? `${sentence.slice(0, 157)}...` : sentence;
};

/** Every item: numbered table rows and lettered lines. A section with neither is one item. */
export function parseQuestions(markdown: string): QuestionItem[] {
  const items: QuestionItem[] = [];
  let section: { name: string; id: string; prose: string; count: number } | null = null;
  const close = (): void => {
    if (section && section.count === 0 && section.prose) items.push({ id: section.id, section: section.name, text: firstSentence(section.prose) });
  };
  for (const line of markdown.split("\n")) {
    if (line.startsWith("## ")) {
      close();
      const name = heading(line);
      section = { name, id: SECTION_IDS[name] ?? name.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, ""), prose: "", count: 0 };
      continue;
    }
    if (!section) continue;
    const row = /^\|\s*(\d+)\s*\|\s*(.+?)\s*\|/.exec(line);
    const lettered = /^([a-z])\)\s+(.+)$/.exec(line);
    if (row || lettered) {
      const [, key, text] = (row ?? lettered)!;
      items.push({ id: `${section.id}.${key}`, section: section.name, text: firstSentence(text!) });
      section.count++;
    } else if (!section.prose && line.trim() && !line.startsWith("|")) {
      section.prose = line;
    }
  }
  close();
  return items;
}

export type QuestionStatus = "contradicted" | "settled" | "partly_settled" | "open" | "not_checked" | "not_a_fact";

type FactsJson = z.infer<typeof FactsFile>;
export type Answer = { item: QuestionItem; status: QuestionStatus; lines: string[] };

/** Each item's status across one or more facts files. */
export function answerQuestions(items: QuestionItem[], runs: Array<{ name: string; facts: FactsJson }>): Answer[] {
  return items.map((item) => {
    const assumed = ASSUMPTIONS.filter((a) => a.question === item.id);
    if (assumed.length === 0) return { item, status: "not_a_fact", lines: [] };
    const seen = runs.flatMap((r) => r.facts.facts.filter((f) => f.question === item.id).map((f) => ({ run: r.name, f })));
    const by = (outcome: string) => seen.filter((s) => s.f.outcome === outcome);
    const of = (n: number): string => `${n} of ${runs.length} ${runs.length === 1 ? "run" : "runs"}`;
    const runsWith = (list: typeof seen): number => new Set(list.map((s) => s.run)).size;
    const contradicted = by("contradicted");
    if (contradicted.length) {
      const details = [...new Set(contradicted.map((s) => s.f.detail))];
      const affects = [...new Set(contradicted.flatMap((s) => s.f.affects))];
      return { item, status: "contradicted", lines: [`Contradicted in ${of(runsWith(contradicted))}: ${details.join(" ")}`, ...(affects.length ? [`The checks that rest on it: ${affects.join(", ")}.`] : [])] };
    }
    const confirmed = by("confirmed");
    if (confirmed.length) {
      const all = assumed.every((a) => confirmed.some((s) => s.f.id === a.id));
      const whole = all && assumed.every((a) => a.covers === "whole");
      return {
        item,
        status: whole ? "settled" : "partly_settled",
        lines: whole
          ? [`Confirmed in ${of(runsWith(confirmed))}.`]
          : [`Confirmed in ${of(runsWith(confirmed))}: ${[...new Set(confirmed.map((s) => s.f.says))].join(" ")}`, all ? "The rest of the item is still open." : "The rest of the item, and the facts not seen yet, are still open."],
      };
    }
    const open = by("open");
    if (open.length) return { item, status: "open", lines: [`Checked in ${of(runsWith(open))}, and not seen yet: ${[...new Set(open.map((s) => s.f.detail))].join(" ")}`] };
    const reasons = [...new Set(seen.map((s) => s.f.detail))];
    return { item, status: "not_checked", lines: [reasons.length ? `Not checked: ${reasons.join(" ")}` : "No facts file had this check."] };
  });
}

const TITLES: Record<Exclude<QuestionStatus, "not_a_fact">, string> = {
  contradicted: "Contradicted: update QUESTIONS.md, and the check that assumed it",
  settled: "Settled",
  partly_settled: "Partly settled",
  open: "Checked, still open",
  not_checked: "Not checked in these runs",
};

export function questionsReport(answers: Answer[], runs: Array<{ name: string; facts: FactsJson }>): string {
  const kinds = [...new Set(runs.map((r) => `${r.facts.connectionKind}, API ${r.facts.apiVersion}`))];
  const out = ["# QUESTIONS.md against the facts", "", `From ${runs.length} ${runs.length === 1 ? "run" : "runs"}: ${runs.map((r) => r.name).join(", ")} (${kinds.join("; ")}).`, ""];
  for (const status of Object.keys(TITLES) as Array<keyof typeof TITLES>) {
    const these = answers.filter((a) => a.status === status);
    if (these.length === 0) continue;
    out.push(`## ${TITLES[status]} (${these.length})`, "");
    for (const a of these) out.push(`- **${a.item.id}** ${a.item.text}`, ...a.lines.map((l) => `  ${l}`));
    out.push("");
  }
  const rest = answers.filter((a) => a.status === "not_a_fact");
  if (rest.length) out.push(`## Not something the facts step answers (${rest.length})`, "", rest.map((a) => a.item.id).join(", "), "");
  return out.join("\n");
}
