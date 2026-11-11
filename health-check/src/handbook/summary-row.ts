import { z } from "zod";
import { Timestamp } from "../schemas/common.ts";
import { plainText } from "./plain-text.ts";
import type { Handbook } from "./schema.ts";
import type { HandbookSummaries } from "./summaries.ts";

// Claude's summaries of the Apex code as the console keeps them, in its
// HealthCheckHandbookSummaries table (DECISIONS.md 494 and 495). The worker
// writes a row while the summaries wait for a person, and reads it back when
// it prints the org handbook. The console is the reader and the stricter: a
// row that breaks its schema reads as unreadable there and holds the run's
// release, so the worker checks the same schema before it writes one, and
// again when it reads one back.

/**
 * blackcloud-next lib/health-check/schemas.ts, HANDBOOK_SUMMARY_LIMITS, word
 * for word. Change the two together: the tests hold the engine's own limits
 * inside these, so raising one there fails here first.
 */
export const CONSOLE_SUMMARY_LIMITS = { items: 250, name: 255, summary: 1200, model: 100 } as const;

const LIMITS = CONSOLE_SUMMARY_LIMITS;

// The console's HandbookSummaryItem, HandbookSummariesLeftOut and HandbookSummaries, as written there.
const ConsoleSummaryItem = z.object({ kind: z.enum(["class", "trigger"]), name: z.string().min(1).max(LIMITS.name), summary: z.string().min(1).max(LIMITS.summary), struck: z.boolean() }).strict();
const SummaryCount = z.number().int().nonnegative();
const ConsoleSummariesLeftOut = z.object({ tests: SummaryCount, tooLarge: SummaryCount, overLimit: SummaryCount, unreadable: SummaryCount, refused: SummaryCount, notWritten: SummaryCount }).strict();
const ConsoleSummariesStatus = z.enum(["waiting", "approved", "left_out"]);
const ConsoleRowShape = z
  .object({
    runId: z.string().min(1),
    status: ConsoleSummariesStatus,
    model: z.string().min(1).max(LIMITS.model),
    madeAt: Timestamp,
    items: z.array(ConsoleSummaryItem).max(LIMITS.items),
    leftOut: ConsoleSummariesLeftOut,
    decidedBy: z.string().min(1).nullable(),
    decidedAt: Timestamp.nullable(),
  })
  .strict();

/** The console's whole row: waiting exactly when nobody and no time decided it, and the two are null together. */
export const ConsoleSummariesRow = ConsoleRowShape.refine((r) => (r.decidedBy === null) === (r.decidedAt === null) && (r.status === "waiting") === (r.decidedBy === null && r.decidedAt === null));
export type ConsoleSummariesRow = z.infer<typeof ConsoleSummariesRow>;

/**
 * The row as the documents job reads it: the same schema with only what it
 * prints from. Never who decided it or when (decision 495), which the
 * database's own checks hold the status to; and not the model or the time,
 * so a timestamp read back in a form we didn't expect can't make an approved
 * row read as none.
 */
const StoredRow = ConsoleRowShape.omit({ decidedBy: true, decidedAt: true, model: true, madeAt: true }).strict();

/** What the run job hands the store: handbook-summaries.json's summaries and counts, never the code. */
export type SummariesToSave = Pick<HandbookSummaries, "model" | "madeAt" | "items" | "leftOut">;

/** What the documents job is given: the decision and the text, never who made it. */
export type StoredSummaries = Pick<ConsoleSummariesRow, "status" | "items" | "leftOut">;

/** Problems in a parse, counted: an issue's message can quote the value. */
const issuesOf = (error: z.ZodError): number => error.issues.length;

/**
 * The row the worker writes: waiting, nobody and no time decided, nothing
 * struck, checked against the console's schema. Only summaries that could be
 * printed as they stand go in, by printableText, under a name that's plain
 * text: anything else would be refused by the database (a U+0000 or a lone
 * surrogate) or changed by printing, and a file written before the summary
 * route checked for them can hold one. Each left out is counted as not
 * written, so the counts still add up to what was read. With none left, there
 * is no row: `row` is null.
 */
export function waitingRow(runId: string, summaries: SummariesToSave): { ok: true; row: ConsoleSummariesRow | null; dropped: number } | { ok: false; issues: number } {
  const kept = summaries.items.filter((i) => printableText(i.summary) && plainText(i.name));
  const dropped = summaries.items.length - kept.length;
  if (kept.length === 0) return { ok: true, row: null, dropped };
  const parsed = ConsoleSummariesRow.safeParse({
    runId,
    status: "waiting",
    model: summaries.model,
    madeAt: summaries.madeAt,
    items: kept.map((i) => ({ kind: i.kind, name: i.name, summary: i.summary, struck: false })),
    leftOut: { ...summaries.leftOut, notWritten: summaries.leftOut.notWritten + dropped },
    decidedBy: null,
    decidedAt: null,
  });
  return parsed.success ? { ok: true, row: parsed.data, dropped } : { ok: false, issues: issuesOf(parsed.error) };
}

/** A row read back, held to the console's schema. One that fails reads as none. */
export function storedFromRow(row: unknown): { ok: true; stored: StoredSummaries } | { ok: false; issues: number } {
  const parsed = StoredRow.safeParse(row);
  if (!parsed.success) return { ok: false, issues: issuesOf(parsed.error) };
  const { status, items, leftOut } = parsed.data;
  return { ok: true, stored: { status, items, leftOut } };
}

/** The most a summary may hold when it's printed: the most the worker ever writes (SUMMARY_LIMITS.characters). */
export const PRINT_MAX = 1_200;

/**
 * Whether a summary can be printed as it stands. It was checked when Claude
 * wrote it, and is printed weeks later from a database the console also
 * writes, so the checks that matter on paper are made again (decision 495):
 * nothing in double square brackets, since anything that restores names would
 * turn a token into a person's name; no line break, which Word splits into
 * lines; no character Word can't hold, which printing would change; something
 * there; and no more than was ever written.
 */
export function printableText(summary: string): boolean {
  return plainText(summary) && !summary.includes("[[") && !/[\r\n\u2028\u2029]/.test(summary) && summary.trim() !== "" && summary.length <= PRINT_MAX;
}

/** The summaries the handbook prints, and how many of the rest were left out and why, worked out here and only counted in the document. */
export type PrintedSummaries = {
  items: Array<{ kind: "class" | "trigger"; name: string; summary: string }>;
  counts: { tests: number; tooLarge: number; overLimit: number; rest: number };
};

/**
 * What the handbook prints from the row (decision 495): nothing unless a
 * person approved it, and of that, each summary not struck out, printable as
 * it stands, and naming exactly one class or trigger of its kind in
 * handbook.json. Salesforce's names are unique within a kind for the code we
 * read (no namespace), so two entries alike come only from a file that's
 * wrong, and then neither is printed; nor is either of two summaries for the
 * same entry. Everything not printed is counted with the rest. Null when
 * nothing prints, so the document is as it would be without summaries.
 */
export function printableSummaries(handbook: Handbook, stored: StoredSummaries | null | undefined): PrintedSummaries | null {
  if (!stored || stored.status !== "approved") return null;
  const names = { class: handbook.apexClasses, trigger: handbook.apexTriggers };
  const entries = (kind: "class" | "trigger", name: string): number => {
    const part = names[kind];
    return part.status === "ok" ? part.items.filter((i) => i.name === name).length : 0;
  };
  const key = (i: { kind: string; name: string }): string => JSON.stringify([i.kind, i.name]);
  const candidates = stored.items.filter((i) => !i.struck && printableText(i.summary) && entries(i.kind, i.name) === 1);
  const per = new Map<string, number>();
  for (const i of candidates) per.set(key(i), (per.get(key(i)) ?? 0) + 1);
  const items = candidates.filter((i) => per.get(key(i)) === 1).map(({ kind, name, summary }) => ({ kind, name, summary }));
  if (items.length === 0) return null;
  const { tests, tooLarge, overLimit, unreadable, refused, notWritten } = stored.leftOut;
  return { items, counts: { tests, tooLarge, overLimit, rest: unreadable + refused + notWritten + stored.items.length - items.length } };
}
