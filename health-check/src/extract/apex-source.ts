import { existsSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { z } from "zod";
import { redactSecrets } from "../privacy/secrets.ts";
import type { ApexScan } from "./apex-scan.ts";

// The Apex code kept at rest for Claude's plain-English summaries (DECISIONS.md
// 490 and 491), only while the worker's switch is on. The Apex step drops each
// body as it reads it, and Claude can only be asked after the analysis, when the
// pseudonym map that masks names exists, so the code waits here between the two.
// It is never under checkpoints/, never uploaded, logged or stored, and deleted
// as soon as the summaries are written, or the run fails for good, or after
// seven days whatever happened (src/worker/retention.ts).

export const APEX_SOURCE_FILE = "apex-source.json";
/** The most classes and triggers kept, together, so the file is bounded however large the org. */
export const APEX_SOURCE_MAX_ITEMS = 200;
/** A body longer than this is left out, not cut short: a summary of half a class would read as one of all of it. */
export const APEX_SOURCE_MAX_BODY = 100_000;

const Count = z.number().int().nonnegative();

export const ApexSourceItem = z.object({ kind: z.enum(["class", "trigger"]), name: z.string().min(1), body: z.string().min(1) }).strict();
export type ApexSourceItem = z.infer<typeof ApexSourceItem>;

export const ApexSource = z
  .object({
    version: z.literal(1),
    /** Worked out from every row the step read, so what was left out can be said in numbers. */
    counts: z.object({ read: Count, tests: Count, tooLarge: Count, overLimit: Count, unreadable: Count }).strict(),
    items: z.array(ApexSourceItem).max(APEX_SOURCE_MAX_ITEMS),
  })
  .strict()
  .refine((s) => s.items.length + s.counts.tests + s.counts.tooLarge + s.counts.overLimit + s.counts.unreadable === s.counts.read, "the counts don't add up to the rows read");
export type ApexSource = z.infer<typeof ApexSource>;

/** One class or trigger as the step read it: its body, still in hand, and the scan made from it. */
export type SourceRow = { kind: "class" | "trigger"; name: string; length: number | null | undefined; body: string | undefined; scan: ApexScan | null };

/**
 * Which classes and triggers may be summarised, and why each other one isn't.
 * A test class is one the scan reads as @isTest on the class itself: the row's
 * own isTest flag matches @isTest anywhere, a comment included, so a class that
 * only mentions it would be lost. A row with no scan had no readable body.
 * The largest come first by LengthWithoutComments, ties by kind then name, so
 * the same org gives the same choice on every run.
 */
export function selectApexSource(rows: SourceRow[]): ApexSource {
  const counts = { read: rows.length, tests: 0, tooLarge: 0, overLimit: 0, unreadable: 0 };
  const eligible: SourceRow[] = [];
  for (const row of rows) {
    if (!row.scan || !row.body || row.body === "(hidden)") counts.unreadable++;
    else if (row.scan.testClass) counts.tests++;
    else if (row.body.length > APEX_SOURCE_MAX_BODY) counts.tooLarge++;
    else eligible.push(row);
  }
  const size = (row: SourceRow): number => (typeof row.length === "number" && Number.isFinite(row.length) ? row.length : 0);
  eligible.sort((a, b) => size(b) - size(a) || (a.kind === b.kind ? 0 : a.kind === "class" ? -1 : 1) || (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
  const kept = eligible.slice(0, APEX_SOURCE_MAX_ITEMS);
  counts.overLimit = eligible.length - kept.length;
  return { version: 1, counts, items: kept.map((row) => ({ kind: row.kind, name: row.name, body: redactSecrets(row.body!) })) };
}

/** Where the Apex step keeps the code. Only the worker, with its switch on, gives the step one. */
export type ApexSourceStore = {
  /** Called as the step starts, so a step that runs again never leaves an earlier read's file. */
  clear(): void;
  /** Called once, when every row has been read. */
  keep(source: ApexSource): void;
};

export function apexSourceStore(dir: string): ApexSourceStore {
  const path = join(dir, APEX_SOURCE_FILE);
  return {
    clear: () => rmSync(path, { force: true }),
    // Written in one go, at the top of the run folder: a temporary file beside it
    // would be a third name holding code, which the seven-day sweep doesn't know.
    keep: (source) => writeFileSync(path, `${JSON.stringify(ApexSource.parse(source))}\n`, { mode: 0o600 }),
  };
}

export const hasApexSource = (dir: string): boolean => existsSync(join(dir, APEX_SOURCE_FILE));
