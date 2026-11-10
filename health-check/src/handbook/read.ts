import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { HANDBOOK_FILE } from "./build.ts";
import { Handbook } from "./schema.ts";

// The one reader of handbook.json (DECISIONS.md 443 and 445). It gives the
// handbook, or why there isn't one in words fixed here: nothing from the file
// is ever in the reason, because the reason goes to the console's warnings and
// the worker's log, and the file is the customer's configuration (452).

export type HandbookUnread = "missing" | "format" | "unreadable";

export type HandbookRead = { ok: true; handbook: Handbook } | { ok: false; why: HandbookUnread };

/**
 * Why a make has no handbook, as the documents job's warnings say it. Each is
 * one sentence from this list and nothing else: the test in
 * documents-job.test.ts holds each to what the console's reader takes.
 */
export const HANDBOOK_NOT_MADE = {
  missing: "The org handbook wasn't made: this run was read before the handbook's data was kept.",
  format: "The org handbook wasn't made: its data isn't in the format this version reads.",
  unreadable: "The org handbook wasn't made: its data couldn't be read from the run's folder.",
  render: "The org handbook wasn't made: it couldn't be written as a Word document.",
} as const;

/**
 * The run folder's handbook, checked against its schema. A file that doesn't
 * parse, or parses as something else, is "format", never half read: a handbook
 * that quietly leaves something out would break decision 453.
 */
export function readHandbook(runDir: string): HandbookRead {
  const path = join(runDir, HANDBOOK_FILE);
  if (!existsSync(path)) return { ok: false, why: "missing" };
  let text: string;
  try {
    text = readFileSync(path, "utf8");
  } catch {
    return { ok: false, why: "unreadable" };
  }
  let data: unknown;
  try {
    data = JSON.parse(text);
  } catch {
    return { ok: false, why: "format" };
  }
  const parsed = Handbook.safeParse(data);
  return parsed.success ? { ok: true, handbook: parsed.data } : { ok: false, why: "format" };
}
