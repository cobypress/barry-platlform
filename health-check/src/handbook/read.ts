import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { HANDBOOK_FILE } from "./build.ts";
import { CHANGES_NOT_KEPT, Handbook, HANDBOOK_VERSION, HandbookVersion2, HandbookVersion3 } from "./schema.ts";

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
  if (parsed.success) return { ok: true, handbook: parsed.data };
  // A run from before the last changes were kept still makes its document,
  // its Apex without them and saying nothing of them (decision 473).
  const three = HandbookVersion3.safeParse(data);
  if (three.success) return { ok: true, handbook: { ...three.data, version: HANDBOOK_VERSION, apexClasses: notKept(three.data.apexClasses), apexTriggers: notKept(three.data.apexTriggers) } };
  // And one from before the recent changes were kept, with that part saying so
  // (decision 462). Its other four parts are whole.
  const two = HandbookVersion2.safeParse(data);
  if (two.success) return { ok: true, handbook: { ...two.data, version: HANDBOOK_VERSION, apexClasses: notKept(two.data.apexClasses), apexTriggers: notKept(two.data.apexTriggers), recentChanges: { status: "unavailable", reason: CHANGES_NOT_KEPT } } };
  return { ok: false, why: "format" };
}

/** An older file's Apex list, each class or trigger with no last change and the list saying they weren't kept. */
function notKept<T extends object>(part: { status: "ok"; items: T[]; total: number; cut: boolean } | { status: "unavailable"; reason: string }) {
  if (part.status !== "ok") return part;
  return { ...part, items: part.items.map((item) => ({ ...item, lastChanged: null })), lastChanges: "not_kept" as const };
}
