import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { baselineRun } from "../render/baseline.ts";
import { Run } from "../schemas/index.ts";
import { HANDBOOK_FILE } from "./build.ts";
import { CHANGES_NOT_KEPT, Handbook, HANDBOOK_VERSION, HandbookVersion2, HandbookVersion3, HandbookVersion4, HandbookVersion5, type HandbookReadBy } from "./schema.ts";

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

/** An older file's readings, which it didn't keep (decision 484). */
const NOT_KNOWN: HandbookReadBy = { metadata: null, apex: null };

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
  // A run from before which reading made it was kept: neither is known, so a
  // re-check set against it compares only what was added or removed and when
  // each was last changed (decision 484). So for every version below.
  const five = HandbookVersion5.safeParse(data);
  if (five.success) return { ok: true, handbook: { ...five.data, version: HANDBOOK_VERSION, readBy: NOT_KNOWN } };
  // A run from before the flows' and validation rules' last changes were kept
  // still makes its document, those without them and saying nothing of them
  // (decision 482). Its Apex is whole.
  const four = HandbookVersion4.safeParse(data);
  if (four.success) return { ok: true, handbook: { ...four.data, version: HANDBOOK_VERSION, readBy: NOT_KNOWN, flows: notKept(four.data.flows), validationRules: notKept(four.data.validationRules) } };
  // And one from before any were kept, its Apex without them too (decision 473).
  const three = HandbookVersion3.safeParse(data);
  if (three.success) return { ok: true, handbook: { ...three.data, version: HANDBOOK_VERSION, readBy: NOT_KNOWN, flows: notKept(three.data.flows), validationRules: notKept(three.data.validationRules), apexClasses: notKept(three.data.apexClasses), apexTriggers: notKept(three.data.apexTriggers) } };
  // And one from before the recent changes were kept, with that part saying so
  // (decision 462). Its other four parts are whole.
  const two = HandbookVersion2.safeParse(data);
  if (two.success) return { ok: true, handbook: { ...two.data, version: HANDBOOK_VERSION, readBy: NOT_KNOWN, flows: notKept(two.data.flows), validationRules: notKept(two.data.validationRules), apexClasses: notKept(two.data.apexClasses), apexTriggers: notKept(two.data.apexTriggers), recentChanges: { status: "unavailable", reason: CHANGES_NOT_KEPT } } };
  return { ok: false, why: "format" };
}

/** A re-check's earlier handbook, and the day its health check read the org. */
export type EarlierHandbook = { handbook: Handbook; asOf: string };

/**
 * The handbook of the health check this run is a re-check of (decision 483),
 * from the folder beside the run's own, by the route the report's comparison
 * takes (render/baseline.ts): the id's shape checked and the same org. Read by
 * `readHandbook`, so an earlier version is upgraded as this run's would be.
 * Only run.json and handbook.json are opened there: never its pseudonym map,
 * since its tokens mean nothing in this run. Undefined when the run names no
 * baseline, so its handbook has no such part; null when it names one and
 * there's no handbook to read, gone, unreadable or another org's, which the
 * part says in one sentence, the same for each.
 */
export function readEarlierHandbook(runDir: string): EarlierHandbook | null | undefined {
  let run: Run;
  try {
    run = Run.parse(JSON.parse(readFileSync(join(runDir, "run.json"), "utf8")));
  } catch {
    return undefined;
  }
  if (!run.baselineRunId) return undefined;
  const baseline = baselineRun(runDir, run);
  if (!baseline) return null;
  const read = readHandbook(baseline.dir);
  return read.ok ? { handbook: read.handbook, asOf: baseline.run.asOf } : null;
}

/** An older file's list, each item with no last change and the list saying they weren't kept. */
function notKept<P extends { status: "ok"; items: object[] } | { status: "unavailable"; reason: string }>(part: P) {
  if (part.status !== "ok") return part as Extract<P, { status: "unavailable" }>;
  const ok = part as Extract<P, { status: "ok" }>;
  return { ...ok, items: ok.items.map((item) => ({ ...item, lastChanged: null })) as Array<Extract<P, { status: "ok" }>["items"][number] & { lastChanged: null }>, lastChanges: "not_kept" as const };
}
