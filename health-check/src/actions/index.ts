import { applyOverrides } from "../narrative/overrides.ts";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import type { BuiltReport } from "../narrative/sections.ts";
import { domainLabel, issuesOf, teamLevelFinding, teamLevelText } from "../narrative/select.ts";
import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import { noteSections, referenceMap } from "../render/references.ts";
import type { Finding } from "../schemas/index.ts";
import { ACTION_LISTS_FILE, ActionLists, OBJECT_LABELS, type ActionList } from "./lists.ts";
import { writeXlsx, type Cell, type Sheet } from "./xlsx.ts";

// The action lists (roadmap Stage 4): a workbook with every record to fix, one
// sheet an issue, and a ticket file with one row an issue, for Jira and tools
// like it. Exports are where named records belong (roadmap decision 10). People
// are named only with the consent recorded at intake (roadmap decision 11):
// without it, a record's owner is given by role, and a user who is the record
// by profile and role (DECISIONS.md 194).

export const ACTION_FILES = { workbook: "action-lists.xlsx", tickets: "tickets.csv" } as const;

/** Jira's default priorities, by severity (DECISIONS.md 195). */
export const PRIORITY: Record<Finding["severity"], string> = { critical: "Highest", high: "High", medium: "Medium", low: "Low", info: "Lowest" };

/** The ticket file's columns, in order. "Labels" repeats, which is how Jira's importer takes more than one. */
export const TICKET_COLUMNS = ["Summary", "Issue Type", "Priority", "Description", "Labels", "Labels", "Labels", "Effort", "Owner role", "Reference", "Records"] as const;

const EFFORT: Record<Finding["effort"]["size"], string> = { XS: "Extra small", S: "Small", M: "Medium", L: "Large", XL: "Extra large" };
const SEVERITY_ORDER = ["critical", "high", "medium", "low", "info"];

export function loadActionLists(dir: string): ActionLists | null {
  const path = join(dir, ACTION_LISTS_FILE);
  return existsSync(path) ? ActionLists.parse(JSON.parse(readFileSync(path, "utf8"))) : null;
}

/** How many records each issue's list holds, by finding id, for the notes and the decks. Counts only: no name leaves the file. */
export function recordCounts(lists: ActionLists | null): Record<string, { rows: number; total: number }> {
  return Object.fromEntries((lists?.lists ?? []).map((l) => [l.findingId, { rows: l.rows.length, total: l.part?.total ?? l.rows.length }]));
}

type Context = { consent: boolean; lists: ActionLists | null; words: (text: string) => string; refOf: (f: Finding) => string };

function context(report: BuiltReport, pseudonyms: Pseudonymiser, lists: ActionLists | null): Context & { issues: Finding[] } {
  const consent = report.input.intake?.namedRepConsent === true;
  const refs = referenceMap(noteSections(report));
  const refOf = (f: Finding): string => refs.get(f.id) ?? "";
  // With the consultant's approved overrides, as the decks have them: a customer never sees a corrected figure.
  const { findings } = applyOverrides(report.input.findings, report.input.metrics, report.input.overrides ?? []);
  // In reference order, so the front sheet and the tickets read as the raw notes do.
  const issues = issuesOf(findings)
    .map((f) => (consent ? f : teamLevelFinding(f)))
    .sort((a, b) => refOf(a).localeCompare(refOf(b), "en", { numeric: true }) || SEVERITY_ORDER.indexOf(a.severity) - SEVERITY_ORDER.indexOf(b.severity));
  // Every other token, such as an account's, is a named record, which an export may hold.
  return { consent, lists, issues, refOf, words: (text) => pseudonyms.restore(consent ? text : teamLevelText(text)) };
}

const sentence = (text: string): string => (/[.!?]$/.test(text.trim()) ? text.trim() : `${text.trim()}.`);

function partNote(list: ActionList): string | null {
  if (!list.part) return null;
  return `${list.part.label ?? `The first ${list.rows.length.toLocaleString("en-GB")} of ${list.part.total.toLocaleString("en-GB")}, in order of Salesforce id.`} ${list.part.rest}`;
}

function listSheet(c: Context, f: Finding, list: ActionList, name: string): Sheet {
  const lists = c.lists!;
  const link = (id: string): Cell => ({ text: "Open in Salesforce", url: `${lists.instanceUrl.replace(/\/+$/, "")}/${id}` });
  const notes = [`${c.refOf(f)} ${c.words(f.title)}. ${sentence(c.words(list.fix))}`, ...(partNote(list) ? [partNote(list)!] : [])];
  if (list.object === "User") {
    const person = (id: string): string => {
      const u = lists.users[id];
      if (c.consent) return u?.name ?? "A user we couldn't name";
      return `${u?.profile ?? "No profile"}, ${u?.role ? `role ${u.role}` : "no role"}`;
    };
    return {
      name,
      notes,
      columns: [
        { header: "Salesforce id", width: 21 },
        { header: "Link", width: 20 },
        { header: c.consent ? "User" : "User's profile and role", width: 34 },
        { header: "What's wrong", width: 60 },
        { header: "Fix", width: 50 },
      ],
      rows: list.rows.map((r) => [r.id, link(r.id), person(r.id), c.words(r.problem), c.words(list.fix)]),
    };
  }
  const owner = (id: string | null): string => {
    if (!id) return "";
    const u = lists.users[id];
    if (c.consent) return u?.name ?? "A user we couldn't name";
    return u?.role ?? "No role";
  };
  return {
    name,
    notes,
    columns: [
      { header: "Salesforce id", width: 21 },
      { header: "Link", width: 20 },
      { header: `${OBJECT_LABELS[list.object]} name`, width: 36 },
      { header: c.consent ? "Owner" : "Owner's role", width: 24 },
      { header: "What's wrong", width: 60 },
      { header: "Fix", width: 50 },
    ],
    rows: list.rows.map((r) => [r.id, link(r.id), r.name ?? "", owner(r.ownerId), c.words(r.problem), c.words(list.fix)]),
  };
}

export function buildWorkbook(report: BuiltReport, pseudonyms: Pseudonymiser, lists: ActionLists | null): { bytes: Uint8Array; sheets: number; rows: number } {
  const c = context(report, pseudonyms, lists);
  const byFinding = new Map((lists?.lists ?? []).map((l) => [l.findingId, l]));
  const sheets: Sheet[] = [];
  const front: Cell[][] = c.issues.map((f, i) => {
    const list = byFinding.get(f.id);
    let where: Cell = "A set-up change: what to change is in the ticket and the raw notes.";
    let count: Cell = null;
    if (list) {
      const name = c.refOf(f) || `Issue ${i + 1}`;
      sheets.push(listSheet(c, f, list, name));
      where = { text: `See sheet ${name}`, sheet: name };
      count = list.rows.length;
      if (list.part) where = { text: `See sheet ${name}: the first ${list.rows.length.toLocaleString("en-GB")} of ${list.part.total.toLocaleString("en-GB")}`, sheet: name };
    } else if (!lists) {
      where = "Record lists weren't gathered for this run.";
    }
    return [c.refOf(f), c.words(f.title), domainLabel(f.domain), f.severity.replace(/^./, (s) => s.toUpperCase()), c.words(f.recommendation), EFFORT[f.effort.size], c.words(f.owner), count, where];
  });
  const notes = [
    "Every issue in the health check, with what to do. Where an issue is about records, its sheet lists each one, with a link to it in Salesforce. The reference is the item in the raw notes with the working.",
    c.consent ? "People are named, as agreed at intake." : "People aren't named, as agreed at intake: owners are given by role, and users by profile and role. Each record's link opens it in Salesforce.",
  ];
  const all: Sheet[] = [
    {
      name: "Issues",
      notes,
      columns: [
        { header: "Reference", width: 11 },
        { header: "Issue", width: 50 },
        { header: "Area", width: 22 },
        { header: "Severity", width: 10 },
        { header: "What to do", width: 70 },
        { header: "Effort", width: 12 },
        { header: "Owner", width: 26 },
        { header: "Records", width: 10 },
        { header: "Where", width: 44 },
      ],
      rows: front,
    },
    ...sheets,
  ];
  return { bytes: writeXlsx(all), sheets: sheets.length, rows: sheets.reduce((sum, s) => sum + s.rows.length, 0) };
}

// A cell starting with one of these can run as a formula when a spreadsheet
// opens the file, so it gets a leading apostrophe (DECISIONS.md 195).
const FORMULA_START = /^[=+\-@\t\r]/;
const csvCell = (value: string): string => {
  const safe = FORMULA_START.test(value) ? `'${value}` : value;
  return `"${safe.replace(/"/g, '""')}"`;
};

export function buildTickets(report: BuiltReport, pseudonyms: Pseudonymiser, lists: ActionLists | null): string {
  const c = context(report, pseudonyms, lists);
  const byFinding = new Map((lists?.lists ?? []).map((l) => [l.findingId, l]));
  const rows = c.issues.map((f) => {
    const ref = c.refOf(f);
    const list = byFinding.get(f.id);
    const items = f.subjects.map((s) => c.words(s.label));
    const records = list
      ? `Records to fix: ${list.part ? `${list.part.total.toLocaleString("en-GB")}, the first ${list.rows.length.toLocaleString("en-GB")} of them` : list.rows.length.toLocaleString("en-GB")} on sheet ${ref} of the action list workbook.`
      : items.length
        ? `Where: ${items.join("; ")}.`
        : "";
    const description = [
      sentence(c.words(f.summary)),
      `What to do: ${sentence(c.words(f.recommendation))}`,
      records,
      `Effort: ${EFFORT[f.effort.size].toLowerCase()}.${f.effort.note ? ` ${sentence(c.words(f.effort.note))}` : ""} Owner: ${c.words(f.owner)}.`,
      ref ? `The working is at ${ref} in the raw notes.` : "",
    ].filter(Boolean);
    return [c.words(f.title), "Task", PRIORITY[f.severity], description.join("\n\n"), "barometer", f.domain, f.ruleId, f.effort.size, c.words(f.owner), ref, list ? String(list.part?.total ?? list.rows.length) : ""];
  });
  return [TICKET_COLUMNS as readonly string[], ...rows].map((row) => row.map(csvCell).join(",")).join("\r\n") + "\r\n";
}

/** Writes both files. They hold names, so they're private to the worker until they go to Drive. */
export function writeActionFiles(report: BuiltReport, pseudonyms: Pseudonymiser, runDir: string, paths: { workbook: string; tickets: string }): void {
  const lists = loadActionLists(runDir);
  writeFileSync(paths.workbook, buildWorkbook(report, pseudonyms, lists).bytes, { mode: 0o600 });
  writeFileSync(paths.tickets, buildTickets(report, pseudonyms, lists), { mode: 0o600 });
}
