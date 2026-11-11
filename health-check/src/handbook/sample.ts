#!/usr/bin/env node
import { mkdirSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join, resolve } from "node:path";
import { assertOutsideGit } from "../engine/run.ts";
import { loadPseudonymMap } from "../render/render-run.ts";
import { docxText } from "../testing/docx-text.ts";
import { olderReading, plantEarlier, withEarlier } from "../testing/earlier-handbook.ts";
import { runFromRecording } from "../testing/harness.ts";
import { loadAnswers } from "../testing/replay.ts";
import { renderHandbookDocx } from "./docx.ts";
import { readEarlierHandbook, readHandbook } from "./read.ts";
import { printableSummaries, type StoredSummaries } from "./summary-row.ts";

/** Made up, for the sample only: an approved row as the console would hold it, one summary struck out. */
const APPROVED: StoredSummaries = {
  status: "approved",
  items: [
    { kind: "class", name: "LeadRouter", summary: "Sends each new lead to a queue chosen by its region and size, and records which rule chose it.", struck: false },
    { kind: "class", name: "OpportunityHandler", summary: "Checks each opportunity as it is saved, and sets its next step when the stage moves forward.", struck: false },
    { kind: "class", name: "FinanceSync", summary: "Copies each paid invoice to the finance system overnight.", struck: true },
    { kind: "trigger", name: "AccountTrigger", summary: "Hands each account that is created or changed to its handler class.", struck: false },
  ],
  leftOut: { tests: 3, tooLarge: 0, overLimit: 0, unreadable: 0, refused: 0, notWritten: 1 },
};

// The org handbook for the recorded synthetic org, for a person to open:
// "Synthetic Co - Org handbook.docx", and handbook.txt beside it with the
// document's words in reading order, headings marked. Written outside the
// repo, as the other samples are. With --since, as a re-check: the synthetic
// org's earlier health check, made up beside it, gives the part on what changed
// since (decision 483); with --older-reading too, that earlier file says no
// reading, as one made before decision 484 does, so the part compares only
// what was added or removed and when each was last changed. With --summaries,
// as a person approved them (decision 495): made-up summaries of a few of the
// synthetic org's classes and triggers, one of them struck out, printed as the
// documents job prints an approved row.
//
//   node src/handbook/sample.ts [folder] [--since [--older-reading]] [--summaries]

const args = process.argv.slice(2);
const since = args.includes("--since");
const older = args.includes("--older-reading");
const summarised = args.includes("--summaries");
const out = resolve(args.find((a) => !a.startsWith("--")) ?? join(homedir(), ".barometer", "sample-handbook"));
assertOutsideGit(out);
const { dir, run } = await runFromRecording({ answers: loadAnswers() });
if (since)
  withEarlier(dir, (h) => {
    plantEarlier(h);
    if (older) olderReading(h);
  });
const read = readHandbook(dir);
if (!read.ok) throw new Error(`The synthetic run has no handbook (${read.why})`);
const summaries = summarised ? printableSummaries(read.handbook, APPROVED) : null;
const bytes = await renderHandbookDocx(read.handbook, { clientName: "Synthetic Co", runDate: run.asOf, pseudonyms: loadPseudonymMap(dir), earlier: readEarlierHandbook(dir), summaries });
mkdirSync(out, { recursive: true, mode: 0o700 });
writeFileSync(join(out, "Synthetic Co - Org handbook.docx"), bytes, { mode: 0o600 });
writeFileSync(join(out, "handbook.txt"), `${docxText(bytes)}\n`, { mode: 0o600 });
console.log(`Written to ${out}`);
