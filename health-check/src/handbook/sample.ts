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

// The org handbook for the recorded synthetic org, for a person to open:
// "Synthetic Co - Org handbook.docx", and handbook.txt beside it with the
// document's words in reading order, headings marked. Written outside the
// repo, as the other samples are. With --since, as a re-check: the synthetic
// org's earlier health check, made up beside it, gives the part on what changed
// since (decision 483); with --older-reading too, that earlier file says no
// reading, as one made before decision 484 does, so the part compares only
// what was added or removed and when each was last changed.
//
//   node src/handbook/sample.ts [folder] [--since [--older-reading]]

const args = process.argv.slice(2);
const since = args.includes("--since");
const older = args.includes("--older-reading");
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
const bytes = await renderHandbookDocx(read.handbook, { clientName: "Synthetic Co", runDate: run.asOf, pseudonyms: loadPseudonymMap(dir), earlier: readEarlierHandbook(dir) });
mkdirSync(out, { recursive: true, mode: 0o700 });
writeFileSync(join(out, "Synthetic Co - Org handbook.docx"), bytes, { mode: 0o600 });
writeFileSync(join(out, "handbook.txt"), `${docxText(bytes)}\n`, { mode: 0o600 });
console.log(`Written to ${out}`);
