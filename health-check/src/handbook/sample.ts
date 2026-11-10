#!/usr/bin/env node
import { mkdirSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join, resolve } from "node:path";
import { assertOutsideGit } from "../engine/run.ts";
import { docxText } from "../testing/docx-text.ts";
import { runFromRecording } from "../testing/harness.ts";
import { loadAnswers } from "../testing/replay.ts";
import { renderHandbookDocx } from "./docx.ts";
import { readHandbook } from "./read.ts";

// The org handbook for the recorded synthetic org, for a person to open:
// "Synthetic Co - Org handbook.docx", and handbook.txt beside it with the
// document's words in reading order, headings marked. Written outside the
// repo, as the other samples are.
//
//   node src/handbook/sample.ts [folder]

const out = resolve(process.argv[2] ?? join(homedir(), ".barometer", "sample-handbook"));
assertOutsideGit(out);
const { dir, run } = await runFromRecording({ answers: loadAnswers() });
const read = readHandbook(dir);
if (!read.ok) throw new Error(`The synthetic run has no handbook (${read.why})`);
const bytes = await renderHandbookDocx(read.handbook, { clientName: "Synthetic Co", runDate: run.asOf });
mkdirSync(out, { recursive: true, mode: 0o700 });
writeFileSync(join(out, "Synthetic Co - Org handbook.docx"), bytes, { mode: 0o600 });
writeFileSync(join(out, "handbook.txt"), `${docxText(bytes)}\n`, { mode: 0o600 });
console.log(`Written to ${out}`);
