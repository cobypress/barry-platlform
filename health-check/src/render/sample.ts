#!/usr/bin/env node
import { writeFileSync } from "node:fs";
import { join } from "node:path";
import { homedir } from "node:os";
import { runFromRecording } from "../testing/harness.ts";
import { loadAnswers } from "../testing/replay.ts";
import { INTERVIEWS_FILE, renderRun } from "./render-run.ts";
import { INTERVIEW_VERSION } from "../interviews/questions.ts";
import { syntheticInterviews } from "../testing/interviews.ts";

// Renders every document from the synthetic org into a run folder outside the
// repo, so they can be looked at. Sample output never goes in git.

const INTAKE = {
  customerName: "Synthetic Co",
  summary: "The sales director told us the forecast can't be trusted, and that reps avoid updating Salesforce.",
  statedProblems: [
    { id: "p1", statement: "We don't trust what the pipeline report tells us.", findingIds: [] as string[], assessment: "The data supports this: open deals include many that should already have closed." },
    { id: "p2", statement: "Salesforce has become hard to change.", findingIds: [] as string[], assessment: "The data supports this: automation is split across several tools." },
  ],
  capturedBy: "Synthetic Consultant",
  capturedAt: "2026-10-30T09:00:00.000Z",
};

const { dir, output } = await runFromRecording({ answers: loadAnswers(), outDir: process.argv[2] ?? join(homedir(), ".barometer", "sample-runs") });
const issue = (pattern: RegExp): string[] => output!.findings.filter((f) => f.status === "issue" && pattern.test(f.title)).map((f) => f.id);
INTAKE.statedProblems[0]!.findingIds = issue(/already have closed/);
INTAKE.statedProblems[1]!.findingIds = issue(/split across several tools/);
writeFileSync(join(dir, "intake.json"), JSON.stringify(INTAKE, null, 2), { mode: 0o600 });
// Five made-up conversations, by role, so the sample has its "What your people told us" chapter.
writeFileSync(join(dir, INTERVIEWS_FILE), JSON.stringify({ version: INTERVIEW_VERSION, interviews: syntheticInterviews() }, null, 2), { mode: 0o600 });

const result = await renderRun(dir);
console.log(`Written to ${dir}`);
console.log(`Executive summary: ${result.layout.executive.pages} pages. Full report: ${result.layout.full.pages} slides. One page summary: ${result.layout.ceo.pages} ${result.layout.ceo.pages === 1 ? "page" : "pages"}.`);
for (const [name, layout] of Object.entries(result.layout)) for (const p of layout.problems) console.log(`${name}, page ${p.page}: ${p.check}: ${p.detail}`);
process.exitCode = result.passed ? 0 : 1;
