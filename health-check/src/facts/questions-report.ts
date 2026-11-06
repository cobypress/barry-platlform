#!/usr/bin/env node
import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { basename, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { FACTS_FILE } from "../extract/facts.ts";
import { answerQuestions, parseQuestions, questionsReport } from "./questions.ts";
import { FactsFile } from "./summary.ts";

// npm run questions -- <facts.json, run folder or folder of runs> ...
// Which QUESTIONS.md items the facts files settle, contradict or leave open
// (DECISIONS.md 293). It reads facts files only, which hold no customer data.

const QUESTIONS = join(dirname(fileURLToPath(import.meta.url)), "..", "..", "docs", "QUESTIONS.md");

/** facts.json files: given directly, in a run folder, or in the run folders one level down. */
function factsFiles(path: string): string[] {
  if (!existsSync(path)) throw new Error(`Nothing at ${path}`);
  if (statSync(path).isFile()) return [path];
  if (existsSync(join(path, FACTS_FILE))) return [join(path, FACTS_FILE)];
  return readdirSync(path)
    .map((name) => join(path, name, FACTS_FILE))
    .filter((p) => existsSync(p))
    .sort();
}

const paths = process.argv.slice(2).filter((a) => !a.startsWith("--"));
if (paths.length === 0) {
  console.log("Usage: npm run questions -- <facts.json, run folder or folder of runs> ...");
  process.exitCode = 2;
} else {
  const files = paths.flatMap(factsFiles);
  if (files.length === 0) throw new Error("No facts.json found. Runs write one from this version on.");
  const runs = files.map((file) => ({ name: basename(dirname(file)), facts: FactsFile.parse(JSON.parse(readFileSync(file, "utf8"))) }));
  console.log(questionsReport(answerQuestions(parseQuestions(readFileSync(QUESTIONS, "utf8")), runs), runs));
}
