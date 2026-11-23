import { readdirSync, readFileSync } from "node:fs";
import { join, relative } from "node:path";
import ts from "typescript";
import { extractFigures } from "./extract.ts";

// How the engine's own vocabulary is found (DECISIONS.md 682): every string the
// source writes, read with the number check's own reader, keeping the words of
// letters and digits. Only a test runs this, so the list in engine-words.ts is
// reviewed when it changes and never grown while a run is going.

/** The folders whose strings can reach a printed page. Extractors, queries and the worker's plumbing can't. */
export const CUSTOMER_TEXT_DIRS = ["actions", "benchmark", "domains", "facts", "interviews", "money", "narrative", "progress", "render", "survey", "value"];

function* sourceFiles(dir: string): Generator<string> {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) {
      if (entry.name !== "__tests__") yield* sourceFiles(path);
    } else if (entry.name.endsWith(".ts")) yield path;
  }
}

/** Each letters-and-digits word in the engine's strings, lower case, with the first file that writes it. */
export function scanEngineWords(srcDir: string): Map<string, string> {
  const words = new Map<string, string>();
  for (const dir of CUSTOMER_TEXT_DIRS) {
    for (const file of sourceFiles(join(srcDir, dir))) {
      const source = ts.createSourceFile(file, readFileSync(file, "utf8"), ts.ScriptTarget.Latest, true);
      const visit = (node: ts.Node): void => {
        if (ts.isStringLiteral(node) || ts.isNoSubstitutionTemplateLiteral(node) || ts.isTemplateHead(node) || ts.isTemplateMiddle(node) || ts.isTemplateTail(node)) {
          // An import's path is code, not text.
          if (!(ts.isStringLiteral(node) && (ts.isImportDeclaration(node.parent) || ts.isExportDeclaration(node.parent)))) {
            for (const f of extractFigures(node.text).figures) if (f.word && !words.has(f.raw.toLowerCase())) words.set(f.raw.toLowerCase(), relative(srcDir, file));
          }
        }
        ts.forEachChild(node, visit);
      };
      visit(source);
    }
  }
  return words;
}
