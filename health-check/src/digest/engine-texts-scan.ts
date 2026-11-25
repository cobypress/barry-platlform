import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript";
import { CUSTOMER_TEXT_DIRS } from "../numbercheck/engine-words-scan.ts";
import { engineTextKey } from "./engine-texts-key.ts";

// How the engine's own texts are found (DECISIONS.md 693): every whole string
// the source writes in the folders whose text can reach a customer, keyed by
// engineTextKey. Only a whole literal: a template's substitutions could be the
// org's, a digits-only stage name among them, so a text with anything put in
// is known only through said, unless all it puts in are the same file's own
// number constants. Only a test and this file's own command run it, so the list
// in engine-texts.ts is regenerated on purpose and reviewed in the diff, never
// grown while a run is going. Run `node src/digest/engine-texts-scan.ts --write`.

/**
 * The folders scanned: those whose strings can reach a page, the engine's and
 * the guards' own, and the digest's, whose builder writes sentences of its own
 * into what the portal serves (the plan's hours note, DECISIONS.md 673, and the
 * overall score's fallback reason) and marks them as ours only through this list
 * (DECISIONS.md 739): left out, an automatic run with a code checker finding in
 * its plan was held for one sentence of our own. And the extractors', whose own
 * reasons a rule passes on whole or after a head (DECISIONS.md 743).
 */
export const ENGINE_TEXT_DIRS = [...CUSTOMER_TEXT_DIRS, "engine", "guards", "digest", "extract"];

function* sourceFiles(dir: string): Generator<string> {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) {
      if (entry.name !== "__tests__") yield* sourceFiles(path);
    } else if (entry.name.endsWith(".ts")) yield path;
  }
}

/**
 * The extractors' folder holds queries, permission and edition names, field
 * lists and shell lines beside the sentences a read's reason is made of, so only
 * its sentences are keyed (DECISIONS.md 743): a capital first, three words or
 * more, and a full stop last. Every other folder keys any text with a space.
 */
export const SENTENCE_ONLY_DIRS: readonly string[] = ["extract"];
export const isSentence = (text: string): boolean => /^[A-Z]/.test(text) && text.trim().split(/\s+/).length >= 3 && text.endsWith(".");

/** Each text keyed, by folder, before it is hashed: for a reviewer to read what a folder adds (a test lists the extractors'). */
export function scanEngineTextsByDir(srcDir: string): Map<string, Set<string>> {
  const out = new Map<string, Set<string>>();
  for (const dir of ENGINE_TEXT_DIRS) {
    const texts = new Set<string>();
    out.set(dir, texts);
    for (const file of sourceFiles(join(srcDir, dir))) {
      const source = ts.createSourceFile(file, readFileSync(file, "utf8"), ts.ScriptTarget.Latest, true);
      // The file's own top-level number constants, such as RECENT_DAYS = 28: a template putting in only these is
      // written by the engine whole, with nothing from the org, so it is read as the text it always makes.
      const numbers = new Map<string, string>();
      for (const statement of source.statements) {
        if (!ts.isVariableStatement(statement) || !(statement.declarationList.flags & ts.NodeFlags.Const)) continue;
        for (const d of statement.declarationList.declarations) if (ts.isIdentifier(d.name) && d.initializer && ts.isNumericLiteral(d.initializer)) numbers.set(d.name.text, d.initializer.text);
      }
      const constant = (node: ts.TemplateExpression): string | null => {
        let text = node.head.text;
        for (const span of node.templateSpans) {
          if (!ts.isIdentifier(span.expression) || !numbers.has(span.expression.text)) return null;
          text += String(Number(numbers.get(span.expression.text))) + span.literal.text;
        }
        return text;
      };
      const visit = (node: ts.Node): void => {
        let text: string | null = null;
        if (ts.isStringLiteral(node) && !ts.isImportDeclaration(node.parent) && !ts.isExportDeclaration(node.parent)) text = node.text;
        // A template with nothing put in, but never one with substitutions: what is put in could be the org's (DECISIONS.md 693).
        else if (ts.isNoSubstitutionTemplateLiteral(node) && !ts.isTaggedTemplateExpression(node.parent)) text = node.text;
        else if (ts.isTemplateExpression(node) && !ts.isTaggedTemplateExpression(node.parent)) text = constant(node);
        if (text !== null && /\s/.test(text.trim()) && (!SENTENCE_ONLY_DIRS.includes(dir) || isSentence(text))) texts.add(text);
        ts.forEachChild(node, visit);
      };
      visit(source);
    }
  }
  return out;
}

/** Each key, sorted. Only texts with a space: a single word could be a value the org chose. */
export function scanEngineTexts(srcDir: string): string[] {
  const keys = new Set<string>();
  for (const texts of scanEngineTextsByDir(srcDir).values()) for (const text of texts) keys.add(engineTextKey(text));
  return [...keys].sort();
}

/** Where a rule or an extractor passes a read's reason on: the same folders, the extractors' among them. */
export const REASON_PREFIX_DIRS = ENGINE_TEXT_DIRS;

/**
 * The words the engine puts before a reason it passes on (DECISIONS.md 702):
 * each template that puts in one thing alone, last, read from a `.reason`, such
 * as `Quotas couldn't be read: ${quotas.reason}`, keyed as its head is. The head
 * is written whole by the engine; what follows is vouched for only where it is
 * a refused read's own sentence (src/salesforce/refusal-words.ts).
 */
export function scanReasonPrefixes(srcDir: string): { keys: string[]; longest: number } {
  const keys = new Set<string>();
  let longest = 0;
  for (const dir of REASON_PREFIX_DIRS) {
    for (const file of sourceFiles(join(srcDir, dir))) {
      const source = ts.createSourceFile(file, readFileSync(file, "utf8"), ts.ScriptTarget.Latest, true);
      const visit = (node: ts.Node): void => {
        if (ts.isTemplateExpression(node) && !ts.isTaggedTemplateExpression(node.parent) && node.templateSpans.length === 1) {
          const span = node.templateSpans[0]!;
          let expression: ts.Expression = span.expression;
          while (ts.isNonNullExpression(expression) || ts.isParenthesizedExpression(expression)) expression = expression.expression;
          if (span.literal.text === "" && ts.isPropertyAccessExpression(expression) && expression.name.text === "reason" && node.head.text !== "") {
            keys.add(engineTextKey(node.head.text));
            longest = Math.max(longest, node.head.text.length);
          }
        }
        ts.forEachChild(node, visit);
      };
      visit(source);
    }
  }
  return { keys: [...keys].sort(), longest };
}

export const REASON_PREFIXES_FILE = fileURLToPath(new URL("./reason-prefixes.ts", import.meta.url));

/** The generated prefixes file's whole text. */
export function reasonPrefixesSource({ keys, longest }: { keys: string[]; longest: number }): string {
  const lines = [];
  for (let i = 0; i < keys.length; i += 8) lines.push(`  ${keys.slice(i, i + 8).map((k) => JSON.stringify(k)).join(", ")},`);
  return `// Generated by src/digest/engine-texts-scan.ts (DECISIONS.md 702). Don't edit: run \`node src/digest/engine-texts-scan.ts --write\`.\n// The keys of the words the engine puts before a reason it passes on, so a refused read's sentence after them is known as ours.\n\nexport const REASON_PREFIX_KEYS: ReadonlySet<string> = new Set([\n${lines.join("\n")}\n]);\n\n/** The longest head, in characters: no search for one looks further into a text. */\nexport const REASON_PREFIX_LONGEST = ${longest};\n`;
}

export const ENGINE_TEXTS_FILE = fileURLToPath(new URL("./engine-texts.ts", import.meta.url));

/** The generated file's whole text. */
export function engineTextsSource(keys: string[]): string {
  const lines = [];
  for (let i = 0; i < keys.length; i += 8) lines.push(`  ${keys.slice(i, i + 8).map((k) => JSON.stringify(k)).join(", ")},`);
  return `// Generated by src/digest/engine-texts-scan.ts (DECISIONS.md 693). Don't edit: run \`node src/digest/engine-texts-scan.ts --write\`.\n// The keys of the engine's own texts, so a text the engine wrote whole, or from a template with only figures put in, is known as ours.\n\nexport const ENGINE_TEXT_KEYS: ReadonlySet<string> = new Set([\n${lines.join("\n")}\n]);\n`;
}

if (process.argv[1] === fileURLToPath(import.meta.url) && process.argv.includes("--write")) {
  writeFileSync(ENGINE_TEXTS_FILE, engineTextsSource(scanEngineTexts(fileURLToPath(new URL("../", import.meta.url)))));
  writeFileSync(REASON_PREFIXES_FILE, reasonPrefixesSource(scanReasonPrefixes(fileURLToPath(new URL("../", import.meta.url)))));
}
