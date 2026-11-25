import { readFileSync } from "node:fs";
import { join, relative } from "node:path";
import ts from "typescript";

// Every place the source writes a string where a digest text comes from
// (DECISIONS.md 739, 743): a rule's title, summary, recommendation or reason, a
// value's reason, a metric's label, definition, reason, assumption or exclusion,
// and the reasons a loader hands on to them. Read with the type checker, so a
// sentence said made (typed Said) is told apart from a plain string. An automatic
// run serves a text only when it knows it as ours or marked: said made it, the
// source writes it whole (src/digest/engine-texts.ts), or it is a reason passed
// on after a head of ours (702). Each producer here is one of those, or signed
// off by file and expression in digest-builder-words.test.ts as a value that is.

export const HEALTH_CHECK = join(import.meta.dirname, "../../..");

/** The folders whose strings become a finding's or a metric's text. */
export const PRODUCER_DIRS = ["src/domains", "src/engine/currency.ts", "src/extract"];

/** The extractors', whose only text a rule passes on is a read's reason. */
const REASON_ONLY = "src/extract";

/** Fields of a rule's result, a value, a metric or a loader's answer that hold text the digest serves. */
const FIELDS = new Set(["title", "summary", "recommendation", "blockedRecommendation", "reason", "definition", "label", "exclusions", "assumptions", "caveat"]);
/** Helpers whose string arguments become a reason. */
const CALLS = /^(notSized|nm|notMeasurable|off|thinReason|apexGap|cantRead)$/;
/** Arrays that become a metric's exclusions or assumptions, or a reason's parts. */
const LISTS = /(exclusions|assumptions|reasons)$/i;
/**
 * Objects whose text the digest never serves: evidence (its notes and
 * exclusions), a metric's segments and series points, a finding's subjects and
 * an action list's rows. Known by a field only they have.
 */
const NOT_SERVED = ["capturedAt", "dimension", "date", "problem", "type", "count", "rows", "rest"];

export type Producer = {
  /** File, relative to the health check folder, and line. */
  at: string;
  /** What writes the text: a field, a push onto a list, a list's item or a helper's argument. */
  slot: string;
  /** How an automatic run knows it: said, whole, passed-on, or ref (a value handed on, signed off by expression). */
  kind: "said" | "whole" | "passed-on" | "ref" | "NOT RECOGNISED";
  expression: string;
};

/**
 * Every producer, each value expression classified. `files` is every source
 * file of the program; the program is built once per call.
 */
export function textProducers(): Producer[] {
  const config = ts.getParsedCommandLineOfConfigFile(join(HEALTH_CHECK, "tsconfig.json"), {}, { ...ts.sys, onUnRecoverableConfigFileDiagnostic: () => {} })!;
  const program = ts.createProgram(config.fileNames, config.options);
  const checker = program.getTypeChecker();
  const out: Producer[] = [];
  const typedAt = (e: ts.Expression): string => checker.typeToString(checker.getTypeAtLocation(e));
  // Each file's own top-level number constants, which the engine-text scan puts in as the text it always makes.
  const numbersOf = new Map<ts.SourceFile, Set<string>>();
  const numbers = (f: ts.SourceFile): Set<string> => {
    let held = numbersOf.get(f);
    if (held) return held;
    held = new Set();
    for (const s of f.statements) {
      if (!ts.isVariableStatement(s) || !(s.declarationList.flags & ts.NodeFlags.Const)) continue;
      for (const d of s.declarationList.declarations) if (ts.isIdentifier(d.name) && d.initializer && ts.isNumericLiteral(d.initializer)) held.add(d.name.text);
    }
    numbersOf.set(f, held);
    return held;
  };
  const said = (e: ts.Expression): boolean => {
    const t = checker.getTypeAtLocation(e);
    return (t.isUnion() ? t.types : [t]).every((p) => /\bSaid\b/.test(checker.typeToString(p)) || p.flags & (ts.TypeFlags.Null | ts.TypeFlags.Undefined));
  };
  const declarationOf = (e: ts.Expression): ts.Declaration | undefined => {
    let symbol = ts.isShorthandPropertyAssignment(e.parent) && e.parent.name === e ? checker.getShorthandAssignmentValueSymbol(e.parent) : checker.getSymbolAtLocation(e);
    if (symbol && symbol.flags & ts.SymbolFlags.Alias) symbol = checker.getAliasedSymbol(symbol);
    return symbol?.valueDeclaration ?? symbol?.declarations?.[0];
  };
  /** What a function returns: its expression body, or every return statement's expression. */
  const returnsOf = (fn: ts.SignatureDeclaration): ts.Expression[] => {
    const body = (fn as ts.FunctionLikeDeclaration).body;
    if (!body) return [];
    if (!ts.isBlock(body)) return [body];
    const found: ts.Expression[] = [];
    const walk = (n: ts.Node): void => {
      if (ts.isReturnStatement(n) && n.expression) found.push(n.expression);
      else if (!ts.isFunctionLike(n)) ts.forEachChild(n, walk);
    };
    walk(body);
    return found;
  };
  const RECOGNISER = "src/salesforce/refusal-words.ts";
  const kinds = (e: ts.Expression, depth = 0): Array<Producer["kind"]> => {
    while (ts.isParenthesizedExpression(e) || ts.isNonNullExpression(e) || ts.isAsExpression(e) || ts.isSatisfiesExpression(e)) e = e.expression;
    if (e.kind === ts.SyntaxKind.NullKeyword || (ts.isIdentifier(e) && e.text === "undefined")) return [];
    if (ts.isStringLiteral(e) || ts.isNoSubstitutionTemplateLiteral(e)) return ["whole"];
    if (ts.isConditionalExpression(e)) return [...kinds(e.whenTrue, depth), ...kinds(e.whenFalse, depth)];
    if (ts.isBinaryExpression(e) && [ts.SyntaxKind.QuestionQuestionToken, ts.SyntaxKind.BarBarToken].includes(e.operatorToken.kind)) return [...kinds(e.left, depth), ...kinds(e.right, depth)];
    if (ts.isArrayLiteralExpression(e)) return e.elements.flatMap((x) => (ts.isSpreadElement(x) ? kinds(x.expression, depth) : kinds(x, depth)));
    if (said(e)) return ["said"];
    if (ts.isTemplateExpression(e)) {
      const own = numbers(e.getSourceFile());
      if (e.templateSpans.every((s) => ts.isIdentifier(s.expression) && own.has(s.expression.text))) return ["whole"];
      const last = e.templateSpans[0]!;
      let x: ts.Expression = last.expression;
      while (ts.isNonNullExpression(x) || ts.isParenthesizedExpression(x)) x = x.expression;
      if (e.templateSpans.length === 1 && last.literal.text === "" && e.head.text !== "" && ts.isPropertyAccessExpression(x) && x.name.text === "reason") return ["passed-on"];
      return ["NOT RECOGNISED"];
    }
    if (ts.isTaggedTemplateExpression(e) || ts.isBinaryExpression(e)) return ["NOT RECOGNISED"];
    if (ts.isCallExpression(e) && /\.join$/.test(e.expression.getText())) return ["NOT RECOGNISED"];
    // A whole literal held in a constant or a literal type: the scan reads the literal where it is written.
    if (/^"[^"]*"(?: \| "[^"]*")*$/.test(typedAt(e))) return ["whole"];
    if (ts.isCallExpression(e)) {
      // A list mapped to sentences: each is what the callback returns.
      if (ts.isPropertyAccessExpression(e.expression) && /^(map|flatMap)$/.test(e.expression.name.text)) {
        const fn = e.arguments[0];
        if (fn && (ts.isArrowFunction(fn) || ts.isFunctionExpression(fn))) return returnsOf(fn).flatMap((r) => kinds(r, depth + 1));
      }
      // A helper of ours: what it returns. The recogniser's own sentences are known by it (src/salesforce/refusal-words.ts).
      const d = declarationOf(e.expression);
      const fn = d && (ts.isFunctionDeclaration(d) ? d : ts.isVariableDeclaration(d) && d.initializer && (ts.isArrowFunction(d.initializer) || ts.isFunctionExpression(d.initializer)) ? d.initializer : undefined);
      if (fn) {
        const where = relative(HEALTH_CHECK, fn.getSourceFile().fileName);
        if (where === RECOGNISER) return ["passed-on"];
        if (depth < 4 && !where.includes("node_modules")) return returnsOf(fn).flatMap((r) => kinds(r, depth + 1));
      }
    }
    // A list of exclusions or assumptions held in a variable or field: its items are producers where they are pushed or written.
    if ((ts.isIdentifier(e) || ts.isPropertyAccessExpression(e)) && LISTS.test(e.getText()) && /^(string|Said)\[\]$/.test(typedAt(e))) return ["whole"];
    // A read's or a loader's reason handed on whole: written where it is made, and read there.
    if (ts.isPropertyAccessExpression(e) && e.name.text === "reason") return ["passed-on"];
    if (ts.isIdentifier(e)) {
      const d = declarationOf(e);
      // A constant: what it holds.
      if (d && ts.isVariableDeclaration(d) && d.initializer && ts.getCombinedNodeFlags(d) & ts.NodeFlags.Const && depth < 4) return kinds(d.initializer, depth + 1);
      // A helper's parameter, every caller of which is read here as a producer.
      if (d && ts.isParameter(d)) {
        const fn = d.parent;
        const name = ts.isFunctionDeclaration(fn) ? fn.name?.text : ts.isVariableDeclaration(fn.parent) && ts.isIdentifier(fn.parent.name) ? fn.parent.name.text : undefined;
        if (name && CALLS.test(name)) return ["whole"];
      }
    }
    return ["ref"];
  };
  for (const source of program.getSourceFiles()) {
    const file = relative(HEALTH_CHECK, source.fileName);
    if (source.isDeclarationFile || file.includes("__tests__") || !PRODUCER_DIRS.some((d) => file === d || file.startsWith(`${d}/`))) continue;
    const typed = (e: ts.Expression): string => checker.typeToString(checker.getTypeAtLocation(e));
    const stringLike = (e: ts.Expression): boolean => {
      const t = checker.getTypeAtLocation(e);
      return (t.isUnion() ? t.types : [t]).every((p) => p.flags & (ts.TypeFlags.StringLike | ts.TypeFlags.Null | ts.TypeFlags.Undefined));
    };
    const add = (slot: string, e: ts.Expression) => {
      const line = source.getLineAndCharacterOfPosition(e.getStart()).line + 1;
      const all = kinds(e);
      const kind = all.includes("NOT RECOGNISED") ? "NOT RECOGNISED" : all.includes("ref") ? "ref" : all.includes("passed-on") ? "passed-on" : all.includes("said") ? "said" : "whole";
      out.push({ at: `${file}:${line}`, slot, kind, expression: e.getText().replace(/\s+/g, " ") });
    };
    const served = (o: ts.ObjectLiteralExpression): boolean => {
      const t = checker.getContextualType(o) ?? checker.getTypeAtLocation(o);
      const names = new Set((t.isUnion() ? t.types : [t]).flatMap((x) => x.getProperties().map((p) => p.name)));
      return !NOT_SERVED.some((n) => names.has(n)) || names.has("outcome") || names.has("definition");
    };
    const reasonOnly = file.startsWith(`${REASON_ONLY}/`);
    const visit = (node: ts.Node): void => {
      if (reasonOnly) {
        if ((ts.isPropertyAssignment(node) || ts.isShorthandPropertyAssignment(node)) && ts.isIdentifier(node.name) && node.name.text === "reason" && stringLike(ts.isPropertyAssignment(node) ? node.initializer : node.name)) add("field:reason", ts.isPropertyAssignment(node) ? node.initializer : node.name);
      } else if ((ts.isPropertyAssignment(node) || ts.isShorthandPropertyAssignment(node)) && ts.isIdentifier(node.name) && FIELDS.has(node.name.text) && ts.isObjectLiteralExpression(node.parent) && served(node.parent)) {
        // A label is text only beside a definition: elsewhere it names a subject or a field.
        if (node.name.text !== "label" || node.parent.properties.some((p) => p.name?.getText() === "definition")) add(`field:${node.name.text}`, ts.isPropertyAssignment(node) ? node.initializer : node.name);
      } else if (ts.isCallExpression(node)) {
        const callee = node.expression;
        if (ts.isPropertyAccessExpression(callee) && callee.name.text === "push" && LISTS.test(callee.expression.getText())) for (const a of node.arguments) add(`push:${callee.expression.getText()}`, a);
        else if (CALLS.test(callee.getText()) || (ts.isPropertyAccessExpression(callee) && callee.name.text === "assume")) for (const a of node.arguments) if (stringLike(a)) add(`call:${callee.getText()}`, a);
      } else if (ts.isVariableDeclaration(node) && ts.isIdentifier(node.name) && LISTS.test(node.name.text) && node.initializer && ts.isArrayLiteralExpression(node.initializer)) {
        add(`list:${node.name.text}`, node.initializer);
      }
      ts.forEachChild(node, visit);
    };
    visit(source);
  }
  return out;
}

/** A producer as the signed-off list names it: file and expression. */
export const producerKey = (p: Producer): string => `${p.at.replace(/:\d+$/, "")}: ${p.expression}`;
