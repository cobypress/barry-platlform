import { test } from "node:test";
import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript";

// A named value is the value the computation used, never a number written beside
// the sentence (DECISIONS.md 768): a literal there would trace the sentence's own
// number to itself, which is the circle the strict check exists to break. So no
// call of named() (imported under any name) or each() may hold a numeric literal
// in its values, nor work a value out again: each value is a name, a property or
// an element of one, and a computed one is worked out once, before the sentence.

const ROOT = fileURLToPath(new URL("../../..", import.meta.url));

function* sources(dir: string): Generator<string> {
  for (const e of readdirSync(dir, { withFileTypes: true })) {
    const p = join(dir, e.name);
    if (e.isDirectory()) {
      if (e.name !== "__tests__" && e.name !== "node_modules") yield* sources(p);
    } else if (e.name.endsWith(".ts")) yield p;
  }
}

/** The local names named() and each() are imported under in a file. */
function namesOf(file: ts.SourceFile): { named: Set<string>; each: Set<string> } {
  const out = { named: new Set<string>(), each: new Set<string>() };
  for (const s of file.statements) {
    if (!ts.isImportDeclaration(s) || !ts.isStringLiteral(s.moduleSpecifier) || !/engine\/named\.ts$|\.\/named\.ts$/.test(s.moduleSpecifier.text)) continue;
    const bindings = s.importClause?.namedBindings;
    if (!bindings || !ts.isNamedImports(bindings)) continue;
    for (const el of bindings.elements) {
      const imported = (el.propertyName ?? el.name).text;
      if (imported === "named") out.named.add(el.name.text);
      if (imported === "each") out.each.add(el.name.text);
    }
  }
  return out;
}

/** Why a value expression isn't simply the computation's own value, or null when it is. */
function fault(e: ts.Expression): string | null {
  if (ts.isNumericLiteral(e) || (ts.isPrefixUnaryExpression(e) && ts.isNumericLiteral(e.operand))) return "a numeric literal";
  if (ts.isIdentifier(e) || ts.isPropertyAccessExpression(e) || ts.isElementAccessExpression(e)) {
    let bad: string | null = null;
    const look = (n: ts.Node): void => {
      if (ts.isNumericLiteral(n) && !(ts.isElementAccessExpression(n.parent) && n.parent.argumentExpression === n)) bad = "a numeric literal";
      else if (ts.isCallExpression(n)) bad = "a value worked out again";
      n.forEachChild(look);
    };
    look(e);
    return bad;
  }
  if (ts.isNonNullExpression(e) || ts.isParenthesizedExpression(e)) return fault(e.expression);
  // A value that may be absent: `worst?.rate`, or `x ? x.rate : null`.
  if (ts.isConditionalExpression(e)) return fault(e.whenTrue) ?? (e.whenFalse.kind === ts.SyntaxKind.NullKeyword ? null : fault(e.whenFalse));
  // An explicit unit: [value, "percent"].
  if (ts.isArrayLiteralExpression(e) && e.elements.length === 2 && ts.isStringLiteral(e.elements[1]!)) return fault(e.elements[0] as ts.Expression);
  if (ts.isCallExpression(e)) return "a value worked out again";
  return "an expression";
}

function faultsIn(written: ts.Expression, at: string, out: string[]): void {
  const values = ts.isParenthesizedExpression(written) ? written.expression : written;
  if (!ts.isObjectLiteralExpression(values)) return void out.push(`${at}: values that aren't an object written out`);
  for (const p of values.properties) {
    if (ts.isShorthandPropertyAssignment(p)) continue;
    if (ts.isSpreadAssignment(p)) {
      if (!ts.isCallExpression(p.expression)) out.push(`${at}: a spread that isn't each()`);
      continue;
    }
    if (!ts.isPropertyAssignment(p)) continue;
    const why = fault(p.initializer);
    if (why) out.push(`${at}: ${p.name.getText()} is ${why}`);
  }
}

function faults(path: string): string[] {
  const text = readFileSync(path, "utf8");
  if (!text.includes("named")) return [];
  const file = ts.createSourceFile(path, text, ts.ScriptTarget.Latest, true);
  const names = namesOf(file);
  const out: string[] = [];
  const visit = (n: ts.Node): void => {
    if (ts.isCallExpression(n) && ts.isIdentifier(n.expression)) {
      const at = `${relative(ROOT, path)}:${file.getLineAndCharacterOfPosition(n.getStart()).line + 1}`;
      if (names.named.has(n.expression.text) && n.arguments[1]) {
        const v = n.arguments[1];
        // each()'s values are read where each() is called, below.
        if (!(ts.isCallExpression(v) && ts.isIdentifier(v.expression) && names.each.has(v.expression.text))) faultsIn(v, at, out);
      }
      if (names.each.has(n.expression.text) && n.arguments[1] && ts.isArrowFunction(n.arguments[1])) {
        const body = n.arguments[1].body;
        faultsIn(body as ts.Expression, at, out);
      }
    }
    n.forEachChild(visit);
  };
  visit(file);
  return out;
}

test("every named value is the computation's own value: no numeric literal, nothing worked out again (DECISIONS.md 768)", () => {
  const found = [...sources(join(ROOT, "src"))].filter((p) => !p.endsWith(join("engine", "named.ts"))).flatMap(faults);
  assert.deepEqual([...new Set(found)], []);
});

test("the lint sees what it should", () => {
  const dir = fileURLToPath(new URL(".", import.meta.url));
  void dir;
  const probe = (body: string): string[] => {
    const file = ts.createSourceFile("x.ts", `import { named, each } from "../engine/named.ts";\n${body}`, ts.ScriptTarget.Latest, true);
    const out: string[] = [];
    const visit = (n: ts.Node): void => {
      if (ts.isCallExpression(n) && ts.isIdentifier(n.expression) && n.expression.text === "named" && n.arguments[1]) {
        const v = n.arguments[1];
        if (ts.isCallExpression(v)) faultsIn((v.arguments[1] as ts.ArrowFunction).body as ts.Expression, "x", out);
        else faultsIn(v, "x", out);
      }
      n.forEachChild(visit);
    };
    visit(file);
    return out;
  };
  assert.deepEqual(probe("named(t, { days: 90 });"), ["x: days is a numeric literal"]);
  assert.deepEqual(probe("named(t, { pct: RATE * 100 });"), ["x: pct is an expression"]);
  assert.deepEqual(probe("named(t, { n: rows.filter((r) => r).length });"), ["x: n is a value worked out again"]);
  assert.deepEqual(probe("named(t, { n: sum(deals) });"), ["x: n is a value worked out again"]);
  assert.deepEqual(probe("named(t, { days, n: x.n, m: totals[o]!, w: worst?.rate, u: [limit, \"percent\"] });"), []);
  assert.deepEqual(probe("named(t, each(rows, (r) => ({ n: 3 })));"), ["x: n is a numeric literal"]);
});

// word() (DECISIONS.md 773) says a number word of ours claims nothing, so the strict check sets it aside.
// It may only ever wrap one fixed word as written: never a digit, which is a figure, and never a value,
// which could be anything; a claim ("one account holds…") is named, never wrapped.
function wordFaults(path: string, text: string): string[] {
  if (!text.includes("word(") && !text.includes("aside(")) return [];
  const file = ts.createSourceFile(path, text, ts.ScriptTarget.Latest, true);
  const local = new Map<string, string>();
  for (const s of file.statements) {
    if (!ts.isImportDeclaration(s) || !ts.isStringLiteral(s.moduleSpecifier) || !/(org-words|named)\.ts$/.test(s.moduleSpecifier.text)) continue;
    const b = s.importClause?.namedBindings;
    // word(w), and aside(text, w), its form for a whole literal: w is what is checked.
    if (b && ts.isNamedImports(b)) for (const el of b.elements) if (["word", "aside"].includes((el.propertyName ?? el.name).text)) local.set(el.name.text, (el.propertyName ?? el.name).text);
  }
  const out: string[] = [];
  const visit = (n: ts.Node): void => {
    if (ts.isCallExpression(n) && ts.isIdentifier(n.expression) && local.has(n.expression.text)) {
      const at = `${relative(ROOT, path)}:${file.getLineAndCharacterOfPosition(n.getStart()).line + 1}`;
      const [arg, ...rest] = local.get(n.expression.text) === "aside" ? n.arguments.slice(1) : n.arguments;
      if (!arg || rest.length > 0 || !(ts.isStringLiteral(arg) || ts.isNoSubstitutionTemplateLiteral(arg))) out.push(`${at}: word() around something other than one literal`);
      else if (/\d/.test(arg.text)) out.push(`${at}: word() around a digit`);
    }
    n.forEachChild(visit);
  };
  visit(file);
  return out;
}

test("word() wraps only one fixed word, never a digit or a value (DECISIONS.md 773)", () => {
  assert.deepEqual([...sources(join(ROOT, "src"))].flatMap((p) => wordFaults(p, readFileSync(p, "utf8"))), []);
  const imp = 'import { word } from "../engine/org-words.ts";\n';
  assert.deepEqual(wordFaults("x.ts", `${imp}said\`in ${"${word(\"one\")}"} stage\`;`), []);
  assert.deepEqual(wordFaults("x.ts", `${imp}word("3 deals");`), ["x.ts:2: word() around a digit"]);
  assert.deepEqual(wordFaults("x.ts", `${imp}word(count);`), ["x.ts:2: word() around something other than one literal"]);
  assert.deepEqual(wordFaults("x.ts", `${imp}word(\`one \${n}\`);`), ["x.ts:2: word() around something other than one literal"]);
  const asideImp = 'import { aside } from "../engine/named.ts";\n';
  assert.deepEqual(wordFaults("x.ts", `${asideImp}aside("Each move from one stage", "one");`), []);
  assert.deepEqual(wordFaults("x.ts", `${asideImp}aside("A gap of 10 points", "10");`), ["x.ts:2: word() around a digit"]);
});
