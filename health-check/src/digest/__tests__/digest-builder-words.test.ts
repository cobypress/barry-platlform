import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import ts from "typescript";
import { checkedDigest, digestTextsAt, marksOf } from "../checked.ts";
import { PLAN_HOURS_APART } from "../build.ts";
import { ENGINE_TEXT_DIRS } from "../engine-texts-scan.ts";
import { engineOwn } from "../../engine/automatic-words.ts";
import { UNSCORED_RULES } from "../../domains/scoring.ts";
import { OBJECT_INVENTORY } from "../../domains/configuration/inventory.ts";
import type { ReportInput } from "../../narrative/sections.ts";
import { loadRun } from "../../render/render-run.ts";
import { refusalSentence } from "../../salesforce/refusal-words.ts";
import type { Finding, Metric } from "../../schemas/index.ts";
import { AutomaticReplay, automaticSyntheticCalibration } from "../../testing/automatic.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording } from "../../testing/harness.ts";

// The sentences the digest's builder writes itself, and the metric texts a rule
// puts a count into, marked on an automatic run (DECISIONS.md 739). A real
// org's first automatic run was held for one: the plan's hours note, which the
// builder writes when a code checker finding is in the plan, and which the
// synthetic and scale orgs never reach, as no code checker finding is raised
// on them. Built here from the synthetic org, with no real data.

const calibration = await automaticSyntheticCalibration();
const made = await runFromRecording({ calibration, reviewMode: "automatic", transport: new AutomaticReplay() });
const loaded = loadRun(made.dir);
const check = (input: ReportInput) => checkedDigest({ input, pseudonyms: loaded.pseudonyms, inventory: null, madeAt: "2026-11-25T09:00:00.000Z" });

describe("the plan's hours note on an automatic run (DECISIONS.md 739)", () => {
  /** The synthetic org's findings with a code checker issue in the plan, as a real org with Apex has. */
  const withCodeChecker = (): ReportInput => {
    const like = loaded.input.findings.find((f) => f.status === "issue" && f.effortHours)!;
    const lint: Finding = { ...like, id: "f-00000000c0de", ruleId: [...UNSCORED_RULES][0]! };
    return { ...loaded.input, findings: [...loaded.input.findings, lint] };
  };

  test("is served marked as ours, and the run is released, not held", () => {
    const c = check(withCodeChecker());
    assert.equal(c.digest!.plan.hoursTotal?.note, PLAN_HOURS_APART);
    assert.deepEqual(c.digest!.plan.hoursTotal?.orgWords?.note, { orgWordsMarked: true, orgWords: [] });
    for (const t of digestTextsAt(c.digest!)) assert.ok(marksOf(c.digest!, t), t.path);
    assert.deepEqual({ unmatched: c.unmatched, unmarked: c.unmarked, held: c.held }, { unmatched: 0, unmarked: 0, held: null });
  });

  test("without a code checker finding there is no note, as on the synthetic org", () => {
    assert.equal(check(loaded.input).digest!.plan.hoursTotal?.note, undefined);
  });

  test("the builder's own sentences are known as the engine's, because its folder is scanned", () => {
    assert.ok(ENGINE_TEXT_DIRS.includes("digest"));
    for (const text of [PLAN_HOURS_APART, "The overall score couldn't be kept."]) assert.ok(engineOwn(text, new Set()), text);
  });

  test("never as ours where the org chose the same words, or where the note reads any other way", () => {
    assert.equal(engineOwn(PLAN_HOURS_APART, new Set([PLAN_HOURS_APART])), false);
    assert.equal(engineOwn(PLAN_HOURS_APART.replace("our code checker", "Invoice__c"), new Set()), false);
  });
});

describe("the object inventory's unnamed layouts and triggers on an automatic run (DECISIONS.md 739)", () => {
  const extracts = loadExtracts(made.dir);
  const inventory = (changed: Map<string, unknown>): Extract<Metric, { status: "measured" }> => {
    const m = analyseExtracts(changed, { calibration }).metrics.find((x) => x.id === OBJECT_INVENTORY)!;
    assert.equal(m.status, "measured");
    return m as Extract<Metric, { status: "measured" }>;
  };
  const marksAt = (m: Extract<Metric, { status: "measured" }>, pattern: RegExp) => {
    const i = m.exclusions.findIndex((e) => pattern.test(e));
    assert.ok(i >= 0, m.exclusions.join(" | "));
    return { text: m.exclusions[i]!, marks: m.orgWords?.exclusions?.[i] ?? null };
  };

  test("the counts of layouts and triggers on objects we couldn't name are marked, with none of the org's words", () => {
    const changed = clone(extracts);
    // A custom object Tooling doesn't list, as a real org's can be: its layouts and triggers can't be named.
    const ids = changed.get("custom-objects") as { data: Array<{ DeveloperName: string }> };
    ids.data = ids.data.filter((r) => r.DeveloperName !== "Invoice");
    const { text, marks } = marksAt(inventory(changed), /^Page layouts \(\d+\) and Apex triggers \(\d+\) on custom objects we couldn't name$/);
    assert.ok(marks, text);
    assert.deepEqual(marks.spans, []);
  });

  test("an unreadable list's reason is passed on in its own text, the object it names marked as the org's", () => {
    const changed = clone(extracts);
    const reason = refusalSentence("EntityDefinition", "no_permission");
    changed.set("custom-objects", { status: "unavailable", reason, source: { kind: "soql", api: "tooling", query: "x" } });
    const m = inventory(changed);
    assert.ok(marksAt(m, /^Page layouts \(\d+\) and Apex triggers \(\d+\) on custom objects we couldn't name$/).marks);
    const { text, marks } = marksAt(m, /^The names of custom objects, because the list of custom objects couldn't be read: /);
    assert.ok(marks, text);
    const start = text.indexOf("EntityDefinition");
    assert.deepEqual(marks.spans, [{ start, end: start + "EntityDefinition".length }]);
  });
});

describe("a metric's exclusion or assumption added as the rule runs (DECISIONS.md 739)", () => {
  test("is said, or a reason passed on after words of ours, never a plain template with something else put in", () => {
    const root = join(import.meta.dirname, "../../domains");
    const files = (dir: string): string[] => readdirSync(dir, { withFileTypes: true }).flatMap((e) => (e.isDirectory() ? (e.name === "__tests__" ? [] : files(join(dir, e.name))) : e.name.endsWith(".ts") ? [join(dir, e.name)] : []));
    // `${x.reason}` alone, last: the passed-on route knows it by its head (702).
    const passedOn = (e: ts.TemplateExpression): boolean => e.templateSpans.length === 1 && e.templateSpans[0]!.literal.text === "" && ts.isPropertyAccessExpression(e.templateSpans[0]!.expression) && e.templateSpans[0]!.expression.name.text === "reason";
    const plain = (e: ts.Expression): boolean => (ts.isTemplateExpression(e) ? !passedOn(e) : ts.isConditionalExpression(e) ? plain(e.whenTrue) || plain(e.whenFalse) : false);
    const found: string[] = [];
    let pushes = 0;
    for (const file of files(root)) {
      const source = ts.createSourceFile(file, readFileSync(file, "utf8"), ts.ScriptTarget.Latest, true);
      const visit = (node: ts.Node): void => {
        if (ts.isCallExpression(node) && ts.isPropertyAccessExpression(node.expression) && node.expression.name.text === "push" && ts.isIdentifier(node.expression.expression) && /^(exclusions|assumptions)$/.test(node.expression.expression.text)) {
          pushes++;
          for (const a of node.arguments) if (plain(a)) found.push(`${file.slice(root.length)}: ${a.getText().slice(0, 100)}`);
        }
        ts.forEachChild(node, visit);
      };
      visit(source);
    }
    assert.ok(pushes > 20, `${pushes} pushes`);
    assert.deepEqual(found, []);
  });
});
