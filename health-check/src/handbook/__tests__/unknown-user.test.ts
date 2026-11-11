import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { SALES_CLOUD_MODULES } from "../../domains/index.ts";
import type { DomainModule } from "../../domains/types.ts";
import { analyse } from "../../engine/evaluate.ts";
import type { Rule } from "../../engine/rule.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { Pseudonymiser, UNKNOWN_USER, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { loadPseudonymMap } from "../../render/render-run.ts";
import { docxText } from "../../testing/docx-text.ts";
import { tempDir, WINDOW } from "../../testing/harness.ts";
import { loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator, type SimulatorOptions } from "../../testing/simulator.ts";
import { APEX_CLASSES, APEX_OUTSIDE_MODIFIER, AS_OF, userId } from "../../testing/synthetic-org.ts";
import { HANDBOOK_FILE } from "../build.ts";
import { renderHandbookDocx } from "../docx.ts";
import { Handbook } from "../schema.ts";

// The engine labels a user id it has no name for with a placeholder, and the
// handbook prints that as a user whose name we couldn't look up, never as the
// placeholder (DECISIONS.md 474).

const read = <T>(dir: string, file: string): T => JSON.parse(readFileSync(join(dir, file), "utf8")) as T;
const OPTIONS = { clientName: "Synthetic Co", runDate: AS_OF };
const NOT_LOOKED_UP = "a user whose name we couldn't look up";
const mapOf = (dir: string): PseudonymEntry[] => read<PseudonymEntry[]>(dir, "pseudonym-map.json");
const textOf = async (dir: string): Promise<string> => docxText(await renderHandbookDocx(Handbook.parse(read(dir, HANDBOOK_FILE)), { ...OPTIONS, pseudonyms: loadPseudonymMap(dir) }));
/** One class's or trigger's paragraph in the document's text. */
const entry = (text: string, name: string): string => text.slice(text.indexOf(`\n${name}\n`) + 1).split("\n\n")[0]!;

/** Ids nobody names: not in the run's user list, and Salesforce gives no name beside them. */
const TRAIL_NOBODY = userId(970);
const APEX_NOBODY = userId(971);

const base = { Action: "changedinteractiondef", Section: "Flows", DelegateUser: null };
const setupAudit = [
  { ...base, Display: "Changed flow Invoice Reminder", CreatedDate: "2026-10-20T09:30:00.000+0000", CreatedById: TRAIL_NOBODY, CreatedBy: null },
  { ...base, Display: "Changed flow Lead Router", CreatedDate: "2026-10-21T09:30:00.000+0000", CreatedById: userId(4), CreatedBy: { Name: "Jo Ellis" } },
];
// FinanceSync last changed by an id with no LastModifiedBy beside it.
const apexClasses = APEX_CLASSES.map((c) => (c.Name === "FinanceSync" ? { ...c, LastModifiedById: APEX_NOBODY, LastModifiedBy: null } : c));
const OPTIONS_SIM: SimulatorOptions = { setupAudit, apexClasses };

async function simulated(outDir = tempDir(), resume?: string) {
  return runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir, now: new Date(AS_OF), transport: new Simulator(OPTIONS_SIM), log: () => {}, resume });
}

describe("a user the engine has no name for", () => {
  let dir: string;
  let text: string;
  before(async () => {
    dir = (await simulated()).dir;
    text = await textOf(dir);
  });

  test("is saved with the placeholder, from the trail and from a class alike", () => {
    const names = new Map(mapOf(dir).map((e) => [e.id, e.name]));
    assert.equal(names.get(TRAIL_NOBODY), UNKNOWN_USER);
    assert.equal(names.get(APEX_NOBODY), UNKNOWN_USER);
  });

  test("and from a rule's ctx.userLabel, so a token an analysis saved reads the same", () => {
    const pseudonyms = new Pseudonymiser();
    const rule: Rule = { id: "t.label", version: 1, domain: "test", title: "Labels a user", evaluate: (ctx) => ({ outcome: "not_measurable", reason: ctx.userLabel(APEX_NOBODY), evidence: [] }) };
    const module: DomainModule = { id: "test", cloud: "sales", label: "Test", extractors: [], rules: [rule] };
    analyse({ modules: [module], extracts: new Map(), calibration: loadCalibration(), now: new Date(AS_OF), window: WINDOW, answers: [], pseudonyms, userDirectory: new Map() });
    assert.deepEqual(pseudonyms.entries().map((e) => [e.id, e.name]), [[APEX_NOBODY, UNKNOWN_USER]]);
  });

  test("a class Salesforce gives an id but no name for, of someone the run's list doesn't hold, prints as a user we couldn't look up", () => {
    assert.match(entry(text, "FinanceSync"), new RegExp(`\\nLast changed 13 Oct 2026 by ${NOT_LOOKED_UP}\\n`));
  });

  test("a trail change by an id with no name anywhere prints the same in the recent changes", () => {
    assert.match(text, new RegExp(`\\n20 Oct 2026 at 09:30 UTC, ${NOT_LOOKED_UP}: Flows {2}changedinteractiondef\\nChanged flow Invoice Reminder(\\n|$)`));
  });

  test("the document never prints the placeholder", () => {
    assert.ok(!text.includes(UNKNOWN_USER));
  });

  test("a real name still prints, in each place", () => {
    assert.match(text, /\n21 Oct 2026 at 09:30 UTC, Jo Ellis: Flows {2}changedinteractiondef\nChanged flow Lead Router\n/);
    assert.match(entry(text, "OpportunityHandler"), /\nLast changed 21 Sep 2026 by Jo Ellis\n/);
    assert.match(entry(text, "InvoiceTrigger"), new RegExp(`\\nLast changed [^\\n]+ by ${APEX_OUTSIDE_MODIFIER.name}\\n`));
  });
});

describe("an id a saved map already holds as the placeholder", () => {
  test("still prints as a user we couldn't look up when the Apex row now gives a name, and its entry isn't rewritten", async () => {
    const first = await simulated();
    // As an earlier analysis would have saved it, before Salesforce named them.
    const saved = mapOf(first.dir).map((e) => (e.id === APEX_OUTSIDE_MODIFIER.id ? { ...e, name: UNKNOWN_USER, aliases: [UNKNOWN_USER, e.id] } : e));
    writeFileSync(join(first.dir, "pseudonym-map.json"), JSON.stringify(saved));
    const token = saved.find((e) => e.id === APEX_OUTSIDE_MODIFIER.id)!.token;
    await simulated(join(first.dir, ".."), first.run.id);

    const after = mapOf(first.dir).find((e) => e.id === APEX_OUTSIDE_MODIFIER.id)!;
    assert.deepEqual([after.token, after.name], [token, UNKNOWN_USER]);
    const h = Handbook.parse(read(first.dir, HANDBOOK_FILE));
    assert.ok(h.apexTriggers.status === "ok");
    assert.equal(h.apexTriggers.items.find((t) => t.name === "InvoiceTrigger")!.lastChanged!.by, token);
    const text = await textOf(first.dir);
    assert.match(entry(text, "InvoiceTrigger"), new RegExp(`\\nLast changed [^\\n]+ by ${NOT_LOOKED_UP}\\n`));
    assert.ok(!text.includes(UNKNOWN_USER) && !text.includes(APEX_OUTSIDE_MODIFIER.name));
  });
});

describe("the placeholder", () => {
  const SRC = join(fileURLToPath(new URL(".", import.meta.url)), "..", "..");
  const files = (path: string): string[] =>
    readdirSync(path, { withFileTypes: true }).flatMap((e) => {
      const next = join(path, e.name);
      if (e.isDirectory()) return e.name === "__tests__" || e.name === "node_modules" ? [] : files(next);
      return e.name.endsWith(".ts") ? [next] : [];
    });

  test("is spelled once, beside the pseudonymiser, so every caller labels with the same word the handbook looks for", () => {
    const all = files(SRC);
    assert.ok(all.length > 100, `read ${all.length} files`);
    const spelled = all.flatMap((f) => (readFileSync(f, "utf8").match(/["'`]Unknown user["'`]/g) ?? []).map(() => relative(SRC, f)));
    assert.deepEqual(spelled, [join("privacy", "pseudonymiser.ts")]);
    for (const file of [join("engine", "evaluate.ts"), join("engine", "run.ts"), join("handbook", "docx.ts")]) assert.match(readFileSync(join(SRC, file), "utf8"), /\bUNKNOWN_USER\b/, file);
  });
});
