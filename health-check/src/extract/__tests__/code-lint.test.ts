import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import { existsSync, mkdirSync, readdirSync, readFileSync, symlinkSync, utimesSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { technicalHealthModule } from "../../domains/technical-health/index.ts";
import { runHealthCheck, type RunOptions } from "../../engine/run.ts";
import { BudgetStopError } from "../../salesforce/budget.ts";
import { SalesforceError } from "../../salesforce/client.ts";
import { tempDir } from "../../testing/harness.ts";
import { loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { AS_OF, CONNECTED_USER } from "../../testing/synthetic-org.ts";
import { codeLintExtractor, LINT_BUDGET_ROOM, type CodeLintData, type CodeLintTools } from "../code-lint.ts";
import { ESLINT_CONFIG, ESLINT_RULES, ESLINT_VERSION, lintJavaScript, NO_JAVASCRIPT, type JsFile } from "../lint/eslint.ts";
import { bound, MAX_FINDINGS_PER_COMPONENT, MAX_FINDINGS_PER_RUN, type LintFinding, type ToolRun } from "../lint/findings.ts";
import { findPmd, LINT_TEMP_PREFIX, livePmdRuns, PMD_NOT_INSTALLED, PMD_WRAPPER, PMD_RULES, PMD_RULESET, PMD_VERSION, redactKeepingLines, runPmd, sweepLintTemp, type ApexFile } from "../lint/pmd.ts";
import type { ExtractContext } from "../types.ts";
import {
  APEX_CLEAN,
  APEX_ENORMOUS,
  APEX_PRIVATE_KEY,
  APEX_SUPPRESSED,
  APEX_WITH_HITS,
  fakePmd,
  JS_BROKEN,
  JS_CLEAN,
  JS_WITH_HITS,
  JS_WITH_INLINE_CONFIG,
  PLANTED_CUSTOMER,
  PLANTED_ESLINTRC,
  PLANTED_SECRET,
  TRIGGER_WITH_HIT,
} from "./lint-fixtures/fixtures.ts";

// Decision 670: PMD and ESLint beside our own code scan, storing only where
// each finding is and which of our rules it broke. PMD here is a stand-in
// script (lint-fixtures/fake-pmd.mjs), so the suite needs no Java; real PMD
// 7.29.0 was run once by hand to check the stand-in's report shape.

const ran = (r: ToolRun): Extract<ToolRun, { status: "ran" }> => {
  assert.equal(r.status, "ran", r.status === "not_run" ? r.reason : "");
  return r as Extract<ToolRun, { status: "ran" }>;
};
const notRunReason = (r: ToolRun): string => {
  assert.equal(r.status, "not_run");
  return (r as Extract<ToolRun, { status: "not_run" }>).reason;
};
const lintTemps = (base: string): string[] => readdirSync(base).filter((n) => n.startsWith(LINT_TEMP_PREFIX));
const apex = (name: string, body: string, kind: ApexFile["kind"] = "class"): ApexFile => ({ kind, name, body });
/** Nothing from the code itself: no planted string, no line of it. */
function assertNoSource(text: string): void {
  assert.ok(!text.includes(PLANTED_SECRET), "the planted secret is in it");
  assert.ok(!text.includes(PLANTED_CUSTOMER), "the planted customer is in it");
  assert.ok(!text.toLowerCase().includes(PLANTED_CUSTOMER.toLowerCase()), "the planted customer's identifier is in it");
  assert.ok(!/SELECT Id FROM/.test(text), "a line of the code is in it");
  assert.ok(!/catch \(Exception/.test(text), "a line of the code is in it");
}

function setup(): { base: string; temp: string; pmd: ReturnType<typeof fakePmd> } {
  const base = tempDir();
  const temp = join(base, "tmp");
  mkdirSync(temp);
  return { base, temp, pmd: fakePmd(base) };
}

describe("PMD: what is stored", () => {
  test("findings per line, with tool, version, rule, category, severity, component, line and column, and never a message or the code", async () => {
    const { temp, pmd } = setup();
    const result = ran(await runPmd([apex("InvoiceSync", APEX_WITH_HITS), apex("Clean", APEX_CLEAN), apex("AccountTrigger", TRIGGER_WITH_HIT, "trigger")], pmd, { tempBase: temp }));
    assert.equal(result.checked, 3);
    assert.deepEqual(
      result.findings.map((f) => [f.kind, f.component, f.rule, f.line]),
      [
        ["class", "InvoiceSync", "ApexCRUDViolation", 3],
        ["class", "InvoiceSync", "UnusedLocalVariable", 5],
        ["class", "InvoiceSync", "EmptyCatchBlock", 6],
        ["class", "InvoiceSync", "UnusedLocalVariable", 7],
        ["trigger", "AccountTrigger", "EmptyCatchBlock", 2],
      ],
    );
    assert.deepEqual(result.findings[0], { tool: "pmd", toolVersion: PMD_VERSION, rule: "ApexCRUDViolation", category: "Security", severity: 3, kind: "class", component: "InvoiceSync", line: 3, column: 30 });
    assert.deepEqual(Object.keys(result.findings[0]!).sort(), ["category", "column", "component", "kind", "line", "rule", "severity", "tool", "toolVersion"]);
    // Our own descriptions, for the rules that have a finding.
    assert.deepEqual(result.rules, { ApexCRUDViolation: PMD_RULES.ApexCRUDViolation, EmptyCatchBlock: PMD_RULES.EmptyCatchBlock, UnusedLocalVariable: PMD_RULES.UnusedLocalVariable });
    assert.deepEqual(result.components, [
      { kind: "class", component: "InvoiceSync", found: 4, stored: 4 },
      { kind: "trigger", component: "AccountTrigger", found: 1, stored: 1 },
    ]);
    assertNoSource(JSON.stringify(result));
  });

  test("PMD is handed the code numbered, never named, with secrets out and every line where it was, and none of the worker's environment", async () => {
    const { temp, pmd } = setup();
    process.env.HC_TOKEN_KEY_PLANTED_670 = PLANTED_SECRET;
    try {
      await runPmd([apex("InvoiceSync", APEX_WITH_HITS), apex("Signer", APEX_PRIVATE_KEY)], pmd, { tempBase: temp });
    } finally {
      delete process.env.HC_TOKEN_KEY_PLANTED_670;
    }
    const seen = JSON.parse(readFileSync(join(pmd.home, "seen.json"), "utf8")) as { files: Record<string, string>; env: Record<string, string>; args: string[] };
    assert.deepEqual(Object.keys(seen.files), ["0.cls", "1.cls"]);
    assert.ok(!seen.files["0.cls"]!.includes(PLANTED_SECRET));
    assert.equal(seen.files["0.cls"]!.split("\n").length, APEX_WITH_HITS.split("\n").length);
    assert.ok(!seen.files["1.cls"]!.includes("MIIEowIBAAKCAQEAplanted"));
    assert.equal(seen.files["1.cls"]!.split("\n").length, APEX_PRIVATE_KEY.split("\n").length);
    assert.deepEqual(Object.keys(seen.env).filter((k) => !["PWD", "SHLVL", "_", "OLDPWD", "__CF_USER_TEXT_ENCODING"].includes(k)).sort(), ["HOME", "LANG", "PATH", "PMD_JAVA_OPTS", "TMPDIR"]);
    assert.ok(!JSON.stringify(seen.env).includes(PLANTED_SECRET));
    assert.match(seen.env.PMD_JAVA_OPTS!, /-DsocksProxyHost=127\.0\.0\.1 -DsocksProxyPort=9/);
    // Our ruleset, and only ours.
    assert.equal(seen.args.filter((a) => a === "--rulesets").length, 1);
    assert.equal(seen.args[seen.args.indexOf("--rulesets") + 1], PMD_RULESET);
    assert.ok(seen.args.includes("--no-cache"));
  });

  test("redaction keeps every line number", () => {
    const body = `a\nString k = 'x';\n-----BEGIN PRIVATE KEY-----\nAAA\nBBB\n-----END PRIVATE KEY-----\nString apiKey = '${PLANTED_SECRET}';\nz`;
    const out = redactKeepingLines(body);
    assert.equal(out.split("\n").length, body.split("\n").length);
    assert.equal(out.split("\n")[7], "z");
    assert.ok(!out.includes(PLANTED_SECRET) && !out.includes("AAA"));
  });

  test("@SuppressWarnings and NOPMD: PMD honours them, and we keep what they hid, marked, without the comment's words", async () => {
    const { temp, pmd } = setup();
    const result = ran(await runPmd([apex("Quiet", APEX_SUPPRESSED)], pmd, { tempBase: temp }));
    assert.deepEqual(
      result.findings.map((f) => [f.rule, f.line, f.suppressed]),
      [
        ["EmptyCatchBlock", 3, true],
        ["UnusedLocalVariable", 4, true],
      ],
    );
    assert.ok(!JSON.stringify(result).includes("team said so"));
    assertNoSource(JSON.stringify(result));
  });

  test("a rule outside our ruleset is dropped, and counted", async () => {
    const { temp, pmd } = setup();
    const result = ran(await runPmd([apex("Odd", "public class Odd { // FAKE_PMD_FOREIGN_RULE\n}")], pmd, { tempBase: temp }));
    assert.equal(result.findings.length, 0);
    assert.equal(result.outsideRules, 1);
  });

  test("a file PMD can't parse is counted, and its error text, which quotes the code, goes no further", async () => {
    const { temp, pmd } = setup();
    const result = ran(await runPmd([apex("Broken", `public class Broken { ${PLANTED_CUSTOMER} FAKE_PMD_PARSE_ERROR`), apex("InvoiceSync", APEX_WITH_HITS)], pmd, { tempBase: temp }));
    assert.equal(result.skipped.unparsable, 1);
    assert.equal(result.checked, 1);
    assertNoSource(JSON.stringify(result));
  });

  test("an enormous body isn't handed over, and is counted", async () => {
    const { temp, pmd } = setup();
    const result = ran(await runPmd([apex("Enormous", APEX_ENORMOUS), apex("Clean", APEX_CLEAN)], pmd, { tempBase: temp }));
    assert.equal(result.skipped.tooLarge, 1);
    const seen = JSON.parse(readFileSync(join(pmd.home, "seen.json"), "utf8")) as { files: Record<string, string> };
    assert.deepEqual(Object.keys(seen.files), ["0.cls"]);
    assert.equal(seen.files["0.cls"], APEX_CLEAN);
  });
});

describe("PMD: bounded output", () => {
  test("at most 50 a component, with how many were found", async () => {
    const { temp, pmd } = setup();
    const result = ran(await runPmd([apex("Many", "public class Many { // FAKE_PMD_MANY\n}")], pmd, { tempBase: temp }));
    assert.equal(result.found, 120);
    assert.equal(result.stored, MAX_FINDINGS_PER_COMPONENT);
    assert.deepEqual(result.components, [{ kind: "class", component: "Many", found: 120, stored: 50 }]);
  });

  test("at most 2,000 a run, worst first, whatever order they came in", () => {
    const all: LintFinding[] = [];
    for (let c = 0; c < 60; c++) for (let i = 0; i < 40; i++) all.push({ tool: "pmd", toolVersion: PMD_VERSION, rule: "EmptyCatchBlock", category: "Error Prone", severity: i === 0 ? 1 : 3, kind: "class", component: `C${String(c).padStart(2, "0")}`, line: i + 1, column: 1 });
    const out = bound(all);
    assert.equal(out.found, 2400);
    assert.equal(out.stored, MAX_FINDINGS_PER_RUN);
    // Every priority 1 kept, though they're spread over every component.
    assert.equal(out.findings.filter((f) => f.severity === 1).length, 60);
    assert.deepEqual(bound([...all].reverse()), out);
    assert.equal(out.components.reduce((n, c) => n + c.stored, 0), MAX_FINDINGS_PER_RUN);
  });
});

describe("PMD: never fails the run, and the temporary folder never outlives it", () => {
  test("after success the folder is gone", async () => {
    const { temp, pmd } = setup();
    ran(await runPmd([apex("InvoiceSync", APEX_WITH_HITS)], pmd, { tempBase: temp }));
    assert.deepEqual(lintTemps(temp), []);
  });

  test("a crash is not run, with no word of what it printed, and the folder is gone", async () => {
    const { temp, pmd } = setup();
    const reason = notRunReason(await runPmd([apex("Crash", `public class Crash { // FAKE_PMD_CRASH ${PLANTED_CUSTOMER}\n}`)], pmd, { tempBase: temp }));
    assert.equal(reason, "PMD stopped with exit code 1, so its findings weren't read.");
    assert.deepEqual(lintTemps(temp), []);
  });

  test("a hang is stopped at the time limit, not run, and the folder is gone", async () => {
    const { temp, pmd } = setup();
    const started = Date.now();
    const reason = notRunReason(await runPmd([apex("Hang", "public class Hang { // FAKE_PMD_HANG\n}")], pmd, { tempBase: temp, timeLimitMs: 1500 }));
    assert.match(reason, /didn't finish within 2 seconds, so it was stopped/);
    assert.ok(Date.now() - started < 10_000);
    assert.deepEqual(lintTemps(temp), []);
  });

  test("a report over the size limit is not read", async () => {
    const { temp, pmd } = setup();
    const reason = notRunReason(await runPmd([apex("Flood", "public class Flood { // FAKE_PMD_FLOOD\n}")], pmd, { tempBase: temp, maxReportBytes: 100_000 }));
    assert.match(reason, /larger than|grew past/);
    assert.deepEqual(lintTemps(temp), []);
  });

  test("a report that isn't JSON, or from another PMD version, is set aside", async () => {
    const { temp, pmd } = setup();
    assert.equal(notRunReason(await runPmd([apex("G", `public class G { // FAKE_PMD_GARBAGE ${PLANTED_CUSTOMER}\n}`)], pmd, { tempBase: temp })), "PMD's report couldn't be read.");
    assert.match(notRunReason(await runPmd([apex("V", "public class V { // FAKE_PMD_WRONG_VERSION\n}")], pmd, { tempBase: temp })), /other than version 7\.29\.0/);
    assert.deepEqual(lintTemps(temp), []);
  });

  test("a PMD that won't start is not run, and the folder is gone", async () => {
    const { temp, pmd } = setup();
    const reason = notRunReason(await runPmd([apex("A", APEX_CLEAN)], { ...pmd, bin: join(pmd.home, "bin", "missing") }, { tempBase: temp }));
    assert.match(reason, /PMD couldn't be started \(exit code 127\)/);
    assert.deepEqual(lintTemps(temp), []);
  });

  test("a crashed step's folder is swept at the next start once it's older than twice the limit; a live one is left", async () => {
    const { temp, pmd } = setup();
    const old = join(temp, `${LINT_TEMP_PREFIX}crashed`);
    const live = join(temp, `${LINT_TEMP_PREFIX}live`);
    mkdirSync(old);
    writeFileSync(join(old, "0.cls"), APEX_WITH_HITS);
    mkdirSync(live);
    const longAgo = new Date(Date.now() - 3 * 60_000);
    utimesSync(old, longAgo, longAgo);
    await runPmd([apex("A", APEX_CLEAN)], pmd, { tempBase: temp, timeLimitMs: 60_000 });
    assert.deepEqual(lintTemps(temp), [`${LINT_TEMP_PREFIX}live`]);
    assert.equal(sweepLintTemp(join(temp, "nowhere"), 0), 0);
  });
});

describe("PMD: where it's found", () => {
  test("absent, a version other than ours, or without Java: not found, with why", () => {
    const base = tempDir();
    assert.deepEqual(findPmd(null, null), { missing: PMD_NOT_INSTALLED });
    const other = fakePmd(join(base, "other"), "7.28.0");
    assert.match((findPmd(other.home, other.javaHome) as { missing: string }).missing, /isn't version 7\.29\.0/);
    const ours = fakePmd(join(base, "ours"));
    assert.match((findPmd(ours.home, join(base, "nojava")) as { missing: string }).missing, /Java isn't/);
    assert.deepEqual(findPmd(ours.home, ours.javaHome), { found: { bin: ours.bin, javaHome: ours.javaHome } });
  });

  test("the ruleset is ours: exactly the rules we describe, in order, each named one by one", () => {
    const xml = readFileSync(PMD_RULESET, "utf8");
    const refs = [...xml.matchAll(/<rule ref="category\/apex\/[a-z]+\.xml\/(\w+)"\/>/g)].map((m) => m[1]);
    assert.deepEqual(refs, Object.keys(PMD_RULES));
    assert.equal(refs.length, 37);
    // No whole category, no ruleset but PMD's own built-in categories.
    assert.equal((xml.match(/<rule /g) ?? []).length, 37);
  });
});

describe("ESLint", () => {
  test("findings per line from our rules, with the version pinned, and never a message or the code", async () => {
    const result = ran(await lintJavaScript([{ kind: "lwc", component: "invoice", source: JS_WITH_HITS }, { kind: "lwc", component: "clean", source: JS_CLEAN }]));
    assert.equal(result.toolVersion, ESLINT_VERSION);
    assert.equal(result.checked, 2);
    assert.deepEqual(
      result.findings.map((f) => [f.component, f.rule, f.severity, f.line, f.column]),
      [
        ["invoice", "no-unused-vars", 1, 4, 11],
        ["invoice", "no-eval", 2, 5, 5],
        ["invoice", "eqeqeq", 1, 6, 16],
        ["invoice", "no-debugger", 1, 6, 29],
      ],
    );
    assert.deepEqual(Object.keys(result.rules), ["eqeqeq", "no-debugger", "no-eval", "no-unused-vars"]);
    assertNoSource(JSON.stringify(result));
  });

  test("inline configuration changes nothing: eslint-disable, a rule switched on and a global are each ignored, and counted", async () => {
    const result = ran(await lintJavaScript([{ kind: "lwc", component: "sneaky", source: JS_WITH_INLINE_CONFIG }]));
    assert.deepEqual(result.findings.map((f) => f.rule), ["no-eval"]);
    // no-console, switched on in the code, isn't one of ours.
    assert.equal(result.inlineConfigIgnored, 4);
    assert.equal(result.outsideRules, 0);
  });

  test("a planted .eslintrc is never read: ESLint is given our configuration object and nothing from disk", async () => {
    const dir = tempDir();
    writeFileSync(join(dir, ".eslintrc.json"), PLANTED_ESLINTRC);
    writeFileSync(join(dir, "eslint.config.js"), "throw new Error('read');");
    const cwd = process.cwd();
    process.chdir(dir);
    try {
      const result = ran(await lintJavaScript([{ kind: "lwc", component: "invoice", source: JS_WITH_HITS }]));
      assert.ok(result.findings.some((f) => f.rule === "no-eval") && result.findings.some((f) => f.rule === "no-debugger"));
    } finally {
      process.chdir(cwd);
    }
    assert.deepEqual(Object.keys(ESLINT_CONFIG[0]!).sort(), ["languageOptions", "linterOptions", "rules"]);
    assert.deepEqual(Object.keys(ESLINT_CONFIG[0]!.rules), Object.keys(ESLINT_RULES));
  });

  test("a file it can't parse, an enormous file, and a linter that crashes are counted, never fatal", async () => {
    const files: JsFile[] = [
      { kind: "lwc", component: "broken", source: JS_BROKEN },
      { kind: "lwc", component: "enormous", source: `${"// x\n".repeat(50_000)}eval(1);` },
    ];
    const result = ran(await lintJavaScript(files));
    assert.deepEqual(result.skipped, { tooLarge: 1, unparsable: 1, unreadable: 0, failed: 0, timeLimit: 0 });
    const crashing = ran(await lintJavaScript([{ kind: "lwc", component: "a", source: JS_CLEAN }], { linter: { verify: () => { throw new Error(PLANTED_CUSTOMER); } } }));
    assert.equal(crashing.skipped.failed, 1);
    assertNoSource(JSON.stringify(crashing));
  });

  test("the time limit is checked between files, and what's left is counted", async () => {
    let now = 0;
    const result = ran(await lintJavaScript([1, 2, 3].map((n) => ({ kind: "lwc" as const, component: `c${n}`, source: JS_CLEAN })), { timeLimitMs: 10, clock: () => (now += 8) }));
    assert.equal(result.checked, 1);
    assert.equal(result.skipped.timeLimit, 2);
    assert.match(result.limits[0]!, /time limit/);
  });

  test("as configured it can't parse a Lightning web component's decorators: counted as unparsable, as row 670 says", async () => {
    const source = "import { LightningElement, api } from 'lwc';\nexport default class X extends LightningElement {\n  @api recordId;\n}\n";
    assert.equal(ran(await lintJavaScript([{ kind: "lwc", component: "x", source }])).skipped.unparsable, 1);
  });

  test("handed nothing, it says no JavaScript is read, never no findings", async () => {
    assert.equal(notRunReason(await lintJavaScript([])), NO_JAVASCRIPT);
  });
});

/** Just enough of a run's context for the step: reads answered from fixtures. */
function context(
  rows: { classes: Array<{ Name: string; Body?: string }>; triggers: Array<{ Name: string; Body?: string }> },
  codeLint: CodeLintTools | undefined,
  calls: string[] = [],
  fail?: (soql: string) => Error | undefined,
  room: (cost: number) => void = () => {},
): ExtractContext {
  return {
    client: {
      budget: { assertCanSpend: room },
      query: async (soql: string) => {
        calls.push(soql);
        const err = fail?.(soql);
        if (err) throw err;
        return { records: /FROM ApexTrigger/.test(soql) ? rows.triggers : rows.classes, truncated: false };
      },
    },
    ...(codeLint ? { codeLint } : {}),
  } as unknown as ExtractContext;
}

describe("the code-lint step", () => {
  test("without PMD it makes no call, and says PMD and ESLint didn't run, never zero findings", async () => {
    const calls: string[] = [];
    const out = await codeLintExtractor.run(context({ classes: [], triggers: [] }, { pmd: null, pmdMissing: PMD_NOT_INSTALLED }, calls));
    assert.deepEqual(calls, []);
    assert.deepEqual(out, { pmd: { status: "not_run", tool: "pmd", reason: PMD_NOT_INSTALLED }, eslint: { status: "not_run", tool: "eslint", reason: NO_JAVASCRIPT } });
  });

  test("with PMD it reads the bodies itself, lints them, and stores nothing of the code", async () => {
    const { temp, pmd } = setup();
    const calls: string[] = [];
    const out = await codeLintExtractor.run(
      context({ classes: [{ Name: "InvoiceSync", Body: APEX_WITH_HITS }, { Name: "Hidden", Body: "(hidden)" }], triggers: [{ Name: "AccountTrigger", Body: TRIGGER_WITH_HIT }] }, { pmd, pmdOptions: { tempBase: temp } }, calls),
    );
    assert.deepEqual(calls, ["SELECT Id, Name, Body FROM ApexClass WHERE NamespacePrefix = null", "SELECT Id, Name, Body FROM ApexTrigger WHERE NamespacePrefix = null"]);
    const result = ran(out.pmd);
    assert.equal(result.found, 5);
    assert.equal(result.skipped.unreadable, 1);
    assertNoSource(JSON.stringify(out));
    assert.deepEqual(lintTemps(temp), []);
  });

  test("Apex that couldn't be read is a stated limit, or not run when none could, with Salesforce's reason", async () => {
    const { temp, pmd } = setup();
    const refuse = (object: string) => (soql: string): Error | undefined => (soql.includes(`FROM ${object} `) ? new SalesforceError(400, "INVALID_TYPE", `sObject type '${object}' is not supported.`) : undefined);
    const rows = { classes: [{ Name: "InvoiceSync", Body: APEX_WITH_HITS }], triggers: [] };
    const some = ran((await codeLintExtractor.run(context(rows, { pmd, pmdOptions: { tempBase: temp } }, [], refuse("ApexTrigger")))).pmd);
    assert.equal(some.found, 4);
    assert.deepEqual(some.limits, ["Apex triggers couldn't be read, so only classes were checked: ApexTrigger isn't available in this org, or to the connected user."]);
    const none = (soql: string): Error | undefined => refuse("ApexClass")(soql) ?? refuse("ApexTrigger")(soql);
    assert.equal(notRunReason((await codeLintExtractor.run(context(rows, { pmd, pmdOptions: { tempBase: temp } }, [], none))).pmd), "The Apex code couldn't be read: ApexClass isn't available in this org, or to the connected user.");
  });

  test("without clear room in the run's API budget it makes no call and PMD didn't run; the room asked for is the stated rule", async () => {
    const { temp, pmd } = setup();
    const calls: string[] = [];
    const asked: number[] = [];
    const short = (cost: number): void => {
      asked.push(cost);
      throw new BudgetStopError("The org has used 9000 of 10000 daily API calls");
    };
    const out = await codeLintExtractor.run(context({ classes: [{ Name: "A", Body: APEX_CLEAN }], triggers: [] }, { pmd, pmdOptions: { tempBase: temp } }, calls, undefined, short));
    assert.deepEqual(calls, []);
    assert.deepEqual(asked, [LINT_BUDGET_ROOM]);
    assert.equal(LINT_BUDGET_ROOM, 200);
    assert.match(notRunReason(out.pmd), /hadn't clear room for reading the Apex code again, so PMD didn't run/);
  });

  test("a budget stop part way through the reads is this step's not run, never the run's pause", async () => {
    const { temp, pmd } = setup();
    const ctx = context({ classes: [], triggers: [] }, { pmd, pmdOptions: { tempBase: temp } }, [], (soql) => (/ApexTrigger/.test(soql) ? new BudgetStopError("budget") : undefined));
    const out = await codeLintExtractor.run(ctx);
    assert.match(notRunReason(out.pmd), /ran short while the Apex code was read again, so PMD didn't run/);
    assert.deepEqual(lintTemps(temp), []);
  });
});

describe("in a run", () => {
  const options = (outDir: string, extra: Partial<RunOptions> = {}): RunOptions => ({
    provider: syntheticProvider,
    connectedUser: CONNECTED_USER,
    calibration: loadCalibration(),
    modules: [technicalHealthModule],
    outDir,
    now: new Date(AS_OF),
    transport: new Simulator(),
    ...extra,
  });

  test("off, a run has no such step; on, it is a step of its own, version 1, stored in the checkpoints, with nothing of the code in it or in the log", async () => {
    const off = await runHealthCheck(options(tempDir()));
    assert.ok(!off.run.steps.some((s) => s.id === "code-lint"));
    const { temp, pmd } = setup();
    const lines: string[] = [];
    const on = await runHealthCheck(options(tempDir(), { codeLint: { pmd, pmdOptions: { tempBase: temp } }, log: (l) => lines.push(l) }));
    const step = on.run.steps.find((s) => s.id === "code-lint")!;
    assert.deepEqual([step.status, step.version], ["done", 1]);
    const stored = readFileSync(join(on.dir, "checkpoints", "code-lint.json"), "utf8");
    const data = JSON.parse(stored) as CodeLintData;
    assert.equal(data.pmd.status, "ran");
    assert.equal(data.eslint.status, "not_run");
    // The synthetic org's code reached the stand-in, and none of it is kept.
    const seen = JSON.parse(readFileSync(join(pmd.home, "seen.json"), "utf8")) as { files: Record<string, string> };
    const bodies = Object.values(seen.files);
    assert.ok(bodies.length > 5);
    for (const body of bodies) for (const line of body.split("\n").map((l) => l.trim()).filter((l) => l.length > 12)) {
      assert.ok(!stored.includes(line), `a line of code is stored: ${line}`);
      assert.ok(!lines.join("\n").includes(line), `a line of code is logged: ${line}`);
    }
    // Only stores: what a customer sees is the same with it off.
    for (const file of ["findings.json", "metrics.json", "scores.json"]) assert.equal(readFileSync(join(on.dir, file), "utf8").replace(/hc-\d{4}-\d{2}-\d{2}-[0-9a-f]{8}/g, ""), readFileSync(join(off.dir, file), "utf8").replace(/hc-\d{4}-\d{2}-\d{2}-[0-9a-f]{8}/g, ""));
    assert.deepEqual(lintTemps(temp), []);
  });

  test("a step that throws costs only itself: the run carries on and finishes", async () => {
    const throwing: CodeLintTools = { pmd: null, eslintOptions: { linter: { verify: () => [] } } };
    Object.defineProperty(throwing, "pmdMissing", { get: () => { throw new Error("unexpected"); } });
    const result = await runHealthCheck(options(tempDir(), { codeLint: throwing, log: () => {} }));
    assert.equal(result.run.steps.find((s) => s.id === "code-lint")!.status, "failed");
    assert.notEqual(result.run.status, "failed");
    assert.ok(result.output);
  });

  test("without PMD on the worker: not run, PMD isn't installed, and no call is spent", async () => {
    const result = await runHealthCheck(options(tempDir(), { codeLint: { pmd: null, pmdMissing: PMD_NOT_INSTALLED }, log: () => {} }));
    const step = result.run.steps.find((s) => s.id === "code-lint")!;
    assert.deepEqual([step.status, step.calls], ["done", 0]);
    const data = JSON.parse(readFileSync(join(result.dir, "checkpoints", "code-lint.json"), "utf8")) as CodeLintData;
    assert.deepEqual(data.pmd, { status: "not_run", tool: "pmd", reason: PMD_NOT_INSTALLED });
    assert.ok(existsSync(join(result.dir, "findings.json")));
  });
});

describe("PMD: what's read from its report is bounded", () => {
  test("a line outside the file, a column below 1 or not a number, a priority PMD hasn't, a rule not text: each dropped, and counted", async () => {
    const { temp, pmd } = setup();
    const result = ran(await runPmd([apex("Odd", "public class Odd { // FAKE_PMD_MALFORMED\n}")], pmd, { tempBase: temp }));
    assert.equal(result.malformed, 6);
    // The one that was only wrong in its category is kept, with no category.
    assert.deepEqual(result.findings.map((f) => [f.rule, f.line, f.column, f.severity, f.category]), [["EmptyCatchBlock", 1, 1, 3, null]]);
  });
});

/** Whether a process is still alive. */
const alive = (pid: number): boolean => {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
};
const until = async (check: () => boolean, ms: number): Promise<boolean> => {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    if (check()) return true;
    await new Promise((r) => setTimeout(r, 100));
  }
  return check();
};
const CHILD = join(import.meta.dirname, "lint-fixtures", "pmd-child.ts");

/** Starts a process running PMD's stand-in, which hangs, and resolves once PMD has its code. */
function child(mode: "exit" | "wait" | "handled"): Promise<{ proc: ChildProcess; root: string; out: string[]; exited: Promise<[number | null, NodeJS.Signals | null]> }> {
  const root = tempDir();
  const proc = spawn(process.execPath, [CHILD, root, mode], { stdio: ["ignore", "pipe", "inherit"] });
  const out: string[] = [];
  const exited = new Promise<[number | null, NodeJS.Signals | null]>((resolve) => proc.on("exit", (code, signal) => resolve([code, signal])));
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("the child never got PMD going")), 15_000);
    proc.stdout!.on("data", (chunk: Buffer) => {
      out.push(...chunk.toString().split("\n").filter(Boolean));
      if (out.includes("ready")) {
        clearTimeout(timer);
        resolve({ proc, root, out, exited });
      }
    });
  });
}
const pmdPid = (root: string): number => (JSON.parse(readFileSync(join(root, "pmd-bin-7.29.0", "seen.json"), "utf8")) as { pid: number }).pid;

describe("PMD never outlives the process that started it, nor leaves its code behind", { timeout: 30_000 }, () => {
  test("process.exit(75) mid-run, as the recycle's watchdog calls it: no folder left and no PMD alive", async () => {
    const { root, exited } = await child("exit");
    assert.deepEqual(await exited, [75, null]);
    const pid = pmdPid(root);
    assert.ok(await until(() => !alive(pid), 3000), "PMD is still running");
    assert.deepEqual(lintTemps(join(root, "tmp")), []);
  });

  test("SIGTERM with nothing else listening: PMD and its folder go, and the process still ends by SIGTERM", async () => {
    const { proc, root, exited } = await child("wait");
    proc.kill("SIGTERM");
    assert.deepEqual(await exited, [null, "SIGTERM"]);
    assert.ok(await until(() => !alive(pmdPid(root)), 3000));
    assert.deepEqual(lintTemps(join(root, "tmp")), []);
  });

  test("SIGTERM with the worker's own handler: PMD and its folder go, and the handler runs and sets the exit as before", async () => {
    const { proc, root, out, exited } = await child("handled");
    proc.kill("SIGTERM");
    assert.deepEqual(await exited, [0, null]);
    assert.ok(out.includes("own handler ran"));
    assert.ok(await until(() => !alive(pmdPid(root)), 3000));
    assert.deepEqual(lintTemps(join(root, "tmp")), []);
  });

  test("SIGKILL, when no hook can run: PMD's own shell sees its parent gone, kills PMD and deletes the folder", async () => {
    const { proc, root, exited } = await child("wait");
    proc.kill("SIGKILL");
    await exited;
    const pid = pmdPid(root);
    assert.ok(await until(() => !alive(pid), 8000), "an orphaned PMD is still running");
    assert.ok(await until(() => lintTemps(join(root, "tmp")).length === 0, 3000), "an orphaned PMD's folder is still there");
  });

  test("the shell's own limit stops PMD whether or not anyone is watching", async () => {
    const dir = tempDir();
    const started = Date.now();
    const proc = spawn("/bin/sh", ["-c", PMD_WRAPPER, "pmd-wrapper", dir, "1", "/bin/sleep", "30"], { stdio: "ignore" });
    const [code] = await new Promise<[number | null]>((resolve) => proc.on("exit", (c) => resolve([c])));
    assert.equal(code, 137);
    assert.ok(Date.now() - started < 5000);
  });

  test("its hooks are there only while PMD runs", async () => {
    const before = [process.listenerCount("exit"), process.listenerCount("SIGTERM"), process.listenerCount("SIGINT")];
    const { temp, pmd } = setup();
    const running = runPmd([apex("Hang", "public class Hang { // FAKE_PMD_HANG\n}")], pmd, { tempBase: temp, timeLimitMs: 1500 });
    await until(() => livePmdRuns() === 1, 3000);
    assert.deepEqual([process.listenerCount("exit"), process.listenerCount("SIGTERM"), process.listenerCount("SIGINT")], before.map((n) => n + 1));
    await running;
    assert.equal(livePmdRuns(), 0);
    assert.deepEqual([process.listenerCount("exit"), process.listenerCount("SIGTERM"), process.listenerCount("SIGINT")], before);
  });

  test("the sweep never follows a link, and leaves anything but a folder alone", () => {
    const root = tempDir();
    const base = join(root, "base");
    const outside = join(root, "outside");
    mkdirSync(base);
    mkdirSync(outside);
    writeFileSync(join(outside, "precious"), "x");
    writeFileSync(join(base, `${LINT_TEMP_PREFIX}file`), "x");
    const old = new Date(Date.now() - 3_600_000);
    utimesSync(outside, old, old);
    utimesSync(join(base, `${LINT_TEMP_PREFIX}file`), old, old);
    symlinkSync(outside, join(base, `${LINT_TEMP_PREFIX}link`));
    assert.equal(sweepLintTemp(base, 600_000), 0);
    assert.ok(existsSync(join(outside, "precious")));
    assert.deepEqual(readdirSync(base).sort(), [`${LINT_TEMP_PREFIX}file`, `${LINT_TEMP_PREFIX}link`]);
  });
});
