import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { ALL_MODULES, SALES_CLOUD_MODULES } from "../../domains/index.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { FACTS_FILE, type Facts } from "../../extract/facts.ts";
import { loadRun } from "../../render/render-run.ts";
import { buildReport } from "../../narrative/sections.ts";
import type { Finding } from "../../schemas/index.ts";
import { tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator, type SimulatorOptions } from "../../testing/simulator.ts";
import { AS_OF, ORG_ID, USERS } from "../../testing/synthetic-org.ts";
import { ASSUMPTIONS } from "../assumptions.ts";
import { answerQuestions, parseQuestions, questionsReport, SECTION_IDS } from "../questions.ts";
import { runFacts } from "../run-facts.ts";
import { FactsFile, factsReport } from "../summary.ts";

// The facts step (DECISIONS.md 287 to 295).

const QUESTIONS = readFileSync(join(import.meta.dirname, "..", "..", "..", "docs", "QUESTIONS.md"), "utf8");

async function run(options: SimulatorOptions = {}, scope?: "essentials") {
  const result = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: ALL_MODULES, outDir: tempDir(), now: new Date(AS_OF), answers: loadAnswers(), transport: new Simulator(options), ...(scope ? { scope } : {}) });
  const text = readFileSync(join(result.dir, FACTS_FILE), "utf8");
  return { ...result, text, facts: FactsFile.parse(JSON.parse(text)) as Facts };
}
const fact = (facts: Facts, id: string) => facts.facts.find((f) => f.id === id)!;

const clean = await run();

describe("a run's facts step", () => {
  test("checks every assumption, and on the synthetic org each is as we expect", () => {
    assert.deepEqual(clean.facts.facts.map((f) => f.id), ASSUMPTIONS.map((a) => a.id));
    assert.deepEqual(clean.facts.facts.filter((f) => f.outcome !== "confirmed").map((f) => f.id), []);
  });

  test("keeps names, booleans and field types only: no record, value, person or org id", () => {
    for (const f of clean.facts.facts) for (const value of Object.values(f.observed)) assert.ok(value === null || typeof value === "boolean" || /^[a-z]+$/.test(value), `${f.id}: ${value}`);
    assert.ok(!clean.text.includes(ORG_ID.slice(0, 15)));
    for (const u of USERS) assert.ok(!clean.text.includes(u.name), u.name);
    assert.ok(!/@/.test(clean.text));
  });

  test("is cheap: one describe-global, the objects whose fields we assume, and one query", () => {
    assert.ok(clean.facts.calls <= 10, `${clean.facts.calls} calls`);
    assert.equal(clean.run.steps.at(-1)!.id, "facts");
  });

  test("runs in a set-up review too, which reads no records", async () => {
    const essentials = await run({}, "essentials");
    assert.equal(essentials.facts.facts.length, ASSUMPTIONS.length);
  });

  test("puts one line in the raw notes' assumptions, and its figures pass the number check", () => {
    assert.match(readFileSync(join(clean.dir, "raw-notes.md"), "utf8"), new RegExp(`- We checked ${ASSUMPTIONS.length} things about how this org is built, and each was as we expected\\.`));
    const { input } = loadRun(clean.dir);
    const report = buildReport(input);
    const assumptions = report.outputs.raw_notes.sections.find((s) => s.key === "assumptions")!;
    const version = report.outputs.raw_notes.versions.find((v) => v.id === assumptions.currentVersionId)!;
    assert.match(version.body, /We checked \d+ things about how this org is built/);
    assert.deepEqual(version.numberCheck.unmatched, []);
  });
});

// Every fact wrong at once, but for the licence rows, which have their own org below.
const wrong = await run({
  missingObjects: ["Scontrol", "PackageLicense", "AuthProvider", "ConnectedApplication", "NamedCredential"],
  dropFields: {
    SBQQ__Quote__c: ["SBQQ__Primary__c"],
    SBQQ__PriceRule__c: ["SBQQ__Active__c"],
    SBQQ__PriceAction__c: ["SBQQ__Field__c"],
    SBQQ__ProductRule__c: ["SBQQ__Active__c"],
    Contract: ["SBQQ__RenewalOpportunity__c"],
    Lead: ["pi__grade__c"],
    PermissionSet: ["PermissionsApiUserOnly", "PermissionsViewAllData"],
    User: ["UserPreferencesLightningExperiencePreferred"],
  },
  asyncApex: null,
});

// Checks that still say something true without the fact, and say what they couldn't read.
const STATES_THE_GAP: Record<string, (f: Finding) => boolean> = {
  "tech.classic-leftovers": (f) => /S-controls couldn't be read/.test(f.summary) && !("scontrols" in (f.facts ?? {})),
  // The run-as scan is incomplete, so nobody is recommended for release (hard rule 3).
  // And says so, so the raw notes' "the check that rests on it says so" is true (night 3 review).
  "utl.dormant-licensed-users": (f) => f.recommendationType !== "deactivate" && (f.status === "not_measurable" || /couldn't complete every check/.test(f.summary)),
};

describe("where a fact contradicts what we assume", () => {
  const contradicted = ASSUMPTIONS.filter((a) => a.kind !== "licence" && a.kind !== "settings" && !(("ifMissing" in a) && a.ifMissing === "open"));

  test("the facts step says so", () => {
    for (const a of contradicted) assert.equal(fact(wrong.facts, a.id).outcome, "contradicted", a.id);
  });

  test("every check that rests on it is not measurable, with the reason, or says what it couldn't read: never a wrong figure", () => {
    for (const a of contradicted) {
      for (const ruleId of a.affects) {
        const findings = wrong.output!.findings.filter((f) => f.ruleId === ruleId);
        assert.ok(findings.length > 0, ruleId);
        for (const f of findings) {
          if (STATES_THE_GAP[ruleId]) assert.ok(STATES_THE_GAP[ruleId](f), `${ruleId}: ${f.summary}`);
          else assert.equal(f.status, "not_measurable", `${a.id} → ${ruleId}: ${f.status}`);
        }
      }
    }
  });

  test("the raw notes count what differed and name the checks that say so", () => {
    const notes = readFileSync(join(wrong.dir, "raw-notes.md"), "utf8");
    const differed = wrong.facts.facts.filter((f) => f.outcome === "contradicted").length;
    assert.match(notes, new RegExp(`; ${differed} differed from what we expected, and those checks say so\\.`));
    assert.match(notes, /Scontrol isn't among the objects the connection can query\. The check that rests on it says so: "Parts of the org still depend on Salesforce Classic"\./);
  });

  test("the console's summary counts each outcome and names what differed and the checks resting on it, with nothing seen (DECISIONS.md 324)", () => {
    const report = factsReport(FactsFile.parse(JSON.parse(wrong.text)));
    const count = (o: string) => wrong.facts.facts.filter((f) => f.outcome === o).length;
    assert.deepEqual(report.counts, { confirmed: count("confirmed"), contradicted: count("contradicted"), open: count("open"), notChecked: count("not_checked") });
    assert.deepEqual(report.differed, wrong.facts.facts.filter((f) => f.outcome === "contradicted").map((f) => ({ id: f.id, question: f.question, affects: f.affects })));
    assert.ok(report.differed.length > 0);
    // Names only: every string in it is an id, apart from when it was checked.
    const strings: string[] = [];
    JSON.stringify({ ...report, checkedAt: undefined }, (_k, v: unknown) => (typeof v === "string" && strings.push(v), v));
    for (const s of strings) assert.match(s, /^[a-z0-9][a-z0-9._-]*$/, s);
    assert.equal(report.checkedAt, wrong.facts.checkedAt);
    assert.deepEqual(factsReport(FactsFile.parse(JSON.parse(clean.text))).differed, []);
  });

  test("a package's names with no licence row is a contradiction that nothing rests on", async () => {
    const unlicensed = await run({ unlicensed: ["SBQQ", "pi"] });
    for (const id of ["cpq.licence", "ae.licence"]) assert.equal(fact(unlicensed.facts, id).outcome, "contradicted", id);
    assert.ok(unlicensed.output!.findings.some((f) => f.ruleId === "cpq.discount-leakage" && f.status === "issue"));
    assert.ok(unlicensed.output!.findings.some((f) => f.ruleId === "mkt.unworked-handovers" && f.status === "issue"));
  });

  test("without a package, its facts aren't checked, and a feature's object that isn't there is only open", async () => {
    const bare = await run({ cpq: false, pardot: false, missingObjects: ["BotDefinition"] });
    for (const id of ["cpq.quote-fields", "cpq.licence", "ae.lead-fields", "ae.licence"]) assert.equal(fact(bare.facts, id).outcome, "not_checked", id);
    assert.equal(fact(bare.facts, "object.bot-definition").outcome, "open");
  });
});

describe("QUESTIONS.md ids", () => {
  const items = parseQuestions(QUESTIONS);

  test("every section has a short name, and every id is unique", () => {
    for (const line of QUESTIONS.split("\n").filter((l) => l.startsWith("## "))) assert.ok(line.replace(/^##\s+/, "").replace(/\s*\(.*\)\s*$/, "").trim() in SECTION_IDS, line);
    assert.equal(new Set(items.map((i) => i.id)).size, items.length);
  });

  test("every assumption answers an item that exists", () => {
    for (const a of ASSUMPTIONS) assert.ok(items.some((i) => i.id === a.question), `${a.id} → ${a.question}`);
  });
});

describe("npm run questions", () => {
  const runs = [{ name: "clean", facts: clean.facts }, { name: "wrong", facts: wrong.facts }];
  const answers = answerQuestions(parseQuestions(QUESTIONS), runs);
  const status = (id: string) => answers.find((a) => a.item.id === id)!.status;

  test("contradicted where any run contradicts, settled where every fact is confirmed, and the rest open or not a fact", () => {
    assert.equal(status("integrations.i"), "contradicted");
    assert.equal(status("pay.49"), "contradicted");
    assert.equal(status("cpq.d"), "settled");
    assert.equal(status("ai.a"), "settled");
    assert.equal(status("ae.b"), "contradicted");
    assert.equal(status("coby.1"), "not_a_fact");
  });

  test("an item whose facts cover only part of it is partly settled", () => {
    const one = answerQuestions(parseQuestions(QUESTIONS), [{ name: "clean", facts: clean.facts }]);
    assert.equal(one.find((a) => a.item.id === "cpq.a")!.status, "partly_settled");
  });

  test("the report groups items by status, and says what was seen", () => {
    const report = questionsReport(answers, runs);
    assert.match(report, /## Contradicted: update QUESTIONS\.md, and the check that assumed it \(\d+\)/);
    assert.match(report, /- \*\*integrations\.i\*\* .*\n {2}Contradicted in 1 of 2 runs: Scontrol isn't among the objects the connection can query\./);
    assert.match(report, /## Not something the facts step answers \(\d+\)/);
  });
});

describe("hc facts", () => {
  test("checks the facts alone, writing a folder that npm run questions reads, without the settings it would need a retrieve for", async () => {
    const out = tempDir();
    const { dir, facts } = await runFacts({ provider: syntheticProvider, outDir: out, transport: new Simulator(), now: new Date(AS_OF) });
    assert.ok(dir.startsWith(out));
    assert.equal(FactsFile.parse(JSON.parse(readFileSync(join(dir, FACTS_FILE), "utf8"))).facts.length, ASSUMPTIONS.length);
    assert.equal(fact(facts, "settings.ai").outcome, "not_checked");
    assert.equal(fact(facts, "object.scontrol").outcome, "confirmed");
  });

  test("won't write inside a git repository", async () => {
    await assert.rejects(runFacts({ provider: syntheticProvider, outDir: import.meta.dirname, transport: new Simulator() }), /inside the git repository/);
  });
});

test("the Sales Cloud modules alone still check the facts a set-up retrieve gives", async () => {
  const { dir } = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(AS_OF), answers: loadAnswers(), transport: new Simulator() });
  const facts = FactsFile.parse(JSON.parse(readFileSync(join(dir, FACTS_FILE), "utf8")));
  assert.equal(facts.facts.find((f) => f.id === "settings.ai")!.outcome, "confirmed");
  assert.equal(facts.facts.find((f) => f.id === "settings.campaign")!.outcome, "not_checked");
});
