import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { ALL_MODULES } from "../../domains/index.ts";
import { overallScore } from "../../domains/scoring.ts";
import { metadataExtractor } from "../../extract/automation.ts";
import { orgDocumentationExtractor, type OrgDocumentation } from "../../extract/documentation.ts";
import { Complexity } from "../../schemas/complexity.ts";
import type { Finding, Score } from "../../schemas/index.ts";
import { fingerprint } from "../../testing/fingerprint.ts";
import { loadExtracts, runFromRecording } from "../../testing/harness.ts";
import { loadAnswers } from "../../testing/replay.ts";
import { bandOf, COMPONENTS, countPoints, MIN_COMPONENTS_READ, MIN_WEIGHT_READ, scoreComplexity, type ComponentInput } from "../formula.ts";
import { buildComplexity, COMPLEXITY_FILE, complexityInputs } from "../read.ts";

// The complexity score (DECISIONS.md 614): a published formula over counts the
// engine already holds, beside the health score and never part of it.

const MADE_AT = new Date("2026-10-31T12:00:00.000Z");
type Measured = Extract<Complexity, { status: "measured" }>;
type MeasuredComponent = Extract<Complexity["components"][number], { status: "measured" }>;

/** Every component read, each count at `count(top)`. */
const inputsAt = (count: (top: number) => number): Record<string, ComponentInput> =>
  Object.fromEntries(COMPONENTS.map((c) => [c.id, { counts: Object.fromEntries(c.counts.map((x) => [x.id, count(x.top)])) }]));
const measured = (c: Complexity): Measured => {
  assert.equal(c.status, "measured");
  return c as Measured;
};
const componentOf = (c: Complexity, id: string): MeasuredComponent => {
  const found = c.components.find((x) => x.id === id);
  assert.equal(found?.status, "measured", id);
  return found as MeasuredComponent;
};

describe("the formula as published", () => {
  test("seven components whose weights sum to 100, each the sum of its counts' points", () => {
    assert.equal(COMPONENTS.length, 7);
    assert.equal(COMPONENTS.reduce((s, c) => s + c.weight, 0), 100);
    for (const c of COMPONENTS) assert.equal(c.counts.reduce((s, x) => s + x.points, 0), c.weight, c.id);
    assert.deepEqual(
      COMPONENTS.map((c) => [c.id, c.weight]),
      [["dataModel", 20], ["declarativeAutomation", 15], ["legacyAutomation", 10], ["code", 20], ["automationOverlap", 10], ["integrations", 15], ["accessModel", 10]],
    );
    assert.equal(MIN_COMPONENTS_READ, 5);
    assert.equal(MIN_WEIGHT_READ, 70);
  });

  test("the bands' edges", () => {
    assert.deepEqual([0, 24, 25, 49, 50, 74, 75, 100].map(bandOf), ["Simple", "Simple", "Moderate", "Moderate", "Complex", "Complex", "Very complex", "Very complex"]);
  });

  test("an org with nothing custom scores 0 and Simple", () => {
    const c = measured(scoreComplexity(inputsAt(() => 0), MADE_AT));
    assert.equal(c.score, 0);
    assert.equal(c.band, "Simple");
    assert.equal(c.weightRead, 100);
    assert.ok(c.components.every((x) => x.status === "measured" && x.points === 0 && x.shareOfScore === 0));
  });

  test("saturation: ten times a count's top earns what the top earns, and no more than its points", () => {
    for (const def of COMPONENTS) {
      for (const x of def.counts) {
        assert.equal(countPoints(x.top * 10, x.top, x.points), countPoints(x.top, x.top, x.points), x.id);
        assert.equal(countPoints(x.top * 10, x.top, x.points), x.points, x.id);
      }
    }
    const atTop = measured(scoreComplexity(inputsAt((top) => top), MADE_AT));
    const tenTimes = measured(scoreComplexity(inputsAt((top) => top * 10), MADE_AT));
    assert.equal(atTop.score, 100);
    assert.deepEqual(tenTimes.components.map((x) => x.status === "measured" && x.counts.map((y) => y.points)), atTop.components.map((x) => x.status === "measured" && x.counts.map((y) => y.points)));
    assert.equal(tenTimes.score, 100);
    // One enormous count on its own moves the score by its own points and no further.
    const fields = inputsAt(() => 0);
    fields.dataModel = { counts: { customObjects: 0, customFields: 1_000_000, recordTypes: 0 } };
    const one = measured(scoreComplexity(fields, MADE_AT));
    assert.equal(one.score, 8);
    assert.equal(one.band, "Simple");
  });

  test("halfway up every scale is half of every weight", () => {
    const c = measured(scoreComplexity(inputsAt((top) => top / 2), MADE_AT));
    assert.equal(c.score, 50);
    assert.equal(c.band, "Complex");
    for (const def of COMPONENTS) assert.equal(componentOf(c, def.id).points, def.weight / 2, def.id);
  });
});

describe("a component that couldn't be read", () => {
  const without = (...ids: string[]): Record<string, ComponentInput> => {
    const inputs = inputsAt((top) => top / 2);
    for (const id of ids) inputs[id] = { reason: `Planted: ${id} couldn't be read.` };
    return inputs;
  };

  test("with six components and 80 points read, the score is from those, and names the one left out", () => {
    const c = measured(scoreComplexity(without("code"), MADE_AT));
    assert.deepEqual(c.leftOut, ["code"]);
    assert.equal(c.weightRead, 80);
    // 40 points earned of the 80 read.
    assert.equal(c.score, 50);
    const left = c.components.find((x) => x.id === "code")!;
    assert.equal(left.status, "not_read");
    assert.ok(left.status === "not_read" && /Planted: code/.test(left.reason));
  });

  test("with five components but less than 70 points, it's not measurable and names each left out", () => {
    // 40 of the 100 left out, so 60 read.
    const c = scoreComplexity(without("dataModel", "code"), MADE_AT);
    assert.equal(c.status, "not_measurable");
    assert.ok(c.status === "not_measurable" && /Data model \(Planted: dataModel/.test(c.reason) && /Code \(Planted: code/.test(c.reason));
    assert.deepEqual(c.leftOut, ["dataModel", "code"]);
    assert.ok(!("score" in c));
  });

  test("with four components, even of 70 points or more, it's not measurable", () => {
    const c = scoreComplexity(without("legacyAutomation", "automationOverlap", "accessModel"), MADE_AT);
    assert.equal(c.status, "not_measurable");
    assert.ok(c.status === "not_measurable" && /Legacy automation/.test(c.reason) && /Automation overlap on objects/.test(c.reason) && /Access model/.test(c.reason));
  });

  test("with five components and 70 points exactly, it's scored from those", () => {
    const c = measured(scoreComplexity(without("legacyAutomation", "integrations"), MADE_AT));
    assert.equal(c.weightRead, 75);
    const d = measured(scoreComplexity(without("legacyAutomation", "automationOverlap"), MADE_AT));
    assert.equal(d.weightRead, 80);
    const e = measured(scoreComplexity(without("integrations", "declarativeAutomation"), MADE_AT));
    assert.equal(e.weightRead, 70);
    assert.equal(e.score, 50);
    assert.deepEqual(e.leftOut, ["declarativeAutomation", "integrations"]);
  });
});

describe("the synthetic org", () => {
  const recorded = runFromRecording({ answers: loadAnswers(), modules: ALL_MODULES });

  test("scores what the formula gives by hand, with each component's working", async () => {
    const { dir } = await recorded;
    const c = measured(Complexity.parse(JSON.parse(readFileSync(join(dir, COMPLEXITY_FILE), "utf8"))));
    // Counts from the synthetic org (src/testing/synthetic-org.ts), points by
    // hand as points × count ÷ top to two places. The Apex is traced in full:
    // four non-test classes of 1,200, 800, 400 and 350 characters, and five
    // triggers of 180, 220, 150, 260 and 140: 3,700.
    const hand: Record<string, Array<[string, number, number]>> = {
      dataModel: [["customObjects", 4, 0.32], ["customFields", 8, 0.04], ["recordTypes", 3, 0.24]],
      declarativeAutomation: [["recordTriggeredFlows", 3, 0.18], ["otherFlows", 1, 0.04], ["validationRules", 3, 0.08]],
      legacyAutomation: [["workflowRules", 3, 0.15], ["processBuilders", 1, 0.1]],
      code: [["apexClasses", 4, 0.06], ["apexTriggers", 5, 0.4], ["apexCharacters", 3700, 0.01]],
      automationOverlap: [["objectsWithSeveralKinds", 3, 1.5]],
      integrations: [["connectedApps", 2, 0.33], ["endpoints", 2, 0.33], ["packages", 3, 0.5]],
      accessModel: [["customProfiles", 3, 0.24], ["customPermissionSets", 5, 0.13], ["roles", 4, 0.04]],
    };
    const componentPoints = { dataModel: 0.6, declarativeAutomation: 0.3, legacyAutomation: 0.25, code: 0.47, automationOverlap: 1.5, integrations: 1.16, accessModel: 0.41 };
    for (const [id, counts] of Object.entries(hand)) {
      const comp = componentOf(c, id);
      assert.deepEqual(comp.counts.map((x) => [x.id, x.count, x.points]), counts, id);
      assert.equal(comp.points, componentPoints[id as keyof typeof componentPoints], id);
    }
    // 0.6 + 0.3 + 0.25 + 0.47 + 1.5 + 1.16 + 0.41 = 4.69 of 100, which rounds to 5.
    assert.equal(c.weightRead, 100);
    assert.equal(c.score, 5);
    assert.equal(c.band, "Simple");
    assert.deepEqual(c.leftOut, []);
    // Each share is its points of the 4.69 earned: 1.5 ÷ 4.69 is 32%.
    assert.equal(componentOf(c, "automationOverlap").shareOfScore, 32);
    assert.equal(componentOf(c, "integrations").shareOfScore, 25);
    assert.equal(c.madeAt, "2026-10-31T12:00:00.000Z");
  });

  test("is the same on a set-up only scan of the org as on a complete run", async () => {
    const { dir } = await recorded;
    const setUp = await runFromRecording({ modules: ALL_MODULES, scope: "essentials" });
    assert.equal(setUp.run.scope, "essentials");
    // Nothing that reads records ran, and the score is the same, byte for byte.
    assert.ok(!readdirSync(join(setUp.dir, "checkpoints")).some((f) => f.startsWith("field-usage") || f.startsWith("opportunities.")));
    assert.equal(readFileSync(join(setUp.dir, COMPLEXITY_FILE), "utf8"), readFileSync(join(dir, COMPLEXITY_FILE), "utf8"));
  });

  test("leaves the Barometer score, every area score and every other output as they were", async () => {
    const { dir } = await recorded;
    // The fingerprint test's golden hashes from before this lane (src/testing/__tests__/fingerprint.test.ts).
    const print = fingerprint(dir);
    assert.equal(print["scores.json"], "17cdb1708449d000");
    // Moved since by decision 628's hours and prompts, then 655's org-word marks, alone, which the fingerprint test holds.
    assert.equal(print["findings.json"], "e6ed3ad89d5e6450");
    assert.equal(print["metrics.json"], "10d5f5733555b541");
    assert.equal(print["raw-notes.md"], "5e5e3581e490bb64");
    const scores = JSON.parse(readFileSync(join(dir, "scores.json"), "utf8")) as Score[];
    const findings = JSON.parse(readFileSync(join(dir, "findings.json"), "utf8")) as Finding[];
    const overall = overallScore(scores, findings);
    assert.equal(overall.status, "measured");
    // A wildly different complexity changes none of it: nothing that scores reads the file.
    const extracts = loadExtracts(dir);
    assert.notDeepEqual(buildComplexity(new Map(), MADE_AT), buildComplexity(extracts, MADE_AT));
  });

  test("no module that scores, finds, notes or prints imports the complexity score", () => {
    const root = join(import.meta.dirname, "..", "..");
    const users: string[] = [];
    const walk = (folder: string): void => {
      for (const entry of readdirSync(folder, { withFileTypes: true })) {
        const path = join(folder, entry.name);
        if (entry.isDirectory()) {
          if (entry.name !== "__tests__" && entry.name !== "complexity") walk(path);
        } else if (entry.name.endsWith(".ts") && /complexity\/|schemas\/complexity/.test(readFileSync(path, "utf8"))) users.push(path.slice(root.length + 1));
      }
    };
    walk(root);
    assert.deepEqual(users.sort(), ["engine/run.ts", "worker/postgres-store.ts", "worker/run-job.ts", "worker/types.ts"]);
  });
});

describe("the counts from the extracts", () => {
  test("metadata that couldn't be read leaves four components out, and the score is not measurable, naming them", async () => {
    const { dir } = await runFromRecording({ modules: ALL_MODULES });
    const extracts = loadExtracts(dir);
    extracts.set(metadataExtractor.id, { status: "unavailable", reason: "Planted refusal", source: { kind: "metadata", path: "x" } });
    const c = buildComplexity(extracts, MADE_AT);
    assert.equal(c.status, "not_measurable");
    assert.deepEqual(c.leftOut, ["dataModel", "declarativeAutomation", "legacyAutomation", "automationOverlap"]);
    assert.ok(c.status === "not_measurable" && /Data model \(The org's metadata couldn't be read: Planted refusal\)/.test(c.reason));
  });

  test("connected apps that couldn't be read leave one component out, and the rest are scored", async () => {
    const { dir } = await runFromRecording({ modules: ALL_MODULES });
    const extracts = loadExtracts(dir);
    const doc = extracts.get(orgDocumentationExtractor.id) as OrgDocumentation;
    extracts.set(orgDocumentationExtractor.id, { ...doc, connectedApps: { status: "unavailable", reason: "Planted refusal", source: doc.connectedApps.source } });
    const c = measured(buildComplexity(extracts, MADE_AT));
    assert.deepEqual(c.leftOut, ["integrations"]);
    assert.equal(c.weightRead, 85);
    // 4.69 earned less the 1.16 of integrations: 3.53 of 85 is 4.15, which rounds to 4.
    assert.equal(c.score, 4);
  });

  test("a list Salesforce cut short isn't counted as the whole", async () => {
    const { dir } = await runFromRecording({ modules: ALL_MODULES });
    const extracts = loadExtracts(dir);
    const doc = extracts.get(orgDocumentationExtractor.id) as OrgDocumentation;
    extracts.set(orgDocumentationExtractor.id, { ...doc, roles: { ...doc.roles, truncated: true } });
    const inputs = complexityInputs(extracts);
    assert.ok("reason" in inputs.accessModel! && /only part of the roles/.test(inputs.accessModel.reason));
  });

  test("a step this run didn't make says so", () => {
    const inputs = complexityInputs(new Map());
    for (const id of COMPONENTS.map((c) => c.id)) assert.ok("reason" in inputs[id]! && /^This run didn't read /.test(inputs[id].reason), id);
  });
});
