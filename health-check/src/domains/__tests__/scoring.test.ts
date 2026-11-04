import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { CONFIDENCE_FACTORS, MIN_RULES_SCORED, RULE_MAXIMUM, SEVERITY_WEIGHTS, scoreDomain, scoreDomains } from "../scoring.ts";
import { SALES_CLOUD_MODULES } from "../index.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import { Score, SALES_CLOUD_DOMAINS, type Finding } from "../../schemas/index.ts";

let counter = 0;
const finding = (ruleId: string, status: Finding["status"], severity: Finding["severity"] = "medium", confidence: Finding["confidence"] = "high", domain = "d"): Finding =>
  ({ id: `f-${++counter}`, ruleId, domain, status, severity, confidence }) as Finding;

describe("the arithmetic", () => {
  test("all passes score 100", () => {
    const score = scoreDomain("d", [finding("a", "passed"), finding("b", "passed")]);
    assert.ok(score.status === "measured" && score.score === 100);
  });

  test("only critical issues score 0", () => {
    const score = scoreDomain("d", [finding("a", "issue", "critical"), finding("b", "issue", "critical")]);
    assert.ok(score.status === "measured" && score.score === 0);
  });

  test("only high issues score 38: each earns 3 of 8, which is 37.5%", () => {
    const score = scoreDomain("d", [finding("a", "issue", "high"), finding("b", "issue", "high")]);
    assert.ok(score.status === "measured" && score.score === 38);
  });

  test("worked example: one pass, one high and one medium-confidence medium", () => {
    // Pass 8. High 8 - 5 = 3. Medium 8 - 3 x 0.75 = 5.75. 16.75 of 24 is 69.8%.
    const score = scoreDomain("d", [finding("a", "passed"), finding("b", "issue", "high"), finding("c", "issue", "medium", "medium")]);
    assert.ok(score.status === "measured" && score.score === 70);
  });

  test("with no passes, severity still decides the score", () => {
    const at = (severity: Finding["severity"]): number => {
      const score = scoreDomain("d", Array.from({ length: 5 }, (_, i) => finding(`r${i}`, "issue", severity)));
      assert.ok(score.status === "measured");
      return score.score;
    };
    assert.ok(at("low") > at("medium") && at("medium") > at("high") && at("high") > at("critical"));
    assert.equal(at("low"), 88);
    assert.equal(at("critical"), 0);
  });

  test("a worse severity costs more", () => {
    const at = (severity: Finding["severity"]): number => {
      const score = scoreDomain("d", [finding("a", "passed"), finding("b", "issue", severity)]);
      assert.ok(score.status === "measured");
      return score.score;
    };
    assert.ok(at("critical") < at("high") && at("high") < at("medium") && at("medium") < at("low"));
  });

  test("lower confidence costs less", () => {
    const at = (confidence: Finding["confidence"]): number => {
      const score = scoreDomain("d", [finding("a", "passed"), finding("b", "issue", "high", confidence)]);
      assert.ok(score.status === "measured");
      return score.score;
    };
    assert.ok(at("high") < at("medium") && at("medium") < at("low"));
  });

  test("ten findings from one rule cost the same as the worst one", () => {
    const one = scoreDomain("d", [finding("a", "passed"), finding("b", "issue", "high")]);
    const ten = scoreDomain("d", [finding("a", "passed"), ...Array.from({ length: 10 }, () => finding("b", "issue", "high"))]);
    assert.ok(one.status === "measured" && ten.status === "measured" && one.score === ten.score);
  });

  test("a rule with a pass and an issue counts once, as the issue", () => {
    // a earns 3 of 8 as its high issue, z earns 8: 11 of 16.
    const score = scoreDomain("d", [finding("a", "passed"), finding("a", "issue", "high"), finding("z", "passed")]);
    assert.ok(score.status === "measured" && score.score === 69);
  });

  test("a rule's worst finding sets what it earns", () => {
    const score = scoreDomain("d", [finding("a", "issue", "low"), finding("a", "issue", "critical"), finding("z", "passed")]);
    assert.ok(score.status === "measured" && score.score === 50);
  });

  test("not measurable doesn't move a score", () => {
    const without = scoreDomain("d", [finding("a", "passed"), finding("b", "issue")]);
    const withIt = scoreDomain("d", [finding("a", "passed"), finding("b", "issue"), finding("c", "not_measurable"), finding("e", "not_measurable")]);
    assert.ok(without.status === "measured" && withIt.status === "measured" && without.score === withIt.score);
  });

  test("the weights are published on the score", () => {
    const score = scoreDomain("d", [finding("a", "passed"), finding("z", "passed")]);
    assert.ok(score.status === "measured");
    assert.equal(score.weights.ruleMaximum, RULE_MAXIMUM);
    assert.equal(RULE_MAXIMUM, SEVERITY_WEIGHTS.critical);
    assert.equal(score.weights["severity.high"], SEVERITY_WEIGHTS.high);
    assert.equal(score.weights["confidence.medium"], CONFIDENCE_FACTORS.medium);
  });
});

describe("not measurable, never zero", () => {
  test("every finding not measurable gives no score", () => {
    const score = scoreDomain("d", [finding("a", "not_measurable"), finding("b", "not_measurable")]);
    assert.equal(score.status, "not_measurable");
    assert.ok(!("score" in score));
  });

  test("no findings at all gives no score", () => assert.equal(scoreDomain("d", []).status, "not_measurable"));

  test("an info-level issue costs nothing, because its weight is zero", () => {
    const score = scoreDomain("d", [finding("a", "issue", "info"), finding("z", "passed")]);
    assert.ok(score.status === "measured" && score.score === 100);
  });

  test("one measured check is too few to score", () => {
    const score = scoreDomain("d", [finding("a", "passed"), finding("b", "not_measurable")]);
    assert.equal(score.status, "not_measurable");
    assert.ok(score.status === "not_measurable" && /too little of this area could be measured/i.test(score.reason));
  });

  test("a score needs at least half the domain's checks measured", () => {
    const measured = [finding("a", "passed"), finding("b", "passed")];
    assert.equal(scoreDomain("d", [...measured, finding("c", "not_measurable"), finding("e", "not_measurable"), finding("g", "not_measurable")]).status, "not_measurable");
    assert.equal(scoreDomain("d", [...measured, finding("c", "not_measurable"), finding("e", "not_measurable")]).status, "measured");
    assert.equal(MIN_RULES_SCORED, 2);
  });

  test("the schema refuses a score with no number, or one out of range", () => {
    assert.throws(() => Score.parse({ status: "measured", domain: "d", score: 101, ruleSetVersion: "x", weights: {}, findingIds: [] }));
  });
});

describe("what doesn't count against the customer", () => {
  test("a gap in our calibration doesn't lower their score", () => {
    const score = scoreDomain("d", [finding("a", "passed"), finding("z", "passed"), finding("pipe.unmapped-stages", "issue", "high")]);
    assert.ok(score.status === "measured" && score.score === 100);
  });

  test("another domain's findings are ignored", () => {
    const score = scoreDomain("d", [finding("a", "passed"), finding("z", "passed"), finding("b", "issue", "high", "high", "other")]);
    assert.ok(score.status === "measured" && score.score === 100);
  });
});

describe("the synthetic run", () => {
  let dir: string;
  let extracts: Map<string, unknown>;

  before(async () => {
    dir = (await runFromRecording()).dir;
    extracts = loadExtracts(dir);
  });

  test("every Sales Cloud domain has a module, and every module gets a row", () => {
    const ids = SALES_CLOUD_MODULES.map((m) => m.id);
    for (const domain of SALES_CLOUD_DOMAINS) assert.ok(ids.includes(domain), domain);
    const scores = scoreDomains(ids, analyseExtracts(extracts).findings);
    assert.deepEqual(scores.map((s) => s.domain), ids);
  });

  test("each score is a whole number from 0 to 100", () => {
    for (const score of scoreDomains(SALES_CLOUD_MODULES.map((m) => m.id), analyseExtracts(extracts).findings)) {
      assert.ok(score.status === "measured", score.domain);
      assert.ok(Number.isInteger(score.score) && score.score >= 0 && score.score <= 100, `${score.domain}: ${score.score}`);
      assert.ok(score.findingIds.length > 0);
    }
  });

  test("the same findings give the same scores", () => {
    const ids = SALES_CLOUD_MODULES.map((m) => m.id);
    assert.deepEqual(scoreDomains(ids, analyseExtracts(extracts).findings), scoreDomains(ids, analyseExtracts(clone(extracts)).findings));
  });

  test("Forecasting being off leaves forecast with no score", () => {
    const changed = clone(extracts);
    (changed.get("forecast") as { items: { data: unknown[] } }).items.data = [];
    const score = scoreDomain("forecast", analyseExtracts(changed).findings);
    assert.equal(score.status, "not_measurable");
  });

  test("a run writes scores.json, and the raw notes show them", () => {
    assert.ok(existsSync(join(dir, "scores.json")));
    const written = JSON.parse(readFileSync(join(dir, "scores.json"), "utf8")) as unknown[];
    assert.equal(written.length, SALES_CLOUD_MODULES.length);
    for (const row of written) Score.parse(row);
    assert.match(readFileSync(join(dir, "raw-notes.md"), "utf8"), /## Scorecard/);
  });
});
