import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { LATEST_RULE_VERSIONS } from "../../domains/index.ts";
import { foundByStricter, issueUnder, LOOSENED, loosenedFrom, loosenedProblems, passedOnlyByChange, wentBack } from "../loosened.ts";
import { compareWithBaseline, type Progress, type ProgressSide } from "../../progress/compare.ts";
import type { Finding } from "../../schemas/index.ts";
import { buildDigest } from "../../digest/build.ts";
import { buildReport } from "../../narrative/sections.ts";
import { loadRun } from "../../render/render-run.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers } from "../../testing/replay.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { join } from "node:path";

// Rule versions that loosened a rule (DECISIONS.md 732): what the declarations may
// say, the one rule that judges a pass, and what the digest carries for the console.

// With the versions behind a switch (DECISIONS.md 777): cur.stuck-in-stage 3 runs only with HC_STUCK_EXPECTED on.
const versions = LATEST_RULE_VERSIONS;

describe("the declarations (decision 732)", () => {
  test("every declaration names a real rule, a version it has had and earlier versions that exist", () => {
    assert.deepEqual(loosenedProblems(LOOSENED, versions), []);
    assert.deepEqual(loosenedFrom("int.api-usage"), { "3": [1, 2] });
    assert.equal(loosenedFrom("tech.storage-use"), undefined);
  });

  test("a declaration naming a version that doesn't exist, or an unknown rule, is refused", () => {
    const at = (version: number, from: number[], on = "2026-11-25") => ({ "int.api-usage": [{ version, from, on }] });
    assert.match(loosenedProblems(at(4, [1, 2]), versions).join(), /version 4 isn't one the rule has had/);
    assert.match(loosenedProblems(at(3, [0, 2]), versions).join(), /from 0, which isn't an earlier version/);
    assert.match(loosenedProblems(at(3, [3]), versions).join(), /from 3, which isn't an earlier version/);
    assert.match(loosenedProblems(at(3, [2, 1]), versions).join(), /ascending/);
    assert.match(loosenedProblems({ "no.such-rule": [{ version: 2, from: [1], on: "2026-11-25" }] }, versions).join(), /no rule has this id/);
    assert.match(loosenedProblems(at(3, [1, 2], "25 November"), versions).join(), /isn't a day/);
  });
});

describe("a version that went back, for any rule (decision 777)", () => {
  // tech.storage-use declares nothing: the rule holds for every rule, a worker switch turned off again being the usual cause.
  const RULE = "tech.storage-use";
  const f = (status: "issue" | "passed", ruleVersion: number) => ({ id: `f-${status}-${ruleVersion}`, ruleId: RULE, domain: "technical_health", status, ruleVersion, title: "T", facts: {} }) as unknown as Finding;
  const side = (runId: string, findings: Finding[], carried?: Record<string, number>): ProgressSide =>
    ({ runId, asOf: "2026-10-31T12:00:00.000Z", findings, scores: [], overall: { status: "not_measurable" }, value: { currency: "GBP", total: { expected: 0 } }, limitsReadAt: null, ...(carried ? { carried } : {}) }) as unknown as ProgressSide;
  const where = (p: Progress): string[] => (["fixed", "remaining", "added", "unknown", "newlyMeasured", "ruleChanged", "stricter", "changedBack"] as const).filter((k) => (p[k] ?? []).some((c) => c.ruleId === RULE));

  test("the rule: a later run reading a rule at an older version than an earlier run", () => {
    assert.equal(wentBack(3, 2), true);
    assert.equal(wentBack(2, 3), false);
    assert.equal(wentBack(2, 2), false);
    assert.equal(wentBack(undefined, 2), false, "an unknown version is judged as before");
    assert.equal(passedOnlyByChange(RULE, 3, 2, undefined), true, "an issue under 3, a pass under 2: the check changed back");
    assert.equal(passedOnlyByChange(RULE, 2, 2, undefined), false, "the same version: a real fix");
    assert.equal(passedOnlyByChange(RULE, 2, 3, undefined), false, "a newer version, nothing declared: a real fix, as before");
    assert.equal(foundByStricter(RULE, 3, 2, undefined), false, "a check that came back is not a stricter one");
  });

  test("an issue under 3, then a pass under 2: apart, never fixed; then another pass under 2: nothing, as two runs alike", () => {
    const p1 = compareWithBaseline(side("a", [f("issue", 3)]), side("b", [f("passed", 2)]));
    assert.deepEqual(where(p1), ["ruleChanged"]);
    assert.deepEqual(p1.carried, { [RULE]: 3 });
    const p2 = compareWithBaseline(side("b", [f("passed", 2)], p1.carried), side("c", [f("passed", 2)]));
    assert.deepEqual(where(p2), [], "the ordinary comparison: a pass after a pass says nothing");
    assert.equal(p2.carried, undefined);
    // While the versions still differ it stays apart: a pass under 2 after a baseline whose pass was under another version.
    const p3 = compareWithBaseline(side("b", [f("passed", 1)], p1.carried), side("c", [f("passed", 2)]));
    assert.deepEqual(where(p3), ["ruleChanged"]);
  });

  test("a pass under 3, then an issue under 2: open, never new, and not 'stricter': a list of its own with nothing said", () => {
    const p = compareWithBaseline(side("a", [f("passed", 3)]), side("b", [f("issue", 2)]));
    assert.deepEqual(where(p), ["changedBack"]);
    assert.equal(p.added.length, 0);
    assert.equal(p.stricter, undefined);
    assert.equal(compareWithBaseline(side("a", [f("passed", 2)]), side("b", [f("issue", 2)])).added.length, 1, "the same version: new");
  });
});

describe("a pass after an issue (decision 732)", () => {
  const pass = (share: number, ruleVersion = 3) => ({ ruleId: "int.api-usage", ruleVersion, status: "passed", facts: { DailyApiRequestsShare: share, DailyApiRequestsUsed: share * 1000, DailyApiRequestsMax: 100_000 } });

  test("the pass's own reading against the old line: 88% would still have been raised under 1 and 2, 40% under neither", () => {
    assert.deepEqual(issueUnder(pass(88), 500), [1, 2]);
    assert.deepEqual(issueUnder(pass(40), 500), []);
    assert.equal(issueUnder(pass(88, 2), 500), undefined, "a version 2 pass: no loosening at or below it");
    assert.equal(issueUnder({ ...pass(88), status: "issue" }, 500), undefined);
    assert.equal(issueUnder({ ruleId: "tech.storage-use", ruleVersion: 3, status: "passed", facts: {} }, 500), undefined);
  });

  test("version 3 reads before the run's own calls and 1 and 2 after them: API requests within the run's calls of 80% would still have been raised", () => {
    // 79.6% read first; the run made 500 calls of a 100,000 allowance, so version 2 could have read 80.1%.
    assert.deepEqual(issueUnder(pass(79.6), 500), [1, 2]);
    assert.deepEqual(issueUnder(pass(79.6), 300), [], "79.9%: below the old line however the calls fell");
    assert.deepEqual(issueUnder(pass(79.6), undefined), [1, 2], "the run's calls unknown: taken as still raised");
    // Background jobs: the run starts none, so their share is judged as read.
    assert.deepEqual(issueUnder({ ruleId: "int.api-usage", ruleVersion: 3, status: "passed", facts: { DailyAsyncApexExecutionsShare: 79.9 } }, 100_000), []);
  });

  test("passed only because the rule changed: only across the loosening, and only where the old line would still raise it", () => {
    assert.equal(passedOnlyByChange("int.api-usage", 2, 3, [1, 2]), true);
    assert.equal(passedOnlyByChange("int.api-usage", 2, 3, []), false, "a real fix");
    assert.equal(passedOnlyByChange("int.api-usage", 3, 3, [1, 2]), false, "same version: a real fix");
    assert.equal(passedOnlyByChange("int.api-usage", 2, 2, undefined), false, "no loosening between");
    assert.equal(passedOnlyByChange("int.api-usage", 2, 3, undefined), true, "can't judge: never a fix we can't show");
    assert.equal(passedOnlyByChange("int.api-usage", undefined, 3, [1, 2]), true, "unknown issue version");
    assert.equal(passedOnlyByChange("int.api-usage", undefined, 3, []), false, "passes under every older version: a fix whatever the issue's");
    assert.equal(passedOnlyByChange("tech.storage-use", 2, 3, undefined), false, "an ordinary bump stays a fix");
  });
});

describe("the digest carries every finding's version, and the verdict for a loosened rule only (decision 732)", () => {
  test("every finding has its rule's version; only int.api-usage's pass has a verdict", async () => {
    const { dir } = await runFromRecording({ answers: loadAnswers() });
    const report = buildReport(loadRun(dir).input);
    const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
    const digest = buildDigest({ report, pseudonyms, inventory: null, madeAt: report.input.run.asOf });
    for (const f of digest.findings) {
      if (f.ruleId === "int.api-usage") {
        assert.equal(f.ruleVersion, 3);
        if (f.status === "passed") assert.ok(Array.isArray(f.issueUnder));
        else assert.equal(f.issueUnder, undefined);
      } else assert.ok(f.ruleVersion === report.teamFindings.find((x) => x.id === f.id)!.ruleVersion && !("issueUnder" in f) && !("carriedFrom" in f), f.ruleId);
    }
    assert.ok(digest.findings.some((f) => f.ruleId === "int.api-usage"));
  });
});
