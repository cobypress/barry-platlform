import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { ALL_MODULES } from "../../domains/index.ts";
import { issueUnder, LOOSENED, loosenedFrom, loosenedProblems, passedOnlyByChange } from "../loosened.ts";
import { buildDigest } from "../../digest/build.ts";
import { buildReport } from "../../narrative/sections.ts";
import { loadRun } from "../../render/render-run.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers } from "../../testing/replay.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { join } from "node:path";

// Rule versions that loosened a rule (DECISIONS.md 732): what the declarations may
// say, the one rule that judges a pass, and what the digest carries for the console.

const versions = new Map(ALL_MODULES.flatMap((m) => m.rules).map((r) => [r.id, r.version]));

describe("the declarations (decision 732)", () => {
  test("every declaration names a real rule, a version it has had and earlier versions that exist", () => {
    assert.deepEqual(loosenedProblems(LOOSENED, versions), []);
    assert.deepEqual(loosenedFrom("int.api-usage"), { "3": [1, 2] });
    assert.equal(loosenedFrom("tech.storage-use"), undefined);
  });

  test("a declaration naming a version that doesn't exist, or an unknown rule, is refused", () => {
    const at = (version: number, from: number[]) => ({ "int.api-usage": [{ version, from }] });
    assert.match(loosenedProblems(at(4, [1, 2]), versions).join(), /version 4 isn't one the rule has had/);
    assert.match(loosenedProblems(at(3, [0, 2]), versions).join(), /from 0, which isn't an earlier version/);
    assert.match(loosenedProblems(at(3, [3]), versions).join(), /from 3, which isn't an earlier version/);
    assert.match(loosenedProblems(at(3, [2, 1]), versions).join(), /ascending/);
    assert.match(loosenedProblems({ "no.such-rule": [{ version: 2, from: [1] }] }, versions).join(), /no rule has this id/);
  });
});

describe("a pass after an issue (decision 732)", () => {
  const pass = (share: number, ruleVersion = 3) => ({ ruleId: "int.api-usage", ruleVersion, status: "passed", facts: { DailyApiRequestsShare: share } });

  test("the pass's own reading against the old line: 88% would still have been raised under 1 and 2, 40% under neither", () => {
    assert.deepEqual(issueUnder(pass(88)), [1, 2]);
    assert.deepEqual(issueUnder(pass(40)), []);
    assert.equal(issueUnder(pass(88, 2)), undefined, "a version 2 pass: no loosening at or below it");
    assert.equal(issueUnder({ ...pass(88), status: "issue" }), undefined);
    assert.equal(issueUnder({ ruleId: "tech.storage-use", ruleVersion: 3, status: "passed", facts: {} }), undefined);
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

describe("the digest carries the version and the verdict for a loosened rule only (decision 732)", () => {
  test("int.api-usage's finding has its version, and nothing new on any other rule", async () => {
    const { dir } = await runFromRecording({ answers: loadAnswers() });
    const report = buildReport(loadRun(dir).input);
    const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
    const digest = buildDigest({ report, pseudonyms, inventory: null, madeAt: report.input.run.asOf });
    for (const f of digest.findings) {
      if (f.ruleId === "int.api-usage") {
        assert.equal(f.ruleVersion, 3);
        if (f.status === "passed") assert.ok(Array.isArray(f.issueUnder));
        else assert.equal(f.issueUnder, undefined);
      } else assert.ok(!("ruleVersion" in f) && !("issueUnder" in f), f.ruleId);
    }
    assert.ok(digest.findings.some((f) => f.ruleId === "int.api-usage"));
  });
});
