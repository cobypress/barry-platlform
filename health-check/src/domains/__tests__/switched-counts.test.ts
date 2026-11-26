import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { analyseExtracts, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import { withPipeline } from "../../testing/steady-pipeline.ts";
import { loadCalibration } from "../../testing/replay.ts";
import { scoreDomains } from "../scoring.ts";
import { SALES_CLOUD_MODULES, SWITCH_MODULES, switchedVersion } from "../index.ts";
import { withStuckExpected, withStuckUnscored } from "../current-pipeline/index.ts";
import { withLeadJourney } from "../lead-funnel/index.ts";
import { withCasePaths } from "../service/index.ts";
import { SERVICE_CLOUD_MODULES } from "../index.ts";
import { benchmarkFigures, SWITCHED_SKIP } from "../../benchmark/export.ts";
import { buildReport, type ReportInput } from "../../narrative/sections.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { chapterSlide } from "../../render/chapter.ts";
import { fullReportHtml } from "../../render/full-report.ts";
import { renderContext } from "../../render/html.ts";
import { renderRawNotes } from "../../notes/raw-notes.ts";
import { ruleChangeOf } from "../../digest/build.ts";
import type { Finding } from "../../schemas/index.ts";

// The counts and the benchmark with a switch's version in a run (DECISIONS.md 779): version 4 of stuck-in-stage is
// never counted as a check or a pass, carries neither old-line verdict in the digest, and any area scored with a
// check only a switch runs gives no benchmark score, the same rule for every switch.

const RULE = "cur.stuck-in-stage";
let extracts: Map<string, unknown>;
let dir: string;
before(async () => {
  const r = await runFromRecording();
  dir = r.dir;
  extracts = loadExtracts(r.dir);
});

describe("version 4 in the counts (decision 779)", () => {
  const built = (on: boolean) => {
    const out = analyseExtracts(extracts, { stuckUnscored: on });
    const run = JSON.parse(readFileSync(join(dir, "run.json"), "utf8"));
    const input: ReportInput = { run, calibration: loadCalibration(), findings: out.findings, metrics: out.metrics, manualSteps: out.manualSteps, scores: scoreDomains(run.modules, out.findings) };
    return { report: buildReport(input), out, run };
  };

  test("the chapter's checks and passes, the overview's passes and the raw notes' passes leave it out", () => {
    const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
    const [off, on] = [built(false), built(true)];
    const v2 = off.out.findings.find((f) => f.ruleId === RULE)!;
    assert.equal(v2.status, "issue", "the synthetic org raises version 2");
    const tiles = (b: ReturnType<typeof built>) => {
      const slide = chapterSlide(renderContext(pseudonyms, "GBP", false), b.report, "current_pipeline");
      return Object.fromEntries([...slide.matchAll(/<b>(\d+)<\/b><span>(Checks?|Passed|Issues?)<\/span>/g)].map((m) => [m[2]!.replace(/s$/, ""), Number(m[1])]));
    };
    const [a, b] = [tiles(off), tiles(on)];
    assert.equal(b.Check, a.Check! - 1, "one check fewer: version 4 is shown, not counted");
    assert.equal(b.Passed, a.Passed, "and never a pass");
    assert.equal(b.Issue, a.Issue! - 1);
    const passedTile = (b: ReturnType<typeof built>) => Number(/<b>(\d+)<\/b><span>Passed<\/span><\/div><\/div>/.exec(fullReportHtml(b.report, pseudonyms))![1]);
    assert.equal(passedTile(on), passedTile(off));
    const rawCount = (b: ReturnType<typeof built>) => Number(/(\d+) checks passed/.exec(renderRawNotes({ run: b.run, calibration: loadCalibration(), findings: b.out.findings, metrics: b.out.metrics, manualSteps: b.out.manualSteps, pseudonyms }))![1]);
    assert.equal(rawCount(on), rawCount(off));
  });

  test("the digest gives a version 4 pass neither issueUnder nor passedUnder", () => {
    const v4 = built(true).out.findings.find((f) => f.ruleId === RULE)!;
    assert.equal(v4.ruleVersion, 4);
    assert.deepEqual(ruleChangeOf(v4, { ownCalls: 0, carried: {} }), { ruleVersion: 4 });
    const v3pass = { ...v4, ruleVersion: 3, facts: { ...v4.facts, wonLineRate: 0 } } as Finding;
    assert.ok("issueUnder" in ruleChangeOf(v3pass, { ownCalls: 0, carried: {} }), "version 3's pass still carries it");
  });
});

describe("the benchmark: one rule for every switch (decision 779)", () => {
  const scored = (o: Parameters<typeof analyseExtracts>[1]) => {
    const out = analyseExtracts(extracts, o);
    const domains = [...new Set(out.findings.map((f) => f.domain))];
    return benchmarkFigures({ metrics: out.metrics, findings: out.findings, scores: scoreDomains(domains, out.findings) });
  };
  test("an area scored with a check only a switch runs gives no score, said; every other area's goes in", () => {
    const off = scored({});
    for (const [o, domain] of [[{ stuckUnscored: true }, "current_pipeline"], [{ stuckExpected: true }, "current_pipeline"], [{ leadJourney: true }, "lead_funnel"]] as const) {
      const on = scored(o);
      const key = `score.${domain}`;
      assert.ok(off.figures.some((x) => x.metricKey === key), `${key} goes in with the switch off`);
      assert.equal(on.figures.find((x) => x.metricKey === key), undefined, JSON.stringify(o));
      assert.equal(on.skipped.find((s) => s.metricKey === key)?.reason, SWITCHED_SKIP);
      assert.deepEqual(on.figures.filter((x) => x.metricKey.startsWith("score.") && x.metricKey !== key), off.figures.filter((x) => x.metricKey.startsWith("score.") && x.metricKey !== key), "the other areas as before");
    }
  });

  test("every module a switch puts in is listed, and nothing the rule set runs reads as switched", () => {
    const swapped = (list: typeof SALES_CLOUD_MODULES, w: (l: typeof SALES_CLOUD_MODULES) => typeof SALES_CLOUD_MODULES) => w(list).filter((m, i) => m !== list[i]);
    const put = [...swapped(SALES_CLOUD_MODULES, withStuckExpected), ...swapped(SALES_CLOUD_MODULES, withStuckUnscored), ...swapped(SALES_CLOUD_MODULES, withLeadJourney), ...swapped(SERVICE_CLOUD_MODULES, withCasePaths)];
    assert.deepEqual(new Set(put), new Set(SWITCH_MODULES));
    for (const r of [...SALES_CLOUD_MODULES, ...SERVICE_CLOUD_MODULES].flatMap((m) => m.rules)) assert.equal(switchedVersion({ ruleId: r.id, ruleVersion: r.version }), false, r.id);
    assert.equal(switchedVersion({ ruleId: RULE, ruleVersion: 4 }), true);
    assert.equal(switchedVersion({ ruleId: "lead.source-win-rate", ruleVersion: 1 }), true);
  });
});

describe("stuck-in-stage with no open deal to compare (decision 779)", () => {
  test("says so in plain words, under version 2 and version 4", () => {
    const opps = extracts.get("opportunities") as { opportunities: { data: Array<{ Id: string; IsClosed: boolean }> }; history: { data: Array<{ OpportunityId: string }> } };
    const open = new Set(opps.opportunities.data.filter((o) => !o.IsClosed).map((o) => o.Id));
    const noEntries = withPipeline(extracts, { opportunities: opps.opportunities.data, history: opps.history.data.filter((h) => !open.has(h.OpportunityId)) });
    for (const stuckUnscored of [false, true]) {
      const f = analyseExtracts(noEntries, { stuckUnscored }).findings.find((x) => x.ruleId === RULE)!;
      assert.equal(f.status, "passed");
      assert.equal(f.summary, "There are no open deals we can compare with won deals in their stage.", `version ${f.ruleVersion}`);
    }
  });
});
