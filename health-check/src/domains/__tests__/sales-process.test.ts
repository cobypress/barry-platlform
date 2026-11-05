import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { strFromU8, unzipSync } from "fflate";
import type { HistoryRow, OpportunityExtract } from "../../extract/opportunities.ts";
import { buildReport } from "../../narrative/sections.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { fullReportHtml } from "../../render/full-report.ts";
import { renderRawNotesDocx } from "../../render/raw-notes-docx.ts";
import { noteSections } from "../../render/references.ts";
import type { Calibration, Finding, Metric } from "../../schemas/index.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { backwardMoves, pathOf, PROCESS_METRIC_IDS, transitionMatrix } from "../historic-pipeline/process.ts";
import type { Deal } from "../historic-pipeline/cohort.ts";

const { output, run, dir } = await runFromRecording({ answers: loadAnswers() });
const extracts = loadExtracts(dir);
const calibration = loadCalibration();
const OPPS = "opportunities";
type Measured = Extract<Metric, { status: "measured" }>;

const one = (findings: Finding[], ruleId: string): Finding => {
  const found = findings.find((f) => f.ruleId === ruleId);
  assert.ok(found, ruleId);
  return found;
};
const metric = (metrics: Metric[], id: string): Metric => {
  const found = metrics.find((m) => m.id === id);
  assert.ok(found, id);
  return found;
};
const measuredOf = (metrics: Metric[], id: string): Measured => {
  const m = metric(metrics, id);
  assert.equal(m.status, "measured", m.status === "not_measurable" ? `${id}: ${m.reason}` : id);
  return m as Measured;
};
const changeHistory = (edit: (rows: HistoryRow[]) => HistoryRow[]): Map<string, unknown> => {
  const changed = clone(extracts);
  const e = changed.get(OPPS) as OpportunityExtract;
  if (e.history.status === "ok") e.history = { ...e.history, data: edit(e.history.data) };
  return changed;
};
const withoutRequired = (): Calibration => ({ ...calibration, stages: calibration.stages.map(({ required: _, ...s }) => s) });
const order = (stage: string): number | null => {
  const s = calibration.stages.find((x) => x.name === stage && x.category === "open");
  return s ? s.order! : null;
};

/** Each deal's rows with any move back to an earlier open stage left out. */
const forwardOnly = (rows: HistoryRow[]): HistoryRow[] => {
  const furthest = new Map<string, number>();
  return [...rows]
    .sort((a, b) => a.OpportunityId.localeCompare(b.OpportunityId) || a.CreatedDate.localeCompare(b.CreatedDate) || a.Id.localeCompare(b.Id))
    .filter((row) => {
      const at = order(row.StageName);
      if (at === null) return true;
      const best = furthest.get(row.OpportunityId) ?? -1;
      if (at < best) return false;
      furthest.set(row.OpportunityId, at);
      return true;
    });
};

describe("the sales process traps fire on the synthetic org", () => {
  const { findings, metrics } = output!;

  test("won deals skip the stage the calibration marks as required", () => {
    const f = one(findings, "pipe.skipped-stages");
    assert.equal(f.status, "issue");
    assert.equal(f.confidence, "high");
    assert.match(f.summary, /164 of 164 won deals \(100%\) never entered a stage we'd expect them to: Legal Review \(164\)/);
    assert.match(f.summary, /the calibration marks as required: Legal Review/);
    assert.equal(f.value.kind, "not_sized");
  });

  test("deals go back a stage, and the EU team's far more than the rest", () => {
    const f = one(findings, "pipe.going-backwards");
    assert.equal(f.status, "issue");
    assert.equal(f.facts.hits, 109);
    assert.match(f.summary, /team Sales EU, 73 of 110 \(66.4%\)/);
    assert.ok(f.segmentsChecked.some((s) => s.dimension === "team"));
    assert.equal(f.value.kind, "not_sized");
  });

  test("lost deals sit in Prospecting far longer than won deals do", () => {
    const f = one(findings, "pipe.dead-end-stage");
    assert.equal(f.status, "issue");
    assert.match(f.title, /Prospecting/);
    assert.equal(f.facts.lost, 104);
    assert.ok(f.facts.wait! > f.facts.line!);
    assert.equal(f.recommendationType, "change");
  });

  test("every process metric is measured, and the matrix holds the moves back", () => {
    for (const id of PROCESS_METRIC_IDS) measuredOf(metrics, id);
    const moves = measuredOf(metrics, "historic.stage_transitions");
    assert.equal(moves.stats.backwards, 109);
    const { from, to, count } = transitionMatrix(moves, calibration);
    assert.deepEqual(from.slice(0, 2), ["Prospecting", "Qualified"]);
    assert.equal(to.at(-1), "Closed No Decision");
    assert.equal(count("Discovery", "Prospecting"), 77);
    assert.equal(count("Negotiation", "Closed Won"), 164);
    assert.equal(count("Closed Won", "Prospecting"), null);
    // Every move is one cell, so the cells add up to the total.
    assert.equal(moves.segments.reduce((sum, s) => sum + s.value!, 0), moves.value);
  });

  test("the most common paths, with how many deals took them", () => {
    const won = measuredOf(metrics, "historic.path_to_won");
    assert.equal(won.segments[0]!.key, "Prospecting > Discovery > Proposal > Negotiation > Closed Won");
    assert.equal(won.value, 95.1);
    const lost = measuredOf(metrics, "historic.path_to_lost");
    assert.equal(lost.segments[0]!.key, "Prospecting > Closed Lost");
    assert.ok(lost.segments.length <= 5);
    assert.equal(measuredOf(metrics, "historic.went_backwards").value, 25.9);
  });
});

describe("the sales process stays quiet on clean data", () => {
  test("with no required stage, won deals are judged by the stages most of them pass through", () => {
    const { findings, metrics } = analyseExtracts(extracts, { calibration: withoutRequired() });
    const f = one(findings, "pipe.skipped-stages");
    assert.equal(f.status, "passed");
    assert.match(f.summary, /more than 50% of won deals passed through: Prospecting, Discovery, Proposal, Negotiation/);
    const skipped = measuredOf(metrics, "historic.won_skipped_stage");
    assert.ok(skipped.value <= 10);
  });

  test("deals that only move forward", () => {
    const { findings, metrics } = analyseExtracts(changeHistory(forwardOnly));
    assert.equal(one(findings, "pipe.going-backwards").status, "passed");
    assert.equal(measuredOf(metrics, "historic.went_backwards").value, 0);
  });

  test("lost deals closed soon after they stall", () => {
    const changed = changeHistory((rows) => {
      const byDeal = new Map<string, HistoryRow[]>();
      for (const row of rows) byDeal.set(row.OpportunityId, [...(byDeal.get(row.OpportunityId) ?? []), row]);
      for (const deal of byDeal.values()) {
        deal.sort((a, b) => a.CreatedDate.localeCompare(b.CreatedDate) || a.Id.localeCompare(b.Id));
        const last = deal.at(-1)!;
        if (!/^Closed (Lost|No Decision)/.test(last.StageName)) continue;
        // The last open stage starts a day before the close.
        const before = new Date(new Date(last.CreatedDate.replace("+0000", "Z")).getTime() - 86_400_000).toISOString().replace("Z", "+0000");
        let i = deal.length - 2;
        while (i >= 0 && deal[i]!.StageName === deal[deal.length - 2]!.StageName) deal[i--]!.CreatedDate = before;
      }
      return rows;
    });
    const f = one(analyseExtracts(changed).findings, "pipe.dead-end-stage");
    assert.equal(f.status, "passed");
    assert.match(f.summary, /no longer than three quarters of won deals took/);
  });
});

describe("the sales process is not measurable when the history can't support it", () => {
  const rules = ["pipe.skipped-stages", "pipe.going-backwards", "pipe.dead-end-stage"];

  test("no stage history", () => {
    const changed = clone(extracts);
    const e = changed.get(OPPS) as OpportunityExtract;
    e.history = { status: "unavailable", reason: "INSUFFICIENT_ACCESS: no", source: e.history.source };
    const { findings, metrics } = analyseExtracts(changed);
    for (const id of rules) assert.equal(one(findings, id).status, "not_measurable", id);
    for (const id of PROCESS_METRIC_IDS) assert.equal(metric(metrics, id).status, "not_measurable", id);
  });

  test("a stage in the history the calibration doesn't cover", () => {
    const { findings, metrics } = analyseExtracts(changeHistory((rows) => rows.map((r, i) => (i === 0 ? { ...r, StageName: "Mystery" } : r))));
    for (const id of rules) assert.match(one(findings, id).summary, /doesn't say what these stages mean: Mystery/, id);
    assert.equal(metric(metrics, "historic.stage_transitions").status, "not_measurable");
  });

  test("too few closed deals for the calibrated minimum", () => {
    const { findings } = analyseExtracts(extracts, { calibration: { ...calibration, minSample: 10_000 } });
    for (const id of rules) assert.equal(one(findings, id).status, "not_measurable", id);
  });
});

describe("reading a deal's stages", () => {
  const deal = (stages: string[]): Deal => ({ history: stages.map((StageName, i) => ({ Id: String(i), OpportunityId: "x", StageName, Amount: null, CloseDate: "2026-01-01", CreatedDate: `2026-01-0${i + 1}T00:00:00.000+0000` })) }) as Deal;

  test("a repeated row in the same stage isn't a move", () => {
    assert.deepEqual(pathOf(deal(["Prospecting", "Prospecting", "Discovery", "Discovery", "Closed Won"])), ["Prospecting", "Discovery", "Closed Won"]);
  });

  test("going back counts each move to an earlier open stage, and never a close", () => {
    assert.equal(backwardMoves(["Prospecting", "Discovery", "Prospecting", "Discovery", "Proposal", "Discovery", "Closed Lost"], order), 2);
    assert.equal(backwardMoves(["Prospecting", "Proposal", "Closed Won"], order), 0);
    assert.equal(backwardMoves(["Negotiation", "Closed Lost", "Prospecting"], order), 0);
  });
});

describe("in the reports", () => {
  const report = buildReport({ run, calibration, findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps, intake: null });
  const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);

  test("the raw notes have a section of their own, numbering every process metric", () => {
    const sections = noteSections(report);
    const section = sections.find((s) => s.key === "sales_process")!;
    assert.equal(section.title, "How your sales process really runs");
    assert.deepEqual(section.items.map((i) => i.id), PROCESS_METRIC_IDS);
    assert.ok(!sections.find((s) => s.key === "pipeline_metrics")!.items.some((i) => PROCESS_METRIC_IDS.includes(i.id)));
  });

  test("the Word notes show the moves as a table, from stage to stage", async () => {
    const xml = strFromU8(unzipSync(new Uint8Array(await renderRawNotesDocx(report, pseudonyms)))["word/document.xml"]!);
    const text = [...xml.matchAll(/<w:t(?: [^>]*)?>([^<]*)<\/w:t>/g)].map((m) => m[1]).join(" ");
    const section = noteSections(report).find((s) => s.key === "sales_process")!;
    assert.match(text, new RegExp(`${section.ref} +How your sales process really runs`));
    assert.match(text, / From +Prospecting +Qualified +Discovery +Proposal +Negotiation +Closed Won/);
  });

  test("the full report has a slide with both paths and the moves as a flow", () => {
    const html = fullReportHtml(report, pseudonyms);
    assert.match(html, /How your sales process really runs/);
    assert.match(html, /Moves between stages/);
    assert.match(html, /data-chart="process-flow"/);
    assert.match(html, /Prospecting &gt; Discovery &gt; Proposal &gt; Negotiation &gt; Closed Won|Prospecting > Discovery > Proposal > Negotiation > Closed Won/);
  });

  test("the slide is left out, and listed as a gap, when the history can't be read", () => {
    const changed = clone(extracts);
    const e = changed.get(OPPS) as OpportunityExtract;
    e.history = { status: "unavailable", reason: "INSUFFICIENT_ACCESS: no", source: e.history.source };
    const out = analyseExtracts(changed);
    const html = fullReportHtml(buildReport({ run, calibration, findings: out.findings, metrics: out.metrics, manualSteps: out.manualSteps, intake: null }), pseudonyms);
    assert.doesNotMatch(html, /data-chart="process-flow"/);
    assert.match(html, /How your sales process really runs/);
  });
});
