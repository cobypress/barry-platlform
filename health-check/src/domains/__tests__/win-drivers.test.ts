import { after, before, describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { strFromU8, unzipSync } from "fflate";
import type { Browser } from "playwright";
import type { HistoryRow, OpportunityExtract, OpportunityRow } from "../../extract/opportunities.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { buildReport, type ReportInput } from "../../narrative/sections.ts";
import { checkNumbers } from "../../numbercheck/index.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { DECK_RANGE, fullReportHtml } from "../../render/full-report.ts";
import { launchBrowser, layoutOf } from "../../render/pdf.ts";
import { renderRawNotesDocx } from "../../render/raw-notes-docx.ts";
import { noteSections } from "../../render/references.ts";
import type { Calibration, Finding, Metric, MetricSegment } from "../../schemas/index.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { AS_OF } from "../../testing/synthetic-org.ts";
import { SALES_CLOUD_MODULES } from "../index.ts";
import { modulesFor } from "../scope.ts";
import { causesOf, DRIVER_GAP, driverMetrics, driverSentence, FACTORS, readDrivers, WIN_DRIVERS, type FactorKey } from "../historic-pipeline/drivers.ts";

// Roadmap 3.3: what separates the customer's wins from their losses, from their
// own closed deals, and the issues it's linked to.

const { output, run, dir } = await runFromRecording({ answers: loadAnswers() });
const extracts = loadExtracts(dir);
const calibration = loadCalibration();
const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
const OPPS = "opportunities";
type Measured = Extract<Metric, { status: "measured" }>;

const drivers = (metrics: Metric[]): Metric => {
  const m = metrics.find((x) => x.id === WIN_DRIVERS);
  assert.ok(m, WIN_DRIVERS);
  return m;
};
const measuredOf = (metrics: Metric[]): Measured => {
  const m = drivers(metrics);
  assert.equal(m.status, "measured", m.status === "not_measurable" ? m.reason : "");
  return m as Measured;
};
const factor = (metrics: Metric[], key: FactorKey) => readDrivers(measuredOf(metrics)).find((d) => d.key === key)!;
const one = (findings: Finding[], ruleId: string): Finding => findings.find((f) => f.ruleId === ruleId)!;
const withoutRequired = (): Calibration => ({ ...calibration, stages: calibration.stages.map(({ required: _, ...s }) => s) });
const reportOf = (out: { findings: Finding[]; metrics: Metric[]; manualSteps: ReportInput["manualSteps"] }, cal: Calibration = calibration) => buildReport({ run, calibration: cal, findings: out.findings, metrics: out.metrics, manualSteps: out.manualSteps, intake: null });

/** Half the deals lost outright, and one won deal in seven, with no logged activity: quiet deals lose. Or only the first few of them. */
function quietDeals(limit = Infinity): Map<string, unknown> {
  const changed = clone(extracts);
  const e = changed.get(OPPS) as OpportunityExtract;
  let k = 0;
  let quiet = 0;
  const silence = (o: OpportunityRow): OpportunityRow => (quiet++ < limit ? { ...o, LastActivityDate: null } : o);
  if (e.opportunities.status === "ok") {
    e.opportunities = { ...e.opportunities, data: e.opportunities.data.map((o) => (o.IsClosed && ((!o.IsWon && o.StageName === "Closed Lost" && k++ % 2 === 0) || (o.IsWon && k++ % 7 === 0)) ? silence(o) : o)) };
  }
  return changed;
}

const changeHistory = (edit: (rows: HistoryRow[]) => HistoryRow[]): Map<string, unknown> => {
  const changed = clone(extracts);
  const e = changed.get(OPPS) as OpportunityExtract;
  if (e.history.status === "ok") e.history = { ...e.history, data: edit(e.history.data) };
  return changed;
};

/** Every third deal that reached Proposal never entered Discovery. */
const skipDiscovery = (rows: HistoryRow[]): HistoryRow[] => {
  const reached = [...new Set(rows.filter((r) => r.StageName === "Proposal").map((r) => r.OpportunityId))].sort();
  const skipping = new Set(reached.filter((_, i) => i % 3 === 0));
  return rows.filter((r) => !(skipping.has(r.OpportunityId) && r.StageName === "Discovery"));
};

describe("what separates wins from losses, on the synthetic org", () => {
  const m = measuredOf(output!.metrics);

  test("covers won and lost deals that closed after being open, with the headline win rate's definition", () => {
    assert.equal(m.n, 352);
    assert.equal(m.stats.won! + m.stats.lost!, m.n);
    // No decision deals are left out, as historic.win_rate leaves them out.
    assert.equal(m.stats.noDecision, 69);
    assert.equal(m.value, Math.round((m.stats.won! / m.n) * 1000) / 10);
    assert.match(m.assumptions.join(" "), /won deals as a share of won and lost deals/);
    const headline = output!.metrics.find((x) => x.id === "historic.win_rate")!;
    assert.match(headline.definition, /share of won and lost deals.*No decision deals are left out/);
    assert.ok(m.exclusions.some((e) => /69 no decision deals/.test(e)));
  });

  test("going back a stage separates them most: none of those deals won", () => {
    const d = factor(output!.metrics, "backwards");
    assert.deepEqual([d.with.n, d.with.rate, d.without.n, d.without.rate, d.gap], [87, 0, 265, 61.9, 61.9]);
    assert.deepEqual([d.with.won, d.without.won], [0, 164]);
    assert.ok(d.isDriver && d.riskLoses);
    // Rule 4: it holds inside every segment with enough deals on both sides.
    assert.deepEqual([d.held, d.compared], [11, 11]);
    assert.equal(readDrivers(m)[0]!.key, "backwards");
  });

  test("every factor is stored as two segments, with and without it", () => {
    for (const f of FACTORS) {
      const sides = m.segments.filter((s) => s.dimension === f.label);
      assert.deepEqual(sides.map((s) => s.key), ["with", "without"], f.key);
      assert.equal(sides[0]!.n + sides[1]!.n, m.n, f.key);
    }
  });

  test("a factor with too few deals on one side is not measurable, with that reason", () => {
    // Every closed deal here has a last activity date, and nobody moves past Legal Review.
    const activity = factor(output!.metrics, "activity");
    assert.equal(activity.gap, null);
    assert.equal(activity.reason, "0 deals without it, below the calibrated minimum of 20 on each side.");
    assert.equal(m.segments.find((s) => s.dimension === activity.label && s.key === "with")!.value, null);
    assert.equal(factor(output!.metrics, "skipped").reason, "0 deals with it, below the calibrated minimum of 20 on each side.");
    assert.ok(!("activityGap" in m.stats));
  });

  test("a close date pushed twice or more, and a long cycle, went with winning here", () => {
    const pushed = factor(output!.metrics, "pushed");
    assert.deepEqual([pushed.with.n, pushed.with.rate, pushed.without.rate, pushed.gap], [43, 55.8, 45.3, 10.5]);
    assert.ok(pushed.isDriver && !pushed.riskLoses);
    const long = factor(output!.metrics, "long");
    assert.deepEqual([long.with.n, long.gap, m.stats.cycleLineDays], [72, 13, 114.8]);
  });
});

describe("each factor, with enough deals and without", () => {
  test("logged activity", () => {
    const d = factor(analyseExtracts(quietDeals()).metrics, "activity");
    assert.deepEqual([d.with.n, d.with.rate, d.without.n, d.without.rate, d.gap], [246, 56.9, 106, 22.6, 34.3]);
    assert.ok(d.isDriver && d.riskLoses);
    const thin = factor(analyseExtracts(quietDeals(12)).metrics, "activity");
    assert.equal(thin.reason, "12 deals without it, below the calibrated minimum of 20 on each side.");
  });

  test("a close date pushed twice or more", () => {
    assert.notEqual(factor(output!.metrics, "pushed").gap, null);
    const thin = factor(analyseExtracts(extracts, { calibration: { ...calibration, minSample: 50 } }).metrics, "pushed");
    assert.equal(thin.reason, "43 deals with it, below the calibrated minimum of 50 on each side.");
  });

  test("moving past an expected stage without entering it", () => {
    const d = factor(analyseExtracts(changeHistory(skipDiscovery), { calibration: withoutRequired() }).metrics, "skipped");
    assert.ok(d.with.n >= 20, String(d.with.n));
    assert.notEqual(d.gap, null);
    // A won deal's close doesn't count as moving past a stage nobody uses.
    assert.equal(factor(output!.metrics, "skipped").with.n, 0);
  });

  test("going back a stage", () => {
    const forward = changeHistory((rows) => {
      const furthest = new Map<string, number>();
      const order = (stage: string): number | null => calibration.stages.find((s) => s.name === stage && s.category === "open")?.order ?? null;
      return [...rows]
        .sort((a, b) => a.OpportunityId.localeCompare(b.OpportunityId) || a.CreatedDate.localeCompare(b.CreatedDate) || a.Id.localeCompare(b.Id))
        .filter((row) => {
          const at = order(row.StageName);
          if (at === null) return true;
          if (at < (furthest.get(row.OpportunityId) ?? -1)) return false;
          furthest.set(row.OpportunityId, at);
          return true;
        });
    });
    assert.equal(factor(analyseExtracts(forward).metrics, "backwards").reason, "0 deals with it, below the calibrated minimum of 20 on each side.");
  });

  test("a long cycle needs enough won deals to say what long is", () => {
    const m = drivers(analyseExtracts(extracts, { calibration: { ...calibration, minSample: 200 } }).metrics);
    assert.equal(m.status, "not_measurable");
    assert.match(m.status === "not_measurable" ? m.reason : "", /No factor has enough deals on both sides to compare\. .*Took longer than 75% of won deals: 164 won deals is below the calibrated minimum of 200, too few to say how long a won deal usually takes\./);
  });

  test("no stage history, and too few closed deals", () => {
    const changed = clone(extracts);
    const e = changed.get(OPPS) as OpportunityExtract;
    e.history = { status: "unavailable", reason: "INSUFFICIENT_ACCESS: no", source: e.history.source };
    assert.equal(drivers(analyseExtracts(changed).metrics).status, "not_measurable");
    assert.equal(drivers(analyseExtracts(extracts, { calibration: { ...calibration, minSample: 10_000 } }).metrics).status, "not_measurable");
  });
});

/** A metric with one factor's figures set by hand. */
function handMade(key: FactorKey, withRate: number, withoutRate: number, gap: number, held: number, compared: number): Measured {
  const f = FACTORS.find((x) => x.key === key)!;
  const segments: MetricSegment[] = FACTORS.flatMap((x) =>
    (["with", "without"] as const).map((side) => ({ dimension: x.label, key: side, n: 40, value: x.key === key ? (side === "with" ? withRate : withoutRate) : null, notMeasurableReason: x.key === key ? null : "0 deals with it, below the calibrated minimum of 20 on each side." })),
  );
  return { ...(drivers(output!.metrics) as Measured), segments, stats: { [`${f.key}Gap`]: gap, [`${f.key}WithWon`]: 10, [`${f.key}WithoutWon`]: 20, [`${f.key}Compared`]: compared, [`${f.key}Held`]: held } };
}

describe("the gap threshold", () => {
  test(`a gap of ${DRIVER_GAP} points is a driver, and anything under it isn't`, () => {
    assert.equal(readDrivers(handMade("backwards", 40, 50, 10, 2, 2))[0]!.isDriver, true);
    assert.equal(readDrivers(handMade("backwards", 40.1, 50, 9.9, 2, 2))[0]!.isDriver, false);
  });

  test("a gap that turns round in most segments isn't a driver", () => {
    assert.equal(readDrivers(handMade("backwards", 30, 50, 20, 1, 3))[0]!.isDriver, false);
    assert.equal(readDrivers(handMade("backwards", 30, 50, 20, 2, 3))[0]!.isDriver, true);
    // With no segment to check it in, the sentence says so.
    const lone = readDrivers(handMade("backwards", 30, 50, 20, 0, 0))[0]!;
    assert.ok(lone.isDriver);
    assert.match(driverSentence(lone), /No team, record type or other segment had enough deals on both sides/);
  });

  test("a gap under the threshold is never linked to a finding", () => {
    const metrics = [handMade("backwards", 40.1, 50, 9.9, 2, 2)];
    assert.deepEqual(causesOf(metrics, output!.findings), []);
  });
});

describe("links to the issues about the same thing", () => {
  test("going back a stage is linked to its issue, with a sentence built from the figures", () => {
    const report = reportOf(output!);
    assert.equal(report.causes.length, 1);
    const [cause] = report.causes;
    assert.equal(cause!.factor, "backwards");
    assert.deepEqual(cause!.findingIds, [one(report.teamFindings, "pipe.going-backwards").id]);
    assert.equal(
      cause!.sentence,
      "Measured from your own closed deals: the 87 that went back a stage at least once won 0% of the time, against 61.9% for the other 265, a gap of 61.9 points. The gap points the same way in 11 of the 11 segments, such as teams and record types, with enough deals on both sides. It shows how often the two went together, not proof that one caused the other.",
    );
    // The finding itself is as the rule made it: the link sits beside it.
    assert.equal(one(report.input.findings, "pipe.going-backwards"), one(output!.findings, "pipe.going-backwards"));
    assert.doesNotMatch(JSON.stringify(report.teamFindings), /Measured from your own closed deals/);
  });

  test("a driver that went with winning isn't attached to the issue it would contradict", () => {
    assert.equal(one(output!.findings, "cur.close-date-pushes").status, "issue");
    assert.ok(factor(output!.metrics, "pushed").isDriver);
    assert.ok(!reportOf(output!).causes.some((c) => c.factor === "pushed"));
  });

  test("deals with no logged activity are linked to the open deals that have gone quiet", () => {
    const out = analyseExtracts(quietDeals());
    const report = reportOf(out);
    const cause = report.causes.find((c) => c.factor === "activity")!;
    assert.deepEqual(cause.findingIds, [one(out.findings, "dq.open-opps-no-activity").id]);
    assert.match(cause.sentence, /^Measured from your own closed deals: the 106 that had no logged activity won 22.6% of the time, against 56.9% for the other 246, a gap of 34.3 points\./);
    assert.match(cause.sentence, /not proof that one caused the other\.$/);
  });

  test("nothing is linked to an issue that didn't fire", () => {
    const passed = output!.findings.map((f) => (f.ruleId === "pipe.going-backwards" ? { ...f, status: "passed" as const } : f));
    assert.deepEqual(causesOf(output!.metrics, passed), []);
  });

  test("a factor with no issue of its own is shown, and linked to nothing", () => {
    assert.deepEqual(FACTORS.find((f) => f.key === "long")!.ruleIds, []);
    assert.ok(!reportOf(output!).causes.some((c) => c.factor === "long"));
  });
});

describe("in the reports", () => {
  const report = reportOf(output!);
  const html = fullReportHtml(report, pseudonyms);
  const slides = [...html.matchAll(/<section class="page[^"]*"[^>]*>([\s\S]*?)<\/section>/g)].map((x) => x[1]!);
  const text = (slide: string): string => slide.replace(/<span class="ref" data-ref>[^<]*<\/span>|<div class="footer"[\s\S]*?<\/div>/g, " ").replace(/<[^>]+>/g, " ").replace(/&#39;/g, "'").replace(/&amp;/g, "&");
  const slide = slides.find((s) => /<h2>What separates your wins from your losses<\/h2>/.test(s))!;
  const section = noteSections(report).find((s) => s.key === "win_drivers")!;

  test("the full report has one slide, ranked by the gap, in the pipeline part", () => {
    assert.equal(slides.filter((s) => /<h2>What separates your wins from your losses<\/h2>/.test(s)).length, 1);
    const rows = [...slide.matchAll(/<tr[^>]*><td>([^<]*)/g)].map((x) => x[1]!.trim());
    assert.deepEqual(rows, ["Went back a stage at least once", "Took longer than 75% of won deals", "Close date pushed twice or more", "Had logged activity", "Moved past an expected stage without entering it"]);
    assert.match(text(slide), /61\.9 points/);
    assert.match(text(slide), /Not measurable\. 0 deals without it/);
    assert.match(text(slide), /not proof that one caused the other/);
    const at = slides.indexOf(slide);
    assert.ok(/How your sales process really runs/.test(slides[at - 1]!), "straight after the sales process");
  });

  test("every figure on the slide and on the linked issue traces to the dataset", () => {
    const issue = slides.find((s) => s.includes(report.causes[0]!.sentence.slice(0, 60)))!;
    assert.ok(issue, "the cause sentence is on the linked issue's card");
    assert.match(issue, /<p class="cause">/);
    for (const page of [slide, issue]) assert.deepEqual(checkNumbers(text(page), report.dataset).unmatched, []);
    // A gap is written in points, which the metric's plain stats couldn't back on their own.
    assert.ok(report.dataset.figures.some((f) => f.source === WIN_DRIVERS && f.kind === "points" && f.value === 61.9));
    const bare = { ...report.dataset, figures: report.dataset.figures.filter((f) => !(f.source === WIN_DRIVERS && f.kind === "points")) };
    assert.notDeepEqual(checkNumbers(text(slide), bare).unmatched, []);
  });

  test("the raw notes number it in a section of its own, and the slide and the cause point there", () => {
    assert.equal(section.title, "What separates your wins from your losses");
    assert.deepEqual(section.items.map((i) => i.id), [WIN_DRIVERS]);
    assert.ok(!noteSections(report).find((s) => s.key === "pipeline_metrics")!.items.some((i) => i.id === WIN_DRIVERS));
    const ref = section.items[0]!.ref;
    assert.match(slide, new RegExp(`data-ref>${ref.replace(".", "\\.")}<`));
    assert.match(html, new RegExp(`<p class="cause">[^<]*<span class="ref" data-ref>${ref.replace(".", "\\.")}</span></p>`));
    assert.equal(report.outputs.raw_notes.sections.find((s) => s.key === "win_drivers")!.status, "draft", "its own text passes the number check");
  });

  test("the Word notes set out the working: both sides of every factor, and what each driver is linked to", async () => {
    const xml = strFromU8(unzipSync(new Uint8Array(await renderRawNotesDocx(report, pseudonyms)))["word/document.xml"]!);
    const words = [...xml.matchAll(/<w:t(?: [^>]*)?>([^<]*)<\/w:t>/g)].map((x) => x[1]).join(" ");
    const ref = section.items[0]!.ref;
    assert.match(words, new RegExp(`${section.ref} +What separates your wins from your losses`));
    assert.match(words, new RegExp(`${ref.replace(".", "\\.")} +What separates your wins from your losses: 46.6%`));
    assert.match(words, /Factor +Deals with it +Won +Win rate +Deals without it +Won +Win rate +Gap, points +Same way in segments +Driver/);
    assert.match(words, /Went back a stage at least once +87 +0 +0% +265 +164 +61.9% +61.9 +11 of 11 +Yes/);
    assert.match(words, /Had logged activity: +Not measurable\./);
    assert.match(words, /Linked to Deals often go back a stage/);
  });

  test("left out, and listed as a gap with its reason, when it can't be measured", () => {
    const out = analyseExtracts(extracts, { calibration: { ...calibration, minSample: 200 } });
    const thin = fullReportHtml(reportOf(out, { ...calibration, minSample: 200 }), pseudonyms);
    assert.doesNotMatch(thin, /<h2>What separates your wins from your losses<\/h2>/);
    assert.match(thin, /What separates your wins from your losses<\/strong><\/td><td>No factor has enough deals on both sides/);
    assert.deepEqual(reportOf(out).causes, []);
  });
});

describe("the layout, with two issues carrying a cause", () => {
  let browser: Browser;
  before(async () => {
    browser = await launchBrowser();
  });
  after(async () => {
    await browser.close();
  });

  test("fits the deck's range, and every layout and number check passes", async () => {
    const out = analyseExtracts(quietDeals());
    const report = reportOf(out);
    assert.equal(report.causes.length, 2);
    const html = fullReportHtml(report, pseudonyms);
    assert.equal((html.match(/<p class="cause">/g) ?? []).length, 2);
    const layout = await layoutOf(browser, html, { minPages: DECK_RANGE.min, maxPages: DECK_RANGE.max, dataset: report.dataset });
    assert.deepEqual(layout.problems, []);
  });
});

describe("a set-up review", () => {
  test("never computes it: the pipeline area isn't run, and it reads deal records", async () => {
    for (const m of modulesFor("essentials", SALES_CLOUD_MODULES)) assert.ok(!(m.metrics ?? []).includes(driverMetrics), m.id);
    assert.ok(modulesFor("complete", SALES_CLOUD_MODULES).some((m) => (m.metrics ?? []).includes(driverMetrics)));
    const { output: essentials } = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(AS_OF), answers: loadAnswers(), scope: "essentials", transport: new Simulator() });
    assert.ok(!essentials!.metrics.some((m) => m.id === WIN_DRIVERS));
    const report = buildReport({ run: { ...run, scope: "essentials" }, calibration, findings: essentials!.findings, metrics: essentials!.metrics, manualSteps: essentials!.manualSteps, intake: null });
    assert.deepEqual(report.causes, []);
    assert.ok(!report.outputs.raw_notes.sections.some((s) => s.key === "win_drivers"));
  });
});
