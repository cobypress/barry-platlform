import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { z } from "zod";
import { applyNarrative } from "../apply.ts";
import type { NarrativeClient } from "../client.ts";
import { Intake } from "../intake.ts";
import { buildPlan, domainLabel, PLAN_CAPACITY, scorecard, teamLevelFinding, teamLevelText, topIssues, verdictOf } from "../select.ts";
import { buildReport, currentBody, TOP_ISSUE_COUNT, type ReportInput } from "../sections.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { canRelease, ReportSection, SALES_CLOUD_DOMAINS, SectionVersion, type Finding, type NumberOverride, type Score } from "../../schemas/index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { readFileSync } from "node:fs";
import { join } from "node:path";

const { output, run, dir } = await runFromRecording({ answers: loadAnswers() });
const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
const base: ReportInput = { run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps };

const intake: Intake = Intake.parse({
  customerName: "Synthetic Co",
  summary: "The sales director told us the forecast is unreliable.",
  statedProblems: [{ id: "p1", statement: "We don't trust the forecast.", findingIds: [output!.findings.find((f) => f.status === "issue")!.id], assessment: "The data supports this." }],
  capturedBy: "Synthetic Consultant",
  capturedAt: "2026-10-30T09:00:00.000Z",
});

const all = (input: ReportInput = base) => {
  const report = buildReport(input);
  return { report, sections: Object.values(report.outputs).flatMap((o) => o.sections), versions: Object.values(report.outputs).flatMap((o) => o.versions) };
};

describe("the three outputs", () => {
  test("are all built, from one dataset, as valid records", () => {
    const { report, sections, versions } = all();
    assert.deepEqual(Object.keys(report.outputs), ["raw_notes", "executive_summary", "full_health_check"]);
    assert.ok(z.array(ReportSection).parse(sections).length > 15);
    assert.equal(z.array(SectionVersion).parse(versions).length, sections.length);
  });

  test("each carries the sections the brief asks for", () => {
    const { report } = all();
    const keys = (o: keyof typeof report.outputs): string[] => report.outputs[o].sections.map((s) => s.key);
    for (const key of ["intake_summary", "assumptions", "queries", "exclusions_limits", "overrides"]) assert.ok(keys("raw_notes").includes(key), key);
    assert.deepEqual(keys("executive_summary"), ["verdict", "scorecard", "top_issues", "plan_90_day"]);
    for (const key of ["stated_problems", "pipeline_deep_dive", "stated_vs_actual", "roadmap", "appendix"]) assert.ok(keys("full_health_check").includes(key), key);
    for (const domain of SALES_CLOUD_DOMAINS) assert.ok(keys("full_health_check").includes(`domain_${domain}`), domain);
  });

  test("order is contiguous within each output", () => {
    for (const built of Object.values(all().report.outputs)) assert.deepEqual(built.sections.map((s) => s.order), built.sections.map((_, i) => i));
  });

  test("the same input gives the same records", () => {
    assert.deepEqual(all().versions, all().versions);
  });

  test("every engine body passes the number check against its own scope", () => {
    for (const version of all({ ...base, intake }).versions) assert.equal(version.numberCheck.passed, true, `${version.id}: ${version.numberCheck.unmatched.join("; ")}`);
  });

  test("every section's finding ids exist", () => {
    const ids = new Set(base.findings.map((f) => f.id));
    for (const section of all().sections) for (const id of section.findingIds) assert.ok(ids.has(id), id);
  });

  test("raw notes between them cover every finding and metric", () => {
    const { sections } = all();
    const raw = sections.filter((s) => s.output === "raw_notes");
    const found = new Set(raw.filter((s) => s.key.startsWith("findings_")).flatMap((s) => s.findingIds));
    assert.equal(found.size, base.findings.length);
    assert.deepEqual(raw.find((s) => s.key === "pipeline_metrics")!.metricIds, base.metrics.map((m) => m.id));
  });
});

describe("what the engine can't know", () => {
  test("no intake means needs input, not invented problems", () => {
    const { report } = all();
    const status = (key: string): string => report.outputs.full_health_check.sections.find((s) => s.key === key)!.status;
    assert.equal(status("stated_problems"), "needs_input");
    assert.equal(status("stated_vs_actual"), "needs_input");
    assert.equal(report.outputs.raw_notes.sections.find((s) => s.key === "intake_summary")!.status, "needs_input");
  });

  test("an intake fills them in, but stated vs actual needs a consultant's link", () => {
    const withLink = all({ ...base, intake }).report.outputs.full_health_check;
    assert.equal(withLink.sections.find((s) => s.key === "stated_vs_actual")!.status, "draft");
    const noLink = Intake.parse({ ...intake, statedProblems: [{ id: "p1", statement: "We don't trust the forecast." }] });
    const without = all({ ...base, intake: noLink }).report.outputs.full_health_check;
    assert.equal(without.sections.find((s) => s.key === "stated_problems")!.status, "draft");
    assert.equal(without.sections.find((s) => s.key === "stated_vs_actual")!.status, "needs_input");
  });

  test("a domain nobody assessed makes no claim", () => {
    const { report } = all({ ...base, findings: base.findings.filter((f) => f.domain !== "forecast") });
    const body = currentBody(report.outputs.full_health_check, "domain_forecast");
    assert.match(body, /didn't assess/);
    assert.doesNotMatch(body, /\d/);
  });

  test("engine text that fails the number check goes to a person", () => {
    const f = base.findings.find((x) => x.domain === "automation" && x.status === "issue")!;
    const broken = { ...f, severity: "critical" as const, title: "The rate is 12,5%", summary: "The rate is 12,5%." };
    const { report } = all({ ...base, findings: [broken, ...base.findings.filter((x) => x.id !== f.id)] });
    const section = report.outputs.full_health_check.sections.find((s) => s.key === "domain_automation")!;
    assert.equal(section.status, "needs_input");
    assert.equal(report.outputs.full_health_check.versions.find((v) => v.id === section.currentVersionId)!.numberCheck.passed, false);
  });

  test("nothing can be released as built", () => {
    for (const built of Object.values(all({ ...base, intake }).report.outputs)) {
      const release = canRelease(built.sections);
      assert.equal(release.ok, false);
      assert.equal(built.sections.some((s) => s.status === "approved"), false);
    }
  });
});

describe("overrides", () => {
  const f = base.findings.find((x) => x.status === "issue" && typeof x.facts.rate === "number")!;
  const override: NumberOverride = { id: "o1", runId: run.id, target: { type: "finding", id: f.id, field: "facts.rate" }, previousValue: f.facts.rate!, newValue: 12.5, reason: "Two deals were test records.", overriddenBy: "Synthetic Consultant", overriddenAt: "2026-10-31T13:00:00.000Z" };

  test("appear in the raw notes with their reason", () => {
    const body = currentBody(all({ ...base, overrides: [override] }).report.outputs.raw_notes, "overrides");
    assert.match(body, /Two deals were test records/);
    assert.match(body, /Synthetic Consultant/);
    assert.match(body, /to 12\.5/);
  });

  test("change what reports show, and leave the record alone", () => {
    const { report } = all({ ...base, overrides: [override] });
    const shown = report.teamFindings.find((x) => x.id === f.id)!;
    assert.equal(shown.facts.rate, 12.5);
    assert.ok(shown.summary.includes("12.5%"));
    assert.ok(!shown.summary.includes(`${override.previousValue}%`));
    assert.equal(base.findings.find((x) => x.id === f.id)!.facts.rate, override.previousValue);
  });

  test("reach a sized value and a metric", () => {
    const sized = base.findings.find((x) => x.value.kind !== "not_sized")!;
    const amount = sized.value.kind === "not_sized" ? 0 : sized.value.amount;
    const onValue: NumberOverride = { ...override, target: { type: "finding", id: sized.id, field: "value.amount" }, previousValue: amount, newValue: 1000 };
    const onMetric: NumberOverride = { ...override, target: { type: "metric", id: "historic.win_rate", field: "value" }, previousValue: 35, newValue: 33 };
    const { report } = all({ ...base, overrides: [onValue, onMetric] });
    const value = report.teamFindings.find((x) => x.id === sized.id)!.value;
    assert.equal(value.kind !== "not_sized" && value.amount, 1000);
    const m = report.metrics.find((x) => x.id === "historic.win_rate")!;
    assert.equal(m.status === "measured" && m.value, 33);
  });

  test("say so when there are none", () => {
    assert.match(currentBody(all().report.outputs.raw_notes, "overrides"), /No figures were overridden/);
  });
});

describe("team level", () => {
  test("no user token survives in an executive output", () => {
    const { report } = all();
    for (const key of ["executive_summary", "full_health_check"] as const) {
      for (const version of report.outputs[key].versions) assert.doesNotMatch(version.body, /\[\[user:/, version.id);
    }
    for (const f of report.teamFindings) assert.doesNotMatch(`${f.title} ${f.summary} ${f.recommendation} ${f.subjects.map((s) => s.label).join()}`, /\[\[user:/);
  });

  test("a brief for Claude holds no person either", () => {
    for (const [id, brief] of all().report.briefs) assert.doesNotMatch(JSON.stringify(brief.sources.filter((s) => s.kind === "finding")), /\[\[user:/, id);
  });

  test("reads as a sentence", () => {
    assert.equal(teamLevelText("[[user:U0004]] made 90% of changes."), "A single person made 90% of changes.");
    assert.equal(teamLevelText("Win rate for rep [[user:U0014]] is low."), "Win rate for a single rep is low.");
    assert.equal(teamLevelText("Rep [[user:U0014]] is low."), "A single rep is low.");
    assert.equal(teamLevelText("Done. [[user:U0014]] made it."), "Done. A single person made it.");
  });

  test("raw notes keep the token, because they're the customer admin's working record", () => {
    assert.ok(base.findings.some((f) => /\[\[user:/.test(f.summary)));
  });
});

describe("ranking", () => {
  const team = base.findings.map(teamLevelFinding);

  test("top issues come by severity, and sized ones first within a severity", () => {
    const top = topIssues(base.findings, TOP_ISSUE_COUNT);
    assert.equal(top.length, TOP_ISSUE_COUNT);
    assert.ok(top.every((f) => f.severity === "high"));
    assert.equal(top[0]!.value.kind, "revenue");
  });

  test("a finding about one person isn't a top issue, before or after team level", () => {
    for (const f of topIssues(base.findings, 50)) assert.doesNotMatch(f.title, /\[\[user:/);
    assert.ok(team.some((f) => /a single rep/.test(f.title)), "the synthetic run has one");
    for (const f of topIssues(team, 50)) assert.doesNotMatch(f.title, /a single/i);
  });

  test("a plan has three phases, with no repeated action", () => {
    const { phases: plan, later } = buildPlan(team);
    assert.deepEqual(plan.map((p) => [p.from, p.to]), [[1, 30], [31, 60], [61, 90]]);
    const actions = [...plan.flatMap((p) => p.findings), ...later].map((f) => f.recommendation);
    assert.equal(new Set(actions).size, actions.length);
    assert.ok(actions.length > 10);
  });

  test("each phase holds what a team can take on, with urgent work first", () => {
    const { phases, later } = buildPlan(team);
    phases.forEach((p, i) => assert.ok(p.findings.length <= PLAN_CAPACITY[i]!, `${p.title}: ${p.findings.length}`));
    assert.ok(later.length > 0, "the synthetic run has more actions than a 90-day plan holds");
    const urgent = (f: Finding): boolean => f.severity === "critical" || f.severity === "high";
    assert.ok(phases[0]!.findings.every((f) => urgent(f) || (f.severity === "medium" && f.effort.size === "XS")), "the first month is urgent work or the quickest medium fixes");
    assert.ok(phases[0]!.findings.length === PLAN_CAPACITY[0], "the first month is used");
    // An urgent action only misses the plan when the plan is full of urgent work.
    for (const f of later.filter(urgent)) assert.ok(phases.every((p, i) => p.findings.length === PLAN_CAPACITY[i] && p.findings.every(urgent)), f.title);
    assert.ok(later.every((f) => f.severity !== "info"));
  });

  test("large work and low-severity tidying never take the first month", () => {
    const { phases } = buildPlan(team);
    for (const f of phases[0]!.findings) assert.ok(["XS", "S", "M"].includes(f.effort.size) && f.severity !== "low", f.title);
  });

  test("the verdict follows the worst severity", () => {
    const issue = base.findings.find((f) => f.status === "issue")!;
    const at = (severity: Finding["severity"]): string => verdictOf([{ ...issue, severity }]).headline;
    assert.equal(at("critical"), "Urgent action needed");
    assert.equal(at("high"), "Significant issues to fix");
    assert.equal(at("low"), "Sound, with issues to fix");
    assert.equal(verdictOf([]).headline, "No material issues found");
  });
});

describe("the scorecard", () => {
  const score: Score = { status: "measured", domain: "automation", score: 62, ruleSetVersion: "x", weights: {}, findingIds: [] };
  const gap: Score = { status: "not_measurable", domain: "forecast", ruleSetVersion: "x", reason: "Forecasting isn't in use." };

  test("never turns a gap into a zero", () => {
    const rows = scorecard(base.findings.filter((f) => f.domain !== "forecast" && f.domain !== "security"), [score, gap]);
    assert.equal(rows.find((r) => r.domain === "automation")!.score, 62);
    assert.equal(rows.find((r) => r.domain === "forecast")!.score, null);
    assert.equal(rows.find((r) => r.domain === "forecast")!.note, "Forecasting isn't in use.");
    assert.equal(rows.find((r) => r.domain === "configuration")!.note, "Not scored in this run");
    assert.equal(rows.find((r) => r.domain === "security")!.note, "Not assessed in this run");
  });

  test("a plug-in domain gets a readable label", () => {
    assert.equal(domainLabel("service_queues"), "Service queues");
  });
});

describe("applying narrative", () => {
  const client = (text: (prompt: string) => string): NarrativeClient => ({
    complete: async ({ prompt, model }) => ({ stopReason: "end_turn", text: text(prompt), model }),
  });

  test("an accepted draft becomes the current version, and v1 stays", async () => {
    const report = buildReport(base);
    const rejections = await applyNarrative(report, client(() => JSON.stringify({ paragraphs: [{ text: "We looked closely at this area.", sources: [] }] })), pseudonyms, new Date("2026-10-31T13:00:00.000Z"));
    assert.deepEqual(rejections, []);
    const verdict = report.outputs.executive_summary.sections.find((s) => s.key === "verdict")!;
    assert.equal(verdict.currentVersionId, `${verdict.id}:v2`);
    assert.equal(currentBody(report.outputs.executive_summary, "verdict"), "We looked closely at this area.");
    const history = report.outputs.executive_summary.versions.filter((v) => v.sectionId === verdict.id);
    assert.deepEqual(history.map((v) => v.source), ["engine", "claude_draft"]);
    assert.equal(history[1]!.changedBy, "claude-opus-5-5");
    assert.equal(verdict.status, "draft", "a draft from Claude is still only a draft");
  });

  test("a rejected draft keeps the engine's text and goes to a person", async () => {
    const report = buildReport(base);
    const before = currentBody(report.outputs.executive_summary, "verdict");
    const rejections = await applyNarrative(report, client(() => JSON.stringify({ paragraphs: [{ text: "You could save £9,999,999.", sources: [] }] })), pseudonyms);
    assert.ok(rejections.length > 0);
    assert.equal(currentBody(report.outputs.executive_summary, "verdict"), before);
    assert.equal(report.outputs.executive_summary.sections.find((s) => s.key === "verdict")!.status, "needs_input");
    assert.match(rejections[0]!.reasons.join(), /9,999,999/);
  });

  test("sections with no brief are never sent", async () => {
    const report = buildReport(base);
    const titles: string[] = [];
    await applyNarrative(report, { complete: async ({ prompt, model }) => (titles.push(prompt.split("\n")[0]!), { stopReason: "end_turn", text: '{"paragraphs":[{"text":"Fine.","sources":[]}]}', model }) }, pseudonyms);
    assert.ok(titles.includes("Section: Verdict"));
    assert.ok(!titles.some((t) => /Assumptions|Queries|Overrides|Intake/.test(t)), "raw notes stay engine text");
  });
});
