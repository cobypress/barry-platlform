import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { AI_ACTIONS, AI_LEGACY_SHARE, AI_PLATFORM } from "../../domains/configuration/ai.ts";
import { parseSettings } from "../../extract/ai.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { executiveHtml } from "../../render/executive.ts";
import { fullReportHtml } from "../../render/full-report.ts";
import { onePagerHtml } from "../../render/one-pager.ts";
import type { Finding, Metric } from "../../schemas/index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { inSentence, readinessLine, readinessOf, type Readiness } from "../readiness.ts";
import { buildReport } from "../sections.ts";

// AI readiness (roadmap Stage 6, DECISIONS.md 236 to 244).

const { output, run, dir } = await runFromRecording({ answers: loadAnswers() });
const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
const report = buildReport({ run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps });
const verdicts = (r: Readiness): Record<string, string> => Object.fromEntries(r.pillars.map((p) => [p.key, p.verdict]));
const metric = (id: string): Metric => output!.metrics.find((m) => m.id === id)!;

/** The synthetic org's metrics, with one changed. */
function withMetric(id: string, change: (m: Extract<Metric, { status: "measured" }>) => Metric): Metric[] {
  return output!.metrics.map((m) => (m.id === id && m.status === "measured" ? change(structuredClone(m)) : m));
}

describe("each pillar's reason", () => {
  const data = (findings: Finding[]): string => readinessOf(run, findings, output!.metrics).pillars.find((p) => p.key === "data")!.reason;

  test("names the area and one of its worst issues, the title in lower case after the colon", () => {
    assert.equal(data(output!.findings), "Data quality has critical or high issues, such as: your pipeline includes deals that should already have closed.");
  });

  test("reads right when there's only one", () => {
    const named = "Your pipeline includes deals that should already have closed";
    const one = output!.findings.map((f) => ((f.severity === "critical" || f.severity === "high") && f.title !== named ? { ...f, severity: "low" as const } : f));
    assert.equal(data(one), "Data quality has a critical or high issue: your pipeline includes deals that should already have closed.");
  });

  test("keeps a name or an acronym at the start of a title as it is", () => {
    assert.equal(inSentence("Salesforce can't see who owns this"), "Salesforce can't see who owns this");
    assert.equal(inSentence("CPQ price rules conflict"), "CPQ price rules conflict");
    assert.equal(inSentence("Some leads have nobody to work them"), "some leads have nobody to work them");
  });
});

describe("the settings", () => {
  test("are read from whichever file holds each type, and anything not returned is unknown", () => {
    const xml = (type: string, field: string, value: string) => `<?xml version="1.0"?><${type} xmlns="http://soap.sforce.com/2006/04/metadata"><${field}>${value}</${field}></${type}>`;
    const settings = parseSettings({ "settings/Anything.settings": xml("KnowledgeSettings", "enableKnowledge", "true"), "x/y.settings": xml("AgentPlatformSettings", "enableAgentPlatform", "false"), "bad.settings": "<not xml" });
    assert.deepEqual(settings, { knowledge: true, generativeAi: null, agentforce: false, dataCloud: null });
  });

  test("give the synthetic org's switches, agents unreadable, and its articles counted", () => {
    const m = metric(AI_PLATFORM);
    assert.equal(m.status, "measured");
    assert.deepEqual(m.status === "measured" ? m.stats : {}, { knowledge: 1, generativeAi: 1, agentforce: 0, dataCloud: 0, articles: 30, articlesCapped: 0, articlesReady: 20 });
    const actions = metric(AI_ACTIONS);
    // One autolaunched flow with no trigger: the org's other three run on a record being saved (DECISIONS.md 302).
    assert.deepEqual(actions.status === "measured" ? [actions.stats.autolaunched, actions.stats.described, actions.stats.invocable] : [], [1, 0, 1]);
    assert.equal(metric(AI_LEGACY_SHARE).status === "measured" && (metric(AI_LEGACY_SHARE) as { value: number }).value, 30.8);
  });
});

describe("the verdicts", () => {
  test("on the synthetic org: data and access not yet, knowledge ready, the rest nearly", () => {
    assert.deepEqual(verdicts(report.readiness), { data: "not_yet", knowledge: "ready", platform: "nearly", actions: "nearly", access: "not_yet", shape: "nearly" });
    assert.equal(report.readiness.overall, "not_yet");
  });

  test("each pillar's findings are real issues", () => {
    const ids = new Map(output!.findings.map((f) => [f.id, f]));
    for (const p of report.readiness.pillars) for (const id of p.findingIds) assert.equal(ids.get(id)?.status, "issue", `${p.key}: ${id}`);
  });

  test("Knowledge off is not yet, and too few articles is nearly", () => {
    const off = readinessOf(run, output!.findings, withMetric(AI_PLATFORM, (m) => ({ ...m, stats: { ...m.stats, knowledge: 0 } })));
    assert.equal(verdicts(off).knowledge, "not_yet");
    const few = readinessOf(run, output!.findings, withMetric(AI_PLATFORM, (m) => ({ ...m, stats: { ...m.stats, articles: 5 } })));
    assert.equal(verdicts(few).knowledge, "nearly");
  });

  test("Agentforce on is ready, generative AI off is not yet, and unreadable settings are not measurable", () => {
    assert.equal(verdicts(readinessOf(run, output!.findings, withMetric(AI_PLATFORM, (m) => ({ ...m, stats: { ...m.stats, agentforce: 1 } })))).platform, "ready");
    assert.equal(verdicts(readinessOf(run, output!.findings, withMetric(AI_PLATFORM, (m) => ({ ...m, stats: { ...m.stats, generativeAi: 0 } })))).platform, "not_yet");
    const unread = output!.metrics.map((m) => (m.id === AI_PLATFORM ? { ...m, status: "not_measurable" as const, reason: "The org's settings couldn't be read: INSUFFICIENT_ACCESS" } : m)) as Metric[];
    const r = readinessOf(run, output!.findings, unread);
    assert.equal(verdicts(r).platform, "not_measurable");
    assert.equal(verdicts(r).knowledge, "not_measurable");
  });

  test("old tools: past the not-yet line is not yet, and within the ready line is ready", () => {
    assert.equal(verdicts(readinessOf(run, output!.findings, withMetric(AI_LEGACY_SHARE, (m) => ({ ...m, value: 50 })))).shape, "not_yet");
    const clean = output!.findings.filter((f) => !["aut.legacy-workflow-rules", "aut.legacy-process-builder", "aut.mixed-automation-on-object", "tech.governor-limit-risk"].includes(f.ruleId));
    assert.equal(verdicts(readinessOf(run, clean, withMetric(AI_LEGACY_SHARE, (m) => ({ ...m, value: 5 })))).shape, "ready");
  });

  test("nothing to call is not yet", () => {
    assert.equal(verdicts(readinessOf(run, output!.findings, withMetric(AI_ACTIONS, (m) => ({ ...m, value: 0 })))).actions, "not_yet");
  });

  test("a set-up review can't judge the data", () => {
    const setup = readinessOf({ scope: "essentials" }, output!.findings.filter((f) => f.domain !== "data_quality"), output!.metrics);
    assert.equal(verdicts(setup).data, "not_measurable");
  });

  test("an unclear picture has no overall verdict, and so no summary line", () => {
    const one = readinessOf(run, output!.findings.filter((f) => f.domain !== "data_quality"), output!.metrics);
    assert.equal(one.overall, null);
    assert.equal(readinessLine(one), null);
  });
});

describe("what to fix first", () => {
  test("at most five issues, the most severe first, and among equals access before data", () => {
    const { fixes } = report.readiness;
    assert.ok(fixes.length > 0 && fixes.length <= 5);
    const rank = (f: Finding): number => ["critical", "high", "medium", "low", "info"].indexOf(f.severity);
    for (let i = 1; i < fixes.length; i++) assert.ok(rank(fixes[i - 1]!) <= rank(fixes[i]!));
    const access = report.readiness.pillars.find((p) => p.key === "access")!.findingIds;
    assert.ok(access.includes(fixes[0]!.id));
  });
});

describe("in the documents", () => {
  test("one slide, no money figure, and every figure traced by the number check", () => {
    const html = fullReportHtml(report, pseudonyms);
    assert.equal((html.match(/Could an AI agent work safely here\?/g) ?? []).length, 1);
    const body = report.outputs.raw_notes.versions.find((v) => v.sectionId.endsWith(":ai_readiness"))!;
    assert.doesNotMatch(body.body, /£/);
    assert.deepEqual(body.numberCheck.unmatched, []);
    const full = report.outputs.full_health_check.versions.find((v) => v.sectionId.endsWith(":ai_readiness"))!;
    assert.deepEqual(full.numberCheck.unmatched, []);
  });

  test("the executive summary and one-pager carry one line, only when the verdict is clear", () => {
    const line = readinessLine(report.readiness)!;
    assert.equal(line, "Ready for an AI agent? Not yet: data and access need work first. The full report sets out what to fix.");
    const escaped = line.replace(/'/g, "&#39;");
    assert.ok(executiveHtml(report, pseudonyms).includes(escaped));
    assert.ok(onePagerHtml(report, pseudonyms).includes(escaped));
    const unclear = { ...report, readiness: { ...report.readiness, overall: null } };
    assert.doesNotMatch(executiveHtml(unclear, pseudonyms), /Ready for an AI agent\?/);
    assert.doesNotMatch(onePagerHtml(unclear, pseudonyms), /Ready for an AI agent\?/);
  });
});
