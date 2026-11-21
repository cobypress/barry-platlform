import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { cpSync, readFileSync, writeFileSync } from "node:fs";
import { basename, join } from "node:path";
import { buildDigest } from "../../digest/build.ts";
import { ALL_MODULES } from "../../domains/index.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { INTERVIEW_VERSION } from "../../interviews/questions.ts";
import { INTERVIEWS_FILE, loadRun, SURVEY_FILE } from "../../render/render-run.ts";
import type { Run } from "../../schemas/index.ts";
import { runFromRecording, tempDir } from "../../testing/harness.ts";
import { syntheticInterviews } from "../../testing/interviews.ts";
import { EARLIER_AS_OF, plantRecheck, RECHECK_AS_OF } from "../../testing/recheck.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { SURVEY_METRIC_IDS } from "../../survey/summary.ts";
import { syntheticSurvey } from "../../testing/survey.ts";
import { AS_OF, CONNECTED_USER } from "../../testing/synthetic-org.ts";
import { TEAM_TIME_LINE } from "../../value/model.ts";
import { buildPlan } from "../select.ts";
import { buildReport } from "../sections.ts";

// Decision 643: nothing the engine stores or serves cites a finding or a
// metric that isn't in the same run. Every shape of run the engine makes,
// with and without interviews, is built here and every id checked against
// the run's own findings.json and metrics.json, as the console's reader
// checks them (and as the worker's database rows hold them).

const readJson = <T>(dir: string, file: string): T => JSON.parse(readFileSync(join(dir, file), "utf8")) as T;
const editRun = (dir: string, change: (run: Run) => Run): void => writeFileSync(join(dir, "run.json"), JSON.stringify(change(readJson<Run>(dir, "run.json"))));

const { dir: complete } = await runFromRecording({ answers: loadAnswers(), modules: ALL_MODULES });
const { dir: essentials } = await runHealthCheck({ provider: syntheticProvider, connectedUser: CONNECTED_USER, calibration: loadCalibration(), modules: ALL_MODULES, outDir: tempDir(), now: new Date(AS_OF), answers: loadAnswers(), scope: "essentials", transport: new Simulator() });

/**
 * A copy of a run as a re-check (or a pulse) of its own earlier copy, which
 * sits beside it. The complete one through the planted re-check, whose earlier
 * run differs; a set-up only run has none of the findings that one plants on.
 */
function recheckOf(dir: string, product: Run["product"]): string {
  const to = join(tempDir(), `${basename(dir)}-again`);
  if (readJson<Run>(dir, "run.json").scope === "essentials") {
    const earlierId = `${basename(dir)}-earlier`;
    cpSync(dir, join(to, "..", earlierId), { recursive: true });
    editRun(join(to, "..", earlierId), (run) => ({ ...run, id: earlierId, asOf: EARLIER_AS_OF, baselineRunId: null }));
    cpSync(dir, to, { recursive: true });
    editRun(to, (run) => ({ ...run, baselineRunId: earlierId, asOf: RECHECK_AS_OF }));
  } else plantRecheck(dir, to);
  editRun(to, (run) => ({ ...run, product }));
  return to;
}

/** A copy beside the run, so a re-check's earlier run is still beside it, with one more file. */
function withFile(dir: string, suffix: string, file: string, value: unknown): string {
  const to = `${dir}-${suffix}`;
  cpSync(dir, to, { recursive: true });
  writeFileSync(join(to, file), JSON.stringify(value));
  return to;
}
const withInterviews = (dir: string): string => withFile(dir, "interviews", INTERVIEWS_FILE, { version: INTERVIEW_VERSION, interviews: syntheticInterviews() });
const withSurvey = (dir: string): string => withFile(dir, "survey", SURVEY_FILE, syntheticSurvey(12));

const SHAPES: Record<string, string> = {
  complete,
  "set-up only": essentials,
  "complete re-check": recheckOf(complete, "barometer"),
  "complete pulse": recheckOf(complete, "pulse"),
  "set-up only pulse": recheckOf(essentials, "pulse"),
};
const RUNS: Array<[string, string]> = Object.entries(SHAPES).flatMap(([name, dir]): Array<[string, string]> => [[name, dir], [`${name} with interviews`, withInterviews(dir)], [`${name} with a survey`, withSurvey(dir)]]);

/** Every id cited that isn't in the run's own files, as "where: id". */
function dangling(dir: string): string[] {
  const findings = new Set(readJson<Array<{ id: string }>>(dir, "findings.json").map((f) => f.id));
  const metrics = new Set(readJson<Array<{ id: string }>>(dir, "metrics.json").map((m) => m.id));
  const steps = new Set(readJson<Array<{ id: string }>>(dir, "manual-steps.json").map((s) => s.id));
  const loaded = loadRun(dir);
  const report = buildReport(loaded.input);
  const out: string[] = [];
  const check = (where: string, ids: Iterable<string>, known: Set<string>): void => {
    for (const id of ids) if (!known.has(id)) out.push(`${where}: ${id}`);
  };
  for (const output of Object.values(report.outputs)) {
    for (const s of output.sections) {
      check(`${s.id} findingIds`, s.findingIds, findings);
      check(`${s.id} metricIds`, s.metricIds, metrics);
      check(`${s.id} manualStepIds`, s.manualStepIds, steps);
    }
  }
  const plan = buildPlan(report.teamFindings, report.added);
  check("plan", [...plan.quickWins, ...plan.phases.flatMap((p) => p.findings), ...plan.later].map((f) => f.id), findings);
  check("value model", [...report.value.groups.flatMap((g) => g.lines.map((l) => l.findingId)), ...report.value.left.map((l) => l.findingId)].filter((id) => id !== TEAM_TIME_LINE), findings);
  if (loaded.input.run.product !== "scan") {
    const digest = buildDigest({ report, pseudonyms: loaded.pseudonyms, inventory: null, madeAt: "2026-11-21T09:00:00.000Z" });
    check("digest findings", digest.findings.map((f) => f.id), findings);
    check("digest metrics", digest.metrics.map((m) => m.id), metrics);
    check("digest areas", digest.areas.flatMap((a) => (a.status === "measured" ? a.findingIds : [])), findings);
    check("digest plan", [...digest.plan.quickWins, ...digest.plan.later, ...digest.plan.phases.flatMap((p) => p.findingIds), ...Object.entries(digest.plan.alsoFor).flat(2), ...(digest.plan.hoursTotal ? [...digest.plan.hoursTotal.findingIds, ...digest.plan.hoursTotal.partialFindingIds, ...digest.plan.hoursTotal.withoutHoursFindingIds] : [])], findings);
    check("digest value lines", digest.value.lines.map((l) => l.id).filter((id) => id !== TEAM_TIME_LINE), findings);
    check("digest value totals", [digest.value.total, ...Object.values(digest.value.byKind), digest.value.exposure].flatMap((t) => t.lineIds).filter((id) => id !== TEAM_TIME_LINE), findings);
    check("digest value left out", digest.value.leftOut, findings);
  }
  return out;
}

// The team survey's figures are built from the survey when the sections are,
// and are never in metrics.json or the worker's stored metrics, though its
// sections and the digest cite them. Known and not settled here (decision 643's
// report): each fix moves either what the sections may quote or what the
// console stores. Pinned, so no other id can hide behind it.
const SURVEY_GAP = (dir: string): string[] => dangling(dir).filter((d) => !SURVEY_METRIC_IDS.some((id) => d.endsWith(`: ${id}`)));

describe("every id a run cites is in that run (decision 643)", () => {
  for (const [name, dir] of RUNS) {
    test(name, () => {
      if (name.endsWith("with a survey")) {
        assert.ok(dangling(dir).length > 0, "the survey's known gap is still there: when it's settled, check every survey run in full");
        assert.deepEqual(SURVEY_GAP(dir), []);
      } else assert.deepEqual(dangling(dir), []);
    });
  }
});

describe("an interview reading cites a metric only when its run measured it (decision 643)", () => {
  const cited = (dir: string): string[] => {
    const report = buildReport(loadRun(dir).input);
    return [...new Set(Object.values(report.outputs).flatMap((o) => o.sections.filter((s) => s.key === "interviews").flatMap((s) => s.metricIds)))].sort();
  };
  const HISTORIC = ["historic.close_date_reliability", "historic.sales_cycle_won", "historic.win_rate"];

  test("a set-up only run with interviews cites no historic metric, and its readings say we couldn't measure them, with no figure", () => {
    const dir = withInterviews(essentials);
    assert.deepEqual(cited(dir).filter((id) => id.startsWith("historic.")), []);
    const view = buildReport(loadRun(dir).input).interviews!;
    const unmeasured = view.themes.filter((t) => ["win_rate", "forecast_hit", "sales_cycle"].includes(t.question));
    assert.equal(unmeasured.length, 3, "the sales director's three figures");
    for (const t of unmeasured) {
      assert.deepEqual([t.metricId, t.reading, t.found, t.why], [null, "cant_tell", "We couldn't measure this in your org.", "There's no figure from the data to set it against."]);
      assert.doesNotMatch(`${t.found} ${t.why}`, /\d/);
    }
  });

  test("a complete run with interviews still cites the three, which it measured", () => {
    const dir = withInterviews(complete);
    const metrics = readJson<Array<{ id: string; status: string }>>(dir, "metrics.json");
    for (const id of HISTORIC) assert.equal(metrics.find((m) => m.id === id)?.status, "measured", id);
    assert.deepEqual(cited(dir).filter((id) => id.startsWith("historic.")), HISTORIC);
  });
});
