import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { RuleContext } from "../../../engine/rule.ts";
import type { Extractor } from "../../../extract/types.ts";
import type { MetadataBundle } from "../../../extract/metadata-parse.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../../privacy/pseudonymiser.ts";
import type { Metric } from "../../../schemas/index.ts";
import { runFromRecording } from "../../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../../testing/replay.ts";
import { AI_LEGACY_SHARE, aiMetrics } from "../ai.ts";

// What AI readiness counts when part of its source can't be read (night 3 review).

const { run, dir } = await runFromRecording({ answers: loadAnswers() });
const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
const read = <T>(id: string): T => JSON.parse(readFileSync(join(dir, "checkpoints", `${id}.json`), "utf8")) as T;

function metricsWith(overrides: Record<string, unknown> = {}): Metric[] {
  const now = new Date(run.asOf);
  const ctx: Omit<RuleContext, "metrics"> = {
    calibration: loadCalibration(),
    now,
    window: run.analysisWindow,
    pseudonyms,
    data: <T>(extractor: Extractor<T>): T => (overrides[extractor.id] ?? read(extractor.id)) as T,
    ev: (source, input = {}) => ({ ...source, count: input.count ?? null, dateRange: input.dateRange ?? null, exclusions: input.exclusions ?? [], note: input.note, capturedAt: now.toISOString() }) as never,
    manual: () => {
      throw new Error("no manual steps here");
    },
    userLabel: () => "a user",
  };
  return aiMetrics(ctx);
}
const find = (metrics: Metric[], id: string) => metrics.find((m) => m.id === id)!;
type Sourced<T> = { status: "ok"; data: T; source: unknown } | { status: "unavailable"; reason: string; source: unknown };

describe("the share of automation still in old tools", () => {
  test("isn't measurable when Workflow Rules couldn't be read, rather than counting them as none", () => {
    const metadata = read<Sourced<MetadataBundle>>("metadata");
    assert.equal(metadata.status, "ok");
    const unread = { ...metadata, data: { ...(metadata as { data: MetadataBundle }).data, workflows: [], unread: { workflowRules: "the Tooling route can't read Workflow Rules" } } };
    const share = find(metricsWith({ metadata: unread }), AI_LEGACY_SHARE);
    assert.equal(share.status, "not_measurable");
    assert.match(share.status === "not_measurable" ? share.reason : "", /Workflow Rules couldn't be read/);
  });

  test("isn't measurable when Apex triggers couldn't be read, rather than counting them as none", () => {
    const apex = read<{ classes: unknown; triggers: unknown }>("apex");
    const share = find(metricsWith({ apex: { ...apex, triggers: { status: "unavailable", reason: "ApexTrigger couldn't be queried", source: { kind: "soql", api: "tooling", query: "SELECT" } } } }), AI_LEGACY_SHARE);
    assert.equal(share.status, "not_measurable");
    assert.match(share.status === "not_measurable" ? share.reason : "", /Apex triggers couldn't be read/);
  });
});
