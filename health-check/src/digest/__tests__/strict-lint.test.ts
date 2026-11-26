import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { digestDataset } from "../checked.ts";
import { buildDigest } from "../build.ts";
import { strictShadow, NAMED_VALUES_FILE, type StrictShadow } from "../strict.ts";
import { automaticCalibration } from "../../connect/auto-calibration.ts";
import { parseStageAnswer } from "../../connect/stage-outcomes.ts";
import { buildReport } from "../../narrative/sections.ts";
import { loadRun } from "../../render/render-run.ts";
import { AutomaticReplay, automaticSyntheticCalibration, stagesOf } from "../../testing/automatic.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { Simulator } from "../../testing/simulator.ts";
import { deals, open, orgWith, REWORK, SKIPS, USUAL } from "./paths-orgs.ts";

// Step 1 of the strict check, held from here on (DECISIONS.md 768, KAN-102):
// every rule's figures on the synthetic org's runs and the constructed
// path-insight orgs that the strict per-finding check can't trace to the
// finding's own computed values, counted by rule and kind of text. The snapshot
// holds the counts as they stand; a rule whose count rises, or a rule or kind
// that wasn't there, fails, so the number can only fall. When it falls, lower
// the snapshot: `STRICT_LINT_WRITE=1 node --test` writes it. The readiness
// scenarios' constructed findings are left out: their rules aren't real.

const SNAPSHOT = join(dirname(fileURLToPath(import.meta.url)), "fixtures", "strict-untraced.json");
type Row = { ruleId: string; kind: string; count: number };

const MADE = "2026-11-25T09:00:00.000Z";
const auto = await automaticSyntheticCalibration();

async function shadowOf(dir: string, loaded = loadRun(dir)): Promise<StrictShadow> {
  const report = buildReport(loaded.input);
  const digest = buildDigest({ report, pseudonyms: loaded.pseudonyms, inventory: null, madeAt: MADE });
  const named = existsSync(join(dir, NAMED_VALUES_FILE)) ? JSON.parse(readFileSync(join(dir, NAMED_VALUES_FILE), "utf8")) : {};
  return strictShadow(digest, report, digestDataset(report, loaded.pseudonyms), named);
}

async function runs(): Promise<Record<string, StrictShadow>> {
  const out: Record<string, StrictShadow> = {};
  const PE = "Professional Edition";
  const synthetic: Record<string, Parameters<typeof runFromRecording>[0]> = {
    consultant: {},
    setup: { scope: "essentials" },
    automatic: { calibration: auto, answers: [], reviewMode: "automatic", transport: new Simulator() },
    readOnly: { calibration: auto, answers: [], reviewMode: "automatic", transport: new Simulator({ readOnly: true }) },
    professional: { calibration: auto, answers: [], reviewMode: "automatic", transport: new Simulator({ organizationType: PE }) },
    gatedPulse: { calibration: auto, answers: [], product: "pulse", transport: new Simulator() },
  };
  for (const [name, options] of Object.entries(synthetic)) out[name] = await shadowOf((await runFromRecording(options)).dir);
  const first = await runFromRecording();
  const stages = stagesOf(first.dir);
  const cal = (o: Record<string, unknown> = {}) => automaticCalibration(stages, { orgId: first.run.orgId, at: "2026-10-31T09:00:00.000Z", currency: "GBP", ...o }).calibration;
  const ALL = { "Closed Lost": "lost", "Closed Lost - No Budget": "lost", "Closed No Decision": "no_decision" };
  out["stages/none"] = await shadowOf((await runFromRecording({ calibration: cal(), reviewMode: "automatic", transport: new AutomaticReplay() })).dir);
  out["stages/answered"] = await shadowOf((await runFromRecording({ calibration: cal({ stageAnswer: parseStageAnswer(ALL) }), reviewMode: "automatic", transport: new AutomaticReplay() })).dir);
  const through = deals(60, USUAL, 30, 60);
  const skip = [...through, ...deals(30, SKIPS, 25, 60)];
  const orgs = {
    "paths/skip": skip,
    "paths/back": [...deals(60, USUAL, 45, 60), ...deals(30, REWORK, 12, 60), ...open(1, USUAL)],
    "paths/both": [...deals(120, USUAL, 75, 60), ...deals(30, SKIPS, 25, 60), ...deals(30, REWORK, 6, 60)],
    "paths/few-closed": deals(9, USUAL, 5, 60),
    "paths/open-kind": [...skip, ...open(6, SKIPS)],
  };
  for (const [name, specs] of Object.entries(orgs)) {
    const org = await orgWith(specs);
    out[name] = await shadowOf(org.dir, org);
  }
  return out;
}

/** The most untraced figures any run gives a rule's kind of text: the count held. */
function rowsOf(shadows: Record<string, StrictShadow>): Row[] {
  const most = new Map<string, Row>();
  for (const s of Object.values(shadows)) {
    const here = new Map<string, number>();
    for (const f of s.figures) here.set(`${f.ruleId}\u0000${f.kind}`, (here.get(`${f.ruleId}\u0000${f.kind}`) ?? 0) + 1);
    for (const [key, count] of here) {
      const [ruleId, kind] = key.split("\u0000") as [string, string];
      if ((most.get(key)?.count ?? 0) < count) most.set(key, { ruleId, kind, count });
    }
  }
  return [...most.values()].sort((a, b) => b.count - a.count || a.ruleId.localeCompare(b.ruleId) || a.kind.localeCompare(b.kind));
}

describe("the strict check's untraced figures can only fall (DECISIONS.md 768)", async () => {
  const now = rowsOf(await runs());
  if (process.env.STRICT_LINT_WRITE === "1") writeFileSync(SNAPSHOT, `[\n${now.map((r) => JSON.stringify(r)).join(",\n")}\n]\n`);
  const held = JSON.parse(readFileSync(SNAPSHOT, "utf8")) as Row[];
  const heldAt = new Map(held.map((r) => [`${r.ruleId}\u0000${r.kind}`, r.count]));

  test("no rule's untraced count is higher than the snapshot holds, and none is new", () => {
    const risen = now.filter((r) => (heldAt.get(`${r.ruleId}\u0000${r.kind}`) ?? 0) < r.count).map((r) => `${r.ruleId} ${r.kind}: ${r.count}, held at ${heldAt.get(`${r.ruleId}\u0000${r.kind}`) ?? "none"}`);
    assert.deepEqual(risen, [], "Name the value each new figure writes (named() beside its sentence, or a fact) rather than raising the snapshot.");
  });

  test("the snapshot names rules, kinds and counts only: no org word, no figure", () => {
    for (const r of held) {
      assert.deepEqual(Object.keys(r).sort(), ["count", "kind", "ruleId"]);
      assert.match(r.ruleId, /^[a-z]+(?:[.:][a-z0-9_-]+)+$/);
      assert.ok(Number.isInteger(r.count) && r.count > 0);
    }
  });
});
