import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { digestDataset } from "../checked.ts";
import { buildDigest } from "../build.ts";
import { strictShadow, NAMED_VALUES_FILE, type StrictShadow } from "../strict.ts";
import { NOT_FIGURES_FILE } from "../../engine/named.ts";
import { automaticCalibration } from "../../connect/auto-calibration.ts";
import { parseStageAnswer } from "../../connect/stage-outcomes.ts";
import { buildReport } from "../../narrative/sections.ts";
import { loadRun } from "../../render/render-run.ts";
import { AutomaticReplay, automaticSyntheticCalibration, stagesOf } from "../../testing/automatic.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { Simulator } from "../../testing/simulator.ts";
import { deals, open, orgWith, REWORK, SKIPS, USUAL } from "./paths-orgs.ts";
import { journeyOrg, journeyRecords, richSpecs } from "../../testing/lead-journey-org.ts";
import { caseOrg, caseRecords, richCases, straightCases, stuckCases } from "../../testing/case-paths-org.ts";
import { ALL_MODULES } from "../../domains/index.ts";

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
  // And where its number words claim nothing (word(), DECISIONS.md 773), as the worker reads them.
  const words = existsSync(join(dir, NOT_FIGURES_FILE)) ? JSON.parse(readFileSync(join(dir, NOT_FIGURES_FILE), "utf8")) : {};
  return strictShadow(digest, report, digestDataset(report, loaded.pseudonyms), named, undefined, undefined, words);
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
    // An unscored area says why from its own checks (HC_AREA_REASONS, DECISIONS.md 769): its reason's figures are held too.
    areaReasons: { calibration: auto, answers: [], reviewMode: "automatic", transport: new Simulator({ readOnly: true }), areaReasons: true },
    areaReasonsConsultant: { areaReasons: true },
    // The lead journey on (HC_LEAD_JOURNEY, DECISIONS.md 770): the synthetic org and constructed lead orgs, so its sentences are counted.
    "leadJourney/synthetic": { calibration: auto, answers: [], reviewMode: "automatic", transport: new Simulator(), leadJourney: true },
    "leadJourney/rich": { calibration: auto, answers: [], reviewMode: "automatic", transport: new Simulator({ records: journeyOrg(journeyRecords(richSpecs())) }), leadJourney: true },
    "leadJourney/rich-consultant": { transport: new Simulator({ records: journeyOrg(journeyRecords(richSpecs())) }), leadJourney: true },
    "leadJourney/untracked": { calibration: auto, answers: [], reviewMode: "automatic", transport: new Simulator({ records: journeyOrg(journeyRecords(richSpecs(), () => [])) }), leadJourney: true },
    "leadJourney/late-history": {
      calibration: auto,
      answers: [],
      reviewMode: "automatic",
      transport: new Simulator({ records: journeyOrg(journeyRecords(richSpecs(), (leads) => leads.filter((l) => l.CreatedDate >= "2026-01-01").map((l, i) => ({ Id: `017${String(950_000 + i).padStart(12, "0")}AAA`, LeadId: l.Id, CreatedDate: l.CreatedDate })))) }),
      leadJourney: true,
    },
    "leadJourney/quick": { calibration: auto, answers: [], reviewMode: "automatic", transport: new Simulator({ records: journeyOrg(journeyRecords(richSpecs().map((x) => (x.outcome ? { ...x, convertedAfter: 1, closedAfter: 5 } : x)))) }), leadJourney: true },
    // The case status paths on (HC_CASE_PATHS, DECISIONS.md 776): consultant-led runs with Service Cloud, the only runs it reaches.
    "casePaths/synthetic": { modules: ALL_MODULES, casePaths: true },
    "casePaths/rich": { modules: ALL_MODULES, transport: new Simulator({ records: caseOrg(caseRecords(richCases())) }), casePaths: true },
    "casePaths/straight": { modules: ALL_MODULES, transport: new Simulator({ records: caseOrg(caseRecords(straightCases())) }), casePaths: true },
    "casePaths/stuck": { modules: ALL_MODULES, transport: new Simulator({ records: caseOrg(caseRecords(stuckCases({ backlog: true, waiting: true }))) }), casePaths: true },
    "casePaths/steady": { modules: ALL_MODULES, transport: new Simulator({ records: caseOrg(caseRecords(stuckCases())) }), casePaths: true },
    "casePaths/untracked": { modules: ALL_MODULES, transport: new Simulator({ records: caseOrg(caseRecords(richCases().map((x) => ({ ...x, noHistory: true })))) }), casePaths: true },
  };
  // The case status paths' runs count its own rules only, on the same terms (DECISIONS.md 776).
  const CASES = new Set(["svc.reopened-time", "svc.closed-straight"]);
  // The lead journey's runs count the journey's own rules only: on constructed lead orgs the base rules reach sentences the
  // synthetic org doesn't, with or without the switch, and those aren't this switch's to hold (DECISIONS.md 770).
  const JOURNEY = new Set(["lead.days_to_won", "lead.won_lead_rate", "lead.source-win-rate"]);
  for (const [name, options] of Object.entries(synthetic)) {
    const shadow = await shadowOf((await runFromRecording(options)).dir);
    out[name] = name.startsWith("leadJourney/") ? { ...shadow, figures: shadow.figures.filter((f) => JOURNEY.has(f.ruleId)) } : name.startsWith("casePaths/") ? { ...shadow, figures: shadow.figures.filter((f) => CASES.has(f.ruleId)) } : shadow;
  }
  const first = await runFromRecording();
  const stages = stagesOf(first.dir);
  const cal = (answer?: Record<string, string>) => {
    const parsed = answer ? parseStageAnswer(answer) : null;
    if (parsed === "malformed") throw new Error("The stage answer is malformed.");
    const built = automaticCalibration(stages, { orgId: first.run.orgId, at: "2026-10-31T09:00:00.000Z", currency: "GBP", stageAnswer: parsed });
    if (!built.ok) throw new Error(built.reason);
    return built.calibration;
  };
  const ALL = { "Closed Lost": "lost", "Closed Lost - No Budget": "lost", "Closed No Decision": "no_decision" };
  out["stages/none"] = await shadowOf((await runFromRecording({ calibration: cal(), reviewMode: "automatic", transport: new AutomaticReplay() })).dir);
  out["stages/answered"] = await shadowOf((await runFromRecording({ calibration: cal(ALL), reviewMode: "automatic", transport: new AutomaticReplay() })).dir);
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

  test("and none is lower: the snapshot is exact, so no slack builds up", () => {
    const nowAt = new Map(now.map((r) => [`${r.ruleId}\u0000${r.kind}`, r.count]));
    const fallen = held.filter((r) => (nowAt.get(`${r.ruleId}\u0000${r.kind}`) ?? 0) < r.count).map((r) => `${r.ruleId} ${r.kind}: ${nowAt.get(`${r.ruleId}\u0000${r.kind}`) ?? 0}, held at ${r.count}`);
    assert.deepEqual(fallen, [], "These fell: rewrite the snapshot with STRICT_LINT_WRITE=1 node --test src/digest/__tests__/strict-lint.test.ts, and say why in decision 768.");
  });

  test("the snapshot names rules, kinds and counts only: no org word, no figure", () => {
    for (const r of held) {
      assert.deepEqual(Object.keys(r).sort(), ["count", "kind", "ruleId"]);
      assert.match(r.ruleId, /^[a-z]+(?:[.:][a-z0-9_-]+)+$/);
      assert.ok(Number.isInteger(r.count) && r.count > 0);
    }
  });
});
