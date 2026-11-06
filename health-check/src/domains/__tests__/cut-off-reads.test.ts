import { test } from "node:test";
import assert from "node:assert/strict";
import { runHealthCheck } from "../../engine/run.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { buildScaleOrg, FIVE_HUNDRED_USERS, scaled } from "../../testing/scale-org.ts";
import { Simulator } from "../../testing/simulator.ts";
import { AS_OF } from "../../testing/synthetic-org.ts";
import { ALL_MODULES } from "../index.ts";

// Hard rule 5 at scale (docs/overnight/night-3/scale.md): a read that stopped
// at its row cap is a part, so no check may pass on it. It may say "at least",
// or "the first N of M", or not measurable. Each capped read in the recorded
// run is marked as cut off in turn, and every check is run again: on the
// synthetic org, full of traps, and on a small generated one, where most
// checks pass.

/** Reads with a row cap: where a big org's records stop being read. COUNT() reads never are. */
const CAPPED = /\bFROM (LoginHistory|SetupAuditTrail|SBQQ__Quote__c|ProcessInstance|ForecastingItem|ForecastingFact|AsyncApexJob|KnowledgeArticleVersion|Case|CaseHistory|CaseMilestone|EmailMessage|AgentWork|CaseArticle|Task|Event|Lead|LeadHistory|CampaignMember|Contact|Account|Opportunity)\b/;

/** Reads made in full, with no cap, so they can't stop part-way. */
const UNCAPPED = new Set(["opportunities/opportunities", "opportunities/history", "next-steps/"]);

type Source = { status: string; truncated?: boolean; source?: { query?: string } };

/** Every read in an extract that can stop at a cap, by where it sits. */
function cappedReads(id: string, value: unknown, path: string[] = [], out: Array<{ path: string[]; query: string }> = []): Array<{ path: string[]; query: string }> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return out;
  const s = value as Source;
  const query = s.source?.query;
  const capped = query && CAPPED.test(query) && !/^SELECT COUNT\(/i.test(query) && !/\bGROUP BY\b/i.test(query) && !UNCAPPED.has(`${id}/${path.join("/")}`);
  if (s.status === "ok" && s.truncated === false && capped) out.push({ path, query });
  else for (const [key, child] of Object.entries(value)) if (key !== "data" && key !== "source") cappedReads(id, child, [...path, key], out);
  return out;
}

const recorded = loadExtracts((await runFromRecording({ answers: loadAnswers(), modules: ALL_MODULES })).dir);
const generated = loadExtracts(
  (await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), answers: loadAnswers(), modules: ALL_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport: new Simulator({ records: buildScaleOrg(scaled(FIVE_HUNDRED_USERS, 0.005)), pageSize: 2000 }) })).dir,
);

/** Each check that passes with one capped read marked as cut off, and which read. */
function passesOnPart(extracts: Map<string, unknown>): string[] {
  const reads = [...extracts].flatMap(([id, value]) => cappedReads(id, value).map((r) => ({ id, ...r })));
  assert.ok(reads.length >= 20, `${reads.length} capped reads found`);
  const out: string[] = [];
  for (const read of reads) {
    const cut = clone(extracts);
    let at = cut.get(read.id) as Record<string, unknown>;
    for (const key of read.path) at = at[key] as Record<string, unknown>;
    (at as Source).truncated = true;
    for (const f of analyseExtracts(cut, { modules: ALL_MODULES, answers: loadAnswers() }).findings) {
      // A pass that says "at least" claims only what it read, such as forecasting being in use.
      if (f.status === "passed" && !/\bat least\b/.test(f.summary ?? "") && f.evidence.some((e) => e.kind === "soql" && e.query === read.query)) out.push(`${f.ruleId} on ${read.id}/${read.path.join("/")}`);
    }
  }
  return [...new Set(out)].sort();
}

test("no check passes on a read that stopped at its row cap: the synthetic org", () => {
  assert.deepEqual(passesOnPart(recorded), []);
});

test("no check passes on a read that stopped at its row cap: a generated org, where most checks pass", () => {
  assert.deepEqual(passesOnPart(generated), []);
});
