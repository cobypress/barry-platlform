import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { ACCOUNT_ENGAGEMENT_MODULE, ACCOUNT_ENGAGEMENT_MODULES, knownModuleIds, modulesForRun, SALES_CLOUD_MODULES } from "../index.ts";
import { ESSENTIALS_DOMAINS, modulesFor } from "../scope.ts";
import { AREA_WEIGHTS } from "../scoring.ts";
import { ACTION_LISTS_FILE, type ActionLists } from "../../actions/lists.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { unworkedBefore, type PardotLeads, type PardotSetup, type SyncedLeadRow } from "../../extract/pardot.ts";
import { buildReport } from "../../narrative/sections.ts";
import type { Finding } from "../../schemas/index.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { AS_OF, userId } from "../../testing/synthetic-org.ts";

// Marketing Cloud Account Engagement (roadmap Stage 6, DECISIONS.md 261 to 274).

const modules = [...SALES_CLOUD_MODULES, ...ACCOUNT_ENGAGEMENT_MODULES];
const { output, dir, run } = await runFromRecording({ answers: loadAnswers(), modules });
const extracts = loadExtracts(dir);
const lists = JSON.parse(readFileSync(join(dir, ACTION_LISTS_FILE), "utf8")) as ActionLists;

const one = (findings: Finding[], ruleId: string): Finding => {
  const found = findings.filter((f) => f.ruleId === ruleId);
  assert.equal(found.length, 1, ruleId);
  return found[0]!;
};
const edited = <T>(id: string, edit: (data: T) => void): Finding[] => {
  const changed = clone(extracts);
  edit(changed.get(id) as T);
  return analyseExtracts(changed, { modules }).findings;
};
const openRows = (l: PardotLeads): SyncedLeadRow[] => (l.open as { data: SyncedLeadRow[] }).data;
const AE_RULES = ACCOUNT_ENGAGEMENT_MODULES.flatMap((m) => m.rules.map((r) => r.id));
const live = (transport: Simulator) => runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules, outDir: tempDir(), now: new Date(AS_OF), answers: loadAnswers(), transport });

describe("Account Engagement as a module", () => {
  test("switched on by its own id, which is its one area, and off unless named", () => {
    assert.equal(ACCOUNT_ENGAGEMENT_MODULE, "account_engagement");
    assert.deepEqual(modulesForRun(["configuration", "account_engagement"]).map((m) => m.id), ["configuration", "account_engagement"]);
    assert.ok(!modulesForRun(["configuration", "cpq"]).some((m) => m.cloud === "pardot"));
    assert.ok(knownModuleIds().has("account_engagement"));
    assert.ok(!knownModuleIds(SALES_CLOUD_MODULES).has("account_engagement"));
  });

  test("a set-up review checks campaign influence only, and reads no lead", () => {
    assert.ok(ESSENTIALS_DOMAINS.includes("account_engagement"));
    const essentials = modulesFor("essentials", ACCOUNT_ENGAGEMENT_MODULES);
    assert.deepEqual(essentials.flatMap((m) => m.rules.map((r) => r.id)), ["mkt.campaign-influence"]);
    assert.ok(!essentials.some((m) => m.extractors.some((e) => e.id === "pardot-leads")));
  });

  test("scored at a weight of 1", () => {
    assert.equal(AREA_WEIGHTS.account_engagement, 1);
    assert.ok(run.modules.includes("account_engagement"));
  });
});

describe("an org without Account Engagement, with the module switched on", async () => {
  const transport = new Simulator({ pardot: false });
  const { output: without } = await live(transport);

  test("every check is not measurable, saying it isn't installed", () => {
    for (const id of AE_RULES) {
      const f = one(without!.findings, id);
      assert.equal(f.status, "not_measurable", id);
      assert.match(f.notMeasurableReason!, /Account Engagement \(the Pardot package, namespace pi\) isn't installed/);
    }
  });

  test("nothing of it is queried and its settings aren't retrieved", () => {
    const sent = transport.requests.map((r) => `${decodeURIComponent(r.url)} ${r.body ?? ""}`);
    assert.ok(!sent.some((s) => /pi__/.test(s)));
    assert.ok(!sent.some((s) => /<met:members>Campaign<\/met:members>/.test(s)));
  });
});

describe("an org whose package lacks a field we read", async () => {
  const { output: missing } = await live(new Simulator({ dropFields: { Lead: ["pi__grade__c"] } }));

  test("the lead checks are not measurable, naming the field, and campaign influence still runs", () => {
    for (const id of ["mkt.scoring-grading", "mkt.unworked-handovers", "mkt.unowned-prospects"]) assert.match(one(missing!.findings, id).notMeasurableReason!, /Lead\.pi__grade__c isn't in this org/, id);
    assert.equal(one(missing!.findings, "mkt.campaign-influence").status, "issue");
  });
});

describe("unworked handovers", () => {
  const f = one(output!.findings, "mkt.unworked-handovers");

  test("ten of the 23 leads owned by a salesperson have no activity 14 to 90 days on, leaving out the one past 90", () => {
    assert.equal(f.status, "issue");
    assert.deepEqual([f.facts.handed, f.facts.unworked, f.facts.rate], [23, 10, 43.5]);
    assert.equal(f.severity, "medium");
  });

  test("listed by lead, with a name for each", () => {
    const list = lists.lists.find((l) => l.ruleId === "mkt.unworked-handovers")!;
    assert.equal(list.object, "Lead");
    assert.equal(list.rows.length, 10);
    assert.ok(list.rows.every((r) => (r.name ?? "").length > 0));
  });

  test("valued at the org's own return per lead, in one pot with the other unworked leads, so they aren't counted twice", () => {
    assert.equal(f.value.kind, "revenue");
    assert.match(f.value.kind === "revenue" ? f.value.period : "", /^one-off, if these leads are worked/);
    const report = buildReport({ run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps });
    const group = report.value.groups.find((g) => g.lines.some((l) => l.ruleId === "mkt.unworked-handovers"))!;
    assert.equal(group.id, "unworked-leads");
    assert.ok(group.lines.some((l) => l.ruleId === "lead.no-first-response" || l.ruleId === "dq.stale-leads"));
  });

  test("quiet once the leads have activity", () => {
    const findings = edited<PardotLeads>("pardot-leads", (l) => openRows(l).forEach((r) => (r.LastActivityDate = AS_OF.slice(0, 10))));
    assert.equal(one(findings, "mkt.unworked-handovers").status, "passed");
  });
});

describe("scoring and grading", () => {
  test("three in four synced leads have no grade", () => {
    const f = one(output!.findings, "mkt.scoring-grading");
    assert.equal(f.status, "issue");
    assert.deepEqual([f.facts.synced, f.facts.ungraded, f.facts.ungradedRate], [240, 180, 75]);
  });

  test("quiet when most are graded and scored", () => {
    const findings = edited<PardotLeads>("pardot-leads", (l) => void ((l.counts.ungraded as { data: number }).data = 20));
    assert.equal(one(findings, "mkt.scoring-grading").status, "passed");
  });
});

describe("unowned prospects", () => {
  test("four untouched in a queue and two owned by someone who has left, listed by lead", () => {
    const f = one(output!.findings, "mkt.unowned-prospects");
    assert.equal(f.status, "issue");
    assert.deepEqual([f.facts.open, f.facts.inQueue, f.facts.inactiveOwner], [30, 4, 2]);
    const list = lists.lists.find((l) => l.ruleId === "mkt.unowned-prospects")!;
    assert.equal(list.object, "Lead");
    assert.equal(list.rows.length, 6);
  });

  test("quiet once an active salesperson owns them, and a lead new to the queue never counts", () => {
    const findings = edited<PardotLeads>("pardot-leads", (l) => openRows(l).forEach((r) => (r.OwnerId === userId(9) || (r.OwnerId.startsWith("00G") && r.CreatedDate < unworkedBefore(new Date(AS_OF)).toISOString())) && (r.OwnerId = userId(1))));
    assert.equal(one(findings, "mkt.unowned-prospects").status, "passed");
  });
});

describe("campaign influence", () => {
  test("on, but not taking Account Engagement's models, is low", () => {
    const f = one(output!.findings, "mkt.campaign-influence");
    assert.equal(f.status, "issue");
    assert.equal(f.severity, "low");
  });

  test("off is medium, and fully set up passes", () => {
    const set = (influence: { customizable: boolean; fromAccountEngagement: boolean }) => edited<PardotSetup>("pardot-setup", (s) => void ((s.influence as { data: unknown }).data = influence));
    assert.equal(one(set({ customizable: false, fromAccountEngagement: false }), "mkt.campaign-influence").severity, "medium");
    assert.equal(one(set({ customizable: true, fromAccountEngagement: true }), "mkt.campaign-influence").status, "passed");
  });
});
