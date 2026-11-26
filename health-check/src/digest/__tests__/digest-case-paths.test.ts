import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { checkedDigest, digestTextsAt, marksOf, type CheckedDigest } from "../checked.ts";
import { ALL_MODULES } from "../../domains/index.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { loadRun } from "../../render/render-run.ts";
import { buildReport } from "../../narrative/sections.ts";
import { tempDir } from "../../testing/harness.ts";
import { caseOrg, caseRecords, richCases, straightCases, stuckCases, type CaseStatus } from "../../testing/case-paths-org.ts";
import { loadAnswers, loadCalibration, loadRecording, ReplayTransport, syntheticProvider } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import type { Transport } from "../../salesforce/transport.ts";
import { AS_OF, CONNECTED_USER } from "../../testing/synthetic-org.ts";
import { automaticSyntheticCalibration } from "../../testing/automatic.ts";
import { NAMED_PEOPLE_SENTENCE } from "../../schemas/digest.ts";

// A consultant-led run with Service Cloud and the case status paths on
// (HC_CASE_PATHS, DECISIONS.md 776), the only run the lane reaches, end to end
// through the real reads, analysis and digest check: its digest checked as with
// the switch off (nothing more unmarked, unmatched or withheld), a status served
// as the org's words, and the stored insight and metric nowhere a customer reads.

const MADE_AT = "2026-11-27T09:00:00.000Z";
const consultant = async (transport: Transport, casePaths = true) => {
  const run = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), answers: loadAnswers(), modules: ALL_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport, connectedUser: CONNECTED_USER, log: () => {}, ...(casePaths ? { casePaths } : {}) });
  const { input, pseudonyms } = loadRun(run.dir);
  return { dir: run.dir, input, checked: checkedDigest({ input, pseudonyms, inventory: null, madeAt: MADE_AT }) };
};
const counts = (c: CheckedDigest) => ({ held: c.held, unmarked: c.unmarked, unmatched: c.unmatched, masks: c.masks });
const SERVED = ["svc.stuck-in-status", "svc.reopened-time", "svc.closed-straight"];
const HOSTILE = "Stage 9 of 12, 50% done";
const STATUSES: CaseStatus[] = [
  { ApiName: "New", MasterLabel: "New", IsClosed: false },
  { ApiName: "Working", MasterLabel: "Working", IsClosed: false },
  { ApiName: "Waiting", MasterLabel: HOSTILE, IsClosed: false },
  { ApiName: "Closed", MasterLabel: "Closed", IsClosed: true },
  { ApiName: "Duplicate", MasterLabel: "500000000000001AAA", IsClosed: true },
];

describe("a consultant-led run with Service Cloud and the case status paths on", () => {
  const VARIANTS: Array<[string, () => Transport]> = [
    ["the synthetic org", () => new ReplayTransport(loadRecording())],
    ["a constructed org with stuck and reopened cases", () => new Simulator({ records: caseOrg(caseRecords(richCases())) })],
    ["a constructed org with many closed straight", () => new Simulator({ records: caseOrg(caseRecords(straightCases())) })],
    ["the same org with no Status history", () => new Simulator({ records: caseOrg(caseRecords(richCases().map((x) => ({ ...x, noHistory: true })))) })],
  ];
  for (const [name, transport] of VARIANTS) {
    test(`${name}: its digest checks as with the switch off, and the new checks are in it`, async () => {
      const [on, off] = [await consultant(transport()), await consultant(transport(), false)];
      assert.deepEqual(counts(on.checked), counts(off.checked), name);
      const ids = on.checked.digest!.findings.map((f) => f.ruleId);
      for (const id of SERVED) assert.ok(ids.includes(id), `${name}: ${id}`);
    });
  }

  test("a status is served marked as the org's, whole, even one with digits and a percent sign", async () => {
    // A backlog waiting on the customer in the last month, so the stuck check names that status.
    const { checked } = await consultant(new Simulator({ records: caseOrg(caseRecords(stuckCases({ backlog: true, waiting: true }), STATUSES)) }));
    const d = checked.digest!;
    const texts = digestTextsAt(d).filter((t) => t.text.includes(HOSTILE));
    assert.ok(texts.length >= 1, "the hostile status is in a served text");
    for (const t of texts) {
      const spans = (marksOf(d, t)?.orgWords ?? []).map((s) => t.text.slice(s.start, s.end));
      assert.ok(spans.includes(HOSTILE), `${t.path}: ${JSON.stringify(spans)}`);
    }
  });

  test("the stored insight and the path table are in the run folder and nowhere a customer reads", async () => {
    const { dir, input, checked } = await consultant(new Simulator({ records: caseOrg(caseRecords(richCases())) }));
    const stored = (JSON.parse(readFileSync(join(dir, "findings.json"), "utf8")) as Array<{ id: string; ruleId: string }>).filter((f) => f.ruleId === "svc.sent-back-cases");
    assert.equal(stored.length, 1);
    const metrics = JSON.parse(readFileSync(join(dir, "metrics.json"), "utf8")) as Array<{ id: string }>;
    assert.ok(metrics.some((m) => m.id === "service.status_paths"), "the path table is stored");
    const served = JSON.stringify(checked.digest);
    assert.ok(!served.includes("svc.sent-back-cases") && !served.includes("service.status_paths"));
    const report = JSON.stringify(buildReport(input));
    assert.ok(!report.includes("svc.sent-back-cases") && !report.includes("service.status_paths"), "nor the report every document is made from");
    assert.ok(!JSON.stringify(input.scores).includes(stored[0]!.id), "no score counts it");
    const rules = JSON.parse(readFileSync(join(dir, "rules.json"), "utf8")) as { rulesRun: string[]; rulesKnown: string[] };
    assert.ok(!rules.rulesRun.includes("svc.sent-back-cases") && !rules.rulesKnown.includes("svc.sent-back-cases"));
    for (const id of SERVED) assert.ok(rules.rulesRun.includes(id) && rules.rulesKnown.includes(id), id);
  });

  test("a status named like an email or an id, on an automatic run with Service Cloud: released as with the switch off, and never said to involve named people", async () => {
    const calibration = await automaticSyntheticCalibration();
    for (const label of ["jane.doe@example.com", "500000000000001AAA"]) {
      const statuses = STATUSES.map((st) => (st.ApiName === "Waiting" ? { ...st, MasterLabel: label } : st));
      const records = caseOrg(caseRecords(stuckCases({ backlog: true, waiting: true }), statuses));
      const automatic = async (casePaths: boolean) => {
        const run = await runHealthCheck({ provider: syntheticProvider, calibration, reviewMode: "automatic", modules: ALL_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport: new Simulator({ records }), connectedUser: CONNECTED_USER, log: () => {}, ...(casePaths ? { casePaths } : {}) });
        const { input, pseudonyms } = loadRun(run.dir);
        return checkedDigest({ input, pseudonyms, inventory: null, madeAt: MADE_AT });
      };
      const [on, off] = [await automatic(true), await automatic(false)];
      assert.deepEqual(counts(on), counts(off), label);
      const stuck = on.digest!.findings.find((f) => f.ruleId === "svc.stuck-in-status")!;
      assert.equal(stuck.status, "issue", label);
      assert.ok(!JSON.stringify(on.digest!.findings.filter((f) => SERVED.includes(f.ruleId))).includes(NAMED_PEOPLE_SENTENCE), label);
      assert.ok(!JSON.stringify(on.digest).includes(label), `${label} is never written`);
    }
  });

  test("with the switch off, the run knows none of it", async () => {
    const { dir } = await consultant(new Simulator({ records: caseOrg(caseRecords(richCases())) }), false);
    const files = ["rules.json", "findings.json", "metrics.json"].map((f) => readFileSync(join(dir, f), "utf8")).join("");
    for (const id of [...SERVED, "svc.sent-back-cases", "service.status_paths"]) assert.ok(!files.includes(id), id);
  });
});
