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
import { caseOrg, caseRecords, richCases, straightCases, stuckCases, type CaseSpec, type CaseStatus } from "../../testing/case-paths-org.ts";
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
const SERVED = ["svc.reopened-time", "svc.closed-straight"];
/** Stored in the run folder and never served (INSIGHT_RULES): the send-back insight, and the stuck check since its seasonal probe. */
const STORED = ["svc.sent-back-cases", "svc.stuck-in-status"];
const HOSTILE = "Stage 9 of 12, 50% done";
/** 300 cases New, Working, Closed in two days, priorities and origins in turn, and one open case. */
const worked = (): CaseSpec[] => [
  { made: 365, steps: [["Working", 0.5], ["Closed", 1]] },
  ...Array.from({ length: 300 }, (_, i): CaseSpec => ({ made: 40 + i, priority: ["High", "Medium", "Low"][i % 3]!, origin: ["Email", "Web", "Phone"][Math.floor(i / 3) % 3]!, steps: [["Working", 0.5], ["Closed", 2]] })),
  { made: 3, steps: [["Working", 0.5]] },
];
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
    // A hundred cases closed as duplicates straight from the status labelled with digits and a percent sign, so the
    // closed-straight check names that starting status.
    const fromWaiting = Array.from({ length: 100 }, (_, i): CaseSpec => ({ made: 50 + i, start: "Waiting", priority: ["High", "Medium", "Low"][i % 3]!, origin: ["Email", "Web", "Phone"][i % 3]!, steps: [["Duplicate", 0.2]] }));
    const { checked } = await consultant(new Simulator({ records: caseOrg(caseRecords([...worked(), ...fromWaiting], STATUSES)) }));
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
    const all = JSON.parse(readFileSync(join(dir, "findings.json"), "utf8")) as Array<{ id: string; ruleId: string }>;
    for (const id of STORED) assert.equal(all.filter((f) => f.ruleId === id).length, 1, `${id} stored`);
    const stored = all.filter((f) => STORED.includes(f.ruleId));
    const metrics = JSON.parse(readFileSync(join(dir, "metrics.json"), "utf8")) as Array<{ id: string }>;
    assert.ok(metrics.some((m) => m.id === "service.status_paths"), "the path table is stored");
    const served = JSON.stringify(checked.digest);
    for (const id of [...STORED, "service.status_paths"]) assert.ok(!served.includes(id), id);
    const report = JSON.stringify(buildReport(input));
    for (const id of [...STORED, "service.status_paths"]) assert.ok(!report.includes(id), `${id}: nor the report every document is made from`);
    for (const f of stored) assert.ok(!JSON.stringify(input.scores).includes(f.id), "no score counts it");
    const rules = JSON.parse(readFileSync(join(dir, "rules.json"), "utf8")) as { rulesRun: string[]; rulesKnown: string[] };
    for (const id of STORED) assert.ok(!rules.rulesRun.includes(id) && !rules.rulesKnown.includes(id), id);
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
      assert.ok(!on.digest!.findings.some((f) => f.ruleId === "svc.stuck-in-status"), label);
      assert.ok(!JSON.stringify(on.digest!.findings.filter((f) => SERVED.includes(f.ruleId))).includes(NAMED_PEOPLE_SENTENCE), label);
      assert.ok(!JSON.stringify(on.digest).includes(label), `${label} is never written`);
    }
  });

  test("an origin shaped like an email or an id over the segment line, on an automatic run with Service Cloud: raised, released as with the switch off, the name never written", async () => {
    const calibration = await automaticSyntheticCalibration();
    for (const name of ["help.desk@example.com", "500000000000009AAA"]) {
      const odd = Array.from({ length: 26 }, (_, i): CaseSpec => ({ made: 50 + i, origin: name, priority: ["High", "Medium", "Low"][i % 3]!, steps: i < 14 ? [["Duplicate", 0.2]] : [["Working", 0.5], ["Closed", 2]] }));
      const records = caseOrg(caseRecords([...worked(), ...odd], STATUSES));
      const automatic = async (casePaths: boolean) => {
        const run = await runHealthCheck({ provider: syntheticProvider, calibration, reviewMode: "automatic", modules: ALL_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport: new Simulator({ records }), connectedUser: CONNECTED_USER, log: () => {}, ...(casePaths ? { casePaths } : {}) });
        const { input, pseudonyms } = loadRun(run.dir);
        return checkedDigest({ input, pseudonyms, inventory: null, madeAt: MADE_AT });
      };
      const [on, off] = [await automatic(true), await automatic(false)];
      assert.deepEqual(counts(on), counts(off), name);
      const straight = on.digest!.findings.find((f) => f.ruleId === "svc.closed-straight")!;
      assert.equal(straight.status, "issue", name);
      assert.match(straight.summary ?? "", /one origin we can't name here is at 53\.8% \(14 of 26\)/, name);
      assert.ok(!JSON.stringify(on.digest!.findings.filter((f) => SERVED.includes(f.ruleId))).includes(name), `${name} is never written by the lane`);
    }
  });

  test("with the switch off, the run knows none of it", async () => {
    const { dir } = await consultant(new Simulator({ records: caseOrg(caseRecords(richCases())) }), false);
    const files = ["rules.json", "findings.json", "metrics.json"].map((f) => readFileSync(join(dir, f), "utf8")).join("");
    for (const id of [...SERVED, ...STORED, "service.status_paths"]) assert.ok(!files.includes(id), id);
  });
});
