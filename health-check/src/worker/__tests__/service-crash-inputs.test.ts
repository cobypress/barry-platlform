import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { createJobHandler } from "../worker.ts";
import { baseDeps, consoleRun, RUN_ID, runInput, syntheticConnection } from "./helpers.ts";
import { ALL_MODULES } from "../../domains/index.ts";
import { Simulator, SYNTHETIC_RECORDS, type OrgRecords } from "../../testing/simulator.ts";
import type { Finding, Metric } from "../../schemas/index.ts";

// Two inputs that stopped a consultant-led run with Service Cloud at the base,
// with the case status paths' switch off or on (DECISIONS.md 776): an org with
// no open case at all, and a case whose Origin is the empty string. Both now
// run through the worker to done, and say what is true.

const SERVICE = ALL_MODULES.map((m) => m.id);
const run = async (records: OrgRecords, casePaths: boolean) => {
  const deps = baseDeps({ dataTransport: new Simulator({ records }), ...(casePaths ? { casePaths: true } : {}) });
  deps.store.addRun(runInput({ run: consoleRun({ reviewMode: "consultant", modules: SERVICE }) }));
  await deps.tokens.save(syntheticConnection());
  const outcome = await createJobHandler(deps)({ kind: "run", runId: RUN_ID }, { final: false } as never);
  const dir = join(deps.runsDir, RUN_ID);
  const read = <T>(f: string): T => JSON.parse(readFileSync(join(dir, f), "utf8")) as T;
  return { outcome, metrics: read<Metric[]>("metrics.json"), findings: read<Finding[]>("findings.json") };
};
const noOpen: OrgRecords = { ...SYNTHETIC_RECORDS, supportCases: { ...SYNTHETIC_RECORDS.supportCases, cases: SYNTHETIC_RECORDS.supportCases.cases.filter((c) => c.IsClosed) } };
const emptyOrigin: OrgRecords = { ...SYNTHETIC_RECORDS, supportCases: { ...SYNTHETIC_RECORDS.supportCases, cases: SYNTHETIC_RECORDS.supportCases.cases.map((c, i) => (i % 13 === 5 ? { ...c, Origin: "" } : c)) } };

describe("Service Cloud inputs that stopped a run", () => {
  for (const casePaths of [false, true]) {
    test(`no open case at all: done, the open cases measure says why it isn't split, and the open-case checks have none to check (switch ${casePaths ? "on" : "off"})`, async () => {
      const { outcome, metrics, findings } = await run(noOpen, casePaths);
      assert.equal(outcome.outcome, "done");
      const open = metrics.find((m) => m.id === "service.open_cases")!;
      assert.equal(open.status, "measured");
      assert.ok(open.status === "measured" && open.value === 0 && open.segments.length === 0);
      assert.equal(open.status === "measured" ? open.segmentExemptReason : null, "No case is open, so there is nothing to split by priority.");
      for (const id of ["svc.backlog-age", "svc.unassigned-cases", "svc.case-customer"]) {
        const f = findings.find((x) => x.ruleId === id)!;
        assert.equal(f.summary, "There are no open cases to check.", id);
      }
    });

    test(`an empty Origin: done, and read as no origin in the cases by channel (switch ${casePaths ? "on" : "off"})`, async () => {
      const { outcome, metrics } = await run(emptyOrigin, casePaths);
      assert.equal(outcome.outcome, "done");
      const origins = metrics.find((m) => m.id === "service.cases_by_origin")!;
      assert.ok(origins.status === "measured" && origins.segments.every((s) => s.key !== ""));
      assert.equal(origins.status === "measured" ? origins.segments.filter((s) => s.key === "No origin").length : 0, 1);
    });
  }
});
