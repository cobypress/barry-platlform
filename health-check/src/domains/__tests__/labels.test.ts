import { test } from "node:test";
import assert from "node:assert/strict";
import { ALL_MODULES } from "../index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers } from "../../testing/replay.ts";

// Customer text names objects and fields by their labels, as the org shows them,
// and API names stay in the working: evidence and facts (DECISIONS.md 411).

const { output } = await runFromRecording({ answers: loadAnswers(), modules: ALL_MODULES });
const findings = output!.findings;
const API_NAME = /\b[A-Za-z][A-Za-z0-9]*__[A-Za-z0-9_]+\b|\b(?:Lead|Opportunity|Account|Contact|Case|User)\.[A-Z][A-Za-z0-9_]+\b/;
const customerText = (f: (typeof findings)[number]): string[] => [f.title, f.summary, f.recommendation ?? "", f.notMeasurableReason ?? "", ...f.subjects.map((s) => s.label)];

// Their labels need reads of their own, which come next.
const LATER = new Set(["cpq.price-rule-conflicts"]);

test("no finding a customer reads names an object or field by its API name", () => {
  const named = findings.filter((f) => !LATER.has(f.ruleId)).flatMap((f) => customerText(f).filter((t) => API_NAME.test(t)).map((t) => `${f.ruleId}: ${API_NAME.exec(t)![0]} in "${t.slice(0, 90)}"`));
  assert.deepEqual(named, []);
});

test("labels come from the org: a field by its own label and an object by its own", () => {
  const picklists = findings.find((f) => f.ruleId === "cfg.picklist-sprawl" && f.status === "issue")!;
  assert.match(picklists.summary, /^Lead Source on Lead has 30 active values/);
  // The working keeps the API name.
  assert.ok(picklists.evidence.some((e) => /Lead\.LeadSource/.test(e.note ?? "")));
  const undocumented = findings.find((f) => f.ruleId === "cfg.undocumented-fields" && f.status === "issue")!;
  assert.match(undocumented.summary, /\bInvoice \(2\)/);
});
