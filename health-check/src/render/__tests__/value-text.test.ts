import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { ALL_MODULES } from "../../domains/index.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers } from "../../testing/replay.ts";
import { renderContext, valueText } from "../html.ts";

// How a finding's figure reads on its card (DECISIONS.md 355).

const { output, dir } = await runFromRecording({ answers: loadAnswers(), modules: ALL_MODULES });
const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
const r = renderContext(pseudonyms, "GBP", false);

test("every figure for open pipeline today says it's at risk", () => {
  // A new check that values affected deals with data quality's exposure() must join EXPOSURE_RULES, or this fails.
  const open = output!.findings.filter((f) => f.value.kind !== "not_sized" && f.value.period === "open pipeline today");
  assert.ok(open.length >= 5, open.map((f) => f.ruleId).join(", "));
  for (const f of open) assert.match(valueText(r, f).amount, /^At risk: £[\d,]+ open pipeline today$/, f.ruleId);
});
