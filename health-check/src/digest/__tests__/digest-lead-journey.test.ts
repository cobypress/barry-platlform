import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { checkedDigest, digestTextsAt, marksOf, type CheckedDigest } from "../checked.ts";
import { SALES_CLOUD_MODULES } from "../../domains/index.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { loadRun } from "../../render/render-run.ts";
import { buildReport } from "../../narrative/sections.ts";
import { AutomaticReplay, automaticSyntheticCalibration } from "../../testing/automatic.ts";
import { tempDir } from "../../testing/harness.ts";
import { journeyOrg, journeyRecords, richSpecs, type LeadSpec } from "../../testing/lead-journey-org.ts";
import { syntheticProvider } from "../../testing/replay.ts";
import { Simulator, type SimulatorOptions } from "../../testing/simulator.ts";
import type { Transport } from "../../salesforce/transport.ts";
import { AS_OF, CONNECTED_USER } from "../../testing/synthetic-org.ts";

// An automatic run with the lead journey on (HC_LEAD_JOURNEY, DECISIONS.md 770),
// end to end through the real reads, analysis and digest check: released with
// nothing unmarked or unmatched, a lead source served as the org's words, and
// the stored insight in the run folder and nowhere a customer reads.

const MADE_AT = "2026-11-26T09:00:00.000Z";
const calibration = await automaticSyntheticCalibration();
const automatic = async (transport: Transport, leadJourney = true, areaReasons = false) => {
  const run = await runHealthCheck({ provider: syntheticProvider, calibration, modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport, connectedUser: CONNECTED_USER, log: () => {}, reviewMode: "automatic", leadJourney, ...(areaReasons ? { areaReasons } : {}) });
  const { input, pseudonyms } = loadRun(run.dir);
  return { dir: run.dir, input, checked: checkedDigest({ input, pseudonyms, inventory: null, madeAt: MADE_AT }) };
};
/**
 * Released, nothing unmarked or unmatched, and no detail withheld that the same org's run withholds with the switch
 * off: the constructed lead orgs' campaign figures withhold 3 at the tip already, with no lead journey at all.
 */
const released = (c: CheckedDigest, what: string, masksOff = 0): void => {
  const unmarked = c.digest ? digestTextsAt(c.digest).filter((t) => !marksOf(c.digest!, t)).map((t) => `${t.path}: ${t.text}`) : [];
  assert.deepEqual({ held: c.held, unmarked: c.unmarked, unmatched: c.unmatched, masks: c.masks }, { held: null, unmarked: 0, unmatched: 0, masks: masksOff }, `${what}\n${unmarked.join("\n")}`);
};
/** The org's run with the switch on, released as the same org's run with it off is. */
const releasedAsOff = async (options: SimulatorOptions, what: string): Promise<Awaited<ReturnType<typeof automatic>>> => {
  const [on, off] = [await automatic(new Simulator(options)), await automatic(new Simulator(options), false)];
  released(off.checked, `${what}, off`, off.checked.masks);
  released(on.checked, what, off.checked.masks);
  return on;
};
const HOSTILE = "Lost 50% of 10 deals.";
const fastWins = (): LeadSpec[] =>
  Array.from({ length: 400 }, (_, k): LeadSpec => ({ source: k % 4 < 2 ? "Web" : "Event", made: 100 + ((k * 7) % 600), owner: (k % 3) + 1, convertedAfter: k % 2 === 0 ? 3 : 20, outcome: (k % 2 === 0 ? k % 20 < 14 : k % 20 < 6) ? "won" : "lost", closedAfter: 60 }));

describe("an automatic run with the lead journey on", () => {
  const VARIANTS: Array<[string, SimulatorOptions]> = [
    ["the synthetic org", {}],
    ["read-only", { readOnly: true }],
    ["Professional Edition", { organizationType: "Professional Edition" }],
    ["read-only on Professional Edition", { readOnly: true, organizationType: "Professional Edition" }],
    ["no CPQ and no Account Engagement", { cpq: false, pardot: false }],
    ["a constructed org whose source converts well and wins poorly", { records: journeyOrg(journeyRecords(richSpecs())) }],
    ["the same org with no Lead Status history", { records: journeyOrg(journeyRecords(richSpecs(), () => [])) }],
    ["the same org on Professional Edition", { records: journeyOrg(journeyRecords(richSpecs())), organizationType: "Professional Edition" }],
  ];
  for (const [name, options] of VARIANTS) {
    test(`${name}: released, nothing unmarked or unmatched, and nothing more withheld than with the switch off`, async () => void (await releasedAsOff(options, name)));
  }

  test("on the replayed synthetic org too", async () => released((await automatic(new AutomaticReplay())).checked, "replay"));

  test("with the areas' own reasons on too (HC_AREA_REASONS, 769): released on every connection and the constructed orgs", async () => {
    for (const [name, options] of VARIANTS) {
      const off = (await automatic(new Simulator(options), false, true)).checked;
      released((await automatic(new Simulator(options), true, true)).checked, `${name}, with area reasons`, off.masks);
    }
    released((await automatic(new AutomaticReplay(), true, true)).checked, "replay, with area reasons");
  });

  test("the source check's finding is served, the source marked as the org's, whole, even a hostile one", async () => {
    const specs = richSpecs().map((s) => (s.source === "Webinar" ? { ...s, source: HOSTILE } : s));
    const { checked } = await releasedAsOff({ records: journeyOrg(journeyRecords(specs)) }, "hostile source");
    const d = checked.digest!;
    const texts = digestTextsAt(d).filter((t) => t.text.includes(`Leads from ${HOSTILE} convert more often`) || t.text.includes(`from ${HOSTILE} were converted`));
    assert.ok(texts.length >= 2, texts.map((t) => t.path).join(", "));
    for (const t of texts) {
      const spans = marksOf(d, t)!.orgWords.map((s) => t.text.slice(s.start, s.end));
      assert.ok(spans.includes(HOSTILE), `${t.path}: ${JSON.stringify(spans)}`);
    }
    // The metrics' segments carry the source as a key, which the digest checks as the org's.
    assert.ok(d.metrics.some((m) => m.id === "lead.won_lead_rate"));
  });

  test("the stored insight is in the run folder's findings and nowhere a customer reads: not the digest, scores, rules or documents' input", async () => {
    const { dir, input, checked } = await releasedAsOff({ records: journeyOrg(journeyRecords(fastWins())) }, "fast wins");
    const stored = (JSON.parse(readFileSync(join(dir, "findings.json"), "utf8")) as Array<{ id: string; ruleId: string; status: string; summary?: string }>).filter((f) => f.ruleId === "lead.fast-conversion");
    assert.equal(stored.length, 1);
    assert.equal(stored[0]!.status, "issue");
    const served = JSON.stringify(checked.digest);
    assert.ok(!served.includes("lead.fast-conversion") && !served.includes("converted within 7 days"));
    // The folder's findings, read back, keep it as they keep the path insights; what is built for a customer leaves it out.
    assert.ok(input.findings.some((f) => f.ruleId === "lead.fast-conversion"));
    assert.ok(!checked.digest!.findings.some((f) => f.id === stored[0]!.id));
    assert.ok(!JSON.stringify(buildReport(input)).includes("converted within 7 days"), "nor the report every document is made from");
    assert.ok(!JSON.stringify(input.scores).includes(stored[0]!.id), "no score counts it");
    const rules = JSON.parse(readFileSync(join(dir, "rules.json"), "utf8")) as { rulesRun: string[]; rulesKnown: string[] };
    assert.ok(!rules.rulesRun.includes("lead.fast-conversion") && !rules.rulesKnown.includes("lead.fast-conversion"));
    assert.ok(rules.rulesRun.includes("lead.source-win-rate") && rules.rulesKnown.includes("lead.source-win-rate"));
  });

  test("with the switch off, the run knows none of it", async () => {
    const { dir, checked } = await automatic(new Simulator({ records: journeyOrg(journeyRecords(fastWins())) }), false);
    const rules = readFileSync(join(dir, "rules.json"), "utf8") + readFileSync(join(dir, "findings.json"), "utf8") + readFileSync(join(dir, "metrics.json"), "utf8");
    for (const id of ["lead.source-win-rate", "lead.fast-conversion", "lead.days_to_won", "lead.won_lead_rate"]) assert.ok(!rules.includes(id), id);
    assert.equal(checked.held, null);
  });
});
