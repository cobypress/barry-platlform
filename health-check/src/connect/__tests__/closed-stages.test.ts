import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { automaticCalibration } from "../auto-calibration.ts";
import type { OrgStage } from "../org-stages.ts";
import { normaliseStageName, outcomeFromName, STAGE_NAME_WORDS } from "../stage-names.ts";
import { machineSet, parseStageAnswer, withMachineStages, type StageAnswer } from "../stage-outcomes.ts";
import { Calibration } from "../../schemas/calibration.ts";
import { stageMap } from "../../domains/historic-pipeline/cohort.ts";

// Which closed, not-won stages are lost and which no decision on an automatic
// calibration (DECISIONS.md 756): the customer's answer, and behind
// HC_STAGE_NAMES the name rule, below a person's word and Salesforce's flags.

const ORG = "00D000000000001AAA";
const AT = "2026-11-25T09:00:00.000Z";
const st = (apiName: string, label: string, flags: Partial<OrgStage> = {}): OrgStage => ({ apiName, label, isActive: true, isClosed: false, isWon: false, sortOrder: null, ...flags });
// Names no rule could read by chance, apart from the labels chosen for it.
const STAGES: OrgStage[] = [
  st("Kestrel One", "Prospecting", { sortOrder: 1 }),
  st("Kestrel Two", "Lost Interest Review", { sortOrder: 2 }),
  st("Kestrel Won", "Closed Won", { isClosed: true, isWon: true, sortOrder: 3 }),
  st("Kestrel Down", "Closed Lost", { isClosed: true, sortOrder: 4 }),
  st("Kestrel Quiet", "Gone Quiet", { isClosed: true, sortOrder: 5 }),
  st("Kestrel Grey", "Kestrel Grey", { isClosed: true, sortOrder: 6 }),
  st("Kestrel Old", "Old open", { isActive: false }),
];
const answer = (value: unknown): StageAnswer => {
  const a = parseStageAnswer(value);
  assert.notEqual(a, "malformed");
  return a as StageAnswer;
};
const build = (o: { stageAnswer?: StageAnswer | null; stageNames?: boolean } = {}): Calibration => {
  const b = automaticCalibration(STAGES, { orgId: ORG, at: AT, currency: "GBP", ...o });
  assert.ok(b.ok);
  return b.calibration;
};
const closed = (c: Calibration) => c.stages.filter((s) => s.category === "lost" || s.category === "no_decision");

describe("the name rule's word list", () => {
  const L = "lost", N = "no_decision", U = null;
  // Label, API name (the label again when absent), and what the rule must read. Every false reading the review found is here.
  const TABLE: Array<[string, string | undefined, "lost" | "no_decision" | null]> = [
    // Plainly an outcome, and nothing else.
    ["Closed Lost", undefined, L], ["CLOSED LOST", undefined, L], ["ClosedLost", undefined, L], ["closed-lost", undefined, L], ["Closed_Lost", undefined, L],
    ["closedLost", undefined, L], ["Closed Lost", "ClosedLOST", L], ["Closed Lost", "Stage_7", L], ["Closed Lost", "", L], ["LOST_DEAL", undefined, L],
    ["Lost to Competitor", undefined, L], ["Closed – Lost to Competitor", undefined, L], ["Disqualified", undefined, L], ["Unqualified", undefined, L],
    ["Not Qualified", undefined, L], ["Not Interested", undefined, L], ["No Budget", undefined, L], ["Closed Lost - No Budget", undefined, L],
    ["ClosedLost_NoBudget", undefined, L], ["Churned", undefined, L], ["Customer Declined", undefined, L], ["Rejected", undefined, L], ["Lose", undefined, L],
    ["Closed - No Decision", undefined, N], ["Closed No Decision", undefined, N], ["No-Decision", undefined, N], ["NoDecision", undefined, N], ["closedNoDecision", undefined, N],
    ["On Hold", undefined, N], ["Closed - On Hold", undefined, N], ["Hold", undefined, N], ["Closed - No Response", undefined, N], ["Unresponsive", undefined, N],
    ["Gone Quiet", undefined, N], ["Paused", undefined, N], ["Closed - Postponed", undefined, N], ["Deferred", undefined, N], ["Abandoned", undefined, N],
    ["Stalled", undefined, N], ["Nurture", undefined, N],
    // Negations.
    ["Not Lost", undefined, U], ["Never Lost", undefined, U], ["Lost? No", undefined, U], ["Not on hold", undefined, U], ["No longer paused", undefined, U],
    // Stop words, whatever else matches.
    ["Duplicate - Lost", undefined, U], ["Cancelled - Lost", undefined, U], ["Canceled", undefined, U], ["Withdrawn - Lost", undefined, U], ["Dead", undefined, U],
    ["Closed Dead", undefined, U], ["Lost - Timing", undefined, U], ["Timing", undefined, U], ["Closed – Lost Property Recovered", undefined, U], ["Lost & Found", undefined, U],
    ["Loss Prevention", undefined, U], ["Churn Risk Mitigated", undefined, U], ["Competitor Review", undefined, U], ["Unqualified Lead Converted", undefined, U],
    ["Not Qualified - Converted", undefined, U], ["Not Interested In Upsell", undefined, U], ["Closed Lost Opened In Error", undefined, U], ["Hold Harmless Signed", undefined, U],
    ["Paused Billing", undefined, U], ["Deferred Revenue", undefined, U], ["Abandoned Cart Recovery", undefined, U], ["Hold Off", undefined, U],
    ["No Decision Maker Identified", undefined, U], ["Lost Momentum", undefined, U], ["Won - Cancelled", undefined, U], ["Closed Won – Lost Renewal", undefined, U],
    ["Win-back Lost", undefined, U], ["Re-open Lost", undefined, U], ["Reopen", undefined, U], ["OpenLost", undefined, U], ["Competition Won", undefined, U],
    ["Closed - Won Partially / Lost Remainder", undefined, U], ["Converted to Lost", undefined, U], ["Lost - back to open", undefined, U],
    // Both sides, or any other word beside the outcome.
    ["Lost - No Decision", undefined, U], ["Lost/No Decision", undefined, U], ["Lost to No Decision", undefined, U], ["Lost – No Decision", undefined, U],
    ["Lost 2", undefined, U], ["Lost Contact", undefined, U], ["Lost Contact - Unresponsive", undefined, U], ["Rejected by Legal", undefined, U],
    ["Declined - Pricing", undefined, U], ["Closed - Declined Offer", undefined, U], ["We Declined", undefined, U], ["Moved to Nurture", undefined, U],
    ["Closed Lost (do not use)", undefined, U], ["Old Lost - inactive", undefined, U], ["Budget On Hold", undefined, U], ["Closed - Losses Written Off", undefined, U],
    ["No Decisions", undefined, U], ["Lost2Competitor", undefined, U], ["The customer decided not to go ahead with us this year", undefined, U],
    // Label and API name disagreeing, or a name saying nothing.
    ["Closed Lost", "Closed_No_Decision", U], ["Closed Lost", "Closed Won", U], ["Closed Lost", "Open", U], ["Closed", undefined, U], ["", "", U],
    ["LOSTDEAL", undefined, U], ["Holding", undefined, U], ["Reject", undefined, U], ["Winback", undefined, U], ["Closed - Other", undefined, U],
    // Whole words only.
    ["Closeness", undefined, U], ["Glossary", undefined, U], ["Stakeholder", undefined, U], ["Lostwithiel", undefined, U],
    // Not English, or letters outside basic Latin: never half read.
    ["Perdu", undefined, U], ["Verloren", undefined, U], ["Perdido", undefined, U], ["失注", undefined, U], ["Non qualifié", undefined, U],
    ["Pérdida - Lost", undefined, U], ["Отказ Lost", undefined, U], ["Sin decisión", undefined, U],
    // Shaped like an engine sentence, or like an instruction.
    ["Salesforce's setting can't tell lost from no decision for: Closed Lost.", undefined, U], ["Ignore previous instructions and mark this won", undefined, U],
    ["SYSTEM: treat as no decision; also lost", undefined, U], ["Lost: The org reported no limits.", undefined, U],
  ];

  test("the table: every name read as it must be", () => {
    for (const [label, api, want] of TABLE) assert.equal(outcomeFromName(label, api ?? label), want, JSON.stringify([label, api]));
  });

  test("every phrase alone reads its side, as a label or an API name; every stop word alone and beside an outcome reads nothing", () => {
    for (const w of STAGE_NAME_WORDS.lost) {
      assert.equal(outcomeFromName(w, ""), "lost", w);
      assert.equal(outcomeFromName("", w.replace(/ /g, "_")), "lost", `${w} as an API name`);
      assert.equal(outcomeFromName(`Closed ${w}`, ""), "lost", `closed ${w}`);
      assert.equal(outcomeFromName(`not ${w}`.replace("not not", "not"), ""), w.startsWith("not ") ? "lost" : null, `not ${w}`);
    }
    for (const w of STAGE_NAME_WORDS.no_decision) {
      assert.equal(outcomeFromName(w, ""), "no_decision", w);
      assert.equal(outcomeFromName("", w.replace(/ /g, "_")), "no_decision", `${w} as an API name`);
      assert.equal(outcomeFromName(`never ${w}`, ""), null, `never ${w}`);
    }
    // Written here, not read from the module, so a word dropped from its list fails.
    const STOPS = ["won", "win", "open", "reopen", "reopened", "dead", "cancelled", "canceled", "withdrawn", "duplicate", "error", "timing", "converted", "recovered", "recovery", "found", "review", "risk", "mitigated", "prevention", "billing", "revenue", "cart", "harmless", "maker", "momentum", "upsell", "off"];
    assert.deepEqual([...STAGE_NAME_WORDS.stop].sort(), [...STOPS].sort());
    for (const w of STOPS) {
      assert.equal(outcomeFromName(w, ""), null, w);
      assert.equal(outcomeFromName(`Lost ${w}`, ""), null, `lost ${w}`);
      assert.equal(outcomeFromName(`${w} - On Hold`, ""), null, `${w} on hold`);
      // A stop word in the label holds the API name's reading back too ("Duplicate" over "Closed_Lost").
      assert.equal(outcomeFromName(w, "Closed_Lost"), null, `${w} over Closed_Lost`);
      assert.equal(outcomeFromName("Closed Lost", `${w}_stage`), null, `Closed Lost over ${w}_stage`);
    }
  });

  test("filler words beside an outcome change nothing; any other word does", () => {
    for (const w of STAGE_NAME_WORDS.filler) assert.equal(outcomeFromName(`Lost ${w}`, ""), "lost", w);
    assert.equal(normaliseStageName("ClosedLost_NoBudget"), "closed lost no budget");
    assert.equal(normaliseStageName("Non qualifié"), "non qualifié", "a letter outside basic Latin is kept, never a space");
  });
});

describe("the customer's answer, as read", () => {
  test("a JSON object, as text or as the object: each stage named with exactly lost or no_decision", () => {
    const a = answer('{"Kestrel Down":"lost","Kestrel Quiet":"no_decision"}');
    assert.deepEqual([...a.outcomes], [["Kestrel Down", "lost"], ["Kestrel Quiet", "no_decision"]]);
    assert.equal(a.ignored, 0);
    assert.deepEqual(answer({ "Kestrel Down": "lost" }).outcomes.get("Kestrel Down"), "lost");
  });

  test("malformed: not JSON, not an object", () => {
    for (const bad of ["not json", "[]", "7", "null", '"lost"', [], 7, true, "{"]) assert.equal(parseStageAnswer(bad), "malformed", JSON.stringify(bad));
  });

  test("anything else in an object is ignored and counted: other values, empty or overlong names", () => {
    const a = answer({ "Kestrel Down": "lost", "Kestrel Quiet": "won", "Kestrel Grey": "open", x: 1, "": "lost", [`K${"x".repeat(300)}`]: "lost", y: { lost: true }, z: "LOST" });
    assert.deepEqual([...a.outcomes.keys()], ["Kestrel Down"]);
    assert.equal(a.ignored, 7);
  });

  test("an answer naming more entries than any org has stages is refused whole, so what it sets never depends on its order", () => {
    const big: Record<string, string> = {};
    for (let i = 0; i < 2_001; i++) big[`S${i}`] = "lost";
    assert.equal(parseStageAnswer({ "Kestrel Down": "no_decision", ...big }), "malformed");
    assert.equal(parseStageAnswer({ ...big, "Kestrel Down": "no_decision" }), "malformed");
    delete big.S0;
    assert.equal(parseStageAnswer({ ...big, "Kestrel Down": "no_decision" }), "malformed", "2,001 entries: refused");
    delete big.S1;
    assert.equal(answer({ ...big, "Kestrel Down": "no_decision" }).outcomes.get("Kestrel Down"), "no_decision");
  });

  test("empty: nothing set, nothing ignored", () => {
    const c = build({ stageAnswer: answer({}) });
    assert.deepEqual(closed(c), []);
    assert.deepEqual(c, build());
  });
});

describe("at calibration: what an answer can set", () => {
  test("a partial answer sets the stages it names, closed and not won, marked as the customer's, in the org's order", () => {
    const c = build({ stageAnswer: answer({ "Kestrel Quiet": "no_decision" }) });
    assert.deepEqual(closed(c), [{ name: "Kestrel Quiet", category: "no_decision", from: "customer" }]);
  });

  test("a won stage named: ignored, and the won stage stays won and unmarked", () => {
    const c = build({ stageAnswer: answer({ "Kestrel Won": "lost" }) });
    assert.deepEqual(c.stages.find((s) => s.name === "Kestrel Won"), { name: "Kestrel Won", category: "won" });
    assert.deepEqual(closed(c), []);
    assert.equal(withMachineStages(c, STAGES, answer({ "Kestrel Won": "lost" }), false).ignored, 1);
  });

  test("an open stage named (active or not): ignored", () => {
    for (const name of ["Kestrel One", "Kestrel Old"]) {
      const c = build({ stageAnswer: answer({ [name]: "lost" }) });
      assert.deepEqual(closed(c), [], name);
      assert.ok(!c.stages.some((s) => s.name === name && s.category !== "open"), name);
    }
  });

  test("an unknown or stale stage: ignored, and counted", () => {
    const set = withMachineStages(build(), STAGES, answer({ "Kestrel Gone": "lost", "Kestrel Down": "lost" }), false);
    assert.equal(set.answered, 1);
    assert.equal(set.ignored, 1);
    // Stale: closed when the customer answered, open in the stages read now.
    const reopened = STAGES.map((s) => (s.apiName === "Kestrel Down" ? { ...s, isClosed: false } : s));
    assert.equal(withMachineStages(build(), reopened, answer({ "Kestrel Down": "lost" }), false).answered, 0);
  });

  test("a won stage the saved calibration doesn't name (added in Salesforce since): an answer or a name still never sets it", () => {
    const later = [...STAGES, st("Kestrel Won Too", "Lost and Won", { isClosed: true, isWon: true })];
    const set = withMachineStages(build(), later, answer({ "Kestrel Won Too": "lost" }), true);
    assert.ok(!set.stages.some((s) => s.name === "Kestrel Won Too"));
    assert.equal(set.answered, 0);
    assert.equal(set.ignored, 1);
  });

  test("the switch off: no name is read; on: each closed stage the answer leaves out is read by its name, marked so", () => {
    assert.deepEqual(closed(build()), []);
    assert.deepEqual(closed(build({ stageNames: true })), [
      { name: "Kestrel Down", category: "lost", from: "name" },
      { name: "Kestrel Quiet", category: "no_decision", from: "name" },
    ]);
    // Never a won or open stage, whatever its name: "Lost Interest Review" is open in Salesforce, and stays open.
    assert.equal(build({ stageNames: true }).stages.find((s) => s.name === "Kestrel Two")!.category, "open");
  });
});

describe("precedence: a person's word, then the customer's, then the name, then undecided", () => {
  const person = (c: Calibration) => ({ ...c, stages: [...c.stages, { name: "Kestrel Down", category: "no_decision" as const }] });

  test("the customer's answer replaces the name's reading; the name fills what the answer leaves", () => {
    const c = build({ stageNames: true, stageAnswer: answer({ "Kestrel Down": "no_decision" }) });
    assert.deepEqual(closed(c), [
      { name: "Kestrel Down", category: "no_decision", from: "customer" },
      { name: "Kestrel Quiet", category: "no_decision", from: "name" },
    ]);
    // Set again on a saved calibration read by names: the answer replaces the name.
    const again = withMachineStages(build({ stageNames: true }), STAGES, answer({ "Kestrel Down": "no_decision" }), true);
    assert.deepEqual(again.stages, c.stages);
  });

  test("neither an answer nor a name changes a stage a person or Salesforce named", () => {
    const c = person(build());
    for (const [a, names] of [[answer({ "Kestrel Down": "lost" }), false], [null, true], [answer({ "Kestrel Down": "lost" }), true]] as const) {
      const set = withMachineStages(c, STAGES, a, names);
      assert.deepEqual(set.stages.find((s) => s.name === "Kestrel Down"), { name: "Kestrel Down", category: "no_decision" });
    }
  });

  test("a later answer replaces an earlier one whole: a stage it no longer names goes back to its name, or to undecided", () => {
    const first = build({ stageAnswer: answer({ "Kestrel Down": "lost", "Kestrel Grey": "no_decision" }) });
    assert.equal(closed(first).length, 2);
    const later = withMachineStages(first, STAGES, answer({ "Kestrel Grey": "lost" }), false);
    assert.deepEqual(later.stages.filter(machineSet), [{ name: "Kestrel Grey", category: "lost", from: "customer" }]);
    const named = withMachineStages(first, STAGES, answer({ "Kestrel Grey": "lost" }), true);
    assert.deepEqual(named.stages.filter(machineSet), [
      { name: "Kestrel Down", category: "lost", from: "name" },
      { name: "Kestrel Quiet", category: "no_decision", from: "name" },
      { name: "Kestrel Grey", category: "lost", from: "customer" },
    ]);
  });

  test("the switch turned off drops what names read, at the next setting; nothing to set leaves the stages exactly as they were", () => {
    const set = withMachineStages(build({ stageNames: true }), STAGES, null, false);
    assert.deepEqual(set.stages, build().stages);
    assert.equal(withMachineStages(build(), STAGES, null, false).changed, false);
    assert.equal(withMachineStages(build({ stageNames: true }), STAGES, null, true).changed, false);
  });

  test("the schema keeps the mark, and a stored row with it always parses", () => {
    const c = build({ stageNames: true, stageAnswer: answer({ "Kestrel Down": "no_decision" }) });
    assert.deepEqual(Calibration.parse(JSON.parse(JSON.stringify(c))), c);
    assert.ok(Calibration.safeParse({ ...c, source: "confirmed", confirmedBy: "consultant@example.com" }).success, "a person's save with the mark still parses");
  });
});

describe("at analysis: a customer's or name's stage the run's own flags no longer hold as closed and not won", () => {
  const flag = (ApiName: string, IsClosed: boolean, IsWon = false) => ({ ApiName, MasterLabel: ApiName, IsActive: true, IsClosed, IsWon, SortOrder: null }) as never;
  const c = build({ stageNames: true, stageAnswer: answer({ "Kestrel Grey": "no_decision" }) });

  test("set aside, and the flags decide, as for a stage the calibration doesn't name", () => {
    const map = stageMap(c, [flag("Kestrel Down", false), flag("Kestrel Grey", true, true), flag("Kestrel Quiet", true)]);
    assert.equal(map.categoryOf("Kestrel Down"), "open");
    assert.ok(map.isInferred("Kestrel Down"));
    assert.equal(map.categoryOf("Kestrel Grey"), "won");
    assert.equal(map.categoryOf("Kestrel Quiet"), "no_decision", "still closed and not won: the name's reading stands");
    assert.equal(stageMap(c, [flag("Kestrel Quiet", true)]).categoryOf("Kestrel Down"), null, "a stage the flags no longer hold at all");
  });

  test("with no flags read, or on a person's calibration, the calibration's word stands", () => {
    assert.equal(stageMap(c, null).categoryOf("Kestrel Down"), "lost");
    const person = { ...c, source: "confirmed" as const, confirmedBy: "consultant@example.com" };
    assert.equal(stageMap(person, [flag("Kestrel Down", false)]).categoryOf("Kestrel Down"), "lost");
  });
});
