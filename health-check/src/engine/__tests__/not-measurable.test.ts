import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { ALL_MODULES } from "../../domains/index.ts";
import { overallScore, scoreDomain, scoreDomains } from "../../domains/scoring.ts";
import { runHealthCheck } from "../run.ts";
import { notMeasurableKind } from "../not-measurable.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { buildScaleOrg, FIVE_HUNDRED_USERS, scaled } from "../../testing/scale-org.ts";
import { Simulator } from "../../testing/simulator.ts";
import { AS_OF, CONNECTED_USER } from "../../testing/synthetic-org.ts";
import { Finding, Metric, Score, type ManualAnswer } from "../../schemas/index.ts";

// DECISIONS.md 627: every not measurable finding and metric says which of four
// kinds its reason is, and nothing that counts or scores findings changes.

const { dir } = await runFromRecording({ answers: loadAnswers(), modules: ALL_MODULES });
const extracts = loadExtracts(dir);
const analyse = (changed: Map<string, unknown> = extracts, options: Parameters<typeof analyseExtracts>[1] = {}) => analyseExtracts(changed, { modules: ALL_MODULES, answers: loadAnswers(), ...options });
const gone = (reason = "INVALID_TYPE: sObject type 'X' is not supported.") => ({ status: "unavailable", reason, source: { kind: "soql", api: "rest", query: "SELECT Id FROM X" } });
const edit = <T>(id: string, change: (value: T) => void): Map<string, unknown> => {
  const changed = clone(extracts);
  change(changed.get(id) as T);
  return changed;
};
const open = (findings: Finding[]): Finding[] => findings.filter((f) => f.status === "not_measurable");
const nmMetrics = (metrics: Metric[]) => metrics.filter((m): m is Extract<Metric, { status: "not_measurable" }> => m.status === "not_measurable");

describe("the kind of a reason", () => {
  const cases: Array<[string, ReturnType<typeof notMeasurableKind>]> = [
    // Not in use: the four families' first, from the real run's reasons as the engine words them.
    ["No revenue forecast type is active, so Forecasting isn't in use for revenue, and we don't report a forecast figure.", "not_in_use"],
    ["Salesforce Knowledge is switched off in this org, so there are no articles to check.", "not_in_use"],
    ["Professional Edition has no Apex, so there's no code to check.", "not_in_use"],
    // Round 2: a field a describe leaves out, in the old words and the new, is something we couldn't see.
    ["Case.Origin couldn't be seen in this org: it may not exist, or the connected user may not have access to it, so this can't be measured. Origin is how we tell which cases came in by email.", "unreadable"],
    ["Case.Origin isn't in this org, so this can't be measured. Origin is how we tell which cases came in by email.", "unreadable"],
    ["SBQQ__Quote__c.SBQQ__Ordered__c isn't in this org, so this can't be measured.", "unreadable"],
    ["LoginHistory.ApiType isn't in this org, so this can't be measured.", "unreadable"],
    ["Campaign.ActualCost isn't in this org, so this can't be measured.", "unreadable"],
    ["Case.Priority isn't in this org, and the read needs it, so it wasn't run.", "unreadable"],
    ["Case.Type and Case.Reason aren't in this org, so there's nothing to say why a case came in.", "unreadable"],
    ["Group.QueueRoutingConfigId isn't in this org, so we can't tell which queues Omni-Channel routes.", "unreadable"],
    // A package said absent from a describe alone (no licence isn't proof for a free package).
    ["Salesforce CPQ (the SBQQ package) isn't installed in this org, so there's nothing of it to check.", "unreadable"],
    ["Account Engagement (the Pardot package, namespace pi) isn't installed in this org, so there's nothing of it to check.", "unreadable"],
    // Round 2: a read that found nothing is too little to judge from, never "not in use".
    ["No case milestone started in the last year, so response and resolution targets aren't set up as entitlements.", "too_little_data"],
    ["No case was closed in the last year.", "too_little_data"],
    ["Omni-Channel offered no work in the last year.", "too_little_data"],
    ["No tasks were logged in the last year, so we can't tell a clean record from an unused one.", "too_little_data"],
    ["No account has a won deal, so there are no past customers to check.", "too_little_data"],
    ["None of the 12 won deals with a primary quote was ordered in Salesforce, so orders don't look to be part of how you close, and we can't judge the handover.", "too_little_data"],
    ["No leads were created in the window, so there's no funnel to measure.", "too_little_data"],
    ["Knowledge is on, but no article is published.", "too_little_data"],
    // Round 2: targets the calibration doesn't give in a form we can read are asked for.
    ["No targets were given in the calibration. Tell us your targets to see pipeline coverage.", "needs_input"],
    ["The calibration keeps targets in Forecasting quotas, but no revenue forecast type is active, so there is no quota to read. Tell us where targets are kept.", "needs_input"],
    // A read cut short is unreadable, whatever else the words say.
    ["Only 12 of the deals could be read before the read stopped at its row limit, and we need 30.", "unreadable"],
    ["1,200 deals is too few to compare because the read stopped at its row limit", "unreadable"],
    // Needs input: what to give, and where.
    ["The calibration doesn't name a loss reason field that exists on Opportunity, so we can't tell a blank reason from a missing field.", "needs_input"],
    ["The calibration doesn't say what these stages mean: Stage 9.", "needs_input"],
    ["No open stage was passed through by more than 80% of won deals, and the calibration marks none as required.", "needs_input"],
    ["No account is calibrated as an integration account, so we can't tell which accounts integrations run on.", "needs_input"],
    ["The manual step was skipped: the customer's admin was away.", "needs_input"],
    ["Salesforce's setting can't tell lost from no decision for: Closed Other. The calibration can say which each is.", "needs_input"],
    // Too little data.
    ["207 of 398 open deals have no amount, so shares of the total would mislead.", "too_little_data"],
    ["No campaign has an actual cost recorded, so a return can't be worked out.", "too_little_data"],
    ["12 slipped deals is below the calibrated minimum of 20.", "too_little_data"],
    ["Only 3 cases came in by email in the last year, fewer than the 20 we need.", "too_little_data"],
    // Unreadable.
    ["We can't yet see how often your team opens dashboards. Salesforce only tells us when the person running the check last opened each one, which says nothing about anyone else.", "unreadable"],
    ["Users couldn't be read: INSUFFICIENT_ACCESS", "unreadable"],
    ["Forecast rows show today's position, not what was called at the time, so accuracy can't be measured from them. It needs a snapshot taken each period.", "unreadable"],
    // The forecasting reason that says both stays unreadable (the rule's own hedge).
    ["Forecasting isn't in use, or can't be read, so we don't report a forecast figure. Salesforce holds no forecast rows for this period.", "unreadable"],
    ["Knowledge articles couldn't be read: sObject type 'KnowledgeArticleVersion' is not supported. Knowledge may not be turned on.", "unreadable"],
    ["Einstein Activity Capture isn't switched on in this org, or the connected user can't see it, so there's nothing to check.", "unreadable"],
    // There's nowhere yet to give these, so they aren't "needs something from you".
    ["The calibration keeps targets in a spreadsheet, and we don't read that yet.", "unreadable"],
    ["The org is multi-currency, and amounts can't be added until historic rates are supplied.", "unreadable"],
  ];
  for (const [reason, kind] of cases) test(`${kind}: ${reason.slice(0, 70)}`, () => assert.equal(notMeasurableKind(reason), kind));

  test("a reason not yet sorted is unreadable", () => {
    assert.equal(notMeasurableKind("Something we haven't seen before stopped this check."), "unreadable");
    assert.equal(notMeasurableKind("The rule passed without segmenting its data, so the result can't be trusted."), "unreadable");
  });

  test("words quoted after a colon never decide the kind", () => {
    assert.equal(notMeasurableKind("Users couldn't be read: 3 deals is below the calibrated minimum of 20."), "unreadable");
    assert.equal(notMeasurableKind("This check failed to run: No case was closed in the last year."), "unreadable");
    assert.equal(notMeasurableKind("Opportunities couldn't be read, and no metric is published without them: The calibration doesn't say what these stages mean."), "unreadable");
  });
});

describe("forecasting that isn't in use", () => {
  const forecastFindings = (findings: Finding[]) => findings.filter((f) => f.domain === "forecast");

  test("no active revenue type among the types read: not in use, and the area says there's nothing to score", () => {
    const { findings, metrics } = analyse(edit<{ types: { data: Array<{ IsActive: boolean }> } }>("forecast-quotas", (q) => q.types.data.forEach((t) => (t.IsActive = false))));
    const own = forecastFindings(findings);
    assert.ok(own.length > 0);
    for (const f of own) {
      assert.equal(f.status, "not_measurable", f.ruleId);
      assert.equal(f.notMeasurableKind, "not_in_use", f.ruleId);
      assert.match(f.notMeasurableReason!, /^No revenue forecast type is active, so Forecasting isn't in use for revenue/);
    }
    assert.equal(metrics.find((m) => m.id === "forecast.commit_against_quota")!.status === "not_measurable" && (metrics.find((m) => m.id === "forecast.commit_against_quota") as { notMeasurableKind?: string }).notMeasurableKind, "not_in_use");
    const score = scoreDomain("forecast", findings);
    assert.equal(score.status, "not_measurable");
    assert.equal(score.status === "not_measurable" && score.reason, "You don't use forecasting, so there is nothing to score.");
    assert.equal(score.status === "not_measurable" && score.notMeasurableKind, "not_in_use");
  });

  const hedged: Array<[string, () => Map<string, unknown>]> = [
    ["the forecast types can't be read", () => edit<{ types: unknown }>("forecast-quotas", (q) => (q.types = gone()))],
    ["no types come back at all", () => edit<{ types: { data: unknown[] } }>("forecast-quotas", (q) => (q.types.data = []))],
    ["there are no forecast rows this period", () => edit<{ items: { data: unknown[] } }>("forecast", (f) => (f.items.data = []))],
    ["the forecast objects are refused", () => edit<{ items: unknown; facts: unknown }>("forecast", (f) => ((f.items = gone()), (f.facts = gone())))],
  ];
  for (const [name, build] of hedged) {
    test(`${name}: unreadable, with the wording that says it may be either`, () => {
      const { findings } = analyse(build());
      const own = forecastFindings(findings).filter((f) => f.status === "not_measurable");
      assert.ok(own.length > 0);
      for (const f of own) assert.equal(f.notMeasurableKind, "unreadable", `${f.ruleId}: ${f.notMeasurableReason}`);
      const score = scoreDomain("forecast", findings);
      assert.ok(score.status === "measured" || score.notMeasurableKind === undefined, "the area isn't said to be unused");
    });
  }
});

describe("Knowledge that isn't turned on", () => {
  const knowledge = (enabled: boolean | null) => {
    const changed = edit<{ articles: unknown }>("service-setup", (s) => (s.articles = gone()));
    (changed.get("ai-signals") as { settings: { status: string; data: { knowledge: boolean | null } } }).settings.data.knowledge = enabled;
    return analyse(changed).findings.filter((f) => f.ruleId.startsWith("svc.knowledge"));
  };

  test("the org's own setting says it's off: not in use", () => {
    const found = knowledge(false);
    assert.equal(found.length, 2);
    for (const f of found) {
      assert.equal(f.notMeasurableKind, "not_in_use", f.ruleId);
      assert.equal(f.notMeasurableReason, "Salesforce Knowledge is switched off in this org, so there are no articles to check.");
    }
  });

  test("without the setting, it may not be on: unreadable, as before", () => {
    for (const f of knowledge(null)) {
      assert.equal(f.notMeasurableKind, "unreadable", f.ruleId);
      assert.match(f.notMeasurableReason!, /Knowledge may not be turned on\.$/);
    }
  });
});

describe("each kind from the engine's own rules", () => {
  test("needs input: a skipped manual step, a calibration with no integration account, and the seats with no cost", () => {
    const skipped: ManualAnswer[] = loadAnswers().map((a) => (a.stepKey === "eac-advanced-sync" ? { stepKey: a.stepKey, outcome: "skipped", values: {}, skipReason: "The admin was away.", answeredBy: a.answeredBy, answeredAt: a.answeredAt } : a));
    const manual = open(analyse(extracts, { answers: skipped }).findings).filter((f) => /^The manual step was skipped/.test(f.notMeasurableReason!));
    assert.ok(manual.length > 0);
    for (const f of manual) assert.equal(f.notMeasurableKind, "needs_input");
    const calibration = { ...loadCalibration(), users: loadCalibration().users.filter((u) => u.kind !== "integration") };
    const integration = open(analyse(extracts, { calibration }).findings).filter((f) => /^No account is calibrated as an integration account/.test(f.notMeasurableReason!));
    assert.ok(integration.length > 0);
    for (const f of integration) assert.equal(f.notMeasurableKind, "needs_input");
    const seats = analyse().metrics.find((m) => m.id === "investment.seat_cost")!;
    assert.equal(seats.status === "not_measurable" && seats.notMeasurableKind, "needs_input");
  });

  test("too little data: a minimum the data can't meet", () => {
    const { findings, metrics } = analyse(extracts, { calibration: { ...loadCalibration(), minSample: 100_000 } });
    const thin = [...open(findings).map((f) => [f.ruleId, f.notMeasurableReason!, f.notMeasurableKind] as const), ...nmMetrics(metrics).map((m) => [m.id, m.reason, m.notMeasurableKind] as const)].filter(([, reason]) => /below the calibrated minimum/.test(reason) && !reason.includes(":"));
    assert.ok(new Set(thin.map(([id]) => id)).size >= 2, "at least two checks");
    for (const [id, , kind] of thin) assert.equal(kind, "too_little_data", id);
  });

  test("unreadable: dashboard use and forecast accuracy on the synthetic org", () => {
    const { findings } = analyse();
    for (const ruleId of ["utl.dashboard-usage", "fct.forecast-accuracy"]) assert.equal(findings.find((f) => f.ruleId === ruleId)!.notMeasurableKind, "unreadable", ruleId);
  });

  test("every not measurable finding and metric has a kind, and nothing else has one", () => {
    for (const out of [analyse(), analyse(extracts, { calibration: { ...loadCalibration(), minSample: 100_000 } })]) {
      for (const f of out.findings) assert.equal(f.notMeasurableKind !== undefined, f.status === "not_measurable", f.ruleId);
      for (const m of out.metrics) assert.equal(m.status === "not_measurable" ? m.notMeasurableKind !== undefined : !("notMeasurableKind" in m), true, m.id);
    }
  });
});

describe("a result written before kinds", () => {
  const findings = JSON.parse(readFileSync(join(dir, "findings.json"), "utf8")) as Finding[];
  const metrics = JSON.parse(readFileSync(join(dir, "metrics.json"), "utf8")) as Metric[];
  const strip = <T extends object>(x: T): T => JSON.parse(JSON.stringify(x, (k, v) => (k === "notMeasurableKind" ? undefined : v))) as T;

  test("still parses, finding, metric and score", () => {
    const old = strip(open(findings)[0]!);
    assert.equal(Finding.parse(old).notMeasurableKind, undefined);
    const metric = strip(metrics.find((m) => m.status === "not_measurable")!);
    assert.equal(Metric.parse(metric).status, "not_measurable");
    assert.ok(Score.parse({ status: "not_measurable", domain: "forecast", ruleSetVersion: "1", reason: "We couldn't measure this area." }));
  });

  test("a kind on anything but a not measurable finding is refused", () => {
    const passed = findings.find((f) => f.status === "passed")!;
    assert.throws(() => Finding.parse({ ...passed, notMeasurableKind: "not_in_use" }), /Only a not measurable finding has a kind/);
  });
});

// The area and overall scores of the fingerprint test's two runs before this
// change (base f6ec2b4), held here so a kind can never move a point.
const BEFORE: Record<string, { scores: Array<[string, number]>; overall: [number, string] }> = {
  recorded: {
    scores: [["configuration", 79], ["automation", 53], ["technical_health", 73], ["utilisation", 63], ["adoption", 68], ["data_quality", 65], ["historic_pipeline", 65], ["security", 64], ["integrations", 63], ["best_practice", 71], ["current_pipeline", 59], ["forecast", 65], ["activity", 72], ["lead_funnel", 71], ["revenue_opportunities", 72], ["service_operations", 71], ["service_setup", 73], ["cpq_setup", 77], ["cpq_quoting", 59], ["account_engagement", 69]],
    overall: [67, "C"],
  },
  generated: {
    scores: [["configuration", 79], ["automation", 53], ["technical_health", 73], ["utilisation", 69], ["adoption", 68], ["data_quality", 83], ["historic_pipeline", 93], ["security", 64], ["integrations", 66], ["best_practice", 80], ["current_pipeline", 70], ["forecast", 80], ["activity", 98], ["lead_funnel", 84], ["revenue_opportunities", 72], ["service_operations", 91], ["service_setup", 73], ["cpq_setup", 77], ["cpq_quoting", 70], ["account_engagement", 88]],
    overall: [76, "B"],
  },
};

describe("every score is as it was", () => {
  const generated = async () => (await runHealthCheck({ provider: syntheticProvider, connectedUser: CONNECTED_USER, calibration: loadCalibration(), answers: loadAnswers(), modules: ALL_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport: new Simulator({ records: buildScaleOrg(scaled(FIVE_HUNDRED_USERS, 0.005)), pageSize: 2000 }) })).dir;
  for (const [name, folder] of [["recorded", async () => dir], ["generated", generated]] as const) {
    test(`the ${name} run's area scores and overall score`, async () => {
      const at = await folder();
      const scores = JSON.parse(readFileSync(join(at, "scores.json"), "utf8")) as Score[];
      const findings = JSON.parse(readFileSync(join(at, "findings.json"), "utf8")) as Finding[];
      assert.deepEqual(scores.map((s) => [s.domain, s.status === "measured" ? s.score : null]), BEFORE[name]!.scores);
      const overall = overallScore(scores, findings);
      assert.deepEqual(overall.status === "measured" ? [overall.score, overall.grade] : null, BEFORE[name]!.overall);
    });
  }

  test("scoring with or without kinds gives the same status and score, wherever the kinds are benign", () => {
    const variants = [
      analyse(edit<{ types: { data: Array<{ IsActive: boolean }> } }>("forecast-quotas", (q) => q.types.data.forEach((t) => (t.IsActive = false)))).findings,
      analyse(extracts, { calibration: { ...loadCalibration(), minSample: 100_000 } }).findings,
    ];
    const domains = [...new Set(ALL_MODULES.flatMap((m) => m.rules.map((r) => r.domain)))];
    for (const findings of variants) {
      const without = findings.map((f) => {
        const { notMeasurableKind: _, ...rest } = f;
        return rest as Finding;
      });
      const shape = (s: Score) => [s.domain, s.status, s.status === "measured" ? s.score : null];
      assert.deepEqual(scoreDomains(domains, findings).map(shape), scoreDomains(domains, without).map(shape));
      assert.deepEqual(overallScore(scoreDomains(domains, findings), findings), overallScore(scoreDomains(domains, without), without));
    }
  });
});

describe("round 2: what may be called not in use", () => {
  const recordedFindings = JSON.parse(readFileSync(join(dir, "findings.json"), "utf8")) as Finding[];
  const PE = "Professional Edition has no Apex, so there's no code to check.";
  const asOpen = (f: Finding, reason: string, kind: Finding["notMeasurableKind"]): Finding => ({ ...f, status: "not_measurable", severity: "info", recommendationType: "none", recommendation: "", guards: [], notMeasurableReason: reason, notMeasurableKind: kind });
  const reasonOf = (s: Score) => (s.status === "not_measurable" ? [s.reason, s.notMeasurableKind] : ["measured"]);
  // The reviewer's case: on Professional Edition the Apex rules are not in use, and two rules still find issues.
  const APEX = new Set(["aut.old-api-versions", "aut.apex-coverage", "aut.multiple-triggers", "tech.test-quality", "tech.governor-limit-risk", "tech.apex-code-size"]);
  const tech = recordedFindings.filter((f) => f.domain === "technical_health");

  test("Professional Edition with two measured issues keeps the old reason, with no kind", () => {
    const seen = new Set<string>();
    const findings = tech.flatMap((f) => (APEX.has(f.ruleId) ? (seen.has(f.ruleId) ? [] : (seen.add(f.ruleId), [asOpen(f, PE, "not_in_use")])) : [f]));
    assert.ok(findings.some((f) => f.status === "issue"));
    assert.deepEqual(reasonOf(scoreDomain("technical_health", findings)), ["Too little of this area could be measured to score it fairly.", undefined]);
  });

  test("the rest unreadable keeps the old reason, with no kind", () => {
    const rules = [...new Set(tech.map((f) => f.ruleId))];
    const findings = tech.map((f) => (f.ruleId === rules[0] ? asOpen(f, PE, "not_in_use") : asOpen(f, "Users couldn't be read: INSUFFICIENT_ACCESS", "unreadable")));
    assert.deepEqual(reasonOf(scoreDomain("technical_health", findings)), ["We couldn't measure this area.", undefined]);
  });

  test("the rest too little data or needing something keeps the old reason too", () => {
    const rules = [...new Set(tech.map((f) => f.ruleId))];
    for (const kind of ["too_little_data", "needs_input"] as const) {
      const findings = tech.map((f) => (rules.indexOf(f.ruleId) < rules.length - 1 ? asOpen(f, PE, "not_in_use") : asOpen(f, "Something else.", kind)));
      assert.equal(reasonOf(scoreDomain("technical_health", findings))[1], undefined, kind);
    }
  });

  test("every rule not in use, or the rest passed: the area says so, with the kind", () => {
    const all = tech.map((f) => asOpen(f, PE, "not_in_use"));
    assert.deepEqual(reasonOf(scoreDomain("technical_health", all)), ["Nothing this area checks is in use in your org, so there is nothing to score.", "not_in_use"]);
    const passed = tech.find((f) => f.status === "passed") ?? { ...tech[0]!, status: "passed" as const, severity: "info" as const, recommendationType: "none" as const, recommendation: "", guards: [], segmentExemptReason: "test" };
    const onePass = [...all.filter((f) => f.ruleId !== passed.ruleId), { ...passed, notMeasurableKind: undefined }];
    assert.deepEqual(reasonOf(scoreDomain("technical_health", onePass)), ["Most of what this area checks isn't in use in your org, and the rest passed, so there is too little to score.", "not_in_use"]);
  });

  test("a field a describe leaves out is something we couldn't see, in its words and its kind", () => {
    const changed = edit<{ open: { missingFields?: string[] }; closedCases: { missingFields?: string[] } }>("service-cases", (c) => {
      c.open.missingFields = [...(c.open.missingFields ?? []), "Case.Origin"];
      c.closedCases.missingFields = [...(c.closedCases.missingFields ?? []), "Case.Origin"];
    });
    const origin = open(analyse(changed).findings).filter((f) => f.notMeasurableReason!.startsWith("Case.Origin"));
    assert.ok(origin.length > 0);
    for (const f of origin) {
      assert.match(f.notMeasurableReason!, /^Case\.Origin couldn't be seen in this org: it may not exist, or the connected user may not have access to it, so this can't be measured\./);
      assert.equal(f.notMeasurableKind, "unreadable", f.ruleId);
    }
  });

  test("targets: \"none\" in the calibration, and quotas with no revenue type, ask for the targets", () => {
    const none = open(analyse(extracts, { calibration: { ...loadCalibration(), targets: { source: "none" } } }).findings).filter((f) => f.notMeasurableReason!.startsWith("No targets were given"));
    assert.ok(none.length > 0);
    for (const f of none) {
      assert.equal(f.notMeasurableReason, "No targets were given in the calibration. Tell us your targets to see pipeline coverage.");
      assert.equal(f.notMeasurableKind, "needs_input");
    }
    const off = edit<{ types: { data: Array<{ IsActive: boolean }> } }>("forecast-quotas", (q) => q.types.data.forEach((t) => (t.IsActive = false)));
    const quota = open(analyse(off, { calibration: { ...loadCalibration(), targets: { source: "forecasting_quota" } } }).findings).filter((f) => f.domain === "current_pipeline" && /Forecasting quotas/.test(f.notMeasurableReason!));
    assert.ok(quota.length > 0);
    for (const f of quota) {
      assert.equal(f.notMeasurableReason, "The calibration keeps targets in Forecasting quotas, but no revenue forecast type is active, so there is no quota to read. Tell us where targets are kept.");
      assert.equal(f.notMeasurableKind, "needs_input");
    }
  });
});
