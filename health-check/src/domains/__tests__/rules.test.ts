import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { SALES_CLOUD_MODULES } from "../index.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { userId } from "../../testing/synthetic-org.ts";
import { apexExtractor, metadataExtractor } from "../../extract/automation.ts";
import { activityCaptureExtractor, licencesExtractor, ownershipExtractor, usersExtractor } from "../../extract/users.ts";
import { duplicateRulesExtractor } from "../../extract/configuration.ts";
import { leadsExtractor } from "../../extract/leads.ts";
import { opportunitiesExtractor } from "../../extract/opportunities.ts";
import { Finding } from "../../schemas/index.ts";
import { USER_GUARD_IDS } from "../../guards/user-guards.ts";

let extracts: Map<string, unknown>;
let tokenOf: (n: number) => string;

before(async () => {
  const { dir } = await runFromRecording();
  extracts = loadExtracts(dir);
  // Tokens follow id order, so user N is token N.
  tokenOf = (n) => `[[user:U${String(n).padStart(4, "0")}]]`;
});

const byRule = (findings: Finding[], ruleId: string): Finding[] => findings.filter((f) => f.ruleId === ruleId);
const issue = (findings: Finding[], ruleId: string): Finding => {
  const found = findings.find((f) => f.ruleId === ruleId && f.status === "issue");
  assert.ok(found, `${ruleId} should raise an issue`);
  return found;
};

describe("the rule set", () => {
  test("has at least 15 rules, with each milestone domain covered", () => {
    const rules = SALES_CLOUD_MODULES.flatMap((m) => m.rules);
    assert.ok(rules.length >= 15, `${rules.length} rules`);
    const domains = new Set(rules.map((r) => r.domain));
    for (const domain of ["configuration", "automation", "utilisation", "data_quality"]) assert.ok(domains.has(domain), domain);
  });

  test("rule ids are unique", () => {
    const ids = SALES_CLOUD_MODULES.flatMap((m) => m.rules.map((r) => r.id));
    assert.equal(new Set(ids).size, ids.length);
  });

  test("every rule produced at least one finding", () => {
    const { findings } = analyseExtracts(extracts);
    for (const rule of SALES_CLOUD_MODULES.flatMap((m) => m.rules)) assert.ok(byRule(findings, rule.id).length > 0, rule.id);
  });

  test("every finding validates and has evidence, owner and a value position", () => {
    for (const finding of analyseExtracts(extracts, { answers: loadAnswers() }).findings) {
      const parsed = Finding.parse(finding);
      assert.ok(parsed.evidence.length > 0, finding.ruleId);
      assert.ok(parsed.owner, finding.ruleId);
    }
  });

  test("no run reports a rule failure", () => {
    const { findings } = analyseExtracts(extracts, { answers: loadAnswers() });
    assert.deepEqual(findings.filter((f) => f.notMeasurableReason?.startsWith("This check failed")).map((f) => f.notMeasurableReason), []);
  });
});

describe("configuration", () => {
  test("custom field sprawl names the object over the limit", () => {
    const f = issue(analyseExtracts(extracts).findings, "cfg.custom-field-sprawl");
    assert.equal(f.facts.Account, 156);
  });

  test("a field filling up on new records isn't called unused", () => {
    const findings = analyseExtracts(extracts).findings;
    const lead = findings.find((f) => f.ruleId === "cfg.unused-custom-fields" && f.summary.includes("on Lead"))!;
    assert.deepEqual(lead.subjects.map((s) => s.label), ["Lead.Old_Campaign_Ref__c"]);
  });

  test("field removal stays investigate until reports can be scanned", () => {
    const fields = byRule(analyseExtracts(extracts).findings, "cfg.unused-custom-fields").filter((f) => f.status === "issue");
    assert.ok(fields.length > 0);
    for (const f of fields) {
      assert.equal(f.recommendationType, "investigate");
      assert.equal(f.value.kind, "not_sized");
    }
  });

  test("a field used in a flow fails its guard, an external id fails its guard", () => {
    const findings = analyseExtracts(extracts).findings;
    const guards = findings.filter((f) => f.ruleId === "cfg.unused-custom-fields").flatMap((f) => f.guards);
    const of = (subject: string, id: string) => guards.find((g) => g.subject === subject && g.guardId === id)?.status;
    assert.equal(of("Opportunity.Legacy_Code__c", "not-referenced-in-automation"), "failed");
    assert.equal(of("Opportunity.Old_Region__c", "not-referenced-in-automation"), "passed");
    assert.equal(of("Lead.Old_Campaign_Ref__c", "not-an-integration-key"), "failed");
  });

  test("unassigned layouts", () => assert.deepEqual(issue(analyseExtracts(extracts).findings, "cfg.unassigned-layouts").subjects.map((s) => s.label), ["Old Opportunity Layout", "Partner Lead Layout"]));
  test("a stage nobody uses", () => assert.deepEqual(issue(analyseExtracts(extracts).findings, "cfg.stage-sprawl").subjects.map((s) => s.label), ["Legal Review"]));
  test("long picklists", () => assert.equal(issue(analyseExtracts(extracts).findings, "cfg.picklist-sprawl").facts["Lead.LeadSource"], 30));

  test("setup concentration ignores user admin, which is filtered in code", () => {
    const f = issue(analyseExtracts(extracts).findings, "cfg.setup-change-concentration");
    assert.equal(f.facts.changes, 30);
    assert.equal(f.facts.topShare, 90);
    assert.ok(f.evidence[0]!.exclusions.includes("8 Manage Users changes"));
    assert.ok(f.summary.includes(tokenOf(4)));
  });
});

describe("automation", () => {
  test("workflow rules count active rules only", () => assert.equal(issue(analyseExtracts(extracts).findings, "aut.legacy-workflow-rules").facts.activeRules, 2));
  test("process builders", () => assert.equal(issue(analyseExtracts(extracts).findings, "aut.legacy-process-builder").facts.activeProcesses, 1));
  test("mixed automation sees Process Builder through processMetadataValues", () => {
    const f = issue(analyseExtracts(extracts).findings, "aut.mixed-automation-on-object");
    assert.match(f.summary, /Opportunity runs Apex trigger, Process Builder, Record-triggered flow, Workflow Rule/);
  });
  test("hardcoded ids ignore draft flows", () => {
    const f = issue(analyseExtracts(extracts).findings, "aut.hardcoded-ids-in-flows");
    assert.deepEqual(f.subjects.map((s) => s.label).sort(), ["Opportunity_Route_Deal", "Renewal_Notice"]);
  });
  test("fault paths: flows that have them aren't flagged", () => {
    const f = issue(analyseExtracts(extracts).findings, "aut.flows-without-fault-paths");
    assert.ok(!f.subjects.some((s) => s.label === "Lead_Assign"));
  });
  test("old API versions", () => {
    const f = issue(analyseExtracts(extracts).findings, "aut.old-api-versions");
    assert.equal(f.facts["Apex classes"], 1);
    assert.equal(f.facts["Active flows"], 1);
  });
  test("a healthy coverage total doesn't hide a weak class", () => {
    const f = issue(analyseExtracts(extracts).findings, "aut.apex-coverage");
    assert.equal(f.facts.percent, 82);
    assert.deepEqual(f.subjects.map((s) => s.label), ["LegacySync"]);
  });
  test("no coverage figure is not measurable, and we don't run tests to get one", () => {
    const changed = clone(extracts);
    (changed.get(apexExtractor.id) as { orgCoverage: { data: unknown[] } }).orgCoverage.data = [{ PercentCovered: null }];
    const [f] = byRule(analyseExtracts(changed).findings, "aut.apex-coverage");
    assert.equal(f?.status, "not_measurable");
    assert.match(f?.notMeasurableReason ?? "", /don't run tests/);
  });
  test("a stored 0% everywhere is missing figures, not untested code", () => {
    const changed = clone(extracts);
    const apex = changed.get(apexExtractor.id) as { orgCoverage: { data: unknown[] }; classCoverage: { data: Array<{ covered: number }> } };
    apex.orgCoverage.data = [{ PercentCovered: 0 }];
    // One class keeps its figure, as on the real org where this was seen.
    apex.classCoverage.data.forEach((c, i) => (c.covered = i === 0 ? c.covered : 0));
    const [f] = byRule(analyseExtracts(changed).findings, "aut.apex-coverage");
    assert.equal(f?.status, "not_measurable");
    assert.match(f?.notMeasurableReason ?? "", /cleared or only some tests have run/);
  });
  test("two triggers on one object", () => assert.deepEqual(issue(analyseExtracts(extracts).findings, "aut.multiple-triggers").subjects.map((s) => s.label), ["Opportunity"]));
  test("unreadable metadata is not measurable, not a clean pass", () => {
    const changed = clone(extracts);
    const source = (changed.get(metadataExtractor.id) as { source: unknown }).source;
    changed.set(metadataExtractor.id, { status: "unavailable", reason: "INSUFFICIENT_ACCESS: no", source });
    const findings = analyseExtracts(changed).findings;
    for (const id of ["aut.legacy-workflow-rules", "aut.legacy-process-builder", "aut.hardcoded-ids-in-flows"]) {
      assert.equal(byRule(findings, id)[0]?.status, "not_measurable", id);
    }
  });
});

describe("licence guards (rule 3)", () => {
  const blockedGuards = (answers = loadAnswers()) => analyseExtracts(extracts, { answers }).findings.filter((f) => f.ruleId === "utl.dormant-licensed-users").flatMap((f) => f.guards);
  const status = (n: number, guardId: string, answers = loadAnswers()): string | undefined => blockedGuards(answers).find((g) => g.subject === tokenOf(n) && g.guardId === guardId)?.status;

  test("with no manual answer, nothing is recommended for removal", () => {
    const { findings } = analyseExtracts(extracts);
    assert.deepEqual(findings.filter((f) => f.recommendationType === "deactivate"), []);
    const blocked = findings.find((f) => f.ruleId === "utl.dormant-licensed-users")!;
    assert.equal(blocked.recommendationType, "investigate");
    assert.equal(blocked.value.kind, "not_sized");
  });

  test("blocked licences say what's in the way in plain words, never a guard id", () => {
    for (const answers of [undefined, loadAnswers()]) {
      const blocked = analyseExtracts(extracts, { answers }).findings.find((f) => f.ruleId === "utl.dormant-licensed-users" && f.recommendationType === "investigate")!;
      for (const id of USER_GUARD_IDS) assert.ok(!blocked.summary.includes(id), `${id} in: ${blocked.summary}`);
    }
    const blocked = analyseExtracts(extracts, { answers: loadAnswers() }).findings.find((f) => f.ruleId === "utl.dormant-licensed-users" && f.recommendationType === "investigate")!;
    assert.match(blocked.summary, /still owns records \(\d+\)/);
    // With no answer, the Default Workflow User check can't finish, which is counted, not named.
    const unanswered = analyseExtracts(extracts).findings.find((f) => f.ruleId === "utl.dormant-licensed-users" && f.recommendationType === "investigate")!;
    assert.match(unanswered.summary, /couldn't complete every check/);
    assert.doesNotMatch(unanswered.summary, /Default Workflow User/);
  });

  test("the one clean dormant user is the only recommendation, with a saving", () => {
    const clear = analyseExtracts(extracts, { answers: loadAnswers() }).findings.find((f) => f.ruleId === "utl.dormant-licensed-users" && f.recommendationType === "deactivate")!;
    assert.deepEqual(clear.subjects.map((s) => s.label), [tokenOf(5)]);
    assert.ok(clear.guards.every((g) => g.status === "passed"));
    assert.ok(clear.value.kind === "licence_cost" && clear.value.amount === 1200);
    assert.ok(clear.value.kind === "licence_cost" && clear.value.assumptions.length > 0);
  });

  test("a shared user who owns most accounts is never recommended", () => {
    const findings = analyseExtracts(extracts, { answers: loadAnswers() }).findings;
    const offered = findings.filter((f) => f.recommendationType === "deactivate").flatMap((f) => f.subjects.map((s) => s.label));
    assert.ok(!offered.includes(tokenOf(6)));
    assert.equal(status(6, "owns-no-records"), "failed");
    assert.equal(status(6, "not-shared-or-system"), "failed");
    assert.match(blockedGuards().find((g) => g.subject === tokenOf(6) && g.guardId === "owns-no-records")!.detail, /310 Account/);
  });

  test("referenced in automation", () => assert.equal(status(7, "not-referenced-in-automation"), "failed"));
  test("the default workflow user", () => assert.equal(status(8, "not-default-workflow-user"), "failed"));
  test("a manager of active users", () => assert.equal(status(12, "not-in-user-hierarchy"), "failed"));
  test("the owner of a scheduled job", () => assert.equal(status(13, "runs-no-automation"), "failed"));
  test("LoginHistory that disagrees with LastLoginDate", () => assert.equal(status(15, "no-recent-login"), "failed"));
  test("failed logins aren't logins", () => assert.equal(status(5, "no-recent-login"), "passed"));

  test("an incomplete ownership scan blocks, rather than reading as no records", () => {
    const changed = clone(extracts);
    (changed.get(ownershipExtractor.id) as { incomplete: string[] }).incomplete = ["Case"];
    const { findings } = analyseExtracts(changed, { answers: loadAnswers() });
    assert.deepEqual(findings.filter((f) => f.recommendationType === "deactivate"), []);
    const guard = findings.flatMap((f) => f.guards).find((g) => g.subject === tokenOf(5) && g.guardId === "owns-no-records");
    assert.equal(guard?.status, "unknown");
  });

  test("unreadable metadata blocks the same way", () => {
    const changed = clone(extracts);
    const source = (changed.get(metadataExtractor.id) as { source: unknown }).source;
    changed.set(metadataExtractor.id, { status: "unavailable", reason: "no", source });
    assert.deepEqual(analyseExtracts(changed, { answers: loadAnswers() }).findings.filter((f) => f.recommendationType === "deactivate"), []);
  });

  test("a calibrated person overrides the shared-name pattern", () => {
    const calibration = loadCalibration();
    calibration.users = calibration.users.filter((u) => u.userId !== userId(6));
    calibration.users.push({ userId: userId(6), kind: "sales_rep" });
    const guards = analyseExtracts(extracts, { answers: loadAnswers(), calibration }).findings.flatMap((f) => f.guards);
    assert.equal(guards.find((g) => g.subject === tokenOf(6) && g.guardId === "not-shared-or-system")?.status, "passed");
    assert.equal(guards.find((g) => g.subject === tokenOf(6) && g.guardId === "owns-no-records")?.status, "failed");
  });

  test("no licence cost means no saving, rather than a guess", () => {
    const calibration = loadCalibration();
    calibration.licenceCosts = {};
    const clear = analyseExtracts(extracts, { answers: loadAnswers(), calibration }).findings.find((f) => f.recommendationType === "deactivate")!;
    assert.equal(clear.value.kind, "not_sized");
  });
});

describe("utilisation", () => {
  test("spare seats ignore free licence types and use the calibrated cost", () => {
    const f = issue(analyseExtracts(extracts).findings, "utl.unassigned-licence-seats");
    assert.equal(f.facts.seats, 15);
    assert.ok(f.value.kind === "licence_cost" && f.value.amount === 18_000);
    assert.ok(!f.summary.includes("Chatter"));
  });
  test("spare seats of a licence that comes free with the edition aren't a cost", () => {
    // As seen on a real org: 4 of 5 Salesforce Integration seats spare, which costs nothing.
    const changed = clone(extracts);
    (changed.get(licencesExtractor.id) as { userLicenses: { data: unknown[] } }).userLicenses.data.push({ Name: "Salesforce Integration", TotalLicenses: 5, UsedLicenses: 1, Status: "Active" });
    const users = (changed.get(usersExtractor.id) as { data: Array<{ IsActive: boolean; Profile: unknown }> }).data;
    users.find((u) => u.IsActive)!.Profile = { Name: "Minimum Access - API Only Integrations", UserLicense: { Name: "Salesforce Integration" } };
    const f = issue(analyseExtracts(changed).findings, "utl.unassigned-licence-seats");
    assert.ok(!f.summary.includes("Salesforce Integration"), f.summary);
    const priced = clone(changed);
    const calibration = { ...loadCalibration(), licenceCosts: { ...loadCalibration().licenceCosts, "Salesforce Integration": 120 } };
    assert.match(issue(analyseExtracts(priced, { calibration }).findings, "utl.unassigned-licence-seats").summary, /Salesforce Integration/, "a priced one still counts");
  });
  test("an integration user who has never logged in isn't a licence to release", () => {
    // As seen on a real org: the Sales Insights integration user, which is API-only.
    const changed = clone(extracts);
    const users = (changed.get(usersExtractor.id) as { data: Array<{ Id: string; IsActive: boolean; LastLoginDate: string | null; CreatedDate: string; Profile: unknown }> }).data;
    const integration = users.find((u) => u.IsActive && u.LastLoginDate !== null)!;
    integration.LastLoginDate = null;
    integration.CreatedDate = "2020-01-01T00:00:00.000Z";
    integration.Profile = { Name: "Sales Insights Integration User", UserLicense: { Name: "Sales Insights Integration User" } };
    const subjects = byRule(analyseExtracts(changed, { answers: loadAnswers() }).findings, "utl.never-logged-in").flatMap((f) => f.subjects.map((s) => s.label));
    integration.Profile = { Name: "Standard User", UserLicense: { Name: "Salesforce" } };
    const control = byRule(analyseExtracts(changed, { answers: loadAnswers() }).findings, "utl.never-logged-in").flatMap((f) => f.subjects.map((s) => s.label));
    assert.equal(control.length, subjects.length + 1, "the same user on a Salesforce licence is still checked");
  });
  test("reports too new to judge are excluded", () => {
    const f = issue(analyseExtracts(extracts).findings, "utl.unused-reports");
    assert.equal(f.facts.reports, 79);
    assert.equal(f.facts.unused, 59);
  });
  test("dashboards running as an inactive user", () => assert.deepEqual(issue(analyseExtracts(extracts).findings, "utl.dashboards-inactive-running-user").subjects.map((s) => s.label), ["EU Pipeline"]));
  test("dashboard use is honestly not measurable", () => {
    const [f] = byRule(analyseExtracts(extracts).findings, "utl.dashboard-usage");
    assert.equal(f?.status, "not_measurable");
    assert.match(f?.notMeasurableReason ?? "", /person running the check/);
  });
  test("a broken activity connection", () => assert.deepEqual(issue(analyseExtracts(extracts).findings, "utl.eac-connections").subjects.map((s) => s.label), [tokenOf(3)]));
  test("activity capture that isn't switched on gets one plain line, not a query error", () => {
    const changed = clone(extracts);
    const source = (changed.get(activityCaptureExtractor.id) as { source: unknown }).source;
    changed.set(activityCaptureExtractor.id, { status: "unavailable", reason: "ActivityUsrConnectionStatus isn't available in this org, or to the connected user.", source });
    const findings = analyseExtracts(changed, { answers: [] }).findings;
    for (const id of ["utl.eac-connections", "utl.eac-advanced-sync"]) {
      const [f] = byRule(findings, id);
      assert.equal(f?.status, "not_measurable");
      assert.match(f?.notMeasurableReason ?? "", /^Einstein Activity Capture isn't switched on/);
    }
  });
});

describe("segment before concluding (rule 4)", () => {
  test("a total under the limit still fails on the segment underneath it", () => {
    const f = issue(analyseExtracts(extracts).findings, "dq.open-opps-missing-amount");
    assert.ok(f.facts.rate! <= 10, `total is ${f.facts.rate}%`);
    assert.ok(f.facts.worstSegmentRate! > 30);
    assert.match(f.summary, /The total looks acceptable, but by record type, Renewal/);
  });

  test("stale leads: the total looks fine, one source doesn't", () => {
    const f = issue(analyseExtracts(extracts).findings, "dq.stale-leads");
    assert.ok(f.facts.rate! <= 30);
    assert.deepEqual(f.subjects.map((s) => s.label), ["Partner"]);
  });

  test("without its segments, stale leads won't pass", () => {
    const changed = clone(extracts);
    const health = changed.get("crm-health") as { staleLeadsBySource: unknown };
    health.staleLeadsBySource = { status: "unavailable", reason: "no", source: { kind: "soql", api: "rest", query: "x" } };
    assert.equal(byRule(analyseExtracts(changed).findings, "dq.stale-leads")[0]?.status, "not_measurable");
  });

  test("pipeline: a collapsed team is found under an ordinary win rate", () => {
    const findings = analyseExtracts(extracts).findings.filter((f) => f.ruleId === "pipe.segment-divergence" && f.status === "issue");
    const eu = findings.find((f) => f.subjects[0]?.label === "Sales EU");
    assert.ok(eu, "Sales EU should be flagged");
    assert.ok(eu.facts.gap! >= 15);
  });

  test("every pass shows what it segmented, or says why it couldn't", () => {
    for (const f of analyseExtracts(extracts, { answers: loadAnswers() }).findings.filter((x) => x.status === "passed")) {
      assert.ok(f.segmentsChecked.length > 0 || f.segmentExemptReason, f.ruleId);
    }
  });
});

describe("data quality", () => {
  test("past close counts deals and sizes the potential revenue in them", () => {
    const f = issue(analyseExtracts(extracts).findings, "dq.open-opps-past-close");
    assert.ok(f.value.kind === "revenue" && f.value.amount > 0);
    assert.ok(f.value.kind === "revenue" && f.value.assumptions.some((a) => a.startsWith("Raw exposure: ")));
    assert.ok(f.value.kind === "revenue" && f.value.assumptions.some((a) => /that were open past their close date at some point, \d+ went on to win/.test(a)));
  });
  test("any open deal with an inactive owner is an issue", () => assert.ok(issue(analyseExtracts(extracts).findings, "dq.opps-inactive-owner").facts.hits! > 0));
  test("loss reasons need a calibrated field, not a guess", () => {
    const changed = clone(extracts);
    (changed.get(opportunitiesExtractor.id) as { lossReasonField: string | null }).lossReasonField = null;
    assert.equal(byRule(analyseExtracts(changed).findings, "dq.loss-reason-missing")[0]?.status, "not_measurable");
  });
  test("records with inactive owners", () => assert.equal(issue(analyseExtracts(extracts).findings, "dq.records-inactive-owner").facts.strandedTotal, 190));
  test("undocumented automation is counted by type, with tests and hidden code left out", () => {
    const f = issue(analyseExtracts(extracts).findings, "aut.undocumented-automation");
    // Flows 3 of 4, Process Builders 1 of 1, Workflow Rules 2 of 2, validation rules 0 of 2,
    // code 5 of 7: the commented class and trigger count, the test class doesn't.
    // The inactive validation rule on Invoice__c isn't counted.
    assert.deepEqual(f.facts, { total: 16, missing: 11, rate: 68.8 });
    assert.match(f.summary, /Validation rules: 0 of 2/);
    assert.match(f.summary, /Apex classes and triggers: 5 of 7/);
  });
  test("an edition with no Apex says so, rather than that the read failed", () => {
    const changed = clone(extracts);
    const apex = changed.get(apexExtractor.id) as { triggers: Record<string, unknown> & { source: unknown } };
    apex.triggers = { status: "unavailable", reason: "Professional Edition has no Apex, so there's no code to check.", source: apex.triggers.source };
    const [f] = byRule(analyseExtracts(changed).findings, "aut.multiple-triggers");
    assert.equal(f?.notMeasurableReason, "Professional Edition has no Apex, so there's no code to check.");
  });
  test("documented automation passes", () => {
    const changed = clone(extracts);
    const metadata = changed.get(metadataExtractor.id) as { data: { flows: Array<{ described: boolean }>; workflows: Array<{ rules: Array<{ described: boolean }> }> } };
    for (const f of metadata.data.flows) f.described = true;
    for (const r of metadata.data.workflows.flatMap((w) => w.rules)) r.described = true;
    const apex = changed.get(apexExtractor.id) as { classes: { data: Array<{ commented: boolean | null }> }; triggers: { data: Array<{ commented: boolean | null }> } };
    for (const c of [...apex.classes.data, ...apex.triggers.data]) c.commented = true;
    assert.equal(byRule(analyseExtracts(changed).findings, "aut.undocumented-automation")[0]?.status, "passed");
  });
  test("custom fields with no help text or description", () => {
    const f = issue(analyseExtracts(extracts).findings, "cfg.undocumented-fields");
    // Opportunity 3 of 4 bare, Invoice__c 2 of 3, Project__c 1 of 1.
    assert.deepEqual(f.facts, { fields: 8, bare: 6, rate: 75, noHelpText: 7 });
    assert.deepEqual(f.subjects.map((s) => s.label), ["Opportunity", "Invoice__c", "Project__c"]);
  });
  test("duplicate rules only count when active", () => assert.deepEqual(issue(analyseExtracts(extracts).findings, "dq.duplicate-rules-missing").subjects.map((s) => s.label), ["Account", "Contact"]));
  test("a missing duplicate rule on Lead isn't raised when no leads are created", () => {
    // As seen on a real org that doesn't use leads.
    const changed = clone(extracts);
    const rules = (changed.get(duplicateRulesExtractor.id) as { data: Array<{ SobjectType: string; IsActive: boolean }> }).data;
    for (const r of rules) if (r.SobjectType === "Lead") r.IsActive = false;
    assert.deepEqual(issue(analyseExtracts(changed).findings, "dq.duplicate-rules-missing").subjects.map((s) => s.label), ["Account", "Contact", "Lead"]);
    (changed.get(leadsExtractor.id) as { leads: { data: unknown[] } }).leads.data = [];
    const f = issue(analyseExtracts(changed).findings, "dq.duplicate-rules-missing");
    assert.deepEqual(f.subjects.map((s) => s.label), ["Account", "Contact"]);
    assert.match(f.summary, /no leads were created/);
  });
  test("contact gaps", () => assert.equal(issue(analyseExtracts(extracts).findings, "dq.contact-completeness").facts.emailRate, 16));
});

describe("manual steps", () => {
  test("two real steps are raised, each with what a consultant needs", () => {
    const { manualSteps } = analyseExtracts(extracts);
    assert.deepEqual(manualSteps.map((s) => s.key).sort(), ["default-workflow-user", "eac-advanced-sync"]);
    for (const step of manualSteps) {
      assert.equal(step.status, "pending");
      assert.ok(step.whyItMatters && step.whereToGo.startsWith("Setup >") && step.whatToCapture && step.fields.length > 0);
      assert.deepEqual([...step.answerMethods].sort(), ["fields", "paste", "screenshot"]);
      assert.ok(step.ruleIds.length > 0);
    }
  });

  test("pending: the related finding is not measurable", () => {
    const [f] = byRule(analyseExtracts(extracts).findings, "utl.eac-advanced-sync");
    assert.equal(f?.status, "not_measurable");
    assert.deepEqual(f?.manualStepIds, ["ms-eac-advanced-sync"]);
    assert.equal(f?.evidence[0]?.kind, "manual");
  });

  test("answered: becomes evidence labelled manual, with who and when", () => {
    const f = issue(analyseExtracts(extracts, { answers: loadAnswers() }).findings, "utl.eac-advanced-sync");
    const evidence = f.evidence[0]!;
    assert.ok(evidence.kind === "manual" && evidence.status === "answered");
    assert.ok(evidence.kind === "manual" && evidence.answeredBy === "Synthetic Consultant");
    assert.ok(evidence.kind === "manual" && evidence.answeredAt?.startsWith("2026-10-30"));
  });

  const skipped = (stepKey: string) => [{ stepKey, outcome: "skipped" as const, values: {}, skipReason: "Admin unavailable", answeredBy: "Synthetic Consultant", answeredAt: "2026-10-30T10:00:00.000Z" }];

  test("skipped: the related finding is not measurable, and says why", () => {
    const [f] = byRule(analyseExtracts(extracts, { answers: skipped("eac-advanced-sync") }).findings, "utl.eac-advanced-sync");
    assert.equal(f?.status, "not_measurable");
    assert.match(f?.notMeasurableReason ?? "", /skipped: Admin unavailable/);
  });

  test("skipped default workflow user: release of the clean users is not measurable, and real blockers still show", () => {
    const findings = byRule(analyseExtracts(extracts, { answers: skipped("default-workflow-user") }).findings, "utl.dormant-licensed-users");
    const open = findings.find((f) => f.status === "not_measurable");
    // Sam is clean, and Lee's only blocker was the answer that's now missing.
    assert.match(open?.notMeasurableReason ?? "", /2 users can be released can't be measured/);
    assert.ok(findings.some((f) => f.status === "issue" && f.recommendationType === "investigate"));
    assert.ok(!findings.some((f) => f.recommendationType === "deactivate"));
  });

  test("the feature not being in use raises no step", () => {
    const changed = clone(extracts);
    (changed.get("activity-capture") as { data: unknown[] }).data = [];
    const { manualSteps, findings } = analyseExtracts(changed);
    assert.ok(!manualSteps.some((s) => s.key === "eac-advanced-sync"));
    assert.equal(byRule(findings, "utl.eac-advanced-sync")[0]?.status, "passed");
  });

  test("not being able to read the feature still raises the step", () => {
    const changed = clone(extracts);
    changed.set("activity-capture", { status: "unavailable", reason: "INVALID_TYPE: nope", source: { kind: "soql", api: "rest", query: "x" } });
    assert.ok(analyseExtracts(changed).manualSteps.some((s) => s.key === "eac-advanced-sync"));
  });
});

describe("calibration against the org", () => {
  test("a stage missing from the calibration is an issue", () => {
    const calibration = loadCalibration();
    calibration.stages = calibration.stages.filter((s) => s.name !== "Closed No Decision");
    const f = issue(analyseExtracts(extracts, { calibration }).findings, "pipe.unmapped-stages");
    assert.deepEqual(f.subjects.map((s) => s.label), ["Closed No Decision"]);
  });

  test("a calibration that contradicts Salesforce's flags is called out", () => {
    const calibration = loadCalibration();
    calibration.stages = calibration.stages.map((s) => (s.name === "Closed Lost" ? { ...s, category: "won" as const } : s));
    const f = issue(analyseExtracts(extracts, { calibration }).findings, "pipe.calibration-contradicts-org");
    assert.match(f.summary, /Closed Lost is calibrated as won/);
  });
});

describe("fiscal year", () => {
  test("a calibration that disagrees with the org is called out", () => {
    const calibration = { ...loadCalibration(), fiscalYearStartMonth: 4 };
    const f = issue(analyseExtracts(extracts, { calibration }).findings, "pipe.calibration-contradicts-org");
    assert.match(f.summary, /starts the fiscal year in month 4, but the org starts it in month 1/);
  });
});

describe("repeatability", () => {
  test("the same inputs give the same findings", () => {
    const first = JSON.stringify(analyseExtracts(extracts, { answers: loadAnswers() }));
    const second = JSON.stringify(analyseExtracts(extracts, { answers: loadAnswers() }));
    assert.equal(first, second);
  });

  test("finding ids are stable, so a repeat check can line up with its baseline", () => {
    const a = analyseExtracts(extracts).findings.map((f) => f.id);
    const b = analyseExtracts(clone(extracts)).findings.map((f) => f.id);
    assert.deepEqual(a, b);
    assert.equal(new Set(a).size, a.length, "ids are unique");
  });
});
