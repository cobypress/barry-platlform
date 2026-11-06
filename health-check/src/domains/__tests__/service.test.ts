import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { runHealthCheck } from "../../engine/run.ts";
import { escalationRulesExtractor, parseEscalationRules, serviceActivityExtractor, serviceCasesExtractor, serviceSetupExtractor, type ServiceActivity, type ServiceCases, type ServiceSetup } from "../../extract/service.ts";
import type { Sourced } from "../../extract/types.ts";
import { usersExtractor, type UserRow } from "../../extract/users.ts";
import type { Finding } from "../../schemas/index.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { AS_OF, CASE_QUEUES, sfId, userId, usersTable } from "../../testing/synthetic-org.ts";
import { ALL_MODULES, knownModuleIds, modulesForRun, SALES_CLOUD_MODULES, SERVICE_CLOUD_MODULES } from "../index.ts";
import { AREA_WEIGHTS } from "../scoring.ts";
import { recordAreasLine } from "../../narrative/select.ts";
import { SERVICE_SEAT } from "../service/setup.ts";

const SERVICE_RULES = SERVICE_CLOUD_MODULES.flatMap((m) => m.rules.map((r) => r.id));
const { output, dir } = await runFromRecording({ answers: loadAnswers(), modules: ALL_MODULES });
const findings = output!.findings;
const of = (list: Finding[], ruleId: string): Finding[] => list.filter((f) => f.ruleId === ruleId);
const one = (list: Finding[], ruleId: string): Finding => {
  const found = of(list, ruleId);
  assert.equal(found.length, 1, ruleId);
  return found[0]!;
};
const tokenOf = (n: number): string => `[[user:U${String(n).padStart(4, "0")}]]`;

describe("Service Cloud's modules", () => {
  test("run only when the run names them, and \"service\" brings in both", () => {
    // Security brings integrations, which grew out of it (DECISIONS.md 171).
    assert.deepEqual(modulesForRun(["configuration", "security"]).map((m) => m.id), ["configuration", "security", "integrations"]);
    assert.deepEqual(modulesForRun(["configuration", "service"]).map((m) => m.id), ["configuration", "service_operations", "service_setup"]);
    assert.deepEqual(modulesForRun(["service_setup"]).map((m) => m.id), ["service_setup"]);
    assert.ok(knownModuleIds().has("service"));
    assert.ok(!knownModuleIds(SALES_CLOUD_MODULES).has("service"));
  });

  test("a set-up review with Service Cloud says case handling is measured from records", () => {
    const line = recordAreasLine({ scope: "essentials", modules: ["configuration", "service_setup"] });
    assert.match(line ?? "", /service operations/);
    assert.doesNotMatch(recordAreasLine({ scope: "essentials", modules: ["configuration"] }) ?? "", /service/);
  });

  test("are scored, operations at 1.25 and set-up at 0.75", () => {
    assert.equal(AREA_WEIGHTS.service_operations, 1.25);
    assert.equal(AREA_WEIGHTS.service_setup, 0.75);
  });

  test("a Sales Cloud run doesn't touch them", async () => {
    const sales = await runFromRecording({ answers: loadAnswers() });
    assert.ok(!sales.output!.findings.some((f) => f.domain.startsWith("service") || f.ruleId.startsWith("svc.") || f.ruleId === "aut.case-routing-overlap"));
    assert.ok(!sales.run.modules.some((m) => m.startsWith("service")));
  });
});

describe("Service Cloud on the synthetic org", () => {
  test("every rule runs, and none is not measurable", () => {
    for (const id of SERVICE_RULES) assert.ok(of(findings, id).length > 0, id);
    assert.deepEqual(findings.filter((f) => SERVICE_RULES.includes(f.ruleId) && f.status === "not_measurable").map((f) => [f.ruleId, f.notMeasurableReason]), []);
  });

  test("each trap fires", () => {
    const issues = new Set(findings.filter((f) => f.status === "issue").map((f) => f.ruleId));
    for (const id of [
      "svc.backlog-growth",
      "svc.backlog-age",
      "svc.priority-resolution",
      "svc.milestone-breaches",
      "svc.reopened-cases",
      "svc.unassigned-cases",
      "svc.origin-missing",
      "svc.escalation-targets",
      "svc.queue-sprawl",
      "svc.omni-routing",
      "svc.agent-licences",
      "svc.knowledge-use",
      "svc.knowledge-current",
      "svc.macros-and-quick-text",
      "aut.case-routing-overlap",
    ]) {
      assert.ok(issues.has(id), id);
    }
    // And what isn't a trap passes.
    for (const id of ["svc.first-reply-email", "svc.case-classification", "svc.case-customer", "svc.escalation-rules"]) assert.equal(one(findings, id).status, "passed", id);
  });

  test("finds what was planted, and leaves out what wasn't a case queue", () => {
    assert.match(one(findings, "svc.queue-sprawl").summary, /^1 of 3 case queues has no members/);
    assert.deepEqual(one(findings, "svc.queue-sprawl").subjects, [{ type: "queue", label: "Old Returns" }]);
    assert.equal(one(findings, "svc.escalation-targets").facts.dead, 2);
    assert.equal(one(findings, "svc.agent-licences").facts.dormant, 2);
    assert.match(one(findings, "aut.case-routing-overlap").summary, /active case assignment rule/);
    assert.equal(one(findings, "aut.case-routing-overlap").domain, "automation");
  });

  test("a user the login log shows logging in is a dormant seat in neither count (DECISIONS.md 361, 394)", () => {
    // Ben Cross (15): LastLoginDate 150 days old, logging in since by the log. Ola Nowak (13): 220 days, and nothing in the log.
    const extracts = loadExtracts(dir);
    const setup = extracts.get(serviceSetupExtractor.id) as ServiceSetup;
    for (const n of [13, 15]) {
      const u = usersTable.find((x) => x.Id === userId(n))!;
      if (setup.supportUsers.status === "ok") setup.supportUsers.data.push({ Id: u.Id, LastLoginDate: u.LastLoginDate, Profile: { UserLicense: { Name: u.Profile.UserLicense.Name } } });
    }
    const both = analyseExtracts(extracts, { modules: ALL_MODULES }).findings;
    const dormantUsers = both.filter((f) => f.ruleId === "utl.dormant-licensed-users").flatMap((f) => f.subjects.map((s) => s.label));
    assert.ok(dormantUsers.includes(tokenOf(13)) && !dormantUsers.includes(tokenOf(15)), dormantUsers.join(", "));
    // Sam Reed (5) and Nina Hart (10) as before, and Ola Nowak; not Ben Cross.
    assert.equal(one(both, "svc.agent-licences").facts.dormant, 3);
  });

  test("a user set up inside the dormant period is too new to judge, in both counts (DECISIONS.md 386)", () => {
    // Eve Lane (16): a Service Cloud user created 10 days ago who hasn't logged in yet.
    const extracts = loadExtracts(dir);
    const users = extracts.get(usersExtractor.id) as Sourced<UserRow[]>;
    const setup = extracts.get(serviceSetupExtractor.id) as ServiceSetup;
    const nina = usersTable.find((x) => x.Id === userId(10))!;
    const eve = { ...clone(nina), Id: userId(16), Name: "Eve Lane", Username: "eve.lane@example.com", Email: "eve.lane@example.com", CreatedDate: new Date(Date.parse(AS_OF) - 10 * 86_400_000).toISOString().replace("Z", "+0000"), LastLoginDate: null };
    if (users.status === "ok") users.data.push(eve as unknown as UserRow);
    if (setup.supportUsers.status === "ok") setup.supportUsers.data.push({ Id: eve.Id, LastLoginDate: null, Profile: { UserLicense: { Name: eve.Profile.UserLicense.Name } } });
    const list = analyseExtracts(extracts, { modules: ALL_MODULES }).findings;
    const never = list.filter((f) => f.ruleId === "utl.never-logged-in").flatMap((f) => f.subjects.map((s) => s.label));
    assert.ok(never.includes(tokenOf(10)) && !never.includes(tokenOf(16)), never.join(", "));
    // Neither dormant nor idle: Sam Reed (5) and Nina Hart (10) as before.
    const before = one(findings, "svc.agent-licences").facts;
    const after = one(list, "svc.agent-licences").facts;
    assert.deepEqual([after.dormant, after.idle, after.users], [2, before.idle, (before.users as number) + 1]);
  });

  test("a finding names no person", () => {
    for (const f of findings.filter((x) => SERVICE_RULES.includes(x.ruleId))) assert.doesNotMatch(JSON.stringify(f), /@example\.com|Kim Fox|Sam Reed|Nina Hart/, f.ruleId);
  });

  test("money only where it's honest: licences with a seat price, nothing else", () => {
    const sized = findings.filter((f) => SERVICE_RULES.includes(f.ruleId) && f.value.kind !== "not_sized");
    assert.deepEqual(sized.map((f) => f.ruleId), []);
    assert.match(one(findings, "svc.agent-licences").value.kind === "not_sized" ? (one(findings, "svc.agent-licences").value as { reason: string }).reason : "", /no price/);
    const priced = analyseExtracts(loadExtracts(dir), { modules: ALL_MODULES, calibration: { ...loadCalibration(), licenceCosts: { [SERVICE_SEAT]: 1200 } } });
    const value = one(priced.findings, "svc.agent-licences").value;
    assert.deepEqual(value.kind === "licence_cost" ? [value.amount, value.unit, value.period] : value, [2400, "GBP", "per year"]);
  });

  test("its metrics are measured and segmented", () => {
    for (const id of ["service.open_cases", "service.resolution_days", "service.cases_created", "service.cases_by_origin", "service.routing_declined"]) {
      const m = output!.metrics.find((x) => x.id === id);
      assert.equal(m?.status, "measured", id);
    }
    const created = output!.metrics.find((m) => m.id === "service.cases_created")!;
    assert.ok(created.status === "measured" && created.series.length >= 12);
  });
});

/** The synthetic org with every Service Cloud trap taken out. */
function cleanOrg(extracts: Map<string, unknown>): Map<string, unknown> {
  const out = clone(extracts);
  const now = new Date(AS_OF).getTime();
  const iso = (ms: number): string => new Date(ms).toISOString().replace("Z", "+0000");
  const cases = out.get(serviceCasesExtractor.id) as ServiceCases;
  const ok = <T>(s: Sourced<T>): T => (s.status === "ok" ? s.data : assert.fail("unavailable"));
  if (cases.closed.status === "ok" && cases.created.status === "ok") cases.closed.data = clone(cases.created.data).map((r) => ({ ...r, n: r.n + 1 }));
  // Every Service Cloud user owns some of the open cases.
  ok(cases.open).forEach((c, i) => Object.assign(c, { CreatedDate: iso(now - 86_400_000 / 2), OwnerId: userId([1, 2, 4, 5, 10][i % 5]!) }));
  ok(cases.closedCases).forEach((c) => Object.assign(c, { Origin: c.Origin ?? "Web", ClosedDate: iso(Date.parse(c.CreatedDate) + (c.Priority === "High" ? 5 : 10) * 3_600_000) }));
  if (cases.origins.status === "ok") cases.origins.data = cases.origins.data.filter((o) => o.Origin);
  ok(cases.open).forEach((c) => (c.Origin = c.Origin ?? "Web"));
  const activity = out.get(serviceActivityExtractor.id) as ServiceActivity;
  if (activity.statusChanges.status === "ok") activity.statusChanges.data = activity.statusChanges.data.filter((h) => h.OldValue !== "Closed");
  ok(activity.milestones).forEach((m) => (m.IsViolated = false));
  if (activity.caseArticles.status === "ok") activity.caseArticles.data = ok(cases.closedCases).filter((_, i) => i % 4 === 0).map((c) => ({ CaseId: c.Id }));
  const setup = out.get(serviceSetupExtractor.id) as ServiceSetup;
  ok(setup.queues).forEach((q) => (q.QueueRoutingConfigId = q.QueueRoutingConfigId ?? sfId("0K3", 1)));
  if (setup.members.status === "ok") setup.members.data.push({ GroupId: CASE_QUEUES[2]!.Id, UserOrGroupId: userId(1) });
  ok(setup.supportUsers).forEach((u) => (u.LastLoginDate = iso(now - 86_400_000)));
  if (setup.macroUsage.status === "ok") setup.macroUsage.data = ok(setup.macros).map((m) => ({ MacroID: m.Id, n: 5 }));
  ok(setup.macros).forEach((m) => (m.IsLightningSupported = true));
  if (setup.quickTextUsage.status === "ok") setup.quickTextUsage.data = ok(setup.quickText).map((q) => ({ QuickTextID: q.Id, n: 5 }));
  ok(setup.articles).forEach((a) => (a.LastPublishedDate = iso(now - 30 * 86_400_000)));
  ok(setup.assignmentRules).forEach((r) => (r.Active = false));
  const rules = out.get(escalationRulesExtractor.id) as Sourced<ReturnType<typeof parseEscalationRules>>;
  ok(rules).forEach((r) => r.actions.forEach((a) => Object.assign(a, a.assignedToType === "Queue" ? { assignedTo: "Support_Tier_1" } : { assignedTo: "jo.ellis@example.com" })));
  return out;
}

describe("Service Cloud on a clean org", () => {
  test("stays quiet: every rule passes", () => {
    const clean = analyseExtracts(cleanOrg(loadExtracts(dir)), { modules: ALL_MODULES }).findings.filter((f) => SERVICE_RULES.includes(f.ruleId));
    assert.deepEqual(clean.filter((f) => f.status !== "passed").map((f) => [f.ruleId, f.status, f.summary ?? f.notMeasurableReason]), []);
  });

  test("never passes on a read that stopped at its row limit: it says the rest weren't read", () => {
    const cutAt = (pick: (out: Map<string, unknown>) => { truncated: boolean }) => {
      const out = cleanOrg(loadExtracts(dir));
      pick(out).truncated = true;
      return analyseExtracts(out, { modules: ALL_MODULES }).findings;
    };
    const activity = (out: Map<string, unknown>) => out.get(serviceActivityExtractor.id) as ServiceActivity;
    const caseRead = (out: Map<string, unknown>) => out.get(serviceCasesExtractor.id) as ServiceCases;
    const checks: Array<[string, (out: Map<string, unknown>) => { truncated: boolean }]> = [
      ["svc.milestone-breaches", (out) => activity(out).milestones as { truncated: boolean }],
      ["svc.reopened-cases", (out) => activity(out).statusChanges as { truncated: boolean }],
      ["svc.first-reply-email", (out) => activity(out).replies as { truncated: boolean }],
      ["svc.priority-resolution", (out) => caseRead(out).closedCases as { truncated: boolean }],
      ["svc.case-classification", (out) => caseRead(out).closedCases as { truncated: boolean }],
      ["svc.unassigned-cases", (out) => caseRead(out).open as { truncated: boolean }],
      ["svc.case-customer", (out) => caseRead(out).open as { truncated: boolean }],
    ];
    for (const [ruleId, pick] of checks) {
      const f = one(cutAt(pick), ruleId);
      assert.equal(f.status, "not_measurable", ruleId);
      assert.match(f.notMeasurableReason!, /stopped at its row limit/, ruleId);
    }
  });

  test("the calibrated limits move the line", () => {
    const loose = analyseExtracts(loadExtracts(dir), { modules: ALL_MODULES, calibration: { ...loadCalibration(), service: { backlogAgeDays: 500, originBlankShare: 20 } } }).findings;
    assert.equal(one(loose, "svc.backlog-age").status, "passed");
    assert.equal(one(loose, "svc.origin-missing").status, "passed");
    assert.equal(one(findings, "svc.backlog-age").facts.days, 30, "30 days by default");
  });
});

describe("Service Cloud where a feature is off", () => {
  test("says so, and doesn't score it as a pass", async () => {
    const off = /\bFROM (CaseMilestone|KnowledgeArticleVersion|ServiceChannel|MacroUsage)\b/;
    const transport = new Simulator({ failQuery: (soql) => (off.test(soql) ? { errorCode: "INVALID_TYPE", message: `sObject type '${off.exec(soql)![1]}' is not supported.` } : null) });
    const { output: out } = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: ALL_MODULES, outDir: tempDir(), now: new Date(AS_OF), answers: loadAnswers(), transport });
    const list = out!.findings;
    assert.equal(one(list, "svc.milestone-breaches").status, "not_measurable");
    assert.match(one(list, "svc.milestone-breaches").notMeasurableReason!, /CaseMilestone isn't available.*Entitlement management may not be turned on/);
    assert.match(one(list, "svc.knowledge-current").notMeasurableReason!, /Knowledge may not be turned on/);
    assert.match(one(list, "svc.omni-routing").notMeasurableReason!, /may not be turned on/);
    // Macro use unreadable: only what can be seen counts, at lower confidence.
    assert.equal(one(list, "svc.macros-and-quick-text").confidence, "medium");
  });

  test("a read-only connection can't read escalation rules, and the entitlement process still counts", async () => {
    const { output: out } = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: ALL_MODULES, outDir: tempDir(), now: new Date(AS_OF), answers: loadAnswers(), transport: new Simulator({ readOnly: true }) });
    assert.equal(one(out!.findings, "svc.escalation-rules").status, "passed");
    assert.match(one(out!.findings, "svc.escalation-rules").summary, /couldn't be read .*entitlement process/);
    assert.equal(one(out!.findings, "svc.escalation-targets").status, "not_measurable");
  });
});

describe("escalation rules from the Metadata API", () => {
  test("parse as the Metadata API guide's sample has them", () => {
    const xml = `<EscalationRules xmlns="http://soap.sforce.com/2006/04/metadata"><escalationRule><fullName>samplerule</fullName><active>false</active><ruleEntry><businessHoursSource>Static</businessHoursSource><escalationAction><assignedTo>someuser@example.com</assignedTo><assignedToType>User</assignedToType><minutesToEscalation>1440</minutesToEscalation><notifyCaseOwner>false</notifyCaseOwner></escalationAction><escalationStartTime>CaseLastModified</escalationStartTime></ruleEntry></escalationRule></EscalationRules>`;
    assert.deepEqual(parseEscalationRules(xml), [{ name: "samplerule", active: false, entries: 1, actions: [{ assignedTo: "someuser@example.com", assignedToType: "User", minutes: 1440, notifyTo: null }] }]);
    assert.deepEqual(parseEscalationRules(`<EscalationRules xmlns="http://soap.sforce.com/2006/04/metadata"/>`), []);
  });
});
