import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { ALL_MODULES } from "../../domains/index.ts";
import { scoreDomains } from "../../domains/scoring.ts";
import { NO_OWN_APEX } from "../../domains/automation/shared.ts";
import { readCheckpoint } from "../../engine/checkpoint.ts";
import { notMeasurableKind } from "../../engine/not-measurable.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { checkedDigest } from "../../digest/checked.ts";
import { isEditionNote } from "../../narrative/edition-note.ts";
import { buildReport } from "../../narrative/sections.ts";
import { executiveHtml } from "../../render/executive.ts";
import { escapeXml } from "../../render/format.ts";
import { fullReportHtml } from "../../render/full-report.ts";
import { onePagerHtml } from "../../render/one-pager.ts";
import { loadRun } from "../../render/render-run.ts";
import { ApiBudget, BudgetStopError } from "../../salesforce/budget.ts";
import { NotInOrgError, readGate, type GatedRead } from "../../salesforce/client.ts";
import { APEX_POINTER, EDITION_NOTE, editionIsWhy, METADATA_POINTER, NO_APEX_IN_EDITION, refusalSpans, TOOLING_AFTER_EDITION } from "../../salesforce/refusal-words.ts";
import type { Finding, OrgProfile, SkippedRead } from "../../schemas/index.ts";
import { automaticSyntheticCalibration } from "../../testing/automatic.ts";
import { tempDir } from "../../testing/harness.ts";
import { syntheticProvider } from "../../testing/replay.ts";
import { Simulator, type SimulatorOptions } from "../../testing/simulator.ts";
import { AS_OF, CONNECTED_USER } from "../../testing/synthetic-org.ts";
import { checkSections, decideRelease, unmeasuredRules } from "../../worker/release.ts";
import { TOOLING_SOURCE } from "../automation.ts";
import { EDITION_FACTS, GATED, gateFor, profileFromOrg, READ_NEEDS } from "../org-profile.ts";

// Know the org before reading it (DECISIONS.md 710): the profile from the
// Organization row, the one table of reads it rules out (the Metadata API's
// retrieves on Professional Edition, and nothing else), and a Professional
// Edition org run end to end.

const ok = (row: Record<string, unknown>) => ({ status: "ok", data: [row] });
const statusOf = (p: OrgProfile, id: string) => p.capabilities.find((c) => c.id === id)?.status;

const RETRIEVE: GatedRead = { kind: "retrieve", types: ["Flow"] };
/** Reads the gate never touches, Professional Edition or not: each is asked, and the org's answer is handled as always. */
const ASKED: GatedRead[] = [
  { kind: "query", api: "rest", soql: "SELECT Id, Name, Body FROM ApexClass WHERE NamespacePrefix = null" },
  { kind: "query", api: "rest", soql: "SELECT Id, Name, Body FROM ApexTrigger WHERE NamespacePrefix = null" },
  { kind: "query", api: "rest", soql: "SELECT Name FROM Scontrol WHERE NamespacePrefix = null" },
  { kind: "query", api: "rest", soql: "SELECT Name, AdminId FROM Site" },
  { kind: "query", api: "tooling", soql: "SELECT PercentCovered FROM ApexOrgWideCoverage" },
  { kind: "query", api: "rest", soql: "SELECT Id, JobType FROM AsyncApexJob" },
  { kind: "query", api: "tooling", soql: "SELECT Id, DeveloperName FROM FlowDefinition" },
  { kind: "query", api: "rest", soql: "SELECT Id FROM Opportunity" },
];

describe("the org profile is built from the Organization row, and never guesses", () => {
  test("Professional Edition: the Metadata API absent; Apex, s-controls and Sites expected absent and asked anyway", () => {
    const p = profileFromOrg(ok({ OrganizationType: "Professional Edition" }));
    assert.equal(p.edition, "Professional Edition");
    for (const id of EDITION_FACTS) assert.equal(statusOf(p, id), "absent", id);
    for (const c of p.capabilities.filter((c) => c.id !== "metadata_api" && (EDITION_FACTS as readonly string[]).includes(c.id))) assert.match(c.from, /We ask anyway/);
    assert.equal(statusOf(p, "multi_currency"), "unknown");
    assert.equal(statusOf(p, "record_types"), "unknown");
  });

  test("only exactly Professional Edition makes anything absent: every other edition, and none read, is present or unknown", () => {
    const editions = ["Developer Edition", "Enterprise Edition", "Unlimited Edition", "Performance Edition", "Essentials Edition", "Starter Edition", "Pro Suite", "Group Edition", "Team Edition", "Personal Edition", "professional edition", "Professional", "Mystery Edition"];
    for (const edition of editions) {
      const p = profileFromOrg(ok({ OrganizationType: edition }));
      assert.deepEqual(p.capabilities.filter((c) => c.status === "absent").map((c) => c.id), [], edition);
    }
    for (const edition of ["Developer Edition", "Enterprise Edition", "Unlimited Edition", "Performance Edition"]) assert.equal(statusOf(profileFromOrg(ok({ OrganizationType: edition })), "metadata_api"), "present", edition);
    for (const org of [ok({}), ok({ OrganizationType: "" }), ok({ OrganizationType: null }), { status: "unavailable" }, undefined]) {
      const p = profileFromOrg(org);
      assert.equal(p.edition, null);
      assert.deepEqual(p.capabilities.filter((c) => c.status === "absent"), [], JSON.stringify(org));
    }
  });
});

describe("the one table: only the Metadata API's retrieve is ever skipped", () => {
  test("the table holds the Metadata API alone", () => {
    assert.deepEqual([...GATED], ["metadata_api"]);
    assert.deepEqual(READ_NEEDS.map((n) => n.capability), ["metadata_api"]);
  });

  test("on Professional Edition the retrieve is skipped, with Salesforce's own code and our sentence, and every query is asked", () => {
    const noted: SkippedRead[] = [];
    const gate = gateFor(profileFromOrg(ok({ OrganizationType: "Professional Edition" })), "a-step", (s) => noted.push(s));
    const err = gate(RETRIEVE);
    assert.ok(err instanceof NotInOrgError);
    assert.equal(err.code, "INSUFFICIENT_ACCESS");
    assert.equal(err.sentence, METADATA_POINTER);
    for (const read of ASKED) assert.equal(gate(read), null, JSON.stringify(read));
    assert.deepEqual(noted, [{ step: "a-step", read: "Metadata API retrieve", capability: "metadata_api" }]);
  });

  test("unknown and present are read: no other edition skips anything", () => {
    for (const edition of [null, "Enterprise Edition", "Developer Edition", "Essentials Edition"]) {
      const gate = gateFor(profileFromOrg(edition === null ? ok({}) : ok({ OrganizationType: edition })), "a-step", () => assert.fail("nothing is skipped"));
      for (const read of [RETRIEVE, ...ASKED]) assert.equal(gate(read), null);
    }
  });

  test("the sentences are ours whole, short, and classed as truly as they can be", () => {
    for (const s of [METADATA_POINTER, APEX_POINTER]) {
      assert.deepEqual(refusalSpans(s), []);
      assert.ok(s.length <= 90 && editionIsWhy(s), s);
    }
    // An edition that keeps the Metadata API from us can still have the flows it would read: that stays "couldn't read".
    assert.equal(notMeasurableKind(METADATA_POINTER), "unreadable");
    assert.equal(notMeasurableKind(`Escalation rules couldn't be read: ${METADATA_POINTER}`), "unreadable");
    assert.equal(METADATA_POINTER, "Needs the Metadata API, which Professional Edition only opens to security-reviewed apps.");
    assert.equal(APEX_POINTER, "Needs Apex of the org's own, which Professional Edition doesn't allow.");
  });

  test("a reason is the edition's wherever a rule puts its own words first", () => {
    assert.ok(editionIsWhy(`Escalation rules couldn't be read: ${METADATA_POINTER}`));
    assert.ok(editionIsWhy(`Metadata couldn't be retrieved: ${TOOLING_AFTER_EDITION}The same set-up through the Tooling API couldn't be read, because it isn't available in this Salesforce edition or isn't switched on.`));
    assert.ok(editionIsWhy(NO_APEX_IN_EDITION));
    assert.ok(!editionIsWhy("Metadata couldn't be retrieved: The org's set-up from the Metadata API couldn't be read, because Salesforce refused this read."));
  });

  test("the metadata step's Tooling sentence on such an org is the one the words name", () => {
    const source = readFileSync(fileURLToPath(new URL("../automation.ts", import.meta.url)), "utf8");
    assert.ok(source.includes(`${TOOLING_AFTER_EDITION}\${tooling.reason}`));
  });

  test("the note is known as ours only as editionNoteFor writes it", () => {
    assert.ok(isEditionNote(EDITION_NOTE));
    assert.ok(isEditionNote(`${EDITION_NOTE} Your Barometer score leaves out the areas we couldn't score: Automation and Technical health.`));
    assert.ok(isEditionNote(`${EDITION_NOTE} Your Barometer score leaves out the areas we couldn't score: Automation.`));
    assert.ok(!isEditionNote(`${EDITION_NOTE} Your Barometer score leaves out the areas we couldn't score: Acme Widgets.`));
    assert.ok(!isEditionNote(`${EDITION_NOTE} Your Barometer score leaves out the areas we couldn't score: Automation, Technical health.`));
  });
});

// The synthetic org, live through the simulator, as an automatic run reads it.
const calibration = await automaticSyntheticCalibration();
const APEX_OWN = /FROM Apex(?:Class|Trigger) WHERE/;
async function runAs(organizationType: string | null, options: SimulatorOptions & { consultant?: boolean } = {}) {
  const { consultant, ...simOptions } = options;
  const sim = new Simulator({ organizationType, ...simOptions });
  const result = await runHealthCheck({ provider: syntheticProvider, connectedUser: CONNECTED_USER, calibration, modules: ALL_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport: sim, reviewMode: consultant ? "consultant" : "automatic", log: () => {} });
  const sent = sim.requests.map((r) => `${r.headers.SOAPAction ?? ""} ${decodeURIComponent(r.url)}`);
  return { ...result, sent, loaded: result.run.status === "stopped_budget" ? null! : loadRun(result.dir) };
}
const invalidType = (match: RegExp) => (soql: string) => (match.test(soql) ? { errorCode: "INVALID_TYPE", message: `sObject type '${/FROM (\w+)/.exec(soql)![1]}' is not supported.` } : null);
const professional = await runAs("Professional Edition");
const enterprise = await runAs("Enterprise Edition");
// Professional Edition answering that it has no Apex or s-controls of its own, as such an org may.
const noApex = await runAs("Professional Edition", { failQuery: invalidType(/FROM (?:Apex(?:Class|Trigger) WHERE|Scontrol\b)/) });
// The same answer on an edition we know nothing about: unread, never none.
const enterpriseNoScontrol = await runAs("Enterprise Edition", { failQuery: invalidType(/FROM Scontrol\b/) });
// Professional Edition where the Tooling API refuses too (the review's run).
const noTooling = await runAs("Professional Edition", { failQuery: invalidType(/FROM FlowDefinition\b/) });
const NOW = "2026-11-24T09:00:00.000Z";

const soap = (sent: string[]) => sent.filter((s) => /\/Soap\/m\//.test(s));
const byEdition = (findings: Finding[]) => findings.filter((f) => f.status === "not_measurable" && editionIsWhy(f.notMeasurableReason ?? ""));
const finding = (r: typeof professional, ruleId: string) => r.loaded.input.findings.find((f) => f.ruleId === ruleId)!;
const THREE = ["aut.undocumented-automation", "aut.mixed-automation-on-object", "aut.old-api-versions"];

describe("a Professional Edition org, run end to end", () => {
  test("the Metadata API's retrieve is never sent; the org's own Apex, s-controls and Sites are asked", () => {
    assert.deepEqual(soap(professional.sent), []);
    assert.ok(professional.sent.some((s) => APEX_OWN.test(s)));
    assert.ok(professional.sent.some((s) => /FROM Scontrol\b/.test(s)) && professional.sent.some((s) => /FROM Site\b/.test(s)));
    assert.ok(soap(enterprise.sent).length > 0);
  });

  test("the gate lasts only while each step runs: none is left after the run, and a later run in the same process reads the Metadata API", async () => {
    assert.equal(readGate.getStore(), undefined);
    const later = await runAs("Enterprise Edition");
    assert.ok(soap(later.sent).length > 0);
    assert.deepEqual(later.run.orgProfile!.skipped, []);
    // Each skip is noted against the step that made the read, never a step before it.
    for (const s of professional.run.orgProfile!.skipped) assert.ok(READ_NEEDS[0]!.steps.includes(s.step), s.step);
    assert.deepEqual(
      professional.run.orgProfile!.skipped.map((s) => s.step).sort(),
      ["ai-signals", "metadata", "pardot-setup", "report-definitions", "service-escalation-rules"],
    );
  });

  test("run.json keeps the profile; skipped reads aren't refusals", () => {
    const profile = professional.run.orgProfile!;
    assert.equal(profile.edition, "Professional Edition");
    assert.ok(!(professional.run.readsRefused ?? []).some((r) => /Metadata API/.test(r.read)));
    assert.equal(statusOf(profile, "record_types"), "present");
    assert.equal(statusOf(profile, "multi_currency"), "absent");
  });

  test("the set-up is read through Tooling and reports through the Analytics API", () => {
    const bundle = readCheckpoint<{ status: string; source: unknown }>(join(professional.dir, "checkpoints", "metadata.json"));
    assert.equal(bundle.status, "ok");
    assert.deepEqual(bundle.source, TOOLING_SOURCE);
    const reports = readCheckpoint<{ status: string; data?: { route: string } }>(join(professional.dir, "checkpoints", "report-definitions.json"));
    assert.equal(reports.data?.route, "analytics");
  });

  test("the three automation checks that also read flows and rules are measured, with Apex rows or none", () => {
    for (const run of [professional, noApex]) for (const id of THREE) assert.notEqual(finding(run, id).status, "not_measurable", `${id}: ${finding(run, id).notMeasurableReason}`);
    // Where the org answered it has no Apex of its own, the figure says only that part wasn't there.
    for (const id of THREE) assert.ok(finding(noApex, id).evidence.some((e) => e.exclusions.includes(NO_OWN_APEX)), id);
    // The Apex-only checks say so in our short words.
    assert.equal(finding(noApex, "tech.apex-code-size").notMeasurableReason, APEX_POINTER);
  });

  test("s-controls are none only where we asked and Professional Edition answered; unread is never none", () => {
    const pe = finding(noApex, "tech.classic-leftovers");
    assert.equal(pe.facts.scontrols, 0);
    assert.ok(!pe.summary.includes("couldn't be read"));
    const ee = finding(enterpriseNoScontrol, "tech.classic-leftovers");
    assert.equal(ee.facts.scontrols, undefined);
    assert.notEqual(ee.status, "passed");
    assert.ok(ee.evidence.some((e) => /Scontrol/.test(JSON.stringify(e)) && e.count === null));
  });

  test("checks the edition leaves unmeasured are never issues, never scored, and nothing else is lost", () => {
    const findings = professional.loaded.input.findings;
    const edition = byEdition(findings);
    assert.ok(edition.length > 0);
    const scored = new Set(scoreDomains(professional.run.modules, findings).flatMap((s) => (s.status === "measured" ? s.findingIds : [])));
    for (const f of edition) assert.ok(f.status === "not_measurable" && !scored.has(f.id), f.id);
    const before = new Map(enterprise.loaded.input.findings.map((f) => [f.id, f]));
    const lost = findings.filter((f) => f.status === "not_measurable" && before.get(f.id)?.status !== "not_measurable" && !editionIsWhy(f.notMeasurableReason ?? ""));
    assert.deepEqual(lost.map((f) => `${f.ruleId}: ${f.notMeasurableReason}`), []);
  });

  test("the automatic outcome: its digest traces whole and it is released", () => {
    const report = buildReport(professional.loaded.input);
    checkSections(report, professional.loaded.pseudonyms);
    const sections = Object.values(report.outputs).flatMap((o) => o.sections);
    const checked = checkedDigest({ input: professional.loaded.input, pseudonyms: professional.loaded.pseudonyms, inventory: null, madeAt: NOW });
    // The live simulator's Enterprise run masks the same three details (a service metric's assumptions it dates differently
    // from the recording, where the digest traces 0/0/0): the edition adds none.
    const plain = checkedDigest({ input: enterprise.loaded.input, pseudonyms: enterprise.loaded.pseudonyms, inventory: null, madeAt: NOW });
    assert.deepEqual({ masks: checked.masks, unmatched: checked.unmatched, unmarked: checked.unmarked, held: checked.held }, { masks: plain.masks, unmatched: 0, unmarked: 0, held: null });
    for (const run of [noApex, noTooling]) {
      const c = checkedDigest({ input: run.loaded.input, pseudonyms: run.loaded.pseudonyms, inventory: null, madeAt: NOW });
      assert.deepEqual({ masks: c.masks, unmatched: c.unmatched, unmarked: c.unmarked, held: c.held }, { masks: plain.masks, unmatched: 0, unmarked: 0, held: null });
    }
    const findings = professional.loaded.input.findings;
    const d = decideRelease({ runId: "pe", sections, checked, findings, overall: null, previousScore: null, now: NOW });
    assert.equal(d.kind, "release");
    assert.ok((unmeasuredRules(findings).edition ?? 0) > 0);
    assert.equal(unmeasuredRules(enterprise.loaded.input.findings).edition, undefined);
  });

  test("when the Tooling API refuses too, the hold's count names every rule the edition left unmeasured", () => {
    const findings = noTooling.loaded.input.findings;
    const rules = new Map<string, boolean>();
    for (const f of findings) rules.set(f.ruleId, (rules.get(f.ruleId) ?? true) && f.status === "not_measurable" && /Professional Edition/.test(f.notMeasurableReason ?? ""));
    const expected = [...rules.values()].filter(Boolean).length;
    assert.ok(expected >= 10, String(expected));
    assert.equal(unmeasuredRules(findings).edition, expected);
  });

  test("a score from fewer areas says which it leaves out, once, in the note", () => {
    const report = buildReport(noTooling.loaded.input);
    const digest = checkedDigest({ input: noTooling.loaded.input, pseudonyms: noTooling.loaded.pseudonyms, inventory: null, madeAt: NOW }).digest!;
    assert.equal(report.overall.status, "measured");
    const left = (noTooling.loaded.input.scores ?? []).filter((s) => s.status !== "measured");
    assert.ok(left.some((s) => s.domain === "automation"));
    assert.match(digest.editionNote!, /^This org is on Professional Edition, which only opens the Metadata API to security-reviewed apps, so checks that need it are marked not measured\. Your Barometer score leaves out the areas we couldn't score: .*Automation/);
    assert.ok(isEditionNote(digest.editionNote!));
    assert.deepEqual(digest.orgWords?.editionNote, { orgWordsMarked: true, orgWords: [] });
  });

  test("the digest says the edition once, marked ours; each check only points at it; an org with everything has none", () => {
    const digest = checkedDigest({ input: professional.loaded.input, pseudonyms: professional.loaded.pseudonyms, inventory: null, madeAt: NOW }).digest!;
    assert.ok(digest.editionNote!.startsWith(EDITION_NOTE));
    const text = JSON.stringify(digest);
    assert.equal(text.split(EDITION_NOTE).length - 1, 1, "the note is said once");
    for (const f of digest.findings.filter((f) => f.notMeasurableReason?.includes(METADATA_POINTER))) assert.ok(f.notMeasurableReason!.length <= 160, f.notMeasurableReason!);
    const plain = checkedDigest({ input: enterprise.loaded.input, pseudonyms: enterprise.loaded.pseudonyms, inventory: null, madeAt: NOW }).digest!;
    assert.equal(plain.editionNote, undefined);
    assert.equal(plain.orgWords, undefined);
  });

  test("each page that prints such a check carries the note once, and no other page does", () => {
    let pages = 0;
    for (const run of [professional, noTooling]) {
      const report = buildReport(run.loaded.input);
      const note = escapeXml(checkedDigest({ input: run.loaded.input, pseudonyms: run.loaded.pseudonyms, inventory: null, madeAt: NOW }).digest!.editionNote!);
      for (const html of [fullReportHtml(report, run.loaded.pseudonyms), executiveHtml(report, run.loaded.pseudonyms), onePagerHtml(report, run.loaded.pseudonyms)]) {
        assert.ok(!html.includes("Not counted: Apex triggers, which couldn&#39;t be read: Needs"));
        for (const page of html.split(/(?=<section class="page)/).slice(1)) {
          const notes = page.split(note).length - 1;
          const points = page.includes(escapeXml(METADATA_POINTER)) || page.includes(escapeXml(TOOLING_AFTER_EDITION));
          assert.equal(notes, points ? 1 : 0);
          pages += notes;
        }
      }
    }
    assert.ok(pages > 0);
    assert.ok(!fullReportHtml(buildReport(enterprise.loaded.input), enterprise.loaded.pseudonyms).includes("edition-note"));
  });

  test("an org with no Apex of its own is said without a doubled stop or a capital after a colon", () => {
    const html = fullReportHtml(buildReport(noApex.loaded.input), noApex.loaded.pseudonyms);
    const counted = [...html.matchAll(/Not counted: [^<]*/g)].map((m) => m[0]);
    assert.ok(counted.includes("Not counted: Apex triggers, which Professional Edition doesn&#39;t allow the org to have of its own."), counted.join("\n"));
    for (const line of counted) assert.ok(!/\.\.$/.test(line) && !/: Needs/.test(line), line);
  });

  test("a reason that ends its own sentence isn't given a second full stop after \"Not counted:\"", () => {
    const input = structuredClone(noApex.loaded.input);
    const inventory = input.metrics.find((m) => m.id === "configuration.object_inventory" && m.status === "measured");
    assert.ok(inventory && inventory.status === "measured");
    inventory.exclusions.push("Page layouts, which couldn't be read: Layout isn't available in this org, or to the connected user.");
    const html = fullReportHtml(buildReport(input), noApex.loaded.pseudonyms);
    // The page says the reason in its customer words, which end the sentence: one full stop, never two.
    const line = /Not counted: Page layouts, which couldn&#39;t be read: [^<]*</.exec(html)?.[0] ?? "";
    assert.ok(line.endsWith(".<") && !line.endsWith("..<"), line);
  });

  test("a removal's guard isn't blocked by Apex the org was asked for and doesn't have", () => {
    const guards = noApex.loaded.input.findings.flatMap((f) => f.guards);
    assert.ok(!guards.some((g) => /Metadata or Apex couldn't be read/.test(g.detail)));
  });

  test("what the customer reads names the edition in our words, and nowhere quotes a code", () => {
    const report = buildReport(professional.loaded.input);
    const pages = [fullReportHtml(report, professional.loaded.pseudonyms), executiveHtml(report, professional.loaded.pseudonyms), onePagerHtml(report, professional.loaded.pseudonyms)].join("\n");
    assert.ok(!/INSUFFICIENT_ACCESS|INVALID_TYPE|Not sent: the org profile/.test(pages));
    assert.ok(!/INSUFFICIENT_ACCESS|INVALID_TYPE|Not sent: the org profile/.test(JSON.stringify(professional.loaded.input.findings)));
  });
});

describe("an automatic run spends at most a tenth of the org's daily limit", () => {
  test("on a small org the automatic run stops at its budget; a consultant-led one doesn't", async () => {
    const auto = await runAs("Professional Edition", { dailyLimit: 1_500, used: 0 });
    assert.equal(auto.run.status, "stopped_budget");
    assert.match(auto.run.budget.stoppedReason ?? "", /10% of the org's 1500 daily API calls/);
    assert.ok(auto.run.budget.callsMade <= 150);
    const led = await runAs("Professional Edition", { dailyLimit: 1_500, used: 0, consultant: true });
    assert.notEqual(led.run.status, "stopped_budget");
  });

  test("the budget refuses the call that would pass the share", () => {
    const budget = new ApiBudget({ stopAtShare: 0.5, maxShareOfLimit: 0.1 });
    budget.observe(0, 1_000);
    assert.doesNotThrow(() => budget.assertCanSpend(100));
    assert.throws(() => budget.assertCanSpend(101), BudgetStopError);
  });
});

describe("702 is intact: an unknown edition is read, and a refusal costs one check", () => {
  test("an edition we don't know reads the Metadata API; refused, it costs that read and the run goes on", async () => {
    const odd = await runAs("Mystery Edition", { failRetrieve: () => ({ faultCode: "API_DISABLED_FOR_ORG", message: "x" }) });
    assert.ok(soap(odd.sent).length > 0);
    assert.notEqual(odd.run.status, "failed");
    assert.ok((odd.run.readsRefused ?? []).some((r) => r.code === "API_DISABLED_FOR_ORG"));
    assert.deepEqual(odd.run.orgProfile!.skipped, []);
  });

  test("a row without OrganizationType: nothing skipped, everything read", async () => {
    const blank = await runAs(null);
    assert.equal(blank.run.orgProfile!.edition, null);
    assert.deepEqual(blank.run.orgProfile!.skipped, []);
    assert.ok(soap(blank.sent).length > 0);
  });
});
