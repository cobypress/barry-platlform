import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { join } from "node:path";
import { ALL_MODULES } from "../../domains/index.ts";
import { scoreDomains } from "../../domains/scoring.ts";
import { readCheckpoint } from "../../engine/checkpoint.ts";
import { notMeasurableKind } from "../../engine/not-measurable.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { checkedDigest } from "../../digest/checked.ts";
import { buildReport } from "../../narrative/sections.ts";
import { executiveHtml } from "../../render/executive.ts";
import { fullReportHtml } from "../../render/full-report.ts";
import { onePagerHtml } from "../../render/one-pager.ts";
import { loadRun } from "../../render/render-run.ts";
import { NotInOrgError, type GatedRead } from "../../salesforce/client.ts";
import { EDITION_POINTER_STEM, editionIsWhy, editionNote, editionPointer, isEditionNote, NO_APEX_IN_EDITION, refusalSpans, TOOLING_AFTER_EDITION } from "../../salesforce/refusal-words.ts";
import { escapeXml } from "../../render/format.ts";
import type { Finding, OrgProfile, SkippedRead } from "../../schemas/index.ts";
import { automaticSyntheticCalibration } from "../../testing/automatic.ts";
import { tempDir } from "../../testing/harness.ts";
import { syntheticProvider } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { AS_OF, CONNECTED_USER } from "../../testing/synthetic-org.ts";
import { checkSections, decideRelease, unmeasuredRules } from "../../worker/release.ts";
import { TOOLING_SOURCE } from "../automation.ts";
import { GATED, gateFor, profileFromOrg, READ_NEEDS } from "../org-profile.ts";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

// Know the org before reading it (DECISIONS.md 710): the profile from the
// Organization row, the one table of reads it rules out, and a Professional
// Edition org run end to end.

const ok = (row: Record<string, unknown>) => ({ status: "ok", data: [row] });
const statusOf = (p: OrgProfile, id: string) => p.capabilities.find((c) => c.id === id)?.status;

/** One read of each kind the table knows, and reads it must never touch. */
const READS: Record<string, GatedRead> = {
  retrieve: { kind: "retrieve", types: ["Flow"] },
  ownClasses: { kind: "query", api: "rest", soql: "SELECT Id, Name, Body FROM ApexClass WHERE NamespacePrefix = null" },
  ownTriggers: { kind: "query", api: "rest", soql: "SELECT Id, Name, Body FROM ApexTrigger WHERE NamespacePrefix = null" },
  scontrols: { kind: "query", api: "rest", soql: "SELECT Name FROM Scontrol WHERE NamespacePrefix = null" },
  sites: { kind: "query", api: "rest", soql: "SELECT Name, AdminId FROM Site" },
};
const UNTOUCHED: GatedRead[] = [
  { kind: "query", api: "tooling", soql: "SELECT PercentCovered FROM ApexOrgWideCoverage" },
  { kind: "query", api: "rest", soql: "SELECT Id, JobType FROM AsyncApexJob" },
  { kind: "query", api: "rest", soql: "SELECT Id, Name FROM ApexClass" },
  { kind: "query", api: "tooling", soql: "SELECT Id, DeveloperName FROM FlowDefinition" },
  { kind: "query", api: "rest", soql: "SELECT Id FROM Opportunity" },
];
const NEED_OF: Record<string, (typeof GATED)[number]> = { retrieve: "metadata_api", ownClasses: "own_apex", ownTriggers: "own_apex", scontrols: "s_controls", sites: "sites" };

describe("the org profile is built from the Organization row, and never guesses", () => {
  test("Professional Edition: each gated capability absent, and why, from the edition", () => {
    const p = profileFromOrg(ok({ OrganizationType: "Professional Edition" }));
    assert.equal(p.edition, "Professional Edition");
    for (const id of GATED) assert.equal(statusOf(p, id), "absent", id);
    assert.ok(p.capabilities.every((c) => c.from.length > 0));
    // What the run learns later starts unknown.
    assert.equal(statusOf(p, "multi_currency"), "unknown");
    assert.equal(statusOf(p, "record_types"), "unknown");
  });

  test("Enterprise Edition: the Metadata API present, the rest unknown, so all are read", () => {
    const p = profileFromOrg(ok({ OrganizationType: "Enterprise Edition" }));
    assert.equal(statusOf(p, "metadata_api"), "present");
    for (const id of ["own_apex", "s_controls", "sites"]) assert.equal(statusOf(p, id), "unknown", id);
  });

  test("a missing, empty or unread edition, or one we don't know, is unknown: never absent", () => {
    for (const org of [ok({}), ok({ OrganizationType: "" }), ok({ OrganizationType: null }), { status: "unavailable" }, undefined, ok({ OrganizationType: "Pro Suite" }), ok({ OrganizationType: "professional edition" })]) {
      const p = profileFromOrg(org);
      for (const id of GATED) assert.notEqual(statusOf(p, id), "absent", `${JSON.stringify(org)} ${id}`);
    }
    assert.equal(profileFromOrg(ok({})).edition, null);
  });
});

describe("the one table: only an absent capability's reads are skipped", () => {
  for (const id of GATED) {
    test(`${id} absent on its own skips its reads alone, with Salesforce's own code and our sentence`, () => {
      const profile: OrgProfile = { edition: "Test", capabilities: GATED.map((c) => ({ id: c, status: c === id ? "absent" : "present", from: "test" })), skipped: [] };
      const noted: SkippedRead[] = [];
      const gate = gateFor(profile, "a-step", (s) => noted.push(s));
      for (const [name, read] of Object.entries(READS)) {
        const err = gate(read);
        if (NEED_OF[name] === id) {
          assert.ok(err instanceof NotInOrgError, name);
          assert.equal(err.capability, id);
          assert.equal(err.code, id === "metadata_api" ? "INSUFFICIENT_ACCESS" : "INVALID_TYPE");
        } else assert.equal(err, null, name);
      }
      for (const read of UNTOUCHED) assert.equal(gate(read), null, JSON.stringify(read));
      assert.ok(noted.length > 0 && noted.every((n) => n.capability === id && n.step === "a-step"));
    });
  }

  test("unknown is read: a profile with every capability unknown skips nothing", () => {
    const profile = profileFromOrg(ok({}));
    const gate = gateFor(profile, "a-step", () => assert.fail("nothing is skipped"));
    for (const read of [...Object.values(READS), ...UNTOUCHED]) assert.equal(gate(read), null);
  });

  test("every row's sentence is ours whole, said the edition way, and classed as truly as it can be", () => {
    for (const need of READ_NEEDS) {
      assert.ok(refusalSpans(need.sentence) !== null, need.sentence);
      assert.ok(editionIsWhy(need.sentence), need.sentence);
    }
    // An edition with no Metadata API can still have the flows it would read: that stays "couldn't read".
    assert.equal(notMeasurableKind(editionPointer("the Metadata API")), "unreadable");
    assert.equal(notMeasurableKind(editionPointer("s-controls")), "not_in_use");
    assert.equal(notMeasurableKind(editionPointer("Apex")), "not_in_use");
    // All ours: a pointer names no object of the org's.
    assert.deepEqual(refusalSpans(editionPointer("Sites")), []);
    // Short, since it stands on many checks, and never the run's note itself.
    for (const need of READ_NEEDS) assert.ok(need.sentence.length <= 70 && !isEditionNote(need.sentence), need.sentence);
  });

  test("the metadata step's Tooling sentence on such an org is the one release counts", () => {
    const source = readFileSync(fileURLToPath(new URL("../automation.ts", import.meta.url)), "utf8");
    assert.ok(source.includes(`${TOOLING_AFTER_EDITION}\${tooling.reason}`));
  });
});

// A Professional Edition org, live through the simulator, as an automatic run reads it.
const calibration = await automaticSyntheticCalibration();
async function runAs(organizationType: string | null, options: { failRetrieve?: boolean } = {}) {
  const sim = new Simulator({
    organizationType,
    ...(options.failRetrieve ? { failRetrieve: () => ({ faultCode: "API_DISABLED_FOR_ORG", message: "x" }) } : {}),
  });
  const result = await runHealthCheck({ provider: syntheticProvider, connectedUser: CONNECTED_USER, calibration, modules: ALL_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport: sim, reviewMode: "automatic", log: () => {} });
  const sent = sim.requests.map((r) => `${r.headers.SOAPAction ?? ""} ${decodeURIComponent(r.url)}`);
  return { ...result, sent, loaded: loadRun(result.dir) };
}
const professional = await runAs("Professional Edition");
const enterprise = await runAs("Enterprise Edition");
const NOW = "2026-11-24T09:00:00.000Z";

const soap = (sent: string[]) => sent.filter((s) => /\/Soap\/m\//.test(s));
const ownApex = (sent: string[]) => sent.filter((s) => /FROM Apex(?:Class|Trigger) WHERE NamespacePrefix = null/.test(s) && !/\/tooling\//.test(s));
const byEdition = (findings: Finding[]) => findings.filter((f) => f.status === "not_measurable" && editionIsWhy(f.notMeasurableReason ?? ""));

describe("the run's one edition note", () => {
  test("names the edition and what it doesn't include, and is known as ours only whole", () => {
    assert.equal(editionNote(["Apex", "the Metadata API", "s-controls", "Sites"]), "This org is on Professional Edition, which doesn't include Apex, the Metadata API, s-controls or Sites. Checks that need them are marked not measured.");
    assert.equal(editionNote(["Sites"]), "This org is on Professional Edition, which doesn't include Sites. Checks that need it are marked not measured.");
    assert.equal(editionNote([]), null);
    assert.ok(isEditionNote(editionNote(["Apex", "Sites"])!));
    assert.ok(!isEditionNote("This org is on Professional Edition, which doesn't include Acme Widgets. Checks that need it are marked not measured."));
  });
});

describe("a Professional Edition org, run end to end", () => {
  test("no read it rules out is sent; everything else is", () => {
    assert.deepEqual(soap(professional.sent), []);
    assert.deepEqual(ownApex(professional.sent), []);
    assert.ok(!professional.sent.some((s) => /FROM (?:Scontrol|Site)\b/.test(s)));
    // Unknown is tried: the same org saying Enterprise Edition makes each of these reads.
    assert.ok(soap(enterprise.sent).length > 0);
    assert.ok(ownApex(enterprise.sent).length > 0);
    assert.ok(enterprise.sent.some((s) => /FROM Scontrol\b/.test(s)) && enterprise.sent.some((s) => /FROM Site\b/.test(s)));
  });

  test("run.json keeps the profile and each read skipped, by a step the table names for it", () => {
    const profile = professional.run.orgProfile!;
    assert.equal(profile.edition, "Professional Edition");
    for (const id of GATED) assert.equal(statusOf(profile, id), "absent");
    const steps = new Set(profile.skipped.map((s) => `${s.capability} ${s.step}`));
    for (const s of profile.skipped) assert.ok(READ_NEEDS.find((n) => n.capability === s.capability)!.steps.includes(s.step), `${s.step} isn't in the table for ${s.capability}`);
    for (const want of ["metadata_api metadata", "metadata_api report-definitions", "metadata_api service-escalation-rules", "metadata_api ai-signals", "own_apex apex", "s_controls classic", "sites run-as"]) assert.ok(steps.has(want), want);
    // Skipped reads aren't refusals: none is in readsRefused.
    assert.ok(!(professional.run.readsRefused ?? []).some((r) => /Metadata API|ApexClass|ApexTrigger|Scontrol|Site/.test(r.read)));
    // Learned during the run: the synthetic org has record types and one currency.
    assert.equal(statusOf(profile, "record_types"), "present");
    assert.equal(statusOf(profile, "multi_currency"), "absent");
    assert.deepEqual(enterprise.run.orgProfile!.skipped, []);
  });

  test("the set-up is read through Tooling and reports through the Analytics API, as for a connection with no Metadata API", () => {
    const bundle = readCheckpoint<{ status: string; source: unknown }>(join(professional.dir, "checkpoints", "metadata.json"));
    assert.equal(bundle.status, "ok");
    assert.deepEqual(bundle.source, TOOLING_SOURCE);
    const reports = readCheckpoint<{ status: string; data?: { route: string } }>(join(professional.dir, "checkpoints", "report-definitions.json"));
    assert.equal(reports.data?.route, "analytics");
  });

  test("checks the edition rules out are not measured, never issues, never scored, and say the edition is why", () => {
    const findings = professional.loaded.input.findings;
    const edition = byEdition(findings);
    assert.ok(edition.length > 0);
    const scored = new Set(scoreDomains(professional.run.modules, findings).flatMap((s) => (s.status === "measured" ? s.findingIds : [])));
    for (const f of edition) {
      assert.equal(f.status, "not_measurable");
      assert.ok(!scored.has(f.id), f.id);
    }
    // Every finding the edition left unmeasured was measured, or a refusal, on the same org saying Enterprise Edition.
    const before = new Map(enterprise.loaded.input.findings.map((f) => [f.id, f]));
    for (const f of edition) assert.ok(before.has(f.id), f.id);
    // Nothing else moved from measured to not: the edition cost only what it rules out.
    const lost = findings.filter((f) => f.status === "not_measurable" && before.get(f.id)?.status !== "not_measurable" && !editionIsWhy(f.notMeasurableReason ?? ""));
    assert.deepEqual(lost.map((f) => `${f.ruleId}: ${f.notMeasurableReason}`), []);
  });

  test("the automatic outcome: its digest traces, and it is released or held with the edition counted", () => {
    const report = buildReport(professional.loaded.input);
    checkSections(report, professional.loaded.pseudonyms);
    const sections = Object.values(report.outputs).flatMap((o) => o.sections);
    const checked = checkedDigest({ input: professional.loaded.input, pseudonyms: professional.loaded.pseudonyms, inventory: null, madeAt: NOW });
    assert.equal(checked.held, null, `held: ${JSON.stringify({ masks: checked.masks, unmatched: checked.unmatched, unmarked: checked.unmarked })}`);
    const findings = professional.loaded.input.findings;
    const unmeasured = unmeasuredRules(findings);
    const d = decideRelease({ runId: "pe", sections, checked, findings, overall: null, previousScore: null, now: NOW });
    if (unmeasured.notMeasurable * 2 > unmeasured.rules) assert.deepEqual(d.kind === "hold" && [d.reason, d.counts.edition], ["mostly_unmeasured", unmeasured.edition]);
    else assert.equal(d.kind, "release");
    assert.ok((unmeasured.edition ?? 0) > 0);
    // The org that has everything carries no edition count, so its flag is as before.
    assert.equal(unmeasuredRules(enterprise.loaded.input.findings).edition, undefined);
  });

  test("the digest says the edition once, marked as ours; each check only points at it; an org with everything has no note", () => {
    const digest = checkedDigest({ input: professional.loaded.input, pseudonyms: professional.loaded.pseudonyms, inventory: null, madeAt: NOW }).digest!;
    assert.equal(digest.editionNote, "This org is on Professional Edition, which doesn't include Apex, the Metadata API, s-controls or Sites. Checks that need them are marked not measured.");
    assert.deepEqual(digest.orgWords?.editionNote, { orgWordsMarked: true, orgWords: [] });
    const text = JSON.stringify(digest);
    assert.equal(text.split(digest.editionNote!).length - 1, 1, "the note is said once");
    assert.ok(!text.includes(NO_APEX_IN_EDITION), "the full Apex sentence is on no check");
    const pointed = digest.findings.filter((f) => f.notMeasurableReason?.includes(EDITION_POINTER_STEM));
    assert.ok(pointed.length >= 9);
    for (const f of pointed) assert.ok(f.notMeasurableReason!.length <= 120, f.notMeasurableReason!);
    const plain = checkedDigest({ input: enterprise.loaded.input, pseudonyms: enterprise.loaded.pseudonyms, inventory: null, madeAt: NOW }).digest!;
    assert.equal(plain.editionNote, undefined);
    assert.equal(plain.orgWords, undefined);
  });

  test("each page that prints such a check carries the note once, and no other page does", () => {
    const report = buildReport(professional.loaded.input);
    const note = escapeXml(editionNote(["Apex", "the Metadata API", "s-controls", "Sites"])!);
    const pointer = escapeXml(EDITION_POINTER_STEM);
    let pages = 0;
    for (const html of [fullReportHtml(report, professional.loaded.pseudonyms), executiveHtml(report, professional.loaded.pseudonyms), onePagerHtml(report, professional.loaded.pseudonyms)]) {
      assert.ok(!html.includes(escapeXml(NO_APEX_IN_EDITION)));
      for (const page of html.split(/(?=<section class="page)/).slice(1)) {
        const notes = page.split(note).length - 1;
        assert.equal(notes, page.includes(pointer) ? 1 : 0);
        pages += notes;
      }
    }
    assert.ok(pages > 0);
    const plain = buildReport(enterprise.loaded.input);
    assert.ok(!fullReportHtml(plain, enterprise.loaded.pseudonyms).includes("edition-note"));
  });

  test("what the customer reads names the edition in our words, and nowhere quotes a code", () => {
    const report = buildReport(professional.loaded.input);
    const pages = [fullReportHtml(report, professional.loaded.pseudonyms), executiveHtml(report, professional.loaded.pseudonyms), onePagerHtml(report, professional.loaded.pseudonyms)].join("\n");
    assert.ok(!/INSUFFICIENT_ACCESS|INVALID_TYPE|Not sent: the org profile/.test(pages));
    assert.ok(!/INSUFFICIENT_ACCESS|INVALID_TYPE|Not sent: the org profile/.test(JSON.stringify(professional.loaded.input.findings)));
  });
});

describe("702 is intact: an unknown edition is read, and a refusal costs one check", () => {
  test("an edition we don't know reads the Metadata API; refused, it costs that read and the run goes on", async () => {
    const odd = await runAs("Mystery Edition", { failRetrieve: true });
    assert.ok(soap(odd.sent).length > 0);
    assert.equal(odd.run.status === "failed", false);
    assert.ok((odd.run.readsRefused ?? []).some((r) => r.code === "API_DISABLED_FOR_ORG"));
    assert.deepEqual(odd.run.orgProfile!.skipped, []);
  });

  test("a row without OrganizationType: nothing skipped, everything read", async () => {
    const blank = await runAs(null);
    assert.equal(blank.run.orgProfile!.edition, null);
    assert.deepEqual(blank.run.orgProfile!.skipped, []);
    assert.ok(soap(blank.sent).length > 0 && ownApex(blank.sent).length > 0);
  });
});

