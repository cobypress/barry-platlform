import { after, before, describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { Browser } from "playwright";
import { strFromU8, unzipSync } from "fflate";
import { SALES_CLOUD_MODULES } from "../index.ts";
import { runHealthCheck } from "../../engine/run.ts";
import type { OrgDocumentation } from "../../extract/documentation.ts";
import { buildMicrositePages, pageText } from "../../microsite/pages.ts";
import { buildReport } from "../../narrative/sections.ts";
import { checkNumbers } from "../../numbercheck/index.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { dataModelDiagram, placeObjects } from "../../render/documented.ts";
import { fullReportHtml } from "../../render/full-report.ts";
import { layoutOf, launchBrowser } from "../../render/pdf.ts";
import { renderRawNotesDocx } from "../../render/raw-notes-docx.ts";
import { noteSections } from "../../render/references.ts";
import type { Metric } from "../../schemas/index.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { AS_OF } from "../../testing/synthetic-org.ts";
import { AUTOMATION_STEPS, DOCUMENTED, DOCUMENTED_METRIC_IDS, readDocument, roleDepth } from "../configuration/documented.ts";

const { output, run, dir } = await runFromRecording({ answers: loadAnswers() });
const extracts = loadExtracts(dir);
const calibration = loadCalibration();
const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
const doc = readDocument(output!.metrics);
const gone = { status: "unavailable", reason: "INSUFFICIENT_ACCESS: no", source: { kind: "soql", api: "tooling", query: "x" } };
const changeDoc = (edit: (d: OrgDocumentation) => void): Metric[] => {
  const changed = clone(extracts);
  edit(changed.get("org-documentation") as OrgDocumentation);
  return analyseExtracts(changed).metrics;
};
const reportOf = (metrics: Metric[]) => buildReport({ run, calibration, findings: output!.findings, metrics, manualSteps: output!.manualSteps, intake: null });

describe("every part, from the synthetic org", () => {
  test("the data model: core and custom objects with records, fields, record types and the most-filled fields", () => {
    const names = doc.model!.objects.map((o) => o.name);
    assert.deepEqual(names, ["Account", "Contact", "Lead", "Opportunity", "Invoice__c", "Project__c", "Project_Line__c"]);
    assert.ok(!names.includes("Empty_Object__c"), "an object with no records is left out");
    const invoice = doc.model!.objects.find((o) => o.name === "Invoice__c")!;
    assert.equal(invoice.records, 12_000);
    assert.equal(invoice.customFields, 3);
    // By label, as Setup shows them (DECISIONS.md 412).
    assert.deepEqual(invoice.recordTypes, [{ name: "Credit Note", active: true }, { name: "Old Format", active: false }, { name: "Standard", active: true }]);
    const opportunity = doc.model!.objects.find((o) => o.name === "Opportunity")!;
    assert.equal(opportunity.mostFilled[0]!.field, "Competitor__c");
    assert.ok(doc.model!.objects.find((o) => o.name === "Account")!.mostFilled.length <= 10);
  });

  test("relationships between documented objects only, master-detail told from lookup", () => {
    const links = doc.relationships!.links;
    assert.ok(links.some((l) => l.from === "Project_Line__c" && l.to === "Project__c" && l.kind === "master-detail"));
    assert.ok(links.some((l) => l.from === "Contact" && l.field === "AccountId" && l.to === "Account" && l.kind === "lookup"));
    assert.ok(!links.some((l) => l.to === "User"), "a lookup to User isn't between documented objects");
    assert.equal(links.length, doc.relationships!.metric.value);
  });

  test("integrations: apps, credentials, remote sites by host only, integration users and API logins", () => {
    const i = doc.integrations!;
    assert.deepEqual(i.connectedApps, [{ name: "Data Loader", adminApprovedOnly: false }, { name: "Finance Sync", adminApprovedOnly: true }]);
    assert.deepEqual(i.namedCredentials, ["Finance API"]);
    assert.deepEqual(i.remoteSites, [{ host: "api.finance.example.com", active: true }, { host: "legacy.example.com", active: false }]);
    assert.doesNotMatch(JSON.stringify(output!.metrics), /not-a-real-key/);
    assert.ok(i.integrationUsers.length > 0);
    assert.ok(i.apiLogins.length > 0);
  });

  test("users and access: profiles, roles with the hierarchy's depth, and permission sets by who holds them", () => {
    const a = doc.access!;
    assert.equal(a.roleCount, 4);
    assert.equal(a.roleDepth, 3);
    assert.equal(a.profiles.reduce((sum, p) => sum + p.users, 0), a.metric.value);
    // User 9 is inactive, so Data Fix Temporary has one holder, not two. Report
    // Builder is held through a permission set group, and Old Territory Access by nobody.
    assert.deepEqual(a.permissionSets, [
      { name: "Sales Console User", users: 5 },
      { name: "CPQ User (Acme CPQ)", users: 2 },
      { name: "Data Fix Temporary", users: 1 },
      { name: "Report Builder", users: 1 },
      { name: "Reporting Everything", users: 1 },
      { name: "Old Territory Access", users: 0 },
    ]);
  });

  test("the security model: each object's internal and external default", () => {
    assert.deepEqual(doc.sharing!.objects.find((o) => o.object === "Account"), { object: "Account", internal: "Read", external: "None" });
    assert.equal(doc.sharing!.objects.length, 7);
    assert.ok(doc.sharing!.metric.exclusions.some((e) => /Sharing rules/.test(e)));
  });

  test("a role hierarchy's depth survives a loop", () => {
    assert.equal(roleDepth([{ Id: "a", ParentRoleId: "b" }, { Id: "b", ParentRoleId: "a" }]), 2);
    assert.equal(roleDepth([]), 0);
  });
});

describe("automation, in the order Salesforce runs it", () => {
  test("the steps follow the Apex Developer Guide's order of execution", () => {
    const numbered = AUTOMATION_STEPS.filter((s) => s.step !== null);
    assert.deepEqual(numbered.map((s) => s.key), ["before-flow", "before-trigger", "validation", "duplicate", "after-trigger", "workflow", "process", "after-flow"]);
    assert.deepEqual(numbered.map((s) => s.step), [3, 4, 5, 6, 8, 11, 13, 14]);
  });

  test("each object lists its automation in that order, with active and described", () => {
    const opportunity = doc.automation!.objects.find((o) => o.object === "Opportunity")!;
    assert.deepEqual(opportunity.steps.map((s) => s.key), ["before-trigger", "validation", "after-trigger", "workflow", "process", "after-flow"]);
    const workflow = opportunity.steps.find((s) => s.key === "workflow")!;
    assert.deepEqual(workflow.items, [{ name: "Notify_Large_Deal", active: true, described: false }, { name: "Old_Rule", active: false, described: false }]);
    assert.deepEqual([workflow.active, workflow.all], [1, 2]);
    const invoice = doc.automation!.objects.find((o) => o.object === "Invoice__c")!;
    // A trigger named by its object's id is still placed, before and after by its own flags.
    assert.ok(invoice.steps.find((s) => s.key === "before-trigger")!.items.some((i) => i.name === "InvoiceTrigger" && i.described === true));
    assert.ok(invoice.steps.find((s) => s.key === "after-trigger")!.items.some((i) => i.name === "InvoiceAudit"));
    for (const o of doc.automation!.objects) {
      const order = o.steps.map((s) => AUTOMATION_STEPS.findIndex((x) => x.key === s.key));
      assert.deepEqual(order, [...order].sort((a, b) => a - b), o.object);
    }
  });

  test("Workflow Rules read only in part make the list not measurable, not short", () => {
    const changed = clone(extracts);
    const metadata = changed.get("metadata") as { data: { unread?: Record<string, string> } };
    metadata.data.unread = { workflowRules: "INSUFFICIENT_ACCESS: no" };
    const m = analyseExtracts(changed).metrics.find((x) => x.id === DOCUMENTED.automation)!;
    assert.equal(m.status, "not_measurable");
  });
});

describe("the data model diagram", () => {
  test("draws the same picture every time", () => {
    const a = dataModelDiagram(doc);
    const b = dataModelDiagram(readDocument(structuredClone(output!.metrics)));
    assert.equal(a.kind, "chart");
    assert.equal(a.kind === "chart" && b.kind === "chart" ? a.svg : "", b.kind === "chart" ? b.svg : "x");
  });

  test("puts the most-related object in the middle and related objects side by side", () => {
    const cells = placeObjects(["A", "B", "C", "D", "E"], [{ from: "B", to: "A" }, { from: "C", to: "A" }, { from: "D", to: "C" }], 3, 2);
    assert.deepEqual(cells.get("A"), { row: 0, col: 1 });
    const near = (x: string, y: string): number => Math.abs(cells.get(x)!.row - cells.get(y)!.row) + Math.abs(cells.get(x)!.col - cells.get(y)!.col);
    assert.equal(near("A", "B"), 1);
    assert.equal(near("A", "C"), 1);
    assert.ok(near("C", "D") <= 2);
    assert.deepEqual(placeObjects(["A", "B", "C", "D", "E"], [{ from: "B", to: "A" }, { from: "C", to: "A" }, { from: "D", to: "C" }], 3, 2), cells);
    assert.equal(new Set([...cells.values()].map((c) => `${c.row},${c.col}`)).size, 5);
  });

  test("draws no more than the 12 busiest objects", () => {
    const many: Metric[] = output!.metrics.map((m) =>
      m.id === DOCUMENTED.model && m.status === "measured"
        ? { ...m, segments: [...m.segments, ...Array.from({ length: 10 }, (_, i) => ({ dimension: "records", key: `Extra_${i}__c`, n: i, value: i, notMeasurableReason: null }))] }
        : m,
    );
    const chart = dataModelDiagram(readDocument(many));
    assert.equal(chart.kind === "chart" ? (chart.svg.match(/<rect /g)?.length ?? 0) - 1 : 0, 12);
  });
});

describe("parts with no data are left out", () => {
  test("no relationships: the diagram still draws its boxes, with no lines and no key", () => {
    const metrics = changeDoc((d) => void (d.relationships = gone as never));
    const report = reportOf(metrics);
    assert.equal(readDocument(metrics).relationships, null);
    const chart = dataModelDiagram(readDocument(metrics));
    assert.ok(chart.kind === "chart" && !/<line /.test(chart.svg));
    assert.doesNotMatch(fullReportHtml(report, pseudonyms), /A solid line is master-detail/);
  });

  test("no integration read at all: the integrations column goes, and the rest stays", () => {
    const metrics = changeDoc((d) => {
      d.connectedApps = gone as never;
      d.namedCredentials = gone as never;
      d.remoteSites = gone as never;
    });
    assert.equal(metrics.find((m) => m.id === DOCUMENTED.integrations)!.status, "not_measurable");
    const html = fullReportHtml(reportOf(metrics), pseudonyms);
    assert.doesNotMatch(html, /<h3>Integrations<span[^>]*data-ref/);
    assert.match(html, /<h3>Users and access<span[^>]*data-ref/);
  });

  test("nothing measured: no slides, no contents line and no microsite page", () => {
    const metrics = output!.metrics.map((m): Metric => (DOCUMENTED_METRIC_IDS.includes(m.id) ? { ...m, status: "not_measurable", reason: "Synthetic gap." } as Metric : m));
    const report = reportOf(metrics);
    const html = fullReportHtml(report, pseudonyms);
    // The raw notes still have the section, saying why each part couldn't be measured.
    assert.doesNotMatch(html, /<div class="eyebrow">Your org, documented<\/div>|<li>Your org, documented<\/li>|The data model|What runs when a record is saved/);
    assert.ok(!buildMicrositePages(report, pseudonyms).pages.some((p) => p.slug === "org"));
  });
});

describe("a set-up review", async () => {
  const { output: essentials } = await runHealthCheck({ provider: syntheticProvider, calibration, modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(AS_OF), answers: loadAnswers(), scope: "essentials", transport: new Simulator() });

  test("documents the org from set-up alone, without the most-filled fields, which need records", () => {
    const d = readDocument(essentials!.metrics);
    for (const part of [d.model, d.relationships, d.automation, d.integrations, d.access, d.sharing]) assert.ok(part);
    assert.ok(d.model!.objects.every((o) => o.mostFilled.length === 0));
    assert.ok(d.model!.metric.exclusions.some((e) => /set-up review doesn't read records/.test(e)));
  });
});

describe("in the outputs", () => {
  const report = reportOf(output!.metrics);
  let browser: Browser;
  before(async () => {
    browser = await launchBrowser();
  });
  after(async () => {
    await browser.close();
  });

  test("the full report has the part before the appendix, and its pages pass the layout and number checks", async () => {
    const html = fullReportHtml(report, pseudonyms);
    assert.ok(html.indexOf("What runs when a record is saved") < html.indexOf("Metric definitions"));
    assert.ok(html.indexOf("Integrations, users and security") > html.indexOf("The data model"));
    const layout = await layoutOf(browser, html, { minPages: 1, maxPages: 999, dataset: report.dataset });
    const pages = [...html.matchAll(/<section class="page[^"]*" data-page="(\d+)">([\s\S]*?)<\/section>/g)].filter((m) => /Your org, documented/.test(m[2]!)).map((m) => Number(m[1]));
    assert.ok(pages.length >= 3);
    assert.deepEqual(layout.problems.filter((p) => pages.includes(p.page)), []);
  });

  test("the microsite's org page has every table, the diagram inline, and every figure traced", () => {
    const org = buildMicrositePages(report, pseudonyms).pages.find((p) => p.slug === "org")!;
    for (const heading of ["The data model", "How the objects relate", "What runs when a record is saved", "Integrations", "Users and access", "Who can see what"]) assert.match(org.html, new RegExp(heading));
    assert.match(org.html, /<svg[^>]*data-chart="data-model"/);
    assert.match(org.html, /Old_Rule/);
    assert.doesNotMatch(org.html, /<script|src=|href=|url\(/);
    assert.deepEqual(checkNumbers(pageText(org.html), report.dataset).unmatched, []);
  });

  test("the raw notes have a section of their own with every list, numbered", async () => {
    const section = noteSections(report).find((s) => s.key === "org_documented")!;
    assert.equal(section.title, "Your org, documented");
    assert.deepEqual(section.items.map((i) => i.id), DOCUMENTED_METRIC_IDS);
    const xml = strFromU8(unzipSync(new Uint8Array(await renderRawNotesDocx(report, pseudonyms)))["word/document.xml"]!);
    const text = [...xml.matchAll(/<w:t(?: [^>]*)?>([^<]*)<\/w:t>/g)].map((m) => m[1]).join(" ");
    assert.match(text, new RegExp(`${section.ref} +Your org, documented`));
    for (const name of ["Project_Line__c", "Old_Rule", "Finance API", "Sales Console User", "Public Read Only", "Apex after triggers \\(step 8\\)"]) assert.match(text, new RegExp(name), name);
  });
});
