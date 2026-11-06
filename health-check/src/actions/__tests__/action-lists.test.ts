import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { strFromU8, unzipSync } from "fflate";
import { buildTickets, buildWorkbook, loadActionLists, PRIORITY, TICKET_COLUMNS } from "../index.ts";
import { ACTION_LISTS_FILE, type ActionLists } from "../lists.ts";
import { ACTION_ROWS, actionRecordsExtractor, type ActionRecords } from "../../extract/action-records.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { SALES_CLOUD_MODULES } from "../../domains/index.ts";
import { buildReport } from "../../narrative/sections.ts";
import { isSalesforceUserName } from "../../privacy/pseudonymiser.ts";
import { fullReportHtml } from "../../render/full-report.ts";
import { executiveHtml } from "../../render/executive.ts";
import { loadRun } from "../../render/render-run.ts";
import type { Finding } from "../../schemas/index.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { AS_OF, LEADS, CONTACTS, USERS } from "../../testing/synthetic-org.ts";

const { output, dir } = await runFromRecording({ answers: loadAnswers() });
const lists = loadActionLists(dir)!;
const { input, pseudonyms } = loadRun(dir);
const unnamed = buildReport({ ...input, intake: null });
const named = buildReport({ ...input, intake: { summary: "Synthetic.", statedProblems: [], namedRepConsent: true, capturedBy: "Synthetic Consultant", capturedAt: "2026-10-30T09:00:00.000Z" } });

const finding = (ruleId: string): Finding => output!.findings.find((f) => f.ruleId === ruleId && f.status === "issue")!;
const listFor = (ruleId: string) => lists.lists.find((l) => l.ruleId === ruleId)!;
const sheetsOf = (bytes: Uint8Array): string => Object.entries(unzipSync(bytes)).filter(([k]) => k.startsWith("xl/worksheets/sheet")).map(([, v]) => strFromU8(v)).join("\n");
// People's names, other than Salesforce's own users, which decision 190 leaves readable.
const PEOPLE = USERS.map((u) => u.name).filter((n) => !isSalesforceUserName(n));

describe("what a run keeps for the action lists", () => {
  test("every record an issue counted, by name, beside the findings and outside checkpoints", () => {
    assert.equal(lists.instanceUrl, "https://synthetic.example.com");
    for (const id of ["dq.open-opps-past-close", "dq.open-opps-missing-amount", "dq.open-opps-no-activity", "dq.opps-inactive-owner", "dq.loss-reason-missing", "cur.stuck-in-stage", "cur.stale-deals", "cur.close-date-pushes", "cur.missing-next-steps"]) {
      assert.equal(listFor(id).rows.length, finding(id).facts.hits, id);
      assert.ok(listFor(id).rows.every((r) => r.name?.startsWith("Synthetic deal ") && r.ownerId?.startsWith("005")), id);
    }
    assert.equal(listFor("dq.stale-leads").rows.length, LEADS.stale);
    // One line for each gap: ten contacts have both.
    assert.equal(listFor("dq.contact-completeness").rows.length, CONTACTS.noAccount + CONTACTS.noEmail);
    assert.ok(lists.lists.every((l) => finding(l.ruleId)), "only issues have lists");
  });

  test("users are listed by their own id, never their token", () => {
    const users = lists.lists.filter((l) => l.object === "User").flatMap((l) => l.rows);
    assert.ok(users.length > 0);
    assert.ok(users.every((r) => /^005\d{12}AAA$/.test(r.id)), JSON.stringify(users.map((r) => r.id)));
  });

  test("no record's name reaches a finding, a metric or the engine's notes (rule 7)", () => {
    for (const file of ["findings.json", "metrics.json", "raw-notes.md"]) assert.doesNotMatch(readFileSync(join(dir, file), "utf8"), /Synthetic (deal|lead|contact) \d/, file);
  });
});

describe("the workbook", () => {
  test("a front sheet of every issue, then a sheet for each list, named by its reference", () => {
    const { bytes, sheets, rows } = buildWorkbook(unnamed, pseudonyms, lists);
    assert.equal(sheets, lists.lists.length);
    assert.equal(rows, lists.lists.reduce((sum, l) => sum + l.rows.length, 0));
    const workbook = strFromU8(unzipSync(bytes)["xl/workbook.xml"]!);
    assert.match(workbook, /<sheet name="Issues" sheetId="1"/);
    assert.match(workbook, /<sheet name="R\d+\.\d+" sheetId="2"/);
    const links = Object.entries(unzipSync(bytes)).filter(([k]) => k.startsWith("xl/worksheets/_rels/")).map(([, v]) => strFromU8(v)).join("\n");
    assert.match(links, /Target="https:\/\/synthetic\.example\.com\/006\d{12}AAA" TargetMode="External"/);
  });

  test("without consent, nobody is named: owners by role, users by profile and role", () => {
    const xml = sheetsOf(buildWorkbook(unnamed, pseudonyms, lists).bytes);
    for (const person of PEOPLE) assert.ok(!xml.includes(person), person);
    assert.match(xml, /Owner&apos;s role|Owner's role/);
    assert.match(xml, /Standard User, (no role|role Sales)/);
    assert.match(xml, /Synthetic deal \d+/, "records themselves are named");
  });

  test("with consent recorded at intake, owners and users are named", () => {
    const xml = sheetsOf(buildWorkbook(named, pseudonyms, lists).bytes);
    assert.match(xml, />Owner</);
    assert.ok(PEOPLE.some((p) => xml.includes(p)));
  });

  test("a list that stopped at its ceiling says it's the first part, and how to see the rest", () => {
    const part: ActionLists = { ...lists, lists: lists.lists.map((l) => (l.ruleId === "dq.stale-leads" ? { ...l, rows: l.rows.slice(0, 2), part: { total: 7210, rest: "A list view shows them all." } } : l)) };
    const xml = sheetsOf(buildWorkbook(unnamed, pseudonyms, part).bytes);
    assert.match(xml, /The first 2 of 7,210, in order of Salesforce id\. A list view shows them all\./);
    assert.match(xml, /the first 2 of 7,210/);
    assert.ok(ACTION_ROWS >= 1000);
  });

  test("a run from before the lists still gets a front sheet that says so", () => {
    const xml = sheetsOf(buildWorkbook(unnamed, pseudonyms, null).bytes);
    assert.match(xml, /Record lists weren&apos;t gathered|Record lists weren't gathered/);
  });
});

describe("the ticket file", () => {
  const csv = buildTickets(unnamed, pseudonyms, lists);
  const parse = (text: string): string[][] => {
    const rows: string[][] = [];
    for (const line of text.split(/\r\n(?=")/)) if (line) rows.push([...line.matchAll(/"((?:[^"]|"")*)"/g)].map((m) => m[1]!.replace(/""/g, '"')));
    return rows;
  };
  const rows = parse(csv);

  test("one row an issue, under Jira's column names", () => {
    assert.deepEqual(rows[0], [...TICKET_COLUMNS]);
    assert.equal(rows.length - 1, output!.findings.filter((f) => f.status === "issue").length);
    assert.ok(rows.slice(1).every((r) => r.length === TICKET_COLUMNS.length));
  });

  test("priority from severity, labels for the area and rule, and the reference", () => {
    const f = finding("dq.open-opps-past-close");
    const row = rows.find((r) => r[6] === f.ruleId)!;
    assert.equal(row[2], PRIORITY[f.severity]);
    assert.deepEqual(row.slice(4, 7), ["barometer", "data_quality", "dq.open-opps-past-close"]);
    assert.match(row[9]!, /^R\d+\.\d+$/);
    assert.match(row[3]!, new RegExp(`Records to fix: ${f.facts.hits} on sheet ${row[9]!.replace(".", "\\.")} of the action list workbook\\.`));
    assert.equal(row[10], String(f.facts.hits));
  });

  test("without consent, nobody is named", () => {
    for (const person of PEOPLE) assert.ok(!csv.includes(person), person);
  });

  test("a part list's ticket counts every record to fix, not only the rows on the sheet (DECISIONS.md 325)", () => {
    const part: ActionLists = { ...lists, lists: lists.lists.map((l) => (l.ruleId === "dq.stale-leads" ? { ...l, rows: l.rows.slice(0, 2), part: { total: 7210, rest: "A list view shows them all." } } : l)) };
    const row = parse(buildTickets(unnamed, pseudonyms, part)).find((r) => r[6] === "dq.stale-leads")!;
    assert.match(row[3]!, /Records to fix: 7,210, the first 2 of them/);
    assert.equal(row[10], "7210");
  });

  test("a cell that would start a formula is made text", () => {
    const report = buildReport({ ...input, intake: null, findings: input.findings.map((f) => (f.ruleId === "dq.open-opps-past-close" ? { ...f, title: "=HYPERLINK(\"http://x\")" } : f)) });
    assert.match(buildTickets(report, pseudonyms, lists), /"'=HYPERLINK\(""http:\/\/x""\)"/);
  });
});

describe("where the action lists are mentioned", () => {
  test("in both decks' detail pages and in the raw notes' counts", () => {
    assert.match(fullReportHtml(unnamed, pseudonyms), /The action lists \(a spreadsheet and a ticket file\)/);
    assert.match(executiveHtml(unnamed, pseudonyms), /the action lists \(a spreadsheet and a ticket file\)/);
    assert.equal(input.recordCounts?.[finding("dq.stale-leads").id]?.rows, LEADS.stale);
  });
});

describe("in a set-up review", async () => {
  const outDir = tempDir();
  const { dir: essentials, run } = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir, now: new Date(AS_OF), answers: loadAnswers(), scope: "essentials", transport: new Simulator() });

  test("only users are listed, and no record list is read", () => {
    const own = JSON.parse(readFileSync(join(essentials, ACTION_LISTS_FILE), "utf8")) as ActionLists;
    assert.ok(own.lists.length > 0);
    assert.ok(own.lists.every((l) => l.object === "User"));
    assert.ok(!run.steps.some((s) => s.id === actionRecordsExtractor.id));
  });
});

describe("approved number overrides (night 3 review)", () => {
  test("the tickets and the workbook carry the corrected figure, as the decks do", () => {
    const f = finding("dq.open-opps-past-close");
    const hits = f.facts.hits!;
    const override = { id: "ov-0001", runId: input.run.id, target: { type: "finding" as const, id: f.id, field: "facts.hits" }, previousValue: hits, newValue: hits - 2, reason: "Two were test deals", overriddenBy: "Synthetic Consultant", overriddenAt: "2026-11-01T09:00:00.000Z" };
    const report = buildReport({ ...input, intake: null, overrides: [override] });
    const corrected = report.teamFindings.find((x) => x.id === f.id)!;
    assert.match(corrected.summary, new RegExp(`\\b${hits - 2} of\\b`), "the decks' wording has the corrected figure");
    // This issue's own ticket: another issue can quote the same figure for its own reasons.
    const row = buildTickets(report, pseudonyms, lists).split("\r\n").find((line) => line.startsWith(`"${corrected.title}",`))!;
    assert.ok(row, "the issue has a ticket");
    assert.ok(row.includes(`${hits - 2} of`), "the ticket has the corrected figure");
    assert.ok(!row.includes(`${hits} of`), "the ticket still has the figure the consultant corrected");
  });
});

describe("a list that's only part of the whole (night 3 review)", () => {
  const extracts = loadExtracts(dir);
  const records = (edit: (actions: ActionRecords, health: { counts: Record<string, { data: number }> }) => void) => {
    const changed = clone(extracts);
    edit(changed.get("action-records") as ActionRecords, changed.get("crm-health") as { counts: Record<string, { data: number }> });
    return analyseExtracts(changed).records;
  };

  test("is one only when the read stopped at its ceiling, not because the count moved between the two reads", () => {
    const list = records((a) => {
      const leads = a.staleLeads as { data: unknown[]; truncated?: boolean };
      leads.data = leads.data.slice(0, leads.data.length - 3);
      leads.truncated = false;
    }).find((r) => r.ruleId === "dq.stale-leads")!;
    assert.equal(list.part, undefined, "three leads worked between the count and the list don't make it a part");
  });

  test("a contact list cut off in one gap only says so gap by gap, not as one first N in order of id", () => {
    const list = records((a, health) => {
      (a.contactsNoAccount as { truncated?: boolean }).truncated = true;
      health.counts.contactsNoAccount!.data = 12_000;
    }).find((r) => r.ruleId === "dq.contact-completeness")!;
    assert.ok(list.part, "it's a part");
    assert.match(list.part!.label ?? "", /^No account: the first [\d,]+ of 12,000, in order of Salesforce id\. No email address: all [\d,]+\.$/);
  });
});

describe("without consent, a person's token anywhere in the files' text becomes a role (night 3 review)", () => {
  test("in a finding's owner, effort note and fix, and in a list's fix and problems", () => {
    const entry = pseudonyms.entries().find((e) => e.token.startsWith("[[user:") && PEOPLE.includes(e.name))!;
    const f = finding("dq.open-opps-past-close");
    const findings = input.findings.map((x) => (x.id === f.id ? { ...x, owner: `Sales ops, with ${entry.token}`, effort: { ...x.effort, note: `${entry.token} knows these deals` }, recommendation: `Ask ${entry.token} to close them` } : x));
    const tokenised = { ...lists, lists: lists.lists.map((l) => (l.findingId === f.id ? { ...l, fix: `Ask ${entry.token}`, rows: l.rows.map((r) => ({ ...r, problem: `${r.problem}; ${entry.token} was told` })) } : l)) };
    const report = buildReport({ ...input, findings, intake: null });
    const csv = buildTickets(report, pseudonyms, tokenised);
    const xml = sheetsOf(buildWorkbook(report, pseudonyms, tokenised).bytes);
    for (const [name, text] of [["tickets", csv], ["workbook", xml]] as const) {
      assert.ok(!text.includes(entry.name), `${name} names ${entry.name}`);
      assert.ok(!text.includes(entry.token), `${name} keeps the token`);
    }
    assert.ok(/\b(one|another) person\b/i.test(xml) || /\b(one|another) person\b/i.test(csv), "the token became a role-free phrase");
  });
});
