import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { reportDefinitionsExtractor, type ReportScan } from "../../extract/report-definitions.ts";
import { SalesforceError } from "../../salesforce/client.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import { loadCalibration } from "../../testing/replay.ts";
import { PRIVATE_REPORTS, sfId } from "../../testing/synthetic-org.ts";
import type { Finding, Guard } from "../../schemas/index.ts";

let extracts: Map<string, unknown>;
let runDir: string;

before(async () => {
  runDir = (await runFromRecording()).dir;
  extracts = loadExtracts(runDir);
});

const fieldFindings = (changed: Map<string, unknown>): Finding[] => analyseExtracts(changed).findings.filter((f) => f.ruleId === "cfg.unused-custom-fields" && f.status === "issue");
const guardOf = (findings: Finding[], subject: string): Guard | undefined => findings.flatMap((f) => f.guards).find((g) => g.subject === subject && g.guardId === "not-used-in-reports");
const scan = (edit: (s: ReportScan) => void): Map<string, unknown> => {
  const changed = clone(extracts);
  edit((changed.get(reportDefinitionsExtractor.id) as { data: ReportScan }).data);
  return changed;
};
const noPrivate = (edit: (s: ReportScan) => void = () => {}): Map<string, unknown> =>
  scan((s) => {
    s.privateReports = 0;
    edit(s);
  });

describe("the scan", () => {
  test("reads every report and finds the field in one", () => {
    const { data } = extracts.get(reportDefinitionsExtractor.id) as { data: ReportScan };
    assert.equal(data.expected, 80);
    assert.equal(data.retrieved, 80);
    assert.equal(data.notRead, 0);
    assert.equal(data.privateReports, PRIVATE_REPORTS);
    assert.deepEqual(data.fields.Old_Region__c, ["reports/Marketing/Report_8.report"]);
  });

  test("keeps field names only: titles and filter values never reach a checkpoint", () => {
    const text = readFileSync(join(runDir, "checkpoints", "report-definitions.json"), "utf8");
    assert.doesNotMatch(text, /Confidential Buyer/);
    assert.doesNotMatch(text, /ACCOUNT\.NAME/);
    for (const file of ["findings.json", "raw-notes.md"]) assert.doesNotMatch(readFileSync(join(runDir, file), "utf8"), /Confidential Buyer/, file);
  });
});

describe("the guard", () => {
  test("a field used in a report fails, naming the report", () => {
    const guard = guardOf(fieldFindings(extracts), "Opportunity.Old_Region__c")!;
    assert.equal(guard.status, "failed");
    assert.match(guard.detail, /Report_8/);
  });

  test("private reports leave a clean field unknown, however clean the scan", () => {
    const guard = guardOf(fieldFindings(extracts), "Account.Old_Sic_Code__c")!;
    assert.equal(guard.status, "unknown");
    assert.match(guard.detail, /4 private reports/);
  });

  test("a clean field with a whole scan passes", () => {
    const guard = guardOf(fieldFindings(noPrivate()), "Account.Old_Sic_Code__c")!;
    assert.equal(guard.status, "passed");
  });

  test("not being able to count private reports blocks", () => {
    const guard = guardOf(fieldFindings(scan((s) => {
      s.privateReports = null;
      s.privateReason = "INSUFFICIENT_ACCESS: no";
    })), "Account.Old_Sic_Code__c")!;
    assert.equal(guard.status, "unknown");
    assert.match(guard.detail, /INSUFFICIENT_ACCESS/);
  });

  test("one report we couldn't read blocks", () => {
    const guard = guardOf(fieldFindings(noPrivate((s) => (s.notRead = 1))), "Account.Old_Sic_Code__c")!;
    assert.equal(guard.status, "unknown");
    assert.match(guard.detail, /1 of 80/);
  });

  test("a use found still fails, even with gaps in the scan", () => {
    const guard = guardOf(fieldFindings(scan((s) => (s.notRead = 5))), "Opportunity.Old_Region__c")!;
    assert.equal(guard.status, "failed");
  });

  test("an unreadable scan blocks", () => {
    const changed = clone(extracts);
    changed.set(reportDefinitionsExtractor.id, { status: "unavailable", reason: "RETRIEVE_FAILED: no", source: { kind: "metadata", path: "Report" } });
    assert.equal(guardOf(fieldFindings(changed), "Account.Old_Sic_Code__c")!.status, "unknown");
  });
});

describe("the recommendation (rule 3)", () => {
  test("with private reports out of reach, no field is recommended for removal", () => {
    for (const f of fieldFindings(extracts)) assert.equal(f.recommendationType, "investigate", f.subjects[0]?.label);
  });

  test("with a whole scan, the clean field is recommended for removal", () => {
    const account = fieldFindings(noPrivate()).find((f) => f.subjects.some((s) => s.label === "Account.Old_Sic_Code__c"))!;
    assert.equal(account.recommendationType, "remove");
    assert.ok(account.guards.every((g) => g.status === "passed"));
  });

  test("a mixed object stays investigate, with every field blocked by the one that failed", () => {
    const opportunity = fieldFindings(noPrivate()).find((f) => f.subjects.some((s) => s.label === "Opportunity.Old_Region__c"))!;
    assert.equal(opportunity.recommendationType, "investigate");
  });

  test("a removal with a calibrated upkeep is sized in hours", () => {
    const calibration = loadCalibration();
    calibration.effortMinutes = { "custom-field-upkeep": 30 };
    const account = analyseExtracts(noPrivate(), { calibration }).findings.find((f) => f.ruleId === "cfg.unused-custom-fields" && f.recommendationType === "remove")!;
    assert.ok(account.value.kind === "hours" && account.value.amount === 0.5);
  });
});

describe("extracting", () => {
  const reports = [
    { Id: sfId("00O", 1), DeveloperName: "In_Folder", OwnerId: sfId("00l", 2) },
    { Id: sfId("00O", 2), DeveloperName: "In_Root", OwnerId: "00D000000000001AAA" },
    { Id: sfId("00O", 3), DeveloperName: "Mine", OwnerId: "005000000000001AAA" },
    { Id: sfId("00O", 4), DeveloperName: "Lost", OwnerId: sfId("00l", 9) },
  ];
  const folders = [{ Id: sfId("00l", 1), DeveloperName: "Top", ParentId: null }, { Id: sfId("00l", 2), DeveloperName: "Sub", ParentId: sfId("00l", 1) }];

  const context = (options: { retrieve?: (members: string[]) => Record<string, string>; privateCount?: () => number }) => {
    const asked: string[] = [];
    const ctx = {
      client: {
        query: async (soql: string) => {
          if (soql.includes("allPrivate")) return { records: [], totalSize: options.privateCount?.() ?? 0, truncated: false };
          return { records: soql.includes("FROM Folder") ? folders : reports, totalSize: 1, truncated: false };
        },
        retrieveMetadata: async (types: Array<{ members: string[] }>) => {
          asked.push(...types[0]!.members);
          return options.retrieve ? options.retrieve(types[0]!.members) : Object.fromEntries(types[0]!.members.map((m) => [`reports/${m}.report`, "<Report/>"]));
        },
      },
    } as never;
    return { ctx, asked };
  };

  test("builds nested folder paths and the unfiled folder", async () => {
    const { ctx, asked } = context({});
    await reportDefinitionsExtractor.run(ctx);
    assert.deepEqual(asked, ["Top/Sub/In_Folder", "unfiled$public/In_Root"]);
  });

  test("a report it can't place counts as not read", async () => {
    const out = await reportDefinitionsExtractor.run(context({}).ctx);
    assert.ok(out.status === "ok");
    assert.equal(out.data.expected, 4);
    assert.equal(out.data.retrieved, 2);
    assert.equal(out.data.notRead, 2);
  });

  test("a report Salesforce doesn't return counts as not read", async () => {
    const out = await reportDefinitionsExtractor.run(context({ retrieve: () => ({ "package.xml": "<Package/>" }) }).ctx);
    assert.ok(out.status === "ok");
    assert.equal(out.data.notRead, 4);
  });

  test("a failed retrieve doesn't fail the run", async () => {
    const out = await reportDefinitionsExtractor.run(context({ retrieve: () => { throw new SalesforceError(200, "RETRIEVE_FAILED", "too big"); } }).ctx);
    assert.equal(out.status, "unavailable");
  });

  test("a budget stop isn't swallowed, so the run can resume", async () => {
    await assert.rejects(reportDefinitionsExtractor.run(context({ retrieve: () => { throw new Error("budget"); } }).ctx), /budget/);
  });

  test("no permission to count private reports is recorded, not read as zero", async () => {
    const out = await reportDefinitionsExtractor.run(context({ privateCount: () => { throw new SalesforceError(400, "INSUFFICIENT_ACCESS", "no"); } }).ctx);
    assert.ok(out.status === "ok");
    assert.equal(out.data.privateReports, null);
  });
});
