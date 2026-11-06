import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { securityModule } from "../security/index.ts";
import { packagePriceKey } from "../utilisation/index.ts";
import { ESSENTIALS_DOMAINS, modulesFor } from "../scope.ts";
import { SALES_CLOUD_MODULES } from "../index.ts";
import { ACTION_LISTS_FILE, type ActionLists } from "../../actions/lists.ts";
import type { AccessData } from "../../extract/security.ts";
import type { DuplicateAccounts } from "../../extract/duplicates.ts";
import type { LicenceData } from "../../extract/users.ts";
import type { Finding } from "../../schemas/index.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { userId } from "../../testing/synthetic-org.ts";

// Roadmap 3.2's remaining areas, as checks in existing areas (DECISIONS.md 201 to 215).

const { output, dir } = await runFromRecording({ answers: loadAnswers() });
const extracts = loadExtracts(dir);
const lists = JSON.parse(readFileSync(join(dir, ACTION_LISTS_FILE), "utf8")) as ActionLists;
const gone = { status: "unavailable", reason: "INSUFFICIENT_ACCESS: no", source: { kind: "soql", api: "rest", query: "x" } };

const one = (findings: Finding[], ruleId: string): Finding => {
  const found = findings.filter((f) => f.ruleId === ruleId);
  assert.equal(found.length, 1, ruleId);
  return found[0]!;
};
const edited = <T>(id: string, edit: (data: T) => void, calibration = loadCalibration()): Finding[] => {
  const changed = clone(extracts);
  edit(changed.get(id) as T);
  return analyseExtracts(changed, { calibration }).findings;
};

describe("profiles and permission sets nobody uses", () => {
  test("fires on the custom profile with no users and the set nobody holds, not the set held through a group, nor a community profile", () => {
    const f = one(output!.findings, "sec.unused-access");
    assert.equal(f.status, "issue");
    assert.equal(f.domain, "security");
    assert.deepEqual(f.subjects.map((s) => s.label), ["Old Sales Profile", "Old Territory Access"]);
    assert.equal(f.recommendationType, "investigate");
  });

  test("a set in a group nobody is assigned is unused", () => {
    const findings = edited<AccessData>("access", (a) => void ((a.assignments as { data: Array<{ PermissionSetGroupId: string | null }> }).data = (a.assignments as { data: Array<{ PermissionSetGroupId: string | null }> }).data.filter((x) => !x.PermissionSetGroupId)));
    assert.ok(one(findings, "sec.unused-access").subjects.some((s) => s.label === "Report Builder"));
  });

  test("not measurable when the groups can't be read, since a set may be held through one", () => {
    assert.equal(one(edited<AccessData>("access", (a) => void (a.groupComponents = gone as never)), "sec.unused-access").status, "not_measurable");
  });
});

describe("access nobody else on the profile has", () => {
  test("fires on the set one person holds through a group, and leaves Modify and View All Data to their own checks", () => {
    const f = one(output!.findings, "sec.peer-outlier-access");
    assert.equal(f.status, "issue");
    assert.equal(f.confidence, "medium");
    assert.match(f.summary, /Report Builder \(Standard User\)/);
    assert.doesNotMatch(f.summary, /Data Fix Temporary|Reporting Everything/);
  });

  test("its people are in the action lists, by id", () => {
    const list = lists.lists.find((l) => l.ruleId === "sec.peer-outlier-access")!;
    assert.deepEqual(list.rows.map((r) => r.id), [userId(1)]);
    assert.match(list.rows[0]!.problem, /Holds Report Builder, which nobody else on the Standard User profile has/);
  });

  test("passes when a second colleague holds it too", () => {
    const findings = edited<AccessData>("access", (a) => void (a.assignments as { data: unknown[] }).data.push({ AssigneeId: userId(3), PermissionSetId: null, PermissionSetGroupId: "0PG000000000001AAA" }));
    assert.equal(one(findings, "sec.peer-outlier-access").status, "passed");
  });

  test("both checks are set-up checks, so a set-up review runs them", () => {
    const security = modulesFor("essentials", SALES_CLOUD_MODULES).find((m) => m.id === "security")!;
    for (const id of ["sec.unused-access", "sec.peer-outlier-access"]) assert.ok(security.rules.some((r) => r.id === id), id);
    assert.ok(securityModule.rules.every((r) => !r.records));
  });
});

describe("installed apps' seats", () => {
  test("fires on spare seats of a seated app, leaving out site licences, free and expired apps, with no figure until the calibration prices a seat", () => {
    const f = one(output!.findings, "utl.package-seats");
    assert.equal(f.status, "issue");
    assert.equal(f.severity, "low");
    assert.match(f.summary, /acmecpq has 8 of 20 seats unassigned/);
    assert.equal(f.value.kind, "not_sized");
  });

  test("sized once the calibration prices the app's seat", () => {
    const calibration = { ...loadCalibration(), licenceCosts: { ...loadCalibration().licenceCosts, [packagePriceKey("acmecpq")]: 300 } };
    const f = one(edited<LicenceData>("licences", () => {}, calibration), "utl.package-seats");
    assert.deepEqual(f.value.kind === "licence_cost" ? f.value.amount : null, 2400);
  });

  test("medium when an app has nobody assigned at all", () => {
    const f = one(edited<LicenceData>("licences", (l) => (l.packageLicenses as { data: Array<{ NamespacePrefix: string | null; UsedLicenses: number }> }).data.forEach((p) => p.NamespacePrefix === "acmecpq" && (p.UsedLicenses = 0))), "utl.package-seats");
    assert.equal(f.severity, "medium");
    assert.equal(f.title, "Some installed apps aren't used by anyone");
  });
});

describe("storage against the allowance", () => {
  test("fires on data storage nearly full, and names the largest objects", () => {
    const f = one(output!.findings, "tech.storage-use");
    assert.equal(f.status, "issue");
    assert.equal(f.domain, "technical_health");
    assert.equal(f.severity, "medium");
    assert.match(f.summary, /Data storage is 87\.3% used \(8,940 of 10,240 MB\)/);
    assert.match(f.summary, /Invoice__c \(12,000\)/);
    assert.equal(f.value.kind, "not_sized");
  });

  test("high past 95%, a pass with room, and not measurable when the org reports none", () => {
    const at = (remaining: number): Finding => one(edited<{ data: Record<string, { Remaining: number }> }>("api-limits", (l) => void (l.data.DataStorageMB!.Remaining = remaining)), "tech.storage-use");
    assert.equal(at(100).severity, "high");
    assert.equal(at(9_000).status, "passed");
    const none = edited<{ data: Record<string, unknown> }>("api-limits", (l) => (delete l.data.DataStorageMB, delete l.data.FileStorageMB));
    assert.equal(one(none, "tech.storage-use").status, "not_measurable");
  });
});

describe("open deals with no account", () => {
  test("fires, and lists every one", () => {
    const f = one(output!.findings, "dq.opps-without-account");
    assert.equal(f.status, "issue");
    const list = lists.lists.find((l) => l.ruleId === "dq.opps-without-account")!;
    assert.equal(list.rows.length, f.facts.hits);
    assert.ok(list.rows.every((r) => r.problem === "No account" && r.name?.startsWith("Synthetic deal ")));
  });
});

describe("accounts that share a name", () => {
  test("fires on the two groups, low at 1% of accounts, as something to check rather than merge", () => {
    const f = one(output!.findings, "dq.duplicate-accounts");
    assert.equal(f.status, "issue");
    assert.equal(f.severity, "low");
    assert.equal(f.recommendationType, "investigate");
    assert.deepEqual(f.facts, { accounts: 5, groups: 2, share: 1, total: 500 });
    assert.doesNotMatch(`${f.title} ${f.summary} ${f.recommendation}`, /Synthetic Holdings|Example Logistics/, "names stay out of the finding");
  });

  test("every account in a group is in the action lists, by name", () => {
    const list = lists.lists.find((l) => l.ruleId === "dq.duplicate-accounts")!;
    assert.equal(list.object, "Account");
    assert.equal(list.rows.length, 5);
    assert.ok(list.rows.some((r) => r.name === "Synthetic Holdings" && r.problem === "Shares its exact name with 2 other accounts"));
  });

  test("passes with no shared names, and is a record check a set-up review leaves out", () => {
    const findings = edited<DuplicateAccounts>("duplicate-accounts", (d) => void ((d.groups as { data: unknown[] }).data = []));
    assert.equal(one(findings, "dq.duplicate-accounts").status, "passed");
    const dq = modulesFor("essentials", SALES_CLOUD_MODULES).find((m) => m.id === "data_quality");
    assert.ok(!ESSENTIALS_DOMAINS.includes("data_quality") && dq === undefined);
  });
});
