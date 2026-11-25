import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { ALL_MODULES } from "../../domains/index.ts";
import type { DomainModule } from "../../domains/types.ts";
import { runHealthCheck, type RunOptions } from "../../engine/run.ts";
import { authorise } from "../../salesforce/allowlist.ts";
import { Inventory } from "../../schemas/inventory.ts";
import { runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator, SYNTHETIC_RECORDS, type SimulatorOptions } from "../../testing/simulator.ts";
import { AS_OF, CONNECTED_USER, USERS, userId } from "../../testing/synthetic-org.ts";
import { buildInventory, FILL_RATE_MAX_OBJECTS, fillRatesExtractor, INVENTORY_FILE, SETUP_ONLY_FILL_RATES } from "../inventory.ts";

// The org's inventory (DECISIONS.md 603 to 605): what each section holds on
// the synthetic org, what it says when a read fails, and what it never reads.

const readInventory = (dir: string): Inventory => Inventory.parse(JSON.parse(readFileSync(join(dir, INVENTORY_FILE), "utf8")));

// A module with no steps of its own, so every inventory step reads for itself
// what an earlier step would have given it. Configuration's id keeps it in a set-up only scope.
const BARE: DomainModule = { id: "configuration", cloud: "sales", label: "Nothing else", extractors: [], rules: [] };

async function simulated(options: SimulatorOptions = {}, run: Partial<RunOptions> = {}) {
  const transport = new Simulator(options);
  const result = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: [BARE], outDir: tempDir(), now: new Date(AS_OF), transport, connectedUser: CONNECTED_USER, inventory: true, ...run });
  return { ...result, transport, inventory: readInventory(result.dir) };
}

const rows = <T>(section: { rows: T[] } | { unavailable: string }): T[] => {
  assert.ok("rows" in section, `unavailable: ${"unavailable" in section ? section.unavailable : ""}`);
  return section.rows;
};

describe("the inventory of the synthetic org, from the recording", async () => {
  const { dir, run } = await runFromRecording({ modules: ALL_MODULES, inventory: true });
  const inventory = readInventory(dir);

  test("is version 1, dated as the run reads the org, and parses", () => {
    assert.equal(inventory.version, 1);
    assert.equal(inventory.madeAt, run.asOf);
    assert.equal(inventory.scope, "complete");
  });

  test("fill rates: the field nobody filled is at 0, from counts, and what Salesforce can't count is skipped with why", () => {
    const objects = rows(inventory.fillRates);
    assert.deepEqual(objects.map((o) => o.object), ["Account", "Contact", "Lead", "Opportunity", "Invoice__c", "Project_Line__c", "Project__c"]);
    const invoice = objects.find((o) => o.object === "Invoice__c")!;
    assert.ok("fields" in invoice);
    assert.equal(invoice.records, 12_000);
    assert.deepEqual(invoice.fields.find((f) => f.field === "Dispute_Reason__c"), { field: "Dispute_Reason__c", filled: 0, share: 0 });
    assert.deepEqual(invoice.fields.find((f) => f.field === "Amount__c"), { field: "Amount__c", filled: 11_400, share: 0.95 });
    assert.deepEqual(
      invoice.skipped.map((s) => s.field),
      ["Id", "Notes__c", "Total_With_Tax__c", "Paid__c"],
    );
    assert.match(invoice.skipped.find((s) => s.field === "Notes__c")!.reason, /can't count a field of this type \(textarea\)/);
    assert.match(invoice.skipped.find((s) => s.field === "Paid__c")!.reason, /checkbox/);
    assert.match(invoice.skipped.find((s) => s.field === "Total_With_Tax__c")!.reason, /formula/);
    const opportunity = objects.find((o) => o.object === "Opportunity")!;
    assert.ok("fields" in opportunity);
    assert.equal(opportunity.fields.find((f) => f.field === "Old_Region__c")?.filled, 0);
    // An object Salesforce wouldn't describe is that object's gap, not the section's.
    assert.ok("unavailable" in objects.find((o) => o.object === "Project_Line__c")!);
    // Every field the describe has is either counted or skipped.
    const account = objects.find((o) => o.object === "Account")!;
    assert.ok("fields" in account);
    assert.equal(account.fields.length + account.skipped.length, 159);
  });

  test("packages: the deprecated version is marked, with its seats, and a namespace with only a licence is listed as not installed", () => {
    const packages = rows(inventory.packages);
    const acme = packages.find((p) => p.namespace === "acmecpq")!;
    assert.deepEqual(acme, { packageId: "033000000000001AAA", namespace: "acmecpq", name: "Acme CPQ", version: "3.1.0.4", versionName: "Spring", installed: true, deprecated: true, managed: true, licence: { status: "Active", allowed: 20, siteWide: false, used: 12, expiresOn: null } });
    assert.equal(packages.find((p) => p.namespace === "SBQQ")!.deprecated, false);
    assert.deepEqual(packages.find((p) => p.namespace === "sitewide")!.licence, { status: "Active", allowed: null, siteWide: true, used: 40, expiresOn: null });
    assert.equal(packages.find((p) => p.namespace === "oldapp")!.installed, false);
    assert.equal(packages.find((p) => p.namespace === "oldapp")!.licence?.expiresOn, "2025-01-31");
  });

  test("connected apps: the app only users authorised, with three people and no policy, and the installed apps' policies", () => {
    const apps = rows(inventory.connectedApps);
    assert.deepEqual(apps.find((a) => a.name === "Notes Helper"), {
      name: "Notes Helper",
      installed: false,
      adminApprovedOnly: null,
      firstAuthorised: "2026-09-01T12:00:00.000Z",
      lastUsed: "2026-10-29T12:00:00.000Z",
      users: 3,
      uses: 29,
    });
    assert.deepEqual(apps.find((a) => a.name === "Data Loader")!.adminApprovedOnly, false);
    assert.deepEqual([apps.find((a) => a.name === "Data Loader")!.users, apps.find((a) => a.name === "Data Loader")!.uses], [2, 53]);
    assert.equal(apps.find((a) => a.name === "Finance Sync")!.adminApprovedOnly, true);
  });

  test("risky permissions: the set granting Author Apex to five active people, and the permission the org doesn't have", () => {
    const section = inventory.riskyPermissions;
    assert.ok("rows" in section);
    assert.deepEqual(section.rows.find((r) => r.apiName === "Sales_Console_User"), { kind: "permission set", name: "Sales Console User", apiName: "Sales_Console_User", namespace: null, grants: ["Author Apex"], activeUsers: 5 });
    assert.deepEqual(section.notInOrg, ["Approve Uninstalled Connected Apps"]);
    assert.equal(section.checked.length, 14);
    assert.match(section.notes.join(" "), /not checked: Approve Uninstalled Connected Apps/);
    // A profile counts its active users; a set nobody's grant touches isn't listed.
    assert.equal(section.rows.find((r) => r.kind === "profile" && r.name === "System Administrator")!.grants.length, 14);
    assert.ok(!section.rows.some((r) => r.apiName === "Old_Territory_Access"));
    // A set held only through a permission set group counts that group's holder.
    assert.equal(section.rows.find((r) => r.apiName === "Report_Builder")!.activeUsers, 1);
  });

  test("limits: every limit the org reported, with used and maximum", () => {
    assert.deepEqual(rows(inventory.limits), [
      { name: "DailyApiRequests", max: 100_000, used: 1_000 },
      { name: "DailyAsyncApexExecutions", max: 250_000, used: 240_000 },
      { name: "DataStorageMB", max: 10_240, used: 8_940 },
      { name: "FileStorageMB", max: 20_480, used: 4_480 },
    ]);
  });

  test("names no person: no user id, name or email anywhere in the file", () => {
    const text = readFileSync(join(dir, INVENTORY_FILE), "utf8");
    assert.doesNotMatch(text, /005\d{12}/);
    assert.doesNotMatch(text, /@/);
    for (const u of USERS) assert.ok(!text.includes(u.name), u.name);
    assert.ok(!text.includes(userId(3).slice(0, 15)));
  });
});

describe("a run without the inventory", () => {
  test("reads and writes what it did before: no inventory step and no file", async () => {
    const { dir, run } = await runFromRecording({ modules: ALL_MODULES });
    assert.ok(!run.steps.some((s) => s.id.startsWith("inventory")));
    assert.ok(!existsSync(join(dir, INVENTORY_FILE)));
  });
});

describe("a set-up only scope", () => {
  test("never counts fill rates: no step, no COUNT query, and the section says why", async () => {
    const { run, transport, inventory } = await simulated({}, { scope: "essentials" });
    assert.ok(!run.steps.some((s) => s.id === fillRatesExtractor.id));
    const sent = transport.requests.flatMap(soqlOf);
    assert.ok(!sent.some((q) => /COUNT\(/i.test(q)), sent.filter((q) => /COUNT\(/i.test(q)).join("\n"));
    assert.deepEqual(inventory.fillRates, { unavailable: SETUP_ONLY_FILL_RATES });
    assert.ok("rows" in inventory.packages && "rows" in inventory.riskyPermissions);
  });

  test("a complete one does count them", async () => {
    const { run } = await simulated();
    assert.ok(run.steps.some((s) => s.id === fillRatesExtractor.id && s.status === "done"));
  });
});

/** Every SOQL a request sends: its q, or each composite subrequest's. */
function soqlOf(request: { url: string; body?: string }): string[] {
  const url = new URL(request.url);
  if (url.pathname.endsWith("/composite/batch")) {
    const { batchRequests } = JSON.parse(request.body ?? "{}") as { batchRequests: Array<{ url: string }> };
    return batchRequests.map((b) => new URLSearchParams(b.url.split("?")[1]).get("q") ?? "");
  }
  const q = url.searchParams.get("q");
  return q ? [q] : [];
}

// Set-up objects: what they hold is how the org is built, not a customer's business records.
const SETUP_OBJECTS = new Set(["InstalledSubscriberPackage", "PackageLicense", "ConnectedApplication", "OauthToken", "PermissionSet", "PermissionSetAssignment", "PermissionSetGroupComponent", "Organization", "ObjectPermissions", "User"]);

describe("what the inventory reads", () => {
  test("no query selects a field's value from a business record: from those, only COUNT", async () => {
    // No module, so every step reads for itself what an earlier step would have given it.
    const { transport, run } = await simulated();
    const before = new Simulator();
    await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: [BARE], outDir: tempDir(), now: new Date(AS_OF), transport: before, connectedUser: CONNECTED_USER });
    const baseline = new Set(before.requests.flatMap(soqlOf));
    const added = transport.requests.flatMap(soqlOf).filter((q) => !baseline.has(q));
    assert.ok(added.length >= 8, added.join("\n"));
    assert.ok(run.steps.filter((s) => s.id.startsWith("inventory")).every((s) => s.status === "done"));
    for (const q of added) {
      const match = /^SELECT (.+?) FROM ([A-Za-z0-9_]+)/.exec(q);
      assert.ok(match, q);
      if (SETUP_OBJECTS.has(match[2]!)) continue;
      for (const item of match[1]!.split(",").map((s) => s.trim())) assert.match(item, /^COUNT\([A-Za-z0-9_]+\) [a-z0-9]+$/, `${match[2]}: ${item}`);
    }
    assert.ok(added.some((q) => /^SELECT COUNT\(Id\) n, COUNT\(Dispute_Reason__c\)/.test(q) || /COUNT\(Dispute_Reason__c\)/.test(q)));
  });

  test("every request goes through a route the allowlist already had: query, describe, composite batch, the record counts or the limits", async () => {
    const { transport } = await simulated();
    const allowed = new Set(["rest.versions", "rest.limits", "rest.recordCount", "rest.query", "rest.queryMore", "rest.describeGlobal", "rest.describe", "tooling.query", "tooling.queryMore", "rest.compositeBatch", "metadata.retrieve", "metadata.checkRetrieveStatus"]);
    for (const r of transport.requests) {
      const url = new URL(r.url);
      assert.ok(allowed.has(authorise({ method: r.method, path: `${url.pathname}${url.search}`, headers: r.headers, body: r.body })));
    }
  });
});

describe("a read that fails is { unavailable }, never a guess", () => {
  test("connected apps, when neither the apps nor the tokens can be read", async () => {
    const { inventory } = await simulated({ failQuery: (q) => (/FROM (ConnectedApplication|OauthToken)\b/.test(q) ? { errorCode: "INSUFFICIENT_ACCESS", message: "No access" } : null) });
    assert.deepEqual(inventory.connectedApps, { unavailable: "Neither the connected apps nor their OAuth tokens could be read: ConnectedApplication couldn't be read, because the person who connected Salesforce doesn't have permission to see it." });
  });

  test("connected apps, with only the tokens unread: the apps are listed, their use null, and a note says why", async () => {
    const { inventory } = await simulated({ failQuery: (q) => (/FROM OauthToken\b/.test(q) ? { errorCode: "INSUFFICIENT_ACCESS", message: "No access" } : null) });
    const apps = rows(inventory.connectedApps);
    assert.deepEqual(apps.map((a) => [a.name, a.users, a.uses]), [["Data Loader", null, null], ["Finance Sync", null, null]]);
    assert.ok("notes" in inventory.connectedApps && /OAuth tokens couldn't be read/.test(inventory.connectedApps.notes[0]!));
  });

  test("packages, when neither the installed list nor the licences can be read", async () => {
    const { inventory } = await simulated({ failQuery: (q) => (/FROM (InstalledSubscriberPackage|PackageLicense)\b/.test(q) ? { errorCode: "INSUFFICIENT_ACCESS", message: "No access" } : null) });
    assert.ok("unavailable" in inventory.packages);
  });

  test("packages, when Salesforce refuses the two flags: read again without them, and they're null, not false", async () => {
    const { inventory } = await simulated({ failQuery: (q) => (/IsDeprecated/.test(q) ? { errorCode: "INVALID_FIELD", message: "No such column 'IsDeprecated' on entity 'SubscriberPackageVersion'." } : null) });
    const acme = rows(inventory.packages).find((p) => p.namespace === "acmecpq")!;
    assert.equal(acme.deprecated, null);
    assert.equal(acme.version, "3.1.0.4");
    assert.ok("notes" in inventory.packages && /didn't say whether each version is deprecated/.test(inventory.packages.notes.join(" ")));
  });

  test("risky permissions, when PermissionSet can't be described", async () => {
    const { inventory } = await simulated({ missingObjects: ["PermissionSet"] });
    assert.ok("unavailable" in inventory.riskyPermissions);
  });

  test("risky permissions, with the assignments unread: each set is listed, its holders null", async () => {
    const { inventory } = await simulated({ failQuery: (q) => (/FROM PermissionSetAssignment\b/.test(q) ? { errorCode: "INSUFFICIENT_ACCESS", message: "No access" } : null) });
    const section = inventory.riskyPermissions;
    assert.ok("rows" in section);
    assert.ok(section.rows.length > 0 && section.rows.every((r) => r.activeUsers === null));
  });

  test("fill rates, when the counts fail: each object says so, and the section still lists them", async () => {
    const { inventory } = await simulated({ failQuery: (q) => (/^SELECT COUNT\(Id\) n,/.test(q) ? { errorCode: "QUERY_TIMEOUT", message: "Your query request was running for too long." } : null) });
    const objects = rows(inventory.fillRates);
    const counted = objects.filter((o) => "fields" in o);
    assert.deepEqual(counted, []);
    assert.equal((objects.find((o) => o.object === "Account") as { unavailable: string }).unavailable, "Account couldn't be read, because Salesforce didn't answer as it should.");
  });

  test("fill rates, without the record counts: the core objects only, and a note says why", async () => {
    const { inventory } = await simulated({ missingObjects: [] }, { transport: new FailRecordCount() });
    assert.deepEqual(rows(inventory.fillRates).map((o) => o.object), ["Account", "Contact", "Lead", "Opportunity"]);
    assert.ok("notes" in inventory.fillRates && /record count of each object couldn't be read/.test(inventory.fillRates.notes[0]!));
  });

  test("limits and any step that didn't run", () => {
    const source = { kind: "rest", path: "limits" } as const;
    const inventory = buildInventory(new Map<string, unknown>([["inventory-limits", { status: "unavailable", reason: "The org reported no limits.", source }]]), "complete", new Date(AS_OF));
    assert.deepEqual(inventory.limits, { unavailable: "The org reported no limits." });
    assert.deepEqual(inventory.packages, { unavailable: "This step didn't run." });
    assert.ok(Inventory.safeParse(inventory).success);
  });
});

/** The simulator, with the record count resource refused as a user without access is. */
class FailRecordCount extends Simulator {
  override async send(request: Parameters<Simulator["send"]>[0]) {
    if (new URL(request.url).pathname.endsWith("/limits/recordCount")) return { status: 403, header: () => null, text: JSON.stringify([{ errorCode: "INSUFFICIENT_ACCESS", message: "No access" }]) };
    return super.send(request);
  }
}

test("the object cap is the 300 with the most records, and the rest are counted in a note", () => {
  assert.equal(FILL_RATE_MAX_OBJECTS, 300);
  const source = { kind: "soql", api: "rest", query: "" } as const;
  const inventory = buildInventory(new Map<string, unknown>([[fillRatesExtractor.id, { status: "ok", source, truncated: true, data: { objects: [], leftOut: 12, scopeNote: null } }]]), "complete", new Date(AS_OF));
  assert.ok("notes" in inventory.fillRates);
  assert.deepEqual(inventory.fillRates.notes, ["12 more objects with records weren't counted: we count the 300 with the most records."]);
});

test("on an org with more objects than the cap, the core four come first, then the most records, and the rest are counted, not read", async () => {
  const many = Array.from({ length: 310 }, (_, i) => ({ name: `Bulk_${String(i).padStart(3, "0")}__c`, count: 1_000 + i }));
  const { inventory, transport } = await simulated({ records: { ...SYNTHETIC_RECORDS, recordCounts: [...many, { name: "Account", count: 500 }] } });
  const objects = rows(inventory.fillRates).map((o) => o.object);
  assert.equal(objects.length, 300);
  assert.deepEqual(objects.slice(0, 6), ["Account", "Contact", "Lead", "Opportunity", "Bulk_309__c", "Bulk_308__c"]);
  assert.ok(!objects.includes("Bulk_013__c"));
  assert.ok("notes" in inventory.fillRates && inventory.fillRates.notes.includes("14 more objects with records weren't counted: we count the 300 with the most records."));
  assert.ok(!transport.requests.some((r) => r.url.includes("/sobjects/Bulk_013__c/")));
});
