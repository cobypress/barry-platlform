import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { buildInventory, connectedAppsExtractor, fillRatesExtractor, INVENTORY_FILE, LIMITS_LISTED, limitsExtractor, NOT_PUT_TOGETHER, packagesExtractor, riskyPermissionsExtractor } from "../../extract/inventory.ts";
import type { ExtractContext, SourceInfo } from "../../extract/types.ts";
import { Inventory, NAME_MAX } from "../../schemas/inventory.ts";
import { loadAnswers, loadRecording, ReplayTransport } from "../../testing/replay.ts";
import { processRunJob } from "../run-job.ts";
import { INVENTORY_MAX_BYTES, inventoryOf, NO_FILE, OBJECT_MISFIT, SECTION_MISFIT, SECTIONS, settleInventory } from "../inventory.ts";
import type { MemoryWorkerStore } from "../memory-store.ts";
import type { RunOutputRecord } from "../types.ts";
import { baseDeps, RUN_ID, runInput, syntheticConnection } from "./helpers.ts";

// DECISIONS.md 618: an inventory is never dropped whole and never silently.
// What a real org can give that the first engine's inventory refused, each
// fixed where it's made or held to its own part; a section that fails costs
// only itself; an oversized one keeps the objects with the most records; a run
// with no file still gets a row saying so; every delivery saves the same; and
// nothing from the org reaches a log line. Every name and value here is made
// up: "PLANTED" marks what must never be logged.

const SOURCE: SourceInfo = { kind: "soql", api: "rest", query: "SELECT made up" };
const ok = <T>(data: T) => ({ status: "ok" as const, data, source: SOURCE, truncated: false });
const MADE = { runId: RUN_ID, madeAt: "2026-10-31T12:00:00.000Z", scope: "complete" as const };
const AS_OF = new Date(MADE.madeAt);
const LONG = `PLANTED_${"x".repeat(300)}`;

/** A clean extract of every step, made up and small. */
function extracts(): Map<string, unknown> {
  return new Map<string, unknown>([
    [fillRatesExtractor.id, ok({ objects: [{ object: "Account", records: 10, fields: [{ field: "Name", filled: 10, share: 1 }], skipped: [] }, { object: "Empty__c", records: 0, fields: [{ field: "Code__c", filled: 0, share: null }], skipped: [] }], leftOut: 0, scopeNote: null })],
    [packagesExtractor.id, { installed: ok([{ SubscriberPackageId: "033000000000001AAA", SubscriberPackage: { Name: "Made Up", NamespacePrefix: "mu" }, SubscriberPackageVersion: { Name: "Spring", MajorVersion: 1, MinorVersion: 2, PatchVersion: 0, BuildNumber: 3, IsDeprecated: false, IsManaged: true } }]), licences: ok([{ NamespacePrefix: "mu", AllowedLicenses: 10, UsedLicenses: 4, Status: "Active", ExpirationDate: "2027-01-31T00:00:00.000+0000" }]) }],
    [connectedAppsExtractor.id, { apps: ok([{ Name: "Made Up App", OptionsAllowAdminApprovedUsersOnly: true }]), use: ok([{ name: "Made Up App", users: 2, firstAuthorised: "2025-01-01T00:00:00.000Z", lastUsed: "2026-10-01T00:00:00.000Z", uses: 9 }]) }],
    [riskyPermissionsExtractor.id, { checked: [{ field: "PermissionsAuthorApex", label: "Author Apex" }], notInOrg: [], sets: ok([{ kind: "permission set", name: "Made Up Set", apiName: "Made_Up_Set", namespace: null, grants: ["Author Apex"], activeUsers: 3 }]), holdersGap: null }],
    [limitsExtractor.id, ok({ DailyApiRequests: { Max: 100, Remaining: 90 } })],
  ]);
}

const sectionsOf = (inv: Inventory) => SECTIONS.map((k) => inv[k]);
/** As the worker saves it: written, read back and checked, with the log kept. */
function saved(inv: unknown): { inventory: Inventory; logs: string[] } {
  const logs: string[] = [];
  const dir = mkdtempSync(join(tmpdir(), "hc-inventory-saved-"));
  writeFileSync(join(dir, INVENTORY_FILE), JSON.stringify(inv, null, 2));
  const inventory = inventoryOf(dir, (l) => void logs.push(l), MADE);
  assert.ok(Inventory.safeParse(inventory).success, "the row the console gets parses");
  return { inventory, logs };
}
const rowsOf = <T>(section: { rows: T[] } | { unavailable: string }): T[] => {
  assert.ok("rows" in section, `unavailable: ${"unavailable" in section ? section.unavailable : ""}`);
  return section.rows;
};
const notesOf = (section: object): string[] => ("notes" in section ? (section.notes as string[]) : []);

describe("each value a real org can give is saved, the bad part marked and the rest intact (DECISIONS.md 618)", () => {
  const clean = buildInventory(extracts(), "complete", AS_OF);

  test("a limit with more remaining than its maximum is kept as reported, below zero, and named in the notes", () => {
    const e = extracts();
    e.set(limitsExtractor.id, ok({ DailyApiRequests: { Max: 100, Remaining: 90 }, PLANTED_Storage: { Max: 5, Remaining: 7 } }));
    const { inventory, logs } = saved(buildInventory(e, "complete", AS_OF));
    assert.deepEqual(rowsOf(inventory.limits), [{ name: "DailyApiRequests", max: 100, used: 10 }, { name: "PLANTED_Storage", max: 5, used: -2 }]);
    assert.ok(notesOf(inventory.limits).some((n) => /more remaining than the maximum for PLANTED_Storage/.test(n)));
    assert.deepEqual(sectionsOf(inventory).slice(0, 4), sectionsOf(clean).slice(0, 4));
    assert.deepEqual(logs, []);
  });

  test("a limit with no maximum isn't listed, and the notes say only limits with both figures are", () => {
    const e = extracts();
    e.set(limitsExtractor.id, ok({ DailyApiRequests: { Max: 100, Remaining: 90 }, NoMax: { Max: Number.NaN, Remaining: 3 } }));
    const { inventory } = saved(buildInventory(e, "complete", AS_OF));
    assert.deepEqual(rowsOf(inventory.limits).map((r) => r.name), ["DailyApiRequests"]);
    assert.ok(notesOf(inventory.limits).includes(LIMITS_LISTED));
  });

  test("a limit's maximum or use that isn't a whole number is kept as reported", () => {
    const e = extracts();
    e.set(limitsExtractor.id, ok({ DataStorageMB: { Max: 1024.5, Remaining: 1000.25 } }));
    const { inventory } = saved(buildInventory(e, "complete", AS_OF));
    assert.deepEqual(rowsOf(inventory.limits), [{ name: "DataStorageMB", max: 1024.5, used: 24.25 }]);
  });

  test("a name longer than we keep is cut, ends in an ellipsis, and the section's notes count it, in every section that names things", () => {
    const e = extracts();
    e.set(packagesExtractor.id, { installed: ok([{ SubscriberPackageId: "033000000000001AAA", SubscriberPackage: { Name: LONG, NamespacePrefix: "mu" }, SubscriberPackageVersion: { Name: LONG, MajorVersion: 1 } }]), licences: ok([]) });
    e.set(connectedAppsExtractor.id, { apps: ok([{ Name: LONG, OptionsAllowAdminApprovedUsersOnly: null }]), use: ok([]) });
    e.set(riskyPermissionsExtractor.id, { checked: [{ field: "PermissionsAuthorApex", label: "Author Apex" }], notInOrg: [], sets: ok([{ kind: "permission set", name: LONG, apiName: LONG, namespace: null, grants: ["Author Apex"], activeUsers: 1 }]), holdersGap: null });
    e.set(limitsExtractor.id, ok({ [LONG]: { Max: 1, Remaining: 1 } }));
    const { inventory } = saved(buildInventory(e, "complete", AS_OF));
    const pkg = rowsOf(inventory.packages)[0]!;
    assert.equal(pkg.name!.length, NAME_MAX);
    assert.ok(pkg.name!.endsWith("…") && pkg.name!.startsWith("PLANTED_"));
    assert.equal(rowsOf(inventory.connectedApps)[0]!.name.length, NAME_MAX);
    assert.equal(rowsOf(inventory.riskyPermissions)[0]!.apiName.length, NAME_MAX);
    assert.equal(rowsOf(inventory.limits)[0]!.name.length, NAME_MAX);
    assert.ok(notesOf(inventory.packages).some((n) => /2 names were longer than we keep/.test(n)));
    for (const key of ["connectedApps", "riskyPermissions", "limits"] as const) assert.ok(notesOf(inventory[key]).some((n) => /longer than we keep/.test(n)), key);
    assert.deepEqual(inventory.fillRates, clean.fillRates);
  });

  test("a cut never splits a character in two", () => {
    const e = extracts();
    e.set(limitsExtractor.id, ok({ [`${"x".repeat(NAME_MAX - 2)}\u{1F600}tail`]: { Max: 1, Remaining: 1 } }));
    const name = rowsOf(saved(buildInventory(e, "complete", AS_OF)).inventory.limits)[0]!.name;
    assert.ok(!/[\uD800-\uDBFF](?![\uDC00-\uDFFF])/.test(name), "no lone high surrogate");
  });

  test("an empty name: a connected app or a set without one is left out and counted in the notes", () => {
    const e = extracts();
    e.set(connectedAppsExtractor.id, { apps: ok([{ Name: "", OptionsAllowAdminApprovedUsersOnly: null }, { Name: "Made Up App", OptionsAllowAdminApprovedUsersOnly: true }]), use: ok([]) });
    e.set(riskyPermissionsExtractor.id, { checked: [{ field: "PermissionsAuthorApex", label: "Author Apex" }], notInOrg: [], sets: ok([{ kind: "permission set", name: "", apiName: "", namespace: null, grants: ["Author Apex"], activeUsers: 1 }, { kind: "profile", name: "", apiName: "X00e000000000001", namespace: null, grants: ["Author Apex"], activeUsers: 1 }]), holdersGap: null });
    const { inventory } = saved(buildInventory(e, "complete", AS_OF));
    assert.deepEqual(rowsOf(inventory.connectedApps).map((r) => r.name), ["Made Up App"]);
    assert.ok(notesOf(inventory.connectedApps).some((n) => /One connected app has no name/.test(n)));
    // A profile without a label goes by its API name, never by nothing.
    assert.deepEqual(rowsOf(inventory.riskyPermissions).map((r) => r.name), ["X00e000000000001"]);
    assert.ok(notesOf(inventory.riskyPermissions).some((n) => /no API name/.test(n)));
  });

  test("a package with no name, version or id is listed with each as null", () => {
    const e = extracts();
    e.set(packagesExtractor.id, { installed: ok([{ SubscriberPackageId: null, SubscriberPackage: null, SubscriberPackageVersion: null }]), licences: ok([]) });
    const row = rowsOf(saved(buildInventory(e, "complete", AS_OF)).inventory.packages)[0]!;
    assert.deepEqual([row.packageId, row.name, row.version, row.versionName, row.namespace], [null, null, null, null, null]);
  });

  test("a package id longer than a Salesforce id isn't kept, and the notes say so", () => {
    const e = extracts();
    e.set(packagesExtractor.id, { installed: ok([{ SubscriberPackageId: "033000000000001AAAPLANTED", SubscriberPackage: { Name: "Made Up", NamespacePrefix: null } }]), licences: ok([]) });
    const { inventory } = saved(buildInventory(e, "complete", AS_OF));
    assert.equal(rowsOf(inventory.packages)[0]!.packageId, null);
    assert.ok(notesOf(inventory.packages).some((n) => /longer than a Salesforce id/.test(n)));
  });

  test("a licence with no status keeps null, and Salesforce's +0000 expiry date is a calendar date", () => {
    const e = extracts();
    e.set(packagesExtractor.id, { installed: ok([]), licences: ok([{ NamespacePrefix: "mu", AllowedLicenses: -1, UsedLicenses: 3, Status: null, ExpirationDate: "2027-01-31T00:00:00.000+0000" }]) });
    const licence = rowsOf(saved(buildInventory(e, "complete", AS_OF)).inventory.packages)[0]!.licence!;
    assert.deepEqual(licence, { status: null, allowed: null, siteWide: true, used: 3, expiresOn: "2027-01-31" });
  });

  test("a licence's seats are kept only as Salesforce counted them: no count is null with a note, never 0, and only -1 allowed is site-wide (rule 5)", () => {
    const e = extracts();
    e.set(packagesExtractor.id, {
      installed: ok([]),
      licences: ok([
        { NamespacePrefix: "aa", AllowedLicenses: 10, UsedLicenses: 4, Status: "Active", ExpirationDate: null },
        { NamespacePrefix: "bb", AllowedLicenses: 10, UsedLicenses: null, Status: "Active", ExpirationDate: null },
        { NamespacePrefix: "cc", AllowedLicenses: -1, UsedLicenses: -1, Status: "Active", ExpirationDate: null },
        { NamespacePrefix: "dd", AllowedLicenses: null, UsedLicenses: 2, Status: "Active", ExpirationDate: null },
      ]),
    });
    const { inventory } = saved(buildInventory(e, "complete", AS_OF));
    const seats = rowsOf(inventory.packages).map((r) => [r.namespace, r.licence!.allowed, r.licence!.siteWide, r.licence!.used]);
    assert.deepEqual(seats, [
      ["aa", 10, false, 4],
      ["bb", 10, false, null],
      ["cc", null, true, null],
      ["dd", null, false, 2],
    ]);
    assert.ok(notesOf(inventory.packages).includes("2 licences gave no count of seats used, so their use isn't shown."), notesOf(inventory.packages).join("\n"));
    assert.ok(notesOf(inventory.packages).includes("One licence gave no number of seats allowed, so its allowance isn't shown."));
  });

  test("an object with no records has a null share and is saved", () => {
    const { inventory } = saved(clean);
    assert.deepEqual(rowsOf(inventory.fillRates)[1], { object: "Empty__c", records: 0, fields: [{ field: "Code__c", filled: 0, share: null }], skipped: [] });
  });

  test("an OAuth token's dates in Salesforce's +0000 form are saved as timestamps", async () => {
    const tokens = [
      { AppName: "Made Up App", UserId: "005000000000001AAA", CreatedDate: "2025-01-01T09:30:00.000+0000", LastUsedDate: "2026-10-01T10:00:00.000+0000", UseCount: 4 },
      { AppName: "Made Up App", UserId: "005000000000002AAA", CreatedDate: "2025-02-01T09:30:00.000+0000", LastUsedDate: null, UseCount: null },
    ];
    const ctx = {
      get: () => {
        throw new Error("not in this run");
      },
      connectedUser: { id: "005000000000001AAA" },
      client: { query: async (soql: string) => ({ records: /FROM OauthToken/.test(soql) ? tokens : [{ Name: "Made Up App", OptionsAllowAdminApprovedUsersOnly: false }], truncated: false }) },
    } as unknown as ExtractContext;
    const e = extracts();
    e.set(connectedAppsExtractor.id, await connectedAppsExtractor.run(ctx));
    const row = rowsOf(saved(buildInventory(e, "complete", AS_OF)).inventory.connectedApps)[0]!;
    assert.deepEqual([row.firstAuthorised, row.lastUsed, row.users, row.uses], ["2025-01-01T09:30:00.000Z", "2026-10-01T10:00:00.000Z", 2, 4]);
  });

  test("a fill-rate object that doesn't fit (an unknown key, a count that isn't whole, a name too long) is that object's unavailable, and the rest of the section is kept", () => {
    const inv = structuredClone(clean) as unknown as { fillRates: { rows: unknown[]; notes: string[] } };
    inv.fillRates.rows.push(
      { object: "PLANTED_Extra__c", records: 5, fields: [], skipped: [], PLANTED_key: "PLANTED_value" },
      { object: "PLANTED_Half__c", records: 5.5, fields: [], skipped: [] },
      { object: LONG, records: 1, fields: [], skipped: [] },
      { records: 1, fields: [] },
    );
    const { inventory, logs } = saved(inv);
    const rows = rowsOf(inventory.fillRates);
    assert.deepEqual(rows.slice(0, 2), rowsOf(clean.fillRates));
    assert.deepEqual(rows.slice(2, 4), [{ object: "PLANTED_Extra__c", unavailable: OBJECT_MISFIT }, { object: "PLANTED_Half__c", unavailable: OBJECT_MISFIT }]);
    assert.equal(rows[4]!.object.length, NAME_MAX);
    assert.equal(rows.length, 5);
    assert.ok(notesOf(inventory.fillRates).some((n) => /3 objects' counts didn't fit/.test(n)));
    assert.ok(notesOf(inventory.fillRates).some((n) => /One object couldn't be named/.test(n)));
    assert.deepEqual(sectionsOf(inventory).slice(1), sectionsOf(clean).slice(1));
    assert.ok(logs.includes(`Run ${RUN_ID}'s inventory: fillRates object 2 didn't fit its shape ((top) unrecognized_keys), so it's saved as unavailable.`), logs.join("\n"));
    assert.ok(logs.includes(`Run ${RUN_ID}'s inventory: fillRates object 3 didn't fit its shape (records invalid_type), so it's saved as unavailable.`), logs.join("\n"));
    assert.ok(logs.includes(`Run ${RUN_ID}'s inventory: fillRates object 4 didn't fit its shape (object too_big), so it's saved as unavailable.`), logs.join("\n"));
  });

  test("an unknown key in any other section costs that section alone, which says so", () => {
    for (const key of ["packages", "connectedApps", "riskyPermissions", "limits"] as const) {
      const inv = structuredClone(clean) as unknown as Record<string, { rows: Array<Record<string, unknown>> }>;
      inv[key]!.rows[0]!.PLANTED_key = "PLANTED_value";
      const { inventory, logs } = saved(inv);
      assert.deepEqual(inventory[key], { unavailable: SECTION_MISFIT }, key);
      for (const other of SECTIONS.filter((k) => k !== key)) assert.deepEqual(inventory[other], clean[other], `${key} cost ${other}`);
      assert.ok(logs.some((l) => l.includes(`${key} didn't fit its shape (rows.0 unrecognized_keys)`)), logs.join("\n"));
    }
  });

  test("a section that can't be put together is unavailable with why, and the others are built", () => {
    const e = extracts();
    e.set(limitsExtractor.id, ok(null));
    const inventory = buildInventory(e, "complete", AS_OF);
    assert.deepEqual(inventory.limits, { unavailable: NOT_PUT_TOGETHER });
    assert.deepEqual(sectionsOf(inventory).slice(0, 4), sectionsOf(clean).slice(0, 4));
  });

  test("a null row in the installed packages costs packages alone, and one log line names the section and the error's class, nothing from the org", () => {
    const e = extracts();
    e.set(packagesExtractor.id, { installed: ok([null, { SubscriberPackageId: "033000000000009AAA", SubscriberPackage: { Name: "PLANTED_PACKAGE", NamespacePrefix: "PLANTED" } }]), licences: ok([]) });
    const logs: string[] = [];
    const inventory = buildInventory(e, "complete", AS_OF, new Map(), (l) => void logs.push(l));
    assert.deepEqual(inventory.packages, { unavailable: NOT_PUT_TOGETHER });
    for (const k of SECTIONS.filter((k) => k !== "packages")) assert.deepEqual(inventory[k], clean[k]);
    assert.deepEqual(logs, ["  The inventory's packages couldn't be put together (TypeError), so it's saved as unavailable."]);
    assert.ok(Inventory.safeParse(saved(inventory).inventory).success);
  });
});

describe("one section failing leaves the other four saved", () => {
  test("a NUL in the packages section costs packages alone", () => {
    const clean = buildInventory(extracts(), "complete", AS_OF);
    const inv = structuredClone(clean);
    rowsOf(inv.packages)[0]!.name = "PLANTED\u0000";
    const { inventory, logs } = saved(inv);
    assert.deepEqual(inventory.packages, { unavailable: SECTION_MISFIT });
    for (const k of SECTIONS.filter((k) => k !== "packages")) assert.deepEqual(inventory[k], clean[k]);
    assert.deepEqual(logs, [`Run ${RUN_ID}'s inventory: packages held a NUL character at rows.0.name, so it's saved as unavailable.`]);
  });
});

describe("a lone half of a UTF-16 pair, which Postgres's jsonb refuses (class 22) and would roll back the whole save on every retry", () => {
  test("costs only its section, or only its object in fill rates, and the log never quotes it", () => {
    const clean = buildInventory(extracts(), "complete", AS_OF);
    const inv = structuredClone(clean);
    rowsOf(inv.connectedApps)[0]!.name = "PLANTED\ud83dX";
    const objects = rowsOf(inv.fillRates) as Array<{ object: string; fields: Array<{ field: string }> }>;
    objects[1]!.fields[0]!.field = "PLANTED\ude00";
    const { inventory, logs } = saved(inv);
    assert.deepEqual(inventory.connectedApps, { unavailable: SECTION_MISFIT });
    assert.deepEqual(rowsOf(inventory.fillRates), [rowsOf(clean.fillRates)[0], { object: "Empty__c", unavailable: OBJECT_MISFIT }]);
    for (const k of ["packages", "riskyPermissions", "limits"] as const) assert.deepEqual(inventory[k], clean[k]);
    assert.ok(!/\\ud[89a-f][0-9a-f]{2}/i.test(JSON.stringify(inventory)), "no surrogate escape left to store");
    assert.deepEqual(logs, [
      `Run ${RUN_ID}'s inventory: fillRates object 1 held a broken character (half of a UTF-16 pair) at fields.0.field, so it's saved as unavailable.`,
      `Run ${RUN_ID}'s inventory: connectedApps held a broken character (half of a UTF-16 pair) at rows.0.name, so it's saved as unavailable.`,
    ]);
  });
});

describe("an inventory larger than the worker sends", () => {
  /** 300 objects, each with more fields than most orgs have: over the cap however it's written. */
  function huge(): Inventory {
    const inv = buildInventory(extracts(), "complete", AS_OF);
    const fields = Array.from({ length: 450 }, (_, j) => ({ field: `Made_Up_Field_Number_${String(j).padStart(4, "0")}__c`, filled: 7, share: 0.5 }));
    const rows = Array.from({ length: 300 }, (_, i) => ({ object: `Made_Up_${String(i).padStart(3, "0")}__c`, records: (i * 7919) % 1000, fields, skipped: [] }));
    return { ...inv, fillRates: { rows: [...rows, { object: "Task", unavailable: "Hidden." }], notes: ["An earlier note."] } };
  }

  test("keeps the other four sections and fill rates for the objects with the most records, and says how many were left out", () => {
    const inv = huge();
    assert.ok(Buffer.byteLength(JSON.stringify(inv)) > INVENTORY_MAX_BYTES, "the fixture is over the cap");
    const { inventory, logs } = saved(inv);
    assert.ok(Buffer.byteLength(JSON.stringify(inventory)) < INVENTORY_MAX_BYTES);
    const kept = rowsOf(inventory.fillRates);
    const counted = kept.filter((r) => "records" in r) as Array<{ records: number }>;
    assert.ok(counted.length > 100 && counted.length < 300, String(counted.length));
    const least = Math.min(...counted.map((r) => r.records));
    const all = rowsOf(inv.fillRates).filter((r) => "records" in r) as Array<{ records: number }>;
    assert.ok(all.filter((r) => r.records > least).length < counted.length, "nothing with more records than one kept was left out");
    const left = rowsOf(inv.fillRates).length - kept.length;
    assert.ok(notesOf(inventory.fillRates).some((n) => n.includes(`kept for the ${kept.length} objects with the most records, and ${left} objects with fewer were left out`)));
    assert.equal(notesOf(inventory.fillRates)[0], "An earlier note.");
    for (const k of SECTIONS.filter((k) => k !== "fillRates")) assert.deepEqual(inventory[k], inv[k]);
    assert.ok(logs.some((l) => /fill rates keep the \d+ objects with the most records and leave out \d+/.test(l)));
  });

  test("one object larger than the cap on its own makes fill rates unavailable, and the rest is kept", () => {
    const inv = buildInventory(extracts(), "complete", AS_OF);
    const fields = Array.from({ length: 150_000 }, (_, j) => ({ field: `Made_Up_Field_${j}__c`, filled: 1, share: 1 }));
    const big = { ...inv, fillRates: { rows: [{ object: "Made_Up__c", records: 1, fields, skipped: [] }], notes: [] } };
    const settled = settleInventory(big, () => {}, MADE);
    assert.ok("unavailable" in settled.fillRates);
    for (const k of SECTIONS.filter((k) => k !== "fillRates")) assert.deepEqual(settled[k], inv[k]);
  });
});

describe("an inventory too large for a reason other than fill rates", () => {
  test("fill rates already unavailable keep their own reason, and the largest other section gives way", () => {
    const inv = buildInventory(extracts(), "complete", AS_OF);
    const notes = Array.from({ length: 150_000 }, (_, i) => `Made-up note number ${i} about a package, long enough to add up.`);
    const big = { ...inv, fillRates: { unavailable: "Made up reason." }, packages: { rows: rowsOf(inv.packages), notes } };
    const logs: string[] = [];
    const settled = settleInventory(big, (l) => void logs.push(l), MADE);
    assert.deepEqual(settled.fillRates, { unavailable: "Made up reason." });
    assert.ok("unavailable" in settled.packages && /too large to store/.test(settled.packages.unavailable));
    for (const k of ["connectedApps", "riskyPermissions", "limits"] as const) assert.deepEqual(settled[k], inv[k]);
    assert.ok(logs.some((l) => /so packages is saved as unavailable/.test(l)));
  });
});

describe("a run with no inventory file", () => {
  test("still gets a row whose five sections each say why", () => {
    const logs: string[] = [];
    const inventory = inventoryOf(mkdtempSync(join(tmpdir(), "hc-inventory-none-")), (l) => void logs.push(l), MADE);
    assert.deepEqual(inventory, { version: 1, madeAt: MADE.madeAt, scope: "complete", ...Object.fromEntries(SECTIONS.map((k) => [k, { unavailable: NO_FILE }])) });
    assert.deepEqual(logs, [`Run ${RUN_ID}'s inventory: there's no inventory.json, so every section is saved as unavailable.`]);
  });
});

describe("every delivery saves the same inventory (the real run job, the synthetic org's recording)", () => {
  async function setup() {
    const deps = baseDeps();
    await deps.tokens.save(syntheticConnection());
    deps.store.addRun(runInput());
    const deliver = (final = true) => {
      deps.dataTransport = new ReplayTransport(loadRecording());
      return processRunJob(RUN_ID, deps, { final });
    };
    const store = deps.store as MemoryWorkerStore;
    const inventory = (): Inventory => (store.runs.get(RUN_ID)!.output as RunOutputRecord).inventory!;
    /** The console saving the answers, then sending the run again. */
    const answer = () => {
      const run = store.runs.get(RUN_ID)!;
      assert.equal(run.run.status, "awaiting_manual");
      run.answers = loadAnswers();
    };
    return { deps, deliver, inventory, answer, dir: join(deps.runsDir, RUN_ID) };
  }

  test("a second delivery after the answers saves the inventory the first did", async () => {
    const w = await setup();
    assert.equal((await w.deliver()).outcome, "done");
    const first = w.inventory();
    assert.ok(SECTIONS.every((k) => "rows" in first[k]), "the synthetic org's inventory is whole");
    w.answer();
    assert.equal((await w.deliver()).outcome, "done");
    assert.deepEqual(w.inventory(), first);
  });

  test("a delivery whose save failed, tried again, saves the inventory one delivery would have", async () => {
    const once = await setup();
    await once.deliver();
    const w = await setup();
    const store = w.deps.store as MemoryWorkerStore;
    const save = store.saveRunOutput.bind(store);
    let tries = 0;
    store.saveRunOutput = async (id, output) => {
      if (tries++ === 0) throw new Error("connection lost");
      return save(id, output);
    };
    await assert.rejects(w.deliver(false), /connection lost/);
    assert.equal((await w.deliver()).outcome, "done");
    assert.deepEqual(w.inventory(), once.inventory());
  });

  test("a delivery that can't write the file saves a row that says so, and the run's results still save", async () => {
    const w = await setup();
    await w.deliver();
    w.answer();
    // Where the file goes is taken, so writing it fails.
    rmSync(join(w.dir, INVENTORY_FILE));
    mkdirSync(join(w.dir, INVENTORY_FILE, "taken"), { recursive: true });
    assert.equal((await w.deliver()).outcome, "done");
    assert.ok(sectionsOf(w.inventory()).every((s) => "unavailable" in s && s.unavailable === NO_FILE));
    assert.ok(w.deps.lines.some((l) => l === `Run ${RUN_ID}'s inventory: there's no inventory.json, so every section is saved as unavailable.`), w.deps.lines.join("\n"));
    // The engine's line names the error's class, never its message, which here would hold the folder's path.
    assert.ok(w.deps.lines.includes("  The inventory couldn't be written (Error), so the run has none."), w.deps.lines.join("\n"));
    assert.ok(!w.deps.lines.some((l) => l.includes(w.dir)));
  });

  test("nothing from the org's data reaches a log line: a limit holding planted values costs its section alone", async () => {
    const w = await setup();
    await w.deliver();
    const first = w.inventory();
    w.answer();
    // The limits step's checkpoint, as a later delivery reads it, now holding names no log may quote.
    const planted = { status: "ok", data: { "PLANTED_LIMIT\u0000": { Max: 5, Remaining: 1 }, PLANTED_OTHER: { Max: 1, Remaining: 0 } }, source: { kind: "rest", path: "/limits" }, truncated: false };
    writeFileSync(join(w.dir, "checkpoints", `${limitsExtractor.id}.json`), `${JSON.stringify(planted, null, 2)}\n`);
    assert.equal((await w.deliver()).outcome, "done");
    const second = w.inventory();
    assert.deepEqual(second.limits, { unavailable: SECTION_MISFIT });
    for (const k of SECTIONS.filter((k) => k !== "limits")) assert.deepEqual(second[k], first[k]);
    assert.ok(w.deps.lines.some((l) => l.includes("limits held a NUL character at rows.0.name")), w.deps.lines.join("\n"));
    assert.ok(!w.deps.lines.some((l) => /PLANTED/.test(l)), "a planted value reached the log");
  });
});

describe("the log never quotes the file", () => {
  test("planted names and values in every section's misfits never reach a line", () => {
    const inv = structuredClone(buildInventory(extracts(), "complete", AS_OF)) as unknown as Record<string, unknown>;
    inv.PLANTED_TOP = "PLANTED_TOP_VALUE";
    inv.madeAt = "PLANTED_DATE";
    inv.packages = { rows: [{ PLANTED_KEY: "PLANTED_VALUE" }], notes: ["PLANTED_NOTE"] };
    inv.connectedApps = { rows: [{ name: "PLANTED\u0000APP" }], notes: [] };
    inv.riskyPermissions = { rows: [], notes: [], checked: ["PLANTED"], notInOrg: [], PLANTED_EXTRA: 1 };
    inv.limits = { rows: [{ name: "PLANTED_LIMIT", max: "PLANTED_MAX", used: 1 }], notes: [] };
    (inv.fillRates as { rows: unknown[] }).rows.push({ object: "PLANTED_OBJECT", records: "PLANTED_RECORDS", fields: [{ PLANTED_FIELD: 1 }], skipped: [] });
    const { inventory, logs } = saved(inv);
    assert.ok(logs.length >= 6, logs.join("\n"));
    assert.ok(!logs.some((l) => /PLANTED/.test(l)), logs.join("\n"));
    assert.equal(inventory.madeAt, MADE.madeAt);
  });
});
