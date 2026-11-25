import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { ALL_MODULES } from "../../domains/index.ts";
import type { DomainModule } from "../../domains/types.ts";
import { runHealthCheck, type RunOptions } from "../../engine/run.ts";
import { Inventory, type RiskyPermissionHoldersRow } from "../../schemas/inventory.ts";
import type { Finding } from "../../schemas/index.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { buildScaleOrg, FIVE_HUNDRED_USERS, scaled } from "../../testing/scale-org.ts";
import { Simulator, type SimulatorOptions } from "../../testing/simulator.ts";
import { AS_OF, CONNECTED_USER } from "../../testing/synthetic-org.ts";
import { inventoryUnavailable, settleInventory } from "../../worker/inventory.ts";
import { FROZEN_NOT_READ, holderCounts, INVENTORY_FILE, MUTES_NOT_READ, NOT_IN_ORG, RISKY_PERMISSIONS, STANDARD_USERS_ONLY, USERS_NOT_READ, type HoldersInput } from "../inventory.ts";
import type { AccessData, PrivilegedPermissions } from "../security.ts";
import type { Sourced } from "../types.ts";
import type { LoginSummary, UserRow } from "../users.ts";

// Each risky permission's active holders, each counted once (DECISIONS.md 735):
// the figure for Modify and View All Data is the one in their checks' findings,
// a read that failed is never a zero, and an older file still parses.

const readInventory = (dir: string): Inventory => Inventory.parse(JSON.parse(readFileSync(join(dir, INVENTORY_FILE), "utf8")));
const findingsOf = (dir: string): Finding[] => {
  const raw = JSON.parse(readFileSync(join(dir, "findings.json"), "utf8")) as Finding[] | { findings: Finding[] };
  return Array.isArray(raw) ? raw : raw.findings;
};
const section = (inventory: Inventory) => {
  const s = inventory.riskyPermissionHolders;
  assert.ok(s && "rows" in s, `unavailable: ${s && "unavailable" in s ? s.unavailable : "missing"}`);
  return s;
};
const measured = (row: RiskyPermissionHoldersRow | undefined) => {
  assert.ok(row && "holders" in row, row && "unavailable" in row ? row.unavailable : "no row");
  return row;
};
const RULED = [
  ["Modify All Data", "sec.modify-all-data"],
  ["View All Data", "sec.view-all-data"],
] as const;

/** Every ruled permission's count against its finding in the same run. */
function assertSameAsFindings(inventory: Inventory, findings: Finding[]): void {
  const rows = section(inventory).rows;
  for (const [label, ruleId] of RULED) {
    const row = rows.find((r) => r.permission === label)!;
    const finding = findings.find((f) => f.ruleId === ruleId)!;
    assert.ok(finding, ruleId);
    assert.equal(row.rule, ruleId);
    if (finding.status === "not_measurable") {
      assert.ok("unavailable" in row, `${label} is counted though its check couldn't measure it`);
      assert.equal(row.unavailable, finding.notMeasurableReason);
    } else assert.equal(measured(row).holders, finding.facts.holders, label);
  }
  // No other row claims a check.
  assert.ok(rows.filter((r) => !RULED.some(([l]) => l === r.permission)).every((r) => r.rule === null));
}

describe("on the synthetic org, from the recording", async () => {
  const { dir } = await runFromRecording({ modules: ALL_MODULES, inventory: true });
  const inventory = readInventory(dir);

  test("Modify and View All Data are counted as their checks count them, and link to them", () => {
    assertSameAsFindings(inventory, findingsOf(dir));
    assert.deepEqual(section(inventory).rows.find((r) => r.permission === "Modify All Data"), { permission: "Modify All Data", rule: "sec.modify-all-data", holders: 3, byProfile: 2, byPermissionSet: 1, byGroup: 0, integration: 1, notLoggedIn: 0, unconfirmedThroughGroups: 0 });
  });

  test("every permission on the list has a row, in the list's order: one the org doesn't have says so, never as nobody", () => {
    const s = section(inventory);
    assert.deepEqual(s.rows.map((r) => r.permission), RISKY_PERMISSIONS.map((p) => p.label));
    const checked = "rows" in inventory.riskyPermissions ? inventory.riskyPermissions.checked : [];
    assert.deepEqual(s.rows.filter((r) => "holders" in r).map((r) => r.permission), checked);
    assert.deepEqual(s.rows.find((r) => r.permission === "Approve Uninstalled Connected Apps"), { permission: "Approve Uninstalled Connected Apps", rule: null, unavailable: NOT_IN_ORG });
    assert.equal(s.dormantDays, 90);
    assert.ok(s.notes.includes(FROZEN_NOT_READ));
    assert.ok(s.notes.includes(STANDARD_USERS_ONLY));
  });

  test("a person who holds a permission several ways is one holder: Author Apex is five on a set and one admin", () => {
    assert.deepEqual(measured(section(inventory).rows.find((r) => r.permission === "Author Apex")), { permission: "Author Apex", rule: null, holders: 6, byProfile: 1, byPermissionSet: 5, byGroup: 0, integration: 0, notLoggedIn: 2, unconfirmedThroughGroups: 0 });
    // Export Reports: 13 on the profile, one of them also on a set.
    const exports = measured(section(inventory).rows.find((r) => r.permission === "Export Reports"));
    assert.equal(exports.holders, 13);
    assert.equal(exports.byProfile + exports.byPermissionSet!, 14);
  });
});

describe("on the scale org", () => {
  test("the counts are the findings' too", async () => {
    const result = await runHealthCheck({ provider: syntheticProvider, connectedUser: CONNECTED_USER, calibration: loadCalibration(), modules: ALL_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport: new Simulator({ records: buildScaleOrg(scaled(FIVE_HUNDRED_USERS, 0.005)), pageSize: 2000 }), inventory: true });
    const inventory = readInventory(result.dir);
    assertSameAsFindings(inventory, findingsOf(result.dir));
    assert.ok(measured(section(inventory).rows.find((r) => r.permission === "Modify All Data")).holders >= 1);
  });
});

// ---------------------------------------------------------------------------
// Generated orgs: the checks and the count, on the same extracts.

/** A small seeded generator, so a failing case can be found again. */
function random(seed: number): () => number {
  let s = seed >>> 0;
  return () => ((s = (s * 1_664_525 + 1_013_904_223) >>> 0) / 2 ** 32);
}

const uid = (n: number): string => `005${String(n).padStart(12, "0")}AAA`;
const psid = (n: number): string => `0PS${String(n).padStart(12, "0")}AAA`;
const GROUP = `0PG${"1".padStart(12, "0")}AAA`;
const ok = <T>(data: T, query = "x"): Sourced<T> => ({ status: "ok", data, source: { kind: "soql", api: "rest", query }, truncated: false });
const refused = (what: string) => ({ status: "unavailable" as const, reason: `${what} couldn't be read, because the person who connected Salesforce doesn't have permission to see it.`, source: { kind: "soql" as const, api: "rest" as const, query: what } });

const at = (days: number) => new Date(new Date(AS_OF).getTime() - days * 86_400_000).toISOString().replace("Z", "+0000");
function user(n: number, profile: string | null, active = true, lastLoginDays: number | null = 1, licence = "Salesforce"): UserRow {
  return { Id: uid(n), Name: `Person ${n}`, Username: `p${n}@example.com`, Email: `p${n}@example.com`, IsActive: active, UserType: "Standard", Profile: profile === null ? null : { Name: profile, UserLicense: { Name: licence } }, UserRole: null, LastLoginDate: lastLoginDays === null ? null : at(lastLoginDays), CreatedDate: at(400) };
}

type Generated = { users: UserRow[]; privileged: PrivilegedPermissions; access: Pick<AccessData, "assignments" | "groupComponents">; sets: HoldersInput["sets"]; logins: Sourced<LoginSummary> };

/**
 * An org of three profiles, four permission sets and one group, with Modify and
 * View All Data given at random, some users inactive. The access read holds one
 * assignment the checks' read doesn't, so a count taken from the wrong read
 * disagrees with the checks.
 */
function generated(seed: number): Generated {
  const r = random(seed);
  const profiles = ["Admin Like", "Sales", "Service"];
  // Some with no profile, some who never logged in, some on an integration licence.
  const users = Array.from({ length: 5 + Math.floor(r() * 30) }, (_, i) => {
    const pick = r();
    return user(i + 1, pick < 0.1 ? null : profiles[Math.floor(r() * 3)]!, r() > 0.2, [1, 200, null][Math.floor(r() * 3)]!, r() < 0.15 ? "Salesforce Integration" : "Salesforce");
  });
  // The login log settles some whose last-login date is old: they logged in five days ago.
  const byUser: LoginSummary["byUser"] = {};
  for (const u of users) if (u.LastLoginDate === at(200) && r() < 0.5) byUser[u.Id] = { successes: 1, lastSuccess: at(5) };
  const logins = ok<LoginSummary>({ from: "2026-08-02", rowsRead: Object.keys(byUser).length, failedOrOther: 0, byUser, apiByUser: {} });
  const grant = () => ({ PermissionsModifyAllData: r() > 0.6, PermissionsViewAllData: r() > 0.4 });
  const profileSets = profiles.map((name, i) => ({ Id: psid(100 + i), Name: `P${i}`, Label: name, IsOwnedByProfile: true, Profile: { Name: name }, NamespacePrefix: null, ...grant() }));
  const plain = [1, 2, 3].map((i) => ({ Id: psid(i), Name: `S${i}`, Label: `Set ${i}`, IsOwnedByProfile: false, Profile: null, NamespacePrefix: null, ...grant() }));
  // The group's own set, as its assignment carries it (DECISIONS.md 487).
  const groupSet = { Id: psid(9), Name: "G", Label: "Group set", IsOwnedByProfile: false, Profile: null, NamespacePrefix: null, ...grant() };
  const all = [...profileSets, ...plain, groupSet];
  const direct = users.flatMap((u) => plain.filter(() => r() > 0.6).map((s) => ({ AssigneeId: u.Id, PermissionSetId: s.Id, PermissionSetGroupId: null as string | null })));
  const inGroup = users.filter(() => r() > 0.6).map((u) => ({ AssigneeId: u.Id, PermissionSetId: groupSet.Id as string | null, PermissionSetGroupId: GROUP as string | null }));
  const own = users.filter((u) => u.IsActive && u.Profile).map((u) => ({ AssigneeId: u.Id, PermissionSetId: profileSets.find((p) => p.Label === u.Profile!.Name)!.Id as string | null, PermissionSetGroupId: null as string | null }));
  const privilegedSets = all.filter((s) => s.PermissionsModifyAllData || s.PermissionsViewAllData);
  const privileged: PrivilegedPermissions = {
    sets: ok(privilegedSets.map((s) => ({ Id: s.Id, Label: s.Label, IsOwnedByProfile: s.IsOwnedByProfile, Profile: s.Profile, PermissionsModifyAllData: s.PermissionsModifyAllData, PermissionsViewAllData: s.PermissionsViewAllData }))),
    // The checks' read takes inactive users' assignments too; the access read doesn't.
    assignments: ok([...direct, ...inGroup].filter((a) => privilegedSets.some((s) => s.Id === a.PermissionSetId)).map((a) => ({ AssigneeId: a.AssigneeId, PermissionSetId: a.PermissionSetId! }))),
  };
  const activeIds = new Set(users.filter((u) => u.IsActive).map((u) => u.Id));
  const extra = users.find((u) => u.IsActive && !direct.some((a) => a.AssigneeId === u.Id && a.PermissionSetId === plain[0]!.Id));
  const accessRows = [...own, ...direct, ...inGroup].filter((a) => activeIds.has(a.AssigneeId));
  if (extra) accessRows.push({ AssigneeId: extra.Id, PermissionSetId: plain[0]!.Id, PermissionSetGroupId: null });
  const access = { assignments: ok(accessRows), groupComponents: ok([{ PermissionSetId: plain[2]!.Id, PermissionSetGroupId: GROUP }]) };
  // As the inventory's own read gives them: every set granting either.
  const sets = privilegedSets.map((s) => ({ ...s, Profile: s.Profile }));
  return { users, privileged, access, sets, logins };
}

const MAD_VAD = RISKY_PERMISSIONS.filter((p) => p.field === "PermissionsModifyAllData" || p.field === "PermissionsViewAllData");
const input = (g: Generated, over: Partial<HoldersInput> = {}): HoldersInput => ({ checked: MAD_VAD, sets: g.sets, access: g.access, users: ok(g.users), privileged: g.privileged, apiOnly: null, logins: null, kinds: [], dormantDays: 90, now: new Date(AS_OF), ...over });

describe("generated orgs", async () => {
  const base = loadExtracts((await runFromRecording({ modules: ALL_MODULES, inventory: true })).dir);

  for (let seed = 1; seed <= 40; seed++) {
    test(`seed ${seed}: each permission's count is its check's, and each person is counted once`, () => {
      const g = generated(seed);
      const changed = clone(base);
      changed.set("users", ok(g.users));
      changed.set("privileged-permissions", g.privileged);
      changed.set("access", { ...(changed.get("access") as object), ...g.access });
      changed.set("logins", g.logins);
      const out = analyseExtracts(changed, { modules: ALL_MODULES });
      const findings = out.findings;
      const counts = holderCounts(input(g, { logins: g.logins }));
      // Who the dormant-user checks found, from their own record lists.
      const lapsed = new Set(out.records.filter((l) => l.ruleId === "utl.dormant-licensed-users" || l.ruleId === "utl.never-logged-in").flatMap((l) => l.rows.map((row) => row.id)));
      assert.ok(!findings.some((f) => (f.ruleId === "utl.dormant-licensed-users" || f.ruleId === "utl.never-logged-in") && f.status === "not_measurable"));
      for (const [label, ruleId] of RULED) {
        const row = measured(counts.rows.find((r) => r.permission === label));
        const finding = findings.find((f) => f.ruleId === ruleId)!;
        assert.equal(row.holders, finding.facts.holders, `${label}, seed ${seed}`);
        // Done by hand: the distinct active people the profile or an assignment reaches.
        const field = label === "Modify All Data" ? "PermissionsModifyAllData" : "PermissionsViewAllData";
        const sets = g.privileged.sets.status === "ok" ? g.privileged.sets.data.filter((s) => s[field]) : [];
        const reached = new Set<string>();
        for (const u of g.users) if (u.IsActive && sets.some((s) => s.IsOwnedByProfile && s.Profile?.Name === u.Profile?.Name)) reached.add(u.Id);
        for (const a of g.privileged.assignments.status === "ok" ? g.privileged.assignments.data : []) if (sets.some((s) => !s.IsOwnedByProfile && s.Id === a.PermissionSetId) && g.users.find((u) => u.Id === a.AssigneeId)?.IsActive) reached.add(a.AssigneeId);
        assert.equal(row.holders, reached.size);
        assert.equal(row.notLoggedIn, [...reached].filter((id) => lapsed.has(id)).length, `not logged in, ${label}, seed ${seed}`);
        // The ways overlap, so each is at most the whole, and together at least it.
        for (const n of [row.byProfile, row.byPermissionSet!, row.byGroup!]) assert.ok(n <= row.holders);
        assert.ok(row.byProfile + row.byPermissionSet! + row.byGroup! >= row.holders);
      }
    });
  }
});

describe("counted once, active only", () => {
  const sets = [
    { Id: psid(100), Name: "P", Label: "Admin Like", IsOwnedByProfile: true, Profile: { Name: "Admin Like" }, NamespacePrefix: null, PermissionsAuthorApex: true },
    { Id: psid(1), Name: "S", Label: "Set", IsOwnedByProfile: false, Profile: null, NamespacePrefix: null, PermissionsAuthorApex: true },
    { Id: psid(9), Name: "G", Label: "Group set", IsOwnedByProfile: false, Profile: null, NamespacePrefix: null, PermissionsAuthorApex: true },
  ];
  const apex = RISKY_PERMISSIONS.filter((p) => p.field === "PermissionsAuthorApex");
  const counts = (users: UserRow[], assignments: AccessData["assignments"], over: Partial<HoldersInput> = {}) =>
    holderCounts({ checked: apex, sets, access: { assignments, groupComponents: ok([]) }, users: ok(users), privileged: null, apiOnly: null, logins: null, kinds: [], dormantDays: 90, now: new Date(AS_OF), ...over });

  test("one person on the profile, a set and a group is one holder, counted under each way", () => {
    const people = [user(1, "Admin Like")];
    const row = measured(counts(people, ok([
      { AssigneeId: uid(1), PermissionSetId: psid(1), PermissionSetGroupId: null },
      { AssigneeId: uid(1), PermissionSetId: psid(9), PermissionSetGroupId: GROUP },
    ])).rows[0]);
    assert.deepEqual([row.holders, row.byProfile, row.byPermissionSet, row.byGroup], [1, 1, 1, 1]);
  });

  test("an inactive user isn't a holder, on the profile or a set", () => {
    const people = [user(1, "Admin Like", false), user(2, "Sales", false), user(3, "Sales")];
    const row = measured(counts(people, ok([{ AssigneeId: uid(2), PermissionSetId: psid(1), PermissionSetGroupId: null }])).rows[0]);
    assert.equal(row.holders, 0);
  });

  test("integration accounts and people who haven't logged in are counted among the holders", () => {
    const people = [user(1, "Admin Like", true, 200), user(2, "Admin Like", true, null), { ...user(3, "Admin Like"), Profile: { Name: "Admin Like", UserLicense: { Name: "Salesforce Integration" } } }, user(4, "Admin Like")];
    const apiOnly = { sets: ok([]), assignments: ok([{ AssigneeId: uid(4), PermissionSetId: psid(5) }]) };
    const row = measured(counts(people, ok([]), { apiOnly }).rows[0]);
    assert.deepEqual([row.holders, row.integration, row.notLoggedIn], [4, 2, 2]);
    // Without the API-only read, no integration count, and a note says why.
    const without = counts(people, ok([]));
    assert.equal(measured(without.rows[0]).integration, null);
    assert.ok(without.notes.some((n) => /API only wasn't read/.test(n)));
  });

  test("not logging in is counted as the dormant checks count it: the login log settles it, and integration licences are left out", () => {
    const people = [user(1, "Admin Like", true, 200), user(2, "Admin Like", true, 200), user(3, "Admin Like", true, null, "Salesforce Integration"), user(4, "Admin Like", true, null)];
    // User 1's last-login date is old, but the log shows them logging in five days ago.
    const logins = ok<LoginSummary>({ from: "2026-08-02", rowsRead: 1, failedOrOther: 0, byUser: { [uid(1)]: { successes: 1, lastSuccess: at(5) } }, apiByUser: {} });
    const out = counts(people, ok([]), { logins });
    assert.equal(measured(out.rows[0]).notLoggedIn, 2);
    assert.ok(!out.notes.some((n) => /login log/.test(n)));
    // Without the log in the run, the last-login date alone, and the note says it wasn't read.
    const unread = counts(people, ok([]));
    assert.equal(measured(unread.rows[0]).notLoggedIn, 3);
    assert.ok(unread.notes.includes("The login log wasn't read in this run, so not logging in is told by each user's last-login date alone."));
    const refusedLog = counts(people, ok([]), { logins: refused("LoginHistory") as unknown as Sourced<LoginSummary> });
    assert.ok(refusedLog.notes.includes("The login log couldn't be read, so not logging in is told by each user's last-login date alone."));
  });

  test("with the assignments unread, who might hold a permission through a group is null, with its own reason", () => {
    const privileged: PrivilegedPermissions = { sets: ok([{ Id: psid(100), Label: "Admin Like", IsOwnedByProfile: true, Profile: { Name: "Admin Like" }, PermissionsModifyAllData: true, PermissionsViewAllData: true }]), assignments: ok([]) };
    const out = holderCounts({ checked: MAD_VAD, sets: [], access: { assignments: refused("PermissionSetAssignment"), groupComponents: ok([]) }, users: ok([user(1, "Admin Like")]), privileged, apiOnly: null, logins: null, kinds: [], dormantDays: 90, now: new Date(AS_OF) });
    const row = measured(out.rows[0]);
    assert.deepEqual([row.holders, row.byPermissionSet, row.byGroup, row.unconfirmedThroughGroups], [1, null, null, null]);
    assert.ok(out.notes.some((n) => /^Who is assigned a permission set group couldn't be read, so nobody is counted as possibly holding a permission through one: /.test(n)));
  });

  test("someone given the permission only through a group's set, which a mute may take away, is counted apart, never as a holder", () => {
    const people = [user(1, "Sales"), user(2, "Sales")];
    // User 1's group holds the set, but its assignment carries no set of its own.
    const out = holderCounts({ checked: apex, sets, access: { assignments: ok([{ AssigneeId: uid(1), PermissionSetId: null, PermissionSetGroupId: GROUP }]), groupComponents: ok([{ PermissionSetId: psid(1), PermissionSetGroupId: GROUP }]) }, users: ok(people), privileged: null, apiOnly: null, logins: null, kinds: [], dormantDays: 90, now: new Date(AS_OF) });
    const row = measured(out.rows[0]);
    assert.deepEqual([row.holders, row.unconfirmedThroughGroups], [0, 1]);
    assert.ok(out.notes.includes(MUTES_NOT_READ));
  });
});

// ---------------------------------------------------------------------------
// Reads that failed: never a zero.

// No module of its own, so nothing but the inventory reads, and no users step.
const BARE: DomainModule = { id: "configuration", cloud: "sales", label: "Nothing else", extractors: [], rules: [] };

async function simulated(options: SimulatorOptions = {}, run: Partial<RunOptions> = {}) {
  const result = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: ALL_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport: new Simulator(options), connectedUser: CONNECTED_USER, inventory: true, ...run });
  return { inventory: readInventory(result.dir), findings: findingsOf(result.dir) };
}
const refuse = (pattern: RegExp): SimulatorOptions => ({ failQuery: (q) => (pattern.test(q) ? { errorCode: "INSUFFICIENT_ACCESS", message: "No access" } : null) });

describe("a read that failed is said, never counted as nobody", () => {
  test("assignments refused: every row says why, Modify and View All Data in their checks' words", async () => {
    const { inventory, findings } = await simulated(refuse(/FROM PermissionSetAssignment\b/));
    const rows = section(inventory).rows;
    assert.ok(rows.length > 0 && rows.every((r) => "unavailable" in r), JSON.stringify(rows));
    assertSameAsFindings(inventory, findings);
    for (const r of rows.filter((r) => r.rule === null && r.permission !== "Approve Uninstalled Connected Apps")) assert.match((r as { unavailable: string }).unavailable, /^Permission set assignments couldn't be read: /);
  });

  test("users refused: every row says the users couldn't be read", async () => {
    const { inventory, findings } = await simulated(refuse(/FROM User WHERE UserType = 'Standard'/));
    const rows = section(inventory).rows;
    assert.ok(rows.every((r) => "unavailable" in r && (/^Users couldn't be read: /.test(r.unavailable) || r.unavailable === NOT_IN_ORG)), JSON.stringify(rows));
    assertSameAsFindings(inventory, findings);
  });

  test("PermissionSet not described: the whole section says why, as the profiles and sets do", async () => {
    const { inventory } = await simulated({ missingObjects: ["PermissionSet"] });
    assert.ok("unavailable" in inventory.riskyPermissions);
    assert.deepEqual(inventory.riskyPermissionHolders, { unavailable: inventory.riskyPermissions.unavailable });
  });

  test("a run without the users step: each row says who is active can't be told", async () => {
    const { inventory } = await simulated({}, { modules: [BARE] });
    const s = section(inventory);
    assert.ok(s.rows.every((r) => "unavailable" in r && (r.unavailable === USERS_NOT_READ || r.unavailable === NOT_IN_ORG) && r.rule === null));
  });
});

describe("older files and the worker", () => {
  test("a file written before the section parses, and the worker saves it without one", async () => {
    const { dir } = await runFromRecording({ modules: ALL_MODULES, inventory: true });
    const file = JSON.parse(readFileSync(join(dir, INVENTORY_FILE), "utf8")) as Record<string, unknown>;
    const older = { ...file };
    delete older.riskyPermissionHolders;
    assert.ok(Inventory.safeParse(older).success);
    const lines: string[] = [];
    const made = { madeAt: String(file.madeAt), scope: "complete" as const };
    const saved = settleInventory(older, (l) => lines.push(l), made);
    assert.ok(!("riskyPermissionHolders" in saved));
    assert.deepEqual(lines, []);
    // With it, the worker keeps it as it was.
    const later: string[] = [];
    assert.deepEqual(settleInventory(file, (l) => later.push(l), made).riskyPermissionHolders, file.riskyPermissionHolders);
    assert.deepEqual(later, []);
  });

  test("a row that doesn't fit costs only this section", async () => {
    const { dir } = await runFromRecording({ modules: ALL_MODULES, inventory: true });
    const file = JSON.parse(readFileSync(join(dir, INVENTORY_FILE), "utf8")) as Inventory;
    const broken = clone(file) as unknown as { riskyPermissionHolders: { rows: Array<Record<string, unknown>> } };
    broken.riskyPermissionHolders.rows[0]!.holders = -1;
    const saved = settleInventory(broken, () => {}, { madeAt: file.madeAt, scope: "complete" });
    assert.equal((saved.riskyPermissionHolders as { unavailable: string }).unavailable, "This part of the inventory didn't fit the shape we store, so it isn't shown.");
    assert.deepEqual(saved.riskyPermissions, file.riskyPermissions);
  });

  test("a run with no file says why in this section too", () => {
    assert.deepEqual(inventoryUnavailable({ madeAt: AS_OF, scope: "complete" }, "why").riskyPermissionHolders, { unavailable: "why" });
  });
});

// A read refused in the generated input, too: Modify All Data in its check's words.
test("generated: the checks' own read refused makes their rows unavailable with the checks' reason", () => {
  const g = generated(7);
  const out = holderCounts(input(g, { privileged: { ...g.privileged, assignments: refused("PermissionSetAssignment") } }));
  for (const row of out.rows) assert.equal((row as { unavailable: string }).unavailable, "Permission set assignments couldn't be read: PermissionSetAssignment couldn't be read, because the person who connected Salesforce doesn't have permission to see it.");
});
