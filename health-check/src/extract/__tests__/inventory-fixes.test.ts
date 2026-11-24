import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { ALL_MODULES } from "../../domains/index.ts";
import type { DomainModule } from "../../domains/types.ts";
import { runHealthCheck, type RunOptions } from "../../engine/run.ts";
import { ApiBudget } from "../../salesforce/budget.ts";
import type { TransportRequest, TransportResponse } from "../../salesforce/transport.ts";
import { Inventory } from "../../schemas/inventory.ts";
import { fingerprint } from "../../testing/fingerprint.ts";
import { tempDir } from "../../testing/harness.ts";
import { loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator, SYNTHETIC_RECORDS, type SimulatorOptions } from "../../testing/simulator.ts";
import { AS_OF, CONNECTED_USER, OAUTH_TOKENS, RECORD_COUNTS, userId } from "../../testing/synthetic-org.ts";
import {
  buildInventory,
  COUNT_TOO_SLOW,
  COUNTS_DISAGREE,
  FILL_RATE_BATCH,
  FILL_RATE_BIG_OBJECT,
  fillRatesExtractor,
  INVENTORY_FILE,
  NOT_SEEN_WHOLE,
  NOT_TOLD_WHO,
  ONLY_OWN_TOKENS,
  packagesExtractor,
} from "../inventory.ts";

// The fix round on the org's inventory (DECISIONS.md 609): it never fails a
// run, counts only what the connected user sees whole, divides each field by
// its own query's records, lists packages by id, says when only the user's own
// tokens are visible, and counts each subrequest against the budget.

const BARE: DomainModule = { id: "configuration", cloud: "sales", label: "Nothing else", extractors: [], rules: [] };
const readInventory = (dir: string): Inventory => Inventory.parse(JSON.parse(readFileSync(join(dir, INVENTORY_FILE), "utf8")));
const run = (transport: Simulator, extra: Partial<RunOptions> = {}) =>
  runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: [BARE], outDir: tempDir(), now: new Date(AS_OF), transport, connectedUser: CONNECTED_USER, inventory: true, ...extra });

type Row = { object: string; unavailable?: string; fields?: Array<{ field: string; filled: number; share: number | null }>; records?: number };
const fillRows = (inventory: Inventory): Row[] => {
  assert.ok("rows" in inventory.fillRates, JSON.stringify(inventory.fillRates));
  return inventory.fillRates.rows as Row[];
};
const fillCounts = (r: TransportRequest): string[] =>
  r.url.endsWith("/composite/batch") ? (JSON.parse(r.body ?? "{}") as { batchRequests: Array<{ url: string }> }).batchRequests.map((b) => new URLSearchParams(b.url.split("?")[1]).get("q") ?? "").filter((q) => /^SELECT COUNT\(Id\) n\b/.test(q)) : [];

/** The simulator, with the first composite call of fill-rate counts timing out as fetch's own timeout does. */
class SlowFirstBatch extends Simulator {
  slowed = 0;
  override async send(request: TransportRequest): Promise<TransportResponse> {
    if (this.slowed === 0 && fillCounts(request).length > 0) {
      this.slowed++;
      this.requests.push(request);
      throw new DOMException("The operation was aborted due to timeout", "TimeoutError");
    }
    return super.send(request);
  }
}

describe("[1] the inventory never fails a run", () => {
  test("a timeout on the first batch leaves the run's status and every finding as they were, marks those objects unavailable, and a retry doesn't send that batch again", async () => {
    const plain = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: ALL_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport: new Simulator(), connectedUser: CONNECTED_USER });
    const slow = new SlowFirstBatch();
    const outDir = tempDir();
    const first = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: ALL_MODULES, outDir, now: new Date(AS_OF), transport: slow, connectedUser: CONNECTED_USER, inventory: true });
    assert.equal(first.run.status, plain.run.status);
    const same = (dir: string) => {
      const { "raw-notes.md": _notes, ...rest } = fingerprint(dir);
      return rest;
    };
    assert.deepEqual(same(first.dir), same(plain.dir));
    const rows = fillRows(readInventory(first.dir));
    // Account's four counts and Contact's one made the first call.
    assert.deepEqual(rows.filter((r) => r.unavailable === COUNT_TOO_SLOW).map((r) => r.object), ["Account", "Contact"]);
    assert.ok(rows.find((r) => r.object === "Lead")?.fields);
    assert.equal(first.run.steps.find((s) => s.id === fillRatesExtractor.id)!.status, "done");

    const timedOut = slow.requests.find((r) => fillCounts(r).length > 0)!;
    const again = new Simulator();
    await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: ALL_MODULES, outDir, now: new Date(AS_OF), transport: again, connectedUser: CONNECTED_USER, inventory: true, resume: first.run.id });
    assert.ok(!again.requests.some((r) => r.body === timedOut.body), "the batch that timed out was sent again");
  });

  test("a step stopped part way keeps the objects whose counts failed, so its next try moves past them", async () => {
    const kept = new Map<string, unknown[]>();
    const sent: string[][] = [];
    const budget = new ApiBudget({ stopAtShare: 1 });
    budget.observe(0, 100_000);
    let calls = 0;
    const client = {
      budget,
      recordCounts: async () => [{ name: "Account", count: 10 }],
      describe: async () => ({ fields: [{ name: "Name", type: "string", calculated: false, aggregatable: true }] }),
      queryBatch: async (qs: string[]) => {
        sent.push(qs);
        if (calls++ === 0) throw new DOMException("The operation was aborted due to timeout", "TimeoutError");
        throw new Error("the network went away");
      },
    };
    const ctx = {
      client,
      get: () => {
        throw new Error("not run");
      },
      connectedUser: CONNECTED_USER,
      partial: { read: (k: string) => kept.get(k) ?? [], append: (k: string, r: unknown[]) => void kept.set(k, [...(kept.get(k) ?? []), ...r]), clear: () => {} },
    };
    // Access is read through the client's query, which this stub doesn't have, so it's given as record-type use's.
    const withAccess = { ...ctx, get: (e: { id: string }) => (e.id === "record-type-use" ? { access: { status: "ok", viewAllData: true, viewAllRecords: [] } } : ctx.get()) };
    const once = await fillRatesExtractor.run(withAccess as never);
    assert.ok(once.status === "ok");
    assert.equal(sent.length, 1);
    await fillRatesExtractor.run(withAccess as never);
    assert.equal(sent.length, 1, "the second try sent the batch again");
    assert.ok((kept.get("inventory-fill-rates") ?? []).length >= 4);
  });

  test("any other error from an inventory step costs only its section: the step is failed with why, and the run goes on", async () => {
    const baseline = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: [BARE], outDir: tempDir(), now: new Date(AS_OF), transport: new Simulator(), connectedUser: CONNECTED_USER });
    const broken = await run(new Simulator({ failQuery: (q) => (/FROM InstalledSubscriberPackage\b/.test(q) && /SubscriberPackageId/.test(q) ? { errorCode: "SERVER_UNAVAILABLE", message: "Something broke" } : null) }));
    assert.equal(broken.run.status, baseline.run.status);
    const step = broken.run.steps.find((s) => s.id === packagesExtractor.id)!;
    assert.equal(step.status, "failed");
    const inventory = readInventory(broken.dir);
    assert.deepEqual(inventory.packages, { unavailable: "This part of the inventory couldn't be read: SERVER_UNAVAILABLE: Something broke" });
    assert.ok("rows" in inventory.connectedApps && "rows" in inventory.fillRates);
  });

  test("an object over the threshold is counted in a call of its own, and no call holds more than five counts", async () => {
    const big = new Simulator({ records: { ...SYNTHETIC_RECORDS, recordCounts: [...RECORD_COUNTS.filter((c) => c.name !== "Invoice__c"), { name: "Invoice__c", count: FILL_RATE_BIG_OBJECT + 1 }] } });
    await run(big, { transport: big });
    const batches = big.requests.map(fillCounts).filter((b) => b.length > 0);
    assert.ok(batches.every((b) => b.length <= FILL_RATE_BATCH));
    const invoice = batches.find((b) => b.some((q) => / FROM Invoice__c$/.test(q)))!;
    assert.ok(invoice.every((q) => / FROM Invoice__c$/.test(q)), "a big object shared its call");
  });
});

describe("[2] only what the connected user sees whole is counted", () => {
  test("a private custom object the user can read but not view all, and Task, are unavailable; what they view all is counted", async () => {
    const sim = new Simulator({
      access: { viewAllData: false, viewAllRecords: ["Account", "Contact", "Lead", "Opportunity", "Project__c"] },
      records: { ...SYNTHETIC_RECORDS, recordCounts: [...RECORD_COUNTS, { name: "Task", count: 30_000 }] },
    });
    const { dir } = await run(sim, { transport: sim });
    const rows = fillRows(readInventory(dir));
    assert.equal(rows.find((r) => r.object === "Invoice__c")!.unavailable, NOT_SEEN_WHOLE);
    assert.equal(rows.find((r) => r.object === "Task")!.unavailable, NOT_SEEN_WHOLE);
    assert.ok(rows.find((r) => r.object === "Project__c")!.fields);
    assert.ok(!sim.requests.some((r) => r.url.includes("/sobjects/Invoice__c/describe") || r.url.includes("/sobjects/Task/describe")));
    assert.ok(!sim.requests.flatMap(fillCounts).some((q) => / FROM (Invoice__c|Task)$/.test(q)));
  });

  test("Task stays unavailable even to a user who views all data", async () => {
    const sim = new Simulator({ records: { ...SYNTHETIC_RECORDS, recordCounts: [...RECORD_COUNTS, { name: "Task", count: 30_000 }] } });
    const { dir } = await run(sim, { transport: sim });
    assert.equal(fillRows(readInventory(dir)).find((r) => r.object === "Task")!.unavailable, NOT_SEEN_WHOLE);
  });

  test("when the record count and the object's own count differ by more than a tenth, neither is printed", async () => {
    // The resource says 20,000 invoices; the user's COUNT(Id) finds 12,000.
    const sim = new Simulator({ records: { ...SYNTHETIC_RECORDS, recordCounts: [...RECORD_COUNTS.filter((c) => c.name !== "Invoice__c"), { name: "Invoice__c", count: 20_000 }] } });
    const { dir } = await run(sim, { transport: sim });
    const invoice = fillRows(readInventory(dir)).find((r) => r.object === "Invoice__c")!;
    assert.deepEqual(invoice, { object: "Invoice__c", unavailable: COUNTS_DISAGREE });
  });

  test("when the user's access can't be read, nothing is counted, and each object says why", async () => {
    const sim = new Simulator({ failQuery: (q) => (/FROM ObjectPermissions\b/.test(q) ? { errorCode: "INSUFFICIENT_ACCESS", message: "No access" } : null) });
    const { dir } = await run(sim, { transport: sim });
    const rows = fillRows(readInventory(dir));
    assert.ok(rows.length > 0 && rows.every((r) => /^We couldn't tell whether the connected user sees every record/.test(r.unavailable ?? "")));
    assert.deepEqual(sim.requests.flatMap(fillCounts), []);
  });
});

/** A stub context for one step, with the access record-type use would give. */
function stubContext(client: Record<string, unknown>) {
  const kept = new Map<string, unknown[]>();
  const budget = new ApiBudget({ stopAtShare: 1 });
  budget.observe(0, 100_000);
  return {
    client: { budget, ...client },
    connectedUser: CONNECTED_USER,
    get: (e: { id: string }) => {
      if (e.id === "record-type-use") return { access: { status: "ok", viewAllData: true, viewAllRecords: [] } };
      throw new Error("not run");
    },
    partial: { read: (k: string) => kept.get(k) ?? [], append: (k: string, r: unknown[]) => void kept.set(k, [...(kept.get(k) ?? []), ...r]), clear: () => {} },
  };
}

describe("[3] each field's share is of its own query's records", () => {
  test("with 60 fields, a field in the second query, which saw 5 more records, is filled in every one of them, and the file parses", async () => {
    const fields = Array.from({ length: 60 }, (_, i) => ({ name: i === 55 ? "Type__c" : `F${i}__c`, type: "string", calculated: false, aggregatable: true }));
    const ctx = stubContext({
      recordCounts: async () => [{ name: "Account", count: 1_000 }],
      describe: async () => ({ fields }),
      queryBatch: async (qs: string[]) =>
        qs.map((q, qi) => {
          const n = 1_000 + qi * 5;
          const record: Record<string, number> = { n };
          for (const [, field, alias] of q.matchAll(/COUNT\(([A-Za-z0-9_]+)\) (f\d+)/g)) record[alias!] = field === "Type__c" ? n : 10;
          return { ok: true, result: { records: [record], totalSize: 1, truncated: false } };
        }),
    });
    const out = await fillRatesExtractor.run(ctx as never);
    const inventory = buildInventory(new Map([[fillRatesExtractor.id, out]]), "complete", new Date(AS_OF));
    assert.ok(Inventory.safeParse(inventory).success);
    const account = fillRows(inventory).find((r) => r.object === "Account")!;
    assert.deepEqual(account.fields!.find((f) => f.field === "Type__c"), { field: "Type__c", filled: 1_005, share: 1 });
    assert.equal(account.fields!.find((f) => f.field === "F0__c")!.share, 0.01);
  });
});

describe("[4] packages are listed by their id", () => {
  test("three packages without a namespace are each listed, and a licence still matches by namespace", async () => {
    const pkg = (id: number, name: string, namespace: string | null) => ({
      SubscriberPackageId: `033${String(id).padStart(12, "0")}AAA`,
      SubscriberPackage: { Name: name, NamespacePrefix: namespace },
      SubscriberPackageVersion: { Name: "v1", MajorVersion: 1, MinorVersion: 0, PatchVersion: 0, BuildNumber: id, IsDeprecated: false, IsManaged: namespace !== null },
    });
    const installed = [pkg(1, "Billing Core", null), pkg(2, "Logging", null), pkg(3, "Trigger Framework", null), pkg(4, "Acme CPQ", "acmecpq")];
    const ctx = stubContext({
      query: async (soql: string) =>
        /InstalledSubscriberPackage/.test(soql)
          ? { records: installed, totalSize: 4, truncated: false }
          : { records: [{ NamespacePrefix: "acmecpq", AllowedLicenses: 20, UsedLicenses: 12, Status: "Active", ExpirationDate: null }], totalSize: 1, truncated: false },
    });
    const out = await packagesExtractor.run(ctx as never);
    const inventory = buildInventory(new Map([[packagesExtractor.id, out]]), "complete", new Date(AS_OF));
    assert.ok("rows" in inventory.packages);
    assert.deepEqual(inventory.packages.rows.map((r) => r.name), ["Billing Core", "Logging", "Trigger Framework", "Acme CPQ"]);
    assert.equal(new Set(inventory.packages.rows.map((r) => r.packageId)).size, 4);
    assert.equal(inventory.packages.rows.find((r) => r.namespace === "acmecpq")!.licence?.used, 12);
    assert.ok(inventory.packages.rows.filter((r) => r.namespace === null).every((r) => r.licence === null));
  });
});

/** The simulator, answering the token read as a user without Manage Users sees it: their own tokens only. */
class OwnTokensOnly extends Simulator {
  readonly own: string;
  constructor(options: SimulatorOptions, own: string) {
    super(options);
    this.own = own;
  }
  override async send(request: TransportRequest): Promise<TransportResponse> {
    if (/FROM OauthToken\b/.test(new URL(request.url).searchParams.get("q") ?? "")) {
      this.requests.push(request);
      const records = OAUTH_TOKENS.map((t) => ({ ...t, UserId: this.own }));
      return { status: 200, header: () => null, text: JSON.stringify({ totalSize: records.length, done: true, records }) };
    }
    return super.send(request);
  }
}

describe("[5] connected apps' use, when the user sees only its own tokens", () => {
  test("is unavailable with why, and the installed apps are still listed", async () => {
    const sim = new OwnTokensOnly({}, CONNECTED_USER.id);
    const { dir } = await run(sim, { transport: sim });
    const apps = readInventory(dir).connectedApps;
    assert.ok("rows" in apps);
    assert.deepEqual(apps.rows.map((a) => [a.name, a.installed, a.users, a.uses, a.lastUsed]), [["Data Loader", true, null, null, null], ["Finance Sync", true, null, null, null]]);
    assert.deepEqual(apps.notes, [`No app's use is shown: ${ONLY_OWN_TOKENS}`]);
  });

  test("tokens of other people are counted as before", async () => {
    const sim = new OwnTokensOnly({}, userId(1));
    const { dir } = await run(sim, { transport: sim });
    const apps = readInventory(dir).connectedApps;
    assert.ok("rows" in apps && apps.rows.some((a) => a.users === 1));
  });

  test("without the connected user's id, the use is unavailable too", async () => {
    const sim = new Simulator();
    const { dir } = await run(sim, { transport: sim, connectedUser: { username: CONNECTED_USER.username } });
    const apps = readInventory(dir).connectedApps;
    assert.ok("notes" in apps);
    assert.deepEqual(apps.notes, [`No app's use is shown: ${NOT_TOLD_WHO}`]);
  });
});

describe("[6] each count is one API call in the budget", () => {
  test("the fill-rate step's calls are its describes and every count inside its composite calls", async () => {
    const sim = new Simulator();
    const result = await run(sim, { transport: sim, modules: ALL_MODULES });
    const describes = sim.requests.filter((r) => /\/sobjects\/[A-Za-z0-9_]+\/describe$/.test(new URL(r.url).pathname)).length;
    const step = result.run.steps.find((s) => s.id === fillRatesExtractor.id)!;
    const counts = sim.requests.flatMap(fillCounts).length;
    assert.ok(counts > sim.requests.filter((r) => fillCounts(r).length > 0).length, "more counts than calls, so the two ways differ");
    // The describes the fill-rate step made are the last seven: the custom objects' and the core four's again.
    assert.equal(step.calls, 7 + counts);
    assert.ok(describes >= 7);
  });
});
