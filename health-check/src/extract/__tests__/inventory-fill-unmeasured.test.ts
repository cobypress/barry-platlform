import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { DomainModule } from "../../domains/types.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { ApiBudget } from "../../salesforce/budget.ts";
import type { TransportRequest, TransportResponse } from "../../salesforce/transport.ts";
import { FillRateObject, Inventory } from "../../schemas/inventory.ts";
import { tempDir } from "../../testing/harness.ts";
import { loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { AS_OF, CONNECTED_USER } from "../../testing/synthetic-org.ts";
import { FIELD_MISFIT, inventoryOf, settleInventory } from "../../worker/inventory.ts";
import { buildInventory, COUNT_NOT_READ, countOf, fillRatesExtractor, INVENTORY_FILE, RECORDS_NOT_READ } from "../inventory.ts";

// DECISIONS.md 668: a real org's run saved one object's fill rates as
// unavailable because one of its fields' `filled` wasn't a whole number
// ("fields.26.filled invalid_type"). A count Salesforce doesn't give as a
// whole number is now that field's "not measured", never 0 and never the
// object's or the section's loss; and the worker, reading a file that still
// holds one, costs only that field. Every name here is made up.

const MADE = { runId: "run_made_up", madeAt: "2026-11-22T12:00:00.000Z", scope: "complete" as const };

/** A stub context for the fill-rate step alone, with every record seen. */
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

const FIELDS = Array.from({ length: 30 }, (_, i) => ({ name: `F${i}__c`, type: "string", calculated: false, aggregatable: true }));
/** Each object's one count query answered with 100 records and 40 filled in every field, but `odd` for field 26. */
const answering = (odd: unknown, objects = ["Account"]) =>
  stubContext({
    recordCounts: async () => objects.map((name) => ({ name, count: 100 })),
    describe: async () => ({ fields: FIELDS }),
    queryBatch: async (qs: string[]) =>
      qs.map((q) => {
        const record: Record<string, unknown> = { n: 100 };
        for (const [, , alias] of q.matchAll(/COUNT\(([A-Za-z0-9_]+)\) (f\d+)/g)) record[alias!] = alias === "f26" && / FROM Account$/.test(q) ? odd : 40;
        if (odd === "omit" && / FROM Account$/.test(q)) delete record.f26;
        return { ok: true, result: { records: [record], totalSize: 1, truncated: false } };
      }),
  });

type Counted = Extract<FillRateObject, { fields: unknown }>;
const counted = (inv: Inventory, object: string): Counted => {
  assert.ok("rows" in inv.fillRates, JSON.stringify(inv.fillRates));
  const row = inv.fillRates.rows.find((r) => r.object === object);
  assert.ok(row && "fields" in row, JSON.stringify(row));
  return row;
};

describe("the cause, reproduced: the count step's old arithmetic on a count that isn't a number", () => {
  test("gives NaN, which the file writes as null, which the worker refuses at exactly fields.26.filled", () => {
    // The pre-668 line: Math.min(Number(row.f26 ?? 0), n). Number() of an object or non-numeric text is NaN.
    for (const odd of [{ value: 3 }, "three", [1, 2]]) {
      const filled = Math.min(Number((odd as unknown) ?? 0), 100);
      assert.ok(Number.isNaN(filled));
      const fields = FIELDS.map((f, i) => ({ field: f.name, filled: i === 26 ? filled : 40, share: i === 26 ? Math.round((filled / 100) * 10_000) / 10_000 : 0.4 }));
      const onDisk = JSON.parse(JSON.stringify({ object: "Account", records: 100, fields, skipped: [] }));
      assert.equal(onDisk.fields[26].filled, null);
      const parsed = FillRateObject.safeParse(onDisk);
      assert.ok(!parsed.success);
    }
    // A fraction fails the same way: Zod 4 names a non-integer invalid_type too.
    assert.ok(!FillRateObject.safeParse({ object: "Account", records: 100, fields: [{ field: "F__c", filled: 1.5, share: 0.015 }], skipped: [] }).success);
  });
});

describe("a count that isn't a whole number is that field's 'not measured', never a figure", () => {
  test("countOf reads only whole, non-negative, safe counts, and digits written as text", () => {
    assert.equal(countOf(7), 7);
    assert.equal(countOf(0), 0);
    assert.equal(countOf("12"), 12);
    for (const v of [undefined, null, NaN, Infinity, -1, 1.5, "", "three", "1e3", {}, [3], true, 2 ** 60]) assert.equal(countOf(v), null, String(v));
  });

  for (const [label, odd] of [
    ["an object", { value: 3 }],
    ["text that isn't a number", "PLANTED_TEXT"],
    ["null", null],
    ["a fraction", 12.5],
    ["a negative", -4],
    ["no answer at all for the field", "omit"],
  ] as const) {
    test(`${label}: listed among the fields not counted with why, the object's other 29 fields kept, and the file parses`, async () => {
      const out = await fillRatesExtractor.run(answering(odd) as never);
      const logs: string[] = [];
      const inv = buildInventory(new Map([[fillRatesExtractor.id, out]]), "complete", new Date(AS_OF), new Map(), (l) => void logs.push(l));
      // As the engine writes it and the worker reads it back.
      const round = Inventory.parse(JSON.parse(JSON.stringify(inv)));
      const account = counted(round, "Account");
      assert.equal(account.records, 100);
      assert.equal(account.fields.length, 29);
      assert.ok(account.fields.every((f) => f.filled === 40 && f.share === 0.4));
      assert.ok(!account.fields.some((f) => f.field === "F26__c"), "no figure, not 0, for the unmeasured field");
      assert.deepEqual(account.skipped, [{ field: "F26__c", reason: COUNT_NOT_READ }]);
      assert.ok("notes" in round.fillRates && round.fillRates.notes.includes("One field's count didn't come back from Salesforce as a whole number, so it's listed among the fields not counted, not shown as empty."));
      assert.deepEqual(logs, ["  The inventory's fill rates: 1 field in 1 object came back without a whole-number count, so it's listed as not measured."]);
      assert.ok(!logs.some((l) => /PLANTED|F26/.test(l)));
    });
  }

  test("a count of records that isn't a whole number makes that object unavailable, with why, and not its neighbours", async () => {
    const ctx = stubContext({
      recordCounts: async () => [{ name: "Account", count: 100 }, { name: "Contact", count: 100 }],
      describe: async () => ({ fields: FIELDS.slice(0, 2) }),
      queryBatch: async (qs: string[]) => qs.map((q) => ({ ok: true, result: { records: [{ n: / FROM Account$/.test(q) ? "PLANTED" : 100, f0: 10, f1: 20 }], totalSize: 1, truncated: false } })),
    });
    const inv = buildInventory(new Map([[fillRatesExtractor.id, await fillRatesExtractor.run(ctx as never)]]), "complete", new Date(AS_OF));
    assert.ok("rows" in inv.fillRates);
    assert.deepEqual(inv.fillRates.rows.find((r) => r.object === "Account"), { object: "Account", unavailable: RECORDS_NOT_READ });
    assert.deepEqual(counted(inv, "Contact").fields.map((f) => f.filled), [10, 20]);
  });
});

describe("the worker, reading a file that still holds a bad field, costs only that field", () => {
  /** 92 counted objects, as the real org had at least, and object 91's field 26 holding what NaN is written as. */
  function file() {
    const clean = buildInventory(new Map(), "complete", new Date(AS_OF));
    const rows = Array.from({ length: 92 }, (_, o) => ({
      object: `Made_Up_${o}__c`,
      records: 100 + o,
      fields: FIELDS.map((f) => ({ field: f.name, filled: 40, share: Math.round((40 / (100 + o)) * 10_000) / 10_000 })),
      skipped: [{ field: "Id", reason: "An id always has a value." }],
    }));
    return { ...clean, fillRates: { rows, notes: [] } };
  }

  test("before: the same file a pre-668 engine wrote; after: one field not measured, and every other object's figures exactly as they were", () => {
    const inv = file();
    const intact = structuredClone(inv.fillRates.rows);
    (inv.fillRates.rows[91]!.fields[26] as { filled: unknown }).filled = null;
    const dir = mkdtempSync(join(tmpdir(), "hc-fill-unmeasured-"));
    writeFileSync(join(dir, INVENTORY_FILE), JSON.stringify(inv));
    const logs: string[] = [];
    const saved = inventoryOf(dir, (l) => void logs.push(l), MADE);
    assert.ok(Inventory.safeParse(saved).success);
    assert.ok("rows" in saved.fillRates);
    assert.equal(saved.fillRates.rows.length, 92);
    assert.deepEqual(saved.fillRates.rows.slice(0, 91), intact.slice(0, 91));
    const object91 = saved.fillRates.rows[91]!;
    assert.ok("fields" in object91);
    assert.equal(object91.records, 191);
    assert.deepEqual(object91.fields, intact[91]!.fields.filter((_, i) => i !== 26));
    assert.deepEqual(object91.skipped, [...intact[91]!.skipped, { field: "F26__c", reason: FIELD_MISFIT }]);
    assert.deepEqual(saved.fillRates.notes, ["One field's count didn't fit the shape we store, so it's listed among the fields not counted, not shown as empty."]);
    for (const k of ["packages", "connectedApps", "riskyPermissions", "limits"] as const) assert.deepEqual(saved[k], inv[k]);
    assert.deepEqual(logs, [`Run ${MADE.runId}'s inventory: fillRates object 91 didn't fit its shape (fields.26.filled invalid_type), so one field is listed as not measured, and the object's other counts are kept.`]);
  });

  test("a bad entry among the fields not counted is kept by name with the misfit reason; one with no name is left out and counted", () => {
    const inv = file();
    (inv.fillRates.rows[3]!.skipped as unknown[]).push({ field: "Made_Up_Text__c", reason: "" }, { reason: "PLANTED" });
    (inv.fillRates.rows[3]!.fields as unknown[]).push({ filled: 1, share: 0.01 });
    const logs: string[] = [];
    const saved = settleInventory(JSON.parse(JSON.stringify(inv)), (l) => void logs.push(l), MADE);
    assert.ok("rows" in saved.fillRates);
    const row = saved.fillRates.rows[3]!;
    assert.ok("fields" in row);
    assert.equal(row.fields.length, 30);
    assert.deepEqual(row.skipped.at(-1), { field: "Made_Up_Text__c", reason: FIELD_MISFIT });
    assert.ok(saved.fillRates.notes.includes("2 fields couldn't be named, so they're left out of their objects."));
    assert.equal(logs.length, 1);
    assert.ok(!logs.some((l) => /PLANTED|Made_Up/.test(l)), logs.join("\n"));
  });

  test("an object whose own frame doesn't fit (its records not a count) is still that object's unavailable, as before", () => {
    const inv = file();
    (inv.fillRates.rows[5] as { records: unknown }).records = null;
    const saved = settleInventory(JSON.parse(JSON.stringify(inv)), () => {}, MADE);
    assert.ok("rows" in saved.fillRates);
    assert.ok("unavailable" in saved.fillRates.rows[5]!);
    assert.equal(saved.fillRates.rows.filter((r) => "fields" in r).length, 91);
  });
});

/** The simulator, answering Account's first fill-rate field with what isn't a count. */
class OddCount extends Simulator {
  done = false;
  override async send(request: TransportRequest): Promise<TransportResponse> {
    const res = await super.send(request);
    if (!request.url.endsWith("/composite/batch")) return res;
    const asked = (JSON.parse(request.body ?? "{}") as { batchRequests: Array<{ url: string }> }).batchRequests.map((b) => new URLSearchParams(b.url.split("?")[1]).get("q") ?? "");
    const body = JSON.parse(res.text) as { results: Array<{ statusCode: number; result: { records?: Array<Record<string, unknown>> } }> };
    asked.forEach((q, i) => {
      // Only the first of Account's count queries, so only its first field.
      if (!this.done && /^SELECT COUNT\(Id\) n, COUNT\(\w+\) f0\b.* FROM Account$/.test(q) && body.results[i]?.statusCode === 200) {
        body.results[i]!.result.records![0]!.f0 = { PLANTED: true };
        this.done = true;
      }
    });
    return { ...res, text: JSON.stringify(body) };
  }
}

describe("a whole run on the synthetic org, with one field's count not a number", () => {
  test("saves every other object's fill rates exactly as a clean run does, and only that field as not measured", async () => {
    const BARE: DomainModule = { id: "configuration", cloud: "sales", label: "Nothing else", extractors: [], rules: [] };
    const go = async (transport: Simulator) => {
      const r = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: [BARE], outDir: tempDir(), now: new Date(AS_OF), transport, connectedUser: CONNECTED_USER, inventory: true });
      return Inventory.parse(JSON.parse(readFileSync(join(r.dir, INVENTORY_FILE), "utf8")));
    };
    const plain = await go(new Simulator());
    const odd = await go(new OddCount());
    assert.ok("rows" in plain.fillRates && "rows" in odd.fillRates);
    assert.deepEqual(odd.fillRates.rows.filter((r) => r.object !== "Account"), plain.fillRates.rows.filter((r) => r.object !== "Account"));
    const before = counted(plain, "Account");
    const after = counted(odd, "Account");
    assert.equal(after.records, before.records);
    assert.deepEqual(after.fields, before.fields.slice(1));
    assert.deepEqual(after.skipped, [...before.skipped, { field: before.fields[0]!.field, reason: COUNT_NOT_READ }]);
    const logs: string[] = [];
    const dir = mkdtempSync(join(tmpdir(), "hc-fill-unmeasured-run-"));
    writeFileSync(join(dir, INVENTORY_FILE), JSON.stringify(odd));
    assert.deepEqual(inventoryOf(dir, (l) => void logs.push(l), MADE), odd);
    assert.deepEqual(logs, []);
  });
});
