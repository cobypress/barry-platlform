import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { DomainModule } from "../../domains/types.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { ApiBudget } from "../../salesforce/budget.ts";
import type { TransportRequest, TransportResponse } from "../../salesforce/transport.ts";
import { FillRateObject, Inventory } from "../../schemas/inventory.ts";
import { Run } from "../../schemas/run.ts";
import { tempDir } from "../../testing/harness.ts";
import { loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { AS_OF, CONNECTED_USER } from "../../testing/synthetic-org.ts";
import { FIELD_ENTRY_MISFIT, FIELD_MISFIT, inventoryOf, OBJECT_LINES_MAX, OBJECT_MISFIT, SECTION_MISFIT, settleInventory } from "../../worker/inventory.ts";
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
    assert.deepEqual(saved.fillRates.notes, ["One field didn't fit the shape we store, so it's listed among the fields not counted, not shown as empty."]);
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
    // Its reason, not its count, didn't fit, and the reason it's given says so.
    assert.deepEqual(row.skipped.at(-1), { field: "Made_Up_Text__c", reason: FIELD_ENTRY_MISFIT });
    assert.ok(saved.fillRates.notes.includes("2 fields couldn't be named, so they're left out of their objects."));
    assert.equal(logs.length, 1);
    assert.ok(!logs.some((l) => /PLANTED|Made_Up/.test(l)), logs.join("\n"));
  });

  test("a field moved for its name or an unknown key, not its count, is given a reason that says so", () => {
    const inv = file();
    const fields = inv.fillRates.rows[2]!.fields as unknown[];
    fields[0] = { field: `Made_Up_${"x".repeat(300)}`, filled: 40, share: 0.4 };
    fields[1] = { ...(fields[1] as object), PLANTED_key: 1 };
    fields[2] = { ...(fields[2] as object), filled: 1.5 };
    const saved = settleInventory(JSON.parse(JSON.stringify(inv)), () => {}, MADE);
    assert.ok("rows" in saved.fillRates);
    const row = saved.fillRates.rows[2]!;
    assert.ok("fields" in row);
    const moved = row.skipped.slice(1);
    assert.deepEqual(moved.map((m) => m.reason), [FIELD_ENTRY_MISFIT, FIELD_ENTRY_MISFIT, FIELD_MISFIT]);
    assert.ok(moved[0]!.field.length <= 255 && moved[0]!.field.endsWith("…"));
    assert.equal(row.fields.length, 27);
  });

  test("a field is never both counted and not counted, nor not counted twice: the first is kept, a count before a reason, and the rest counted in the notes", () => {
    const inv = file();
    const row4 = inv.fillRates.rows[4]!;
    (row4.fields as unknown[]).push({ field: "F0__c", filled: null, share: null }, { field: "F1__c", filled: 7, share: 0.07 });
    (row4.skipped as unknown[]).push({ field: "F2__c", reason: "A made up reason." }, { field: "Id", reason: "Again." }, { field: "New__c", reason: "" }, { field: "New__c", reason: "A made up reason." });
    const logs: string[] = [];
    const saved = settleInventory(JSON.parse(JSON.stringify(inv)), (l) => void logs.push(l), MADE);
    assert.ok("rows" in saved.fillRates);
    const row = saved.fillRates.rows[4]!;
    assert.ok("fields" in row);
    const names = [...row.fields.map((f) => f.field), ...row.skipped.map((f) => f.field)];
    assert.equal(new Set(names).size, names.length, "a field named twice");
    assert.deepEqual(row.fields, (inv.fillRates.rows[4]!.fields as unknown[]).slice(0, 30));
    // A reason read from the file comes before one the worker gave a misfit, so New__c keeps its own reason, and its misfit entry is the repeat.
    assert.deepEqual(row.skipped, [{ field: "Id", reason: "An id always has a value." }, { field: "New__c", reason: "A made up reason." }]);
    assert.ok(saved.fillRates.notes.includes("5 fields were listed twice in their objects, so only the first is kept."), saved.fillRates.notes.join("\n"));
    assert.ok(!logs.some((l) => /Made_Up|New__c|F\d__c/.test(l)), logs.join("\n"));
  });

  test("an empty note in the section's frame costs the section, as before 668", () => {
    const inv = file();
    (inv.fillRates.rows[0]!.fields[0] as { filled: unknown }).filled = null;
    (inv.fillRates.notes as string[]).push("");
    const saved = settleInventory(JSON.parse(JSON.stringify(inv)), () => {}, MADE);
    assert.deepEqual(saved.fillRates, { unavailable: SECTION_MISFIT });
  });

  test("many objects put right: the first few logged one line each, then one line with counts only", () => {
    const inv = file();
    for (const r of inv.fillRates.rows) (r.fields[26] as { filled: unknown }).filled = null;
    const logs: string[] = [];
    const saved = settleInventory(JSON.parse(JSON.stringify(inv)), (l) => void logs.push(l), MADE);
    assert.equal(logs.length, OBJECT_LINES_MAX + 1);
    assert.equal(logs.at(-1), `fillRates: ${92 - OBJECT_LINES_MAX} more objects didn't fit their shape; in all, 92 kept with 92 fields not measured, 0 unnamed fields left out and 0 repeats kept once, and 0 saved as unavailable.`);
    assert.ok("rows" in saved.fillRates && saved.fillRates.rows.every((r) => "fields" in r && r.fields.length === 29));
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

describe("the save's last check (DECISIONS.md 668)", () => {
  const clean = () => buildInventory(new Map(), "complete", new Date(AS_OF));
  const withFill = () => ({ ...clean(), fillRates: { rows: [{ object: "Made_Up__c", records: 1, fields: [{ field: "F__c", filled: null, share: null }], skipped: [] }], notes: [] } });

  test("an object a repair got wrong is saved as that object's unavailable, and the rest is kept", () => {
    const logs: string[] = [];
    const wrong = () => ({ rows: [{ object: "Made_Up__c", records: 1, fields: [{ field: "F__c", filled: Number.NaN, share: null }], skipped: [] }, { object: "Kept__c", records: 2, fields: [], skipped: [] }], notes: [] });
    const saved = settleInventory(withFill(), (l) => void logs.push(l), MADE, wrong);
    assert.ok(Inventory.safeParse(saved).success);
    assert.deepEqual(saved.fillRates, { rows: [{ object: "Made_Up__c", unavailable: OBJECT_MISFIT }, { object: "Kept__c", records: 2, fields: [], skipped: [] }], notes: [] });
    assert.ok(logs.includes("the last check before saving caught 1 fill-rate object that still didn't fit, so it's saved as unavailable."), logs.join("\n"));
  });

  test("a section a repair got wrong is saved as unavailable, and the other four are kept", () => {
    const logs: string[] = [];
    const saved = settleInventory(withFill(), (l) => void logs.push(l), MADE, () => ({ rows: [], notes: [7] }));
    assert.ok(Inventory.safeParse(saved).success);
    assert.deepEqual(saved.fillRates, { unavailable: SECTION_MISFIT });
    for (const k of ["packages", "connectedApps", "riskyPermissions", "limits"] as const) assert.deepEqual(saved[k], clean()[k]);
    assert.ok(logs.some((l) => l.startsWith("at the last check before saving, fillRates still didn't fit its shape")), logs.join("\n"));
  });
});

describe("a fill-rate step caught part way across a version change", () => {
  test("starts afresh: its partial rows from the earlier version are cleared, not reused", async () => {
    const BARE: DomainModule = { id: "configuration", cloud: "sales", label: "Nothing else", extractors: [], rules: [] };
    const opts = { provider: syntheticProvider, calibration: loadCalibration(), modules: [BARE], now: new Date(AS_OF), connectedUser: CONNECTED_USER, inventory: true };
    const outDir = tempDir();
    const first = await runHealthCheck({ ...opts, outDir, transport: new Simulator() });
    const runPath = join(first.dir, "run.json");
    const run = Run.parse(JSON.parse(readFileSync(runPath, "utf8")));
    const step = run.steps.find((s) => s.id === fillRatesExtractor.id)!;
    // As a run left mid-step by the earlier engine: version 2, not done, with a partial row holding a 0 never counted.
    step.version = 2;
    step.status = "pending";
    writeFileSync(runPath, JSON.stringify(run));
    const partial = join(first.dir, "checkpoints", `${fillRatesExtractor.id}.partial.jsonl`);
    writeFileSync(partial, `${JSON.stringify({ object: "Account", records: 1, fields: [{ field: "Made_Up__c", filled: 0, share: 0 }], skipped: [] })}\n`);
    const again = await runHealthCheck({ ...opts, outDir, transport: new Simulator(), resume: first.run.id });
    assert.equal(again.run.steps.find((s) => s.id === fillRatesExtractor.id)!.version, fillRatesExtractor.version);
    const inv = Inventory.parse(JSON.parse(readFileSync(join(again.dir, INVENTORY_FILE), "utf8")));
    const account = counted(inv, "Account");
    assert.ok(!account.fields.some((f) => f.field === "Made_Up__c"), "the earlier version's partial row was reused");
    assert.ok(account.records > 1);
    assert.ok(!existsSync(partial));
  });

  test("the same version's partial rows are still reused on a resume", async () => {
    const BARE: DomainModule = { id: "configuration", cloud: "sales", label: "Nothing else", extractors: [], rules: [] };
    const opts = { provider: syntheticProvider, calibration: loadCalibration(), modules: [BARE], now: new Date(AS_OF), connectedUser: CONNECTED_USER, inventory: true };
    const outDir = tempDir();
    const first = await runHealthCheck({ ...opts, outDir, transport: new Simulator() });
    const runPath = join(first.dir, "run.json");
    const run = Run.parse(JSON.parse(readFileSync(runPath, "utf8")));
    run.steps.find((s) => s.id === fillRatesExtractor.id)!.status = "pending";
    writeFileSync(runPath, JSON.stringify(run));
    writeFileSync(join(first.dir, "checkpoints", `${fillRatesExtractor.id}.partial.jsonl`), `${JSON.stringify({ object: "Account", records: 1, fields: [{ field: "Made_Up__c", filled: 0, share: 0 }], skipped: [] })}\n`);
    const again = await runHealthCheck({ ...opts, outDir, transport: new Simulator(), resume: first.run.id });
    const inv = Inventory.parse(JSON.parse(readFileSync(join(again.dir, INVENTORY_FILE), "utf8")));
    assert.ok(counted(inv, "Account").fields.some((f) => f.field === "Made_Up__c"));
  });
});
