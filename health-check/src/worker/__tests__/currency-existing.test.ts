import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join, relative } from "node:path";
import { ORG_SETTINGS_CALIBRATOR } from "../../connect/auto-calibration.ts";
import { ORG_CURRENCY_SOQL, ORG_LOCALE_SOQL } from "../../connect/org-currency.ts";
import { Calibration } from "../../schemas/index.ts";
import { automaticSyntheticCalibration } from "../../testing/automatic.ts";
import { ORG_SETTINGS_VARIANTS, Simulator, type OrgSettingsVariant } from "../../testing/simulator.ts";
import { ORG_ID } from "../../testing/synthetic-org.ts";
import { OAuthConnectionProvider } from "../../salesforce/oauth-provider.ts";
import { MemoryWorkerStore } from "../memory-store.ts";
import { CURRENCY_READ_FILE, nameCurrencyBeforeRun, unnamedBySalesforce } from "../name-currency.ts";
import { calibrationFromRow, calibrationRow, NAME_CALIBRATION_CURRENCY_SQL, PostgresWorkerStore, type SqlPool } from "../postgres-store.ts";
import { approvedFromConsole } from "../run-job.ts";
import type { Run } from "../../schemas/index.ts";
import type { Transport, TransportRequest, TransportResponse } from "../../salesforce/transport.ts";
import { createJobHandler } from "../worker.ts";
import { baseDeps, consoleRun, oauthClient, RUN_ID, runInput, syntheticConnection } from "./helpers.ts";

// An automatic calibration saved with no currency from Salesforce (before 740,
// or after a settings read that couldn't say) is named from the org's settings
// when its run starts afresh, through the same read and acceptance rule as at
// connect; anything else leaves it exactly as it was (DECISIONS.md 745).

const ORG15 = ORG_ID.slice(0, 15);
const ON = { on: true, orgs: new Set([ORG15]) };
const AGAIN = { final: false, jobId: `run-${RUN_ID}-again-1764000000000` };
const SETTINGS = /\/connect\/organization$/;
const ACCEPTED: OrgSettingsVariant[] = ["top", "features", "both"];
const REJECTED = ORG_SETTINGS_VARIANTS.filter((v) => !ACCEPTED.includes(v));

/** The synthetic org's automatic calibration as connect saved it before 740: one currency, not named, the locale's suggestion beside it. */
async function unnamed(): Promise<Calibration> {
  const { currency: _c, currencyFrom: _f, currencySuggested: _s, ...rest } = await automaticSyntheticCalibration();
  return Calibration.parse({ ...rest, currency: "XXX", currencyFrom: "org", currencySuggested: "GBP" });
}
/** The same calibration as connect saves it since 740 when the settings name GBP. */
async function namedAtConnect(code = "GBP"): Promise<Calibration> {
  const { currency: _c, currencyFrom: _f, currencySuggested: _s, ...rest } = await automaticSyntheticCalibration();
  return Calibration.parse({ ...rest, currency: code, currencyFrom: "connect_api" });
}
async function answered(code: string): Promise<Calibration> {
  const { currency: _c, currencyFrom: _f, currencySuggested: _s, ...rest } = await automaticSyntheticCalibration();
  return Calibration.parse({ ...rest, currency: code, currencyFrom: "customer" });
}

type Start = { calibration: Calibration; settings?: OrgSettingsVariant; orgCode?: string; used?: number; reviewMode?: Run["reviewMode"]; marked?: boolean; prepare?: (deps: ReturnType<typeof baseDeps>) => void };

/** The org's own currency query answering with a code, as an org with several currencies does (the simulator's Organization has none). */
class WithOwnCode implements Transport {
  readonly inner: Simulator;
  readonly code: string;
  constructor(inner: Simulator, code: string) {
    this.inner = inner;
    this.code = code;
  }
  asked = 0;
  async send(request: TransportRequest): Promise<TransportResponse> {
    if (new URL(request.url).searchParams.get("q") !== ORG_CURRENCY_SOQL) return this.inner.send(request);
    this.asked++;
    return { status: 200, header: () => null, text: JSON.stringify({ totalSize: 1, done: true, records: [{ attributes: { type: "Organization" }, DefaultCurrencyIsoCode: this.code }] }) };
  }
}

/** One delivery of the run with this calibration saved, on a simulated org with one currency (or several). */
async function start(s: Start, attempt: { final: boolean; jobId?: string } = { final: false }) {
  // `used`: the org's API usage when the run reads its limits, so a run that made no read before it can be set where one that did stands.
  const sim = new Simulator({ orgSettings: s.settings ?? "top", ...(s.used === undefined ? {} : { used: s.used }), ...(s.orgCode ? {} : { dropFields: { Organization: ["DefaultCurrencyIsoCode"] } }) });
  const deps = baseDeps({ dataTransport: s.orgCode ? new WithOwnCode(sim, s.orgCode) : sim, automatic: ON, stagesTimeoutMs: 300 });
  deps.store.addRun(runInput({ run: consoleRun({ reviewMode: s.reviewMode ?? "automatic" }), calibration: structuredClone(s.calibration), intake: null, answers: [] }));
  await deps.tokens.save(syntheticConnection());
  const dir = join(deps.runsDir, RUN_ID);
  if (s.marked) {
    mkdirSync(dir, { recursive: true });
    writeFileSync(join(dir, CURRENCY_READ_FILE), "{}\n");
  }
  s.prepare?.(deps);
  const outcome = await createJobHandler(deps)({ kind: "run", runId: RUN_ID }, attempt as never);
  const urls = sim.requests.map((r) => new URL(r.url));
  return {
    outcome,
    deps,
    dir,
    stored: deps.store.runs.get(RUN_ID)!,
    settingsCalls: urls.filter((u) => SETTINGS.test(u.pathname)).length,
    orgReads: urls.filter((u) => u.searchParams.get("q") === ORG_CURRENCY_SOQL || u.searchParams.get("q") === ORG_LOCALE_SOQL).length,
  };
}
type Started = Awaited<ReturnType<typeof start>>;

/** Everything the run wrote: its folder (but the read's own mark) and its stored results. */
function outputs(r: Started): Record<string, unknown> {
  const files: Record<string, string> = {};
  const walk = (d: string) => {
    for (const e of readdirSync(d, { withFileTypes: true })) {
      const p = join(d, e.name);
      if (e.isDirectory()) walk(p);
      else files[relative(r.dir, p)] = readFileSync(p, "utf8");
    }
  };
  walk(r.dir);
  delete files[CURRENCY_READ_FILE];
  const all = JSON.stringify({ files, output: r.stored.output, status: r.stored.run.status, digest: r.deps.store.digests.get(RUN_ID) ?? null });
  // The real clock's times (a step's finish, when /limits was read) differ between any two runs; the fixed run times stay.
  return JSON.parse(all.replace(/\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z/g, (t) => (Math.abs(Date.parse(t) - Date.now()) < 3_600_000 ? "<clock>" : t))) as Record<string, unknown>;
}

/** Without the fields a currency's name moves: the rest of the calibration, which nothing may touch. */
const rest = (c: Calibration) => {
  const { currency: _c, currencyFrom: _f, currencySuggested: _s, ...others } = c;
  return others;
};

const nameLines = (r: Started) => r.deps.lines.filter((l) => /org's settings/.test(l));

describe("which saved calibrations are ours to name (745)", () => {
  test("only an automatic run's, read from the org's settings by no person, unnamed or named only by the customer", async () => {
    const run = consoleRun({ reviewMode: "automatic" });
    assert.deepEqual(unnamedBySalesforce({ run, calibration: await unnamed() }), { currency: "XXX", currencyFrom: "org" });
    assert.deepEqual(unnamedBySalesforce({ run, calibration: await answered("EUR") }), { currency: "EUR", currencyFrom: "customer" });
    const never: Array<[string, Calibration | null, Run]> = [
      ["the settings named it", await namedAtConnect(), run],
      ["the org's own code", await automaticSyntheticCalibration(), run],
      ["unread", Calibration.parse({ ...(await unnamed()), currencyFrom: "unread", currencySuggested: undefined }), run],
      ["a person's", Calibration.parse({ ...(await automaticSyntheticCalibration()), source: "confirmed", confirmedBy: "consultant@example.com" }), run],
      ["org_settings but confirmedBy a person", Calibration.parse({ ...(await unnamed()), confirmedBy: "consultant@example.com" }), run],
      ["a consultant-led run", await unnamed(), consoleRun({ reviewMode: "consultant" })],
      ["none", null, run],
    ];
    for (const [why, calibration, r] of never) assert.equal(unnamedBySalesforce({ run: r, calibration }), null, why);
  });
});

describe("a first delivery of an automatic run whose saved calibration has no currency from Salesforce (745)", () => {
  for (const settings of ACCEPTED) {
    test(`${settings}: the settings' code is saved with its mark before the run, nothing else changes, and the run is released`, async () => {
      const before = await unnamed();
      const r = await start({ calibration: before, settings });
      assert.equal(r.outcome.outcome, "done", r.outcome.detail);
      const c = r.stored.calibration!;
      assert.deepEqual([c.currency, c.currencyFrom, c.currencySuggested], ["GBP", "connect_api", undefined]);
      assert.deepEqual(rest(c), rest(before));
      assert.equal(r.settingsCalls, 1);
      assert.equal(r.orgReads, 1, "the currency query once, and never the locale");
      assert.equal(r.stored.run.status, "released");
      assert.deepEqual(r.deps.store.writes.filter((w) => w.method === "nameCalibrationCurrency"), [{ method: "nameCalibrationCurrency", id: RUN_ID, detail: "GBP" }]);
      assert.ok(r.deps.lines.includes(`Automatic run ${RUN_ID}'s org has one currency, which its settings name: GBP.`));
      assert.ok(r.deps.lines.some((l) => /saved calibration named no currency from Salesforce, so the org's settings were read before it ran: its currency is now GBP/.test(l)));
      // The run used it: the folder's calibration is the named one.
      assert.deepEqual(JSON.parse(readFileSync(join(r.dir, "calibration.json"), "utf8")), JSON.parse(JSON.stringify(c)));
    });
  }

  test("named: the outputs equal a run whose code was read at connect", async () => {
    const updated = await start({ calibration: await unnamed(), settings: "top" });
    // The two calls the update made before the run (the currency query and the settings) are the org's usage when it starts.
    const atConnect = await start({ calibration: await namedAtConnect(), settings: "top", used: 1002 });
    assert.equal(atConnect.settingsCalls, 0, "already named at connect: no read");
    assert.deepEqual(updated.stored.calibration, atConnect.stored.calibration);
    const a = outputs(updated);
    const b = outputs(atConnect);
    assert.deepEqual(a, b);
  });

  test("every rejected answer: the calibration deep-equal to before, the run released as before, its outputs byte for byte as with no read, one line, never thrown or held", async () => {
    const before = await unnamed();
    // The run as it goes with no read, on an org whose usage stands where the read left it (a 500 is tried again by the client's own policy).
    const bases = new Map<number, Record<string, unknown>>();
    const baseFor = async (used: number) => {
      if (!bases.has(used)) {
        const baseline = await start({ calibration: before, settings: "top", marked: true, used });
        assert.equal(baseline.settingsCalls, 0);
        assert.equal(baseline.outcome.outcome, "done", baseline.outcome.detail);
        bases.set(used, outputs(baseline));
      }
      return bases.get(used)!;
    };
    for (const settings of REJECTED) {
      const r = await start({ calibration: before, settings });
      assert.equal(r.outcome.outcome, "done", `${settings}: ${r.outcome.detail}`);
      assert.deepEqual(r.stored.calibration, before, settings);
      assert.equal(r.stored.run.status, "released", settings);
      assert.equal(r.deps.store.held.size, 0, settings);
      assert.ok(!r.deps.store.writes.some((w) => w.method === "nameCalibrationCurrency"), settings);
      assert.equal(r.settingsCalls, settings === "500" ? 4 : 1, settings);
      assert.equal(r.orgReads, 1, `${settings}: the locale is never read for a saved calibration`);
      assert.equal(r.deps.lines.filter((l) => /^The org's settings didn't name its currency \(.*\), so the calibration's currency is left as it was\./.test(l)).length, 1, settings);
      assert.deepEqual(outputs(r), await baseFor(1001 + r.settingsCalls), settings);
    }
  });

  test("a multi-currency org: the settings are never read and the calibration is untouched", async () => {
    const before = await unnamed();
    const r = await start({ calibration: before, orgCode: "USD" });
    assert.equal(r.settingsCalls, 0);
    assert.equal(r.outcome.outcome, "done", r.outcome.detail);
    assert.deepEqual(r.stored.calibration, before);
    assert.ok(nameLines(r).some((l) => /now gives its own currency code/.test(l)));
  });

  test("already named by Salesforce, or a person's: nothing about the currency is read at all", async () => {
    const person = Calibration.parse({ ...(await automaticSyntheticCalibration()), source: "confirmed", confirmedBy: "consultant@example.com" });
    const personXxx = Calibration.parse({ ...(await unnamed()), confirmedBy: "consultant@example.com" });
    for (const calibration of [await namedAtConnect(), await automaticSyntheticCalibration(), person, personXxx]) {
      const r = await start({ calibration });
      assert.equal(r.settingsCalls, 0, calibration.confirmedBy);
      assert.equal(r.orgReads, 0, calibration.confirmedBy);
      assert.deepEqual(r.stored.calibration, calibration);
      assert.equal(nameLines(r).length, 0);
    }
  });

  test("a customer's portal answer is put under Salesforce's, and a disagreement logged with 740's line", async () => {
    const before = await answered("EUR");
    const r = await start({ calibration: before, settings: "top" });
    const c = r.stored.calibration!;
    assert.deepEqual([c.currency, c.currencyFrom], ["GBP", "connect_api"]);
    assert.deepEqual(rest(c), rest(before));
    assert.ok(r.deps.lines.includes(`Automatic run ${RUN_ID}'s customer named EUR in the portal, but the org's settings name GBP: the settings' currency is used.`));
    // The same answer: the mark moves to Salesforce's, and no disagreement is said.
    const same = await start({ calibration: await answered("GBP"), settings: "top" });
    assert.equal(same.stored.calibration!.currencyFrom, "connect_api");
    assert.ok(!same.deps.lines.some((l) => /customer named/.test(l)));
  });

  test("a store that can't save the code: the run goes on with the calibration as stored (here, the one it had), released, one line", async () => {
    const before = await unnamed();
    const r = await start({ calibration: before, prepare: (d) => (d.store.failCurrencyName = true) });
    assert.equal(r.outcome.outcome, "done", r.outcome.detail);
    assert.deepEqual(r.stored.calibration, before);
    assert.equal(r.deps.lines.filter((l) => /may not have been saved .*so the run goes on with the calibration as stored/.test(l)).length, 1);
    assert.equal(JSON.parse(readFileSync(join(r.dir, "calibration.json"), "utf8")).currency, "XXX", "the run used the one it had");
  });

  test("a consultant-led run is never touched (and still refused for an unconfirmed calibration)", async () => {
    const before = await unnamed();
    const r = await start({ calibration: before, reviewMode: "consultant" });
    assert.equal(r.outcome.outcome, "failed");
    assert.equal(r.settingsCalls, 0);
    assert.deepEqual(r.stored.calibration, before);
  });

  test("a calibration written for another org: nothing is read through this connection", async () => {
    const before = Calibration.parse({ ...(await unnamed()), orgId: "00D000000000099AAA" });
    // The run itself refuses it on its last delivery, as before; nothing about the currency was read first.
    const r = await start({ calibration: before }, { final: true });
    assert.equal(r.outcome.outcome, "failed");
    assert.equal(r.settingsCalls, 0);
    assert.equal(r.orgReads, 0);
    assert.deepEqual(r.stored.calibration, before);
  });

  test("a start whose folder is marked read (a redelivery of the same start) reads nothing more", async () => {
    const before = await unnamed();
    const r = await start({ calibration: before, marked: true });
    assert.equal(r.settingsCalls, 0);
    assert.equal(r.orgReads, 0);
    assert.deepEqual(r.stored.calibration, before);
    assert.ok(nameLines(r).some((l) => /on an earlier delivery of this start/.test(l)));
  });
});

describe("a person's Run again of a released automatic run with a saved unnamed calibration (the production case)", () => {
  test("the folder is cleared, the settings read once, the code saved, and the run released named; the same press again reads nothing", async () => {
    const before = await unnamed();
    const r = await start({ calibration: before, settings: "features" }, AGAIN);
    assert.equal(r.outcome.outcome, "done", r.outcome.detail);
    assert.deepEqual([r.stored.calibration!.currency, r.stored.calibration!.currencyFrom], ["GBP", "connect_api"]);
    assert.equal(r.settingsCalls, 1);
    assert.ok(existsSync(join(r.dir, CURRENCY_READ_FILE)));
    // The same press delivered again: the run is released, so nothing is read.
    const again = await createJobHandler(r.deps)({ kind: "run", runId: RUN_ID }, AGAIN as never);
    assert.equal(again.outcome, "skipped");
  });

  test("a rejected answer on Run again: released exactly as before, with the calibration it had", async () => {
    const before = await unnamed();
    const r = await start({ calibration: before, settings: "500" }, AGAIN);
    assert.equal(r.outcome.outcome, "done", r.outcome.detail);
    assert.deepEqual(r.stored.calibration, before);
  });
});

describe("two deliveries at once, and the store's guard", () => {
  test("only one writes; the other goes on with the one saved", async () => {
    const before = await unnamed();
    const deps = baseDeps({ dataTransport: new Simulator({ dropFields: { Organization: ["DefaultCurrencyIsoCode"] } }), automatic: ON });
    const input = runInput({ run: consoleRun({ reviewMode: "automatic" }), calibration: structuredClone(before), intake: null, answers: [] });
    deps.store.addRun(structuredClone(input));
    const stored = syntheticConnection();
    await deps.tokens.save(stored);
    const one = (n: number) =>
      nameCurrencyBeforeRun({ runId: RUN_ID, input, stored, provider: new OAuthConnectionProvider("synthetic-co", { store: deps.tokens, client: oauthClient, env: approvedFromConsole("synthetic-co") }), dir: join(deps.runsDir, `d${n}`), deps, log: (l) => deps.lines.push(l) });
    const [a, b] = await Promise.all([one(1), one(2)]);
    assert.equal(deps.store.writes.filter((w) => w.method === "nameCalibrationCurrency").length, 1);
    assert.deepEqual(a, b, "both go on with the same calibration");
    assert.equal(a.currencyFrom, "connect_api");
    assert.equal(deps.lines.filter((l) => /changed while the org's settings were read/.test(l)).length, 1);
  });

  test("memory: a row that no longer matches, a person's, or one already named is left as it is", async () => {
    const store = new MemoryWorkerStore();
    store.addRun(runInput({ run: consoleRun({ reviewMode: "automatic" }), calibration: await unnamed() }));
    assert.equal(await store.nameCalibrationCurrency(RUN_ID, { currency: "EUR", currencyFrom: "customer" }, "GBP"), "unchanged");
    assert.equal(await store.nameCalibrationCurrency(RUN_ID, { currency: "XXX", currencyFrom: "org" }, "GBP"), "updated");
    assert.equal(await store.nameCalibrationCurrency(RUN_ID, { currency: "XXX", currencyFrom: "org" }, "EUR"), "unchanged");
    await assert.rejects(store.nameCalibrationCurrency(RUN_ID, { currency: "XXX", currencyFrom: "org" }, "QQQ"));
    store.runs.get(RUN_ID)!.calibration = Calibration.parse({ ...(await unnamed()), confirmedBy: "consultant@example.com" });
    assert.equal(await store.nameCalibrationCurrency(RUN_ID, { currency: "XXX", currencyFrom: "org" }, "GBP"), "unchanged");
  });

  test("Postgres: one guarded statement on the code and its standing only; a refusal is thrown by SQLSTATE alone", async () => {
    const sent: Array<{ text: string; values: unknown[] }> = [];
    let rowCount = 1;
    const pool = { query: async (text: string, values: unknown[] = []) => (sent.push({ text, values }), { rows: [], rowCount }) } as unknown as SqlPool;
    const store = new PostgresWorkerStore(pool);
    assert.equal(await store.nameCalibrationCurrency(RUN_ID, { currency: "XXX", currencyFrom: "org" }, "GBP"), "updated");
    assert.equal(sent[0]!.text, NAME_CALIBRATION_CURRENCY_SQL);
    assert.deepEqual(sent[0]!.values, [RUN_ID, "GBP", ORG_SETTINGS_CALIBRATOR, "XXX", "org"]);
    for (const guard of [`"runId" = $1`, `"source" = 'org_settings'`, `"confirmedBy" = $3`, `"currency" = $4`, `"fields" ->> 'currencyFrom' = $5`]) assert.ok(NAME_CALIBRATION_CURRENCY_SQL.includes(guard), guard);
    // Only "currency" and "fields" are set.
    assert.deepEqual([...NAME_CALIBRATION_CURRENCY_SQL.split(" WHERE ")[0]!.matchAll(/"(\w+)" =/g)].map((m) => m[1]), ["currency", "fields"]);
    rowCount = 0;
    assert.equal(await store.nameCalibrationCurrency(RUN_ID, { currency: "XXX", currencyFrom: "org" }, "GBP"), "unchanged");
    await assert.rejects(store.nameCalibrationCurrency(RUN_ID, { currency: "XXX", currencyFrom: "org" }, "QQQ"));
    const broken = new PostgresWorkerStore({ query: async () => Promise.reject(Object.assign(new Error("planted"), { code: "57014" })) } as unknown as SqlPool);
    await assert.rejects(broken.nameCalibrationCurrency(RUN_ID, { currency: "XXX", currencyFrom: "org" }, "GBP"), (e: Error) => /SQLSTATE 57014/.test(e.message) && !e.message.includes("planted"));
  });

  test("the row as the statement leaves it reads back as the named calibration, so a pulse or scan copied from it after carries the code and the mark", async () => {
    const row = calibrationRow(RUN_ID, await unnamed());
    // What the statement does to the row: the code, and "fields" without the suggestion and with the mark.
    const { currencySuggested: _s, ...fields } = row.fields as Record<string, unknown>;
    // A database gives null where the writer left a column out.
    const after = Object.fromEntries(Object.entries({ ...row, source: "org_settings", currency: "GBP", fields: { ...fields, currencyFrom: "connect_api" } }).map(([k, v]) => [k, v === undefined ? null : v]));
    assert.deepEqual(calibrationFromRow(after), await namedAtConnect());
  });
});
