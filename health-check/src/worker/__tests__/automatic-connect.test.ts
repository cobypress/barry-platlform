import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { beginAuthorisation, createConnectLink } from "../../connect/link.ts";
import { CALIBRATION_UNREADABLE, NO_WON_STAGE, ORG_SETTINGS_CALIBRATOR, STAGES_UNREAD } from "../../connect/auto-calibration.ts";
import { StoreError } from "../../connect/org-stages.ts";
import { ORG_CURRENCY_SOQL } from "../../connect/org-currency.ts";
import { approvedReport, AUTOMATIC_DOCUMENTS_NOT_BUILT } from "../../render/render-released.ts";
import type { Transport, TransportRequest, TransportResponse } from "../../salesforce/transport.ts";
import type { ConnectLink } from "../../schemas/connect.ts";
import { Calibration, Run } from "../../schemas/index.ts";
import { FAKE, FakeTokenEndpoint } from "../../testing/fake-oauth.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, loadRecording, ReplayTransport } from "../../testing/replay.ts";
import type { AutomaticSwitch } from "../automatic.ts";
import { AUTOMATIC_OFF_AT_CONNECT, AUTOMATIC_ORG_AT_CONNECT } from "../connect-job.ts";
import { sealConnectSecret } from "../connect-crypto.ts";
import { CALIBRATION_WRITE, HOLD_RUN_SQL, PostgresWorkerStore, REVIEW_MODE_SQL, type SqlClient, type SqlPool, type SqlResult } from "../postgres-store.ts";
import { ORG_SETTINGS_ON_CONSULTANT_RUN } from "../run-job.ts";
import { ORG_AWAITING_CONNECT } from "../types.ts";
import { createJobHandler } from "../worker.ts";
import { baseDeps, consoleRun, INTAKE, oauthClient, RUN_ID, runInput, syntheticConnection } from "./helpers.ts";

// An automatic full check is calibrated from the org's own stages and currency
// at connect and queued (DECISIONS.md 692, 693). A consultant-led run and a
// free scan connect as before.

const NAME = "acme";
const once = { final: false };
const NOW = new Date("2026-11-23T12:00:00.000Z");
const ORG15 = FAKE.orgId.slice(0, 15);
const ON: AutomaticSwitch = { on: true, orgs: new Set([ORG15]) };

const stage = (ApiName: string, SortOrder: number | null, flags: Partial<{ IsActive: boolean; IsClosed: boolean; IsWon: boolean }> = {}) => ({
  attributes: { type: "OpportunityStage" },
  ApiName,
  MasterLabel: `${ApiName} label`,
  IsActive: true,
  IsClosed: false,
  IsWon: false,
  SortOrder,
  ...flags,
});
// Names no log line or outcome could hold by chance.
const STAGES = [
  stage("Quokka Won", 4, { IsClosed: true, IsWon: true }),
  stage("Quokka Prospect", 1),
  stage("Quokka Lost", 5, { IsClosed: true }),
  stage("Quokka Retired", null, { IsActive: false }),
  stage("Quokka Proposal", 2),
];

class Org implements Transport {
  readonly requests: TransportRequest[] = [];
  stages: unknown[] | "refused" = STAGES;
  currency: string | "refused" = "EUR";
  async send(request: TransportRequest): Promise<TransportResponse> {
    this.requests.push(request);
    const url = new URL(request.url);
    const ok = (json: unknown): TransportResponse => ({ status: 200, header: () => null, text: JSON.stringify(json) });
    if (url.pathname === "/services/data/") return ok([{ version: "68.0" }]);
    if (url.pathname.endsWith("/limits")) return ok({ DailyApiRequests: { Max: 1000, Remaining: 990 } });
    const q = url.searchParams.get("q") ?? "";
    if (/FROM PermissionSet WHERE/.test(q)) return ok({ records: [{ Name: "Barometer_Read_Only" }], totalSize: 1, done: true });
    // The org's own currency, read for an automatic run's calibration (DECISIONS.md 693).
    if (q === ORG_CURRENCY_SOQL) {
      if (this.currency === "refused") return { status: 400, header: () => null, text: JSON.stringify([{ errorCode: "INSUFFICIENT_ACCESS", message: "no" }]) };
      return ok({ records: [{ attributes: { type: "Organization" }, DefaultCurrencyIsoCode: this.currency }], totalSize: 1, done: true });
    }
    if (/FROM OpportunityStage/.test(q)) {
      if (this.stages === "refused") return { status: 400, header: () => null, text: JSON.stringify([{ errorCode: "INSUFFICIENT_ACCESS", message: "no" }]) };
      return ok({ records: this.stages, totalSize: this.stages.length, done: true });
    }
    return ok({ records: [], totalSize: 0, done: true });
  }
}

type Flow = Awaited<ReturnType<typeof flow>>;

/** A run behind a pending link, with nothing calibrated, and a worker whose queue is recorded. */
async function flow(options: { reviewMode?: Run["reviewMode"]; product?: Run["product"]; automatic?: AutomaticSwitch } = {}) {
  const fake = new FakeTokenEndpoint();
  const org = new Org();
  const queued: string[] = [];
  const kinds: boolean[] = [];
  const deps = baseDeps({ oauthTransport: fake, dataTransport: org, now: () => NOW, automatic: options.automatic ?? ON, queueRun: async (runId, options) => { queued.push(runId); kinds.push(options?.automatic === true); } });
  deps.store.addRun(runInput({ run: consoleRun({ orgId: ORG_AWAITING_CONNECT, product: options.product ?? "barometer", reviewMode: options.reviewMode ?? "automatic" }), calibration: null }));
  const f = { fake, org, deps, queued, kinds, handle: createJobHandler(deps) };
  const link = addLink(f);
  return { ...f, link, job: { kind: "connect" as const, linkId: link.id } };
}

function addLink(f: { fake: FakeTokenEndpoint; deps: ReturnType<typeof baseDeps> }): ConnectLink {
  const { link } = createConnectLink({ baseUrl: "https://console.example.com", customerRef: "SYN-0001", connectionName: NAME, createdBy: "consultant@example.com" });
  const begun = beginAuthorisation(link, oauthClient);
  const code = f.fake.issueCode(begun.authorisationUrl);
  f.deps.store.addLink(link, "pending", { pending: sealConnectSecret(JSON.stringify(begun.pending), f.deps.connectKey, link.id), code: sealConnectSecret(code, f.deps.connectKey, link.id) }, RUN_ID);
  return link;
}

const methods = (f: Flow, method: string) => f.deps.store.writes.filter((w) => w.method === method).length;
const exchanges = (f: Flow) => f.fake.requests.filter((r) => r.form.get("grant_type") === "authorization_code").length;
const noStageNames = (f: Flow, text: string) => {
  for (const s of STAGES) assert.ok(!text.includes(s.ApiName), `${s.ApiName} in ${text}`);
  for (const line of f.deps.lines) for (const s of STAGES) assert.ok(!line.includes(s.ApiName), line);
};

describe("an automatic full check's connect, with the switch on and the org listed", () => {
  test("saves a calibration read from the org's settings, then queues the run", async () => {
    const f = await flow();
    const outcome = await f.handle(f.job, once);
    assert.equal(outcome.outcome, "connected");
    assert.match(outcome.detail, / The run is calibrated from the org's settings and queued\.$/);
    const c = f.deps.store.runs.get(RUN_ID)!.calibration!;
    assert.equal(c.source, "org_settings");
    assert.equal(c.confirmedBy, ORG_SETTINGS_CALIBRATOR);
    assert.equal(c.confirmedAt, NOW.toISOString());
    assert.equal(c.orgId, FAKE.orgId);
    assert.deepEqual(c.stages, [
      { name: "Quokka Prospect", category: "open", order: 1 },
      { name: "Quokka Proposal", category: "open", order: 2 },
      { name: "Quokka Won", category: "won" },
    ]);
    assert.deepEqual(f.queued, [RUN_ID]);
    // Under its one fixed queue id, so a second message is no second run (DECISIONS.md 693).
    assert.deepEqual(f.kinds, [true]);
    assert.deepEqual(f.deps.store.queued, [RUN_ID]);
    // The stages for the console's screen are written as before, and the calibration after them.
    const order = f.deps.store.writes.map((w) => w.method);
    assert.ok(order.indexOf("markLinkUsed") < order.indexOf("saveOrgStages") && order.indexOf("saveOrgStages") < order.indexOf("saveCalibration") && order.indexOf("saveCalibration") < order.indexOf("recordRunQueued"));
    assert.equal(f.deps.store.held.size, 0);
    noStageNames(f, outcome.detail);
  });

  test("a redelivered job saves once and queues once, and so does a second link for the same run", async () => {
    const f = await flow();
    await f.handle(f.job, once);
    const first = structuredClone(f.deps.store.runs.get(RUN_ID)!.calibration);
    assert.equal((await f.handle(f.job, once)).outcome, "skipped");
    assert.equal((await f.handle(f.job, { final: true })).outcome, "skipped");
    // Another link to the same run, say sent again: it connects, and leaves the run as it was.
    f.org.stages = [stage("Quokka Other Won", 1, { IsClosed: true, IsWon: true })];
    const again = addLink(f);
    const second = await f.handle({ kind: "connect", linkId: again.id }, once);
    assert.equal(second.outcome, "connected");
    // Said for what it is (DECISIONS.md 693): the run was started already, and this connect changed nothing.
    assert.match(second.detail, /The run is already started, so this connect changed nothing\.$/);
    assert.equal(methods(f, "saveCalibration"), 1);
    assert.deepEqual(f.queued, [RUN_ID]);
    assert.deepEqual(f.deps.store.queued, [RUN_ID]);
    assert.deepEqual(f.deps.store.runs.get(RUN_ID)!.calibration, first);
  });

  test("no won stage: nothing saved or queued, and the run is held as calibration_unreadable", async () => {
    const f = await flow();
    f.org.stages = STAGES.map((s) => ({ ...s, IsWon: false }));
    const outcome = await f.handle(f.job, once);
    assert.equal(outcome.outcome, "connected");
    assert.ok(outcome.detail.endsWith(` Held: ${CALIBRATION_UNREADABLE}. ${NO_WON_STAGE}`));
    assert.equal(f.deps.store.runs.get(RUN_ID)!.calibration, null);
    assert.deepEqual(f.queued, []);
    assert.equal(f.deps.store.held.get(RUN_ID), CALIBRATION_UNREADABLE);
    noStageNames(f, outcome.detail);
  });

  test("a failed stage read: nothing saved or queued, and the run is held", async () => {
    const f = await flow();
    f.org.stages = "refused";
    const outcome = await f.handle(f.job, once);
    assert.equal(outcome.outcome, "connected");
    assert.ok(outcome.detail.endsWith(` Held: ${CALIBRATION_UNREADABLE}. ${STAGES_UNREAD}`));
    assert.equal(f.deps.store.runs.get(RUN_ID)!.calibration, null);
    assert.deepEqual(f.queued, []);
    assert.equal(f.deps.store.held.get(RUN_ID), CALIBRATION_UNREADABLE);
  });

  test("a console without the hold's column still connects, and says the hold wasn't recorded", async () => {
    const f = await flow();
    f.org.stages = "refused";
    f.deps.store.heldReasonColumn = false;
    const outcome = await f.handle(f.job, once);
    assert.equal(outcome.outcome, "connected");
    assert.match(outcome.detail, /Held: calibration_unreadable\./);
    // Changed by 700: the log line names the true cause and its counts.
    assert.ok(f.deps.lines.includes(`Automatic run ${RUN_ID} wasn't calibrated or queued: calibration_unreadable (no stages could be read; 0 stages read, 0 won), and the hold couldn't be recorded.`));
  });

  test("a console that can't hold the calibration's source gets no calibration and no queued run", async () => {
    const f = await flow();
    f.deps.store.calibrationSourceColumn = false;
    const outcome = await f.handle(f.job, once);
    assert.equal(outcome.outcome, "connected");
    assert.match(outcome.detail, /The run's calibration couldn't be saved, so the run wasn't started\.$/);
    assert.equal(f.deps.store.runs.get(RUN_ID)!.calibration, null);
    assert.deepEqual(f.queued, []);
    assert.ok(f.deps.lines.some((l) => /has no "source" column/.test(l)));
  });

  test("a store that refuses the calibration leaves the link connected and the run unqueued, naming no stage", async () => {
    const f = await flow();
    f.deps.store.saveCalibration = async (_r, c) => Promise.reject(new Error(`bad value ${c.stages[0]!.name}`));
    const outcome = await f.handle(f.job, once);
    assert.equal(outcome.outcome, "connected");
    assert.deepEqual(f.queued, []);
    noStageNames(f, outcome.detail);
    const g = await flow();
    g.deps.store.saveCalibration = async () => Promise.reject(new StoreError("the console's database refused the calibration (SQLSTATE 23514)"));
    await g.handle(g.job, once);
    assert.ok(g.deps.lines.includes(`Automatic run ${RUN_ID}'s calibration couldn't be saved, so it wasn't queued: the console's database refused the calibration (SQLSTATE 23514).`));
  });
});

describe("the switch at connect", () => {
  test("off: refused before anything is exchanged, with nothing saved, read or queued", async () => {
    for (const automatic of [{ on: false, orgs: new Set([ORG15]) }, { on: false, orgs: new Set<string>() }]) {
      const f = await flow({ automatic });
      const outcome = await f.handle(f.job, once);
      assert.deepEqual(outcome, { outcome: "refused", detail: AUTOMATIC_OFF_AT_CONNECT });
      assert.equal(exchanges(f), 0);
      assert.equal(f.org.requests.length, 0);
      assert.equal(f.deps.store.links.get(f.link.id)!.state, "refused");
      assert.equal(f.deps.store.runs.get(RUN_ID)!.calibration, null);
      assert.deepEqual(f.queued, []);
      assert.equal(f.deps.tokens.has(NAME), false);
    }
  });

  test("on, with the org not listed: the tokens are revoked and nothing is read, saved or queued", async () => {
    const f = await flow({ automatic: { on: true, orgs: new Set(["00D000000000002"]) } });
    const outcome = await f.handle(f.job, once);
    assert.deepEqual(outcome, { outcome: "refused", detail: AUTOMATIC_ORG_AT_CONNECT });
    assert.equal(exchanges(f), 1);
    assert.equal(f.fake.liveRefreshTokens(), 0, "revoked");
    assert.equal(f.org.requests.length, 0);
    assert.equal(f.deps.store.runs.get(RUN_ID)!.calibration, null);
    assert.deepEqual(f.queued, []);
    assert.equal(f.deps.tokens.has(NAME), false);
  });

  test("a consultant-led full check and a free scan connect exactly as before, whatever the switch", async () => {
    const trace = async (product: Run["product"], automatic?: AutomaticSwitch) => {
      const f = await flow({ reviewMode: "consultant", product, automatic });
      const outcome = await f.handle(f.job, once);
      return { outcome, writes: f.deps.store.writes.map((w) => w.method), queued: f.queued, calibration: f.deps.store.runs.get(RUN_ID)!.calibration };
    };
    for (const product of ["barometer", "scan"] as const) {
      const off = await trace(product, { on: false, orgs: new Set() });
      const on = await trace(product, ON);
      assert.deepEqual(on, off, product);
      assert.ok(!on.writes.includes("saveCalibration") && !on.writes.includes("holdRun"), product);
      assert.equal(on.calibration, null);
      assert.deepEqual(on.queued, product === "scan" ? [RUN_ID] : [], product);
    }
  });
});

describe("lane E2's review, closed (DECISIONS.md 693)", () => {
  test("the calibration takes the org's own currency, never the schema's pounds", async () => {
    const f = await flow();
    await f.handle(f.job, once);
    assert.equal(f.deps.store.runs.get(RUN_ID)!.calibration!.currency, "EUR");
  });

  // Changed by 700: a currency that can't be read never holds the run.
  test("no currency read: calibrated with none and queued, with no hold and no flag (DECISIONS.md 700)", async () => {
    const f = await flow();
    f.org.currency = "refused";
    const outcome = await f.handle(f.job, once);
    assert.equal(outcome.outcome, "connected");
    assert.match(outcome.detail, / The run is calibrated from the org's settings and queued\.$/);
    const c = f.deps.store.runs.get(RUN_ID)!.calibration!;
    assert.equal(c.currency, "XXX");
    assert.equal(c.currencyFrom, "none");
    assert.deepEqual(f.queued, [RUN_ID]);
    assert.equal(f.deps.store.held.size, 0);
    assert.deepEqual(f.deps.store.flagsOf(RUN_ID), []);
  });

  test("every hold at connect raises its one flag, and a redelivery raises no second", async () => {
    const f = await flow();
    f.org.stages = "refused";
    await f.handle(f.job, once);
    await f.handle(f.job, { final: true });
    assert.deepEqual(f.deps.store.flagsOf(RUN_ID), [CALIBRATION_UNREADABLE]);
    assert.equal(methods(f, "raiseFlag"), 1);
  });

  test("a delivery that died after marking the link used, before the calibration: the next one reads the org again, calibrates and queues", async () => {
    const f = await flow();
    await f.handle(f.job, once);
    // As the store stands when the worker dies just after markLinkUsed: the link used, the tokens kept, nothing else.
    f.deps.store.runs.get(RUN_ID)!.calibration = null;
    f.deps.store.queued.length = 0;
    f.queued.length = 0;
    const again = await f.handle(f.job, once);
    assert.equal(again.outcome, "skipped");
    assert.match(again.detail, /The run is calibrated from the org's settings and queued\.$/);
    assert.equal(f.deps.store.runs.get(RUN_ID)!.calibration!.source, "org_settings");
    assert.deepEqual(f.queued, [RUN_ID]);
    assert.deepEqual(f.deps.store.queued, [RUN_ID]);
    // And once finished, a further delivery does nothing.
    const third = await f.handle(f.job, { final: true });
    assert.match(third.detail, /needs nothing more/);
    assert.deepEqual(f.queued, [RUN_ID]);
    noStageNames(f, again.detail);
  });

  test("a delivery that died after the calibration, before the queue: the next one queues it, and reads nothing", async () => {
    const f = await flow();
    await f.handle(f.job, once);
    f.deps.store.queued.length = 0;
    f.queued.length = 0;
    const calls = f.org.requests.length;
    const again = await f.handle(f.job, once);
    assert.match(again.detail, /The run is calibrated from the org's settings and queued\.$/);
    assert.deepEqual(f.queued, [RUN_ID]);
    assert.equal(f.org.requests.length, calls);
    assert.equal(methods(f, "saveCalibration"), 1);
  });

  test("a queue that worked but couldn't be recorded is said as queued, never as not queued", async () => {
    const f = await flow();
    f.deps.store.recordRunQueued = async () => Promise.reject(new Error("the database went away"));
    const outcome = await f.handle(f.job, once);
    assert.match(outcome.detail, /The run is calibrated from the org's settings and queued\.$/);
    assert.deepEqual(f.queued, [RUN_ID]);
    assert.ok(f.deps.lines.some((l) => l.startsWith(`Automatic run ${RUN_ID} is queued, but that couldn't be recorded:`)));
    assert.ok(!f.deps.lines.some((l) => l.includes("couldn't be queued")));
  });

  test("a worker with no queue leaves the run calibrated and waiting, for the console to start, and says so", async () => {
    const f = await flow();
    f.deps.queueRun = undefined;
    const outcome = await f.handle(f.job, once);
    assert.match(outcome.detail, /The run is calibrated from the org's settings, and waits to be started\.$/);
    assert.equal(f.deps.store.runs.get(RUN_ID)!.calibration!.source, "org_settings");
    assert.deepEqual(await f.deps.store.automaticRunState(RUN_ID), { calibrated: true, queued: false, held: false, status: "extracting" });
    assert.ok(f.deps.lines.some((l) => /waits for the console to start it/.test(l)));
  });

  test("the connect refusals an automatic run's customer can meet name no consultant", () => {
    for (const text of [AUTOMATIC_OFF_AT_CONNECT, AUTOMATIC_ORG_AT_CONNECT]) assert.ok(!/consultant/i.test(text), text);
  });
});

describe("fix round 1: a connect never holds, reads or starts what it shouldn't (DECISIONS.md 693)", () => {
  test("probe A: a second link whose stage read fails leaves a calibrated, queued run as it was: no hold, no flag, and it says so", async () => {
    const f = await flow();
    await f.handle(f.job, once);
    const calibration = structuredClone(f.deps.store.runs.get(RUN_ID)!.calibration);
    f.org.stages = "refused";
    const again = addLink(f);
    const second = await f.handle({ kind: "connect", linkId: again.id }, once);
    assert.equal(second.outcome, "connected");
    assert.match(second.detail, /The run is already started, so this connect changed nothing\.$/);
    assert.equal(f.deps.store.held.has(RUN_ID), false);
    assert.deepEqual(f.deps.store.flagsOf(RUN_ID), []);
    assert.deepEqual(f.deps.store.runs.get(RUN_ID)!.calibration, calibration);
    assert.deepEqual(f.queued, [RUN_ID]);
  });

  for (const [name, sw] of [["switched off", { on: false, orgs: new Set<string>() }], ["the org no longer listed", { on: true, orgs: new Set(["00D000000000009"]) }]] as const) {
    test(`probe B: a redelivered used link with automatic runs ${name} reads nothing, saves nothing and queues nothing`, async () => {
      const f = await flow();
      await f.handle(f.job, once);
      f.deps.store.runs.get(RUN_ID)!.calibration = null;
      f.deps.store.queued.length = 0;
      f.queued.length = 0;
      const reads = f.org.requests.length;
      const saves = methods(f, "saveCalibration");
      f.deps.automatic = sw;
      const again = await createJobHandler(f.deps)(f.job, once);
      assert.equal(again.outcome, "skipped");
      assert.match(again.detail, /nothing was read\.$/);
      assert.equal(f.org.requests.length, reads);
      assert.equal(methods(f, "saveCalibration"), saves);
      assert.equal(f.deps.store.runs.get(RUN_ID)!.calibration, null);
      assert.deepEqual(f.queued, []);
    });
  }

  test("a failed automatic run is never queued again by a connect: only a person runs it again", async () => {
    const f = await flow();
    await f.handle(f.job, once);
    f.deps.store.queued.length = 0;
    f.queued.length = 0;
    await f.deps.store.recordRunFailure(RUN_ID, "The run stopped with an error", NOW.toISOString());
    const again = await f.handle(f.job, { final: true });
    assert.match(again.detail, /needs nothing more/);
    const third = await f.handle({ kind: "connect", linkId: addLink(f).id }, once);
    assert.match(third.detail, /The run is failed, so this connect changed nothing\.$/);
    assert.deepEqual(f.queued, []);
    assert.deepEqual(f.deps.store.flagsOf(RUN_ID), ["run_failed"]);
  });

  test("a held run is never queued by a redelivery, and its first hold stands", async () => {
    const f = await flow();
    f.org.stages = "refused";
    await f.handle(f.job, once);
    f.org.stages = STAGES;
    const again = await f.handle(f.job, { final: true });
    assert.match(again.detail, /needs nothing more/);
    assert.deepEqual(f.queued, []);
    assert.equal(f.deps.store.held.get(RUN_ID), CALIBRATION_UNREADABLE);
    assert.equal(await f.deps.store.holdRun(RUN_ID, CALIBRATION_UNREADABLE, NOW.toISOString()), false);
  });
});

describe("the calibration check when a run starts", () => {
  const orgSettings = (): Calibration => ({ ...loadCalibration(), source: "org_settings", confirmedBy: ORG_SETTINGS_CALIBRATOR });
  const start = async (reviewMode: Run["reviewMode"], calibration: Calibration | null) => {
    const transport = new ReplayTransport(loadRecording());
    const deps = baseDeps({ dataTransport: transport, automatic: ON });
    deps.store.addRun(runInput({ run: consoleRun({ reviewMode, orgId: FAKE.orgId }), calibration }));
    await deps.tokens.save(syntheticConnection({ orgId: FAKE.orgId }));
    const outcome = await createJobHandler(deps)({ kind: "run", runId: RUN_ID }, once);
    return { outcome, calls: transport.requests.length, claimed: deps.store.writes.some((w) => w.method === "claimRun") };
  };

  test("a consultant-led run with a calibration read from the org is refused, with nothing read", async () => {
    const r = await start("consultant", orgSettings());
    assert.deepEqual(r.outcome, { outcome: "failed", detail: ORG_SETTINGS_ON_CONSULTANT_RUN });
    assert.equal(r.calls, 0);
  });

  test("a consultant-led run with a person's calibration passes it (and runs, as every other test shows)", async () => {
    const r = await start("consultant", loadCalibration());
    assert.notEqual(r.outcome.detail, ORG_SETTINGS_ON_CONSULTANT_RUN);
    assert.equal(r.outcome.outcome, "done", r.outcome.detail);
  });

  test("an automatic run passes it with either source, is claimed and read: lane E3 took away the stop before extraction (DECISIONS.md 693)", async () => {
    for (const calibration of [orgSettings(), loadCalibration()]) {
      const r = await start("automatic", calibration);
      assert.equal(r.outcome.outcome, "done", r.outcome.detail);
      assert.ok(r.calls > 0);
      assert.ok(r.claimed);
    }
  });

  test("an automatic run with no calibration still says so", async () => {
    const r = await start("automatic", null);
    assert.deepEqual(r.outcome, { outcome: "failed", detail: "The calibration hasn't been confirmed, so the run can't start." });
  });
});

describe("the run folder's mode", () => {
  test("an automatic run's run.json says automatic, so its documents read it as one", async () => {
    const { dir } = await runFromRecording({ answers: loadAnswers(), reviewMode: "automatic" });
    assert.equal(Run.parse(JSON.parse(readFileSync(join(dir, "run.json"), "utf8"))).reviewMode, "automatic");
    assert.throws(() => approvedReport(dir, { intake: INTAKE, sections: [], versions: [], overrides: [] }), new RegExp(AUTOMATIC_DOCUMENTS_NOT_BUILT.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));
  });

  test("a consultant-led run's run.json is the same text whether the mode is given or left out", async () => {
    const given = await runFromRecording({ answers: loadAnswers(), reviewMode: "consultant", runId: "hc-2026-10-31-mode0001" });
    const left = await runFromRecording({ answers: loadAnswers(), runId: "hc-2026-10-31-mode0001" });
    // The steps' and the run's finishing times are the clock's, so they're set aside.
    const text = (dir: string) => readFileSync(join(dir, "run.json"), "utf8").replace(/"finishedAt": "[^"]+"/g, '"finishedAt": "-"');
    assert.equal(text(given.dir), text(left.dir));
    assert.match(readFileSync(join(left.dir, "run.json"), "utf8"), /"reviewMode": "consultant"/);
  });

  test("the run job tells the engine the run's mode", async () => {
    // Read from the run job's source.
    const source = readFileSync(new URL("../run-job.ts", import.meta.url), "utf8");
    assert.match(source, /reviewMode: input\.run\.reviewMode,/);
  });
});

// ─── The Postgres store ──────────────────────────────────────────────────────

type Handler = (text: string, values: unknown[]) => SqlResult | undefined;
class FakePool implements SqlPool {
  readonly statements: Array<{ text: string; values: unknown[] }> = [];
  readonly handlers: Handler[] = [];
  async query(text: string, values: unknown[] = []): Promise<SqlResult> {
    this.statements.push({ text, values });
    for (const h of this.handlers) {
      const r = h(text, values);
      if (r) return r;
    }
    return { rows: [], rowCount: 1 };
  }
  async connect(): Promise<SqlClient & { release(): void }> {
    return { query: (t, v) => this.query(t, v), release: () => {} };
  }
  texts(): string[] {
    return this.statements.map((s) => s.text);
  }
}
const PLANTED = "planted-value-0001";
const LINK = { id: "00000000-0000-4000-8000-000000000001", tokenHash: "A".repeat(43), customerRef: "SYN-0001", connectionName: "acme", loginUrl: "https://login.salesforce.com", createdBy: "consultant@example.com", createdAt: "2026-11-03T20:00:00.000Z", expiresAt: "2026-11-06T20:00:00.000Z", usedAt: null, revokedAt: null };
const RUN_SELECT = `SELECT "orgId", "id", "product" FROM "HealthCheckRun"`;

async function connectJobOf(product: string, reviewMode: string | null | "no column") {
  const pool = new FakePool();
  pool.handlers.push((text) => (text.startsWith(RUN_SELECT) ? { rows: [{ orgId: "awaiting-connect", id: RUN_ID, product }], rowCount: 1 } : undefined));
  pool.handlers.push((text) => (text.includes(`FROM "HealthCheckConnectLink"`) ? { rows: [{ ...LINK, state: "pending", sealedPending: null, sealedCode: null }], rowCount: 1 } : undefined));
  pool.handlers.push((text) => (text === REVIEW_MODE_SQL ? { rows: reviewMode === "no column" ? [{ reviewMode: null }] : [{ reviewMode }], rowCount: 1 } : undefined));
  const job = await new PostgresWorkerStore(pool).loadConnectJob(LINK.id);
  return { job: job!, texts: pool.texts() };
}

describe("the Postgres store's side of it", () => {
  test("a full check's review mode is read through the row as JSON; a scan sends exactly the statements it did", async () => {
    const automatic = await connectJobOf("barometer", "automatic");
    assert.equal(automatic.job.automaticRunId, RUN_ID);
    assert.equal(automatic.job.scanRunId, undefined);
    assert.ok(automatic.texts.includes(REVIEW_MODE_SQL));
    assert.match(REVIEW_MODE_SQL, /to_jsonb\("HealthCheckRun"\) ->> 'reviewMode'/);
    for (const mode of [null, "no column"] as const) assert.equal((await connectJobOf("barometer", mode)).job.automaticRunId, undefined);
    const scan = await connectJobOf("scan", "automatic");
    assert.equal(scan.job.scanRunId, RUN_ID);
    assert.equal(scan.job.automaticRunId, undefined);
    assert.equal(scan.texts.length, 2);
    assert.ok(!scan.texts.includes(REVIEW_MODE_SQL));
    assert.equal((await connectJobOf("pulse", "automatic")).job.automaticRunId, undefined);
  });

  test("the calibration is one parametrised insert with its source, written only where the run has none", async () => {
    const c = Calibration.parse({ ...loadCalibration(), source: "org_settings", confirmedBy: ORG_SETTINGS_CALIBRATOR });
    const pool = new FakePool();
    assert.equal(await new PostgresWorkerStore(pool).saveCalibration(RUN_ID, c), "saved");
    assert.equal(pool.statements.length, 1);
    const { text, values } = pool.statements[0]!;
    assert.match(text, /^INSERT INTO "HealthCheckCalibration" \(.*"source"\) VALUES \(.*\) ON CONFLICT \("runId"\) DO NOTHING$/);
    assert.equal(values.length, Object.keys(CALIBRATION_WRITE).length);
    assert.equal(values[0], RUN_ID);
    assert.equal(values.at(-1), "org_settings");
    assert.ok(!text.includes("Quokka") && !text.includes(c.stages[0]!.name), "no value in the text");
    const exists = new FakePool();
    exists.handlers.push(() => ({ rows: [], rowCount: 0 }));
    assert.equal(await new PostgresWorkerStore(exists).saveCalibration(RUN_ID, c), "exists");
  });

  test("without the source column nothing is written; any other refusal is thrown by its SQLSTATE alone", async () => {
    const c = Calibration.parse({ ...loadCalibration(), source: "org_settings" });
    const failing = (code: string) => {
      const pool = new FakePool();
      pool.handlers.push(() => {
        throw Object.assign(new Error(`${PLANTED} broke`), { code });
      });
      return new PostgresWorkerStore(pool);
    };
    assert.equal(await failing("42703").saveCalibration(RUN_ID, c), "no_source_column");
    await assert.rejects(failing("23514").saveCalibration(RUN_ID, c), (err: Error) => err instanceof StoreError && err.message === "the console's database refused the calibration (SQLSTATE 23514)");
  });

  test("the hold and its flag are one statement, an automatic run's only; a console without the column says so by SQLSTATE and goes on", async () => {
    const pool = new FakePool();
    pool.handlers.push((text) => (text === HOLD_RUN_SQL ? { rows: [{ held: 1 }], rowCount: 1 } : undefined));
    assert.equal(await new PostgresWorkerStore(pool).holdRun(RUN_ID, CALIBRATION_UNREADABLE, NOW.toISOString()), true);
    // Changed by 700: the flag carries its counts, numbers only, as the fourth value.
    assert.deepEqual(pool.statements, [{ text: HOLD_RUN_SQL, values: [RUN_ID, CALIBRATION_UNREADABLE, NOW.toISOString(), "{}"] }]);
    assert.match(HOLD_RUN_SQL, /"reviewMode" = 'automatic' AND "heldReason" IS NULL/);
    // The flag only from the row the hold set, raised once (DECISIONS.md 693).
    assert.match(HOLD_RUN_SQL, /INSERT INTO "HealthCheckRunFlag" \("runId", "kind", "raisedAt", "counts"\) SELECT "id", \$2, \$3::timestamp\(3\), \$4::jsonb FROM held ON CONFLICT \("runId", "kind"\) DO UPDATE .* WHERE "HealthCheckRunFlag"\."clearedAt" IS NOT NULL\) /);
    // A run already held: nothing set, so nothing said held.
    const already = new FakePool();
    already.handlers.push(() => ({ rows: [{ held: 0 }], rowCount: 1 }));
    assert.equal(await new PostgresWorkerStore(already).holdRun(RUN_ID, CALIBRATION_UNREADABLE, NOW.toISOString()), false);
    const without = new FakePool();
    without.handlers.push(() => {
      throw Object.assign(new Error(`column "heldReason" ${PLANTED}`), { code: "42703" });
    });
    const lines: string[] = [];
    assert.equal(await new PostgresWorkerStore(without, { log: (l) => void lines.push(l) }).holdRun(RUN_ID, CALIBRATION_UNREADABLE, NOW.toISOString()), false);
    assert.equal(lines.length, 1);
    assert.match(lines[0]!, /SQLSTATE 42703/);
    assert.ok(!lines[0]!.includes(PLANTED));
  });
});
