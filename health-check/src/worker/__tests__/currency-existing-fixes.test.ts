import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { StoreError } from "../../connect/org-stages.ts";
import { ORG_CURRENCY_SOQL } from "../../connect/org-currency.ts";
import { authorisationUrl, exchangeCode, newPkce, newState } from "../../salesforce/oauth.ts";
import type { OAuthTransport, Transport, TransportRequest, TransportResponse } from "../../salesforce/transport.ts";
import { Calibration } from "../../schemas/index.ts";
import { automaticSyntheticCalibration } from "../../testing/automatic.ts";
import { FAKE, FakeTokenEndpoint } from "../../testing/fake-oauth.ts";
import { Simulator } from "../../testing/simulator.ts";
import { CONNECTED_USER, ORG_ID } from "../../testing/synthetic-org.ts";
import { MemoryWorkerStore } from "../memory-store.ts";
import { CURRENCY_READ_FILE, unnamedBySalesforce } from "../name-currency.ts";
import type { CurrencyWas, RunJobInput } from "../types.ts";
import { createJobHandler } from "../worker.ts";
import { baseDeps, consoleRun, FlakyTransport, oauthClient, RUN_ID, runInput, syntheticConnection } from "./helpers.ts";

// Decision 745's fix round: a write whose acknowledgement is lost, a reload
// that fails, a crash after the run's folder records its calibration, and the
// last delivery or not, never end in "the calibration has changed" or a run
// failed that would have succeeded before 745; the naming step's token work
// settles before the run's own connection loads the stored token; the real
// cause of a failed currency query is said.

const ORG15 = ORG_ID.slice(0, 15);
const ON = { on: true, orgs: new Set([ORG15]) };
const SETTINGS = /\/connect\/organization$/;
const single = (extra: ConstructorParameters<typeof Simulator>[0] = {}) => new Simulator({ orgSettings: "top", dropFields: { Organization: ["DefaultCurrencyIsoCode"] }, ...extra });

async function unnamed(): Promise<Calibration> {
  const { currency: _c, currencyFrom: _f, currencySuggested: _s, ...rest } = await automaticSyntheticCalibration();
  return Calibration.parse({ ...rest, currency: "XXX", currencyFrom: "org", currencySuggested: "GBP" });
}

type Write = "committed then threw" | "lost";

/** A store whose currency write commits or not and then throws, and whose next reload may fail once. */
class FaultyStore extends MemoryWorkerStore {
  write: Write = "lost";
  failReload = false;
  #reloadFails = false;
  override async nameCalibrationCurrency(runId: string, was: CurrencyWas, code: string): Promise<"updated" | "unchanged"> {
    if (this.write === "committed then threw") await super.nameCalibrationCurrency(runId, was, code);
    this.#reloadFails = this.failReload;
    throw new StoreError("the console's database refused the calibration's currency (SQLSTATE 08006)");
  }
  override async loadRunJob(runId: string): Promise<RunJobInput | null> {
    if (this.#reloadFails) {
      this.#reloadFails = false;
      throw new Error("the store didn't answer");
    }
    return super.loadRunJob(runId);
  }
}

const CHANGED = /calibration has changed/;
/** Requests the naming step makes before the run: the API versions, the limits, the currency query and the settings. */
const NAMING_REQUESTS = 4;

/**
 * Two deliveries: the first as given, crashing (every request failing) after
 * `crashAfter` requests or not at all; then, unless the first was the last, a
 * last one through a healthy org. `base`: as before 745, the folder marked so
 * nothing is read or written for the currency.
 */
async function sequence(s: { write: Write; failReload: boolean; crashAfter: number | null; firstFinal: boolean; base?: boolean }) {
  const store = new FaultyStore();
  store.write = s.write;
  store.failReload = s.failReload;
  const sim = single();
  const deps = baseDeps({ store, dataTransport: s.crashAfter === null ? sim : new FlakyTransport(sim, s.crashAfter), automatic: ON, stagesTimeoutMs: 300 });
  deps.store.addRun(runInput({ run: consoleRun({ reviewMode: "automatic" }), calibration: await unnamed(), intake: null, answers: [] }));
  await deps.tokens.save(syntheticConnection());
  const dir = join(deps.runsDir, RUN_ID);
  if (s.base) {
    mkdirSync(dir, { recursive: true });
    writeFileSync(join(dir, CURRENCY_READ_FILE), "{}\n");
  }
  const said: string[] = [];
  const deliver = async (d: typeof deps, final: boolean) => {
    try {
      const out = await createJobHandler(d)({ kind: "run", runId: RUN_ID }, { final } as never);
      said.push(`${out.outcome}: ${out.detail}`);
    } catch (err) {
      said.push(`threw: ${(err as Error).message}`);
    }
  };
  await deliver(deps, s.firstFinal);
  const runJsonAfterFirst = existsSync(join(dir, "run.json"));
  if (!s.firstFinal) await deliver({ ...deps, dataTransport: single() }, true);
  const stored = store.runs.get(RUN_ID)!;
  const folder = existsSync(join(dir, "calibration.json")) ? (JSON.parse(readFileSync(join(dir, "calibration.json"), "utf8")) as Calibration) : null;
  return { said, status: stored.run.status, calibration: stored.calibration!, folder, runJsonAfterFirst, lines: deps.lines };
}

describe("a currency write that threw, a reload that failed, a crash and the last delivery (745 fix round)", () => {
  const cases: Array<{ write: Write; failReload: boolean; crashAfter: number | null; firstFinal: boolean }> = [];
  for (const write of ["committed then threw", "lost"] as const)
    for (const failReload of [false, true])
      for (const crashAfter of [null, 40])
        for (const firstFinal of [false, true]) cases.push({ write, failReload, crashAfter, firstFinal });

  for (const c of cases) {
    test(`${c.write}, reload ${c.failReload ? "fails" : "works"}, ${c.crashAfter === null ? "no crash" : "a crash after run.json"}, first delivery ${c.firstFinal ? "the last" : "not the last"}`, async () => {
      const ours = await sequence(c);
      const base = await sequence({ ...c, crashAfter: c.crashAfter === null ? null : c.crashAfter - NAMING_REQUESTS, base: true });
      for (const line of [...ours.said, ...ours.lines]) assert.doesNotMatch(line, CHANGED, line);
      if (c.crashAfter !== null && !(c.failReload && !c.firstFinal)) assert.ok(ours.runJsonAfterFirst, "the crash came after run.json");
      // Never failed for good where the run before 745 wasn't, and ended as that run did.
      assert.equal(ours.status, base.status, JSON.stringify(ours.said));
      if (base.status !== "failed") assert.notEqual(ours.status, "failed");
      // What the run used is what the store holds, unless the last delivery couldn't read the row (then it used the one it had, as before 745).
      if (ours.folder && !(c.failReload && c.firstFinal)) assert.equal(ours.folder.currency, ours.calibration.currency, JSON.stringify(ours.said));
      if (c.failReload && !c.firstFinal) assert.match(ours.said[0]!, /^threw: .*couldn't be read again.*the queue delivers it again/);
    });
  }

  test("the reviewer's probe: the ack is lost after the row is named, the run crashes after run.json, and the redelivery finishes", async () => {
    const r = await sequence({ write: "committed then threw", failReload: false, crashAfter: 40, firstFinal: false });
    assert.equal(r.calibration.currency, "GBP");
    assert.equal(r.folder!.currency, "GBP", "the run went on with the row as stored");
    assert.match(r.said[1]!, /^done: /);
    assert.equal(r.status, "released");
  });
});

/** The OAuth endpoint, answering a refresh only after `delayMs`: Salesforce has already rotated the refresh token when it does. */
class SlowRefresh implements OAuthTransport {
  readonly inner: FakeTokenEndpoint;
  delayMs = 0;
  constructor(inner: FakeTokenEndpoint) {
    this.inner = inner;
  }
  async postForm(url: string, form: URLSearchParams): Promise<TransportResponse> {
    const answer = await this.inner.postForm(url, form);
    if (form.get("grant_type") === "refresh_token" && this.delayMs > 0) await new Promise((r) => setTimeout(r, this.delayMs));
    return answer;
  }
}

/** The org, refusing the first access token as expired, so the first request refreshes. */
class ExpiredFirst implements Transport {
  readonly inner: Transport;
  readonly expired: string;
  constructor(inner: Transport, expired: string) {
    this.inner = inner;
    this.expired = expired;
  }
  async send(request: TransportRequest): Promise<TransportResponse> {
    if (request.headers.Authorization === `Bearer ${this.expired}`) return { status: 401, header: () => null, text: JSON.stringify([{ errorCode: "INVALID_SESSION_ID", message: "Session expired or invalid" }]) };
    return this.inner.send(request);
  }
}

describe("the naming step's token refresh settles before the run connects (745 fix round)", () => {
  test("a refresh slower than the naming step's time limit, with rotation: the run finds the rotated token and is released", async () => {
    const fake = new FakeTokenEndpoint();
    fake.orgId = ORG_ID;
    const { verifier, challenge } = newPkce();
    const code = fake.issueCode(authorisationUrl({ loginUrl: FAKE.loginUrl, client: oauthClient, state: newState(), codeChallenge: challenge }), CONNECTED_USER.id);
    const tokens = await exchangeCode({ transport: fake, loginUrl: FAKE.loginUrl, client: oauthClient, code, codeVerifier: verifier });
    const slow = new SlowRefresh(fake);
    slow.delayMs = 400;
    const deps = baseDeps({ dataTransport: new ExpiredFirst(single(), tokens.accessToken), oauthTransport: slow, automatic: ON, stagesTimeoutMs: 100 });
    deps.store.addRun(runInput({ run: consoleRun({ reviewMode: "automatic" }), calibration: await unnamed(), intake: null, answers: [] }));
    await deps.tokens.save(syntheticConnection({ instanceUrl: FAKE.instanceUrl, accessToken: tokens.accessToken, refreshToken: tokens.refreshToken }));
    const out = await createJobHandler(deps)({ kind: "run", runId: RUN_ID }, { final: true } as never);
    assert.equal(out.outcome, "done", out.detail);
    assert.equal(deps.store.runs.get(RUN_ID)!.run.status, "released");
    assert.equal(fake.requests.filter((r) => r.form.get("grant_type") === "refresh_token").length, 1, "one refresh, the naming step's, and the run used what it stored");
    assert.ok(deps.lines.some((l) => /the connection didn't open in time/.test(l)), "the naming step gave up on time");
    assert.deepEqual(deps.store.runs.get(RUN_ID)!.calibration, await unnamed());
  });
});

describe("the real cause of a failed currency query, and the eligibility branches (745 fix round)", () => {
  /** The org's currency query answering as told: a lost token's 401 with no refresh token that works, or the budget. */
  async function queryFails(how: "budget" | "revoked") {
    const sim = single(how === "budget" ? { used: 60_000 } : {});
    const fake = new FakeTokenEndpoint();
    const transport: Transport = {
      send: async (request) => {
        const q = new URL(request.url).searchParams.get("q");
        if (how === "revoked" && q === ORG_CURRENCY_SOQL) return { status: 401, header: () => null, text: JSON.stringify([{ errorCode: "INVALID_SESSION_ID", message: "Session expired or invalid" }]) };
        return sim.send(request);
      },
    };
    const deps = baseDeps({ dataTransport: transport, oauthTransport: fake, automatic: ON, stagesTimeoutMs: 300 });
    deps.store.addRun(runInput({ run: consoleRun({ reviewMode: "automatic" }), calibration: await unnamed(), intake: null, answers: [] }));
    // A refresh token the token endpoint has never issued: refused, as a revoked one is.
    await deps.tokens.save(syntheticConnection({ instanceUrl: FAKE.instanceUrl }));
    await createJobHandler(deps)({ kind: "run", runId: RUN_ID }, { final: true } as never).catch(() => null);
    return { deps, settingsCalls: sim.requests.filter((r) => SETTINGS.test(new URL(r.url).pathname)).length };
  }

  test("a budget stop says the budget stopped it, in the org's settings' line, and nothing is named", async () => {
    const { deps, settingsCalls } = await queryFails("budget");
    assert.equal(settingsCalls, 0);
    const lines = deps.lines.filter((l) => /org's settings/.test(l));
    assert.ok(lines.some((l) => /the API budget stopped it/.test(l)), lines.join("\n"));
    assert.ok(!deps.lines.some((l) => /couldn't say whether it has one currency/.test(l)));
    assert.deepEqual(deps.store.runs.get(RUN_ID)!.calibration, await unnamed());
  });

  test("a token Salesforce refuses says so, in the org's settings' line, and nothing is named", async () => {
    const { deps, settingsCalls } = await queryFails("revoked");
    assert.equal(settingsCalls, 0);
    const lines = deps.lines.filter((l) => /org's settings/.test(l));
    assert.ok(lines.some((l) => /Salesforce refused the connection's token/.test(l)), lines.join("\n"));
    assert.deepEqual(deps.store.runs.get(RUN_ID)!.calibration, await unnamed());
  });

  test("the switch off: the run is refused before anything about the currency is read", async () => {
    const sim = single();
    const deps = baseDeps({ dataTransport: sim, automatic: { on: false, orgs: new Set([ORG15]) } });
    deps.store.addRun(runInput({ run: consoleRun({ reviewMode: "automatic" }), calibration: await unnamed(), intake: null, answers: [] }));
    await deps.tokens.save(syntheticConnection());
    const out = await createJobHandler(deps)({ kind: "run", runId: RUN_ID }, { final: true } as never);
    assert.equal(out.outcome, "failed");
    assert.equal(sim.requests.length, 0);
    assert.deepEqual(deps.store.runs.get(RUN_ID)!.calibration, await unnamed());
  });

  test("a customer's answer whose code is off our list isn't ours to name", async () => {
    const { currency: _c, currencyFrom: _f, currencySuggested: _s, ...rest } = await automaticSyntheticCalibration();
    const off = Calibration.parse({ ...rest, currency: "QQQ", currencyFrom: "customer" });
    assert.equal(unnamedBySalesforce({ run: consoleRun({ reviewMode: "automatic" }), calibration: off }), null);
  });
});
