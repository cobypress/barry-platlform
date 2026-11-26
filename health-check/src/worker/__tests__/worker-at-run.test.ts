import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { atRunRow, atRunStartLine, BUILD_COMMIT, WORKER_SWITCHES, WorkerAtRun, workerAtRun } from "../at-run.ts";
import { readWorkerEnv, WORKER_ENV, type WorkerConfig } from "../env.ts";
import { processRunJob } from "../run-job.ts";
import { PostgresWorkerStore, RUN_WORKER_SQL, type SqlPool, type SqlResult } from "../postgres-store.ts";
import { workerCommand, type WorkerModules } from "../../cli/worker.ts";
import type { BullModule, QueueWorker } from "../bullmq.ts";
import { loadRecording, ReplayTransport } from "../../testing/replay.ts";
import { baseDeps, consoleRun, RUN_ID, runInput, syntheticConnection, temp } from "./helpers.ts";

// What each run records of the worker that ran it (DECISIONS.md 774): the
// boolean switches by name, with "set but not on" for a variable that's there
// and isn't the word, and the build's commit. Never a raw value. The column
// may be missing (the console's migration not yet applied): the run goes on.

const base = (): NodeJS.ProcessEnv => ({
  REDIS_URL: "redis://localhost:6379",
  HC_WORKER_DATABASE_URL: "postgres://worker@db.example.com/console",
  HC_CONNECT_KEY: Buffer.alloc(32, 1).toString("base64"),
  HC_TOKEN_KEY: Buffer.alloc(32, 2).toString("base64"),
  HC_CONNECTIONS_DIR: "/tmp/hc-at-run/connections",
  HC_RUNS_DIR: "/tmp/hc-at-run/runs",
  HC_SF_CLIENT_ID: "3MVG9fake.client.id",
  HC_SF_CLIENT_SECRET: "fake-client-secret",
  HC_SF_REDIRECT_URI: "https://console.example.com/connect/callback",
});
const configOf = (env: NodeJS.ProcessEnv): WorkerConfig => {
  const read = readWorkerEnv(env);
  assert.ok(read.ok, read.ok ? "" : read.problems.join("; "));
  return read.config;
};
const atRunOf = (extra: NodeJS.ProcessEnv) => {
  const env = { ...base(), ...extra };
  return workerAtRun(configOf(env), env);
};

describe("what is stored: the boolean switches, by name, and nothing else (DECISIONS.md 774)", () => {
  test("the exact list, pinned: adding any setting to what is stored fails here first", () => {
    assert.deepEqual(
      WORKER_SWITCHES.map((s) => [s.key, s.variable]),
      [
        ["automatic", "HC_AUTOMATIC"],
        ["documentsOnDemand", "HC_DOCUMENTS_ON_DEMAND"],
        ["handbookSummaries", "HC_HANDBOOK_SUMMARIES"],
        ["stageNames", "HC_STAGE_NAMES"],
        ["stageNamesModel", "HC_STAGE_NAMES_MODEL"],
        ["stageNamesModelConsole", "HC_STAGE_NAMES_MODEL_CONSOLE"],
        ["areaReasons", "HC_AREA_REASONS"],
        ["leadJourney", "HC_LEAD_JOURNEY"],
      ],
    );
    const stored = atRunRow(atRunOf({}), "2026-11-26T10:00:00.000Z");
    assert.deepEqual(Object.keys(stored), ["version", "at", "build", "switches"]);
    assert.deepEqual(Object.keys(stored.switches), WORKER_SWITCHES.map((s) => s.key));
    for (const [key, s] of Object.entries(stored.switches)) assert.deepEqual(Object.keys(s), ["on", "state"], key);
  });

  test("each is an on/off switch of the worker's, never a key, address, folder, org list, id or model name", () => {
    for (const s of WORKER_SWITCHES) {
      const why = (WORKER_ENV.optional as Record<string, string>)[s.variable];
      assert.ok(why, `${s.variable} is the worker's own variable`);
      // Each is a switch that a single exact word turns on.
      assert.match(why, /^(on|ready), exactly/, s.variable);
      assert.doesNotMatch(s.variable, /KEY|SECRET|TOKEN|URL|URI|DIR|HOME|ORGS|EMAIL|_ID$|VERSION|CONCURRENCY|SHARE/, s.variable);
    }
    // The config each is read from is a boolean, whatever the variables say.
    for (const value of [undefined, "on", "ready", "On", "1"]) {
      const env = { ...base(), ...Object.fromEntries(WORKER_SWITCHES.map((s) => [s.variable, value])) };
      const config = configOf(env);
      for (const s of WORKER_SWITCHES) assert.equal(typeof s.on(config), "boolean", s.key);
    }
  });

  test("the schema refuses anything more: another key, a value in place of a state, or on that disagrees with its state", () => {
    const row = atRunRow(atRunOf({}), "2026-11-26T10:00:00.000Z");
    assert.ok(WorkerAtRun.safeParse(row).success);
    assert.equal(WorkerAtRun.safeParse({ ...row, env: {} }).success, false);
    assert.equal(WorkerAtRun.safeParse({ ...row, switches: { ...row.switches, automaticOrgs: { on: true, state: "on" } } }).success, false);
    assert.equal(WorkerAtRun.safeParse({ ...row, switches: { ...row.switches, stageNames: { on: false, state: "On" } } }).success, false);
    assert.equal(WorkerAtRun.safeParse({ ...row, switches: { ...row.switches, stageNames: { on: false, state: "set_not_on", value: "On" } } }).success, false);
    assert.equal(WorkerAtRun.safeParse({ ...row, switches: { ...row.switches, stageNames: { on: true, state: "unset" } } }).success, false);
    assert.equal(WorkerAtRun.safeParse({ ...row, build: "main" }).success, false);
  });
});

describe("the three states", () => {
  test("unset, on, and set but not on, for every switch", () => {
    for (const s of WORKER_SWITCHES) {
      const word = s.variable === "HC_STAGE_NAMES_MODEL_CONSOLE" ? "ready" : "on";
      assert.deepEqual(atRunOf({}).switches[s.key], { on: false, state: "unset" }, s.key);
      assert.deepEqual(atRunOf({ [s.variable]: "" }).switches[s.key], { on: false, state: "unset" }, `${s.key} empty`);
      assert.deepEqual(atRunOf({ [s.variable]: word }).switches[s.key], { on: true, state: "on" }, s.key);
      for (const near of ["On", "ON", "true", "1", " on", "on ", "yes", word === "ready" ? "on" : "ready"]) {
        assert.deepEqual(atRunOf({ [s.variable]: near }).switches[s.key], { on: false, state: "set_not_on" }, `${s.key}=${JSON.stringify(near)}`);
      }
    }
  });

  test("the start line names the variables set but not on, never their values", () => {
    const line = atRunStartLine(atRunOf({ HC_STAGE_NAMES: "on", HC_AREA_REASONS: "On", RENDER_GIT_COMMIT: "97ec3a8f00000000000000000000000000000000" }));
    assert.match(line, /build \(97ec3a8\)/);
    assert.match(line, /Set but not on, as they aren't exactly the word that turns them on: HC_AREA_REASONS\./);
    assert.doesNotMatch(line, /On\b|HC_STAGE_NAMES[^_]/);
    assert.match(atRunStartLine(atRunOf({})), /build \(not given by the host\)/);
    assert.doesNotMatch(atRunStartLine(atRunOf({})), /Set but not on/);
  });
});

describe("the build", () => {
  test("RENDER_GIT_COMMIT when it's 7 to 40 hex characters, lower case; anything else is not given", () => {
    const full = "97ec3a8f12345678901234567890abcdef012345";
    assert.equal(atRunOf({ RENDER_GIT_COMMIT: full }).build, full);
    assert.equal(atRunOf({ RENDER_GIT_COMMIT: full.toUpperCase() }).build, full);
    assert.equal(atRunOf({ RENDER_GIT_COMMIT: " 97ec3a8 " }).build, "97ec3a8");
    for (const bad of ["", "97ec3a", `${full}0`, "main", "97ec3a8g", "97ec3a8; DROP", "postgres://x@y/z"]) assert.equal(atRunOf({ RENDER_GIT_COMMIT: bad }).build, null, bad);
    assert.equal(atRunOf({}).build, null);
    assert.ok(BUILD_COMMIT.test("abcdef0"));
    assert.ok(WORKER_ENV.optional.RENDER_GIT_COMMIT);
  });
});

// Every variable the worker reads, each set to a value of its own that can be found again.
const tag = (name: string) => `CANARY${name.replace(/[^A-Z0-9]/g, "")}`;
const key44 = (name: string) => `${`${tag(name)}${"A".repeat(43)}`.slice(0, 43)}=`;
function canaryEnv(): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = {
    REDIS_URL: `redis://${tag("REDIS")}@localhost:6379`,
    HC_WORKER_DATABASE_URL: `postgres://${tag("DBUSER")}:${tag("DBPASS")}@db.example.com/console`,
    HC_CONNECT_KEY: key44("CONNECT"),
    HC_TOKEN_KEY: key44("TOKEN"),
    HC_CONNECTIONS_DIR: `/tmp/${tag("CONNECTIONSDIR")}`,
    HC_RUNS_DIR: `${temp()}/${tag("RUNSDIR")}`,
    HC_SF_CLIENT_ID: tag("SFCLIENTID"),
    HC_SF_CLIENT_SECRET: tag("SFCLIENTSECRET"),
    HC_SF_REDIRECT_URI: `https://${tag("REDIRECT").toLowerCase()}.example.com/cb`,
    HC_WORKER_CONCURRENCY: "3",
    HC_API_BUDGET_SHARE: "0.5",
    HC_SF_API_VERSION: tag("APIVERSION"),
    HC_DRIVE_SA_EMAIL: `${tag("DRIVEEMAIL")}@example.com`,
    HC_DRIVE_SA_KEY: `-----BEGIN PRIVATE KEY-----${tag("DRIVEKEY")}-----END PRIVATE KEY-----`,
    HC_DRIVE_FOLDER_ID: tag("DRIVEFOLDER"),
    HC_DRIVE_IMPERSONATE: `${tag("IMPERSONATE")}@example.com`,
    ANTHROPIC_API_KEY: tag("ANTHROPICKEY"),
    ANTHROPIC_FEDERATION_RULE_ID: tag("FEDRULE"),
    ANTHROPIC_ORGANIZATION_ID: tag("ANTHROPICORG"),
    ANTHROPIC_SERVICE_ACCOUNT_ID: tag("SERVICEACCOUNT"),
    ANTHROPIC_WORKSPACE_ID: tag("WORKSPACE"),
    ANTHROPIC_IDENTITY_TOKEN_FILE: `/tmp/${tag("IDTOKENFILE")}`,
    HC_CLEANUP_DRY_RUN: "1",
    HC_SF_SIGNIN_CLIENT_ID: tag("SIGNINID"),
    HC_SF_SIGNIN_CLIENT_SECRET: tag("SIGNINSECRET"),
    HC_SF_SIGNIN_REDIRECT_URI: `https://${tag("SIGNINREDIRECT").toLowerCase()}.example.com/cb`,
    HC_WORKER_RECYCLE_AT: tag("RECYCLE"),
    HC_PMD_HOME: `/tmp/${tag("PMDHOME")}`,
    HC_JAVA_HOME: `/tmp/${tag("JAVAHOME")}`,
    HC_AUTOMATIC_ORGS: "00DCANARYORG001,00DCANARYORG002",
    HC_MICROSITE_KEY: key44("MICROSITE"),
    HC_DOCUMENTS_KEY: key44("DOCUMENTS"),
    RENDER_GIT_COMMIT: tag("RENDERCOMMIT"),
  };
  // Every switch set, to a value that isn't its word.
  for (const s of WORKER_SWITCHES) env[s.variable] = tag(s.variable);
  return env;
}
/** The values that could be told apart if they leaked: every one but the plain small numbers. */
const canaries = (env: NodeJS.ProcessEnv) => Object.entries(env).filter(([, v]) => v && v.length >= 6).map(([k, v]) => [k, v!] as const);

describe("never a raw value: every variable set to a canary", () => {
  test("covers every variable the worker declares", () => {
    const env = canaryEnv();
    for (const name of [...Object.keys(WORKER_ENV.required), ...Object.keys(WORKER_ENV.optional)]) assert.ok(env[name] !== undefined, name);
  });

  test("none appears in what a run stores, or in what this lane logs, at start or on a run", async () => {
    const env = canaryEnv();
    const config = configOf(env);
    const atRun = workerAtRun(config, env);
    for (const s of WORKER_SWITCHES) assert.deepEqual(atRun.switches[s.key], { on: false, state: "set_not_on" }, s.key);
    assert.equal(atRun.build, null);

    // A run taken with it, through the memory store.
    const deps = baseDeps({ dataTransport: new ReplayTransport(loadRecording()), atRun });
    deps.store.addRun(runInput({ run: consoleRun() }));
    await deps.tokens.save(syntheticConnection());
    assert.equal((await processRunJob(RUN_ID, deps, { final: false })).outcome, "done");
    const stored = JSON.stringify(Object.fromEntries(deps.store.workerAtRun));
    assert.ok(stored.includes(RUN_ID));

    // The same, through the Postgres store: the statement's values.
    const pool = new Pool();
    assert.equal(await new PostgresWorkerStore(pool).recordRunWorker(RUN_ID, deps.store.workerAtRun.get(RUN_ID)!), true);
    const sent = JSON.stringify(pool.statements);

    // And the worker's own start, with the fakes bullmq.test.ts uses: this lane's line.
    const lines: string[] = [];
    const modules: WorkerModules = { bull: fakeBull, pg: { Pool: DeadPool as unknown as WorkerModules["pg"]["Pool"] } };
    assert.equal(await workerCommand(env, { load: async () => modules, until: Promise.resolve(), log: (l) => lines.push(l), claudeCheck: async () => ({ ok: true, detail: "ok" }), lintTempBase: temp() }), 0);
    const ours = lines.filter((l) => l.startsWith("Each run records"));
    assert.equal(ours.length, 1);
    assert.match(ours[0]!, /not given by the host/);

    const laneLogged = [...ours, ...deps.lines.filter((l) => /switches and build/.test(l))].join("\n");
    for (const [name, value] of canaries(env)) {
      for (const [where, text] of [["the memory store", stored], ["the Postgres statement", sent], ["the log", laneLogged]] as const) {
        assert.ok(!text.includes(value), `${name}'s value is in ${where}`);
        // Even a part of one: the tag that makes it distinctive.
        const t = value.match(/CANARY[A-Z0-9]+/)?.[0];
        if (t) assert.ok(!text.includes(t), `${name}'s canary is in ${where}`);
      }
    }
    assert.doesNotMatch(stored + sent + laneLogged, /CANARY/);
  });
});

class Pool implements SqlPool {
  readonly statements: Array<{ text: string; values: unknown[] }> = [];
  readonly fail: string | undefined;
  constructor(fail?: string) {
    this.fail = fail;
  }
  async query(text: string, values: unknown[] = []): Promise<SqlResult> {
    this.statements.push({ text, values });
    if (this.fail) throw Object.assign(new Error(`refused: ${String(values[1])}`), { code: this.fail });
    return { rows: [], rowCount: 1 };
  }
  async connect(): Promise<never> {
    throw new Error("not used");
  }
}
class DeadPool {
  async query(): Promise<never> {
    throw new Error("no database in tests");
  }
  async connect(): Promise<never> {
    throw new Error("no database in tests");
  }
  async end(): Promise<void> {}
}
class FakeWorker implements QueueWorker {
  on(): this {
    return this;
  }
  async close(): Promise<void> {}
}
const fakeBull: BullModule = {
  Worker: FakeWorker as unknown as BullModule["Worker"],
  Queue: class {
    async add() {}
    async close() {}
  } as unknown as BullModule["Queue"],
  UnrecoverableError: class extends Error {},
};

describe("the column may not be there yet: the run goes on either way (DECISIONS.md 774)", () => {
  const ROW = atRunRow(atRunOf({ HC_STAGE_NAMES: "on", HC_AREA_REASONS: "On", RENDER_GIT_COMMIT: "97ec3a8" }), "2026-11-26T10:00:00.000Z");

  test("the Postgres store: one UPDATE of the one column, the row as JSON", async () => {
    const pool = new Pool();
    assert.equal(await new PostgresWorkerStore(pool).recordRunWorker(RUN_ID, ROW), true);
    assert.deepEqual(pool.statements, [{ text: RUN_WORKER_SQL, values: [RUN_ID, JSON.stringify(ROW)] }]);
    assert.equal(RUN_WORKER_SQL, `UPDATE "HealthCheckRun" SET "worker" = $2::jsonb WHERE "id" = $1`);
  });

  test("no column (42703): false, never throws, said once per worker process with the migration, never a value", async () => {
    const lines: string[] = [];
    const store = new PostgresWorkerStore(new Pool("42703"), { log: (l) => lines.push(l) });
    for (let i = 0; i < 3; i++) assert.equal(await store.recordRunWorker(RUN_ID, ROW), false);
    assert.equal(lines.length, 1);
    assert.match(lines[0]!, /no "worker" column yet[\s\S]*20261221_health_check_run_worker\.sql/);
    assert.doesNotMatch(lines[0]!, /97ec3a8|set_not_on|refused/);
  });

  test("any other error: false, never throws, said by its SQLSTATE alone", async () => {
    for (const code of ["57014", "42501", "08006", undefined]) {
      const lines: string[] = [];
      const pool = code === undefined ? Object.assign(new Pool(), { query: async () => Promise.reject(new Error("socket closed 97ec3a8")) }) : new Pool(code);
      assert.equal(await new PostgresWorkerStore(pool, { log: (l) => lines.push(l) }).recordRunWorker(RUN_ID, ROW), false);
      assert.equal(lines.length, 1);
      assert.doesNotMatch(lines[0]!, /97ec3a8|refused|socket/);
    }
  });

  test("a row the console couldn't read is never sent", async () => {
    const pool = new Pool();
    const bad = { ...ROW, switches: { ...ROW.switches, extra: { on: true, state: "on" } } } as unknown as WorkerAtRun;
    assert.equal(await new PostgresWorkerStore(pool, { log: () => {} }).recordRunWorker(RUN_ID, bad), false);
    assert.equal(pool.statements.length, 0);
  });

  test("nothing else the Postgres store writes names the column", () => {
    const source = readFileSync(new URL("../postgres-store.ts", import.meta.url), "utf8");
    // A step whose id is "worker" is a row's value, not the column.
    assert.equal(source.match(/"worker"\s*=/g)?.length, 1, "RUN_WORKER_SQL alone sets it");
    assert.doesNotMatch(source, /SELECT[^`]*"worker"/, "and the worker never reads it");
  });

  async function runWith(setup: (deps: ReturnType<typeof baseDeps>) => void, atRun = true) {
    const deps = baseDeps({ dataTransport: new ReplayTransport(loadRecording()), ...(atRun ? { atRun: atRunOf({ HC_STAGE_NAMES: "on", HC_AREA_REASONS: "On", RENDER_GIT_COMMIT: "97ec3a8" }) } : {}) });
    deps.store.addRun(runInput({ run: consoleRun() }));
    await deps.tokens.save(syntheticConnection());
    setup(deps);
    const outcome = await processRunJob(RUN_ID, deps, { final: false });
    return { deps, outcome };
  }
  const dumpOf = (deps: ReturnType<typeof baseDeps>) =>
    JSON.stringify({ ...deps.store, workerAtRun: undefined, workerColumn: undefined, recordRunWorker: undefined, deliveries: deps.store.deliveries.map((d) => ({ ...d, steps: d.steps.map((s) => ({ ...s, ms: 0 })) })) }, (_k, v) => (v instanceof Map ? [...v] : v instanceof Set ? [...v] : v instanceof Buffer ? "<buf>" : v))
      .replace(/\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z/g, (t) => (Math.abs(Date.parse(t) - Date.now()) < 36e5 ? "<NOW>" : t))
      .split(deps.runsDir)
      .join("<RUNS>");

  test("with the column: recorded, and the run is as it was without it", async () => {
    const { deps, outcome } = await runWith(() => {});
    assert.equal(outcome.outcome, "done");
    const row = deps.store.workerAtRun.get(RUN_ID)!;
    assert.equal(row.build, "97ec3a8");
    assert.deepEqual(row.switches.stageNames, { on: true, state: "on" });
    assert.deepEqual(row.switches.areaReasons, { on: false, state: "set_not_on" });
    assert.deepEqual(row.switches.leadJourney, { on: false, state: "unset" });
    const without = await runWith(() => {}, false);
    assert.equal(without.deps.store.workerAtRun.size, 0);
    assert.deepEqual(without.outcome, outcome);
    assert.equal(dumpOf(deps), dumpOf(without.deps));
  });

  test("without the column, or a store that throws, or one that hangs: the run is done just the same", async () => {
    const plain = await runWith(() => {}, false);
    for (const [name, setup] of [
      ["no column", (d: ReturnType<typeof baseDeps>) => void (d.store.workerColumn = false)],
      ["throws 42703", (d: ReturnType<typeof baseDeps>) => void (d.store.recordRunWorker = async () => Promise.reject(Object.assign(new Error("column \"worker\" does not exist"), { code: "42703" })))],
      ["throws at once", (d: ReturnType<typeof baseDeps>) => void (d.store.recordRunWorker = (() => { throw new Error("boom"); }) as never)],
      ["hangs", (d: ReturnType<typeof baseDeps>) => void ((d.store.recordRunWorker = () => new Promise<boolean>(() => {})), (d.deliveryWriteMs = 20))],
    ] as const) {
      const { deps, outcome } = await runWith(setup);
      assert.equal(outcome.outcome, "done", name);
      assert.deepEqual(outcome, plain.outcome, name);
      assert.equal(deps.store.workerAtRun.size, 0, name);
      assert.equal(dumpOf(deps), dumpOf(plain.deps), name);
      assert.ok(!deps.lines.join("\n").includes("does not exist"), name);
    }
  });
});
