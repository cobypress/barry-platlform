import { randomBytes } from "node:crypto";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SALES_CLOUD_MODULES } from "../../domains/index.ts";
import type { Intake } from "../../narrative/intake.ts";
import type { OAuthClient } from "../../salesforce/oauth.ts";
import { TokenStore, type StoredConnection } from "../../salesforce/token-store.ts";
import type { Transport, TransportRequest, TransportResponse } from "../../salesforce/transport.ts";
import { Run } from "../../schemas/index.ts";
import { FAKE } from "../../testing/fake-oauth.ts";
import { loadCalibration } from "../../testing/replay.ts";
import { AS_OF, ORG_ID } from "../../testing/synthetic-org.ts";
import type { WorkerDeps } from "../deps.ts";
import { MemoryWorkerStore } from "../memory-store.ts";
import type { RunJobInput } from "../types.ts";

export const temp = (prefix = "hc-worker-"): string => mkdtempSync(join(tmpdir(), prefix));

export const oauthClient: OAuthClient = { clientId: FAKE.clientId, clientSecret: FAKE.clientSecret, redirectUri: FAKE.redirectUri };

export const RUN_ID = "hc-2026-10-31-00000001";
export const CONNECTION = "synthetic-co";
export const ACCESS = `${ORG_ID.slice(0, 15)}!syntheticaccesstoken000000000001`;
export const REFRESH = "5Aepsyntheticrefreshtoken000000000001";

export function syntheticConnection(overrides: Partial<StoredConnection> = {}): StoredConnection {
  return {
    version: 1,
    name: CONNECTION,
    grant: "web-server",
    loginUrl: FAKE.loginUrl,
    clientId: FAKE.clientId,
    instanceUrl: "https://synthetic.example.com",
    orgId: ORG_ID,
    userId: "005000000000001AAA",
    scope: ["api", "refresh_token"],
    accessToken: ACCESS,
    refreshToken: REFRESH,
    issuedAt: Date.parse(AS_OF),
    connectedAt: "2026-10-30T09:00:00.000Z",
    ...overrides,
  };
}

/** A run as the console's Postgres store creates it: waiting to extract, nothing measured. */
export function consoleRun(overrides: Partial<Run> = {}): Run {
  return Run.parse({
    id: RUN_ID,
    product: "barometer",
    customerRef: "SYN-0001",
    orgId: ORG_ID,
    isSandbox: false,
    connectionKind: "oauth",
    status: "extracting",
    modules: SALES_CLOUD_MODULES.map((m) => m.id),
    ruleSetVersion: "not-started",
    calibrationHash: "unconfirmed",
    baselineRunId: null,
    asOf: AS_OF,
    analysisWindow: { from: "2024-10-31", to: "2026-10-31" },
    apiVersion: "not-started",
    budget: { stopAtShare: 0.5, dailyLimit: null, usedAtStart: null, usedAtLastCheck: null, callsMade: 0, stoppedReason: null },
    steps: [{ id: "extract", version: 1, status: "pending", calls: 0, finishedAt: null, error: null }],
    benchmarkOptIn: true,
    createdBy: "consultant@example.com",
    startedAt: "2026-10-31T09:00:00.000Z",
    finishedAt: null,
    ...overrides,
  });
}

export const INTAKE: Intake = {
  summary: "Forecasts are unreliable.",
  statedProblems: [{ id: "p1", statement: "Forecasts are unreliable.", findingIds: [] }],
  capturedBy: "consultant@example.com",
  capturedAt: "2026-10-31T09:00:00.000Z",
};

export function runInput(overrides: Partial<RunJobInput> = {}): RunJobInput {
  return { run: consoleRun(), intake: INTAKE, calibration: loadCalibration(), connectionName: CONNECTION, answers: [], ...overrides };
}

export function tokenStore(): TokenStore {
  const key = randomBytes(32);
  return new TokenStore({ dir: join(temp("hc-worker-tokens-"), "connections"), key: async () => key });
}

export function baseDeps(overrides: Partial<WorkerDeps> = {}): WorkerDeps & { store: MemoryWorkerStore; lines: string[] } {
  const lines: string[] = [];
  const root = temp();
  return {
    store: new MemoryWorkerStore(),
    tokens: tokenStore(),
    runsDir: join(root, "runs"),
    locksDir: join(root, "locks"),
    oauthClient,
    connectKey: randomBytes(32),
    now: () => new Date(AS_OF),
    log: (line) => lines.push(line),
    lines,
    ...overrides,
  } as WorkerDeps & { store: MemoryWorkerStore; lines: string[] };
}

/** Passes requests through until `after` have gone, then fails like a dropped network. */
export class FlakyTransport implements Transport {
  readonly #inner: Transport;
  #left: number;
  constructor(inner: Transport, after: number) {
    this.#inner = inner;
    this.#left = after;
  }
  async send(request: TransportRequest): Promise<TransportResponse> {
    if (this.#left-- <= 0) throw new Error("fetch failed: ECONNRESET");
    return this.#inner.send(request);
  }
}
