import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { generateKeyPairSync } from "node:crypto";
import { readFileSync } from "node:fs";
import { GoogleDrive, READER_PERMISSION, type FolderSharing } from "../drive.ts";
import { MemoryWorkerStore } from "../memory-store.ts";
import { PostgresWorkerStore, readersFromRow, shareFromRow, type SqlClient, type SqlPool, type SqlResult } from "../postgres-store.ts";
import { processShareJob, syncShares, type ShareDeps } from "../share-job.ts";
import { parseJob, type DocumentsJobInput } from "../types.ts";
import { baseDeps, consoleRun } from "./helpers.ts";

// Sharing a released run's Drive folder with its readers (DECISIONS.md 251 to 255).

const RUN = "hc-2026-11-01-0000000a";
const FOLDER = "folder-0000000001";
const T = new Date("2026-11-06T10:00:00.000Z");

/** Drive's permissions, as plain records. `fail` makes the next calls for an address throw. */
class FakeSharing implements FolderSharing {
  readonly permissions = new Map<string, { folderId: string; email: string }>();
  readonly calls: string[] = [];
  readonly fail = new Set<string>();
  /** Drive makes the share, and its answer is lost on the way back. */
  readonly lose = new Set<string>();
  #next = 1;
  async share(folderId: string, email: string): Promise<string> {
    this.calls.push(`share ${folderId} ${email}`);
    if (this.fail.has(email)) throw new Error("Google Drive refused a request (403): Sharing outside the organisation is off");
    const id = `perm-${String(this.#next++).padStart(4, "0")}`;
    this.permissions.set(id, { folderId, email });
    if (this.lose.has(email)) throw new Error("fetch failed: socket hang up");
    return id;
  }
  async findReader(folderId: string, email: string): Promise<string | null> {
    this.calls.push(`find ${folderId} ${email}`);
    if (this.fail.has(email)) throw new Error("Google Drive refused a request (500): backend error");
    return [...this.permissions.entries()].find(([, p]) => p.folderId === folderId && p.email === email)?.[0] ?? null;
  }
  async unshare(folderId: string, permissionId: string): Promise<void> {
    this.calls.push(`unshare ${folderId} ${permissionId}`);
    const p = this.permissions.get(permissionId);
    if (p && this.fail.has(p.email)) throw new Error("Google Drive refused a request (500): backend error");
    this.permissions.delete(permissionId);
  }
  who(folderId = FOLDER): string[] {
    return [...this.permissions.values()].filter((p) => p.folderId === folderId).map((p) => p.email).sort();
  }
}

function setup(options: { status?: "released" | "in_review"; folderId?: string | null; readers?: string[] } = {}) {
  const store = new MemoryWorkerStore();
  const input = { run: consoleRun({ id: RUN, status: options.status ?? "released" }), driveFolderId: options.folderId === undefined ? FOLDER : options.folderId, files: [], state: "done" } as unknown as DocumentsJobInput;
  store.addDocumentsJob(input);
  store.readers.set(RUN, options.readers ?? ["ceo@customer.example.com", "Ops@Customer.example.com"]);
  const sharing = new FakeSharing();
  const lines: string[] = [];
  const deps: ShareDeps = { store, sharing, now: () => T, log: (l) => lines.push(l) };
  return { store, sharing, deps, lines };
}

describe("sharing a run's folder with its readers", () => {
  test("a released run's folder goes to each reader, as a reader, and each share is recorded", async () => {
    const { store, sharing, deps, lines } = setup();
    const result = await syncShares(RUN, deps);
    assert.deepEqual([result.shared, result.removed, result.failed], [2, 0, 0]);
    assert.deepEqual(sharing.who(), ["ceo@customer.example.com", "ops@customer.example.com"], "addresses are lower-cased");
    assert.deepEqual(store.shares.map((s) => [s.email, s.folderId, s.state, s.sharedAt, s.permissionId !== null]), [
      ["ceo@customer.example.com", FOLDER, "shared", T.toISOString(), true],
      ["ops@customer.example.com", FOLDER, "shared", T.toISOString(), true],
    ]);
    // Counts only in the log: an address is a person's.
    assert.ok(lines.every((l) => !l.includes("@")));
    // Run again, nothing changes.
    assert.deepEqual([(await syncShares(RUN, deps)).shared, sharing.calls.length], [0, 2]);
  });

  test("never before release (rule 10), and not without a folder", async () => {
    for (const options of [{ status: "in_review" as const }, { folderId: null }]) {
      const { store, sharing, deps } = setup(options);
      await syncShares(RUN, deps);
      assert.deepEqual([sharing.calls, store.shares], [[], []]);
    }
  });

  test("taking a reader off the list takes their access away, and adding them back shares it again", async () => {
    const { store, sharing, deps } = setup();
    await syncShares(RUN, deps);
    store.readers.set(RUN, ["ceo@customer.example.com"]);
    assert.equal((await syncShares(RUN, deps)).removed, 1);
    assert.deepEqual(sharing.who(), ["ceo@customer.example.com"]);
    const ops = store.shares.find((s) => s.email === "ops@customer.example.com")!;
    assert.deepEqual([ops.state, ops.removedWhy, ops.removedAt], ["removed", "reader_removed", T.toISOString()]);
    store.readers.set(RUN, ["ceo@customer.example.com", "ops@customer.example.com"]);
    await syncShares(RUN, deps);
    assert.equal(store.shares.find((s) => s.email === "ops@customer.example.com")!.state, "shared");
    assert.equal(store.shares.length, 2, "one row a run, folder and address");
  });

  test("a new folder is shared, and the old one's access is taken away", async () => {
    const { store, sharing, deps } = setup({ readers: ["ceo@customer.example.com"] });
    await syncShares(RUN, deps);
    store.documents.get(RUN)!.input.driveFolderId = "folder-0000000002";
    await syncShares(RUN, deps);
    assert.deepEqual([sharing.who(FOLDER), sharing.who("folder-0000000002")], [[], ["ceo@customer.example.com"]]);
    assert.equal(store.shares.find((s) => s.folderId === FOLDER)!.removedWhy, "folder_changed");
  });

  test("a share Drive refuses is recorded as failed with the reason, and tried again", async () => {
    const { store, sharing, deps } = setup({ readers: ["ceo@customer.example.com"] });
    sharing.fail.add("ceo@customer.example.com");
    assert.equal((await syncShares(RUN, deps)).failed, 1);
    assert.deepEqual([store.shares[0]!.state, store.shares[0]!.error], ["failed", "Google Drive refused a request (403): Sharing outside the organisation is off"]);
    sharing.fail.clear();
    await syncShares(RUN, deps);
    assert.deepEqual([store.shares[0]!.state, store.shares[0]!.error], ["shared", null]);
  });

  test("access that can't be taken away stays recorded as shared, with why", async () => {
    const { store, sharing, deps } = setup({ readers: ["ceo@customer.example.com"] });
    await syncShares(RUN, deps);
    sharing.fail.add("ceo@customer.example.com");
    store.readers.set(RUN, []);
    assert.equal((await syncShares(RUN, deps)).failed, 1);
    assert.equal(store.shares[0]!.state, "shared");
    assert.match(store.shares[0]!.error!, /^Couldn't take access away/);
  });

  test("a share Drive made but whose answer was lost is found and taken away when the reader is removed", async () => {
    const { store, sharing, deps } = setup({ readers: ["ceo@customer.example.com"] });
    sharing.lose.add("ceo@customer.example.com");
    assert.equal((await syncShares(RUN, deps)).failed, 1);
    assert.deepEqual([store.shares[0]!.state, store.shares[0]!.permissionId, sharing.who()], ["failed", null, ["ceo@customer.example.com"]], "recorded as failed, but Drive has it");
    store.readers.set(RUN, []);
    assert.equal((await syncShares(RUN, deps)).removed, 1);
    assert.deepEqual(sharing.who(), []);
    assert.deepEqual([store.shares[0]!.state, store.shares[0]!.removedWhy], ["removed", "reader_removed"]);
  });

  test("a failed share Drive never made is closed without taking anything, and one that can't be checked stays, with why", async () => {
    const { store, sharing, deps } = setup({ readers: ["ceo@customer.example.com", "ops@customer.example.com"] });
    sharing.fail.add("ceo@customer.example.com");
    sharing.fail.add("ops@customer.example.com");
    await syncShares(RUN, deps);
    store.readers.set(RUN, []);
    sharing.fail.delete("ceo@customer.example.com");
    const result = await syncShares(RUN, deps);
    assert.deepEqual([result.removed, result.failed], [1, 1]);
    const by = (email: string) => store.shares.find((s) => s.email === email)!;
    assert.equal(by("ceo@customer.example.com").state, "removed");
    assert.deepEqual([by("ops@customer.example.com").state, /^Couldn't check whether Drive shared it/.test(by("ops@customer.example.com").error!)], ["failed", true]);
  });

  test("everyone's access goes when asked, whatever the list says", async () => {
    const { store, sharing, deps } = setup();
    await syncShares(RUN, deps);
    await syncShares(RUN, deps, { revokeAll: "retention" });
    assert.deepEqual(sharing.who(), []);
    assert.ok(store.shares.every((s) => s.state === "removed" && s.removedWhy === "retention"));
  });

  test("12 months after release nobody gets the folder again: a later reader change or documents make takes access away instead", async () => {
    const { store, sharing, deps } = setup();
    store.releasedAt.set(RUN, "2025-10-06T09:00:00.000Z");
    assert.deepEqual([(await syncShares(RUN, deps)).shared, sharing.who()], [0, []]);
    // Shared before the 12 months were up, and not yet swept: the next change takes it away, as retention would.
    store.releasedAt.set(RUN, "2025-12-01T09:00:00.000Z");
    await syncShares(RUN, deps);
    assert.equal(sharing.who().length, 2);
    await syncShares(RUN, { ...deps, now: () => new Date("2026-12-01T09:00:00.000Z") });
    assert.deepEqual(sharing.who(), []);
    assert.ok(store.shares.every((s) => s.state === "removed" && s.removedWhy === "retention"));
  });

  test("without the console's share table, or without Drive, nothing is shared", async () => {
    const one = setup();
    one.store.sharesRecordable = false;
    assert.match((await syncShares(RUN, one.deps)).detail, /can't record shares yet/);
    assert.deepEqual(one.sharing.calls, []);
    const two = setup();
    assert.match((await syncShares(RUN, { ...two.deps, sharing: null })).detail, /Google Drive isn't set up/);
    assert.deepEqual(two.store.shares, []);
  });
});

describe("the share job", () => {
  test("takes ids only, and its failure is retried until the last delivery", async () => {
    assert.deepEqual(parseJob({ kind: "share", runId: RUN }), { kind: "share", runId: RUN });
    assert.throws(() => parseJob({ kind: "share", runId: RUN, email: "ceo@customer.example.com" }));
    const deps = baseDeps();
    const { store, sharing } = setup({ readers: ["ceo@customer.example.com"] });
    sharing.fail.add("ceo@customer.example.com");
    const job = { ...deps, store, sharing };
    await assert.rejects(processShareJob(RUN, job, { final: false }));
    assert.equal((await processShareJob(RUN, job, { final: true })).outcome, "failed");
    sharing.fail.clear();
    assert.equal((await processShareJob(RUN, job, { final: true })).outcome, "done");
  });
});

describe("Google Drive's sharing", () => {
  const KEY = generateKeyPairSync("rsa", { modulusLength: 2048 }).privateKey.export({ type: "pkcs8", format: "pem" }).toString();
  const config = { clientEmail: "documents@example.iam.gserviceaccount.com", privateKey: KEY, rootFolderId: "root-folder-0001", subject: null };
  function drive(respond: (url: string, init: RequestInit) => Response) {
    const calls: Array<{ url: string; init: RequestInit }> = [];
    const impl = (async (input: string | URL | Request, init: RequestInit = {}) => {
      const url = String(input);
      calls.push({ url, init });
      if (url === "https://oauth2.googleapis.com/token") return Response.json({ access_token: "ya29.synthetic-token", expires_in: 3600 });
      return respond(url, init);
    }) as typeof fetch;
    return { client: new GoogleDrive(config, impl), calls };
  }

  test("gives one named person read access, emailing them, and nothing wider", async () => {
    const { client, calls } = drive(() => Response.json({ id: "08123456789012345678" }));
    assert.equal(await client.share(FOLDER, "ceo@customer.example.com"), "08123456789012345678");
    const call = calls[1]!;
    const url = new URL(call.url);
    assert.equal(url.pathname, `/drive/v3/files/${FOLDER}/permissions`);
    assert.equal(url.searchParams.get("supportsAllDrives"), "true");
    assert.equal(url.searchParams.get("sendNotificationEmail"), "true");
    assert.deepEqual(JSON.parse(String(call.init.body)), { type: "user", role: "reader", emailAddress: "ceo@customer.example.com" });
    assert.ok(Object.isFrozen(READER_PERMISSION));
  });

  test("refuses anything that isn't an address, and takes a permission away by id", async () => {
    const { client, calls } = drive((_url, init) => (init.method === "DELETE" ? new Response(null, { status: 204 }) : Response.json({})));
    await assert.rejects(client.share(FOLDER, "anyone"));
    await assert.rejects(client.share(FOLDER, "a@b.example.com\"},{\"type\":\"anyone"));
    await client.unshare(FOLDER, "08123456789012345678");
    assert.equal(calls.at(-1)!.init.method, "DELETE");
    assert.equal(new URL(calls.at(-1)!.url).pathname, `/drive/v3/files/${FOLDER}/permissions/08123456789012345678`);
    await assert.rejects(client.unshare(FOLDER, "../files"));
  });

  test("finds a person's reader permission on the folder, page by page, reading only", async () => {
    const pages: Record<string, unknown> = {
      first: { nextPageToken: "page-2", permissions: [{ id: "01111", type: "user", role: "writer", emailAddress: "ceo@customer.example.com" }, { id: "02222", type: "user", role: "reader", emailAddress: "ops@customer.example.com" }] },
      second: { permissions: [{ id: "03333", type: "domain", role: "reader" }, { id: "04444", type: "user", role: "reader", emailAddress: "CEO@Customer.example.com" }] },
    };
    const { client, calls } = drive((url) => Response.json(new URL(url).searchParams.get("pageToken") === "page-2" ? pages.second : pages.first));
    assert.equal(await client.findReader(FOLDER, "ceo@customer.example.com"), "04444", "the reader one, not the writer one");
    const lists = calls.filter((c) => c.url.includes("/permissions"));
    assert.ok(lists.every((c) => (c.init.method ?? "GET") === "GET"));
    assert.equal(new URL(lists[0]!.url).searchParams.get("supportsAllDrives"), "true");
    assert.equal(await client.findReader(FOLDER, "nobody@customer.example.com"), null);
    const gone = drive(() => new Response("{}", { status: 404 }));
    assert.equal(await gone.client.findReader(FOLDER, "ceo@customer.example.com"), null, "a folder that's gone holds nobody's access");
    await assert.rejects(client.findReader("../x", "ceo@customer.example.com"));
  });

  test("a permission already gone is fine", async () => {
    const { client } = drive(() => new Response("{}", { status: 404 }));
    await client.unshare(FOLDER, "08123456789012345678");
  });

  test("the worker's code never makes a domain, group or anyone-with-the-link permission, or more than reader", () => {
    for (const file of ["src/worker/drive.ts", "src/worker/share-job.ts", "src/worker/documents-job.ts", "src/worker/retention.ts"]) {
      const code = readFileSync(file, "utf8").replace(/\/\/.*$/gm, "").replace(/\/\*[\s\S]*?\*\//g, "");
      assert.doesNotMatch(code, /["'](?:anyone|domain|group)["']/, file);
      assert.doesNotMatch(code, /["'](?:writer|commenter|fileOrganizer|organizer|owner)["']/, file);
      assert.doesNotMatch(code, /allowFileDiscovery/, file);
    }
  });
});

describe("the Postgres store, on shares", () => {
  class Pool implements SqlPool {
    readonly statements: Array<{ text: string; values: unknown[] }> = [];
    readonly answer: (text: string) => SqlResult | Error;
    constructor(answer: (text: string) => SqlResult | Error) {
      this.answer = answer;
    }
    async query(text: string, values: unknown[] = []): Promise<SqlResult> {
      this.statements.push({ text, values });
      const a = this.answer(text);
      if (a instanceof Error) throw a;
      return a;
    }
    async connect(): Promise<SqlClient & { release(): void }> {
      return { query: (t, v) => this.query(t, v), release: () => {} };
    }
  }
  const row = { runId: RUN, email: "ceo@customer.example.com", folderId: FOLDER, permissionId: "perm-0001", state: "shared", sharedAt: T.toISOString(), removedAt: null, removedWhy: null, error: null, updatedAt: T.toISOString() };

  test("reads the run, its folder, its readers and its shares", async () => {
    const pool = new Pool((text) => {
      if (text.startsWith(`SELECT "status"`)) return { rows: [{ status: "released" }], rowCount: 1 };
      if (text.includes(`"HealthCheckDocuments"`)) return { rows: [{ driveFolderId: FOLDER }], rowCount: 1 };
      if (text.includes(`"HealthCheckMicrositeAccess"`)) return { rows: [{ readers: [{ email: "CEO@customer.example.com", addedBy: "x", addedAt: "y" }, { nope: 1 }] }], rowCount: 1 };
      if (text.includes(`"HealthCheckDriveShare"`)) return { rows: [row, { ...row, state: "odd" }], rowCount: 2 };
      if (text.includes(`MAX("releasedAt")`) && text.includes(`"HealthCheckRelease"`)) return { rows: [{ releasedAt: "2026-11-01T09:00:00.000Z" }], rowCount: 1 };
      return { rows: [], rowCount: 0 };
    });
    const input = (await new PostgresWorkerStore(pool).loadShareJob(RUN))!;
    assert.deepEqual([input.released, input.releasedAt, input.folderId, input.readers, input.shares.length, input.recordable], [true, "2026-11-01T09:00:00.000Z", FOLDER, ["ceo@customer.example.com"], 1, true]);
    assert.deepEqual(readersFromRow(undefined), []);
    assert.equal(shareFromRow({ ...row, state: "shared", removedWhy: "whatever" })!.removedWhy, null);
  });

  test("a console without the share table records nothing, and so shares nothing", async () => {
    const missing = Object.assign(new Error("relation does not exist"), { code: "42P01" });
    const pool = new Pool((text) => (text.includes(`"HealthCheckDriveShare"`) || text.includes(`"HealthCheckMicrositeAccess"`) ? missing : text.startsWith(`SELECT "status"`) ? { rows: [{ status: "released" }], rowCount: 1 } : { rows: [], rowCount: 0 }));
    assert.equal((await new PostgresWorkerStore(pool).loadShareJob(RUN))!.recordable, false);
  });

  test("writes one row a run, folder and address, replacing what was there", async () => {
    const pool = new Pool(() => ({ rows: [], rowCount: 1 }));
    await new PostgresWorkerStore(pool).saveShare(shareFromRow(row)!);
    const [statement] = pool.statements;
    assert.match(statement!.text, /^INSERT INTO "HealthCheckDriveShare" \("runId", "email", "folderId", "permissionId", "state", "sharedAt", "removedAt", "removedWhy", "error", "updatedAt"\)/);
    assert.match(statement!.text, /ON CONFLICT \("runId", "folderId", "email"\) DO UPDATE SET "permissionId" = EXCLUDED\."permissionId", "state" = EXCLUDED\."state"/);
    assert.equal(statement!.values[1], "ceo@customer.example.com");
  });
});
