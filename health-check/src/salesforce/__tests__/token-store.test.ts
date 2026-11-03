import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { chmodSync, existsSync, mkdtempSync, readdirSync, readFileSync, renameSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createKeychainKey, KEYCHAIN_SERVICE, tokenKeySource, TokenStore, type StoredConnection } from "../token-store.ts";

const key = randomBytes(32);
const store = (dir = mkdtempSync(join(tmpdir(), "hc-tokens-")), k = key) => new TokenStore({ dir: join(dir, "connections"), key: async () => k });

const connection = (name = "acme"): StoredConnection => ({
  version: 1,
  name,
  grant: "web-server",
  loginUrl: "https://login.salesforce.com",
  clientId: "3MVG9fake",
  instanceUrl: "https://acme.my.salesforce.com",
  orgId: "00D000000000001AAA",
  userId: "005000000000001AAA",
  scope: ["api", "refresh_token"],
  accessToken: "00D000000000001!ACCESS-SECRET",
  refreshToken: "5Aep-REFRESH-SECRET",
  issuedAt: 1_700_000_000_000,
  connectedAt: "2026-11-03T12:00:00.000Z",
});

describe("TokenStore", () => {
  test("round-trips a connection", async () => {
    const s = store();
    await s.save(connection());
    assert.deepEqual(await s.load("acme"), connection());
  });

  test("the file holds no token, and only the owner can read it", async () => {
    const s = store();
    await s.save(connection());
    const path = join(s.dir, "acme.json");
    const text = readFileSync(path, "utf8");
    assert.ok(!text.includes("SECRET"));
    assert.ok(!text.includes("acme.my.salesforce.com"), "even the instance is inside the ciphertext");
    assert.equal(statSync(path).mode & 0o777, 0o600);
    assert.equal(statSync(s.dir).mode & 0o777, 0o700);
    assert.deepEqual(readdirSync(s.dir), ["acme.json"], "no temporary file left behind");
  });

  test("the wrong key can't read it", async () => {
    const dir = mkdtempSync(join(tmpdir(), "hc-tokens-"));
    await store(dir).save(connection());
    await assert.rejects(store(dir, randomBytes(32)).load("acme"), /couldn't be decrypted/);
  });

  test("a file renamed to another connection's name won't open", async () => {
    const s = store();
    await s.save(connection("acme"));
    renameSync(join(s.dir, "acme.json"), join(s.dir, "other.json"));
    await assert.rejects(s.load("other"), /couldn't be decrypted/);
  });

  test("a tampered file won't open", async () => {
    const s = store();
    await s.save(connection());
    const path = join(s.dir, "acme.json");
    const envelope = JSON.parse(readFileSync(path, "utf8")) as { data: string };
    const bytes = Buffer.from(envelope.data, "base64");
    bytes[0]! ^= 1;
    const { writeFileSync } = await import("node:fs");
    writeFileSync(path, JSON.stringify({ ...envelope, data: bytes.toString("base64") }), { mode: 0o600 });
    await assert.rejects(s.load("acme"), /couldn't be decrypted/);
  });

  test("a file other users can read is refused", async () => {
    const s = store();
    await s.save(connection());
    chmodSync(join(s.dir, "acme.json"), 0o644);
    await assert.rejects(s.load("acme"), /readable by other users/);
  });

  test("names can't reach outside the folder", async () => {
    const s = store();
    await assert.rejects(s.load("../runs/x"));
    assert.throws(() => s.has("ACME"));
  });

  test("a pending authorisation is read once, even if reading fails", async () => {
    const dir = mkdtempSync(join(tmpdir(), "hc-tokens-"));
    await store(dir).savePending("acme", { verifier: "v" });
    assert.deepEqual(await store(dir).takePending("acme"), { verifier: "v" });
    assert.equal(await store(dir).takePending("acme"), undefined);
    await store(dir).savePending("acme", { verifier: "v" });
    await assert.rejects(store(dir, randomBytes(32)).takePending("acme"));
    assert.equal(existsSync(join(dir, "connections", "acme.pending.json")), false);
  });

  test("a pending file can't stand in for a connection", async () => {
    const s = store();
    await s.savePending("acme", connection());
    renameSync(join(s.dir, "acme.pending.json"), join(s.dir, "acme.json"));
    await assert.rejects(s.load("acme"), /couldn't be decrypted/);
  });
});

describe("the token key", () => {
  test("comes from HC_TOKEN_KEY first", async () => {
    let called = false;
    const k = await tokenKeySource({ env: { HC_TOKEN_KEY: key.toString("base64") } as NodeJS.ProcessEnv, platform: "darwin", exec: async () => ((called = true), "") })();
    assert.deepEqual(k, key);
    assert.equal(called, false);
  });

  test("must be 32 bytes", async () => {
    await assert.rejects(tokenKeySource({ env: { HC_TOKEN_KEY: randomBytes(16).toString("base64") } as NodeJS.ProcessEnv })(), /32 bytes/);
  });

  test("otherwise comes from the macOS keychain", async () => {
    let args: string[] = [];
    const k = await tokenKeySource({ env: {} as NodeJS.ProcessEnv, platform: "darwin", exec: async (a) => ((args = a), `${key.toString("base64")}\n`) })();
    assert.deepEqual(k, key);
    assert.deepEqual(args.slice(0, 3), ["find-generic-password", "-s", KEYCHAIN_SERVICE]);
  });

  test("off macOS, HC_TOKEN_KEY is required", async () => {
    await assert.rejects(tokenKeySource({ env: {} as NodeJS.ProcessEnv, platform: "linux" })(), /HC_TOKEN_KEY/);
  });

  test("a missing keychain item says how to make one", async () => {
    await assert.rejects(tokenKeySource({ env: {} as NodeJS.ProcessEnv, platform: "darwin", exec: async () => { throw new Error("not found"); } })(), /hc connect key/);
  });

  test("hc connect key makes a 32-byte key once", async () => {
    const calls: string[][] = [];
    assert.equal(await createKeychainKey(async (a) => (calls.push(a), "")), "exists");
    assert.equal(calls.length, 1);
    const made: string[][] = [];
    const result = await createKeychainKey(async (a) => {
      made.push(a);
      if (a[0] === "find-generic-password") throw new Error("not found");
      return "";
    });
    assert.equal(result, "created");
    assert.equal(Buffer.from(made[1]!.at(-1)!, "base64").length, 32);
  });
});
