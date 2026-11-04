import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { randomBytes, webcrypto } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { connectKey, ConnectSecretError, openConnectSecret, sealConnectSecret } from "../connect-crypto.ts";
import { PendingAuthorisation } from "../../schemas/connect.ts";

type Vector = {
  keyBase64: string;
  cases: Array<{ name: string; linkId: string; ivBase64url: string; plaintext: string; sealed: string }>;
  mustFail: Array<{ name: string; linkId: string; sealed: string }>;
};

const vector = JSON.parse(readFileSync(fileURLToPath(new URL("../../../docs/overnight/night-2/connect-crypto-vector.json", import.meta.url)), "utf8")) as Vector;
const key = Buffer.from(vector.keyBase64, "base64");
const LINK = "00000000-0000-4000-8000-000000000001";

describe("the fixed vector shared with the console", () => {
  test("every case opens to its plaintext", () => {
    for (const c of vector.cases) assert.equal(openConnectSecret(c.sealed, key, c.linkId), c.plaintext, c.name);
  });

  test("sealing with the case's IV gives exactly the sealed text", () => {
    for (const c of vector.cases) assert.equal(sealConnectSecret(c.plaintext, key, c.linkId, Buffer.from(c.ivBase64url, "base64url")), c.sealed, c.name);
  });

  test("WebCrypto, a separate implementation path, agrees", async () => {
    const k = await webcrypto.subtle.importKey("raw", key, "AES-GCM", false, ["decrypt"]);
    for (const c of vector.cases) {
      const [, iv, tag, data] = c.sealed.split(".") as [string, string, string, string];
      const joined = Buffer.concat([Buffer.from(data, "base64url"), Buffer.from(tag, "base64url")]);
      const plain = await webcrypto.subtle.decrypt({ name: "AES-GCM", iv: Buffer.from(iv, "base64url"), additionalData: Buffer.from(c.linkId, "utf8"), tagLength: 128 }, k, joined);
      assert.equal(Buffer.from(plain).toString("utf8"), c.plaintext, c.name);
    }
  });

  test("the pending case is a real PendingAuthorisation", () => {
    const pending = vector.cases.find((c) => c.name === "pending authorisation")!;
    assert.ok(PendingAuthorisation.parse(JSON.parse(pending.plaintext)));
  });

  test("the must-fail cases fail", () => {
    for (const c of vector.mustFail) assert.throws(() => openConnectSecret(c.sealed, key, c.linkId), ConnectSecretError, c.name);
  });
});

describe("sealing and opening", () => {
  test("round trips, with a fresh IV each time", () => {
    const a = sealConnectSecret("aPrx-code", key, LINK);
    const b = sealConnectSecret("aPrx-code", key, LINK);
    assert.notEqual(a, b);
    assert.equal(openConnectSecret(a, key, LINK), "aPrx-code");
    assert.match(a, /^v1\.[A-Za-z0-9_-]{16}\.[A-Za-z0-9_-]{22}\.[A-Za-z0-9_-]+$/);
  });

  test("refuses another key, another link, and any change to any part", () => {
    const sealed = sealConnectSecret("aPrx-code", key, LINK);
    assert.throws(() => openConnectSecret(sealed, randomBytes(32), LINK), /couldn't be opened/);
    assert.throws(() => openConnectSecret(sealed, key, "00000000-0000-4000-8000-000000000009"), /couldn't be opened/);
    const parts = sealed.split(".");
    for (const i of [1, 2, 3]) {
      const changed = [...parts];
      const bytes = Buffer.from(changed[i]!, "base64url");
      bytes[0]! ^= 1;
      changed[i] = bytes.toString("base64url");
      assert.throws(() => openConnectSecret(changed.join("."), key, LINK), ConnectSecretError, `part ${i}`);
    }
  });

  test("refuses the wrong shape before trying to decrypt", () => {
    assert.throws(() => openConnectSecret("v2.a.b.c", key, LINK), /Not a v1/);
    assert.throws(() => openConnectSecret("v1.a.b", key, LINK), /Not a v1/);
    assert.throws(() => openConnectSecret("v1.AAECAwQFBgcICQoL.a+b.c", key, LINK), /base64url/);
    assert.throws(() => openConnectSecret("v1.AAEC.tCYCnwgGg7znC-Oj76_j3A.AA", key, LINK), /IV isn't 12/);
    assert.throws(() => openConnectSecret("v1.AAECAwQFBgcICQoL.AAEC.AA", key, LINK), /tag isn't 16/);
  });

  test("an error never repeats the secret or the key", () => {
    const sealed = sealConnectSecret("aPrxSECRETCODE", key, LINK);
    try {
      openConnectSecret(sealed, randomBytes(32), LINK);
      assert.fail("should throw");
    } catch (err) {
      const text = String(err);
      assert.ok(!text.includes("aPrxSECRETCODE") && !text.includes(sealed) && !text.includes(vector.keyBase64));
    }
  });
});

describe("connectKey", () => {
  test("takes 32 bytes of base64, and says what's wrong without the value", () => {
    assert.equal(connectKey(vector.keyBase64).length, 32);
    assert.throws(() => connectKey(undefined), /HC_CONNECT_KEY is not set/);
    assert.throws(() => connectKey("  "), /is not set/);
    const short = randomBytes(16).toString("base64");
    assert.throws(() => connectKey(short), (err: Error) => /32 bytes/.test(err.message) && !err.message.includes(short));
  });
});
