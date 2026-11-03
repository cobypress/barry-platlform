import { createCipheriv, createDecipheriv, randomBytes } from "node:crypto";

// The console keeps two secrets for a connect link between the admin's return
// from Salesforce and the worker's code exchange: the pending authorisation
// (it holds the PKCE verifier) and the authorisation code. It seals both with
// HC_CONNECT_KEY, and only the worker opens them. The format is fixed by the
// contract with the console (lane C2), so both sides test the same vector:
//
//   v1.<iv base64url>.<tag base64url>.<ciphertext base64url>
//
// AES-256-GCM, a 12-byte IV, a 16-byte tag, and the link id (UTF-8) as the
// associated data, so a secret copied onto another link won't open.

export const CONNECT_KEY_VAR = "HC_CONNECT_KEY";

const B64URL = /^[A-Za-z0-9_-]*$/;

export class ConnectSecretError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ConnectSecretError";
  }
}

/** The 32-byte key from its base64 form. The message never repeats the value. */
export function connectKey(raw: string | undefined): Buffer {
  if (!raw || !raw.trim()) throw new ConnectSecretError(`${CONNECT_KEY_VAR} is not set`);
  const key = Buffer.from(raw.trim(), "base64");
  if (key.length !== 32) throw new ConnectSecretError(`${CONNECT_KEY_VAR} must be 32 bytes, base64-encoded`);
  return key;
}

function assertKey(key: Buffer): void {
  if (key.length !== 32) throw new ConnectSecretError("The connect key must be 32 bytes");
}

/** `iv` is for the fixed test vector only. Everything else lets it default. */
export function sealConnectSecret(plaintext: string, key: Buffer, linkId: string, iv: Buffer = randomBytes(12)): string {
  assertKey(key);
  if (iv.length !== 12) throw new ConnectSecretError("The IV must be 12 bytes");
  if (!linkId) throw new ConnectSecretError("A link id is needed as associated data");
  const cipher = createCipheriv("aes-256-gcm", key, iv);
  cipher.setAAD(Buffer.from(linkId, "utf8"));
  const data = Buffer.concat([cipher.update(plaintext, "utf8"), cipher.final()]);
  return ["v1", iv.toString("base64url"), cipher.getAuthTag().toString("base64url"), data.toString("base64url")].join(".");
}

/** Throws ConnectSecretError, saying only which check failed, never what was in it. */
export function openConnectSecret(sealed: string, key: Buffer, linkId: string): string {
  assertKey(key);
  const parts = sealed.split(".");
  if (parts.length !== 4 || parts[0] !== "v1") throw new ConnectSecretError("Not a v1 sealed secret");
  const [, ivText, tagText, dataText] = parts as [string, string, string, string];
  if (![ivText, tagText, dataText].every((p) => B64URL.test(p))) throw new ConnectSecretError("A sealed secret part isn't base64url");
  const iv = Buffer.from(ivText, "base64url");
  const tag = Buffer.from(tagText, "base64url");
  if (iv.length !== 12) throw new ConnectSecretError("The sealed secret's IV isn't 12 bytes");
  if (tag.length !== 16) throw new ConnectSecretError("The sealed secret's tag isn't 16 bytes");
  try {
    const decipher = createDecipheriv("aes-256-gcm", key, iv);
    decipher.setAAD(Buffer.from(linkId, "utf8"));
    decipher.setAuthTag(tag);
    return Buffer.concat([decipher.update(Buffer.from(dataText, "base64url")), decipher.final()]).toString("utf8");
  } catch {
    // Wrong key, a tampered value, or one sealed for another link.
    throw new ConnectSecretError("The sealed secret couldn't be opened with this key for this link");
  }
}
