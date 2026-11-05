import { createCipheriv, randomBytes } from "node:crypto";

// The customer microsite's pages are sealed here and opened only by the console
// (roadmap decision 26). AES-256-GCM under HC_MICROSITE_KEY, with the run id as
// associated data, as v1.<iv>.<tag>.<ciphertext>, each base64url: the same shape
// as the connect sealing (DECISIONS 70). blackcloud-next's
// docs/health-check/microsite-crypto-vector.json is the fixed vector both sides test.

export const MICROSITE_KEY_VAR = "HC_MICROSITE_KEY";
const VERSION = "v1";
const IV_BYTES = 12;

export class MicrositeKeyError extends Error {}

/** Exactly 32 bytes of base64, and not the same as either connect key. Errors never repeat a value. */
export function micrositeKey(env: Record<string, string | undefined>): Buffer | null {
  const raw = env[MICROSITE_KEY_VAR]?.trim();
  if (!raw) return null;
  if (!/^[A-Za-z0-9+/]+={0,2}$/.test(raw)) throw new MicrositeKeyError(`${MICROSITE_KEY_VAR} isn't base64`);
  const key = Buffer.from(raw, "base64");
  if (key.length !== 32) throw new MicrositeKeyError(`${MICROSITE_KEY_VAR} must be 32 bytes, base64`);
  for (const other of ["HC_CONNECT_KEY", "HC_TOKEN_KEY"]) {
    const value = env[other]?.trim();
    if (value && Buffer.from(value, "base64").equals(key)) throw new MicrositeKeyError(`${MICROSITE_KEY_VAR} must differ from ${other}`);
  }
  return key;
}

/** `iv` is only for the fixed test vector. Everything else gets a fresh random IV. */
export function sealPages(plaintext: string, runId: string, key: Buffer, iv: Buffer = randomBytes(IV_BYTES)): string {
  if (key.length !== 32) throw new MicrositeKeyError("The key must be 32 bytes");
  if (iv.length !== IV_BYTES) throw new Error("The IV must be 12 bytes");
  if (!runId) throw new Error("The run id is needed as associated data");
  const cipher = createCipheriv("aes-256-gcm", key, iv);
  cipher.setAAD(Buffer.from(runId, "utf8"));
  const ciphertext = Buffer.concat([cipher.update(plaintext, "utf8"), cipher.final()]);
  return [VERSION, iv.toString("base64url"), cipher.getAuthTag().toString("base64url"), ciphertext.toString("base64url")].join(".");
}
