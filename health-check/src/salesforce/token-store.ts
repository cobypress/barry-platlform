import { execFile } from "node:child_process";
import { createCipheriv, createDecipheriv, randomBytes } from "node:crypto";
import { chmodSync, existsSync, mkdirSync, readFileSync, renameSync, rmSync, statSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { z } from "zod";

// OAuth tokens at rest. Each connection is one file, encrypted with AES-256-GCM.
// The key never sits beside the files: it comes from HC_TOKEN_KEY (the worker),
// or from the macOS keychain (a consultant's machine). The connection name is
// bound in as associated data, so one org's file can't be renamed into another's.

const execFileAsync = promisify(execFile);

export const KEYCHAIN_SERVICE = "barometer-token-key";
const KEYCHAIN_ACCOUNT = "barometer";
const FORMAT = "barometer-connection";

export const ConnectionName = z.string().regex(/^[a-z0-9][a-z0-9-]{0,62}$/, "Use lower-case letters, digits and hyphens");

export const StoredConnection = z.object({
  version: z.literal(1),
  name: ConnectionName,
  grant: z.enum(["web-server", "jwt-bearer"]),
  loginUrl: z.string().url(),
  clientId: z.string().min(1),
  instanceUrl: z.string().url(),
  orgId: z.string().regex(/^00D[A-Za-z0-9]{12,15}$/),
  userId: z.string().regex(/^005[A-Za-z0-9]{12,15}$/),
  scope: z.array(z.string()),
  accessToken: z.string().min(1),
  refreshToken: z.string().min(1).optional(),
  issuedAt: z.number().int(),
  connectedAt: z.string(),
  /** JWT bearer only. The private key stays in its own file, owner-only. */
  jwt: z.object({ username: z.string().min(1), audience: z.string().url(), privateKeyPath: z.string().min(1) }).optional(),
});
export type StoredConnection = z.infer<typeof StoredConnection>;

const Envelope = z.object({
  format: z.literal(FORMAT),
  version: z.literal(1),
  alg: z.literal("A256GCM"),
  iv: z.string(),
  tag: z.string(),
  data: z.string(),
});

export type KeySource = () => Promise<Buffer>;
export type SecurityExec = (args: string[]) => Promise<string>;

const runSecurity: SecurityExec = async (args) => (await execFileAsync("/usr/bin/security", args)).stdout;

function decodeKey(raw: string, from: string): Buffer {
  const key = Buffer.from(raw.trim(), "base64");
  if (key.length !== 32) throw new Error(`The token key from ${from} must be 32 bytes, base64-encoded`);
  return key;
}

/** HC_TOKEN_KEY if set, otherwise the macOS keychain. Anywhere else, HC_TOKEN_KEY is required. */
export function tokenKeySource(options: { env?: NodeJS.ProcessEnv; platform?: NodeJS.Platform; exec?: SecurityExec } = {}): KeySource {
  const env = options.env ?? process.env;
  const platform = options.platform ?? process.platform;
  const exec = options.exec ?? runSecurity;
  return async () => {
    if (env.HC_TOKEN_KEY) return decodeKey(env.HC_TOKEN_KEY, "HC_TOKEN_KEY");
    if (platform !== "darwin") throw new Error("Set HC_TOKEN_KEY to a base64 32-byte key. The keychain is only used on macOS");
    let raw: string;
    try {
      raw = await exec(["find-generic-password", "-s", KEYCHAIN_SERVICE, "-a", KEYCHAIN_ACCOUNT, "-w"]);
    } catch {
      throw new Error(`No token key in the keychain. Create one with: hc connect key`);
    }
    return decodeKey(raw, "the keychain");
  };
}

/**
 * Makes a key and puts it in the login keychain, unless one is there already.
 * `security` takes the key as an argument, so it's briefly visible to this user's
 * own processes. It never touches disk or the terminal.
 */
export async function createKeychainKey(exec: SecurityExec = runSecurity): Promise<"created" | "exists"> {
  try {
    await exec(["find-generic-password", "-s", KEYCHAIN_SERVICE, "-a", KEYCHAIN_ACCOUNT]);
    return "exists";
  } catch {
    await exec(["add-generic-password", "-s", KEYCHAIN_SERVICE, "-a", KEYCHAIN_ACCOUNT, "-l", "Black Cloud Barometer token key", "-w", randomBytes(32).toString("base64")]);
    return "created";
  }
}

export function encryptJson(value: unknown, key: Buffer, aad: string): string {
  const iv = randomBytes(12);
  const cipher = createCipheriv("aes-256-gcm", key, iv);
  cipher.setAAD(Buffer.from(aad));
  const data = Buffer.concat([cipher.update(JSON.stringify(value), "utf8"), cipher.final()]);
  return JSON.stringify({ format: FORMAT, version: 1, alg: "A256GCM", iv: iv.toString("base64"), tag: cipher.getAuthTag().toString("base64"), data: data.toString("base64") });
}

export function decryptJson(text: string, key: Buffer, aad: string): unknown {
  let envelope: z.infer<typeof Envelope>;
  try {
    envelope = Envelope.parse(JSON.parse(text));
  } catch {
    throw new Error("The stored connection isn't in the expected format");
  }
  try {
    const decipher = createDecipheriv("aes-256-gcm", key, Buffer.from(envelope.iv, "base64"));
    decipher.setAAD(Buffer.from(aad));
    decipher.setAuthTag(Buffer.from(envelope.tag, "base64"));
    return JSON.parse(Buffer.concat([decipher.update(Buffer.from(envelope.data, "base64")), decipher.final()]).toString("utf8"));
  } catch {
    // Wrong key, a tampered file, or one renamed from another connection.
    throw new Error("The stored connection couldn't be decrypted with this key");
  }
}

export function defaultConnectionsDir(): string {
  return join(homedir(), ".barometer", "connections");
}

/** One encrypted file per connection, plus at most one pending authorisation. */
export class TokenStore {
  readonly dir: string;
  readonly #key: KeySource;

  constructor(options: { dir?: string; key: KeySource }) {
    this.dir = options.dir ?? defaultConnectionsDir();
    this.#key = options.key;
  }

  #path(name: string, kind: "connection" | "pending"): string {
    ConnectionName.parse(name);
    return join(this.dir, kind === "connection" ? `${name}.json` : `${name}.pending.json`);
  }

  async #write(path: string, value: unknown, aad: string): Promise<void> {
    const text = encryptJson(value, await this.#key(), aad);
    mkdirSync(this.dir, { recursive: true, mode: 0o700 });
    chmodSync(this.dir, 0o700);
    // Write then rename, so a crash mid-write never leaves half a file. That
    // matters with rotation: the old refresh token is already dead.
    const tmp = `${path}.${randomBytes(6).toString("hex")}.tmp`;
    writeFileSync(tmp, text, { mode: 0o600, flag: "wx" });
    renameSync(tmp, path);
  }

  async #read(path: string, aad: string): Promise<unknown | undefined> {
    if (!existsSync(path)) return undefined;
    if ((statSync(path).mode & 0o077) !== 0) throw new Error(`${path} is readable by other users. Make it 600 first`);
    return decryptJson(readFileSync(path, "utf8"), await this.#key(), aad);
  }

  save(connection: StoredConnection): Promise<void> {
    const parsed = StoredConnection.parse(connection);
    return this.#write(this.#path(parsed.name, "connection"), parsed, `connection:${parsed.name}`);
  }

  async load(name: string): Promise<StoredConnection> {
    const value = await this.#read(this.#path(name, "connection"), `connection:${name}`);
    if (value === undefined) throw new Error(`No stored connection called "${name}"`);
    const parsed = StoredConnection.parse(value);
    if (parsed.name !== name) throw new Error("The stored connection is for a different name");
    return parsed;
  }

  has(name: string): boolean {
    return existsSync(this.#path(name, "connection"));
  }

  delete(name: string): void {
    rmSync(this.#path(name, "connection"), { force: true });
  }

  savePending(name: string, pending: unknown): Promise<void> {
    return this.#write(this.#path(name, "pending"), pending, `pending:${name}`);
  }

  /** Read once: the file is deleted whatever happens next, so a code can't be replayed. */
  async takePending(name: string): Promise<unknown | undefined> {
    const path = this.#path(name, "pending");
    try {
      return await this.#read(path, `pending:${name}`);
    } finally {
      rmSync(path, { force: true });
    }
  }
}
