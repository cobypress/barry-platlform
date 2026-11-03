import { execFile } from "node:child_process";
import { promisify } from "node:util";
import type { ConnectionProvider, OrgConnection } from "./connection.ts";

const execFileAsync = promisify(execFile);

export type SfExec = (args: string[]) => Promise<string>;

export class OrgNotApprovedError extends Error {
  constructor(alias: string) {
    super(
      `Org alias "${alias}" isn't approved. This machine has customer orgs logged in, so an alias must be listed in ` +
        `HC_APPROVED_ORG_ALIASES before anything talks to it. Customer orgs need Coby's say-so first.`,
    );
    this.name = "OrgNotApprovedError";
  }
}

// sf 2.150 puts "[REDACTED] Use 'sf org auth show-access-token' to view" where the token was.
const REDACTED = /redacted/i;

const runSf: SfExec = async (args) => {
  const { stdout } = await execFileAsync("sf", args, { maxBuffer: 1024 * 1024 });
  return stdout;
};

export function approvedAliases(env: NodeJS.ProcessEnv = process.env): string[] {
  return (env.HC_APPROVED_ORG_ALIASES ?? "")
    .split(",")
    .map((a) => a.trim())
    .filter(Boolean);
}

// Development only. Borrows an existing `sf` login on this machine. The approval
// check runs first, because even `sf org display` can refresh a token in the org.
export class SfCliConnectionProvider implements ConnectionProvider {
  readonly kind = "sf-cli";
  readonly #alias: string;
  readonly #exec: SfExec;

  constructor(alias: string, options: { exec?: SfExec; env?: NodeJS.ProcessEnv } = {}) {
    const env = options.env ?? process.env;
    if (env.NODE_ENV === "production") throw new Error("The Salesforce CLI provider is for development only");
    if (!approvedAliases(env).includes(alias)) throw new OrgNotApprovedError(alias);
    this.#alias = alias;
    this.#exec = options.exec ?? runSf;
  }

  async connect(): Promise<OrgConnection> {
    const result = await this.#read(["org", "display", "--target-org", this.#alias, "--json"]);
    // Newer CLIs redact the token in `org display` and give it its own command.
    // Older ones don't have that command, so their display token is used.
    let accessToken = result?.accessToken && !REDACTED.test(result.accessToken) ? result.accessToken : undefined;
    if (result?.instanceUrl && !accessToken) {
      accessToken = (await this.#read(["org", "auth", "show-access-token", "--target-org", this.#alias, "--json"]))?.accessToken;
    }
    if (!accessToken || !result?.instanceUrl || REDACTED.test(accessToken)) throw new Error(`The CLI login for "${this.#alias}" has no active session`);
    return { instanceUrl: result.instanceUrl, accessToken, orgId: result.id, username: result.username };
  }

  async #read(args: string[]): Promise<Record<string, string> | undefined> {
    try {
      return (JSON.parse(await this.#exec(args)) as { result?: Record<string, string> }).result;
    } catch {
      // The CLI's error, or a parse error quoting its output, can hold the token.
      throw new Error(`Couldn't read the Salesforce CLI login for "${this.#alias}". Is it logged in?`);
    }
  }

  refresh(): Promise<OrgConnection> {
    return this.connect();
  }
}
