import { mkdirSync, writeFileSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { join } from "node:path";
import { BUDGET_DEFAULTS } from "../config.ts";
import { assertOutsideGit } from "../engine/run.ts";
import { checkFacts, FACTS_FILE, type Facts } from "../extract/facts.ts";
import { ApiBudget } from "../salesforce/budget.ts";
import { SalesforceClient } from "../salesforce/client.ts";
import type { ConnectionProvider } from "../salesforce/connection.ts";
import type { Transport } from "../salesforce/transport.ts";

// `hc facts`: the facts step on its own, for Black Cloud's own development org
// (DECISIONS.md 292). The provider holds the org gate: an alias or connection
// must be in HC_APPROVED_ORG_ALIASES, as for a run. It reads no records and
// retrieves no metadata, so the settings facts are left unchecked.

export type FactsRunOptions = { provider: ConnectionProvider; outDir: string; transport?: Transport; apiVersion?: string; now?: Date; stopAtShare?: number };

export async function runFacts(options: FactsRunOptions): Promise<{ dir: string; facts: Facts }> {
  assertOutsideGit(options.outDir);
  const now = options.now ?? new Date();
  const budget = new ApiBudget({ stopAtShare: options.stopAtShare ?? BUDGET_DEFAULTS.stopAtShare });
  const client = await SalesforceClient.connect(options.provider, { budget, transport: options.transport, apiVersion: options.apiVersion, sleep: options.transport ? async () => {} : undefined });
  const facts = await checkFacts(client, now, { ai: null, pardot: null });
  // A folder per check, so `npm run questions` reads these as it reads run folders.
  const dir = join(options.outDir, `facts-${now.toISOString().slice(0, 10)}-${randomUUID().slice(0, 8)}`);
  mkdirSync(dir, { recursive: true, mode: 0o700 });
  writeFileSync(join(dir, FACTS_FILE), `${JSON.stringify(facts, null, 2)}\n`, { mode: 0o600 });
  return { dir, facts };
}
