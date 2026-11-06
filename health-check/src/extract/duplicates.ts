import { ACTION_ROWS, type NamedRow } from "./action-records.ts";
import { readSoql, type Extractor, type Sourced } from "./types.ts";

// Accounts that share an exact name (DECISIONS.md 205). The groups come from one
// aggregate query, largest first; the accounts in them are then read by name, a
// few names a query, up to the action lists' ceiling. Names stay in the worker:
// the rule counts, and only the action list shows them.

/** How many groups are read. A larger org's smallest groups are left out, and the evidence says so. */
export const DUPLICATE_GROUPS = 200;
const NAMES_A_QUERY = 50;

export type DuplicateAccounts = { groups: Sourced<Array<{ Name: string; c: number }>>; accounts: Sourced<NamedRow[]> };

const quote = (name: string): string => `'${name.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`;

export const duplicateAccountsExtractor: Extractor<DuplicateAccounts> = {
  id: "duplicate-accounts",
  version: 1,
  records: true,
  async run(ctx) {
    const groups = await readSoql<{ Name: string; c: number }>(ctx, `SELECT Name, COUNT(Id) c FROM Account GROUP BY Name HAVING COUNT(Id) > 1 ORDER BY COUNT(Id) DESC LIMIT ${DUPLICATE_GROUPS}`);
    const source = { kind: "soql", api: "rest", query: "SELECT Id, Name, OwnerId FROM Account WHERE Name IN (...)" } as const;
    if (groups.status !== "ok") return { groups, accounts: { status: "unavailable", reason: groups.reason, source } };
    const names = groups.data.map((g) => g.Name).filter((n) => typeof n === "string" && n.length > 0);
    const accounts: NamedRow[] = [];
    for (let i = 0; i < names.length && accounts.length < ACTION_ROWS; i += NAMES_A_QUERY) {
      const part = await readSoql<NamedRow>(ctx, `SELECT Id, Name, OwnerId FROM Account WHERE Name IN (${names.slice(i, i + NAMES_A_QUERY).map(quote).join(", ")}) ORDER BY Name, Id`);
      if (part.status !== "ok") return { groups, accounts: part };
      accounts.push(...part.data);
    }
    return { groups, accounts: { status: "ok", data: accounts.slice(0, ACTION_ROWS), source, truncated: accounts.length >= ACTION_ROWS } };
  },
};
