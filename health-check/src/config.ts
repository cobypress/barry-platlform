// Service-wide settings. Secrets never live here: the only values read from the
// environment are the ones below, and docs/DECISIONS.md lists them.

export const RULE_SET_VERSION = "2026.11-m3";

// Model ids are config so a model change never touches analysis code. Nothing
// calls Claude in Milestone 1. Checked against the Anthropic docs on 2026-11-02.
export const MODELS = {
  synthesis: "claude-opus-5-5",
  notesAndScreenshots: "claude-sonnet-5-5",
  // The cheapest model, one call per class or trigger, at most 200 a run (DECISIONS.md 490).
  handbookSummaries: "claude-haiku-5-5",
} as const;

export const QUEUE_NAME = "health-check-jobs";

export const SF_DEFAULTS = {
  // The floor we've written queries against. The run uses the org's newest
  // version at or above this, unless HC_SF_API_VERSION pins one.
  minApiVersion: 60,
  pageSize: 2000,
  compositeBatchSize: 25,
  retrievePollMs: 5_000,
  retrieveMaxPolls: 120,
  requestTimeoutMs: 120_000,
  maxRetries: 3,
};

export const BUDGET_DEFAULTS = {
  // Stop once the org as a whole reaches this share of its daily API limit, so
  // the customer's own integrations always keep the rest.
  stopAtShare: 0.5,
  // A hard ceiling on this run alone, whatever the org's limit is.
  maxCallsPerRun: 5_000,
  // Metadata API responses don't carry a usage header, so re-read /limits this often.
  refreshEveryCalls: 50,
};

export function envNumber(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return fallback;
  const n = Number(raw);
  if (!Number.isFinite(n)) throw new Error(`${name} must be a number`);
  return n;
}
