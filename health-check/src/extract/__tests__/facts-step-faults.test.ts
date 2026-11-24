import { test } from "node:test";
import assert from "node:assert/strict";
import { lastDelivery, SalesforceError, type SalesforceClient } from "../../salesforce/client.ts";
import { ApiBudget } from "../../salesforce/budget.ts";
import { checkFacts } from "../facts.ts";

// Audit risk 7 (DECISIONS.md 710): the facts step runs last, after the run has
// spent its budget, and no finding reads it. Salesforce not answering it on
// the worker's last delivery costs the facts it would settle, never the run;
// before then the run stops and the next delivery reads it again, as every
// step does (702). Salesforce's own message is never kept in facts.json.

const SECRET = "a value from the org";
const failing = (err: () => Error): SalesforceClient =>
  ({
    budget: new ApiBudget({ stopAtShare: 0.5 }),
    apiVersion: "62.0",
    connectionKind: "oauth",
    limitNames: [],
    describeGlobal: async () => {
      throw err();
    },
    describe: async () => {
      throw err();
    },
    query: async () => {
      throw err();
    },
  }) as unknown as SalesforceClient;
const NOW = new Date("2026-10-31T12:00:00Z");
const READS = { ai: null, pardot: null };

test("an outage on the last delivery costs the facts, and says so in our words", async () => {
  const client = failing(() => new SalesforceError(503, "SERVER_UNAVAILABLE", SECRET));
  const facts = await lastDelivery.run(true, () => checkFacts(client, NOW, READS));
  const text = JSON.stringify(facts);
  assert.ok(!text.includes(SECRET));
  assert.ok(text.includes("Salesforce didn't answer as it should"));
  assert.ok(facts.facts.every((f) => f.outcome !== "confirmed"));
});

test("before the last delivery an outage stops the step, to be read again", async () => {
  const client = failing(() => new SalesforceError(503, "SERVER_UNAVAILABLE", SECRET));
  await assert.rejects(checkFacts(client, NOW, READS));
});

test("a refusal costs the fact on any delivery, without Salesforce's message", async () => {
  const client = failing(() => new SalesforceError(400, "SOME_NEW_CODE", SECRET));
  const text = JSON.stringify(await checkFacts(client, NOW, READS));
  assert.ok(!text.includes(SECRET));
  assert.ok(text.includes("Salesforce refused this read"));
});

test("a lost session still stops it, even on the last delivery", async () => {
  const client = failing(() => new SalesforceError(401, "INVALID_SESSION_ID", SECRET));
  await assert.rejects(lastDelivery.run(true, () => checkFacts(client, NOW, READS)));
});
