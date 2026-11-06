import { ASSUMPTIONS, DESCRIBED_OBJECTS, type Assumption } from "../facts/assumptions.ts";
import { isUnavailable, type SalesforceClient } from "../salesforce/client.ts";
import { aiSignalsExtractor, SETTINGS, type AiSignals } from "./ai.ts";
import { pardotSetupExtractor, type PardotSetup } from "./pardot.ts";
import type { ExtractContext, Extractor } from "./types.ts";

// The facts step (DECISIONS.md 287 to 295): on every run, it checks the names the
// engine assumes against the org itself, so the first real orgs settle QUESTIONS.md.
// It keeps names, booleans and field types only: never a record, a value, or a
// name the org chose. So facts.json is no more sensitive than the findings.

export const FACTS_FILE = "facts.json";
export const FACTS_VERSION = 1;

export type FactOutcome = "confirmed" | "contradicted" | "open" | "not_checked";

export type Fact = {
  id: string;
  question: string;
  covers: Assumption["covers"];
  says: string;
  outcome: FactOutcome;
  /** Why, in a sentence: what was or wasn't there. */
  detail: string;
  /** What was seen, by the names we assume only. A field's value is its type, or null when it isn't there. */
  observed: Record<string, boolean | string | null>;
  affects: string[];
};

export type Facts = {
  version: number;
  checkedAt: string;
  apiVersion: string;
  connectionKind: string;
  facts: Fact[];
  /** Calls the step made, so its cost is on record (rule 9). */
  calls: number;
};

const listed = (items: string[]): string => (items.length <= 1 ? (items[0] ?? "") : `${items.slice(0, -1).join(", ")} and ${items.at(-1)}`);

type GlobalObject = { name: string; queryable: boolean };
type Describe = { fields: Array<{ name: string; type: string }> };

/** An earlier step's output, or null when that step isn't part of this run. */
function earlier<T>(ctx: ExtractContext, extractor: Extractor<T>): T | null {
  try {
    return ctx.get(extractor);
  } catch {
    return null;
  }
}

/** A package counts as installed when its licence row is there, or any object or field carries its namespace. */
function installed(namespace: string, objects: Map<string, GlobalObject>, fields: Map<string, Map<string, string> | null>, licensed: boolean | null): boolean {
  const prefix = `${namespace.toLowerCase()}__`;
  if (licensed) return true;
  if ([...objects.keys()].some((o) => o.toLowerCase().startsWith(prefix))) return true;
  return [...fields.values()].some((f) => f !== null && [...f.keys()].some((name) => name.toLowerCase().startsWith(prefix)));
}

/** What the run's modules already retrieved, so the settings facts cost nothing. Null when that module didn't run. */
export type EarlierReads = { ai: AiSignals | null; pardot: PardotSetup | null };

export const factsExtractor: Extractor<Facts> = {
  id: "facts",
  version: 1,
  run: (ctx) => checkFacts(ctx.client, ctx.now, { ai: earlier(ctx, aiSignalsExtractor), pardot: earlier(ctx, pardotSetupExtractor) }),
};

/** The step itself, which `hc facts` also runs on its own. */
export async function checkFacts(client: SalesforceClient, now: Date, reads: EarlierReads): Promise<Facts> {
  const before = client.budget.callsMade;
  const facts: Fact[] = [];
  const add = (a: Assumption, outcome: FactOutcome, detail: string, observed: Fact["observed"] = {}): void =>
    void facts.push({ id: a.id, question: a.question, covers: a.covers, says: a.says, outcome, detail, observed, affects: a.affects });

  // One call lists every object the connection can see, and whether it can query each.
  let objects: Map<string, GlobalObject> | null = null;
  let globalGap = "";
  try {
    const global = await client.describeGlobal<{ sobjects: GlobalObject[] }>();
    objects = new Map(global.sobjects.map((o) => [o.name, { name: o.name, queryable: o.queryable }]));
  } catch (err) {
    if (!isUnavailable(err)) throw err;
    globalGap = `Salesforce wouldn't list the org's objects: ${err.message}`;
  }

  // Only the objects whose fields we assume, and only where they exist, so no describe fails.
  const fields = new Map<string, Map<string, string> | null>();
  for (const object of DESCRIBED_OBJECTS) {
    if (!objects?.has(object)) continue;
    try {
      const d = await client.describe<Describe>(object);
      fields.set(object, new Map(d.fields.map((f) => [f.name, f.type])));
    } catch (err) {
      if (!isUnavailable(err)) throw err;
      fields.set(object, null);
    }
  }

  // Whether each namespace we assume has a licence row. The other namespaces are never kept.
  let namespaces: string[] | null = null;
  let licenceGap = "";
  try {
    namespaces = (await client.query<{ NamespacePrefix: string | null }>("SELECT NamespacePrefix FROM PackageLicense")).records.map((p) => (p.NamespacePrefix ?? "").toLowerCase());
  } catch (err) {
    if (!isUnavailable(err)) throw err;
    licenceGap = err.message;
  }
  const licensed = (namespace: string): boolean | null => (namespaces ? namespaces.includes(namespace.toLowerCase()) : null);

  const limits = client.limitNames;
  const { ai, pardot } = reads;

  for (const a of ASSUMPTIONS) {
    if (a.kind === "limit") {
      if (limits.length === 0) add(a, "not_checked", "Salesforce's /limits wasn't read.");
      else if (limits.includes(a.name)) add(a, "confirmed", `/limits reports ${a.name}.`, { [a.name]: true });
      else add(a, "contradicted", `/limits doesn't report ${a.name}.`, { [a.name]: false });
      continue;
    }
    if (a.kind === "settings") {
      const returned = settingsReturned(a.members, ai, pardot);
      if (typeof returned === "string") {
        add(a, "not_checked", returned);
        continue;
      }
      const missing = a.members.filter((m) => !returned[m]);
      if (missing.length === 0) add(a, "confirmed", `Each member returned its settings.`, returned);
      else add(a, a.ifMissing === "contradicts" ? "contradicted" : "open", `${listed(missing)} returned nothing. The member name may be wrong, or the feature isn't on in this org.`, returned);
      continue;
    }
    if (!objects) {
      add(a, "not_checked", globalGap);
      continue;
    }
    if (a.kind === "object" || a.kind === "objects") {
      const names = a.kind === "object" ? [a.object] : a.objects;
      const observed = Object.fromEntries(names.map((n) => [n, objects!.get(n)?.queryable ?? false]));
      const missing = names.filter((n) => !observed[n]);
      if (missing.length === 0) add(a, "confirmed", `The connection can query ${listed(names)}.`, observed);
      else add(a, a.ifMissing === "contradicts" ? "contradicted" : "open", `${listed(missing)} ${missing.length === 1 ? "isn't" : "aren't"} among the objects the connection can query.`, observed);
      continue;
    }
    if (a.kind === "licence") {
      const isIn = installed(a.namespace, objects, fields, false);
      const row = licensed(a.namespace);
      if (!isIn) add(a, "not_checked", `Nothing with namespace ${a.namespace} is in this org.`, { installed: false });
      else if (row === null) add(a, "not_checked", `PackageLicense couldn't be read: ${licenceGap}`, { installed: true });
      else add(a, row ? "confirmed" : "contradicted", row ? `Namespace ${a.namespace} has a licence row.` : `Namespace ${a.namespace} is in the org with no licence row.`, { installed: true, licenceRow: row });
      continue;
    }
    // Fields.
    if (a.namespace && !installed(a.namespace, objects, fields, licensed(a.namespace))) {
      add(a, "not_checked", `Nothing with namespace ${a.namespace} is in this org.`);
      continue;
    }
    if (!objects.has(a.object)) {
      // With its package installed, a missing object is as wrong as a missing field.
      add(a, a.namespace ? "contradicted" : "not_checked", `${a.object} isn't among the objects the connection can see.`, Object.fromEntries(a.fields.map((f) => [`${a.object}.${f}`, null])));
      continue;
    }
    const described = fields.get(a.object);
    if (!described) {
      add(a, "not_checked", `${a.object} couldn't be described.`);
      continue;
    }
    const observed = Object.fromEntries(a.fields.map((f) => [`${a.object}.${f}`, described.get(f) ?? null]));
    const missing = a.fields.filter((f) => !described.has(f));
    if (missing.length === 0) add(a, "confirmed", `${a.object} has ${listed(a.fields)}.`, observed);
    else add(a, "contradicted", `${a.object} has no ${missing.join(" or ")}, or the connection can't see ${missing.length === 1 ? "it" : "them"}.`, observed);
  }
  return { version: FACTS_VERSION, checkedAt: now.toISOString(), apiVersion: client.apiVersion, connectionKind: client.connectionKind, facts, calls: client.budget.callsMade - before };
}

/** Which Settings members came back in the run's own retrieves, or why that can't be told. */
function settingsReturned(members: string[], ai: AiSignals | null, pardot: PardotSetup | null): Record<string, boolean> | string {
  const out: Record<string, boolean> = {};
  for (const member of members) {
    if (member === "Campaign") {
      if (!pardot) return "Account Engagement wasn't part of this run.";
      if (pardot.influence.status !== "ok") return `The campaign settings weren't retrieved: ${pardot.influence.status === "unavailable" ? pardot.influence.reason : "unknown"}`;
      out[member] = pardot.influence.data.customizable !== null;
      continue;
    }
    const key = (Object.keys(SETTINGS) as Array<keyof typeof SETTINGS>).find((k) => SETTINGS[k].member === member);
    if (!key) return `${member} isn't a member we retrieve.`;
    if (!ai) return "The AI readiness reads weren't part of this run.";
    if (ai.settings.status !== "ok") return `The settings weren't retrieved: ${ai.settings.status === "unavailable" ? ai.settings.reason : "unknown"}`;
    out[member] = ai.settings.data[key] !== null;
  }
  return out;
}
