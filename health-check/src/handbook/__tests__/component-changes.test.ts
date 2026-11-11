import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { existsSync, mkdirSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { join, sep } from "node:path";
import { ALL_MODULES, SALES_CLOUD_MODULES } from "../../domains/index.ts";
import { modulesFor } from "../../domains/scope.ts";
import { metadataExtractor } from "../../extract/automation.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { componentChangesExtractor, flowChanges, ruleChanges, type ComponentChangesData, type FlowVersionRow, type RuleChangeRow } from "../../extract/component-changes.ts";
import type { MetadataBundle } from "../../extract/metadata-parse.ts";
import type { Sourced } from "../../extract/types.ts";
import { Pseudonymiser, tokenSource, UNKNOWN_USER, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { loadPseudonymMap } from "../../render/render-run.ts";
import type { Transport } from "../../salesforce/transport.ts";
import { docxText } from "../../testing/docx-text.ts";
import { runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator, type SimulatorOptions } from "../../testing/simulator.ts";
import { APEX_OUTSIDE_MODIFIER, AS_OF, COMPONENT_OUTSIDE_MODIFIER, METADATA_FILES, userId } from "../../testing/synthetic-org.ts";
import { buildHandbook, HANDBOOK_FILE, labelComponentModifiers } from "../build.ts";
import { renderHandbookDocx } from "../docx.ts";
import { readHandbook } from "../read.ts";
import { Handbook, HANDBOOK_VERSION, type HandbookFlow, type HandbookValidationRule } from "../schema.ts";
import { olderComponents, withoutReadings } from "./older-files.ts";

// When each flow and validation rule was last changed, and by whom (DECISIONS.md
// 482). A step of its own reads two Tooling lists; who is a token in
// handbook.json, and a name only in the Word document.

const read = <T>(dir: string, file: string): T => JSON.parse(readFileSync(join(dir, file), "utf8")) as T;
const OPTIONS = { clientName: "Synthetic Co", runDate: AS_OF };
const TOKEN = new RegExp(tokenSource());
// Written out, not imported, so a change to either query's text fails here first.
const FLOW_QUERY = "SELECT Id, DefinitionId, Definition.DeveloperName, Definition.NamespacePrefix, VersionNumber, Status, LastModifiedDate, LastModifiedById, LastModifiedBy.Name FROM Flow";
const RULE_QUERY = "SELECT Id, ValidationName, EntityDefinition.QualifiedApiName, NamespacePrefix, LastModifiedDate, LastModifiedById, LastModifiedBy.Name FROM ValidationRule";
const FLOWS_REFUSED = "We couldn't read when your flows were last changed, or by whom, so this handbook doesn't say.";
const RULES_REFUSED = "We couldn't read when your validation rules were last changed, or by whom, so this handbook doesn't say.";
/** Salesforce's own words for a refusal: in the step's checkpoint, and in nothing made from it. */
const REFUSAL = "COMPONENTS-REFUSED-0482: you can't list these";
const refuse = (kind: "Flow" | "ValidationRule"): SimulatorOptions["failQuery"] => (soql) => (soql.endsWith(`LastModifiedBy.Name FROM ${kind}`) ? { errorCode: "INSUFFICIENT_ACCESS", message: REFUSAL } : null);
const PARTIAL = "component-changes-refused.partial.jsonl";

async function simulated(options: SimulatorOptions = {}) {
  const transport = new Simulator(options);
  const result = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport, log: () => {} });
  return { ...result, transport };
}

/** Each SOQL query the transport was asked, in order, with the API it went to. */
function queries(sim: Simulator): Array<{ api: "tooling" | "rest"; soql: string }> {
  return sim.requests.flatMap((r) => {
    const url = decodeURIComponent(r.url);
    const api = url.includes("/tooling/") ? ("tooling" as const) : ("rest" as const);
    return [...`${url}\n${r.body ?? ""}`.matchAll(/q=(SELECT[^&"\n]+)/g)].map((m) => ({ api, soql: decodeURIComponent(m[1]!.replace(/\+/g, " ")) }));
  });
}
const lastChangeQueries = (sim: Simulator): string[] => queries(sim).filter((q) => q.soql === FLOW_QUERY || q.soql === RULE_QUERY).map((q) => q.soql);

const handbookOf = (dir: string): Handbook => Handbook.parse(read(dir, HANDBOOK_FILE));
const flowsOf = (h: Handbook) => (h.flows.status === "ok" ? h.flows : assert.fail("flows unavailable"));
const rulesOf = (h: Handbook) => (h.validationRules.status === "ok" ? h.validationRules : assert.fail("rules unavailable"));
const mapOf = (dir: string): PseudonymEntry[] => read<PseudonymEntry[]>(dir, "pseudonym-map.json");
const textOf = async (dir: string): Promise<string> => docxText(await renderHandbookDocx(handbookOf(dir), { ...OPTIONS, pseudonyms: loadPseudonymMap(dir) }));
/** The lines under a flow's or rule's heading in the document's text, up to the next heading. */
const under = (text: string, heading: string): string => text.slice(text.indexOf(`\n${heading}\n`) + 1).split(/\n#{2,4} /)[0]!;
/** "What it covers", up to the first part. */
const covers = (text: string): string => text.slice(text.indexOf("## What it covers"), text.indexOf("\n## Flows\n"));

/** Run ids and times, which differ between two runs of the same org, as the fingerprint leaves them out. */
const VOLATILE = /hc-\d{4}-\d{2}-\d{2}-[0-9a-f]{8}|"(startedAt|finishedAt|updatedAt|createdAt|madeAt|generatedAt)":\s*"[^"]*"/g;
const OUTPUTS = ["findings.json", "metrics.json", "scores.json", "manual-steps.json", "action-lists.json"];
const outputsOf = (dir: string) => Object.fromEntries(OUTPUTS.map((f) => [f, readFileSync(join(dir, f), "utf8").replace(VOLATILE, "")]));

/** Every file in a run folder, by its path inside it. */
function filesIn(dir: string): Map<string, string> {
  const files = new Map<string, string>();
  for (const name of readdirSync(dir, { recursive: true }) as string[]) {
    const path = join(dir, name);
    if (statSync(path).isFile()) files.set(name.split(sep).join("/"), readFileSync(path, "latin1"));
  }
  return files;
}

describe("the recorded synthetic org's flows and validation rules", () => {
  let dir: string;
  let text: string;
  before(async () => {
    dir = (await runFromRecording()).dir;
    text = await textOf(dir);
  });

  test("every flow and rule has its line, from the active version or the highest, and none from the managed package's", () => {
    const h = handbookOf(dir);
    assert.equal(h.version, HANDBOOK_VERSION);
    assert.deepEqual([flowsOf(h).lastChanges, rulesOf(h).lastChanges], ["read", "read"]);
    // Every flow the retrieve gives is named by its file, and each has its line.
    const files = Object.keys(METADATA_FILES).filter((p) => p.startsWith("flows/")).map((p) => p.slice(6).replace(/\.flow$/, ""));
    assert.deepEqual(flowsOf(h).items.map((f) => f.name).sort(), files.sort());
    assert.ok([...flowsOf(h).items, ...rulesOf(h).items].every((i) => i.lastChanged !== null));
    const jo = mapOf(dir).find((e) => e.id === userId(4))!.token;
    // Version 2 is active; version 3, saved later by Alex, isn't; the managed package's version 9 isn't the org's.
    assert.deepEqual(flowsOf(h).items.find((f) => f.name === "Lead_Assign")!.lastChanged, { at: "2026-07-03T12:00:00.000+0000", by: jo });
    // None active, so version 2, Priya's.
    assert.deepEqual(flowsOf(h).items.find((f) => f.name === "Draft_Idea")!.lastChanged, { at: "2026-10-01T12:00:00.000+0000", by: mapOf(dir).find((e) => e.id === userId(2))!.token });
    // Jo's, not the managed rule's of the same name on the same object.
    assert.deepEqual(rulesOf(h).items.find((r) => r.name === "Needs_Competitor")!.lastChanged, { at: "2026-08-17T12:00:00.000+0000", by: jo });
  });

  test("the document prints each under its flow or rule: a name, someone outside the user list, and a date alone", () => {
    assert.match(under(text, "### Assign Lead"), /^### Assign Lead\n\nAPI name: Lead_Assign\n\nStatus: Active\n\nLast changed 3 Jul 2026 by Jo Ellis\n\nType: /);
    assert.match(under(text, "### Draft"), /\nStatus: Draft\n\nLast changed 1 Oct 2026 by Priya Shah\n/);
    // The person who changed two pieces of Apex changed this flow too, under the same token.
    assert.match(under(text, "### Set Invoice Status"), new RegExp(`\\nLast changed 13 Oct 2026 by ${APEX_OUTSIDE_MODIFIER.name}\\n`));
    assert.equal(under(text, "#### Due_Date_Required").split("\n\n").slice(0, 3).join("\n\n"), `#### Due_Date_Required\n\nActive: Yes\n\nLast changed 6 Oct 2026 by ${COMPONENT_OUTSIDE_MODIFIER.name}`);
    assert.equal(under(text, "#### Old_Check").split("\n\n").slice(0, 3).join("\n\n"), "#### Old_Check\n\nActive: No\n\nLast changed 18 Jun 2025");
    assert.equal(text.match(/\nLast changed /g)?.length, 8 + 4 + 12);
    assert.doesNotMatch(text, TOKEN);
  });

  test("\"What it covers\" says so under the flows' line and the rules', the date alone only where one is printed", () => {
    assert.match(covers(text), /\nFlows: 8 flows, 5 of them active\.\n\nEach flow shows when it was last changed and by whom\.\n\nValidation rules: 4 validation rules, 3 of them active, on [^\n]*\.\n\nEach validation rule shows when it was last changed and by whom, or the date alone where Salesforce records no user\.\n\nApex classes: /);
    assert.ok(!text.includes(FLOWS_REFUSED) && !text.includes(RULES_REFUSED));
  });

  test("someone the run's user list doesn't hold is labelled from LastModifiedBy.Name, after Apex's, and the file holds the token", () => {
    const map = mapOf(dir);
    assert.deepEqual(map.find((e) => e.id === COMPONENT_OUTSIDE_MODIFIER.id), { token: "[[user:U0017]]", id: COMPONENT_OUTSIDE_MODIFIER.id, name: COMPONENT_OUTSIDE_MODIFIER.name, aliases: [COMPONENT_OUTSIDE_MODIFIER.name, COMPONENT_OUTSIDE_MODIFIER.id] });
    assert.equal(map.find((e) => e.id === APEX_OUTSIDE_MODIFIER.id)!.token, "[[user:U0016]]");
    assert.equal(rulesOf(handbookOf(dir)).items.find((r) => r.name === "Due_Date_Required")!.lastChanged!.by, "[[user:U0017]]");
    const file = readFileSync(join(dir, HANDBOOK_FILE), "utf8");
    for (const name of [COMPONENT_OUTSIDE_MODIFIER.name, APEX_OUTSIDE_MODIFIER.name, "Jo Ellis", "Priya Shah"]) assert.ok(!file.includes(name), name);
  });

  test("the step keeps one line for each flow and rule of the org's own, and its names rest in its checkpoint and the map alone", () => {
    const step = read<ComponentChangesData>(join(dir, "checkpoints"), "component-changes.json");
    assert.ok(step.flows.status === "read" && step.validationRules.status === "read");
    assert.deepEqual(step.flows.items.map((f) => f.name), ["Draft_Idea", "Invoice_Set_Status", "Lead_Assign", "Opportunity_Old_Process", "Opportunity_Route_Deal", "Opportunity_Stage_Review", "Renewal_Notice", "Weekly_Renewal_Check"]);
    assert.deepEqual(step.validationRules.items.map((r) => `${r.object}.${r.name}`), ["Case.Reason_When_Escalated", "Invoice__c.Due_Date_Required", "Invoice__c.Old_Check", "Opportunity.Needs_Competitor"]);
    assert.deepEqual(step.validationRules.items.find((r) => r.name === "Old_Check"), { object: "Invoice__c", name: "Old_Check", at: "2025-06-18T12:00:00.000+0000", byId: null, byName: null });
    const files = filesIn(dir);
    assert.deepEqual([...files.keys()].filter((f) => files.get(f)!.includes(COMPONENT_OUTSIDE_MODIFIER.name)).sort(), ["checkpoints/component-changes.json", "pseudonym-map.json"]);
  });

  test("every module that reads the metadata lists the step after it, and a set-up review keeps it", () => {
    const listing = (modules: typeof ALL_MODULES) => modules.filter((m) => m.extractors.includes(metadataExtractor));
    assert.deepEqual(listing(ALL_MODULES).map((m) => m.id).sort(), ["automation", "best_practice", "configuration", "technical_health", "utilisation"]);
    for (const module of [...listing(ALL_MODULES), ...listing(modulesFor("essentials", ALL_MODULES))]) {
      const at = module.extractors.indexOf(componentChangesExtractor);
      assert.ok(at > module.extractors.indexOf(metadataExtractor), module.id);
    }
    // It reads set-up, not records.
    assert.equal(componentChangesExtractor.records, undefined);
  });

  test("no rule reads the step: no module's rule or metric names it", () => {
    const root = new URL("../../domains/", import.meta.url);
    const naming = (readdirSync(root, { recursive: true }) as string[]).filter((f) => f.endsWith(".ts") && !f.includes("__tests__") && /componentChangesExtractor\)/.test(readFileSync(new URL(f, root), "utf8")));
    assert.deepEqual(naming, []);
  });
});

describe("the step's reads", () => {
  let clean: Awaited<ReturnType<typeof simulated>>;
  before(async () => {
    clean = await simulated();
  });

  test("are the two Tooling list queries, each once, as written in decision 482", () => {
    const asked = queries(clean.transport).filter((q) => q.soql.includes("LastModified") && /FROM (Flow|ValidationRule)$/.test(q.soql));
    assert.deepEqual(asked, [
      { api: "tooling", soql: FLOW_QUERY },
      { api: "tooling", soql: RULE_QUERY },
    ]);
    // Each its own GET, never a composite batch.
    assert.ok(clean.transport.requests.filter((r) => /LastModifiedBy\.Name(\+|%20| )FROM(\+|%20| )(Flow|ValidationRule)/.test(r.url)).every((r) => r.method === "GET"));
    assert.equal(clean.run.steps.find((s) => s.id === componentChangesExtractor.id)!.calls, 2);
  });

  test("on the Tooling route too, whose flows are named by their definitions, every flow and rule has its line", async () => {
    const tooling = await simulated({ readOnly: true });
    const h = handbookOf(tooling.dir);
    assert.ok(read<{ data: MetadataBundle }>(join(tooling.dir, "checkpoints"), "metadata.json").data.route === "tooling");
    assert.equal(flowsOf(h).items.length, 8);
    assert.ok([...flowsOf(h).items, ...rulesOf(h).items].every((i) => i.lastChanged !== null));
    assert.deepEqual(flowsOf(h).items.find((f) => f.name === "Lead_Assign")!.lastChanged, flowsOf(handbookOf(clean.dir)).items.find((f) => f.name === "Lead_Assign")!.lastChanged);
  });

  for (const [kind, refused, read, sentence] of [
    ["Flow", "flows", "validationRules", FLOWS_REFUSED],
    ["ValidationRule", "validationRules", "flows", RULES_REFUSED],
  ] as const) {
    test(`a refused ${kind} list leaves the other read, says so for its own kind, and moves no finding`, async () => {
      const run = await simulated({ failQuery: refuse(kind) });
      const h = handbookOf(run.dir);
      assert.equal(h[refused].status === "ok" && h[refused].lastChanges, "refused");
      assert.equal(h[read].status === "ok" && h[read].lastChanges, "read");
      assert.ok((h[refused] as { items: Array<{ lastChanged: unknown }> }).items.every((i) => i.lastChanged === null));
      assert.ok((h[read] as { items: Array<{ lastChanged: unknown }> }).items.every((i) => i.lastChanged !== null));
      const text = await textOf(run.dir);
      assert.equal(text.split(sentence).length - 1, 1);
      assert.ok(!text.includes(sentence === FLOWS_REFUSED ? RULES_REFUSED : FLOWS_REFUSED));
      // Said in "What it covers", under its part's own line.
      assert.match(covers(text), new RegExp(`\\n(Flows|Validation rules): [^\\n]*\\n\\n${sentence.replace(/[.?()]/g, "\\$&")}\\n`));
      assert.deepEqual(outputsOf(run.dir), outputsOf(clean.dir));
      // Salesforce's reason rests in the step's checkpoint alone, and the record of the refusal is cleared.
      const files = filesIn(run.dir);
      assert.deepEqual([...files.keys()].filter((f) => files.get(f)!.includes(REFUSAL)), ["checkpoints/component-changes.json"]);
      assert.ok(!existsSync(join(run.dir, "checkpoints", PARTIAL)));
    });
  }

  test("both refused, the run finishes with the handbook saying so for each", async () => {
    const run = await simulated({ failQuery: (soql) => refuse("Flow")!(soql) ?? refuse("ValidationRule")!(soql) });
    assert.ok(run.output);
    const text = await textOf(run.dir);
    assert.ok(text.includes(FLOWS_REFUSED) && text.includes(RULES_REFUSED));
    assert.equal(text.match(/\nLast changed /g)?.length, 12, "Apex's lines only");
  });

  test("a network error stops the step, as it would any other", async () => {
    const sim = new Simulator();
    const transport: Transport = { send: (request) => (decodeURIComponent(request.url).endsWith(FLOW_QUERY) ? Promise.reject(new TypeError("fetch failed")) : sim.send(request)) };
    const outDir = tempDir();
    await assert.rejects(runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir, now: new Date(AS_OF), transport, log: () => {}, runId: "hc-network" }), /fetch failed/);
    const run = read<{ status: string; steps: Array<{ id: string; status: string }> }>(join(outDir, "hc-network"), "run.json");
    assert.equal(run.status, "failed");
    assert.equal(run.steps.find((s) => s.id === componentChangesExtractor.id)!.status, "failed");
  });
});

describe("a run stopped at the budget after the flows' list was refused", () => {
  test("resumes without asking for the flows again, and reads the rules", async () => {
    const outDir = tempDir();
    const options = { provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir, now: new Date(AS_OF), log: () => {} };
    // The flows' list is refused, and the budget runs out on the rules' list that follows.
    const first = new Simulator({ failQuery: (soql) => refuse("Flow")!(soql) ?? (soql === RULE_QUERY ? { errorCode: "REQUEST_LIMIT_EXCEEDED", message: "TotalRequests Limit exceeded." } : null) });
    const stopped = await runHealthCheck({ ...options, transport: first });
    assert.equal(stopped.run.status, "stopped_budget");
    assert.deepEqual(lastChangeQueries(first), [FLOW_QUERY, RULE_QUERY]);
    // What the stopped run keeps: the kind and Salesforce's reason, nothing read from the org.
    const kept = readFileSync(join(stopped.dir, "checkpoints", PARTIAL), "utf8");
    assert.deepEqual(kept.trim().split("\n").map((line) => JSON.parse(line)), [{ kind: "flows", reason: `INSUFFICIENT_ACCESS: ${REFUSAL}` }]);

    // Resumed, the flows' list would be refused again if it were asked for.
    const second = new Simulator({ failQuery: refuse("Flow") });
    const resumed = await runHealthCheck({ ...options, transport: second, resume: stopped.run.id });
    assert.ok(resumed.output, "the run finished");
    assert.deepEqual(lastChangeQueries(second), [RULE_QUERY]);
    const h = handbookOf(resumed.dir);
    assert.deepEqual([flowsOf(h).lastChanges, rulesOf(h).lastChanges], ["refused", "read"]);
    assert.ok(!existsSync(join(resumed.dir, "checkpoints", PARTIAL)), "cleared once the step is saved");
  });
});

describe("a run in flight when this is deployed", () => {
  type Steps = { status: string; steps: Array<{ id: string; version: number; status: string }> };
  /** A run as an engine without the step left it: no such step, no checkpoint, a version 4 handbook and a map without the user only it adds. */
  async function fromBefore(edit: (run: Steps, dir: string) => void = () => {}) {
    const first = await simulated();
    const was = outputsOf(first.dir);
    const run = read<Steps>(first.dir, "run.json");
    run.steps = run.steps.filter((s) => s.id !== componentChangesExtractor.id);
    edit(run, first.dir);
    writeFileSync(join(first.dir, "run.json"), JSON.stringify(run));
    rmSync(join(first.dir, "checkpoints", "component-changes.json"));
    writeFileSync(join(first.dir, HANDBOOK_FILE), JSON.stringify(olderComponents({ ...read<Record<string, unknown>>(first.dir, HANDBOOK_FILE), version: 4 })));
    writeFileSync(join(first.dir, "pseudonym-map.json"), JSON.stringify(mapOf(first.dir).filter((e) => e.id !== COMPONENT_OUTSIDE_MODIFIER.id)));
    return { first, was };
  }
  const resume = async (id: string, outDir: string) => {
    const transport = new Simulator();
    await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir, now: new Date(), transport, log: () => {}, resume: id });
    return transport;
  };

  test("one that's not resumed again makes its document from its version 4 file, saying nothing of flows' or rules' last changes", async () => {
    const { first } = await fromBefore();
    const older = readHandbook(first.dir);
    assert.ok(older.ok);
    assert.deepEqual([flowsOf(older.handbook).lastChanges, rulesOf(older.handbook).lastChanges], ["not_kept", "not_kept"]);
    const text = docxText(await renderHandbookDocx(older.handbook, { ...OPTIONS, pseudonyms: loadPseudonymMap(first.dir) }));
    assert.equal(text.match(/\nLast changed /g)?.length, 12, "Apex's lines only");
    assert.doesNotMatch(text, /Each (flow|validation rule) shows|your (flows|validation rules) were last changed/);
  });

  test("one whose extraction had finished, delivered again, reads the two lists and nothing else, and its findings don't move", async () => {
    const { first, was } = await fromBefore();
    const transport = await resume(first.run.id, join(first.dir, ".."));
    // No other query: every other step is reused from its checkpoint.
    assert.deepEqual(queries(transport).map((q) => q.soql), [FLOW_QUERY, RULE_QUERY]);
    assert.deepEqual(outputsOf(first.dir), was);
    const after = read<Steps>(first.dir, "run.json");
    assert.deepEqual(after.steps.find((s) => s.id === componentChangesExtractor.id), { ...after.steps.find((s) => s.id === componentChangesExtractor.id), version: 1, status: "done" });
    const h = handbookOf(first.dir);
    assert.deepEqual([flowsOf(h).lastChanges, rulesOf(h).lastChanges], ["read", "read"]);
    assert.equal(mapOf(first.dir).find((e) => e.id === COMPONENT_OUTSIDE_MODIFIER.id)?.token, "[[user:U0017]]");
    assert.equal((await textOf(first.dir)).match(/\nLast changed /g)?.length, 24);
  });

  test("one stopped part-way through its extraction reads the lists with the steps it hadn't done", async () => {
    const { first, was } = await fromBefore((run, dir) => {
      // Stopped at the budget at the Apex step, as an engine without the new step left it.
      run.status = "stopped_budget";
      run.steps.find((s) => s.id === "apex")!.status = "pending";
      rmSync(join(dir, "checkpoints", "apex.json"));
    });
    const transport = await resume(first.run.id, join(first.dir, ".."));
    const asked = queries(transport).map((q) => q.soql);
    assert.ok(asked.some((q) => q.includes("FROM ApexClass")), "the Apex step ran");
    assert.deepEqual(asked.filter((q) => q === FLOW_QUERY || q === RULE_QUERY), [FLOW_QUERY, RULE_QUERY]);
    assert.deepEqual(outputsOf(first.dir), was);
    const h = handbookOf(first.dir);
    assert.deepEqual([flowsOf(h).lastChanges, rulesOf(h).lastChanges], ["read", "read"]);
  });
});

describe("handbook.json of an earlier version", () => {
  let current: Record<string, unknown>;
  let dir: string;
  before(async () => {
    current = read<Record<string, unknown>>((await runFromRecording()).dir, HANDBOOK_FILE);
    dir = join(tempDir(), "run");
    mkdirSync(dir, { recursive: true });
  });
  const write = (file: unknown) => writeFileSync(join(dir, HANDBOOK_FILE), JSON.stringify(file));

  test("version 6 reads back exactly as written", () => {
    assert.equal(current.version, 6);
    write(current);
    const back = readHandbook(dir);
    assert.ok(back.ok);
    assert.deepEqual(back.handbook, current);
  });

  test("version 5, as an engine at 8344688 wrote it, reads whole, with neither reading known (decision 484)", () => {
    write(withoutReadings({ ...current, version: 5 }));
    const back = readHandbook(dir);
    assert.ok(back.ok);
    assert.deepEqual(back.handbook, { ...current, readBy: { metadata: null, apex: null } });
    // But a version 5 file that says which reading made it is no version we wrote.
    write({ ...current, version: 5 });
    assert.deepEqual(readHandbook(dir), { ok: false, why: "format" });
  });

  test("version 4 still reads, its flows and rules with no line and listed as not kept, its Apex whole", async () => {
    write(olderComponents({ ...current, version: 4 }));
    const back = readHandbook(dir);
    assert.ok(back.ok);
    assert.equal(back.handbook.version, HANDBOOK_VERSION);
    assert.deepEqual([flowsOf(back.handbook).lastChanges, rulesOf(back.handbook).lastChanges], ["not_kept", "not_kept"]);
    assert.ok([...flowsOf(back.handbook).items, ...rulesOf(back.handbook).items].every((i) => i.lastChanged === null));
    assert.equal(back.handbook.apexClasses.status === "ok" && back.handbook.apexClasses.lastChanges, "read");
    // The rest is the file's own.
    const strip = <T extends { lastChanged: unknown }>(items: T[]) => items.map(({ lastChanged: _gone, ...item }) => item);
    assert.deepEqual(strip(flowsOf(back.handbook).items), strip(flowsOf(Handbook.parse(current)).items));
  });

  for (const version of [3, 2]) {
    test(`version ${version} still reads, its flows and rules listed as not kept`, () => {
      const older = (part: { status: string; items?: Array<Record<string, unknown>>; lastChanges?: string }) => {
        const { lastChanges: _said, ...rest } = part;
        return { ...rest, items: part.items!.map(({ lastChanged: _gone, ...item }) => item) };
      };
      const { recentChanges, ...file } = olderComponents<Record<string, unknown>>({ ...current, version, apexClasses: older(current.apexClasses as never), apexTriggers: older(current.apexTriggers as never) });
      write(version === 3 ? { ...file, recentChanges } : file);
      const back = readHandbook(dir);
      assert.ok(back.ok, `version ${version}`);
      assert.deepEqual([flowsOf(back.handbook).lastChanges, rulesOf(back.handbook).lastChanges], ["not_kept", "not_kept"]);
    });
  }

  test("but a version 4 file with the lines, or a version 5 file without them, is read as neither", () => {
    for (const wrong of [withoutReadings({ ...current, version: 4 }), olderComponents({ ...current, version: 5 })]) {
      write(wrong);
      assert.deepEqual(readHandbook(dir), { ok: false, why: "format" });
    }
  });
});

describe("the matching on its own", () => {
  const version = (name: string, n: number, status: string, at: string, byId: string | null = null, namespace: string | null = null): FlowVersionRow => ({
    Id: `301${String(n).padStart(12, "0")}AAA`,
    Definition: { DeveloperName: name, NamespacePrefix: namespace },
    VersionNumber: n,
    Status: status,
    LastModifiedDate: at,
    LastModifiedById: byId,
    LastModifiedBy: byId ? { Name: `Person ${byId.slice(-6, -3)}` } : null,
  });
  const rule = (object: string, name: string, at: string, namespace: string | null = null): RuleChangeRow => ({ Id: "03d000000000001AAA", ValidationName: name, EntityDefinition: { QualifiedApiName: object }, NamespacePrefix: namespace, LastModifiedDate: at, LastModifiedById: null, LastModifiedBy: null });

  test("a flow's line is its active version's, even with a later version saved since", () => {
    // In either order, as Salesforce gives no order.
    const rows = [version("Alpha", 1, "Obsolete", "2026-01-01"), version("Alpha", 2, "Active", "2026-02-02"), version("Alpha", 3, "Draft", "2026-03-03")];
    for (const order of [rows, [...rows].reverse()]) assert.deepEqual(flowChanges(order), [{ name: "Alpha", at: "2026-02-02", byId: null, byName: null }]);
  });

  test("with none active, the highest-numbered version's, read as a number", () => {
    const rows = [version("Beta", 9, "Draft", "2026-09-09"), version("Beta", 10, "Obsolete", "2026-10-10"), version("Beta", 2, "InvalidDraft", "2026-12-12")];
    for (const order of [rows, [...rows].reverse()]) assert.deepEqual(flowChanges(order).map((f) => f.at), ["2026-10-10"]);
    // Tooling can send the number as text.
    assert.deepEqual(flowChanges([{ ...version("Beta", 9, "Draft", "a"), VersionNumber: "9" }, { ...version("Beta", 10, "Draft", "b"), VersionNumber: "10" }]).map((f) => f.at), ["b"]);
  });

  test("a managed package's flow or rule is never the org's own, whatever its name, status or date", () => {
    assert.deepEqual(flowChanges([version("Alpha", 1, "Draft", "own"), version("Alpha", 7, "Active", "managed", null, "acme")]).map((f) => f.at), ["own"]);
    assert.deepEqual(ruleChanges([rule("Account", "Rule", "own"), rule("Account", "Rule", "managed", "acme")]).map((r) => r.at), ["own"]);
    // A row with no name to match by is left out rather than guessed at.
    assert.deepEqual(flowChanges([{ ...version("Gamma", 1, "Active", "x"), Definition: null }]), []);
    assert.deepEqual(ruleChanges([{ ...rule("Account", "Rule", "x"), EntityDefinition: null }]), []);
  });

  // A flow's handbook detail, as either route reads it, with nothing else to it.
  const flow = (name: string): HandbookFlow => ({
    name,
    label: name,
    status: "Active",
    processType: "AutoLaunchedFlow",
    description: null,
    start: { object: null, triggerType: null, recordTriggerType: null, schedule: null, entry: { logic: null, formula: null, formulaCut: false, formulaLength: null, conditions: [], conditionsTotal: 0, cut: false }, paths: [], pathsTotal: 0, pathsCut: false },
    steps: [],
    stepsTotal: 0,
    cut: false,
  });
  const detail = (object: string, name: string): HandbookValidationRule => ({ object, name, active: true, description: null, formula: "TRUE", formulaCut: false, formulaLength: 4, message: null, messageField: null });
  const metadata = (flows: HandbookFlow[], rules: HandbookValidationRule[]): Sourced<MetadataBundle> => ({
    status: "ok",
    data: { handbook: { flows, validationRules: rules, validationRulesCover: { customObjects: "all", standardObjects: "all" } } } as unknown as MetadataBundle,
    source: { kind: "metadata", path: "Flow" },
    truncated: false,
  });
  const JO = userId(4);
  const JO_TOKEN = "[[user:U0004]]";
  const at = "2026-03-03T09:15:00.000+0000";
  const read = (data: Partial<ComponentChangesData>): ComponentChangesData => ({ flows: { status: "read", items: [] }, validationRules: { status: "read", items: [] }, ...data });
  const pseudonyms = new Pseudonymiser([{ token: JO_TOKEN, id: JO, name: "Jo Ellis", aliases: [] }]);
  const render = async (h: Handbook, names: Pseudonymiser | null = pseudonyms) => docxText(await renderHandbookDocx(h, { ...OPTIONS, pseudonyms: names }));

  test("a rule is matched by its object and its name, so the same name on two objects is kept apart", () => {
    const h = buildHandbook(
      metadata([], [detail("Account", "Needs_Owner"), detail("Contact", "Needs_Owner"), detail("Lead", "Needs_Owner")]),
      undefined,
      undefined,
      undefined,
      new Map([[JO, JO_TOKEN]]),
      read({ validationRules: { status: "read", items: [{ object: "Contact", name: "Needs_Owner", at, byId: JO, byName: "Jo Ellis" }, { object: "Account", name: "Needs_Owner", at: "2025-01-01T00:00:00.000+0000", byId: null, byName: null }] } }),
    );
    assert.deepEqual(rulesOf(h).items.map((r) => [r.object, r.lastChanged]), [
      ["Account", { at: "2025-01-01T00:00:00.000+0000", by: null }],
      ["Contact", { at, by: JO_TOKEN }],
      // No line for Lead's: Salesforce's list didn't have it.
      ["Lead", null],
    ]);
  });

  test("an object and name that a dot would join alike aren't taken for each other", () => {
    const h = buildHandbook(metadata([], [detail("A.B", "C"), detail("A", "B.C")]), undefined, undefined, undefined, new Map(), read({ validationRules: { status: "read", items: [{ object: "A", name: "B.C", at, byId: null, byName: null }] } }));
    assert.deepEqual(rulesOf(h).items.map((r) => r.lastChanged), [null, { at, by: null }]);
  });

  test("a flow with no line in Salesforce's list prints none, and \"What it covers\" says how many", async () => {
    const h = buildHandbook(metadata([flow("Alpha"), flow("Beta"), flow("constructor")], []), undefined, undefined, undefined, new Map([[JO, JO_TOKEN]]), read({ flows: { status: "read", items: [{ name: "Alpha", at, byId: JO, byName: "Jo Ellis" }] } }));
    assert.deepEqual(flowsOf(h).items.map((f) => f.lastChanged), [{ at, by: JO_TOKEN }, null, null]);
    const text = await render(h);
    assert.match(under(text, "### Alpha"), /\nLast changed 3 Mar 2026 by Jo Ellis\n/);
    assert.doesNotMatch(under(text, "### Beta"), /Last changed/);
    assert.match(covers(text), /\nEach flow shows when it was last changed and by whom\. Salesforce's list of changes had nothing for 2 flows, so those show neither\.\n/);
    // And none at all, said as that.
    const none = await render(buildHandbook(metadata([flow("Beta")], []), undefined, undefined, undefined, new Map(), read({})));
    assert.match(covers(none), /\nSalesforce's list of changes had nothing for your flows, so none shows when it was last changed\.\n/);
  });

  test("a user the map can't name, or named only as the placeholder, prints as a user whose name we couldn't look up, never as the token", async () => {
    const unknown = "[[user:U0999]]";
    const placeholder = "[[user:U0998]]";
    const names = new Pseudonymiser([{ token: placeholder, id: userId(998), name: UNKNOWN_USER, aliases: [] }]);
    const h = buildHandbook(
      metadata([flow("Alpha")], [detail("Account", "Rule")]),
      undefined,
      undefined,
      undefined,
      new Map([
        [userId(999), unknown],
        [userId(998), placeholder],
      ]),
      read({ flows: { status: "read", items: [{ name: "Alpha", at, byId: userId(999), byName: "Someone" }] }, validationRules: { status: "read", items: [{ object: "Account", name: "Rule", at, byId: userId(998), byName: null }] } }),
    );
    for (const map of [names, null]) {
      const text = await render(h, map);
      assert.match(under(text, "### Alpha"), /\nLast changed 3 Mar 2026 by a user whose name we couldn't look up\n/);
      assert.match(under(text, "#### Rule"), /\nLast changed 3 Mar 2026 by a user whose name we couldn't look up(\n|$)/);
      for (const shown of [unknown, placeholder, "U0999", "0999", "[[", "Someone", UNKNOWN_USER]) assert.ok(!text.includes(shown), shown);
    }
  });

  test("everyone is labelled once, in id order, from the flows and then the rules, and a list not read labels no one", () => {
    const labelled: string[] = [];
    const tokens = labelComponentModifiers(
      read({ flows: { status: "read", items: [{ name: "A", at, byId: userId(962), byName: "Later Id" }] }, validationRules: { status: "read", items: [{ object: "X", name: "B", at, byId: userId(961), byName: "Earlier Id" }, { object: "X", name: "C", at, byId: null, byName: null }] } }),
      (id, name) => {
        labelled.push(`${id} ${name}`);
        return `[[user:U${String(labelled.length).padStart(4, "0")}]]`;
      },
    );
    assert.deepEqual(labelled, [`${userId(961)} Earlier Id`, `${userId(962)} Later Id`]);
    assert.equal(tokens.size, 2);
    assert.equal(labelComponentModifiers({ flows: { status: "refused", reason: REFUSAL }, validationRules: { status: "refused", reason: REFUSAL } }, () => assert.fail("labelled")).size, 0);
    assert.equal(labelComponentModifiers(undefined, () => assert.fail("labelled")).size, 0);
  });

  test("a run without the step lists them as not kept, and says nothing", async () => {
    const h = buildHandbook(metadata([flow("Alpha")], [detail("Account", "Rule")]));
    assert.deepEqual([flowsOf(h).lastChanges, rulesOf(h).lastChanges], ["not_kept", "not_kept"]);
    assert.doesNotMatch(await render(h), /last changed/i);
  });

  test("Salesforce's reason for a refusal reaches neither handbook.json nor the document", async () => {
    const h = buildHandbook(metadata([flow("Alpha")], [detail("Account", "Rule")]), undefined, undefined, undefined, new Map(), { flows: { status: "refused", reason: REFUSAL }, validationRules: { status: "refused", reason: REFUSAL } });
    assert.ok(!JSON.stringify(h).includes("COMPONENTS-REFUSED"));
    const text = await render(h);
    assert.ok(!text.includes("COMPONENTS-REFUSED"));
    assert.ok(text.includes(FLOWS_REFUSED) && text.includes(RULES_REFUSED));
  });
});
