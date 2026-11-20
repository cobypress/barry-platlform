import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { join, relative } from "node:path";
import { APEX_SOURCE_FILE } from "../../extract/apex-source.ts";
import { SUMMARIES_FILE } from "../../handbook/summaries.ts";
import { APEX_SUMMARY_PROMPT } from "../../narrative/apex-prompt.ts";
import type { NarrativeClient, NarrativeRequest } from "../../narrative/client.ts";
import { renderReleased, type ReleasedDocuments } from "../../render/render-released.ts";
import { BASELINE_READ_FILE, INTERVIEWS_FILE, SURVEY_FILE } from "../../render/render-run.ts";
import type { ManualAnswer, ManualStep } from "../../schemas/index.ts";
import { FAKE, FakeTokenEndpoint } from "../../testing/fake-oauth.ts";
import { docxText } from "../../testing/docx-text.ts";
import { syntheticInterviews } from "../../testing/interviews.ts";
import { loadAnswers, loadCalibration, loadRecording, ReplayTransport } from "../../testing/replay.ts";
import { sfId } from "../../testing/synthetic-org.ts";
import { syntheticSurvey } from "../../testing/survey.ts";
import type { Transport, TransportRequest, TransportResponse } from "../../salesforce/transport.ts";
import type { DocumentDrive, DriveItem } from "../drive.ts";
import { processDocumentsJob } from "../documents-job.ts";
import { MemoryWorkerStore } from "../memory-store.ts";
import { needsReconnection } from "../apps.ts";
import { processRunJob } from "../run-job.ts";
import type { RunJobInput, RunOutputRecord, RunProgress } from "../types.ts";
import { baseDeps, CONNECTION, consoleRun, INTAKE, RUN_ID, runInput, syntheticConnection } from "./helpers.ts";

// A manual answer reaches the findings, the sections and the Word notes
// (DECISIONS.md 541): the console saves it while the run awaits manual steps
// and sends the run again, and that delivery reads the run after its claim
// (542) and drafts through Claude only when it will reach review (543). Each
// case goes through the real run job, with the synthetic org's recording and
// the memory store.

const EAC = "eac-advanced-sync";
const DWU = "default-workflow-user";
const EAC_TITLE = "Check Einstein Activity Capture advanced sync settings";
const DWU_TITLE = "Confirm the Default Workflow User";
const WAITING_LINE = `Run ${RUN_ID} is awaiting manual steps, so its sections are the engine's own text until the answers are in.`;

const answer = (key: string): ManualAnswer => loadAnswers().find((a) => a.stepKey === key)!;
const skip = (key: string, reason: string): ManualAnswer => ({ stepKey: key, outcome: "skipped", values: {}, skipReason: reason, answeredBy: "consultant@example.com", answeredAt: "2026-11-02T10:00:00.000Z" });

/** The memory store, keeping what each `saveRunOutput` was given and counting the readings, with a place to act just before the claim's statement. */
class ConsoleStore extends MemoryWorkerStore {
  readonly saved: RunOutputRecord[] = [];
  readings = 0;
  beforeClaim: () => void = () => {};
  /** Changes the reading with this number (1 is the first). */
  reading: (n: number, input: RunJobInput | null) => RunJobInput | null = (_n, input) => input;
  override async loadRunJob(runId: string): Promise<RunJobInput | null> {
    return this.reading(++this.readings, await super.loadRunJob(runId));
  }
  override async claimRun(runId: string): Promise<void> {
    this.beforeClaim();
    await super.claimRun(runId);
  }
  override async saveRunOutput(runId: string, output: RunOutputRecord): Promise<void> {
    await super.saveRunOutput(runId, output);
    this.saved.push(structuredClone(output));
  }
}

/**
 * What the console's answer writes, in its own transaction: the run's row
 * where it awaits manual steps first (DECISIONS.md 545), and nothing at all
 * when that matches no row; then each answer and its step's status.
 */
function consoleAnswers(store: MemoryWorkerStore, answers: ManualAnswer[]): boolean {
  const run = store.runs.get(RUN_ID)!;
  if (run.run.status !== "awaiting_manual") return false;
  for (const a of answers) {
    run.answers = [...run.answers.filter((x) => x.stepKey !== a.stepKey), structuredClone(a)];
    const step = run.output!.manualSteps.find((s) => s.key === a.stepKey)!;
    step.status = a.outcome;
  }
  return true;
}

/** A stub Claude that counts what it's asked: a section gets a paragraph, an Apex summary a sentence. */
function claude(): NarrativeClient & { sections: number; summaries: number } {
  const client = {
    sections: 0,
    summaries: 0,
    complete: async (request: NarrativeRequest) => {
      if (request.system === APEX_SUMMARY_PROMPT) {
        client.summaries++;
        return { stopReason: "end_turn", model: request.model, text: JSON.stringify({ summary: "Runs the work of this class when it is called." }) };
      }
      client.sections++;
      return { stopReason: "end_turn", model: request.model, text: JSON.stringify({ paragraphs: [{ text: "We looked closely at this area.", sources: [] }] }) };
    },
  };
  return client as NarrativeClient & { sections: number; summaries: number };
}

async function setup(input: RunJobInput = runInput(), overrides: Parameters<typeof baseDeps>[0] = {}) {
  const store = new ConsoleStore();
  const deps = baseDeps({ store, ...overrides });
  await deps.tokens.save(syntheticConnection());
  store.addRun(input);
  const dir = join(deps.runsDir, RUN_ID);
  const stored = () => store.runs.get(RUN_ID)!;
  let transport = new ReplayTransport(loadRecording());
  /** A delivery, with a fresh replay of the recording unless a transport is given. */
  const deliver = (final = true, data?: Transport) => {
    transport = new ReplayTransport(loadRecording());
    deps.dataTransport = data ?? transport;
    return processRunJob(RUN_ID, deps, { final });
  };
  return { store, deps, dir, stored, deliver, requests: () => transport.requests };
}

/** The steps as the console was handed them at the last save. */
const stepsSaved = (store: ConsoleStore): Record<string, ManualStep["status"]> => Object.fromEntries(store.saved.at(-1)!.manualSteps.map((s) => [s.key, s.status]));

/** Each request the delivery sent, as its method and path, the API version taken out. */
const paths = (requests: TransportRequest[]): string[] => requests.map((r) => `${r.method} ${new URL(r.url).pathname.replace(/\/v\d+\.\d+\//, "/vNN/")}`);
const ONLY_START = ["GET /services/data/", "GET /services/data/vNN/limits"];

const findingOf = (output: RunOutputRecord, ruleId: string) => output.findings.filter((f) => f.ruleId === ruleId);

/** A Drive in memory, for the documents job. */
function memoryDrive(): DocumentDrive {
  const items = new Map<string, DriveItem & { parent: string; folder: boolean }>();
  let next = 1;
  const add = (parent: string, name: string, folder: boolean): DriveItem => {
    const item = { id: `drive-item-${String(next++).padStart(4, "0")}`, name, url: "https://drive.example.com/item", parent, folder, createdAt: "2026-11-04T18:00:00.000Z" };
    items.set(item.id, item);
    return item;
  };
  return {
    rootFolderId: "root-folder-0001",
    folder: async (id) => (items.get(id)?.folder ? items.get(id)! : null),
    folders: async (parent) => [...items.values()].filter((i) => i.parent === parent && i.folder),
    files: async (parent) => [...items.values()].filter((i) => i.parent === parent && !i.folder),
    createFolder: async (parent, name) => add(parent, name, true),
    putFile: async (parent, name, _mime, _body, replaceId) => (replaceId ? items.get(replaceId)! : add(parent, name, false)),
    moveFile: async (id, _from, to) => void (items.get(id)!.parent = to),
  };
}

/** Every section approved and the run released, as the console does, then the real documents make. The Word notes' text. */
async function releaseAndMake(w: Awaited<ReturnType<typeof setup>>): Promise<string> {
  const run = w.stored();
  run.run.status = "released";
  const sections = run.output!.sections.map((s) => ({ ...s, status: "approved" as const, approval: { approvedBy: "consultant@example.com", approvedAt: "2026-11-04T18:00:00.000Z" } }));
  const intake = { ...INTAKE, customerName: "Synthetic Co" };
  w.store.addDocumentsJob({ run: run.run, intake, sections, versions: run.output!.versions, overrides: [], preparedBy: null, state: "queued", driveFolderId: null, files: [] });
  let printed: ReleasedDocuments | undefined;
  w.deps.drive = memoryDrive();
  w.deps.renderReleased = async (dir, approved, out) => {
    const made = await renderReleased(dir, approved, out);
    assert.ok(!("refused" in made), JSON.stringify(made));
    printed = made;
    return made;
  };
  const outcome = await processDocumentsJob(RUN_ID, w.deps, { final: true });
  assert.equal(outcome.outcome, "done", outcome.detail);
  return docxText(readFileSync(printed!.files.raw_notes));
}

const stepsInFolder = (dir: string): Record<string, string> => Object.fromEntries((JSON.parse(readFileSync(join(dir, "manual-steps.json"), "utf8")) as ManualStep[]).map((s) => [s.key, s.status]));

/** Every file under the folder, by its path, with its size and time. */
function listing(dir: string): Map<string, string> {
  const out = new Map<string, string>();
  const walk = (d: string): void => {
    for (const name of readdirSync(d)) {
      const path = join(d, name);
      const s = statSync(path);
      if (s.isDirectory()) walk(path);
      else out.set(relative(dir, path), `${s.size} ${s.mtimeMs}`);
    }
  };
  walk(dir);
  return out;
}

describe("answers saved just before the claim (DECISIONS.md 542)", () => {
  test("a delivery whose claim lands just after the console saved both answers analyses with them, and ends in review", async () => {
    const w = await setup();
    assert.equal((await w.deliver()).detail, `Run ${RUN_ID} is awaiting manual`);
    // The console's transaction commits between the job's first reading and its claim.
    w.store.beforeClaim = () => assert.ok(consoleAnswers(w.store, [answer(EAC), answer(DWU)]));
    assert.deepEqual(await w.deliver(), { outcome: "done", detail: `Run ${RUN_ID} is in review` });
    assert.deepEqual(stepsSaved(w.store), { [EAC]: "answered", [DWU]: "answered" });
    assert.equal(w.stored().run.status, "in_review");
  });
});

describe("from the answer to the printed page (DECISIONS.md 541)", () => {
  test("both answered: the next delivery reads nothing more of the org, the finding reads as the answer makes it, and the Word notes say answered", async () => {
    const w = await setup();
    assert.equal((await w.deliver()).detail, `Run ${RUN_ID} is awaiting manual`);
    assert.deepEqual(stepsSaved(w.store), { [EAC]: "pending", [DWU]: "pending" });
    assert.match(findingOf(w.store.saved[0]!, "utl.eac-advanced-sync")[0]!.notMeasurableReason!, /hasn't been answered yet/);
    assert.ok(consoleAnswers(w.store, [answer(EAC), answer(DWU)]));
    assert.deepEqual(await w.deliver(), { outcome: "done", detail: `Run ${RUN_ID} is in review` });
    assert.deepEqual(paths(w.requests()), ONLY_START);
    const [eac] = findingOf(w.store.saved.at(-1)!, "utl.eac-advanced-sync");
    // The synthetic answer says Relate Synced Events is off.
    assert.equal(eac!.status, "issue");
    assert.equal(eac!.title, "Synced calendar events may not be reaching your deals");
    assert.match(eac!.summary, /^Relate Synced Events is off, so events can sync without being related/);
    assert.equal(eac!.notMeasurableReason, null);
    assert.deepEqual(stepsSaved(w.store), { [EAC]: "answered", [DWU]: "answered" });
    assert.deepEqual(stepsInFolder(w.dir), { [EAC]: "answered", [DWU]: "answered" });
    const notes = await releaseAndMake(w);
    assert.ok(notes.includes(`${EAC_TITLE} (answered)`), "the EAC step");
    assert.ok(notes.includes(`${DWU_TITLE} (answered)`), "the Default Workflow User step");
    assert.ok(!notes.includes("(pending)"));
  });

  test("the EAC step skipped with a reason: the finding gives the reason, the notes say skipped, and the run is in review", async () => {
    const reason = "The admin couldn't reach Setup before the readout.";
    const w = await setup();
    await w.deliver();
    assert.ok(consoleAnswers(w.store, [skip(EAC, reason), answer(DWU)]));
    assert.deepEqual(await w.deliver(), { outcome: "done", detail: `Run ${RUN_ID} is in review` });
    const [eac] = findingOf(w.store.saved.at(-1)!, "utl.eac-advanced-sync");
    assert.equal(eac!.status, "not_measurable");
    assert.equal(eac!.notMeasurableReason, `The manual step was skipped: ${reason}`);
    assert.deepEqual(stepsInFolder(w.dir), { [EAC]: "skipped", [DWU]: "answered" });
    const notes = await releaseAndMake(w);
    assert.ok(notes.includes(`${EAC_TITLE} (skipped)`));
    assert.ok(notes.includes(`${DWU_TITLE} (answered)`));
    assert.ok(!notes.includes("(pending)"));
  });

  test("the Default Workflow User step skipped with a reason: a finding says which users it holds up, with the reason", async () => {
    const reason = "The customer's admin is on leave until December.";
    const w = await setup();
    await w.deliver();
    assert.ok(consoleAnswers(w.store, [answer(EAC), skip(DWU, reason)]));
    assert.deepEqual(await w.deliver(), { outcome: "done", detail: `Run ${RUN_ID} is in review` });
    const reasons = w.store.saved.at(-1)!.findings.map((f) => f.notMeasurableReason ?? "");
    assert.ok(
      reasons.some((r) => /^Whether \d+ users? can be released can't be measured, because the Default Workflow User step was skipped: /.test(r) && r.endsWith(reason)),
      reasons.join("\n"),
    );
    assert.deepEqual(stepsInFolder(w.dir), { [EAC]: "answered", [DWU]: "skipped" });
  });

  test("one answer of two: the run still awaits manual steps, the answer is kept, and nothing is drafted through Claude", async () => {
    const client = claude();
    const w = await setup(runInput(), { narrative: client });
    await w.deliver();
    assert.ok(consoleAnswers(w.store, [answer(EAC)]));
    assert.deepEqual(await w.deliver(), { outcome: "done", detail: `Run ${RUN_ID} is awaiting manual` });
    assert.deepEqual(stepsSaved(w.store), { [EAC]: "answered", [DWU]: "pending" });
    assert.equal(findingOf(w.store.saved.at(-1)!, "utl.eac-advanced-sync")[0]!.status, "issue");
    assert.equal(client.sections, 0);
    assert.ok(w.store.saved.at(-1)!.versions.every((v) => v.source === "engine"));
  });
});

describe("Claude drafts only a delivery that will reach review (DECISIONS.md 543)", () => {
  test("the first delivery, awaiting manual steps, asks for no section; the one after the answers drafts and hands them over in review", async () => {
    const client = claude();
    const w = await setup(runInput(), { narrative: client });
    assert.equal((await w.deliver()).detail, `Run ${RUN_ID} is awaiting manual`);
    assert.equal(client.sections, 0);
    const first = w.store.saved[0]!;
    assert.ok(first.versions.every((v) => v.source === "engine"));
    assert.ok(first.sections.every((s) => s.status === "draft" || s.status === "needs_input"));
    assert.equal(w.deps.lines.filter((l) => l === WAITING_LINE).length, 1);
    assert.ok(!w.deps.lines.some((l) => /Claude drafted/.test(l)));

    assert.ok(consoleAnswers(w.store, [answer(EAC), answer(DWU)]));
    const lines = w.deps.lines.length;
    assert.equal((await w.deliver()).detail, `Run ${RUN_ID} is in review`);
    assert.ok(client.sections > 10, `${client.sections} section calls`);
    const second = w.store.saved.at(-1)!;
    assert.equal(second.versions.filter((v) => v.source === "claude_draft").length, client.sections);
    assert.ok(second.sections.some((s) => s.status === "in_review") && !second.sections.some((s) => s.status === "draft"));
    assert.ok(!w.deps.lines.slice(lines).includes(WAITING_LINE));
  });

  test("a run whose first delivery has both answers drafts on that delivery, as before", async () => {
    const client = claude();
    const w = await setup(runInput({ answers: loadAnswers() }), { narrative: client });
    assert.equal((await w.deliver()).detail, `Run ${RUN_ID} is in review`);
    assert.ok(client.sections > 10);
    assert.ok(!w.deps.lines.includes(WAITING_LINE));
  });

  test("with no client, neither delivery drafts and neither logs the line", async () => {
    const w = await setup();
    await w.deliver();
    consoleAnswers(w.store, [answer(EAC), answer(DWU)]);
    await w.deliver();
    assert.ok(w.store.saved.every((o) => o.versions.every((v) => v.source === "engine")));
    assert.ok(!w.deps.lines.includes(WAITING_LINE));
  });

  test("with the Apex switch on, a delivery awaiting manual steps still asks for the summaries, and keeps them", async () => {
    const client = claude();
    const w = await setup(runInput(), { narrative: client, apexSummaries: true });
    assert.equal((await w.deliver()).detail, `Run ${RUN_ID} is awaiting manual`);
    assert.ok(client.summaries > 0);
    assert.equal(client.sections, 0);
    assert.ok(existsSync(join(w.dir, SUMMARIES_FILE)));
    assert.ok(w.store.saved[0]!.handbookSummaries!.items.length > 0);
  });
});

/** Every data request refused as an expired session. */
const expired: Transport = { send: async (): Promise<TransportResponse> => ({ status: 401, header: () => null, text: JSON.stringify([{ errorCode: "INVALID_SESSION_ID", message: "Session expired or invalid" }]) }) };

describe("a connection revoked between the first delivery and the answers (DECISIONS.md 547 (2), 617)", () => {
  test("fails the run at once as needs reconnection, keeping the checkpoints and the answers; a new connection then finishes it", async () => {
    const w = await setup();
    await w.deliver();
    assert.ok(consoleAnswers(w.store, [answer(EAC), answer(DWU)]));
    const checkpoints = listing(join(w.dir, "checkpoints"));
    assert.ok(checkpoints.size > 10);
    // Salesforce refuses the refresh token: it was revoked. A refresh goes only
    // to a Salesforce host, which the synthetic connection's isn't, so the
    // connection names one here; every data request is refused anyway.
    await w.deps.tokens.save(syntheticConnection({ instanceUrl: FAKE.instanceUrl }));
    const endpoint = new FakeTokenEndpoint();
    w.deps.oauthTransport = endpoint;
    // No retry: such a token never works again, so the first delivery ends it (DECISIONS.md 617).
    assert.deepEqual(await w.deliver(false, expired), { outcome: "failed", detail: needsReconnection(CONNECTION) });
    assert.deepEqual(endpoint.requests.map((r) => r.form.get("grant_type")), ["refresh_token"], "one refused refresh");
    assert.equal(w.stored().run.status, "failed");
    assert.equal(w.stored().run.steps.find((s) => s.id === "worker")!.error, needsReconnection(CONNECTION));
    assert.deepEqual(listing(join(w.dir, "checkpoints")), checkpoints);
    assert.deepEqual(w.stored().answers.map((a) => a.stepKey).sort(), [DWU, EAC]);

    // The admin connects again, and "Queue again".
    w.deps.oauthTransport = undefined;
    await w.deps.tokens.save(syntheticConnection());
    assert.deepEqual(await w.deliver(), { outcome: "done", detail: `Run ${RUN_ID} is in review` });
    assert.deepEqual(paths(w.requests()), ONLY_START);
    assert.deepEqual(stepsSaved(w.store), { [EAC]: "answered", [DWU]: "answered" });
  });
});

describe("the second reading (DECISIONS.md 542)", () => {
  test("a run gone between the readings: skipped, and the folder as it was", async () => {
    const w = await setup();
    await w.deliver();
    // The Apex code kept for summaries, which a failure would delete.
    writeFileSync(join(w.dir, APEX_SOURCE_FILE), '{"planted":true}\n', { mode: 0o600 });
    const before = listing(w.dir);
    const writes = w.store.writes.length;
    w.store.readings = 0;
    w.store.reading = (n, input) => (n === 2 ? null : input);
    assert.deepEqual(await w.deliver(), { outcome: "skipped", detail: `No run ${RUN_ID}` });
    assert.equal(w.store.readings, 2);
    assert.deepEqual(listing(w.dir), before);
    assert.deepEqual(w.store.writes.slice(writes).map((x) => x.method), ["claimRun"]);
  });

  test("the folder's survey and interviews are the second reading's", async () => {
    const w = await setup(runInput({ survey: syntheticSurvey(6), interviews: [] }));
    w.store.reading = (n, input) => (n === 2 && input ? { ...input, survey: syntheticSurvey(12), interviews: syntheticInterviews() } : input);
    assert.equal((await w.deliver()).outcome, "done");
    assert.equal(w.store.readings, 2);
    assert.equal(JSON.parse(readFileSync(join(w.dir, SURVEY_FILE), "utf8")).responses.length, 12);
    assert.deepEqual(JSON.parse(readFileSync(join(w.dir, INTERVIEWS_FILE), "utf8")).interviews.map((i: { id: string }) => i.id), syntheticInterviews().map((i) => i.id));
  });

  test("a second reading with a different calibration is refused as a resume is", async () => {
    const w = await setup();
    await w.deliver();
    const changed = { ...loadCalibration(), analysisWindowMonths: loadCalibration().analysisWindowMonths + 1 };
    w.store.readings = 0;
    w.store.reading = (n, input) => (n === 2 && input ? { ...input, calibration: changed } : input);
    const outcome = await w.deliver();
    assert.equal(outcome.outcome, "failed");
    assert.match(outcome.detail, /^The run stopped with an error: The calibration has changed since this run started\./);
  });
});

/** A second reading that throws as a dropped database connection does. */
const dropped = (n: number, input: RunJobInput | null): RunJobInput | null => {
  if (n === 2) throw new Error("Connection terminated unexpectedly");
  return input;
};

describe("an error from the second reading (DECISIONS.md 548 (1))", () => {
  test("before the last attempt it goes back to the queue with the run analysing, and the next delivery claims and finishes as any does", async () => {
    const w = await setup();
    await w.deliver();
    assert.equal(w.stored().run.status, "awaiting_manual");
    w.store.readings = 0;
    w.store.reading = dropped;
    await assert.rejects(w.deliver(false), (err: Error) => err.message === "Connection terminated unexpectedly");
    assert.equal(w.stored().run.status, "analysing");
    w.store.readings = 0;
    w.store.reading = (_n, input) => input;
    const writes = w.store.writes.length;
    assert.deepEqual(await w.deliver(false), { outcome: "done", detail: `Run ${RUN_ID} is awaiting manual` });
    assert.equal(w.store.readings, 2);
    assert.equal(w.store.writes[writes]!.method, "claimRun");
    assert.equal(w.stored().run.status, "awaiting_manual");
  });

  test("on the last attempt the run fails with the reason, and the kept Apex code goes as any failure deletes it", async () => {
    const w = await setup();
    await w.deliver();
    writeFileSync(join(w.dir, APEX_SOURCE_FILE), '{"planted":true}\n', { mode: 0o600 });
    w.store.readings = 0;
    w.store.reading = dropped;
    const reason = "The run stopped with an error: Connection terminated unexpectedly";
    assert.deepEqual(await w.deliver(true), { outcome: "failed", detail: reason });
    assert.equal(w.stored().run.status, "failed");
    assert.equal(w.stored().run.steps.find((s) => s.id === "worker")!.error, reason);
    assert.equal(existsSync(join(w.dir, APEX_SOURCE_FILE)), false);
  });
});

describe("every field the job takes from the run is the second reading's (DECISIONS.md 548 (4))", () => {
  /** A setup whose second reading changes the run job input as given. */
  async function changed(change: (input: RunJobInput) => RunJobInput, first: RunJobInput = runInput(), overrides: Parameters<typeof baseDeps>[0] = {}) {
    const w = await setup(first, overrides);
    w.store.reading = (n, input) => (n === 2 && input ? change(structuredClone(input)) : input);
    return w;
  }
  const withRun = (input: RunJobInput, run: Partial<RunJobInput["run"]>): RunJobInput => ({ ...input, run: consoleRun({ ...input.run, ...run }) });

  test("the intake written to the folder", async () => {
    const w = await changed((input) => ({ ...input, intake: { ...INTAKE, summary: "Renewals are missed." } }));
    assert.equal((await w.deliver()).outcome, "done");
    assert.equal(JSON.parse(readFileSync(join(w.dir, "intake.json"), "utf8")).summary, "Renewals are missed.");
  });

  test("the connection loaded", async () => {
    const w = await changed((input) => ({ ...input, connectionName: "synthetic-co-later" }));
    assert.deepEqual(await w.deliver(), { outcome: "failed", detail: `The connection "synthetic-co-later" couldn't be loaded: No stored connection called "synthetic-co-later"` });
  });

  test("what's checked before the run starts: a calibration since withdrawn", async () => {
    const w = await changed((input) => ({ ...input, calibration: null }));
    assert.deepEqual(await w.deliver(), { outcome: "failed", detail: "The calibration hasn't been confirmed, so the run can't start." });
  });

  test("the org the connection must match", async () => {
    const w = await changed((input) => withRun(input, { orgId: sfId("00D", 2) }));
    assert.deepEqual(await w.deliver(), { outcome: "failed", detail: `The connection "${CONNECTION}" is for a different org from this run, so nothing was read.` });
  });

  test("the modules run", async () => {
    const w = await changed((input) => withRun(input, { modules: ["configuration"] }));
    assert.equal((await w.deliver()).outcome, "done");
    assert.deepEqual(JSON.parse(readFileSync(join(w.dir, "run.json"), "utf8")).modules, ["configuration"]);
  });

  test("the product: a scan on the second reading keeps no Apex code, drafts nothing, and is finished as a scan", async () => {
    const client = claude();
    const w = await changed((input) => withRun(input, { product: "scan", scope: "essentials" }), runInput({ answers: loadAnswers() }), { narrative: client, apexSummaries: true });
    assert.deepEqual(await w.deliver(), { outcome: "done", detail: `Run ${RUN_ID} is in review` });
    assert.equal(w.stored().run.status, "in_review");
    assert.equal(client.summaries, 0);
    assert.equal(client.sections, 0);
    assert.ok(w.store.scanResults.has(RUN_ID));
  });

  test("the baseline a re-check names", async () => {
    const w = await changed((input) => withRun(input, { baselineRunId: "hc-2026-09-30-00000001" }));
    assert.equal((await w.deliver()).outcome, "done");
    assert.equal(JSON.parse(readFileSync(join(w.dir, "run.json"), "utf8")).baselineRunId, "hc-2026-09-30-00000001");
    assert.deepEqual(JSON.parse(readFileSync(join(w.dir, BASELINE_READ_FILE), "utf8")), { read: false });
  });

  test("whether the org is a sandbox, in every progress saved", async () => {
    const w = await changed((input) => withRun(input, { isSandbox: true }));
    const progress: RunProgress[] = [];
    const save = w.store.saveRunProgress.bind(w.store);
    w.store.saveRunProgress = async (id, p, dir) => (progress.push(structuredClone(p)), save(id, p, dir));
    assert.equal((await w.deliver()).outcome, "done");
    assert.ok(progress.length > 0);
    assert.ok(progress.every((p) => p.isSandbox === true));
  });

  test("who created the run and the customer's reference, in run.json on a first delivery", async () => {
    const w = await changed((input) => withRun(input, { createdBy: "second-consultant@example.com", customerRef: "SYN-0002" }));
    assert.equal((await w.deliver()).outcome, "done");
    const run = JSON.parse(readFileSync(join(w.dir, "run.json"), "utf8"));
    assert.equal(run.createdBy, "second-consultant@example.com");
    assert.equal(run.customerRef, "SYN-0002");
  });
});
