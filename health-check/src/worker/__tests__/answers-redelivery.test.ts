import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { join, relative } from "node:path";
import { APEX_SOURCE_FILE } from "../../extract/apex-source.ts";
import { SUMMARIES_FILE } from "../../handbook/summaries.ts";
import { APEX_SUMMARY_PROMPT } from "../../narrative/apex-prompt.ts";
import type { NarrativeClient, NarrativeRequest } from "../../narrative/client.ts";
import { renderReleased, type ReleasedDocuments } from "../../render/render-released.ts";
import { INTERVIEWS_FILE, SURVEY_FILE } from "../../render/render-run.ts";
import type { ManualAnswer, ManualStep } from "../../schemas/index.ts";
import { FAKE, FakeTokenEndpoint } from "../../testing/fake-oauth.ts";
import { docxText } from "../../testing/docx-text.ts";
import { syntheticInterviews } from "../../testing/interviews.ts";
import { loadAnswers, loadCalibration, loadRecording, ReplayTransport } from "../../testing/replay.ts";
import { syntheticSurvey } from "../../testing/survey.ts";
import type { Transport, TransportRequest, TransportResponse } from "../../salesforce/transport.ts";
import type { DocumentDrive, DriveItem } from "../drive.ts";
import { processDocumentsJob } from "../documents-job.ts";
import { MemoryWorkerStore } from "../memory-store.ts";
import { processRunJob } from "../run-job.ts";
import type { RunJobInput, RunOutputRecord } from "../types.ts";
import { baseDeps, CONNECTION, INTAKE, RUN_ID, runInput, syntheticConnection } from "./helpers.ts";

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

describe("a connection revoked between the first delivery and the answers (DECISIONS.md 547 (2))", () => {
  test("throws for the queue, then fails the run with the connection's sentence, keeping the checkpoints and the answers; a new connection then finishes it", async () => {
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
    // An OAuthError's message starts with Salesforce's code (oauth.ts), so the reason does too.
    const sentence = `invalid_grant: The connection "${CONNECTION}" was revoked or expired. The customer's admin needs to connect again`;
    await assert.rejects(w.deliver(false, expired), (err: Error) => err.message === sentence);
    assert.deepEqual(await w.deliver(true, expired), { outcome: "failed", detail: `The run stopped with an error: ${sentence}` });
    assert.deepEqual(endpoint.requests.map((r) => r.form.get("grant_type")), ["refresh_token", "refresh_token"], "one refused refresh on each delivery");
    assert.equal(w.stored().run.status, "failed");
    assert.equal(w.stored().run.steps.find((s) => s.id === "worker")!.error, `The run stopped with an error: ${sentence}`);
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
