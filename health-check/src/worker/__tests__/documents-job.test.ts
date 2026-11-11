import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { generateKeyPairSync } from "node:crypto";
import { cpSync, existsSync, mkdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { buildReport, currentBody, type ReportOutputKey } from "../../narrative/sections.ts";
import { approvedReport, heldFigures, isPlainFigure, layoutWarnings, renderReleased, type ApprovedRun, type DocumentKey, type HeldFigure } from "../../render/render-released.ts";
import { loadRun, SURVEY_FILE } from "../../render/render-run.ts";
import { withoutComments } from "../../survey/questions.ts";
import { syntheticSurvey } from "../../testing/survey.ts";
import { ReportOutput, type ReportSection, type SectionVersion } from "../../schemas/index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { docxParagraphs, docxText } from "../../testing/docx-text.ts";
import { strFromU8, unzipSync } from "fflate";
import { loadPseudonymMap } from "../../render/render-run.ts";
import type { ConsoleSummariesRow } from "../../handbook/summary-row.ts";
import { HANDBOOK_FILE } from "../../handbook/build.ts";
import { HANDBOOK_DOCX, NO_EARLIER, SINCE_TITLE } from "../../handbook/docx.ts";
import { withEarlier } from "../../testing/earlier-handbook.ts";
import { HANDBOOK_NOT_MADE } from "../../handbook/read.ts";
import { HANDBOOK_MARKER } from "../../testing/synthetic-org.ts";
import { loadAnswers } from "../../testing/replay.ts";
import { clientNameOf, folderFor, heldBackReason, heldBackSummary, INTERNAL_FOLDER, laterSurveyAnswers, processDocumentsJob } from "../documents-job.ts";
import { DriveError, GoogleDrive, type DocumentDrive, type DriveItem } from "../drive.ts";
import { z } from "zod";
import type { DocumentFile, DocumentsJobInput } from "../types.ts";
import { baseDeps, consoleRun, INTAKE, RUN_ID, temp } from "./helpers.ts";

// The synthetic org's run folder, recorded once, stands in for one on the worker's disk.
const { dir: recorded } = await runFromRecording({ answers: loadAnswers() });

/** Every section approved, as the console holds it after release, with the verdict reworded by a consultant. */
function approvedRun(edit = "We reworded this verdict in the console."): ApprovedRun {
  const report = buildReport(loadRun(recorded).input);
  const sections: ReportSection[] = [];
  const versions: SectionVersion[] = [];
  for (const built of Object.values(report.outputs)) {
    for (const s of built.sections) {
      const v = built.versions.find((x) => x.id === s.currentVersionId)!;
      const reworded = s.key === "verdict" ? { ...v, id: `${v.id}-2`, version: 2, body: `${edit}\n\n${v.body}`, source: "consultant_edit" as const, changedBy: "consultant@example.com" } : v;
      sections.push({ ...s, status: "approved", currentVersionId: reworded.id, approval: { approvedBy: "consultant@example.com", approvedAt: "2026-11-04T18:00:00.000Z" } });
      versions.push(reworded);
    }
  }
  return { intake: { ...INTAKE, customerName: "Synthetic Co" }, sections, versions, overrides: [] };
}

class MemoryDrive implements DocumentDrive {
  readonly rootFolderId = "root-folder-0001";
  readonly items = new Map<string, DriveItem & { parent: string; folder: boolean; bytes?: number; uploads: number; mime?: string; body?: Buffer }>();
  #next = 1;
  #add(parent: string, name: string, folder: boolean): DriveItem {
    const id = `drive-item-${String(this.#next++).padStart(4, "0")}`;
    const item = { id, name, url: `https://drive.example.com/${id}`, parent, folder, uploads: 0, createdAt: "2026-11-04T18:00:00.000Z" };
    this.items.set(id, item);
    return item;
  }
  async folder(id: string) {
    const f = this.items.get(id);
    return f?.folder ? f : null;
  }
  async folders(parentId: string) {
    return [...this.items.values()].filter((i) => i.parent === parentId && i.folder);
  }
  async files(parentId: string) {
    return [...this.items.values()].filter((i) => i.parent === parentId && !i.folder);
  }
  async createFolder(parentId: string, name: string) {
    return this.#add(parentId, name, true);
  }
  async putFile(parentId: string, name: string, mime: string, body: Buffer, replaceId?: string) {
    const item = replaceId ? this.items.get(replaceId)! : (this.#add(parentId, name, false) as DriveItem & { uploads: number; bytes?: number });
    Object.assign(item, { name, mime, body, bytes: body.length, uploads: (item as { uploads: number }).uploads + 1 });
    return item;
  }
  async moveFile(id: string, from: string, to: string) {
    const item = this.items.get(id)!;
    assert.equal(item.parent, from);
    item.parent = to;
  }
  path(id: string): string {
    const parts: string[] = [];
    for (let item = this.items.get(id); item; item = this.items.get(item.parent)) parts.unshift(item.name);
    return parts.join(" / ");
  }
}

/** Prints placeholder files, so the job's tests need no browser. */
async function fakeRender(_dir: string, _approved: ApprovedRun, out: string) {
  mkdirSync(out, { recursive: true });
  const files = {} as Record<DocumentKey, string>;
  for (const output of ["raw_notes", "executive_summary", "full_health_check", "ceo_one_pager", "action_lists", "tickets", "readout_kit"] as DocumentKey[]) {
    files[output] = join(out, `${output}.bin`);
    writeFileSync(files[output], `synthetic ${output}`);
  }
  return { files, warnings: ["The full health check, page 4: overflow"], held: [] as HeldFigure[] };
}

function jobInput(overrides: Partial<DocumentsJobInput> = {}): DocumentsJobInput {
  const { sections, versions } = approvedRun();
  return {
    run: consoleRun({ status: "released", customerRef: "SYN-0001" }),
    intake: { ...INTAKE, customerName: "Synthetic Co" },
    sections,
    versions,
    overrides: [],
    preparedBy: null,
    state: "queued",
    driveFolderId: null,
    files: [],
    ...overrides,
  };
}

function setup(input: DocumentsJobInput = jobInput(), drive: DocumentDrive | null = new MemoryDrive()) {
  const deps = baseDeps({ drive, renderReleased: fakeRender });
  // The run folder the run job left, under the worker's own runs folder.
  mkdirSync(join(deps.runsDir, input.run.id), { recursive: true });
  writeFileSync(join(deps.runsDir, input.run.id, "run.json"), "{}");
  deps.store.addDocumentsJob(input);
  return deps;
}

describe("a documents job", () => {
  test("puts the documents in Health checks / client / Health check 1, and tells the console", async () => {
    const drive = new MemoryDrive();
    const deps = setup(jobInput(), drive);
    const result = await processDocumentsJob(RUN_ID, deps, { final: true });
    assert.equal(result.outcome, "done", result.detail);
    const made = deps.store.documents.get(RUN_ID)!.made!;
    assert.equal(made.clientName, "Synthetic Co");
    assert.equal(made.folderName, "Health check 1");
    assert.equal(drive.path(made.driveFolderId), "Synthetic Co / Health check 1");
    // The action lists go beside the decks, and the console's documents card is told of them, with their own outputs (DECISIONS.md 197).
    assert.deepEqual(made.files.map((f) => f.name).sort(), ["Synthetic Co - Action lists.xlsx", "Synthetic Co - Executive summary.pdf", "Synthetic Co - Full health check.pdf", "Synthetic Co - One page summary.pdf", "Synthetic Co - Raw notes.docx", "Synthetic Co - Tickets.csv"]);
    assert.deepEqual(made.files.filter((f) => f.name.endsWith(".xlsx") || f.name.endsWith(".csv")).map((f) => f.output).sort(), ["action_lists", "tickets"]);
    const inFolder = (await drive.files(made.driveFolderId)).map((f) => f.id);
    for (const f of made.files) assert.ok(inFolder.includes(f.driveFileId), `${f.name} is the file in the folder`);
    // This run folder has no handbook.json, so the warnings say why there's no handbook (DECISIONS.md 445).
    assert.deepEqual(made.warnings, ["The full health check, page 4: overflow", HANDBOOK_NOT_MADE.missing]);
    assert.equal(deps.store.documents.get(RUN_ID)!.input.state, "done");
  });

  test("a client's next health check is number 2, in the same client folder, whatever the case of its name", async () => {
    const drive = new MemoryDrive();
    await processDocumentsJob(RUN_ID, setup(jobInput(), drive), { final: true });
    const second = jobInput({ run: consoleRun({ id: "hc-2026-12-01-00000002", status: "released" }), intake: { ...INTAKE, customerName: "synthetic co " } });
    const deps = setup(second, drive);
    assert.equal((await processDocumentsJob(second.run.id, deps, { final: true })).outcome, "done");
    assert.equal(deps.store.documents.get(second.run.id)!.made!.folderName, "Health check 2");
    // One client folder, beside Black Cloud's own.
    assert.deepEqual((await drive.folders(drive.rootFolderId)).map((f) => f.name).sort(), [INTERNAL_FOLDER, "Synthetic Co"]);
  });

  test("made again, the first make moves into Version 1, keeping its links, and the new one goes into Version 2", async () => {
    const drive = new MemoryDrive();
    const deps = setup(jobInput(), drive);
    const doc = deps.store.documents.get(RUN_ID)!;
    await processDocumentsJob(RUN_ID, deps, { final: true });
    const first = doc.made!;
    doc.input.state = "queued";
    const result = await processDocumentsJob(RUN_ID, deps, { final: true });
    assert.equal(result.outcome, "done", result.detail);
    const again = doc.made!;
    // The run's folder and its link stay the same; the record points at the newest files.
    assert.equal(again.driveFolderId, first.driveFolderId);
    assert.equal(again.folderName, "Health check 1");
    for (const f of first.files) assert.equal(drive.path(f.driveFileId), `Synthetic Co / Health check 1 / Version 1, 4 Nov 2026 / ${f.name}`);
    for (const f of again.files) assert.match(drive.path(f.driveFileId), /^Synthetic Co \/ Health check 1 \/ Version 2, \d{1,2} [A-Z][a-z]{2} \d{4} \/ Synthetic Co - /);
    assert.equal((await drive.files(first.driveFolderId)).length, 0, "nothing left loose in the run's folder");
    // A third make adds Version 3 and leaves the others alone.
    doc.input.state = "queued";
    await processDocumentsJob(RUN_ID, deps, { final: true });
    assert.deepEqual((await drive.folders(first.driveFolderId)).map((f) => f.name.split(",")[0]).sort(), ["Version 1", "Version 2", "Version 3"]);
  });

  test("a make that failed half way is finished by the retry, not started again in a new version", async () => {
    const drive = new MemoryDrive();
    const deps = setup(jobInput(), drive);
    const doc = deps.store.documents.get(RUN_ID)!;
    await processDocumentsJob(RUN_ID, deps, { final: true });
    doc.input.state = "queued";
    const put = drive.putFile.bind(drive);
    let calls = 0;
    drive.putFile = async (...args) => {
      if (++calls === 2) throw new Error("Google Drive refused a request (503): backend error");
      return put(...args);
    };
    await assert.rejects(processDocumentsJob(RUN_ID, deps, { final: false }), /503/);
    drive.putFile = put;
    assert.equal((await processDocumentsJob(RUN_ID, deps, { final: true })).outcome, "done");
    const folders = (await drive.folders(doc.made!.driveFolderId)).map((f) => f.name.split(",")[0]).sort();
    assert.deepEqual(folders, ["Version 1", "Version 2"]);
    const v2 = (await drive.folders(doc.made!.driveFolderId)).find((f) => f.name.startsWith("Version 2"))!;
    assert.equal((await drive.files(v2.id)).length, 6, "the three documents, the one-pager and the two action lists, none twice");
  });

  test("with its folder gone from Drive, the run gets the client's next number", async () => {
    const drive = new MemoryDrive();
    const deps = setup(jobInput(), drive);
    await processDocumentsJob(RUN_ID, deps, { final: true });
    const doc = deps.store.documents.get(RUN_ID)!;
    const gone = doc.made!.driveFolderId;
    drive.items.delete(gone);
    doc.input.state = "queued";
    await processDocumentsJob(RUN_ID, deps, { final: true });
    assert.equal(doc.made!.folderName, "Health check 1");
    assert.notEqual(doc.made!.driveFolderId, gone);
  });

  test("does nothing for a run that isn't released, or documents nobody asked for", async () => {
    const unreleased = setup(jobInput({ run: consoleRun({ status: "in_review" }) }));
    assert.equal((await processDocumentsJob(RUN_ID, unreleased, { final: true })).outcome, "skipped");
    const done = setup(jobInput({ state: "done" }));
    assert.equal((await processDocumentsJob(RUN_ID, done, { final: true })).outcome, "skipped");
    assert.deepEqual(done.store.writes, []);
  });

  test("without Drive set up, fails saying which variables to set", async () => {
    const deps = setup(jobInput(), null);
    const result = await processDocumentsJob(RUN_ID, deps, { final: true });
    assert.equal(result.outcome, "failed");
    assert.match(deps.store.documents.get(RUN_ID)!.error!, /HC_DRIVE_SA_EMAIL, HC_DRIVE_SA_KEY and HC_DRIVE_FOLDER_ID/);
  });

  test("without the run's folder on the worker, fails saying so", async () => {
    const deps = baseDeps({ drive: new MemoryDrive(), renderReleased: fakeRender });
    deps.store.addDocumentsJob(jobInput());
    await processDocumentsJob(RUN_ID, deps, { final: true });
    assert.match(deps.store.documents.get(RUN_ID)!.error!, /folder isn't on the worker/);
  });

  test("a Drive error is retried while the queue will deliver again, then recorded", async () => {
    const drive = new MemoryDrive();
    drive.createFolder = async () => {
      throw new DriveError("Google Drive refused a request (403): The user does not have sufficient permissions");
    };
    const deps = setup(jobInput(), drive);
    await assert.rejects(processDocumentsJob(RUN_ID, deps, { final: false }), /sufficient permissions/);
    assert.equal(deps.store.documents.get(RUN_ID)!.input.state, "making");
    const result = await processDocumentsJob(RUN_ID, deps, { final: true });
    assert.equal(result.outcome, "failed");
    assert.match(deps.store.documents.get(RUN_ID)!.error!, /^The documents couldn't be made: .*sufficient permissions/);
  });

  test("hands the renderer the consultant the cover names, as the console holds it", async () => {
    const seen: ApprovedRun[] = [];
    const deps = setup(jobInput({ preparedBy: "Synthetic Consultant" }));
    deps.renderReleased = async (dir, approved, out) => (seen.push(approved), fakeRender(dir, approved, out));
    assert.equal((await processDocumentsJob(RUN_ID, deps, { final: true })).outcome, "done");
    assert.equal(seen[0]!.preparedBy, "Synthetic Consultant");
  });

  test("the client's folder is the company's name, else the reference", () => {
    assert.equal(clientNameOf({ intake: { ...INTAKE, customerName: "  Synthetic   Co " }, run: consoleRun() }), "Synthetic Co");
    assert.equal(clientNameOf({ intake: INTAKE, run: consoleRun({ customerRef: "SYN-0001" }) }), "SYN-0001");
    assert.equal(clientNameOf({ intake: null, run: consoleRun({ customerRef: null }) }), "Unnamed client");
  });

  test("a page whose figure can't be traced holds the documents back", async () => {
    const drive = new MemoryDrive();
    let uploads = 0;
    const put = drive.putFile.bind(drive);
    drive.putFile = async (...args) => (uploads++, put(...args));
    const folder = "folder-0000000001";
    // The recorded run folder, opted in and banded, so the benchmark would take its figures if the job got that far.
    const deps = baseDeps({ drive, renderReleased: fakeRender });
    const input = jobInput({ driveFolderId: folder, intake: { ...INTAKE, customerName: "Synthetic Co", industry: "technology", headcountBand: "51-200" }, run: consoleRun({ status: "released", customerRef: "SYN-0001", benchmarkOptIn: true }) });
    cpSync(recorded, join(deps.runsDir, input.run.id), { recursive: true });
    deps.store.addDocumentsJob(input);
    deps.micrositeKey = Buffer.alloc(32, 9);
    deps.renderReleased = async (dir, approved, out) => ({
      ...(await fakeRender(dir, approved, out)),
      warnings: ["The full health check, page 12: number check: £1.99m: not in the data"],
      held: [{ document: "The full health check", page: 12, figure: "£1.99m", reason: "not in the data" }],
      microsite: { ok: true as const, pages: { pages: [{ slug: "overview", title: "Overview", html: "<p>Synthetic.</p>" }] } },
    });
    // A reader taken off the list still loses access, as on any failure, and a reader added since isn't given the last make's folder.
    deps.store.readers.set(RUN_ID, ["new@customer.example.com"]);
    deps.store.shares = [{ runId: RUN_ID, email: "gone@customer.example.com", folderId: folder, permissionId: "perm-0001", state: "shared", sharedAt: "2026-11-01T09:00:00.000Z", removedAt: null, removedWhy: null, error: null, updatedAt: "2026-11-01T09:00:00.000Z" }];
    const taken: string[] = [];
    const given: string[] = [];
    deps.sharing = { share: async (_f, email) => (given.push(email), "perm-0002"), findReader: async () => null, unshare: async (_f, id) => void taken.push(id) };
    // Not the queue's last try, and still no throw: the same wording would fail again.
    const result = await processDocumentsJob(RUN_ID, deps, { final: false });
    assert.equal(result.outcome, "failed");
    assert.equal(uploads, 0, "nothing uploaded");
    assert.deepEqual([...drive.items.values()], [], "no folder made either");
    assert.equal(deps.store.microsites.size, 0, "no microsite");
    assert.equal(deps.store.benchmarkRows.length, 0, "nothing added to the benchmark set");
    const doc = deps.store.documents.get(RUN_ID)!;
    assert.equal(doc.input.state, "failed");
    assert.ok(!doc.made, "no documents recorded as made");
    for (const part of ["The full health check", "page 12", "£1.99m", "not in the data"]) assert.ok(doc.error!.includes(part), doc.error!);
    assert.deepEqual(taken, ["perm-0001"]);
    assert.equal(deps.store.shares[0]!.state, "removed");
    assert.deepEqual(given, [], "nobody new is given the folder");
    // The log and the queue get counts only; the figure stays in the error the console shows.
    assert.equal(result.detail, "Held back: 1 figure on 1 page can't be traced to the data. See the run's Release tab.");
    assert.ok(deps.lines.some((l) => l.includes("weren't uploaded") && l.includes(result.detail!)), deps.lines.join("\n"));
    for (const line of [...deps.lines, result.detail!]) assert.ok(!line.includes("£1.99m") && !line.includes("not in the data"), line);
  });

  test("a held value that isn't a plain figure is masked everywhere, and logged nowhere", async () => {
    const values = ["001000000000001AAA", "jo@example.com", "https://example.my.salesforce.com/001000000000001"];
    const deps = setup();
    deps.renderReleased = async (dir, approved, out) => {
      const made = await fakeRender(dir, approved, out);
      const executive = { pages: 6, passed: false, problems: values.map((v, i) => ({ page: i + 2, check: "number check", detail: `${v}: this id isn't in the data` })) };
      const full = { pages: 40, passed: false, problems: [{ page: 12, check: "number check", detail: "£1.99m: not in the data" }] };
      return { ...made, warnings: layoutWarnings({ executive, full }), held: heldFigures({ executive, full }) };
    };
    const result = await processDocumentsJob(RUN_ID, deps, { final: true });
    assert.equal(result.outcome, "failed");
    const error = deps.store.documents.get(RUN_ID)!.error!;
    assert.match(error, /^Held back: 4 figures on 4 pages /);
    assert.equal(error.match(/an identifier: this id isn't in the data/g)?.length, 3, error);
    assert.ok(error.includes("The full health check, page 12: £1.99m: not in the data"), error);
    for (const text of [error, result.detail!, ...deps.lines]) {
      for (const v of [...values, "001000000000001", "example.com"]) assert.ok(!text.includes(v), `${v} in ${text}`);
    }
    for (const text of [result.detail!, ...deps.lines]) assert.ok(!text.includes("£1.99m"), text);
  });

  test("a layout problem that isn't a number check still uploads, with the warning", async () => {
    const drive = new MemoryDrive();
    const deps = setup(jobInput(), drive);
    deps.renderReleased = async (dir, approved, out) => ({ ...(await fakeRender(dir, approved, out)), warnings: ["The full health check, page 4: overflow"], held: [] });
    assert.equal((await processDocumentsJob(RUN_ID, deps, { final: true })).outcome, "done");
    const made = deps.store.documents.get(RUN_ID)!.made!;
    assert.equal((await drive.files(made.driveFolderId)).length, 6);
    assert.deepEqual(made.warnings, ["The full health check, page 4: overflow", HANDBOOK_NOT_MADE.missing]);
  });

  test("numbers past any health check folder already there, and ignores other folders", async () => {
    const drive = new MemoryDrive();
    const client = await drive.createFolder(drive.rootFolderId, "Synthetic Co");
    await drive.createFolder(client.id, "Health check 3");
    await drive.createFolder(client.id, "Notes");
    assert.equal((await folderFor(drive, "Synthetic Co", null)).name, "Health check 4");
  });
});

describe("the released documents", () => {
  test("carry the console's approved wording, not the engine's draft", () => {
    const { report } = approvedReport(recorded, approvedRun("Reworded in the console for this test."));
    assert.match(currentBody(report.outputs.executive_summary, "verdict"), /^Reworded in the console for this test\./);
  });

  test("aren't made while any section is unapproved", () => {
    const approved = approvedRun();
    approved.sections[3] = { ...approved.sections[3]!, status: "in_review", approval: null };
    assert.throws(() => approvedReport(recorded, approved), /isn't|is approved/);
  });

  test("print as two PDFs and a Word file, and say what the layout checks found without quoting the pages", async () => {
    const dir = join(temp(), RUN_ID);
    cpSync(recorded, dir, { recursive: true });
    const result = await renderReleased(dir, { ...approvedRun(), preparedBy: "Synthetic Consultant" }, join(dir, "documents"));
    for (const file of Object.values(result.files)) assert.ok(existsSync(file) && statSync(file).size > 1000, file);
    // Every figure on the approved pages traces to the data, so nothing holds them back, and no warning carries a detail.
    assert.deepEqual(result.held, []);
    assert.ok(result.warnings.every((w) => /^The (executive summary|full health check)(, page \d+)?: [a-z ]+$/.test(w)), JSON.stringify(result.warnings));
    // The cover has room for the consultant's line.
    assert.deepEqual(result.warnings.filter((w) => /, page 1:/.test(w)), []);
  });

  test("layout warnings leave the detail out, except a number check's figure and reason", () => {
    const warnings = layoutWarnings({
      executive: { pages: 6, passed: false, problems: [{ page: 2, check: "empty section", detail: 'only "Ada Lovelace"' }] },
      full: { pages: 40, passed: false, problems: [{ page: 0, check: "page count", detail: "61 pages" }, { page: 12, check: "number check", detail: "£1.99m: not in the data" }] },
    });
    assert.deepEqual(warnings, ["The executive summary, page 2: empty section", "The full health check: page count", "The full health check, page 12: number check: £1.99m: not in the data"]);
  });

  test("the figures held back are the number check's alone, from every document", () => {
    const layout = {
      executive: { pages: 6, passed: false, problems: [{ page: 4, check: "number check", detail: "31.4: not in the data" }, { page: 5, check: "overflow", detail: "p: Ada Lovelace owns 40 deals" }] },
      full: { pages: 40, passed: false, problems: [{ page: 12, check: "number check", detail: "£1.99m: can't be £1.99m of £1.2m" }, { page: 13, check: "leftover", detail: "NaN" }] },
      ceo: { pages: 1, passed: false, problems: [{ page: 1, check: "number check", detail: "(whole text): The number check failed to run: boom" }] },
    };
    assert.deepEqual(heldFigures(layout), [
      { document: "The executive summary", page: 4, figure: "31.4", reason: "not in the data" },
      // Split at the first ": " only, so a reason that has one keeps it.
      { document: "The full health check", page: 12, figure: "£1.99m", reason: "can't be £1.99m of £1.2m" },
      { document: "The one page summary", page: 1, figure: "(whole text)", reason: "The number check failed to run: boom" },
    ]);
    assert.deepEqual(layoutWarnings(layout).filter((w) => /overflow|leftover/.test(w)), ["The executive summary, page 5: overflow", "The full health check, page 13: leftover"]);
    assert.deepEqual(heldFigures({ executive: { pages: 6, passed: true, problems: [] }, full: { pages: 40, passed: false, problems: [{ page: 3, check: "overflow", detail: "x" }] } }), []);
  });

  test("only a plain figure is repeated; an id, an email or a web address is masked", () => {
    for (const raw of ["£1.99m", "31.4", "2,423,000", "43%", "10.32x", "90 days", "(whole text)", "-12%", "1.5 percentage points", "12-15", "31 March 2026", "half", "0.5", "£250k"]) assert.ok(isPlainFigure(raw), raw);
    for (const raw of ["001000000000001AAA", "001000000000001", "jo@example.com", "f-0001", "https://example.my.salesforce.com/001000000000001", "www.example.com", "hc-2026-11-01-0000000a", "Q3", "1.2.3", "May", "Ada 12", "1234567890"]) assert.ok(!isPlainFigure(raw), raw);
    const held = heldFigures({ executive: { pages: 6, passed: false, problems: [{ page: 3, check: "number check", detail: "001000000000001AAA: this id isn't in the data" }] }, full: { pages: 40, passed: true, problems: [] } });
    assert.deepEqual(held, [{ document: "The executive summary", page: 3, figure: "an identifier", reason: "this id isn't in the data", masked: true }]);
    assert.deepEqual(layoutWarnings({ executive: { pages: 6, passed: false, problems: [{ page: 3, check: "number check", detail: "jo@example.com: this id isn't in the data" }] }, full: { pages: 40, passed: true, problems: [] } }), ["The executive summary, page 3: number check: an identifier: this id isn't in the data"]);
  });

  test("the reason the documents were held back names each figure, ten at most, one line each", () => {
    const one: HeldFigure = { document: "The full health check", page: 12, figure: "£1.99m", reason: "not in the data" };
    assert.equal(
      heldBackReason([one, { document: "The executive summary", page: 4, figure: "31.4", reason: "not in the data" }]),
      "Held back: 2 figures on 2 pages can't be traced to the data.\nThe full health check, page 12: £1.99m: not in the data.\nThe executive summary, page 4: 31.4: not in the data.\nEdit the wording in those sections, approve them again, and make the documents again.",
    );
    assert.match(heldBackReason([one]), /^Held back: 1 figure on 1 page can't be traced/);
    const many = Array.from({ length: 13 }, (_, i) => ({ ...one, figure: `£${i + 1}m`, page: i < 2 ? 12 : 20 }));
    const reason = heldBackReason(many);
    assert.match(reason, /^Held back: 13 figures on 2 pages /);
    assert.match(reason, /\n.*£10m: not in the data\.\nAnd 3 more\.\nEdit the wording/);
    assert.equal(reason.split("\n").length, 13, "the count, ten figures, the rest counted, then what to do");
    assert.doesNotMatch(reason, /£11m/);
    // The checker's own error message is cut short.
    const long = heldBackReason([{ document: "The full health check", page: 12, figure: "(whole text)", reason: `The number check failed to run: ${"x".repeat(500)}` }]);
    assert.ok(long.includes(`\nThe full health check, page 12: (whole text): The number check failed to run: ${"x".repeat(200 - 32)}.\nEdit`), long);
    assert.equal(heldBackSummary([one, one]), "Held back: 2 figures on 1 page can't be traced to the data. See the run's Release tab.");
  });

  test("a newline or a run of spaces inside a held reason doesn't start a line of its own", () => {
    const one: HeldFigure = { document: "The full health check", page: 12, figure: "£1.99m", reason: "it\nbroke\r\n\n  badly." };
    const lines = heldBackReason([one]).split("\n");
    assert.deepEqual(lines, [
      "Held back: 1 figure on 1 page can't be traced to the data.",
      "The full health check, page 12: £1.99m: it broke badly.",
      "Edit the wording in those sections, approve them again, and make the documents again.",
    ]);
  });
});

describe("the one-pager and the readout kit", () => {
  test("the one-pager goes in the customer's folder, the kit only in Black Cloud's own, and the console hears of the other six, never the kit", async () => {
    const drive = new MemoryDrive();
    const deps = setup(jobInput(), drive);
    assert.equal((await processDocumentsJob(RUN_ID, deps, { final: true })).outcome, "done");
    const made = deps.store.documents.get(RUN_ID)!.made!;
    const customer = (await drive.files(made.driveFolderId)).map((f) => f.name).sort();
    assert.deepEqual(customer, ["Synthetic Co - Action lists.xlsx", "Synthetic Co - Executive summary.pdf", "Synthetic Co - Full health check.pdf", "Synthetic Co - One page summary.pdf", "Synthetic Co - Raw notes.docx", "Synthetic Co - Tickets.csv"]);
    const all = [...drive.items.values()];
    const kits = all.filter((i) => !i.folder && /Readout kit/.test(i.name));
    assert.equal(kits.length, 1);
    assert.equal(drive.path(kits[0]!.id), `${INTERNAL_FOLDER} / Synthetic Co - Health check 1 / Synthetic Co - Readout kit.docx`);
    // Nowhere under the client's folder, so sharing that folder can never share the kit.
    assert.ok(!drive.path(kits[0]!.id).startsWith("Synthetic Co /"));
    assert.deepEqual(made.files.map((f) => f.output).sort(), ["action_lists", "ceo_one_pager", "executive_summary", "full_health_check", "raw_notes", "tickets"], "everything in the client's folder, and never the readout kit");
  });

  test("made again, the one-pager moves into Version 1 with the rest, and the kit is replaced, not copied", async () => {
    const drive = new MemoryDrive();
    const deps = setup(jobInput(), drive);
    await processDocumentsJob(RUN_ID, deps, { final: true });
    const doc = deps.store.documents.get(RUN_ID)!;
    doc.input = { ...doc.input, state: "queued", driveFolderId: doc.made!.driveFolderId, files: doc.made!.files };
    await processDocumentsJob(RUN_ID, deps, { final: true });
    const v1 = (await drive.folders(doc.made!.driveFolderId)).find((f) => f.name.startsWith("Version 1"))!;
    assert.ok((await drive.files(v1.id)).some((f) => f.name === "Synthetic Co - One page summary.pdf"));
    assert.deepEqual((await drive.files(doc.made!.driveFolderId)).map((f) => f.name), [], "nothing left loose in the run's folder");
    const kits = [...drive.items.values()].filter((i) => !i.folder && /Readout kit/.test(i.name));
    assert.equal(kits.length, 1);
    assert.equal((kits[0] as { uploads: number }).uploads, 2);
  });
});

describe("sharing, after the documents are made", () => {
  test("the run's new folder is shared with its readers, and a sharing failure doesn't fail the documents", async () => {
    const drive = new MemoryDrive();
    const deps = setup(jobInput(), drive);
    deps.store.readers.set(RUN_ID, ["ceo@customer.example.com"]);
    const shared: Array<[string, string]> = [];
    deps.sharing = { share: async (folderId, email) => (shared.push([folderId, email]), "perm-0001"), findReader: async () => null, unshare: async () => {} };
    assert.equal((await processDocumentsJob(RUN_ID, deps, { final: true })).outcome, "done");
    const made = deps.store.documents.get(RUN_ID)!.made!;
    assert.deepEqual(shared, [[made.driveFolderId, "ceo@customer.example.com"]], "the Health check folder, not the client's, and never Black Cloud's own");
    assert.equal(deps.store.shares[0]!.state, "shared");

    const failing = setup(jobInput({ run: consoleRun({ id: "hc-2026-11-01-0000000b", status: "released", customerRef: "SYN-0002" }) }), new MemoryDrive());
    failing.store.readers.set("hc-2026-11-01-0000000b", ["ceo@customer.example.com"]);
    failing.sharing = { share: async () => { throw new Error("Google Drive refused a request (403): no"); }, findReader: async () => null, unshare: async () => {} };
    assert.equal((await processDocumentsJob("hc-2026-11-01-0000000b", failing, { final: true })).outcome, "done");
    assert.equal(failing.store.shares[0]!.state, "failed");
  });

  test("a reader taken off while the documents were being made loses access even when the make fails", async () => {
    // Their own share job found the run held by this one, and gave up after its last try.
    const folder = "folder-0000000001";
    const deps = setup(jobInput({ driveFolderId: folder }));
    deps.store.readers.set(RUN_ID, []);
    deps.store.shares = [{ runId: RUN_ID, email: "gone@customer.example.com", folderId: folder, permissionId: "perm-0001", state: "shared", sharedAt: "2026-11-01T09:00:00.000Z", removedAt: null, removedWhy: null, error: null, updatedAt: "2026-11-01T09:00:00.000Z" }];
    const taken: string[] = [];
    deps.sharing = { share: async () => "perm-0002", findReader: async () => null, unshare: async (_f, id) => void taken.push(id) };
    deps.renderReleased = async () => {
      throw new Error("the printer stopped");
    };
    assert.equal((await processDocumentsJob(RUN_ID, deps, { final: true })).outcome, "failed");
    assert.deepEqual(taken, ["perm-0001"]);
    assert.equal(deps.store.shares[0]!.state, "removed");
  });
});

describe("GoogleDrive", () => {
  const KEY = generateKeyPairSync("rsa", { modulusLength: 2048 }).privateKey.export({ type: "pkcs8", format: "pem" }).toString();
  const config = { clientEmail: "documents@example.iam.gserviceaccount.com", privateKey: KEY, rootFolderId: "root-folder-0001", subject: null };

  function fakeFetch(routes: Array<(url: string, init: RequestInit) => Response | undefined>) {
    const calls: Array<{ url: string; init: RequestInit }> = [];
    const impl = (async (input: string | URL | Request, init: RequestInit = {}) => {
      const url = String(input);
      calls.push({ url, init });
      if (url === "https://oauth2.googleapis.com/token") return Response.json({ access_token: "ya29.synthetic-token", expires_in: 3600 });
      for (const route of routes) {
        const r = route(url, init);
        if (r) return r;
      }
      return new Response("{}", { status: 500 });
    }) as typeof fetch;
    return { impl, calls };
  }

  test("lists a folder's subfolders across shared drives, signed in as the service account", async () => {
    const { impl, calls } = fakeFetch([(url) => (url.startsWith("https://www.googleapis.com/drive/v3/files?") ? Response.json({ files: [{ id: "client-folder-0001", name: "Synthetic Co", webViewLink: "https://drive.google.com/x" }] }) : undefined)]);
    const folders = await new GoogleDrive(config, impl).folders("root-folder-0001");
    assert.deepEqual(folders, [{ id: "client-folder-0001", name: "Synthetic Co", url: "https://drive.google.com/x" }]);
    const list = new URL(calls[1]!.url);
    assert.match(list.searchParams.get("q")!, /^'root-folder-0001' in parents and trashed = false and mimeType = 'application\/vnd.google-apps.folder'$/);
    assert.equal(list.searchParams.get("supportsAllDrives"), "true");
    assert.equal(list.searchParams.get("includeItemsFromAllDrives"), "true");
    assert.equal((calls[1]!.init.headers as Record<string, string>).Authorization, "Bearer ya29.synthetic-token");
  });

  test("uploads in two steps, and replaces a file in place when given its id", async () => {
    const session = "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&upload_id=synthetic";
    const { impl, calls } = fakeFetch([
      (url, init) => (url.startsWith("https://www.googleapis.com/upload/drive/v3/files/file-0000000001?") && init.method === "PATCH" ? new Response(null, { status: 200, headers: { location: session } }) : undefined),
      (url, init) => (url === session && init.method === "PUT" ? Response.json({ id: "file-0000000001", name: "Synthetic Co - Raw notes.docx", webViewLink: "https://drive.google.com/f" }) : undefined),
    ]);
    const item = await new GoogleDrive(config, impl).putFile("folder-0000000001", "Synthetic Co - Raw notes.docx", "application/octet-stream", Buffer.from("bytes"), "file-0000000001");
    assert.equal(item.id, "file-0000000001");
    // A replace sends the name too, so a file made under an old name takes the new one (the org handbook's, DECISIONS.md 446).
    assert.deepEqual(JSON.parse(String(calls[1]!.init.body)), { name: "Synthetic Co - Raw notes.docx" });
    assert.match(calls[1]!.url, /fields=id%2Cname%2CwebViewLink|fields=id,name,webViewLink/);
    assert.equal((calls[1]!.init.headers as Record<string, string>)["X-Upload-Content-Length"], "5");
  });

  test("an upload whose answer has no link comes back with none: the documents job checks the handbook's before listing it", async () => {
    const session = "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&upload_id=synthetic";
    const { impl } = fakeFetch([
      (url, init) => (url.startsWith("https://www.googleapis.com/upload/drive/v3/files?") && init.method === "POST" ? new Response(null, { status: 200, headers: { location: session } }) : undefined),
      (url, init) => (url === session && init.method === "PUT" ? Response.json({ id: "file-0000000002", name: "Synthetic Co - Org handbook.docx" }) : undefined),
    ]);
    const item = await new GoogleDrive(config, impl).putFile("folder-0000000001", "Synthetic Co - Org handbook.docx", "application/octet-stream", Buffer.from("bytes"));
    assert.deepEqual([item.id, item.url], ["file-0000000002", undefined]);
  });

  test("moves a file between folders in one request, keeping its id", async () => {
    const { impl, calls } = fakeFetch([(url, init) => (url.startsWith("https://www.googleapis.com/drive/v3/files/file-0000000001?") && init.method === "PATCH" ? Response.json({ id: "file-0000000001" }) : undefined)]);
    await new GoogleDrive(config, impl).moveFile("file-0000000001", "folder-0000000001", "folder-0000000002");
    const move = new URL(calls[1]!.url);
    assert.equal(move.searchParams.get("addParents"), "folder-0000000002");
    assert.equal(move.searchParams.get("removeParents"), "folder-0000000001");
    assert.equal(move.searchParams.get("supportsAllDrives"), "true");
  });

  test("won't send the token to an upload address that isn't Google's", async () => {
    const { impl, calls } = fakeFetch([(url) => (url.includes("/upload/") ? new Response(null, { status: 200, headers: { location: "https://elsewhere.example.com/upload" } }) : undefined)]);
    await assert.rejects(new GoogleDrive(config, impl).putFile("folder-0000000001", "x.pdf", "application/pdf", Buffer.from("x")), /doesn't trust/);
    assert.ok(!calls.some((c) => c.url.startsWith("https://elsewhere.example.com")));
  });

  test("a top folder it can't see says which account to share it with", async () => {
    const { impl } = fakeFetch([() => Response.json({ error: { message: "File not found" } }, { status: 404 })]);
    await assert.rejects(new GoogleDrive(config, impl).folders("root-folder-0001"), /can't see the Health checks folder \(HC_DRIVE_FOLDER_ID\)\. Add documents@example\.iam\.gserviceaccount\.com to its shared drive as a Content manager/);
  });

  test("says what Google refused, and refuses an id that could change a query", async () => {
    const { impl } = fakeFetch([() => Response.json({ error: { message: "File not shared with the service account" } }, { status: 403 })]);
    await assert.rejects(new GoogleDrive(config, impl).folders("root-folder-0001"), /\(403\): File not shared with the service account/);
    await assert.rejects(new GoogleDrive(config, impl).folders("x' or name contains 'a"), /isn't a Drive folder id/);
  });
});

describe("the cover", () => {
  const coverOf = (html: string): string => html.match(/<section class="page dark cover"[\s\S]*?<\/section>|<section[^>]*class="[^"]*cover[^"]*"[\s\S]*?<\/section>/)![0];

  test("says who prepared it, by name and Black Cloud, when the console names someone", async () => {
    const { executiveHtml } = await import("../../render/executive.ts");
    const { fullReportHtml } = await import("../../render/full-report.ts");
    const { report, pseudonyms } = approvedReport(recorded, { ...approvedRun(), preparedBy: "Synthetic Consultant" });
    for (const html of [executiveHtml(report, pseudonyms), fullReportHtml(report, pseudonyms)]) {
      assert.match(coverOf(html), /<p class="for">Prepared for<\/p><p>Synthetic Co<\/p><p>by Synthetic Consultant, Black Cloud<\/p><p>\d{1,2} \w+ \d{4}<\/p>/);
    }
  });

  test("is as it was when nobody is named", async () => {
    const { executiveHtml } = await import("../../render/executive.ts");
    const { fullReportHtml } = await import("../../render/full-report.ts");
    for (const preparedBy of [undefined, null]) {
      const { report, pseudonyms } = approvedReport(recorded, { ...approvedRun(), preparedBy });
      for (const html of [executiveHtml(report, pseudonyms), fullReportHtml(report, pseudonyms)]) {
        assert.match(coverOf(html), /<p class="for">Prepared for<\/p><p>Synthetic Co<\/p><p>\d{1,2} \w+ \d{4}<\/p>/);
        assert.doesNotMatch(html, /Black Cloud<\/p>|by .*, Black Cloud/);
      }
    }
  });

  test("a name is escaped like any other text", async () => {
    const { executiveHtml } = await import("../../render/executive.ts");
    const { report, pseudonyms } = approvedReport(recorded, { ...approvedRun(), preparedBy: "Sam <b>O'Neil</b>" });
    assert.match(coverOf(executiveHtml(report, pseudonyms)), /by Sam &lt;b&gt;O(&apos;|&#39;|')Neil&lt;\/b&gt;, Black Cloud/);
  });

  test("a name that reads as a number isn't a figure the number check refuses, and no section can quote it", async () => {
    const { checkNumbers, scopeDataset } = await import("../../numbercheck/index.ts");
    const { input } = loadRun(recorded);
    const report = buildReport({ ...input, preparedBy: "Ten Eyck" });
    assert.deepEqual(checkNumbers("by Ten Eyck, Black Cloud", report.dataset).unmatched, []);
    // The intake summary is where a preparer's name would most likely leak, and its own figures hold no 10.
    // (The verdict used to serve, until its top issues came to hold a 10 of their own.)
    const intake = report.outputs.raw_notes.sections.find((s) => s.key === "intake_summary")!;
    assert.notDeepEqual(checkNumbers("Ten Eyck", scopeDataset(report.dataset, [...intake.findingIds, ...intake.metricIds])).unmatched, []);
  });

  test("the name is in nothing Claude would be sent", () => {
    const { input } = loadRun(recorded);
    // Not the recorded intake's "Synthetic Consultant", who captured it.
    const report = buildReport({ ...input, preparedBy: "Robin Example" });
    assert.ok(report.briefs.size > 0);
    assert.doesNotMatch(JSON.stringify([...report.briefs.values()]), /Robin Example/);
    assert.doesNotMatch(JSON.stringify(Object.values(report.outputs)), /Robin Example/);
  });

  test("a customer reference with digits on the cover isn't a figure the number check refuses", async () => {
    const { checkNumbers } = await import("../../numbercheck/index.ts");
    const { input } = loadRun(recorded);
    const report = buildReport({ ...input, run: { ...input.run, customerRef: "BT-E2E-SCRATCH5" }, intake: null });
    assert.deepEqual(checkNumbers("BT-E2E-SCRATCH5", report.dataset).unmatched, []);
    // A section still can't quote it: no section's scope takes the cover.
    const verdict = report.outputs.executive_summary.sections.find((s) => s.key === "verdict")!;
    const { scopeDataset } = await import("../../numbercheck/index.ts");
    const scoped = scopeDataset(report.dataset, [...verdict.findingIds, ...verdict.metricIds]);
    assert.notDeepEqual(checkNumbers("BT-E2E-SCRATCH5", scoped).unmatched, []);
  });
});

describe("what a customer reads", () => {
  test("the checks' working terms come out in plain words, with the figures kept", async () => {
    const { plainWords } = await import("../../render/format.ts");
    assert.equal(plainWords("0 closed deals is below the calibrated minimum of 20."), "There are no closed deals yet, and we need at least 20 for a reliable figure.");
    assert.equal(plainWords("12 closed deals is below the calibrated minimum of 20."), "12 closed deals is fewer than the 20 we need for a reliable figure.");
    assert.equal(plainWords("ForecastingType isn't available in this org, or to the connected user."), "Forecasting isn't switched on in this org.");
    assert.equal(plainWords("None of the 0 dashboards run as an inactive user."), "There are no dashboards.");
    assert.equal(plainWords("All 0 active flows that write data have fault paths."), "There are no active flows that write data.");
    assert.equal(plainWords("None of the 3 personal accounts made 5 or more API logins."), "None of the 3 personal accounts made 5 or more API logins.");
  });

  test("no page says how it was made: no manual steps, rule set, calibration or connected user", async () => {
    const { executiveHtml } = await import("../../render/executive.ts");
    const { fullReportHtml } = await import("../../render/full-report.ts");
    const { report, pseudonyms } = approvedReport(recorded, approvedRun());
    const text = [executiveHtml(report, pseudonyms), fullReportHtml(report, pseudonyms)].join(" ").replace(/<style>[\s\S]*?<\/style>/g, "").replace(/<[^>]+>/g, " ");
    for (const tell of [/manual steps/i, /rule set/i, /calibrat/i, /the connected user/i, /\brules\)/, /auto-generated|generated by|engine/i, /\bdomains?\b/i]) {
      assert.doesNotMatch(text, tell, String(tell));
    }
  });
});

describe("wording approved before the change", () => {
  test("still reads plainly, figures kept", async () => {
    const { plainWords } = await import("../../render/format.ts");
    assert.equal(plainWords("We ran 76 checks across 12 domains and found 8 issues: 0 critical and 1 high."), "We ran 76 checks across 12 areas of your Salesforce and found 8 issues: 1 high, none critical.");
    assert.equal(plainWords("We found 8 issues, of which 0 are critical and 1 are high."), "We found 8 issues: 1 high, none critical.");
    assert.equal(plainWords("Every check in this domain was not measurable (6 rules)."), "We couldn't measure this area.");
    assert.equal(plainWords("None of the 0 active users have never logged in."), "There are no active users.");
  });
});

describe("a consenting run adds to the benchmark set when its documents are made", () => {
  /** The recorded synthetic run, as the run job left it on the worker's disk. */
  function benchmarkSetup(overrides: Partial<DocumentsJobInput> = {}) {
    const deps = baseDeps({ drive: new MemoryDrive(), renderReleased: fakeRender });
    const input = jobInput({ intake: { ...INTAKE, customerName: "Synthetic Co", industry: "technology", headcountBand: "51-200" }, ...overrides });
    cpSync(recorded, join(deps.runsDir, input.run.id), { recursive: true });
    deps.store.addDocumentsJob(input);
    return { deps, input };
  }

  test("adds banded, anonymous figures once, and a re-make adds nothing more", async () => {
    const { deps, input } = benchmarkSetup();
    assert.equal((await processDocumentsJob(input.run.id, deps, { final: true })).outcome, "done");
    const rows = deps.store.benchmarkRows;
    assert.ok(rows.length > 10, `${rows.length} rows`);
    for (const row of rows) {
      assert.deepEqual(Object.keys(row).sort(), ["dealSizeBand", "headcountBand", "industry", "metricKey", "period", "ruleSetVersion", "unit", "userBand", "value"]);
      assert.equal(row.industry, "technology");
      assert.ok(!JSON.stringify(row).includes(input.run.id) && !JSON.stringify(row).includes(input.run.orgId.slice(0, 15)));
    }
    assert.ok(existsSync(join(deps.runsDir, input.run.id, "benchmark-consent.json")));
    deps.store.documents.get(input.run.id)!.input.state = "queued";
    await processDocumentsJob(input.run.id, deps, { final: true });
    assert.equal(deps.store.benchmarkRows.length, rows.length, "counted once");
  });

  test("a run that didn't opt in adds nothing", async () => {
    const { deps, input } = benchmarkSetup({ run: consoleRun({ status: "released", benchmarkOptIn: false }) });
    await processDocumentsJob(input.run.id, deps, { final: true });
    assert.equal(deps.store.benchmarkRows.length, 0);
  });

  test("a run that can't be banded says why, and its documents are still made", async () => {
    const { deps, input } = benchmarkSetup({ intake: { ...INTAKE, customerName: "Synthetic Co" } });
    assert.equal((await processDocumentsJob(input.run.id, deps, { final: true })).outcome, "done");
    assert.equal(deps.store.benchmarkRows.length, 0);
    assert.ok(deps.lines.some((l) => /didn't add to the benchmark set: Give --industry/.test(l)), deps.lines.join("\n"));
  });
});

describe("the documents compare the run with the benchmark set", () => {
  test("they're given every row, and told whether this run's own figures are among them", async () => {
    const seen: Array<ApprovedRun["benchmark"]> = [];
    const render = async (dir: string, approved: ApprovedRun, out: string) => (seen.push(approved.benchmark), fakeRender(dir, approved, out));
    const deps = baseDeps({ drive: new MemoryDrive(), renderReleased: render });
    const input = jobInput({ intake: { ...INTAKE, customerName: "Synthetic Co", industry: "technology", headcountBand: "51-200" } });
    cpSync(recorded, join(deps.runsDir, input.run.id), { recursive: true });
    deps.store.addDocumentsJob(input);
    await processDocumentsJob(input.run.id, deps, { final: true });
    assert.equal(seen[0]?.rows.length, 0);
    assert.equal(seen[0]?.includesThisRun, false);
    // Made again: its figures went in the first time.
    deps.store.documents.get(input.run.id)!.input.state = "queued";
    await processDocumentsJob(input.run.id, deps, { final: true });
    assert.equal(seen[1]?.rows.length, deps.store.benchmarkRows.length);
    assert.ok(seen[1]!.rows.length > 0);
    assert.equal(seen[1]?.includesThisRun, true);
  });

  test("a set that can't be read leaves published research only, and the documents are still made", async () => {
    const seen: Array<ApprovedRun["benchmark"]> = [];
    const deps = setup();
    deps.renderReleased = async (dir, approved, out) => (seen.push(approved.benchmark), fakeRender(dir, approved, out));
    deps.store.loadBenchmarkRows = async () => Promise.reject(new Error("connection reset"));
    assert.equal((await processDocumentsJob(RUN_ID, deps, { final: true })).outcome, "done");
    assert.equal(seen[0], null);
  });
});

describe("the survey the documents print", () => {
  test("is the one the sections were reviewed from, and later answers are only counted", () => {
    const dir = temp();
    cpSync(recorded, dir, { recursive: true });
    assert.equal(laterSurveyAnswers(dir, syntheticSurvey(3)), 3, "with no survey kept, every answer is later");
    writeFileSync(join(dir, SURVEY_FILE), JSON.stringify(withoutComments(syntheticSurvey(6))));
    assert.equal(loadRun(dir).input.survey!.responses.length, 6);
    assert.equal(laterSurveyAnswers(dir, syntheticSurvey(8)), 2);
    assert.equal(laterSurveyAnswers(dir, null), 0);
  });
});

describe("the customer microsite is made with the documents", () => {
  const KEY = Buffer.alloc(32, 9);
  const pages = { pages: [{ slug: "overview", title: "Overview", html: "<p>Synthetic.</p>" }] };
  const withSite = async (dir: string, approved: ApprovedRun, out: string) => ({ ...(await fakeRender(dir, approved, out)), microsite: { ok: true as const, pages } });

  test("sealed for this run, kept 12 months, and its version moves on when it's made again", async () => {
    const deps = setup();
    deps.renderReleased = withSite;
    deps.micrositeKey = KEY;
    deps.now = () => new Date("2026-11-05T12:00:00.000Z");
    await processDocumentsJob(RUN_ID, deps, { final: true });
    const site = deps.store.microsites.get(RUN_ID)!;
    assert.equal(site.version, 1);
    assert.match(site.sealedPages, /^v1\.[\w-]+\.[\w-]+\.[\w-]+$/);
    assert.ok(!site.sealedPages.includes("Synthetic"), "sealed, not in the clear");
    assert.equal(site.expiresAt, "2027-11-05T12:00:00.000Z");
    deps.store.documents.get(RUN_ID)!.input.state = "queued";
    await processDocumentsJob(RUN_ID, deps, { final: true });
    assert.equal(deps.store.microsites.get(RUN_ID)!.version, 2);
  });

  test("without a key, or with pages that failed their check, the documents are still made and the log says why", async () => {
    const lines: string[] = [];
    const deps = setup();
    deps.renderReleased = withSite;
    deps.log = (l) => lines.push(l);
    assert.equal((await processDocumentsJob(RUN_ID, deps, { final: true })).outcome, "done");
    assert.equal(deps.store.microsites.size, 0);
    assert.ok(lines.some((l) => /HC_MICROSITE_KEY isn't set/.test(l)));

    const failed = setup();
    failed.renderReleased = async (dir, approved, out) => ({ ...(await fakeRender(dir, approved, out)), microsite: { ok: false as const, reason: "The overview page has 1 figure the number check can't trace, so the microsite wasn't made" } });
    failed.micrositeKey = KEY;
    failed.log = (l) => lines.push(l);
    assert.equal((await processDocumentsJob(RUN_ID, failed, { final: true })).outcome, "done");
    assert.equal(failed.store.microsites.size, 0);
    assert.ok(lines.some((l) => /number check/.test(l)));
  });
});

describe("a case study draft", () => {
  test("goes only to Black Cloud's own folder, never the client's, and only when there is one", async () => {
    const drive = new MemoryDrive();
    const deps = setup(jobInput(), drive);
    deps.renderReleased = async (d, approved, out) => {
      const made = await fakeRender(d, approved, out);
      const caseStudy = join(out, "case-study-draft.docx");
      writeFileSync(caseStudy, "synthetic case study");
      return { ...made, caseStudy };
    };
    assert.equal((await processDocumentsJob(RUN_ID, deps, { final: true })).outcome, "done");
    const studies = [...drive.items.values()].filter((i) => !i.folder && /Case study draft/.test(i.name));
    assert.equal(studies.length, 1);
    assert.equal(drive.path(studies[0]!.id), `${INTERNAL_FOLDER} / Synthetic Co - Health check 1 / Synthetic Co - Case study draft.docx`);
    assert.ok(!deps.store.documents.get(RUN_ID)!.made!.files.some((f) => /Case study/.test(f.name)), "never listed to the console");

    const plain = new MemoryDrive();
    await processDocumentsJob(RUN_ID, setup(jobInput(), plain), { final: true });
    assert.ok(![...plain.items.values()].some((i) => /Case study/.test(i.name)));
  });
});

describe("the org handbook", () => {
  const DOCX = "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
  const NAME = "Synthetic Co - Org handbook.docx";
  const SIX = ["Synthetic Co - Action lists.xlsx", "Synthetic Co - Executive summary.pdf", "Synthetic Co - Full health check.pdf", "Synthetic Co - One page summary.pdf", "Synthetic Co - Raw notes.docx", "Synthetic Co - Tickets.csv"];
  const OVERFLOW = "The full health check, page 4: overflow";

  /** A run folder as the run job leaves one, with the recorded synthetic org's handbook.json, or `contents` in its place. */
  function withHandbook(input: DocumentsJobInput = jobInput(), drive: MemoryDrive = new MemoryDrive(), contents?: string) {
    const deps = setup(input, drive);
    const path = join(deps.runsDir, input.run.id, HANDBOOK_FILE);
    if (contents === undefined) cpSync(join(recorded, HANDBOOK_FILE), path);
    else writeFileSync(path, contents);
    return deps;
  }
  const handbooks = (drive: MemoryDrive) => [...drive.items.values()].filter((i) => !i.folder && /Org handbook/.test(i.name));

  test("goes in the customer's folder, as a Word file named for the client, and the console is told of it last, as Drive returned it", async () => {
    const drive = new MemoryDrive();
    const deps = withHandbook(jobInput(), drive);
    // What Drive returned for the upload, as its own copy, with a name and link
    // of Drive's choosing: the entry must be these, not what the job asked for.
    const put = drive.putFile.bind(drive);
    let returned: DriveItem | undefined;
    drive.putFile = async (...args) => {
      const item = await put(...args);
      if (args[1] !== NAME) return item;
      const copy = { ...item, name: `${NAME} as Drive keeps it`, url: `https://drive.example.com/open?id=${item.id}` };
      returned = copy;
      return copy;
    };
    const result = await processDocumentsJob(RUN_ID, deps, { final: true });
    assert.equal(result.outcome, "done", result.detail);
    const made = deps.store.documents.get(RUN_ID)!.made!;
    const [handbook, ...more] = handbooks(drive);
    assert.equal(more.length, 0);
    assert.equal(drive.path(handbook!.id), `Synthetic Co / Health check 1 / ${NAME}`);
    assert.equal(handbook!.mime, DOCX);
    // Its own words, from the run folder's handbook.json, in a real Word file.
    assert.ok(docxText(handbook!.body!).includes(HANDBOOK_MARKER));
    // Shared with the rest: it's in the folder the share job gives readers (share-job.ts, input.folderId).
    assert.equal(drive.items.get(handbook!.id)!.parent, made.driveFolderId);
    // In the record after the six, now the console knows the kind (DECISIONS.md 477): Drive's name, id and link, and nothing else.
    assert.equal(made.files.length, 7);
    assert.deepEqual(made.files.slice(0, 6).map((f) => f.name).sort(), SIX);
    assert.deepEqual(made.files.slice(0, 6).map((f) => f.output).sort(), ["action_lists", "ceo_one_pager", "executive_summary", "full_health_check", "raw_notes", "tickets"]);
    assert.deepEqual(made.files[6], { output: "org_handbook", name: returned!.name, driveFileId: returned!.id, url: returned!.url });
    assert.equal(returned!.id, handbook!.id);
    assert.deepEqual(made.warnings, [OVERFLOW]);
    assert.match(result.detail!, /the org handbook in Health check 1/);
    // Printed on the worker's disk beside the other documents, for its user only.
    const onDisk = join(deps.runsDir, RUN_ID, "documents", HANDBOOK_DOCX);
    assert.equal(statSync(onDisk).mode & 0o777, 0o600);
    assert.deepEqual(readFileSync(onDisk), handbook!.body);
  });

  // Who made each recent change is a token in the file, and a name only here (DECISIONS.md 421 and 461).
  test("names who made each recent change from the run's pseudonym map, and prints no token", async () => {
    const drive = new MemoryDrive();
    const deps = withHandbook(jobInput(), drive);
    cpSync(join(recorded, "pseudonym-map.json"), join(deps.runsDir, RUN_ID, "pseudonym-map.json"));
    assert.equal((await processDocumentsJob(RUN_ID, deps, { final: true })).outcome, "done");
    const text = docxText(handbooks(drive)[0]!.body!);
    assert.match(text, /, Jo Ellis \(support\.desk@example\.com logged in as them\): Flows/);
    assert.doesNotMatch(text, /\[\[[a-z]+:[A-Z]\d{4,}\]\]/);
    assert.ok(!deps.lines.some((l) => /names no one/.test(l)));
  });

  test("without the run's pseudonym map it's still made, naming no one and printing no token, and the log says why", async () => {
    for (const map of [undefined, "not json"]) {
      const drive = new MemoryDrive();
      const deps = withHandbook(jobInput(), drive);
      if (map !== undefined) writeFileSync(join(deps.runsDir, RUN_ID, "pseudonym-map.json"), map);
      assert.equal((await processDocumentsJob(RUN_ID, deps, { final: true })).outcome, "done");
      const text = docxText(handbooks(drive)[0]!.body!);
      assert.match(text, /, a user whose name we couldn't look up \(support\.desk@example\.com logged in as them\): Flows/);
      assert.ok(!text.includes("Jo Ellis"));
      assert.doesNotMatch(text, /\[\[[a-z]+:[A-Z]\d{4,}\]\]/);
      assert.ok(deps.lines.some((l) => l === `Run ${RUN_ID}: the org handbook names no one who made a recent change, because the run's pseudonym map couldn't be read.`), map ?? "missing");
    }
  });

  test("made again, it's replaced in place: one file, its id and link kept, outside the version folders", async () => {
    const drive = new MemoryDrive();
    const deps = withHandbook(jobInput(), drive);
    await processDocumentsJob(RUN_ID, deps, { final: true });
    const doc = deps.store.documents.get(RUN_ID)!;
    const first = handbooks(drive)[0]!;
    const listed = doc.made!.files.at(-1)!;
    assert.deepEqual(listed, { output: "org_handbook", name: NAME, driveFileId: first.id, url: first.url });
    for (let make = 2; make <= 3; make++) {
      // The record from the last make, handbook entry and all, as the console's row holds it.
      doc.input = { ...doc.input, state: "queued", driveFolderId: doc.made!.driveFolderId, files: doc.made!.files };
      assert.equal(doc.input.files.length, 7);
      assert.equal((await processDocumentsJob(RUN_ID, deps, { final: true })).outcome, "done");
      const all = handbooks(drive);
      assert.equal(all.length, 1, `make ${make}`);
      assert.equal(all[0]!.id, first.id);
      assert.equal(all[0]!.uploads, make);
      assert.equal(drive.path(all[0]!.id), `Synthetic Co / Health check 1 / ${NAME}`);
      // Listed again, last, with the same id and link as the first make's.
      assert.equal(doc.made!.files.length, 7, `make ${make}`);
      assert.deepEqual(doc.made!.files.at(-1), listed, `make ${make}`);
    }
    // The reports moved into their versions as before, and the handbook is the one thing left beside them.
    assert.deepEqual((await drive.files(doc.made!.driveFolderId)).map((f) => f.name), [NAME]);
    assert.deepEqual((await drive.folders(doc.made!.driveFolderId)).map((f) => f.name.split(",")[0]).sort(), ["Version 1", "Version 2", "Version 3"]);
  });

  test("made again, the version folders are the ones made before it was listed: its entry moves nothing and decides nothing", async () => {
    /** Everything under the run's folder: each path, with its id, and for a file how many times it was uploaded. */
    const tree = (drive: MemoryDrive, folderId: string): string[] => {
      const under = `${drive.path(folderId)} / `;
      return [...drive.items.values()].filter((i) => drive.path(i.id).startsWith(under)).map((i) => `${drive.path(i.id)} [${i.id}${i.folder ? "" : `, uploaded ${i.uploads}`}]`).sort();
    };
    // The same three makes twice: from the record as it is now, and from the
    // record without the handbook's entry, as every make before decision 477 left it.
    async function threeMakes(withEntry: boolean) {
      const drive = new MemoryDrive();
      const deps = withHandbook(jobInput(), drive);
      assert.equal((await processDocumentsJob(RUN_ID, deps, { final: true })).outcome, "done");
      const doc = deps.store.documents.get(RUN_ID)!;
      for (let make = 2; make <= 3; make++) {
        const files = withEntry ? doc.made!.files : doc.made!.files.filter((f) => f.output !== "org_handbook");
        assert.equal(files.length, withEntry ? 7 : 6);
        doc.input = { ...doc.input, state: "queued", driveFolderId: doc.made!.driveFolderId, files };
        assert.equal((await processDocumentsJob(RUN_ID, deps, { final: true })).outcome, "done");
      }
      return tree(drive, doc.made!.driveFolderId);
    }
    const listed = await threeMakes(true);
    assert.deepEqual(listed, await threeMakes(false));
    // Which is: the handbook alone beside the versions, the same file uploaded
    // three times; the first make's six moved into Version 1 with their ids; a
    // new six in each later version, each uploaded once.
    const H = "Synthetic Co / Health check 1";
    const six = (version: string, ids: number[]) => [
      `${H} / ${version} [drive-item-${String(ids[0]).padStart(4, "0")}]`,
      ...["Executive summary.pdf", "Full health check.pdf", "Raw notes.docx", "One page summary.pdf", "Action lists.xlsx", "Tickets.csv"].map((name, i) => `${H} / ${version} / Synthetic Co - ${name} [drive-item-${String(ids[i + 1]).padStart(4, "0")}, uploaded 1]`),
    ];
    assert.deepEqual(
      listed,
      [
        `${H} / ${NAME} [drive-item-0009, uploaded 3]`,
        ...six("Version 1, 4 Nov 2026", [13, 3, 4, 5, 6, 7, 8]),
        ...six("Version 2, 31 Oct 2026", [14, 15, 16, 17, 18, 19, 20]),
        ...six("Version 3, 31 Oct 2026", [21, 22, 23, 24, 25, 26, 27]),
      ].sort(),
    );
  });

  /** Makes the documents again, as the console asks for a remake, under the client's name now. */
  async function remake(deps: ReturnType<typeof setup>, customerName: string) {
    const doc = deps.store.documents.get(RUN_ID)!;
    doc.input = { ...doc.input, intake: { ...doc.input.intake!, customerName }, state: "queued", driveFolderId: doc.made!.driveFolderId, files: doc.made!.files };
    const result = await processDocumentsJob(RUN_ID, deps, { final: true });
    assert.equal(result.outcome, "done", result.detail);
  }

  test("a client renamed or re-cased between makes still has one handbook: three makes, three names, one file under the last", async () => {
    const drive = new MemoryDrive();
    const deps = withHandbook(jobInput(), drive);
    await processDocumentsJob(RUN_ID, deps, { final: true });
    const first = handbooks(drive)[0]!;
    await remake(deps, "Synthetic Company");
    await remake(deps, "SYNTHETIC company");
    const all = handbooks(drive);
    assert.equal(all.length, 1);
    assert.equal(all[0]!.id, first.id, "the same file, so its link keeps working");
    assert.equal(all[0]!.uploads, 3);
    // Drive renames it on the replace, so it carries the client's name now.
    assert.equal(drive.path(all[0]!.id), "Synthetic Co / Health check 1 / SYNTHETIC company - Org handbook.docx");
    assert.ok(!deps.lines.some((l) => /org handbooks/.test(l)), "one was there each time, so nothing to say");
  });

  test("a folder already holding two handbooks has the first one made replaced and the other left, and the log says how many with nothing from the files", async () => {
    const drive = new MemoryDrive();
    const deps = withHandbook(jobInput(), drive);
    await processDocumentsJob(RUN_ID, deps, { final: true });
    const folderId = deps.store.documents.get(RUN_ID)!.made!.driveFolderId;
    const older = handbooks(drive)[0]!;
    // As a make before decision 446 left it after a rename: a second under the other name, made later.
    const newer = drive.items.get((await drive.putFile(folderId, "Synthetic Co Ltd - ORG HANDBOOK.DOCX", "application/octet-stream", Buffer.from("old"))).id)!;
    newer.createdAt = "2026-11-05T09:00:00.000Z";
    await remake(deps, "Synthetic Group");
    const all = [...drive.items.values()].filter((i) => !i.folder && /org handbook\.docx$/i.test(i.name));
    assert.equal(all.length, 2, "none added");
    assert.deepEqual([older.uploads, older.name], [2, "Synthetic Group - Org handbook.docx"], "the first one made is the one whose link was shared");
    assert.deepEqual([newer.uploads, newer.name, newer.body!.toString()], [1, "Synthetic Co Ltd - ORG HANDBOOK.DOCX", "old"], "the newer one left as it was");
    const said = deps.lines.filter((l) => /org handbooks/.test(l));
    assert.deepEqual(said, [`Run ${RUN_ID}'s Drive folder held 2 org handbooks, so the first one made was replaced and the other 1 left as they were.`]);
    assert.doesNotMatch(deps.lines.join("\n"), /Synthetic Co Ltd|ORG HANDBOOK|Synthetic Co - Org/);
  });

  test("a copy someone made in Drive doesn't take the handbook's place: a remake updates the original, whose link was shared, and leaves the copy", async () => {
    const drive = new MemoryDrive();
    const deps = withHandbook(jobInput(), drive);
    await processDocumentsJob(RUN_ID, deps, { final: true });
    const folderId = deps.store.documents.get(RUN_ID)!.made!.driveFolderId;
    const original = handbooks(drive)[0]!;
    // Stale, so the remake's contents can be told from the first make's.
    original.body = Buffer.from("stale");
    // Drive's "Make a copy": the same ending, made later.
    const copy = drive.items.get((await drive.putFile(folderId, `Copy of ${NAME}`, DOCX, Buffer.from("their copy"))).id)!;
    copy.createdAt = "2026-11-05T09:00:00.000Z";
    await remake(deps, "Synthetic Co");
    assert.equal(handbooks(drive).length, 2, "none added");
    assert.deepEqual([original.uploads, drive.path(original.id)], [2, `Synthetic Co / Health check 1 / ${NAME}`]);
    assert.ok(docxText(original.body!).includes(HANDBOOK_MARKER), "the original has the new contents");
    assert.deepEqual([copy.uploads, copy.name, copy.body!.toString()], [1, `Copy of ${NAME}`, "their copy"], "the copy untouched");
    assert.deepEqual(
      deps.lines.filter((l) => /org handbooks/.test(l)),
      [`Run ${RUN_ID}'s Drive folder held 2 org handbooks, so the first one made was replaced and the other 1 left as they were.`],
    );
  });

  test("handbooks made at the same time, or with no time from Drive, replace the same one whatever order Drive lists them in: the lowest id, after any with a time", async () => {
    // Two handbooks, `second` made after `first` so with the higher id, their times set by `times`.
    async function twoHandbooks(times: [string | undefined, string | undefined], listed: "as made" | "reversed") {
      const drive = new MemoryDrive();
      const deps = withHandbook(jobInput(), drive);
      await processDocumentsJob(RUN_ID, deps, { final: true });
      const folderId = deps.store.documents.get(RUN_ID)!.made!.driveFolderId;
      const first = handbooks(drive)[0]!;
      const second = drive.items.get((await drive.putFile(folderId, `Copy of ${NAME}`, DOCX, Buffer.from("their copy"))).id)!;
      assert.ok(first.id < second.id);
      for (const [item, at] of [[first, times[0]], [second, times[1]]] as const) {
        if (at === undefined) delete item.createdAt;
        else item.createdAt = at;
      }
      if (listed === "reversed") {
        const files = drive.files.bind(drive);
        drive.files = async (parentId: string) => (await files(parentId)).reverse();
      }
      await remake(deps, "Synthetic Co");
      return { first, second };
    }
    for (const listed of ["as made", "reversed"] as const) {
      const same = await twoHandbooks(["2026-11-04T18:00:00.000Z", "2026-11-04T18:00:00.000Z"], listed);
      assert.deepEqual([same.first.uploads, same.second.uploads], [2, 1], `equal times, listed ${listed}`);
      const neither = await twoHandbooks([undefined, undefined], listed);
      assert.deepEqual([neither.first.uploads, neither.second.uploads], [2, 1], `no times, listed ${listed}`);
      // The one with a time is replaced, though the other has the lower id.
      const one = await twoHandbooks([undefined, "2026-11-05T09:00:00.000Z"], listed);
      assert.deepEqual([one.first.uploads, one.second.uploads], [1, 2], `one time, listed ${listed}`);
    }
  });

  test("a run from before the handbook was kept gets the six documents as before, and one warning saying why", async () => {
    const drive = new MemoryDrive();
    const deps = setup(jobInput(), drive);
    const result = await processDocumentsJob(RUN_ID, deps, { final: true });
    assert.equal(result.outcome, "done", result.detail);
    const made = deps.store.documents.get(RUN_ID)!.made!;
    assert.equal(made.files.length, 6);
    assert.deepEqual(made.files.map((f) => f.name).sort(), SIX);
    assert.ok(!made.files.some((f) => f.output === "org_handbook"));
    assert.deepEqual((await drive.files(made.driveFolderId)).map((f) => f.name).sort(), SIX);
    assert.deepEqual(handbooks(drive), []);
    assert.deepEqual(made.warnings, [OVERFLOW, HANDBOOK_NOT_MADE.missing]);
    assert.ok((Object.values(HANDBOOK_NOT_MADE) as string[]).includes(made.warnings.at(-1)!), "one of the four sentences in read.ts");
    assert.doesNotMatch(result.detail!, /handbook/);
    assert.ok(deps.lines.includes(`Run ${RUN_ID}: ${HANDBOOK_NOT_MADE.missing}`), deps.lines.join("\n"));
  });

  test("a file that doesn't fit the format gets the same, with the other sentence and nothing from the file", async () => {
    for (const contents of [JSON.stringify({ version: 1, flows: { status: "ok", items: [{ name: "HB-NOT-FIT-0445" }] } }), "{ not json HB-NOT-FIT-0445"]) {
      const drive = new MemoryDrive();
      const deps = withHandbook(jobInput(), drive, contents);
      const result = await processDocumentsJob(RUN_ID, deps, { final: true });
      assert.equal(result.outcome, "done", result.detail);
      const made = deps.store.documents.get(RUN_ID)!.made!;
      assert.equal(made.files.length, 6);
      assert.deepEqual(made.files.map((f) => f.name).sort(), SIX);
      assert.deepEqual(handbooks(drive), []);
      assert.deepEqual(made.warnings, [OVERFLOW, HANDBOOK_NOT_MADE.format]);
      assert.doesNotMatch(JSON.stringify([made, result, deps.lines, deps.store.writes]), /HB-NOT-FIT-0445/);
    }
  });

  test("a version 1 file, written before the Apex was kept, gets the six documents and the format warning, never half a handbook", async () => {
    // As an engine before decisions 456 and 457 wrote it: the same flows and rules, no Apex parts.
    const { apexClasses: _classes, apexTriggers: _triggers, ...rest } = JSON.parse(readFileSync(join(recorded, HANDBOOK_FILE), "utf8")) as Record<string, unknown>;
    const v1 = JSON.stringify({ ...rest, version: 1 });
    assert.ok(v1.includes(HANDBOOK_MARKER), "the recorded flows and rules, marker included");
    const drive = new MemoryDrive();
    const deps = withHandbook(jobInput(), drive, v1);
    const result = await processDocumentsJob(RUN_ID, deps, { final: true });
    assert.equal(result.outcome, "done", result.detail);
    const made = deps.store.documents.get(RUN_ID)!.made!;
    assert.equal(made.files.length, 6);
    assert.deepEqual(made.files.map((f) => f.name).sort(), SIX);
    assert.deepEqual((await drive.files(made.driveFolderId)).map((f) => f.name).sort(), SIX);
    assert.deepEqual(handbooks(drive), []);
    assert.ok(!existsSync(join(deps.runsDir, RUN_ID, "documents", HANDBOOK_DOCX)));
    assert.deepEqual(made.warnings, [OVERFLOW, HANDBOOK_NOT_MADE.format]);
    assert.doesNotMatch(JSON.stringify([made, result, deps.lines, deps.store.writes]), new RegExp(HANDBOOK_MARKER));
  });

  test("a make held back by the number check uploads no handbook, and makes none", async () => {
    const drive = new MemoryDrive();
    const deps = withHandbook(jobInput(), drive);
    deps.renderReleased = async (dir, approved, out) => ({ ...(await fakeRender(dir, approved, out)), held: [{ document: "The full health check", page: 12, figure: "£1.99m", reason: "not in the data" }] });
    assert.equal((await processDocumentsJob(RUN_ID, deps, { final: true })).outcome, "failed");
    assert.deepEqual([...drive.items.values()], [], "nothing in Drive at all");
    assert.ok(!existsSync(join(deps.runsDir, RUN_ID, "documents", HANDBOOK_DOCX)));
  });

  test("a make that fails before Drive uploads no handbook either", async () => {
    const drive = new MemoryDrive();
    const deps = withHandbook(jobInput(), drive);
    deps.renderReleased = async () => {
      throw new Error("the printer stopped");
    };
    assert.equal((await processDocumentsJob(RUN_ID, deps, { final: true })).outcome, "failed");
    assert.deepEqual(handbooks(drive), []);
  });

  test("Drive refusing the handbook fails the make as it would for any file, and the retry finishes it", async () => {
    const drive = new MemoryDrive();
    const deps = withHandbook(jobInput(), drive);
    const put = drive.putFile.bind(drive);
    drive.putFile = async (...args) => {
      if (args[1] === NAME) throw new DriveError("Google Drive refused a request (503): backend error");
      return put(...args);
    };
    await assert.rejects(processDocumentsJob(RUN_ID, deps, { final: false }), /503/);
    assert.ok(!deps.store.documents.get(RUN_ID)!.made, "nothing recorded as made");
    drive.putFile = put;
    assert.equal((await processDocumentsJob(RUN_ID, deps, { final: true })).outcome, "done");
    assert.equal(handbooks(drive).length, 1);
    // The retry lists it, as a first make would.
    const files = deps.store.documents.get(RUN_ID)!.made!.files;
    assert.deepEqual([files.length, files.at(-1)!.output, files.at(-1)!.driveFileId], [7, "org_handbook", handbooks(drive)[0]!.id]);
  });

  test("Drive refusing the handbook on the last try fails the make, recording nothing as made", async () => {
    const drive = new MemoryDrive();
    const deps = withHandbook(jobInput(), drive);
    const put = drive.putFile.bind(drive);
    drive.putFile = async (...args) => {
      if (args[1] === NAME) throw new DriveError("Google Drive refused a request (503): backend error");
      return put(...args);
    };
    const result = await processDocumentsJob(RUN_ID, deps, { final: true });
    assert.equal(result.outcome, "failed");
    const doc = deps.store.documents.get(RUN_ID)!;
    assert.equal(doc.made, null);
    assert.equal(doc.error, "The documents couldn't be made: Google Drive refused a request (503): backend error");
    assert.deepEqual(doc.input.files, [], "the console's row keeps the list it had");
  });

  test("a complete run and an essentials run both get it", async () => {
    for (const scope of ["complete", "essentials"] as const) {
      const drive = new MemoryDrive();
      const deps = withHandbook(jobInput({ run: consoleRun({ status: "released", customerRef: "SYN-0001", scope }) }), drive);
      assert.equal((await processDocumentsJob(RUN_ID, deps, { final: true })).outcome, "done", scope);
      assert.equal(handbooks(drive).length, 1, scope);
      const files = deps.store.documents.get(RUN_ID)!.made!.files;
      assert.equal(files.length, 7, scope);
      assert.deepEqual([files.at(-1)!.output, files.at(-1)!.driveFileId], ["org_handbook", handbooks(drive)[0]!.id], scope);
    }
  });

  describe("a re-check's (decision 483)", () => {
    const NO_EARLIER_LOGGED = `Run ${RUN_ID}: the org handbook says there was no earlier handbook to compare with.`;
    /** The synthetic run as a re-check, its planted earlier health check beside it on the worker's disk. */
    function recheck(drive: MemoryDrive) {
      const deps = withHandbook(jobInput(), drive);
      const dir = join(deps.runsDir, RUN_ID);
      cpSync(join(recorded, "run.json"), join(dir, "run.json"));
      cpSync(join(recorded, "pseudonym-map.json"), join(dir, "pseudonym-map.json"));
      return { deps, dir, earlier: withEarlier(dir) };
    }
    const partOf = (body: Buffer): string => {
      const text = docxText(body);
      return text.slice(text.indexOf(`\n## ${SINCE_TITLE}\n`) + 1, text.indexOf("\n## Recent changes\n"));
    };

    test("handbook has the part, from the earlier folder beside the run's own, and the console hears nothing of it", async () => {
      const drive = new MemoryDrive();
      const { deps } = recheck(drive);
      const result = await processDocumentsJob(RUN_ID, deps, { final: true });
      assert.equal(result.outcome, "done", result.detail);
      const part = partOf(handbooks(drive)[0]!.body!);
      assert.match(part, /^## Changed since your last health check\n\nThis compares your org on 31 Oct 2026 with your health check of 14 Jul 2026\./);
      assert.match(part, /\nRemoved: 1 flow\nLead Nurture \(old\) \(Lead_Nurture_Old\)\n/);
      assert.match(part, /\nFinanceSync: last changed 13 Oct 2026 by Ash Rowe\n/);
      const made = deps.store.documents.get(RUN_ID)!.made!;
      assert.deepEqual(made.warnings, [OVERFLOW]);
      assert.doesNotMatch(JSON.stringify([made, result, deps.lines]), /Lead_Nurture_Old|earlier handbook/);
    });

    test("with its baseline named and nothing to read, the handbook is still made, with the one sentence, and the log says so in fixed words", async () => {
      const spoil: Array<[string, (dir: string, earlier: string) => void]> = [
        ["folder gone", (_dir, earlier) => rmSync(earlier, { recursive: true })],
        ["run.json missing", (_dir, earlier) => rmSync(join(earlier, "run.json"))],
        ["handbook.json missing", (_dir, earlier) => rmSync(join(earlier, HANDBOOK_FILE))],
        ["handbook.json damaged", (_dir, earlier) => writeFileSync(join(earlier, HANDBOOK_FILE), "{ HB-DAMAGED-0483")],
        ["another org's", (_dir, earlier) => {
          const other = JSON.parse(readFileSync(join(earlier, "run.json"), "utf8")) as Record<string, unknown>;
          writeFileSync(join(earlier, "run.json"), JSON.stringify({ ...other, orgId: "00D000000000777AAA" }));
        }],
        ["a path in its id", (dir) => {
          const own = JSON.parse(readFileSync(join(dir, "run.json"), "utf8")) as Record<string, unknown>;
          writeFileSync(join(dir, "run.json"), JSON.stringify({ ...own, baselineRunId: `../runs/${RUN_ID}-earlier` }));
        }],
      ];
      for (const [what, spoilIt] of spoil) {
        const drive = new MemoryDrive();
        const { deps, dir, earlier } = recheck(drive);
        spoilIt(dir, earlier);
        const result = await processDocumentsJob(RUN_ID, deps, { final: true });
        assert.equal(result.outcome, "done", what);
        assert.equal(partOf(handbooks(drive)[0]!.body!), `## ${SINCE_TITLE}\n\n${NO_EARLIER}\n`, what);
        assert.deepEqual(deps.store.documents.get(RUN_ID)!.made!.warnings, [OVERFLOW], what);
        assert.deepEqual(deps.lines.filter((l) => /earlier handbook/.test(l)), [NO_EARLIER_LOGGED], what);
        assert.doesNotMatch(JSON.stringify([result, deps.lines]), /00D000000000777|HB-DAMAGED|-earlier/, what);
      }
    });

    test("with no baseline named, nothing is said of one, in the document or the log", async () => {
      const drive = new MemoryDrive();
      const deps = withHandbook(jobInput(), drive);
      cpSync(join(recorded, "run.json"), join(deps.runsDir, RUN_ID, "run.json"));
      assert.equal((await processDocumentsJob(RUN_ID, deps, { final: true })).outcome, "done");
      assert.doesNotMatch(docxText(handbooks(drive)[0]!.body!), /Changed since|earlier handbook/);
      assert.ok(!deps.lines.some((l) => /earlier handbook/.test(l)));
    });

    test("made again after retention took the earlier folder, the same file in Drive loses the part and says so", async () => {
      const drive = new MemoryDrive();
      const { deps, earlier } = recheck(drive);
      await processDocumentsJob(RUN_ID, deps, { final: true });
      const first = handbooks(drive)[0]!;
      assert.match(partOf(first.body!), /\nRemoved: 1 flow\n/);
      // The earlier run's folder goes 12 months after its release (retention.ts), before this one's.
      rmSync(earlier, { recursive: true });
      const doc = deps.store.documents.get(RUN_ID)!;
      doc.input = { ...doc.input, state: "queued", driveFolderId: doc.made!.driveFolderId, files: doc.made!.files };
      assert.equal((await processDocumentsJob(RUN_ID, deps, { final: true })).outcome, "done");
      const all = handbooks(drive);
      assert.deepEqual([all.length, all[0]!.id, all[0]!.uploads], [1, first.id, 2]);
      assert.equal(partOf(all[0]!.body!), `## ${SINCE_TITLE}\n\n${NO_EARLIER}\n`);
      assert.deepEqual(readFileSync(join(deps.runsDir, RUN_ID, "documents", HANDBOOK_DOCX)), all[0]!.body);
    });
  });

  describe("every entry the job lists fits what the console reads (decision 477)", () => {
    // The console's DocumentFile, word for word (blackcloud-next, read-only here,
    // lib/health-check/schemas.ts:118-127 on origin/main). Its documentsFromRow
    // parses the whole row, so one entry it can't read hides the other six too.
    const DocumentOutput = z.enum([...ReportOutput.options, "ceo_one_pager", "action_lists", "tickets", "org_handbook"]);
    const ConsoleFile = z.object({ output: DocumentOutput, name: z.string().min(1), driveFileId: z.string().min(1), url: z.url() });
    const SEVEN = ["action_lists", "ceo_one_pager", "executive_summary", "full_health_check", "org_handbook", "raw_notes", "tickets"];

    function fits(files: DocumentFile[], what: string) {
      assert.ok(z.array(ConsoleFile).safeParse(files).success, what);
      for (const f of files) {
        assert.deepEqual(Object.keys(f).sort(), ["driveFileId", "name", "output", "url"], what);
        assert.ok(typeof f.name === "string" && f.name.length > 0, what);
        assert.ok(typeof f.driveFileId === "string" && f.driveFileId.length > 0, what);
        assert.ok(SEVEN.includes(f.output), `${what}: ${f.output}`);
        // Stricter than z.url(), which takes any scheme: a link a consultant can open.
        assert.match(new URL(f.url).protocol, /^https?:$/, what);
      }
    }

    // What limits the client's name in a file's: the intake's customerName is
    // at least one character (src/narrative/intake.ts), and clientNameOf
    // collapses its spaces, trims it and keeps 120 characters, else "Unnamed client".
    const NAMES: Array<[string, Partial<DocumentsJobInput>]> = [
      ["one character", { intake: { ...INTAKE, customerName: "X" } }],
      ["only spaces", { intake: { ...INTAKE, customerName: "   " } }],
      ["no intake and no reference", { intake: null, run: consoleRun({ status: "released", customerRef: null }) }],
      ["a slash, quotes and letters beyond ASCII", { intake: { ...INTAKE, customerName: `Ōtautahi / "Ngā" Kōrero's — Société\\Ünïcødé 東京 Ltd.` } }],
      ["longer than 120 characters", { intake: { ...INTAKE, customerName: "Synthetic Holdings ".repeat(10) } }],
    ];

    test("on a first make and a remake, for a client name at each extreme: seven entries, one of each kind, the handbook last", async () => {
      for (const [what, overrides] of NAMES) {
        const drive = new MemoryDrive();
        const deps = withHandbook(jobInput(overrides), drive);
        assert.equal((await processDocumentsJob(RUN_ID, deps, { final: true })).outcome, "done", what);
        const doc = deps.store.documents.get(RUN_ID)!;
        const first = doc.made!.files;
        doc.input = { ...doc.input, state: "queued", driveFolderId: doc.made!.driveFolderId, files: first };
        assert.equal((await processDocumentsJob(RUN_ID, deps, { final: true })).outcome, "done", what);
        for (const [make, files] of [["first make", first], ["remake", doc.made!.files]] as const) {
          assert.equal(files.length, 7, `${what}, ${make}`);
          assert.deepEqual(files.map((f) => f.output).sort(), SEVEN, `${what}, ${make}`);
          assert.equal(files.at(-1)!.output, "org_handbook", `${what}, ${make}`);
          assert.ok(files.at(-1)!.name.endsWith(" - Org handbook.docx"), `${what}, ${make}`);
          fits(files, `${what}, ${make}`);
        }
        assert.deepEqual(doc.made!.files.at(-1), first.at(-1), `${what}: the same file and link on the remake`);
      }
    });

    test("a handbook Drive returns no name, id or usable link for is left out, so the six still reach the console", async () => {
      const spoilt: Array<Partial<Record<keyof DriveItem, string | undefined>>> = [{ url: undefined }, { url: "" }, { url: "not a link" }, { url: "javascript:alert(1)" }, { name: "" }, { id: "" }];
      for (const spoil of spoilt) {
        const what = JSON.stringify(spoil);
        const drive = new MemoryDrive();
        const deps = withHandbook(jobInput(), drive);
        const put = drive.putFile.bind(drive);
        // As the real client returns an upload whose answer lacks a field (drive.ts, item).
        drive.putFile = async (...args) => {
          const item = await put(...args);
          return args[1] === NAME ? ({ ...item, ...spoil } as typeof item) : item;
        };
        assert.equal((await processDocumentsJob(RUN_ID, deps, { final: true })).outcome, "done", what);
        const made = deps.store.documents.get(RUN_ID)!.made!;
        assert.deepEqual(made.files.map((f) => f.name).sort(), SIX, what);
        fits(made.files, what);
        assert.equal(handbooks(drive).length, 1, `${what}: still in the customer's folder`);
        assert.deepEqual(made.warnings, [OVERFLOW], what);
        assert.ok(deps.lines.includes(`Run ${RUN_ID}: the org handbook is in Drive but isn't listed to the console, because Drive didn't return a name, id and link it can use.`), what);
      }
    });
  });

  test("each sentence begins exactly with the words the console splits on", () => {
    // The console's DocumentsCard tells the handbook's warning from a layout
    // warning by this prefix, word for word, so none of the four may change it.
    for (const sentence of Object.values(HANDBOOK_NOT_MADE)) assert.ok(sentence.startsWith("The org handbook wasn't made:"), sentence);
    assert.equal(Object.keys(HANDBOOK_NOT_MADE).length, 4);
  });

  test("every warning it can add fits what the console reads", () => {
    // The console's reader (blackcloud-next, read-only here):
    //   lib/health-check/schemas.ts:143        warnings: z.array(z.string()).nullable()   (no cap on count or length)
    //   lib/health-check/postgres-rows.ts:385  documentsFromRow parses the whole row with RunDocuments, so one bad value refuses it
    //   migrations/20261105_health_check_documents.sql:20  "warnings" JSONB, which can't store a NUL
    //   app/internal/health-check/(console)/[runId]/release/DocumentsCard.tsx:168-172  each warning is a list item keyed by its text
    // So each sentence is a string on one line with no control character, and none repeats a layout warning.
    const ConsoleWarnings = z.array(z.string()).nullable();
    const sentences = Object.values(HANDBOOK_NOT_MADE);
    assert.equal(new Set(sentences).size, sentences.length);
    for (const sentence of sentences) {
      assert.ok(ConsoleWarnings.safeParse([OVERFLOW, sentence]).success, sentence);
      assert.doesNotMatch(sentence, /[\u0000-\u001F\u007F]/, sentence);
      assert.match(sentence, /^The org handbook wasn't made: [^.]+\.$/, sentence);
      assert.ok(sentence.length <= 200, sentence);
      assert.doesNotMatch(sentence, /^The (executive summary|full health check|one page summary)/, "never read as a layout warning's page");
    }
  });
});

// Decision 495: the org handbook prints a summary only when a person approved
// the row, and only one it can print as it stands, under its own class or
// trigger. Every other case makes the document it made before summaries.
describe("Claude's summaries in the org handbook", () => {
  const LEFT_OUT = { tests: 0, tooLarge: 0, overLimit: 0, unreadable: 0, refused: 0, notWritten: 0 };
  type Item = ConsoleSummariesRow["items"][number];
  const row = (status: ConsoleSummariesRow["status"], items: Item[], leftOut = LEFT_OUT): ConsoleSummariesRow => ({
    runId: RUN_ID,
    status,
    model: "claude-haiku-5-5",
    madeAt: "2026-11-03T10:00:00.000Z",
    items,
    leftOut,
    decidedBy: status === "waiting" ? null : "approver.zz@example.com",
    decidedAt: status === "waiting" ? null : "2026-11-05T11:11:00.000Z",
  });
  const item = (kind: Item["kind"], name: string, summary: string, struck = false): Item => ({ kind, name, summary, struck });
  const GOOD = item("class", "LeadRouter", "Quokka routes each new lead to the right queue.");
  const STRUCK = item("class", "FinanceSync", "Zebrastruck copies each invoice to finance.", true);
  const LABEL = "Summary (written by Claude, checked by us): ";
  const INTRO = "Some classes and triggers below carry a short summary. Claude, an AI model, wrote each one from the code, and one of our consultants read and approved it before it reached you. A summary is a guide to what the code does: the code is what runs.";
  const OPENING_BEFORE = "We haven't added to it or summarised it, except that what each piece of Apex touches is read from its code, as that part explains.";
  const OPENING_AFTER = "We haven't added to it, except that what each piece of Apex touches is read from its code and that some classes and triggers carry a short summary written by Claude and approved by us, as that part explains.";

  /** A make with the recorded handbook and the run's map; the row as the console holds it, or none. */
  async function make(summaries: unknown, switchOn = true) {
    const drive = new MemoryDrive();
    const deps = setup(jobInput(), drive);
    deps.apexSummaries = switchOn;
    for (const file of [HANDBOOK_FILE, "pseudonym-map.json"]) cpSync(join(recorded, file), join(deps.runsDir, RUN_ID, file));
    if (summaries !== undefined) deps.store.summaries.set(RUN_ID, summaries as ConsoleSummariesRow);
    const result = await processDocumentsJob(RUN_ID, deps, { final: true });
    assert.equal(result.outcome, "done", result.detail);
    const body = [...drive.items.values()].find((i) => !i.folder && /Org handbook/.test(i.name))!.body!;
    return { body, text: docxText(body), paragraphs: docxParagraphs(body), deps };
  }

  /**
   * The Word file's parts, with the two times the docx package stamps on every
   * file it makes taken out of docProps/core.xml: everything else, byte for byte.
   */
  const parts = (bytes: Buffer): Record<string, string> =>
    Object.fromEntries(Object.entries(unzipSync(new Uint8Array(bytes))).map(([name, data]) => [name, name === "docProps/core.xml" ? strFromU8(data).replace(/<dcterms:(created|modified)[^>]*>[^<]*<\/dcterms:\1>/g, "") : strFromU8(data)]));

  test("an approved row with two, one struck out: the other once, under its own class, labelled; the intro, the counts and the changed opening", async () => {
    const { text, paragraphs } = await make(row("approved", [GOOD, STRUCK], { ...LEFT_OUT, tests: 2, tooLarge: 1, unreadable: 1 }));
    assert.equal(text.split(`${LABEL}${GOOD.summary}`).length - 1, 1);
    const own = paragraphs.filter((p) => p.text.includes(GOOD.summary));
    assert.equal(own.length, 1);
    assert.match(own[0]!.text, /^LeadRouter\n/);
    assert.ok(own[0]!.text.endsWith(`\n${LABEL}${GOOD.summary}`), "after the entry's own lines");
    assert.ok(!text.includes("Zebrastruck"));
    // Said once, at the top of the Apex classes, then counted: the struck one with the unreadable one.
    const at = paragraphs.findIndex((p) => p.heading === 2 && p.text === "Apex classes");
    assert.deepEqual(paragraphs.slice(at + 1, at + 3).map((p) => p.text), [INTRO, "Of the Apex classes and triggers we read, 1 carries a summary, and the others are 2 test classes, 1 that was too large to summarise whole and 2 that we couldn't summarise or chose to leave out."]);
    assert.equal(text.split(INTRO).length - 1, 1);
    assert.ok(text.includes(OPENING_AFTER) && !text.includes(OPENING_BEFORE));
    // Who approved it is never printed.
    assert.ok(!text.includes("approver.zz") && !text.includes("11:11"));
  });

  test("no row, waiting, left out, the switch off, a row that doesn't read, or all struck: the document made without summaries, part for part", async () => {
    const plain = await make(undefined, false);
    const without = parts(plain.body);
    assert.ok(plain.text.includes(OPENING_BEFORE));
    const cases: Array<[string, unknown, boolean]> = [
      ["no row", undefined, true],
      ["waiting", row("waiting", [GOOD]), true],
      ["left out", row("left_out", [GOOD]), true],
      ["the switch off, approved", row("approved", [GOOD]), false],
      ["a row that doesn't read", { ...row("approved", [GOOD]), items: [{ ...GOOD, struck: "no" }] }, true],
      ["a row with a status the console doesn't have", { ...row("approved", [GOOD]), status: "done" }, true],
      // Over the console's limit, so the whole row is unreadable, as the console reads it: the good one doesn't print either.
      ["a row holding a 1,201-character summary", row("approved", [GOOD, item("trigger", "InvoiceAudit", `Zebra5 ${"s".repeat(1_194)}`)]), true],
      ["approved, all struck", row("approved", [{ ...GOOD, struck: true }, STRUCK]), true],
      ["approved, nothing printable", row("approved", [item("class", "NotInThisOrg", "Zebragone.")]), true],
    ];
    for (const [what, summaries, on] of cases) {
      const made = await make(summaries, on);
      assert.deepEqual(parts(made.body), without, what);
      assert.ok(!made.text.includes("Quokka") && !made.text.includes(LABEL), what);
    }
  });

  test("planted and not printed, each counted with the rest; a summary that only looks like a name prints as written", async () => {
    const baseline = (await make(undefined, false)).text;
    // A person the run's map knows whom the handbook doesn't otherwise name.
    const [person, other] = loadPseudonymMap(recorded).entries().filter((e) => !baseline.includes(e.name) && /^\w+ \w+$/.test(e.name));
    assert.ok(person && other, "the map has two people the handbook doesn't name");
    // Another of them, by name and by their token's code without its brackets: text, not a token.
    const LOOKS_LIKE = item("trigger", "InvoiceTrigger", `Quokkalike fills in ${other.token.slice(2, -2)} for ${other.name} as the code spells it.`);
    // A summary over 1,200 characters can't be in a row the console reads (the case above);
    // printableText refuses one all the same (summary-row.test.ts), and a line break stands in for it here.
    const planted = [
      item("class", "FinanceSync", `Zebra1 copies invoices for ${person.token} each night.`),
      item("class", "LegacySync", "Zebra2 runs for [[user-9999]] alone."),
      item("class", "OpportunityHandler", "Zebra3 reads the record [[id]] first."),
      item("trigger", "AccountTrigger", "Zebra4 hands each account on.\nThen it stops."),
      item("class", "NotInThisOrg", "Zebra6 does nothing here."),
      item("trigger", "LeadRouter", "Zebra7 is a class, not a trigger."),
    ];
    const { text } = await make(row("approved", [GOOD, ...planted, LOOKS_LIKE]));
    for (const n of [1, 2, 3, 4, 6, 7]) assert.ok(!text.includes(`Zebra${n}`), `Zebra${n}`);
    assert.ok(!text.includes(person.name) && !text.includes(person.token), "the person's name, and their token");
    assert.ok(!text.includes("[[user-9999]]") && !text.includes("[[id]]"));
    assert.ok(text.includes("Of the Apex classes and triggers we read, 2 carry a summary, and the others are 6 that we couldn't summarise or chose to leave out."));
    // Never through the run's names: printed exactly as approved.
    assert.ok(text.includes(`${LABEL}${LOOKS_LIKE.summary}`));
    assert.ok(text.includes(`${LABEL}${GOOD.summary}`));
  });

  test("the job asks the store for the row only with the switch on", async () => {
    for (const on of [false, true]) {
      const deps = setup(jobInput(), new MemoryDrive());
      deps.apexSummaries = on;
      const asked: unknown[] = [];
      const load = deps.store.loadDocumentsJob.bind(deps.store);
      deps.store.loadDocumentsJob = async (runId, reads) => (asked.push(reads), load(runId, reads));
      await processDocumentsJob(RUN_ID, deps, { final: true });
      assert.deepEqual(asked, [{ handbookSummaries: on }]);
    }
  });

  test("the reports are never given the summaries", async () => {
    const drive = new MemoryDrive();
    const given: unknown[] = [];
    const deps = baseDeps({ drive, renderReleased: async (dir, approved, out) => (given.push(approved), fakeRender(dir, approved, out)), apexSummaries: true });
    mkdirSync(join(deps.runsDir, RUN_ID), { recursive: true });
    writeFileSync(join(deps.runsDir, RUN_ID, "run.json"), "{}");
    cpSync(join(recorded, HANDBOOK_FILE), join(deps.runsDir, RUN_ID, HANDBOOK_FILE));
    deps.store.addDocumentsJob(jobInput());
    deps.store.summaries.set(RUN_ID, row("approved", [GOOD]));
    assert.equal((await processDocumentsJob(RUN_ID, deps, { final: true })).outcome, "done");
    assert.equal(given.length, 1);
    assert.ok(!("handbookSummaries" in (given[0] as object)) && !JSON.stringify(given[0]).includes("Quokka"));
  });
});
