import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { generateKeyPairSync } from "node:crypto";
import { cpSync, existsSync, mkdirSync, statSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { buildReport, currentBody, type ReportOutputKey } from "../../narrative/sections.ts";
import { approvedReport, layoutWarnings, renderReleased, type ApprovedRun } from "../../render/render-released.ts";
import { loadRun } from "../../render/render-run.ts";
import type { ReportSection, SectionVersion } from "../../schemas/index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers } from "../../testing/replay.ts";
import { clientNameOf, folderFor, processDocumentsJob } from "../documents-job.ts";
import { DriveError, GoogleDrive, type DocumentDrive, type DriveItem } from "../drive.ts";
import type { DocumentsJobInput } from "../types.ts";
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
  readonly items = new Map<string, DriveItem & { parent: string; folder: boolean; bytes?: number; uploads: number }>();
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
  async putFile(parentId: string, name: string, _mime: string, body: Buffer, replaceId?: string) {
    const item = replaceId ? this.items.get(replaceId)! : (this.#add(parentId, name, false) as DriveItem & { uploads: number; bytes?: number });
    Object.assign(item, { name, bytes: body.length, uploads: (item as { uploads: number }).uploads + 1 });
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
  const files = {} as Record<ReportOutputKey, string>;
  for (const output of ["raw_notes", "executive_summary", "full_health_check"] as ReportOutputKey[]) {
    files[output] = join(out, `${output}.bin`);
    writeFileSync(files[output], `synthetic ${output}`);
  }
  return { files, warnings: ["The full health check, page 4: overflow"] };
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
  test("puts the three documents in Health checks / client / Health check 1, and tells the console", async () => {
    const drive = new MemoryDrive();
    const deps = setup(jobInput(), drive);
    const result = await processDocumentsJob(RUN_ID, deps, { final: true });
    assert.equal(result.outcome, "done", result.detail);
    const made = deps.store.documents.get(RUN_ID)!.made!;
    assert.equal(made.clientName, "Synthetic Co");
    assert.equal(made.folderName, "Health check 1");
    assert.equal(drive.path(made.driveFolderId), "Synthetic Co / Health check 1");
    assert.deepEqual(made.files.map((f) => f.name).sort(), ["Synthetic Co - Executive summary.pdf", "Synthetic Co - Full health check.pdf", "Synthetic Co - Raw notes.docx"]);
    assert.deepEqual(made.warnings, ["The full health check, page 4: overflow"]);
    assert.equal(deps.store.documents.get(RUN_ID)!.input.state, "done");
  });

  test("a client's next health check is number 2, in the same client folder, whatever the case of its name", async () => {
    const drive = new MemoryDrive();
    await processDocumentsJob(RUN_ID, setup(jobInput(), drive), { final: true });
    const second = jobInput({ run: consoleRun({ id: "hc-2026-12-01-00000002", status: "released" }), intake: { ...INTAKE, customerName: "synthetic co " } });
    const deps = setup(second, drive);
    assert.equal((await processDocumentsJob(second.run.id, deps, { final: true })).outcome, "done");
    assert.equal(deps.store.documents.get(second.run.id)!.made!.folderName, "Health check 2");
    assert.equal((await drive.folders(drive.rootFolderId)).length, 1);
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
    assert.equal((await drive.files(v2.id)).length, 3, "three files, none twice");
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
    assert.ok(result.warnings.every((w) => /^The (executive summary|full health check)(, page \d+)?: [a-z ]+$/.test(w)), JSON.stringify(result.warnings));
    // The cover has room for the consultant's line.
    assert.deepEqual(result.warnings.filter((w) => /, page 1:/.test(w)), []);
  });

  test("layout warnings leave the detail out", () => {
    const warnings = layoutWarnings({
      executive: { pages: 6, passed: false, problems: [{ page: 2, check: "empty section", detail: 'only "Ada Lovelace"' }] },
      full: { pages: 40, passed: false, problems: [{ page: 0, check: "page count", detail: "61 pages" }] },
    });
    assert.deepEqual(warnings, ["The executive summary, page 2: empty section", "The full health check: page count"]);
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
    assert.match(calls[1]!.url, /fields=id%2Cname%2CwebViewLink|fields=id,name,webViewLink/);
    assert.equal((calls[1]!.init.headers as Record<string, string>)["X-Upload-Content-Length"], "5");
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
    const verdict = report.outputs.executive_summary.sections.find((s) => s.key === "verdict")!;
    assert.notDeepEqual(checkNumbers("Ten Eyck", scopeDataset(report.dataset, [...verdict.findingIds, ...verdict.metricIds])).unmatched, []);
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
