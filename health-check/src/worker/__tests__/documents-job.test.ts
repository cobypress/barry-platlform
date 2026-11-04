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
    const item = { id, name, url: `https://drive.example.com/${id}`, parent, folder, uploads: 0 };
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

  test("made again, the run keeps its folder and each file keeps its id", async () => {
    const drive = new MemoryDrive();
    const deps = setup(jobInput(), drive);
    await processDocumentsJob(RUN_ID, deps, { final: true });
    const first = deps.store.documents.get(RUN_ID)!.made!;
    deps.store.documents.get(RUN_ID)!.input.state = "queued";
    assert.equal((await processDocumentsJob(RUN_ID, deps, { final: true })).outcome, "done");
    const again = deps.store.documents.get(RUN_ID)!.made!;
    assert.equal(again.driveFolderId, first.driveFolderId);
    assert.deepEqual(again.files.map((f) => f.driveFileId).sort(), first.files.map((f) => f.driveFileId).sort());
    assert.ok(again.files.every((f) => (drive.items.get(f.driveFileId) as { uploads: number }).uploads === 2));
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
    const result = await renderReleased(dir, approvedRun(), join(dir, "documents"));
    for (const file of Object.values(result.files)) assert.ok(existsSync(file) && statSync(file).size > 1000, file);
    assert.ok(result.warnings.every((w) => /^The (executive summary|full health check)(, page \d+)?: [a-z ]+$/.test(w)), JSON.stringify(result.warnings));
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

  test("won't send the token to an upload address that isn't Google's", async () => {
    const { impl, calls } = fakeFetch([(url) => (url.includes("/upload/") ? new Response(null, { status: 200, headers: { location: "https://elsewhere.example.com/upload" } }) : undefined)]);
    await assert.rejects(new GoogleDrive(config, impl).putFile("folder-0000000001", "x.pdf", "application/pdf", Buffer.from("x")), /doesn't trust/);
    assert.ok(!calls.some((c) => c.url.startsWith("https://elsewhere.example.com")));
  });

  test("says what Google refused, and refuses an id that could change a query", async () => {
    const { impl } = fakeFetch([() => Response.json({ error: { message: "File not shared with the service account" } }, { status: 403 })]);
    await assert.rejects(new GoogleDrive(config, impl).folders("root-folder-0001"), /\(403\): File not shared with the service account/);
    await assert.rejects(new GoogleDrive(config, impl).folders("x' or name contains 'a"), /isn't a Drive folder id/);
  });
});
