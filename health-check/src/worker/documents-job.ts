import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import type { ReportOutputKey } from "../narrative/sections.ts";
import { renderReleased } from "../render/render-released.ts";
import { safeReason, type Attempt, type JobOutcome, type WorkerDeps } from "./deps.ts";
import type { DocumentDrive, DriveItem } from "./drive.ts";
import { withJobLock } from "./lock.ts";
import type { DocumentFile, DocumentsJobInput } from "./types.ts";

// A documents job: once the console has released a run, print its three
// documents in the approved wording and put them in Google Drive, as
//
//   Health checks / <client> / Health check <n> / <client> - Executive summary.pdf
//
// The number counts that client's health checks. A run made again goes back to
// the folder it had, and each file is replaced in place, so a link someone
// already has keeps working. The documents hold real names, so they're printed
// in the run folder on the worker's disk and go only to Drive. What the console
// is told holds no names beyond the client's, which it already has.

const RUN_ID = /^[A-Za-z0-9][A-Za-z0-9_-]{0,99}$/;
const NUMBERED = /^Health check (\d+)$/;

const FILES: Record<ReportOutputKey, { title: string; ext: string; mime: string }> = {
  executive_summary: { title: "Executive summary", ext: "pdf", mime: "application/pdf" },
  full_health_check: { title: "Full health check", ext: "pdf", mime: "application/pdf" },
  raw_notes: { title: "Raw notes", ext: "docx", mime: "application/vnd.openxmlformats-officedocument.wordprocessingml.document" },
};

/** The client's folder name: the company the customer gave, else the consultant's reference. */
export function clientNameOf(input: Pick<DocumentsJobInput, "intake" | "run">): string {
  const name = (input.intake?.customerName ?? input.run.customerRef ?? "").replace(/\s+/g, " ").trim().slice(0, 120);
  return name || "Unnamed client";
}

const same = (a: string, b: string): boolean => a.trim().toLowerCase() === b.trim().toLowerCase();

/** The run's folder: the one it had, or the client's next "Health check <n>". */
export async function folderFor(drive: DocumentDrive, clientName: string, earlier: string | null): Promise<DriveItem> {
  if (earlier) {
    const folder = await drive.folder(earlier);
    if (folder) return folder;
  }
  const client = (await drive.folders(drive.rootFolderId)).find((f) => same(f.name, clientName)) ?? (await drive.createFolder(drive.rootFolderId, clientName));
  const taken = (await drive.folders(client.id)).map((f) => Number(NUMBERED.exec(f.name.trim())?.[1] ?? 0));
  return drive.createFolder(client.id, `Health check ${Math.max(0, ...taken) + 1}`);
}

export async function processDocumentsJob(runId: string, deps: WorkerDeps, attempt: Attempt): Promise<JobOutcome> {
  if (!RUN_ID.test(runId)) return { outcome: "failed", detail: "The run id isn't one the worker can use as a folder name" };
  // The run's own lock: a run job for it can't be writing its folder meanwhile.
  return withJobLock(deps.locksDir, `run-${runId}`, () => documentsLocked(runId, deps, attempt));
}

async function documentsLocked(runId: string, deps: WorkerDeps, attempt: Attempt): Promise<JobOutcome> {
  const log = deps.log ?? (() => {});
  const now = () => (deps.now ?? (() => new Date()))().toISOString();
  const input = await deps.store.loadDocumentsJob(runId);
  if (!input) return { outcome: "skipped", detail: `No run ${runId}` };
  if (input.run.status !== "released") return { outcome: "skipped", detail: `Run ${runId} isn't released, so it has no documents yet` };
  if (input.state !== "queued" && input.state !== "making") return { outcome: "skipped", detail: `The documents for ${runId} weren't asked for` };

  const fail = async (reason: string): Promise<JobOutcome> => {
    await deps.store.failDocuments(runId, reason, now());
    return { outcome: "failed", detail: reason };
  };
  if (!deps.drive) return fail("Google Drive isn't set up on the worker. Set HC_DRIVE_SA_EMAIL, HC_DRIVE_SA_KEY and HC_DRIVE_FOLDER_ID, then make the documents again.");
  // The folder is the worker's own, named after the run, never a path from the database.
  const dir = join(deps.runsDir, runId);
  if (!existsSync(join(dir, "run.json"))) return fail("The run's folder isn't on the worker any more, so its documents can't be made.");
  if (!(await deps.store.startDocuments(runId, now()))) return { outcome: "skipped", detail: `The documents for ${runId} weren't asked for` };

  try {
    const clientName = clientNameOf(input);
    const printed = await (deps.renderReleased ?? renderReleased)(dir, input, join(dir, "documents"));
    const folder = await folderFor(deps.drive, clientName, input.driveFolderId);
    const existing = await deps.drive.files(folder.id);
    const files: DocumentFile[] = [];
    for (const output of Object.keys(FILES) as ReportOutputKey[]) {
      const { title, ext, mime } = FILES[output];
      const name = `${clientName} - ${title}.${ext}`;
      // Ours from last time, or one of the same name someone put there.
      const ours = input.files.find((f) => f.output === output)?.driveFileId;
      const replaceId = existing.find((e) => e.id === ours)?.id ?? existing.find((e) => e.name === name)?.id;
      const item = await deps.drive.putFile(folder.id, name, mime, readFileSync(printed.files[output]), replaceId);
      files.push({ output, name: item.name, driveFileId: item.id, url: item.url });
    }
    await deps.store.saveDocuments(runId, { at: now(), clientName, folderName: folder.name, driveFolderId: folder.id, driveFolderUrl: folder.url, files, warnings: printed.warnings });
    log(`Documents for ${runId} are in Drive${printed.warnings.length ? `, with ${printed.warnings.length} layout warnings` : ""}.`);
    return { outcome: "done", detail: `${files.length} documents in ${folder.name}` };
  } catch (err) {
    const reason = safeReason(err);
    // Printing and uploading again is safe: the folder and files are reused.
    if (!attempt.final) throw new Error(reason);
    return fail(`The documents couldn't be made: ${reason}`);
  }
}
