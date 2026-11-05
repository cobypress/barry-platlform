import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import type { ReportOutputKey } from "../narrative/sections.ts";
import { AlreadyContributed, contributeBenchmark } from "../benchmark/export.ts";
import { longDate } from "../render/format.ts";
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
// the folder it had, with each make in its own numbered version folder inside it
// (versionFolderFor). The documents hold real names, so they're printed
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

const VERSION = /^Version (\d+)\b/;

/**
 * Where this make's files go. The first time, straight into the run's folder.
 * Made again, the earlier files move into "Version 1, <date>" (a move keeps a
 * file's id, so links already sent still work) and the new ones go into the
 * next numbered version. A version folder holding none of the last make's files
 * is one a failed attempt started, so a retry finishes it rather than starting
 * another.
 */
export async function versionFolderFor(drive: DocumentDrive, folder: DriveItem, previous: DocumentFile[], now: string): Promise<DriveItem> {
  if (previous.length === 0) return folder;
  const ids = new Set(previous.map((f) => f.driveFileId));
  const versions = (await drive.folders(folder.id))
    .map((f) => ({ folder: f, n: Number(VERSION.exec(f.name.trim())?.[1] ?? 0) }))
    .filter((v) => v.n > 0)
    .sort((a, b) => a.n - b.n);
  const loose = (await drive.files(folder.id)).filter((f) => ids.has(f.id));
  if (loose.length > 0) {
    const made = loose.map((f) => f.createdAt).filter((d): d is string => Boolean(d)).sort()[0];
    let first = versions.find((v) => v.n === 1)?.folder;
    if (!first) {
      first = await drive.createFolder(folder.id, made ? `Version 1, ${longDate(made.slice(0, 10))}` : "Version 1");
      versions.unshift({ folder: first, n: 1 });
    }
    for (const f of loose) await drive.moveFile(f.id, folder.id, first.id);
  }
  const last = versions.at(-1);
  // The last make's files have gone from Drive altogether: start again in the run's folder.
  if (!last) return folder;
  if (!(await drive.files(last.folder.id)).some((f) => ids.has(f.id))) return last.folder;
  return drive.createFolder(folder.id, `Version ${last.n + 1}, ${longDate(now.slice(0, 10))}`);
}

/**
 * A run whose customer opted in at intake adds its figures to the benchmark set
 * once it's released: the figures are final then, and nothing about them leads
 * back to the run. Once per run and once per org in any year, which the consent
 * record in the run folder enforces. Never fails the documents: a run that can't
 * contribute (too few won deals to band, no industry recorded) just says why.
 */
async function contribute(runId: string, dir: string, input: DocumentsJobInput, deps: WorkerDeps, log: (line: string) => void): Promise<void> {
  try {
    const { rows } = await contributeBenchmark(
      { runDir: dir, industry: input.intake?.industry, headcount: input.intake?.headcountBand, consent: `Opted in at intake by ${input.run.createdBy}; added when the run was released`, now: (deps.now ?? (() => new Date()))() },
      (rows) => deps.store.addBenchmarkRows(rows),
    );
    log(`Run ${runId} added ${rows.length} figures to the benchmark set.`);
  } catch (err) {
    if (err instanceof AlreadyContributed) return;
    log(`Run ${runId} didn't add to the benchmark set: ${safeReason(err)}`);
  }
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
    const target = await versionFolderFor(deps.drive, folder, input.files, now());
    const existing = await deps.drive.files(target.id);
    const files: DocumentFile[] = [];
    for (const output of Object.keys(FILES) as ReportOutputKey[]) {
      const { title, ext, mime } = FILES[output];
      const name = `${clientName} - ${title}.${ext}`;
      // Ours from last time, or one of the same name someone put there.
      const ours = input.files.find((f) => f.output === output)?.driveFileId;
      const replaceId = existing.find((e) => e.id === ours)?.id ?? existing.find((e) => e.name === name)?.id;
      const item = await deps.drive.putFile(target.id, name, mime, readFileSync(printed.files[output]), replaceId);
      files.push({ output, name: item.name, driveFileId: item.id, url: item.url });
    }
    await deps.store.saveDocuments(runId, { at: now(), clientName, folderName: folder.name, driveFolderId: folder.id, driveFolderUrl: folder.url, files, warnings: printed.warnings });
    log(`Documents for ${runId} are in Drive${printed.warnings.length ? `, with ${printed.warnings.length} layout warnings` : ""}.`);
    if (input.run.benchmarkOptIn) await contribute(runId, dir, input, deps, log);
    return { outcome: "done", detail: `${files.length} documents in ${folder.name}${target.id === folder.id ? "" : ` / ${target.name}`}` };
  } catch (err) {
    const reason = safeReason(err);
    // Printing and uploading again is safe: the folders and files are reused.
    if (!attempt.final) throw new Error(reason);
    return fail(`The documents couldn't be made: ${reason}`);
  }
}
