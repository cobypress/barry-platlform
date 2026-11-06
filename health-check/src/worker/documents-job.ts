import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import type { ReportOutputKey } from "../narrative/sections.ts";
import type { BenchmarkInput } from "../benchmark/compare.ts";
import { AlreadyContributed, CONSENT_FILE, contributeBenchmark } from "../benchmark/export.ts";
import { longDate } from "../render/format.ts";
import { renderReleased, type DocumentKey, type ReleasedDocuments } from "../render/render-released.ts";
import { safeReason, type Attempt, type JobOutcome, type WorkerDeps } from "./deps.ts";
import type { DocumentDrive, DriveItem } from "./drive.ts";
import { withJobLock } from "./lock.ts";
import { sealPages } from "../microsite/seal.ts";
import { addMonths } from "./retention.ts";
import type { DocumentFile, DocumentsJobInput } from "./types.ts";
import { SURVEY_FILE } from "../render/render-run.ts";
import { Survey } from "../survey/questions.ts";

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

const DOCX = "application/vnd.openxmlformats-officedocument.wordprocessingml.document";

/** What goes in the client's folder, shared with the customer. */
const FILES: Record<Exclude<DocumentKey, "readout_kit">, { title: string; ext: string; mime: string }> = {
  executive_summary: { title: "Executive summary", ext: "pdf", mime: "application/pdf" },
  full_health_check: { title: "Full health check", ext: "pdf", mime: "application/pdf" },
  raw_notes: { title: "Raw notes", ext: "docx", mime: DOCX },
  ceo_one_pager: { title: "One page summary", ext: "pdf", mime: "application/pdf" },
  // The action lists (DECISIONS.md 197). In the client's folder beside the decks,
  // and listed to the console, whose documents card says what each is.
  action_lists: { title: "Action lists", ext: "xlsx", mime: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" },
  tickets: { title: "Tickets", ext: "csv", mime: "text/csv" },
};

/**
 * The files the console's documents card lists: the three reviewed outputs, the
 * one page summary (blackcloud-next 52efbf6, DECISIONS.md 139) and the action
 * lists and tickets, which its DocumentOutput accepts from hc/action-docs
 * (DECISIONS.md 197). That console must be live before this worker: an output it
 * doesn't know makes it refuse the whole documents row. The readout kit is never
 * listed: it's for Black Cloud only.
 */
const LISTED_TO_CONSOLE: ReadonlySet<DocumentKey> = new Set<DocumentKey>(["executive_summary", "full_health_check", "raw_notes", "ceo_one_pager", "action_lists", "tickets"]);

/**
 * Black Cloud's own folder for readout kits, beside the clients' folders, never
 * inside one. Drive gives a subfolder everyone its parent is shared with, so a
 * kit inside a client's folder would reach the customer the day that folder is
 * shared (DECISIONS.md 138).
 */
export const INTERNAL_FOLDER = "Black Cloud only (not for clients)";

/** The client's folder name: the company the customer gave, else the consultant's reference. */
/**
 * Survey answers the console holds that the reviewed sections weren't built
 * from. The documents print what was approved, so these are only counted.
 */
export function laterSurveyAnswers(dir: string, current: Survey | null | undefined): number {
  if (!current) return 0;
  const path = join(dir, SURVEY_FILE);
  const seen = new Set(existsSync(path) ? Survey.parse(JSON.parse(readFileSync(path, "utf8"))).responses.map((r) => r.id) : []);
  return current.responses.filter((r) => !seen.has(r.id)).length;
}

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
export async function versionFolderFor(drive: DocumentDrive, folder: DriveItem, previous: DocumentFile[], now: string, alsoOurs: string[] = []): Promise<DriveItem> {
  if (previous.length === 0) return folder;
  const ids = new Set(previous.map((f) => f.driveFileId));
  // Ours too, though the console isn't told of them: the one-pager, by its name.
  for (const f of await drive.files(folder.id)) if (alsoOurs.includes(f.name)) ids.add(f.id);
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

/** The readout kit, in Black Cloud's own folder: one per client and health check, replaced on each make. */
async function putReadoutKit(drive: DocumentDrive, clientName: string, folderName: string, path: string): Promise<DriveItem> {
  return putInternal(drive, clientName, folderName, `${clientName} - Readout kit.docx`, path);
}

/** A file for Black Cloud only, beside the clients' folders and never inside one, replaced on each make. */
async function putInternal(drive: DocumentDrive, clientName: string, folderName: string, file: string, path: string): Promise<DriveItem> {
  const internal = (await drive.folders(drive.rootFolderId)).find((f) => same(f.name, INTERNAL_FOLDER)) ?? (await drive.createFolder(drive.rootFolderId, INTERNAL_FOLDER));
  const name = `${clientName} - ${folderName}`;
  const own = (await drive.folders(internal.id)).find((f) => same(f.name, name)) ?? (await drive.createFolder(internal.id, name));
  const replaceId = (await drive.files(own.id)).find((f) => f.name === file)?.id;
  return drive.putFile(own.id, file, DOCX, readFileSync(path), replaceId);
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

/**
 * Other customers' figures to compare with. A run that has already added its
 * own (a consent record in its folder) is in the set, and the comparison takes
 * it out. If the table can't be read, the documents compare with published
 * research only rather than not being made.
 */
async function benchmarkFor(dir: string, deps: WorkerDeps, log: (line: string) => void): Promise<BenchmarkInput | null> {
  try {
    return { rows: await deps.store.loadBenchmarkRows(), includesThisRun: existsSync(join(dir, CONSENT_FILE)) };
  } catch (err) {
    log(`The benchmark set couldn't be read, so the documents compare with published research only: ${safeReason(err)}`);
    return null;
  }
}

/**
 * The customer microsite: its pages, sealed for the console to open, kept for 12
 * months (decision 21). Never a reason to fail the documents, which are already
 * in Drive: a missing key or a page that fails the number check is logged.
 */
async function publishMicrosite(runId: string, microsite: ReleasedDocuments["microsite"], deps: WorkerDeps, at: string, log: (line: string) => void): Promise<void> {
  if (!microsite) return;
  if (!deps.micrositeKey) return log(`Run ${runId} has no microsite: HC_MICROSITE_KEY isn't set on the worker.`);
  if (!microsite.ok) return log(`Run ${runId} has no microsite: ${microsite.reason}.`);
  try {
    const sealed = sealPages(JSON.stringify(microsite.pages), runId, deps.micrositeKey);
    await deps.store.saveMicrosite(runId, sealed, at, addMonths(new Date(at), 12).toISOString());
    log(`Run ${runId}'s microsite is ready, with ${microsite.pages.pages.length} pages.`);
  } catch (err) {
    log(`Run ${runId}'s microsite couldn't be saved: ${safeReason(err)}`);
  }
}

async function documentsLocked(runId: string, deps: WorkerDeps, attempt: Attempt): Promise<JobOutcome> {
  const log = deps.log ?? (() => {});
  const now = () => (deps.now ?? (() => new Date()))().toISOString();
  const input = await deps.store.loadDocumentsJob(runId);
  if (!input) return { outcome: "skipped", detail: `No run ${runId}` };
  if (input.run.product === "pulse") return { outcome: "skipped", detail: `Run ${runId} is a pulse, which never has documents` };
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
    const benchmark = await benchmarkFor(dir, deps, log);
    const later = laterSurveyAnswers(dir, input.survey);
    if (later) log(`${later} survey answers came in after the sections were built, so the documents leave them out, as approved`);
    const printed = await (deps.renderReleased ?? renderReleased)(dir, { ...input, benchmark }, join(dir, "documents"));
    const folder = await folderFor(deps.drive, clientName, input.driveFolderId);
    const nameOf = (output: keyof typeof FILES): string => `${clientName} - ${FILES[output].title}.${FILES[output].ext}`;
    // Also by name, for a make from before the console was told of these three.
    const target = await versionFolderFor(deps.drive, folder, input.files, now(), [nameOf("ceo_one_pager"), nameOf("action_lists"), nameOf("tickets")]);
    const existing = await deps.drive.files(target.id);
    const files: DocumentFile[] = [];
    for (const output of Object.keys(FILES) as Array<keyof typeof FILES>) {
      const { mime } = FILES[output];
      const name = nameOf(output);
      // Ours from last time, or one of the same name someone put there.
      const ours = input.files.find((f) => f.output === output)?.driveFileId;
      const replaceId = existing.find((e) => e.id === ours)?.id ?? existing.find((e) => e.name === name)?.id;
      const item = await deps.drive.putFile(target.id, name, mime, readFileSync(printed.files[output]), replaceId);
      if (LISTED_TO_CONSOLE.has(output)) files.push({ output: output as DocumentFile["output"], name: item.name, driveFileId: item.id, url: item.url });
    }
    await putReadoutKit(deps.drive, clientName, folder.name, printed.files.readout_kit);
    // Only a re-check whose customer agreed to be a case study has one, and it's a draft for us to finish with them.
    if (printed.caseStudy) await putInternal(deps.drive, clientName, folder.name, `${clientName} - Case study draft.docx`, printed.caseStudy);
    await deps.store.saveDocuments(runId, { at: now(), clientName, folderName: folder.name, driveFolderId: folder.id, driveFolderUrl: folder.url, files, warnings: printed.warnings });
    log(`Documents for ${runId} are in Drive${printed.warnings.length ? `, with ${printed.warnings.length} layout warnings` : ""}.`);
    await publishMicrosite(runId, printed.microsite, deps, now(), log);
    if (input.run.benchmarkOptIn) await contribute(runId, dir, input, deps, log);
    return { outcome: "done", detail: `${Object.keys(FILES).length} documents in ${folder.name}${target.id === folder.id ? "" : ` / ${target.name}`}, and the readout kit in ${INTERNAL_FOLDER}` };
  } catch (err) {
    const reason = safeReason(err);
    // Printing and uploading again is safe: the folders and files are reused.
    if (!attempt.final) throw new Error(reason);
    return fail(`The documents couldn't be made: ${reason}`);
  }
}
