import { existsSync, mkdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import type { ReportOutputKey } from "../narrative/sections.ts";
import type { BenchmarkInput } from "../benchmark/compare.ts";
import { AlreadyContributed, CONSENT_FILE, contributeBenchmark } from "../benchmark/export.ts";
import { longDate } from "../render/format.ts";
import { describeHeld, renderReleased, type DocumentKey, type HeldFigure, type ReleasedDocuments } from "../render/render-released.ts";
import { safeReason, type Attempt, type JobOutcome, type WorkerDeps } from "./deps.ts";
import type { DocumentDrive, DriveItem } from "./drive.ts";
import { withJobLock } from "./lock.ts";
import { syncShares } from "./share-job.ts";
import { sealPages } from "../microsite/seal.ts";
import { addMonths } from "./retention.ts";
import type { DocumentFile, DocumentsJobInput } from "./types.ts";
import { SURVEY_FILE, write } from "../render/render-run.ts";
import { HANDBOOK_DOCX, renderHandbookDocx } from "../handbook/docx.ts";
import { HANDBOOK_NOT_MADE, readHandbook } from "../handbook/read.ts";
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
 * listed: it's for Black Cloud only. Nor is the org handbook, which isn't a
 * DocumentKey at all: it's made and uploaded on its own (putHandbook) until a
 * console that knows the kind is live (DECISIONS.md 443).
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

/**
 * The org handbook (DECISIONS.md 443 to 445), made from the run folder's
 * handbook.json into the documents folder. It's the customer's own
 * configuration, so nothing from it goes anywhere but this file and their
 * Drive folder: what comes back is a path or a sentence fixed in read.ts.
 * Without one, the other documents are made as before and the warnings say
 * why; nothing here fails the make.
 */
async function makeHandbook(dir: string, out: string, clientName: string, runDate: string): Promise<{ ok: true; path: string } | { ok: false; warning: string }> {
  const read = readHandbook(dir);
  if (!read.ok) return { ok: false, warning: HANDBOOK_NOT_MADE[read.why] };
  try {
    const path = join(out, HANDBOOK_DOCX);
    mkdirSync(out, { recursive: true, mode: 0o700 });
    write(path, await renderHandbookDocx(read.handbook, { clientName, runDate }));
    return { ok: true, path };
  } catch {
    // The error could quote the handbook, so it isn't kept.
    return { ok: false, warning: HANDBOOK_NOT_MADE.render };
  }
}

const HANDBOOK_SUFFIX = " - Org handbook.docx";

/**
 * The handbook, in the run's own folder in the customer's Drive, beside the
 * version folders, since it's the same for every make. It replaces the one
 * there whose name ends as a handbook's, whatever the client was called when it
 * was made, so a remake leaves one, with the same id and link; Drive renames it
 * to the client's name now (DECISIONS.md 446). It's never in `files`: the
 * console doesn't know the kind yet and would refuse the whole row (443).
 * Shared with the folder, as everything in it is.
 */
async function putHandbook(drive: DocumentDrive, folder: DriveItem, clientName: string, path: string, runId: string, log: (line: string) => void): Promise<DriveItem> {
  const handbooks = (await drive.files(folder.id)).filter((f) => f.name.trim().toLowerCase().endsWith(HANDBOOK_SUFFIX.toLowerCase()));
  // A folder can hold more than one: made before a rename was followed, or a
  // copy someone made in Drive ("Copy of … - Org handbook.docx"). The oldest is
  // ours, the first one made and so the one whose link was shared; a newer one
  // may be a copy someone keeps, so it's left for a person. Only the count is
  // logged: the names are the customer's.
  if (handbooks.length > 1) log(`Run ${runId}'s Drive folder held ${handbooks.length} org handbooks, so the first one made was replaced and the other ${handbooks.length - 1} left as they were.`);
  const ours = [...handbooks].sort(oldestFirst)[0];
  return drive.putFile(folder.id, `${clientName}${HANDBOOK_SUFFIX}`, DOCX, readFileSync(path), ours?.id);
}

/**
 * By Drive's created time, oldest first. One Drive gave no time for goes after
 * every one it did, since it can't be shown to be the first. Equal times, or
 * none, fall back to the id: Drive's ids carry no order, but the same file
 * wins on every make, whatever order Drive lists them in.
 */
function oldestFirst(a: DriveItem, b: DriveItem): number {
  if (a.createdAt !== b.createdAt) {
    if (a.createdAt === undefined) return 1;
    if (b.createdAt === undefined) return -1;
    return a.createdAt < b.createdAt ? -1 : 1;
  }
  return a.id < b.id ? -1 : a.id > b.id ? 1 : 0;
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

/** How many held figures the error lists by name. The rest are counted. */
const HELD_LISTED = 10;
/** A reason can carry the checker's own error message, so each is cut short. */
const HELD_REASON_MAX = 200;

/** "Held back: 2 figures on 2 pages can't be traced to the data." */
function heldBackCount(held: HeldFigure[]): string {
  const pages = new Set(held.map((h) => `${h.document}, page ${h.page}`)).size;
  return `Held back: ${held.length} ${held.length === 1 ? "figure" : "figures"} on ${pages} ${pages === 1 ? "page" : "pages"} can't be traced to the data.`;
}

/**
 * Why the documents were held back, for the console's Release tab only: each
 * document, page, figure and reason, so a person knows which section to edit
 * (DECISIONS.md 430). Values that aren't plain figures are already masked
 * (heldFigures). One line per figure (DECISIONS.md 434): the count, each figure,
 * then what to do. Whitespace is collapsed within each line, so a newline in a
 * reason can't start a line of its own.
 */
export function heldBackReason(held: HeldFigure[]): string {
  const sentence = (text: string): string => (/[.!?]$/.test(text) ? text : `${text}.`);
  const listed = held.slice(0, HELD_LISTED).map((h) => sentence(describeHeld({ ...h, reason: safeReason(new Error(h.reason)).slice(0, HELD_REASON_MAX) })));
  const more = held.length > HELD_LISTED ? [`And ${held.length - HELD_LISTED} more.`] : [];
  return [heldBackCount(held), ...listed, ...more, "Edit the wording in those sections, approve them again, and make the documents again."].map((line) => line.replace(/\s+/g, " ")).join("\n");
}

/**
 * The same, for the log and the job's outcome, which the queue logs too: counts
 * only, since even a figure is the customer's.
 */
export function heldBackSummary(held: HeldFigure[]): string {
  return `${heldBackCount(held)} See the run's Release tab.`;
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

  // Brings the folder's sharing in line with the readers. A share job that found the run held by this one may have given up, so this runs however the make ends.
  const share = async (options: { grant?: boolean } = {}): Promise<void> => {
    try {
      await syncShares(runId, { store: deps.store, sharing: deps.sharing ?? null, now: () => new Date(now()), log }, options);
    } catch (err) {
      log(`The sharing for ${runId} couldn't be brought up to date: ${safeReason(err)}`);
    }
  };
  // A failed make only takes access away: the folder still holds the last make, which a new reader was never given.
  // `detail` is what the log and the queue are told, when it mustn't be the whole reason the console shows.
  const fail = async (reason: string, detail = reason): Promise<JobOutcome> => {
    await deps.store.failDocuments(runId, reason, now());
    await share({ grant: false });
    return { outcome: "failed", detail };
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
    // A figure we can't trace stops everything before Drive: nothing uploaded, shared anew, published or contributed.
    // Not thrown, so the queue doesn't retry: the same wording would fail the same way.
    if (printed.held.length > 0) {
      const summary = heldBackSummary(printed.held);
      log(`Run ${runId}'s documents weren't uploaded. ${summary}`);
      return fail(heldBackReason(printed.held), summary);
    }
    const handbook = await makeHandbook(dir, join(dir, "documents"), clientName, input.run.asOf);
    if (!handbook.ok) log(`Run ${runId}: ${handbook.warning}`);
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
    if (handbook.ok) await putHandbook(deps.drive, folder, clientName, handbook.path, runId, log);
    await putReadoutKit(deps.drive, clientName, folder.name, printed.files.readout_kit);
    // Only a re-check whose customer agreed to be a case study has one, and it's a draft for us to finish with them.
    if (printed.caseStudy) await putInternal(deps.drive, clientName, folder.name, `${clientName} - Case study draft.docx`, printed.caseStudy);
    await deps.store.saveDocuments(runId, { at: now(), clientName, folderName: folder.name, driveFolderId: folder.id, driveFolderUrl: folder.url, files, warnings: [...printed.warnings, ...(handbook.ok ? [] : [handbook.warning])] });
    log(`Documents for ${runId} are in Drive${printed.warnings.length ? `, with ${printed.warnings.length} layout warnings` : ""}.`);
    // The folder is now the run's: share it with its readers (DECISIONS.md 253). The documents are made either way.
    await share();
    await publishMicrosite(runId, printed.microsite, deps, now(), log);
    if (input.run.benchmarkOptIn) await contribute(runId, dir, input, deps, log);
    return { outcome: "done", detail: `${Object.keys(FILES).length} documents in ${folder.name}${target.id === folder.id ? "" : ` / ${target.name}`}${handbook.ok ? `, the org handbook in ${folder.name}` : ""}, and the readout kit in ${INTERNAL_FOLDER}` };
  } catch (err) {
    const reason = safeReason(err);
    // Printing and uploading again is safe: the folders and files are reused.
    if (!attempt.final) throw new Error(reason);
    return fail(`The documents couldn't be made: ${reason}`);
  }
}
