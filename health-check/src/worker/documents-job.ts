import { existsSync, mkdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import type { ReportOutputKey } from "../narrative/sections.ts";
import { refusalReason, refusalSummary } from "../narrative/overrides.ts";
import type { BenchmarkInput } from "../benchmark/compare.ts";
import { AlreadyContributed, CONSENT_FILE, contributeBenchmark } from "../benchmark/export.ts";
import { longDate } from "../render/format.ts";
import { CHECKED_OUTSIDE_AUTOMATIC, SectionsOfRun } from "../schemas/report.ts";
import { AUTOMATIC_DOCUMENTS_NOT_BUILT, baselineRefusedSummary, describeHeld, renderReleased, repairWarnings, type DocumentKey, type HeldFigure, type ReleasedDocuments } from "../render/render-released.ts";
import { safeReason, type Attempt, type JobOutcome, type WorkerDeps } from "./deps.ts";
import type { DocumentDrive, DriveItem } from "./drive.ts";
import { withJobLock } from "./lock.ts";
import { syncShares } from "./share-job.ts";
import { sealPages } from "../microsite/seal.ts";
import { addMonths } from "./retention.ts";
import type { DocumentFile, DocumentsJobInput, PortalDocumentRow } from "./types.ts";
import { forbiddenInDraft, forbiddenInFrame, HELD_PLACEHOLDER, PORTAL_DOCUMENT_DAYS, PORTAL_OUTPUTS, PORTAL_PDF_MAX_BYTES, sealDocument, type PortalOutput } from "./portal-documents.ts";
import { BASELINE_GONE, BASELINE_UNAPPROVED, keepSurveyMetrics, loadPseudonymMap, OVERRIDES_FILE, SURVEY_FILE, write, writeRows } from "../render/render-run.ts";
import { HANDBOOK_DOCX, renderHandbookDocx } from "../handbook/docx.ts";
import { HANDBOOK_NOT_MADE, readEarlierHandbook, readHandbook } from "../handbook/read.ts";
import { printableSummaries, type StoredSummaries } from "../handbook/summary-row.ts";
import { Survey } from "../survey/questions.ts";
import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import type { NumberOverride } from "../schemas/index.ts";
import { inventoryOf } from "./inventory.ts";

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
 * listed: it's for Black Cloud only. The org handbook isn't a DocumentKey: it's
 * made and uploaded on its own (putHandbook) and listed after these, once the
 * console knew the kind (DECISIONS.md 443, 477).
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
async function makeHandbook(dir: string, out: string, clientName: string, runDate: string, stored: StoredSummaries | null, render: typeof renderHandbookDocx = renderHandbookDocx): Promise<{ ok: true; path: string; unnamed: boolean; noEarlier: boolean } | { ok: false; warning: string }> {
  const read = readHandbook(dir);
  if (!read.ok) return { ok: false, warning: HANDBOOK_NOT_MADE[read.why] };
  // Only what a person approved, checked again as it's printed (decision 495). The row's text, never the run folder's file.
  const summaries = printableSummaries(read.handbook, stored);
  // Who made each recent change is a token in the file, named from the run's
  // map as the reports are (DECISIONS.md 421). Without the map the handbook is
  // still made, naming no one: a token never reaches the customer.
  let pseudonyms: Pseudonymiser | null = null;
  try {
    pseudonyms = loadPseudonymMap(dir);
  } catch {
    pseudonyms = null;
  }
  try {
    // A re-check's handbook says what changed since its earlier one (DECISIONS.md
    // 483), worked out here from the two files and kept nowhere. The earlier
    // folder is only read: its run.json and handbook.json, never its map.
    const earlier = readEarlierHandbook(dir);
    const path = join(out, HANDBOOK_DOCX);
    mkdirSync(out, { recursive: true, mode: 0o700 });
    write(path, await render(read.handbook, { clientName, runDate, pseudonyms, earlier, summaries }));
    return { ok: true, path, unnamed: pseudonyms === null, noEarlier: earlier === null };
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
 * to the client's name now (DECISIONS.md 446). The console is told of it, last
 * in `files`, by what Drive returns here (477). Shared with the folder, as
 * everything in it is.
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
 * Whether the console can read an entry: a name, an id and a link it can open
 * (its DocumentFile, lib/health-check/schemas.ts in the console). Drive is
 * asked for the link but the client doesn't insist on it (drive.ts, item), and
 * one entry the console can't read makes it refuse the whole row, the six
 * reports' links with it.
 */
function listable(item: DriveItem): boolean {
  const url = typeof item.url === "string" && URL.canParse(item.url) ? new URL(item.url) : null;
  return typeof item.name === "string" && item.name.length > 0 && typeof item.id === "string" && item.id.length > 0 && (url?.protocol === "https:" || url?.protocol === "http:");
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

/**
 * What the Release tab says when a make is held for us (decision 682): the one
 * case a person is needed, since a report full of withheld details, or with a
 * headline figure withheld, shouldn't reach a customer unseen.
 */
export const HELD_FOR_US = "Held for us: too much in these documents couldn't be traced to the data to send them unseen. They were made and are in Black Cloud's own folder in Drive, not the customer's; nothing was shared, published or added to the connector, and the last documents the customer had stay as they were. That's ours to fix: once it's fixed, make the documents again.";

/** A held make's files, in Black Cloud's own folder under "Held for us", replaced on each make. Never in the client's folder. */
async function putHeld(drive: DocumentDrive, clientName: string, file: string, mime: string, path: string): Promise<DriveItem> {
  const internal = (await drive.folders(drive.rootFolderId)).find((f) => same(f.name, INTERNAL_FOLDER)) ?? (await drive.createFolder(drive.rootFolderId, INTERNAL_FOLDER));
  const name = `${clientName} - Held for us`;
  const own = (await drive.folders(internal.id)).find((f) => same(f.name, name)) ?? (await drive.createFolder(internal.id, name));
  const replaceId = (await drive.files(own.id)).find((f) => f.name === file)?.id;
  return drive.putFile(own.id, file, mime, readFileSync(path), replaceId);
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
      // The rows the documents were made with, so the set reads what the customer does (DECISIONS.md 521).
      { runDir: dir, industry: input.intake?.industry, headcount: input.intake?.headcountBand, consent: `Opted in at intake by ${input.run.createdBy}; added when the run was released`, now: (deps.now ?? (() => new Date()))(), overrides: input.overrides },
      (rows) => deps.store.addBenchmarkRows(rows),
    );
    log(`Run ${runId} added ${rows.length} figures to the benchmark set.`);
  } catch (err) {
    if (err instanceof AlreadyContributed) return;
    log(`Run ${runId} didn't add to the benchmark set: ${safeReason(err)}`);
  }
}

/**
 * The rows a make printed with, kept in the run folder for a later re-check's
 * draft to read its baseline as the customer's documents showed it (DECISIONS.md
 * 522, 528). Nothing lists a run folder to send it, and it goes with the folder.
 */
function keepOverrides(dir: string, rows: NumberOverride[]): void {
  writeRows(join(dir, OVERRIDES_FILE), rows);
}

export async function processDocumentsJob(runId: string, deps: WorkerDeps, attempt: Attempt, deliver?: "portal"): Promise<JobOutcome> {
  if (!RUN_ID.test(runId)) return { outcome: "failed", detail: "The run id isn't one the worker can use as a folder name" };
  // Only with the portal's documents switched on: a worker without them never names the table (DECISIONS.md 698).
  if (deps.portalDocuments?.on === true) await sweepPortalDocuments(deps);
  // The run's own lock: a run job for it can't be writing its folder meanwhile, nor a second make of its portal documents.
  return withJobLock(deps.locksDir, `run-${runId}`, () => (deliver === "portal" ? portalLocked(runId, deps, attempt) : documentsLocked(runId, deps, attempt)));
}

/**
 * The portal's expired documents, deleted at the start of every documents job
 * while the switch is on (DECISIONS.md 698), under short lock and statement
 * time limits. Never a reason to fail or hold up one, a consultant's make
 * included: a console without the table says so once a process, and any other
 * error (a missing right, a timeout) is logged and the make goes on.
 */
let saidNoPortalTable = false;
async function sweepPortalDocuments(deps: WorkerDeps): Promise<void> {
  const log = deps.log ?? (() => {});
  try {
    const deleted = await deps.store.deleteExpiredPortalDocuments((deps.now ?? (() => new Date()))().toISOString());
    if (deleted === null && !saidNoPortalTable) {
      saidNoPortalTable = true;
      log("The console's database has no HealthCheckPortalDocument table yet, so there are no portal documents to clear. Apply the console's migration 20261212_health_check_portal_documents.sql.");
    }
    if (deleted) log(`Deleted ${deleted} expired portal ${deleted === 1 ? "document" : "documents"}.`);
  } catch (err) {
    log(`Expired portal documents couldn't be cleared: ${safeReason(err)}`);
  }
}

/** Why a portal make was refused before anything was read or printed (DECISIONS.md 698). Plain, fixed words: the console may show them. */
export const PORTAL_REFUSALS = {
  off: "Documents on demand are switched off on the worker (HC_DOCUMENTS_ON_DEMAND isn't on), so nothing was made.",
  noKey: "the portal's documents can't be sealed",
  consultant: "Only an automatic run's documents are made for the portal. A consultant-led run's documents go to Drive once a person has released it.",
  notReleased: "This run isn't released, so it has no documents for the portal yet.",
  held: "This run is held for a person, so it has no documents for the portal.",
  noFolder: "The run's folder isn't on the worker any more, so its documents can't be made.",
} as const;

/** Each portal output, by the name the renderer gives its pages. */
const PORTAL_PAGES: Record<PortalOutput, "executive" | "full" | "ceo"> = { executive_summary: "executive", full_health_check: "full", ceo_one_pager: "ceo" };

/**
 * A released automatic run's three PDFs for the portal (DECISIONS.md 698).
 * Printed by renderReleased exactly as for a consultant's make, so every page's
 * number check and decision 682's repair run as they do there. Nothing goes to
 * Drive, nobody is shared anything, no microsite, readout kit or case study is
 * published and the benchmark is neither read nor added to: the only thing
 * written anywhere is the portal's rows, in one transaction. A document is held
 * for us (kept, unservable, with a flag for a person) when the make is, when a
 * page holds a word claiming a person, a review or a draft, or when it's too
 * big to keep; the others are sealed for the customer.
 */
async function portalLocked(runId: string, deps: WorkerDeps, attempt: Attempt): Promise<JobOutcome> {
  const log = deps.log ?? (() => {});
  const now = () => (deps.now ?? (() => new Date()))().toISOString();
  const refuse = (reason: string): JobOutcome => {
    log(`Run ${runId}'s portal documents weren't made: ${reason}`);
    return { outcome: "refused", detail: reason };
  };
  // The switch and the key before anything is read.
  const portal = deps.portalDocuments;
  if (portal?.on !== true) return refuse(PORTAL_REFUSALS.off);
  if (!portal.key) return refuse(`${portal.keyProblem ?? "HC_DOCUMENTS_KEY isn't set on the worker"}, so ${PORTAL_REFUSALS.noKey} and nothing was made.`);
  const key = portal.key;
  const input = await deps.store.loadDocumentsJob(runId);
  if (!input) return { outcome: "skipped", detail: `No run ${runId}` };
  if (input.run.product === "pulse") return { outcome: "skipped", detail: `Run ${runId} is a pulse, which never has documents` };
  if (input.run.reviewMode !== "automatic") return refuse(PORTAL_REFUSALS.consultant);
  if (input.run.status !== "released") return refuse(PORTAL_REFUSALS.notReleased);
  // A person's release of a held run clears its hold; one still set, whatever the status, is never served.
  if (input.heldReason !== null && input.heldReason !== undefined) return refuse(PORTAL_REFUSALS.held);
  const fit = SectionsOfRun.safeParse({ reviewMode: input.run.reviewMode, sections: input.sections });
  if (!fit.success) return refuse(fit.error.issues[0]?.message ?? CHECKED_OUTSIDE_AUTOMATIC);
  const dir = join(deps.runsDir, runId);
  if (!existsSync(join(dir, "run.json"))) return refuse(PORTAL_REFUSALS.noFolder);

  const madeAt = now();
  const expiresAt = new Date(Date.parse(madeAt) + PORTAL_DOCUMENT_DAYS * 86_400_000).toISOString();
  const held = (output: PortalOutput): PortalDocumentRow => ({ runId, output, sealed: HELD_PLACEHOLDER, madeAt, expiresAt, heldForUs: true });
  /** The rows and any flag, all or none. A refusal is tried again while the queue will; on the last delivery it's the outcome. */
  const save = async (rows: PortalDocumentRow[], heldCounts: Record<string, number> | null): Promise<string | null> => {
    try {
      await deps.store.savePortalDocuments(runId, rows, heldCounts);
      return null;
    } catch (err) {
      const reason = safeReason(err);
      if (!attempt.final) throw new Error(reason);
      log(`Run ${runId}'s portal documents couldn't be saved: ${reason}`);
      return reason;
    }
  };
  /**
   * A make that failed, as opposed to one held for what it printed: nothing is
   * written and no flag raised, so good documents still in date stay as they
   * are, and the job's own failure is the record (DECISIONS.md 698).
   */
  const failMake = (why: string): JobOutcome => {
    log(`Run ${runId}'s portal documents couldn't be made, and the ones already there are left as they are: ${why}`);
    return { outcome: "failed", detail: `Run ${runId}'s portal documents couldn't be made: ${why}` };
  };
  /** Every document held for us, for a make whose content can't be served. */
  const holdAll = async (counts: Record<string, number>, why: string): Promise<JobOutcome> => {
    const failed = await save(PORTAL_OUTPUTS.map(held), { documents: PORTAL_OUTPUTS.length, held: PORTAL_OUTPUTS.length, ...counts });
    if (failed) return { outcome: "failed", detail: `Run ${runId}'s portal documents couldn't be saved: ${failed}` };
    log(`Run ${runId}'s portal documents are held for us: ${why}`);
    return { outcome: "done", detail: `Run ${runId}'s portal documents are held for us: ${why}` };
  };

  let printed: Awaited<ReturnType<typeof renderReleased>>;
  const pages: Partial<Record<"executive" | "full" | "ceo", string>> = {};
  try {
    printed = await (deps.renderReleased ?? renderReleased)(
      dir,
      // No consultant to name, no benchmark read, no digest: the one written with the release stays (DECISIONS.md 693).
      { intake: input.intake, sections: input.sections, versions: input.versions, overrides: input.overrides, preparedBy: null, benchmark: null, portal: { onPrinted: (doc, html) => void (pages[doc] = html) } },
      join(dir, "portal-documents"),
    );
  } catch (err) {
    const reason = safeReason(err);
    if (!attempt.final) throw new Error(reason);
    return failMake(reason.slice(0, 200));
  }
  // A correction we can't carry: nothing was printed.
  if ("refused" in printed) return failMake(refusalSummary(printed.refused));
  // A page we didn't see printed is a page we didn't read: never sealed unread, and a defect of ours, not the content's.
  const unread = PORTAL_OUTPUTS.filter((o) => pages[PORTAL_PAGES[o]] === undefined);
  if (unread.length) return failMake(`the words on ${unread.map((o) => o.replace(/_/g, " ")).join(", ")} weren't seen as printed, so nothing was sealed unread.`);
  const masks = Object.values(printed.masks ?? {}).reduce((a, b) => a + b, 0);
  const fellBack = printed.fellBack?.length ?? 0;
  // A figure no mask could clear fails a consultant's make (decision 682); here, held for us, so a person is told.
  if (printed.held.length > 0) return holdAll({ untraced: printed.held.length, masks, fellBack }, heldBackSummary(printed.held));
  if (printed.repairs === "held for us") {
    await alertRepairs(runId, printed, deps, log);
    return holdAll({ masks, fellBack }, "too much was withheld, or a number was withheld where a page prints it (decision 682).");
  }
  if (printed.repairs === "some" || printed.repairs === "heavy") await alertRepairs(runId, printed, deps, log);

  // Claude's own text, as it prints: each section it drafted that didn't fall back, read for the words an automatic run may not say.
  // The engine's text and the org's words are never searched (DECISIONS.md 698).
  const fell = new Set((printed.fellBack ?? []).map((f) => `${f.output}:${f.key}`));
  const versionOf = new Map(input.versions.map((v) => [v.id, v]));
  const drafted: Partial<Record<"executive" | "full" | "ceo", string[]>> = {};
  for (const section of input.sections) {
    const page = section.output === "executive_summary" ? "executive" : section.output === "full_health_check" ? "full" : null;
    const version = versionOf.get(section.currentVersionId ?? "");
    if (!page || !version || version.source === "engine" || fell.has(`${section.output}:${section.key}`)) continue;
    drafted[page] = [...(drafted[page] ?? []), ...forbiddenInDraft(version.body)];
  }
  const rows: PortalDocumentRow[] = [];
  let words = 0;
  let tooLarge = 0;
  for (const output of PORTAL_OUTPUTS) {
    // Claude's words on this document, then the page's fixed frame.
    const found = [...new Set([...(drafted[PORTAL_PAGES[output]] ?? []), ...forbiddenInFrame(pages[PORTAL_PAGES[output]]!)])];
    if (found.length) {
      words++;
      // The labels only: never the words around them, which may be a draft's.
      log(`Run ${runId}'s ${output.replace(/_/g, " ")} is held for us: a page holds ${found.join(", ")}.`);
      rows.push(held(output));
      continue;
    }
    const pdf = readFileSync(printed.files[output]);
    if (pdf.length > PORTAL_PDF_MAX_BYTES) {
      tooLarge++;
      log(`Run ${runId}'s ${output.replace(/_/g, " ")} is held for us: it weighs ${pdf.length} bytes, over the ${PORTAL_PDF_MAX_BYTES} we keep.`);
      rows.push(held(output));
      continue;
    }
    rows.push({ runId, output, sealed: sealDocument(pdf, runId, output, key), madeAt, expiresAt, heldForUs: false });
  }
  const heldCount = rows.filter((r) => r.heldForUs).length;
  const failed = await save(rows, heldCount ? { documents: rows.length, held: heldCount, words, tooLarge, masks, fellBack } : null);
  if (failed) return { outcome: "failed", detail: `Run ${runId}'s portal documents couldn't be saved: ${failed}` };
  const detail = heldCount ? `Run ${runId}'s portal documents: ${rows.length - heldCount} sealed, ${heldCount} held for us` : `Run ${runId}'s ${rows.length} portal documents are sealed, kept until ${expiresAt}`;
  log(detail);
  return { outcome: "done", detail };
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
 * The line the consultants are sent when a make repaired something (decision
 * 682, item 7): the run, how heavy, and counts. No title, figure or word of the
 * customer's. Never a reason to fail the make.
 */
export function repairAlert(runId: string, printed: Pick<ReleasedDocuments, "repairs" | "fellBack" | "masks" | "micrositeHeld">): string {
  const fellBack = printed.fellBack ?? [];
  const edits = fellBack.filter((f) => f.source === "consultant_edit").length;
  const drafts = fellBack.length - edits;
  const masks = Object.values(printed.masks ?? {}).reduce((a, b) => a + b, 0);
  const counts = [`${drafts} drafted ${drafts === 1 ? "section" : "sections"} and ${edits} edited ${edits === 1 ? "section" : "sections"} in our own wording`, `${masks} ${masks === 1 ? "detail" : "details"} withheld in the reports`, ...(printed.micrositeHeld ? ["no microsite"] : [])];
  if (printed.repairs === "held for us") return `Run ${runId}'s documents are held for us and went to no customer: ${counts.join(", ")}. Ours to fix, then make them again. See the run's Release tab.`;
  return `Run ${runId}'s documents were made with ${printed.repairs === "heavy" ? "heavy" : "some"} repairs: ${counts.join(", ")}. See the run's Release tab.`;
}

async function alertRepairs(runId: string, printed: ReleasedDocuments, deps: WorkerDeps, log: (line: string) => void): Promise<void> {
  const line = repairAlert(runId, printed);
  log(line);
  try {
    await deps.alert?.(line);
  } catch (err) {
    log(`Run ${runId}: the consultants couldn't be told of the repair: ${safeReason(err)}`);
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
  // Never "those sections" without naming one (decision 682, item 6).
  const sections = [...new Set(held.flatMap((h) => (h.section ? [`"${h.section}"`] : [])))];
  const todo = [
    ...(sections.length ? [`Edit the wording in ${sections.join(", ")}, approve ${sections.length === 1 ? "it" : "them"} again, and make the documents again.`] : []),
    ...(held.some((h) => !h.section) ? [`${sections.length ? "The rest are" : "These are"} in our own text, not in a section's wording, so they're ours to fix: tell Black Cloud.`] : []),
  ];
  return [heldBackCount(held), ...listed, ...more, ...todo].map((line) => line.replace(/\s+/g, " ")).join("\n");
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
  // The switch gates printing as well as asking, so unsetting it is a complete off (decision 495).
  const summariesOn = deps.apexSummaries === true;
  const input = await deps.store.loadDocumentsJob(runId, { handbookSummaries: summariesOn });
  if (!input) return { outcome: "skipped", detail: `No run ${runId}` };
  const { handbookSummaries, ...job } = input;
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
  // Never released today, so never here; but a row that says otherwise gets a plain refusal, not documents (DECISIONS.md 688).
  if (input.run.reviewMode !== "consultant") return fail(AUTOMATIC_DOCUMENTS_NOT_BUILT);
  const fit = SectionsOfRun.safeParse({ reviewMode: input.run.reviewMode, sections: input.sections });
  if (!fit.success) return fail(fit.error.issues[0]?.message ?? CHECKED_OUTSIDE_AUTOMATIC);
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
    // The summaries are the handbook's alone: the reports are never given them.
    // The inventory as the run's results gave it to the console, so the digest withholds the names that row shows (DECISIONS.md 632). Silent: the run's save logged its faults.
    const inventory = inventoryOf(dir, () => {}, { runId, madeAt: input.run.asOf, scope: input.run.scope });
    const printed = await (deps.renderReleased ?? renderReleased)(dir, { ...job, benchmark, digest: { inventory, madeAt: now() } }, join(dir, "documents"));
    // A correction we can't carry stops the make before a file is written, and so before Drive, sharing, the
    // microsite and the benchmark. Not thrown, as for a held figure: the same overrides would fail the same way.
    if ("refused" in printed) {
      const summary = refusalSummary(printed.refused);
      log(`Run ${runId}'s documents weren't made. ${summary}`);
      return fail(refusalReason(printed.refused), summary);
    }
    if (printed.baselineRefused) log(`Run ${runId}: ${baselineRefusedSummary(printed.baselineRefused)}`);
    // Built with no comparison, where its approved sections hold one (DECISIONS.md 534, 535). Not held: once the earlier folder is gone, nothing could clear it.
    if (printed.baselineGone) log(`Run ${runId}: ${BASELINE_GONE}`);
    // Built with no comparison because no approved section holds one, whatever the folder gives (DECISIONS.md 535).
    if (printed.baselineUnapproved) log(`Run ${runId}: ${BASELINE_UNAPPROVED}`);
    // A figure we can't trace stops everything before Drive: nothing uploaded, shared anew, published or contributed.
    // Not thrown, so the queue doesn't retry: the same wording would fail the same way.
    if (printed.held.length > 0) {
      const summary = heldBackSummary(printed.held);
      log(`Run ${runId}'s documents weren't uploaded. ${summary}`);
      return fail(heldBackReason(printed.held), summary);
    }
    // Too much withheld, or a number withheld where a page prints it: made for us, kept from the customer (decision 682).
    if (printed.repairs === "held for us") {
      for (const output of Object.keys(FILES) as Array<keyof typeof FILES>) {
        await putHeld(deps.drive, clientName, `${clientName} - ${FILES[output].title}.${FILES[output].ext}`, FILES[output].mime, printed.files[output]);
      }
      await alertRepairs(runId, printed, deps, log);
      return fail(HELD_FOR_US, `Run ${runId}'s documents are held for us: made, in ${INTERNAL_FOLDER} only.`);
    }
    // Repaired, not held (decision 682): a defect in our own text worked around is logged by run id, with rule and metric ids only.
    const leftOut = printed.leftOut ?? [];
    if (leftOut.length) {
      const where = [...new Set(leftOut.map((l) => `${l.ruleId ?? l.metricId ?? l.stepId ?? "fixed text"} (${l.field})`))];
      log(`Run ${runId}: worked around a defect: ${leftOut.length} untraceable ${leftOut.length === 1 ? "figure" : "figures"} masked, from ${where.join(", ")}.`);
    }
    if (printed.fellBack?.length) log(`Run ${runId}: ${printed.fellBack.length} ${printed.fellBack.length === 1 ? "section" : "sections"} printed in our own wording.`);
    if (printed.stillIn?.length) log(`Run ${runId}: a masked figure still shows in ${printed.stillIn.join(", ")}.`);
    if (printed.micrositeHeld) log(`Run ${runId}: the microsite wasn't made, because a page still held a figure after the repair.`);
    // Some or heavy: one line to the consultants, counts only, never the customer's text (decision 682, item 7).
    if (printed.repairs === "some" || printed.repairs === "heavy") await alertRepairs(runId, printed, deps, log);
    // Neither refused nor held: these rows are the ones the documents print. None, and nothing is written or removed.
    if (input.overrides.length > 0) keepOverrides(dir, input.overrides);
    const handbook = await makeHandbook(dir, join(dir, "documents"), clientName, input.run.asOf, summariesOn ? (handbookSummaries ?? null) : null, deps.renderHandbook);
    if (!handbook.ok) log(`Run ${runId}: ${handbook.warning}`);
    else if (handbook.unnamed) log(`Run ${runId}: the org handbook names no one who made a recent change, because the run's pseudonym map couldn't be read.`);
    // Fixed words: nothing from either folder, and not which check it failed, since one is that it's another org's.
    if (handbook.ok && handbook.noEarlier) log(`Run ${runId}: the org handbook says there was no earlier handbook to compare with.`);
    const folder = await folderFor(deps.drive, clientName, input.driveFolderId);
    const nameOf = (output: keyof typeof FILES): string => `${clientName} - ${FILES[output].title}.${FILES[output].ext}`;
    // Also by name, for a make from before the console was told of these three.
    // Never the handbook: it stays loose in the run's folder, replaced in place,
    // so it neither moves into a version nor decides whether one is made.
    const reports = input.files.filter((f) => f.output !== "org_handbook");
    const target = await versionFolderFor(deps.drive, folder, reports, now(), [nameOf("ceo_one_pager"), nameOf("action_lists"), nameOf("tickets")]);
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
    if (handbook.ok) {
      // Listed last, by what Drive returned, so a remake lists the same file and link.
      const item = await putHandbook(deps.drive, folder, clientName, handbook.path, runId, log);
      if (listable(item)) files.push({ output: "org_handbook", name: item.name, driveFileId: item.id, url: item.url });
      else log(`Run ${runId}: the org handbook is in Drive but isn't listed to the console, because Drive didn't return a name, id and link it can use.`);
    }
    await putReadoutKit(deps.drive, clientName, folder.name, printed.files.readout_kit);
    // Only a re-check whose customer agreed to be a case study has one, and it's a draft for us to finish with them.
    if (printed.caseStudy) await putInternal(deps.drive, clientName, folder.name, `${clientName} - Case study draft.docx`, printed.caseStudy);
    // The digest goes with the documents' row, once they're in Drive: a make held or refused above writes none (DECISIONS.md 632).
    if (printed.digest && !printed.digest.ok) log(`Run ${runId}'s digest couldn't be made (${printed.digest.error}), so the connector has none for it.`);
    // None to write takes away an earlier make's, whose figures these documents may have corrected.
    const digest = printed.digest?.ok ? printed.digest.digest : null;
    // The survey's metrics as these documents print them, with their row: a run drafted before the run's results held them gets them now (DECISIONS.md 645).
    const saved = await deps.store.saveDocuments(runId, { at: now(), clientName, folderName: folder.name, driveFolderId: folder.id, driveFolderUrl: folder.url, files, warnings: [...repairWarnings(printed.fellBack ?? [], leftOut, printed.micrositeHeld), ...printed.warnings, ...(handbook.ok ? [] : [handbook.warning])] }, digest, printed.surveyMetrics);
    // The folder follows the rows, and only once they're written: a row the console moved on meanwhile changed neither.
    if (saved && printed.surveyMetrics) keepSurveyMetrics(dir, printed.surveyMetrics);
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
