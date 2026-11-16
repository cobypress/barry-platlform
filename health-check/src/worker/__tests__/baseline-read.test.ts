import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs, { chmodSync, existsSync, mkdirSync, readdirSync, readFileSync, renameSync, rmSync, statSync, writeFileSync } from "node:fs";
import { syncBuiltinESMExports } from "node:module";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { unzipSync } from "fflate";
import { writeActionFiles } from "../../actions/index.ts";
import { HANDBOOK_FILE } from "../../handbook/build.ts";
import { HANDBOOK_DOCX, SINCE_TITLE } from "../../handbook/docx.ts";
import { Handbook } from "../../handbook/schema.ts";
import { buildMicrositePages } from "../../microsite/pages.ts";
import type { Intake } from "../../narrative/intake.ts";
import { buildReport } from "../../narrative/sections.ts";
import { approvedReport, BASELINE_ROWS_UNREAD, baselineRefusedSummary, renderReleased, type ApprovedRun } from "../../render/render-released.ts";
import { BASELINE_GONE, BASELINE_OVERRIDES_FILE, BASELINE_READ_FILE, BASELINE_UNAPPROVED, loadRun, OUTPUT_FILES, printDocuments } from "../../render/render-run.ts";
import { renderCaseStudyDocx } from "../../render/case-study.ts";
import { fullReportHtml } from "../../render/full-report.ts";
import { executiveHtml } from "../../render/executive.ts";
import { onePagerHtml } from "../../render/one-pager.ts";
import type { NumberOverride, ReportSection, SectionVersion } from "../../schemas/index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { docxText } from "../../testing/docx-text.ts";
import { plantEarlier as earlierHandbook, REMOVED_FLOW } from "../../testing/earlier-handbook.ts";
import { plantEarlier } from "../../testing/recheck.ts";
import { loadAnswers, loadRecording, ReplayTransport } from "../../testing/replay.ts";
import { processDocumentsJob } from "../documents-job.ts";
import type { DocumentDrive, DriveItem } from "../drive.ts";
import { processRunJob } from "../run-job.ts";
import type { DocumentsJobInput } from "../types.ts";
import { baseDeps, consoleRun, INTAKE, RUN_ID, runInput, syntheticConnection } from "./helpers.ts";

// A re-check records at its draft whether it read its earlier health check, and
// every later build follows the record (DECISIONS.md 534). Each case goes
// through the real run job, which drafts, and the real documents job, which
// makes, with the synthetic org's recording and its earlier health check
// planted beside it.

const { dir: recorded } = await runFromRecording({ answers: loadAnswers() });

const EARLIER_ID = `${RUN_ID}-earlier`;
const GONE = `Run ${RUN_ID}: the earlier health check can't be read now, so the documents have no comparison with it.`;
/** Each document a customer or a consultant reads, by name. */
const DOCUMENTS = ["full health check", "executive summary", "one page summary", "readout kit", "microsite", "raw notes"] as const;
type Document = (typeof DOCUMENTS)[number];
const EVERY = Object.fromEntries(DOCUMENTS.map((d) => [d, true])) as Record<Document, boolean>;
const NONE = Object.fromEntries(DOCUMENTS.map((d) => [d, false])) as Record<Document, boolean>;
/** What the draft had and the documents follow: the comparison everywhere but the Word notes, which print what was approved. */
const ONLY_THE_NOTES = { ...NONE, "raw notes": true };

class MemoryDrive implements DocumentDrive {
  readonly rootFolderId = "root-folder-0001";
  readonly items = new Map<string, DriveItem & { parent: string; folder: boolean; body?: Buffer }>();
  #next = 1;
  #add(parent: string, name: string, folder: boolean): DriveItem {
    const id = `drive-item-${String(this.#next++).padStart(4, "0")}`;
    const item = { id, name, url: `https://drive.example.com/${id}`, parent, folder, createdAt: "2026-11-04T18:00:00.000Z" };
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
    const item = replaceId ? this.items.get(replaceId)! : this.items.get(this.#add(parentId, name, false).id)!;
    Object.assign(item, { name, body });
    return item;
  }
  async moveFile(id: string, _from: string, to: string) {
    this.items.get(id)!.parent = to;
  }
}

/** The re-check drafted by the run job from the synthetic org's recording, its earlier health check planted beside it. */
async function worker(options: { baselineRunId?: string | null } = {}) {
  const drive = new MemoryDrive();
  const deps = baseDeps({ drive, dataTransport: new ReplayTransport(loadRecording()) });
  await deps.tokens.save(syntheticConnection());
  const earlier = join(deps.runsDir, EARLIER_ID);
  const { rows } = plantEarlier(recorded, earlier);
  const dir = join(deps.runsDir, RUN_ID);
  deps.store.addRun(runInput({ run: consoleRun({ baselineRunId: options.baselineRunId === undefined ? EARLIER_ID : options.baselineRunId }) }));
  const printed: Awaited<ReturnType<typeof renderReleased>>[] = [];
  /** What the documents job handed each make, the benchmark it read included. */
  const handed: ApprovedRun[] = [];
  deps.renderReleased = async (...args) => {
    handed.push(args[1]);
    const result = await renderReleased(...args);
    printed.push(result);
    return result;
  };
  /** The run job's delivery: a run not yet in review, so it drafts. */
  const draft = async () => {
    deps.dataTransport = new ReplayTransport(loadRecording());
    deps.store.runs.get(RUN_ID)!.run.status = "extracting";
    const outcome = await processRunJob(RUN_ID, deps, { final: true });
    assert.equal(outcome.outcome, "done", outcome.detail);
  };
  /**
   * A delivery started while the run awaited manual steps, whose save the
   * console refused because it had moved the run to review meanwhile: skipped,
   * with its files written and the console's sections those of the delivery
   * before (DECISIONS.md 535).
   */
  const refused = async () => {
    deps.dataTransport = new ReplayTransport(loadRecording());
    const stored = deps.store.runs.get(RUN_ID)!;
    stored.run.status = "awaiting_manual";
    const save = deps.store.saveRunOutput;
    deps.store.saveRunOutput = async (id, output) => {
      stored.run.status = "in_review";
      return save.call(deps.store, id, output);
    };
    try {
      const outcome = await processRunJob(RUN_ID, deps, { final: true });
      assert.equal(outcome.outcome, "skipped", outcome.detail);
    } finally {
      deps.store.saveRunOutput = save;
    }
  };
  /**
   * The re-check released with the drafts the console holds, approved as they
   * are, and its documents made. `plant` changes the approved set first.
   */
  const make = async (options: { intake?: Intake; plant?: (sections: ReportSection[], versions: SectionVersion[]) => void } = {}) => {
    const output = deps.store.runs.get(RUN_ID)!.output!;
    const sections: ReportSection[] = output.sections.map((s) => ({ ...s, status: "approved" as const, approval: { approvedBy: "consultant@example.com", approvedAt: "2026-11-04T18:00:00.000Z" } }));
    const versions: SectionVersion[] = [...output.versions];
    options.plant?.(sections, versions);
    const intake = options.intake ?? { ...INTAKE, customerName: "Synthetic Co" };
    deps.store.addDocumentsJob(jobInput({ sections, versions, intake }));
    const from = deps.lines.length;
    const outcome = await processDocumentsJob(RUN_ID, deps, { final: true });
    assert.equal(outcome.outcome, "done", outcome.detail);
    const result = printed.at(-1)!;
    assert.ok(!("refused" in result));
    assert.deepEqual(result.held, []);
    const approved: ApprovedRun = { intake, sections, versions, overrides: [], preparedBy: null };
    const { report, pseudonyms } = approvedReport(dir, approved);
    const files = fileParts(result.files, result.microsite?.ok ? result.microsite.pages : null, result.caseStudy);
    // What each document was printed from, and the files themselves where they're the same bytes each make.
    const texts = {
      "full health check": fullReportHtml(report, pseudonyms),
      "executive summary": executiveHtml(report, pseudonyms),
      "one page summary": onePagerHtml(report, pseudonyms),
      "readout kit": docxText(readFileSync(result.files.readout_kit)),
      microsite: files.microsite,
      "raw notes": docxText(readFileSync(result.files.raw_notes)),
      "full health check file": files["full health check"],
      "executive summary file": files["executive summary"],
      "one page summary file": files["one page summary"],
      "raw notes file": files["raw notes"],
      "readout kit file": files["readout kit"],
      "action lists": files["action lists"],
      tickets: files.tickets,
      "case study draft": files["case study draft"],
    };
    const progress: Record<Document, boolean> = {
      "full health check": texts["full health check"].includes("<h2>What has changed</h2>"),
      "executive summary": texts["executive summary"].includes("<h2>What has changed</h2>"),
      "one page summary": texts["one page summary"].includes("Since your last health check"),
      "readout kit": texts["readout kit"].includes("Since the last health check"),
      microsite: Boolean(result.microsite?.ok && result.microsite.pages.pages.some((p) => p.slug === "progress")),
      "raw notes": /^#+ R\d+ Since your last health check$/m.test(texts["raw notes"]),
    };
    const handbook = join(dir, "documents", HANDBOOK_DOCX);
    return { progress, texts, files, report, approved, caseStudy: result.caseStudy !== undefined, handbook: existsSync(handbook) ? docxText(readFileSync(handbook)) : "", lines: deps.lines.slice(from) };
  };
  const record = (): string | null => (existsSync(join(dir, BASELINE_READ_FILE)) ? readFileSync(join(dir, BASELINE_READ_FILE), "utf8") : null);
  const draftHasProgress = (): boolean => deps.store.runs.get(RUN_ID)!.output!.sections.some((s) => s.output === "raw_notes" && s.key === "progress");
  return { deps, drive, dir, earlier, rows, draft, refused, make, record, draftHasProgress, handed };
}

function jobInput(overrides: Partial<DocumentsJobInput> = {}): DocumentsJobInput {
  return {
    run: consoleRun({ status: "released", customerRef: "SYN-0001", baselineRunId: EARLIER_ID }),
    intake: { ...INTAKE, customerName: "Synthetic Co" },
    sections: [],
    versions: [],
    overrides: [],
    preparedBy: null,
    state: "queued",
    driveFolderId: null,
    files: [],
    ...overrides,
  };
}

/**
 * A Word file's every part, byte for byte, but for the moment it was written:
 * the zip's own times and the created and modified times in its properties,
 * which differ between two makes on the tip as here.
 */
const zipParts = (path: string): string =>
  JSON.stringify(Object.entries(unzipSync(new Uint8Array(readFileSync(path)))).map(([name, bytes]) => [name, name === "docProps/core.xml" ? Buffer.from(bytes).toString().replace(/(<dcterms:(?:created|modified)[^>]*>)[^<]*/g, "$1") : Buffer.from(bytes).toString("base64")]));

/**
 * A PDF's bytes but for the two dates Chromium stamps it with, which differ
 * between two makes on the tip as here (measured), as a hash: the printed
 * decks themselves, not only the HTML they're printed from.
 */
const pdfParts = (path: string): string =>
  createHash("sha256")
    .update(readFileSync(path).toString("latin1").replace(/\/(CreationDate|ModDate) \([^)]*\)/g, ""))
    .digest("hex");

/** Every file a make writes but the handbook, byte for byte as `texts` compares them, and the microsite's pages. */
function fileParts(files: Record<"raw_notes" | "executive_summary" | "full_health_check" | "ceo_one_pager" | "readout_kit" | "action_lists" | "tickets", string>, microsite: unknown, caseStudy: string | undefined) {
  return {
    "full health check": pdfParts(files.full_health_check),
    "executive summary": pdfParts(files.executive_summary),
    "one page summary": pdfParts(files.ceo_one_pager),
    "raw notes": zipParts(files.raw_notes),
    "readout kit": zipParts(files.readout_kit),
    "action lists": readFileSync(files.action_lists).toString("base64"),
    tickets: readFileSync(files.tickets, "utf8"),
    microsite: microsite ? JSON.stringify(microsite) : "",
    "case study draft": caseStudy ? zipParts(caseStudy) : "",
  };
}

/**
 * The documents as f208173 built them, with no check of the approved sections
 * (DECISIONS.md 535): `approvedReport` and `renderReleased` as they were, the
 * earlier health check read as the record says, for a byte comparison.
 */
async function uncheckedBuild(dir: string, approved: ApprovedRun, out: string) {
  const { input, pseudonyms } = loadRun(dir);
  const report = buildReport({ ...input, overrides: approved.overrides, intake: approved.intake ?? input.intake, preparedBy: approved.preparedBy ?? null, benchmark: approved.benchmark ?? null });
  for (const built of Object.values(report.outputs)) {
    built.sections = approved.sections.filter((s) => s.output === built.output).sort((a, b) => a.order - b.order);
    const current = new Set(built.sections.map((s) => s.currentVersionId));
    built.versions = approved.versions.filter((v) => current.has(v.id));
  }
  mkdirSync(out, { recursive: true });
  const files = {
    raw_notes: join(out, OUTPUT_FILES.rawNotes),
    executive_summary: join(out, OUTPUT_FILES.executive),
    full_health_check: join(out, OUTPUT_FILES.full),
    ceo_one_pager: join(out, OUTPUT_FILES.ceo),
    readout_kit: join(out, OUTPUT_FILES.kit),
    action_lists: join(out, OUTPUT_FILES.actionLists),
    tickets: join(out, OUTPUT_FILES.tickets),
  };
  await printDocuments(report, pseudonyms, { rawNotes: files.raw_notes, executive: files.executive_summary, full: files.full_health_check, ceo: files.ceo_one_pager, kit: files.readout_kit, layout: join(out, OUTPUT_FILES.layout) });
  writeActionFiles(report, pseudonyms, dir, { workbook: files.action_lists, tickets: files.tickets });
  const study = await renderCaseStudyDocx(report, pseudonyms);
  const caseStudy = study ? join(out, "case-study-draft.docx") : undefined;
  if (study && caseStudy) writeFileSync(caseStudy, study);
  return fileParts(files, buildMicrositePages(report, pseudonyms), caseStudy);
}

const gone = (lines: string[]): string[] => lines.filter((l) => /can't be read now/.test(l));
const UNAPPROVED = `Run ${RUN_ID}: the approved sections hold no comparison with the earlier health check, so the documents have none.`;
/** Every sentence the worker's log has of the earlier health check or a comparison with it: decisions 522, 531, 534 and 535's. */
const said = (lines: string[]): string[] => lines.filter((l) => /earlier health check|comparison/.test(l));
type Worker = Awaited<ReturnType<typeof worker>>;

/** The earlier folder can't be read at the draft, in each of the ways measured, and is readable again after. */
const UNREADABLE_AT_DRAFT: Array<[string, (w: Worker) => () => void]> = [
  [
    "metrics.json can't be read",
    (w) => {
      chmodSync(join(w.earlier, "metrics.json"), 0);
      return () => chmodSync(join(w.earlier, "metrics.json"), 0o600);
    },
  ],
  [
    "the earlier folder is absent",
    (w) => {
      renameSync(w.earlier, `${w.earlier}-away`);
      return () => renameSync(`${w.earlier}-away`, w.earlier);
    },
  ],
];

describe("a re-check records whether its draft read the earlier health check, and its documents follow (decision 534)", () => {
  for (const [what, spoil] of UNREADABLE_AT_DRAFT) {
    test(`drafted while ${what}, readable at the make: the record is false, the documents are made, and none holds a progress page or section`, async () => {
      const w = await worker();
      const restore = spoil(w);
      await w.draft();
      restore();
      assert.equal(w.draftHasProgress(), false);
      assert.equal(w.record(), '{"read":false}');
      const made = await w.make();
      assert.deepEqual(made.progress, NONE);
      assert.equal(made.report.input.baseline, null);
      // Every document, the printed decks too, as with no earlier health check at all.
      rmSync(w.earlier, { recursive: true });
      assert.deepEqual((await w.make()).texts, made.texts);
      assert.deepEqual(gone(w.deps.lines), [], "not read at the draft: nothing to say");
    });
  }

  test("drafted with the earlier folder readable: the record is true, and the documents are byte for byte those built with no record", async () => {
    const w = await worker();
    await w.draft();
    assert.equal(w.record(), '{"read":true}');
    assert.equal(statSync(join(w.dir, BASELINE_READ_FILE)).mode & 0o777, 0o600);
    assert.deepEqual(readdirSync(w.dir).filter((f) => f.endsWith(".writing")), []);
    const kept = await w.make();
    assert.deepEqual(kept.progress, EVERY);
    rmSync(join(w.dir, BASELINE_READ_FILE));
    const tip = await w.make();
    assert.deepEqual(kept.texts, tip.texts);
    assert.deepEqual(gone(w.deps.lines), []);
  });

  test("read at the draft and the earlier folder removed before the make: the documents are made, the decks have no progress page, the Word notes hold the drafted section, and the log says so once a make", async () => {
    const w = await worker();
    await w.draft();
    assert.equal(w.record(), '{"read":true}');
    const whole = await w.make();
    rmSync(w.earlier, { recursive: true });
    const first = await w.make();
    assert.deepEqual(first.progress, ONLY_THE_NOTES);
    // The section drafted and approved, word for word as when the earlier health check could be read.
    assert.equal(first.texts["raw notes"], whole.texts["raw notes"]);
    assert.deepEqual(gone(first.lines), [GONE]);
    const second = await w.make();
    assert.deepEqual(second.progress, ONLY_THE_NOTES);
    assert.deepEqual(gone(second.lines), [GONE]);
    assert.deepEqual(gone(w.deps.lines), [GONE, GONE]);
    assert.deepEqual(gone(whole.lines), []);
    // Fixed words: no path, run id of the earlier one, or error.
    for (const line of gone(w.deps.lines)) assert.ok(!line.includes(w.earlier) && !line.includes(EARLIER_ID) && !/ENOENT|JSON/.test(line), line);
  });

  for (const [what, spoil] of UNREADABLE_AT_DRAFT) {
    test(`no record (drafted before this deploys), drafted while ${what}: no comparison, since the approved sections hold none, and the log says so once a make (decision 535)`, async () => {
      const w = await worker();
      const restore = spoil(w);
      await w.draft();
      restore();
      rmSync(join(w.dir, BASELINE_READ_FILE));
      const made = await w.make();
      // 534 left the old fault here for a re-check drafted before the record; 535 mends it from the approved sections.
      assert.deepEqual(made.progress, NONE);
      assert.equal(made.report.input.baseline, null);
      assert.deepEqual(said(made.lines), [UNAPPROVED]);
      // The same as a record that says it was read: byte for byte.
      writeFileSync(join(w.dir, BASELINE_READ_FILE), '{"read":true}');
      const read = await w.make();
      assert.deepEqual(read.texts, made.texts);
      assert.deepEqual(said(read.lines), [UNAPPROVED]);
      assert.deepEqual(gone(w.deps.lines), []);
    });
  }

  test("no record, drafted with the earlier folder readable: built as before", async () => {
    const w = await worker();
    await w.draft();
    rmSync(join(w.dir, BASELINE_READ_FILE));
    const made = await w.make();
    assert.deepEqual(made.progress, EVERY);
    rmSync(w.earlier, { recursive: true });
    const after = await w.make();
    assert.deepEqual(after.progress, ONLY_THE_NOTES);
    assert.deepEqual(gone(w.deps.lines), [], "with no record, nothing says the draft read it");
  });

  test("a damaged record leaves no earlier health check: the documents are made with no progress page, and nothing is logged", async () => {
    const damage: Array<[string, (path: string) => void]> = [
      ["half a file", (path) => writeFileSync(path, "{")],
      ["a list", (path) => writeFileSync(path, "[]")],
      ["a word for read", (path) => writeFileSync(path, '{"read":"yes"}')],
      ["empty", (path) => writeFileSync(path, "")],
      ["a field more", (path) => writeFileSync(path, '{"read":true,"at":"2026-11-04"}')],
      ["unreadable", (path) => chmodSync(path, 0)],
    ];
    for (const [what, spoil] of damage) {
      const w = await worker();
      await w.draft();
      assert.equal(w.record(), '{"read":true}');
      spoil(join(w.dir, BASELINE_READ_FILE));
      const made = await w.make();
      assert.deepEqual(made.progress, ONLY_THE_NOTES, what);
      assert.equal(made.report.input.baseline, null, what);
      assert.deepEqual(gone(w.deps.lines), [], what);
    }
  });

  test("drafted twice, unreadable then readable: the record is true and the documents hold the progress the second draft drafted; the other order, false and none", async () => {
    const one = await worker();
    const restore = UNREADABLE_AT_DRAFT[0]![1](one);
    await one.draft();
    restore();
    assert.equal(one.record(), '{"read":false}');
    await one.draft();
    assert.equal(one.record(), '{"read":true}');
    assert.ok(one.draftHasProgress());
    assert.deepEqual((await one.make()).progress, EVERY);

    const other = await worker();
    await other.draft();
    assert.equal(other.record(), '{"read":true}');
    const restoreOther = UNREADABLE_AT_DRAFT[0]![1](other);
    await other.draft();
    restoreOther();
    assert.equal(other.record(), '{"read":false}');
    assert.equal(other.draftHasProgress(), false);
    assert.deepEqual((await other.make()).progress, NONE);
  });

  test("the record is written before the sections file and the console's save (decision 529)", async () => {
    const w = await worker();
    await w.draft();
    rmSync(join(w.dir, "report-sections.json"));
    const seen: Array<{ sections: boolean; saved: boolean }> = [];
    let saved = false;
    const save = w.deps.store.saveRunOutput.bind(w.deps.store);
    w.deps.store.saveRunOutput = async (id, output) => ((saved = true), save(id, output));
    const real = fs.renameSync;
    fs.renameSync = ((from: fs.PathLike, to: fs.PathLike) => {
      if (String(to).endsWith(BASELINE_READ_FILE)) seen.push({ sections: existsSync(join(w.dir, "report-sections.json")), saved });
      return real(from, to);
    }) as typeof fs.renameSync;
    syncBuiltinESMExports();
    try {
      await w.draft();
    } finally {
      fs.renameSync = real;
      syncBuiltinESMExports();
    }
    assert.deepEqual(seen, [{ sections: false, saved: false }]);
    assert.ok(existsSync(join(w.dir, "report-sections.json")) && saved);
  });

  test("a first health check keeps no record", async () => {
    const w = await worker({ baselineRunId: null });
    await w.draft();
    assert.equal(w.record(), null);
    assert.deepEqual(readdirSync(w.dir).filter((f) => /baseline/.test(f)), []);
  });

  test("the record goes nowhere but the run folder", async () => {
    const w = await worker();
    await w.draft();
    const sent = JSON.stringify(w.deps.store.runs.get(RUN_ID)!.output);
    const made = await w.make();
    assert.deepEqual(made.progress, EVERY);
    for (const item of w.drive.items.values()) assert.doesNotMatch(item.name, /baseline/i);
    const out = JSON.stringify([sent, w.deps.lines, w.deps.store.benchmarkRows, [...w.deps.store.microsites.values()]]);
    assert.ok(!out.includes(BASELINE_READ_FILE) && !out.includes('{"read":'));
  });

  test("hc render follows the record: not read, no progress page; read and the earlier folder gone, it says so on stderr once", async () => {
    const cli = (dir: string) => spawnSync(process.execPath, [fileURLToPath(new URL("../../cli/hc.ts", import.meta.url)), "render", "--run", dir], { encoding: "utf8" });
    const notRead = await worker();
    const restore = UNREADABLE_AT_DRAFT[0]![1](notRead);
    await notRead.draft();
    restore();
    const quiet = cli(notRead.dir);
    assert.equal(quiet.status, 0, quiet.stdout + quiet.stderr);
    const sections = JSON.parse(readFileSync(join(notRead.dir, "report-sections.json"), "utf8")) as Array<{ sections: Array<{ key: string }> }>;
    assert.ok(!sections.some((o) => o.sections.some((s) => s.key === "progress")));
    assert.ok(!/^#+ R\d+ Since your last health check$/m.test(docxText(readFileSync(join(notRead.dir, "raw-notes.docx")))));
    assert.ok(!docxText(readFileSync(join(notRead.dir, "readout-kit.docx"))).includes("Since the last health check"));
    assert.equal(loadRun(notRead.dir).input.baseline, null);
    assert.ok(!/can't be read now/.test(quiet.stderr + quiet.stdout));
    // The printed decks, as with no earlier health check at all.
    const decks = () => ["full-health-check.pdf", "executive-summary.pdf", "ceo-one-pager.pdf"].map((f) => pdfParts(join(notRead.dir, f)));
    const printed = decks();
    rmSync(notRead.earlier, { recursive: true });
    assert.equal(cli(notRead.dir).status, 0);
    assert.deepEqual(decks(), printed);

    const read = await worker();
    await read.draft();
    rmSync(read.earlier, { recursive: true });
    const said = cli(read.dir);
    assert.equal(said.status, 0, said.stdout + said.stderr);
    assert.deepEqual(said.stderr.split("\n").filter((l) => /can't be read now/.test(l)), [GONE]);
    assert.ok(!said.stdout.includes("can't be read now"));
    // The run's own id from its run.json and fixed words: no folder, error or name on stderr at all.
    assert.deepEqual(said.stderr.split("\n").filter(Boolean), [GONE]);
    for (const leak of [read.dir, read.earlier, EARLIER_ID, "Synthetic", "ENOENT", "Error"]) assert.ok(!said.stderr.includes(leak), leak);
  });
});

describe("the kept corrections of decision 528 beside the record", () => {
  const STORAGE = "8,200";
  const CORRECTED = ["Data storage: 8,200 of 10,240 MB used then", "£704k to", "£704,113 to", "(4,123 records)"];
  /** As the earlier health check's documents job keeps them (decision 522). */
  const keepEarlier = (w: Worker, rows: NumberOverride[]) => writeFileSync(join(w.earlier, "overrides.json"), JSON.stringify(rows));

  test("drafted with rows kept and read true: the documents are those built with no record, corrected", async () => {
    const w = await worker();
    const rows = [w.rows.storage, w.rows.sized, w.rows.titled];
    keepEarlier(w, rows);
    await w.draft();
    assert.equal(w.record(), '{"read":true}');
    assert.equal(readFileSync(join(w.dir, BASELINE_OVERRIDES_FILE), "utf8"), JSON.stringify(rows));
    const kept = await w.make();
    assert.deepEqual(kept.progress, EVERY);
    assert.ok(kept.texts["raw notes"].includes(`Data storage: ${STORAGE} of 10,240 MB used then`));
    rmSync(join(w.dir, BASELINE_READ_FILE));
    assert.deepEqual((await w.make()).texts, kept.texts);
  });

  test("read true and the kept corrections unreadable: no earlier health check, the Word notes hold the approved section, and the log says nothing of the earlier one (528's case)", async () => {
    const spoils: Array<[string, (path: string) => void]> = [
      ["half a file", (path) => writeFileSync(path, "[{")],
      ["unreadable", (path) => chmodSync(path, 0)],
    ];
    for (const [what, spoil] of spoils) {
      const w = await worker();
      keepEarlier(w, [w.rows.storage, w.rows.sized, w.rows.titled]);
      await w.draft();
      assert.equal(w.record(), '{"read":true}');
      spoil(join(w.dir, BASELINE_OVERRIDES_FILE));
      const made = await w.make();
      // The reverse case's shape: what was approved, and no comparison printed that nobody drafted.
      assert.deepEqual(made.progress, ONLY_THE_NOTES, what);
      assert.equal(made.report.input.baseline, null, what);
      assert.deepEqual(made.lines.filter((l) => /earlier|comparison|baseline/i.test(l)), [], what);
    }
  });

  test("read false with a stale kept file planted: no earlier health check, and none of its figures anywhere", async () => {
    const w = await worker();
    const rows = [w.rows.storage, w.rows.sized, w.rows.titled];
    keepEarlier(w, rows);
    const restore = UNREADABLE_AT_DRAFT[0]![1](w);
    await w.draft();
    restore();
    assert.equal(w.record(), '{"read":false}');
    assert.equal(existsSync(join(w.dir, BASELINE_OVERRIDES_FILE)), false);
    writeFileSync(join(w.dir, BASELINE_OVERRIDES_FILE), JSON.stringify(rows));
    const made = await w.make();
    assert.deepEqual(made.progress, NONE);
    assert.equal(made.report.input.baseline, null);
    for (const [name, text] of Object.entries(made.texts)) for (const figure of CORRECTED) assert.ok(!text.includes(figure), `${name}: ${figure}`);
  });
});

describe("a re-check's documents compare with its earlier health check only if the approved Word notes hold the progress section (decision 535)", () => {
  /** Drafted with the earlier folder unreadable, then a second delivery that read it, refused at the console's save: the record is ahead of the approved sections. */
  async function ahead(): Promise<Worker> {
    const w = await worker();
    const restore = UNREADABLE_AT_DRAFT[0]![1](w);
    await w.draft();
    restore();
    assert.equal(w.record(), '{"read":false}');
    await w.refused();
    assert.equal(w.record(), '{"read":true}');
    assert.equal(w.draftHasProgress(), false, "the console holds the first delivery's sections");
    return w;
  }

  test("the record ahead of the approved sections (a second delivery refused at the console's save): no document compares, and the log says so once a make", async () => {
    // The sentence is the exported one, and `said` sees every other sentence of the earlier health check: 522's, 531's and 534's.
    assert.equal(UNAPPROVED, `Run ${RUN_ID}: ${BASELINE_UNAPPROVED}`);
    for (const other of [BASELINE_GONE, BASELINE_ROWS_UNREAD, baselineRefusedSummary(1), baselineRefusedSummary(2)]) assert.deepEqual(said([other]), [other]);
    const w = await ahead();
    const first = await w.make();
    assert.deepEqual(first.progress, NONE);
    assert.equal(first.report.input.baseline, null);
    assert.deepEqual(said(first.lines), [UNAPPROVED]);
    // Nothing held or refused (make asserts both), and no case study draft.
    assert.equal(first.caseStudy, false);
    const second = await w.make();
    assert.deepEqual(second.progress, NONE);
    assert.deepEqual(said(second.lines), [UNAPPROVED]);
    assert.deepEqual(said(w.deps.lines), [UNAPPROVED, UNAPPROVED]);
    // Fixed words: no path, run id of the earlier one, name or error.
    for (const line of said(w.deps.lines)) for (const leak of [w.dir, w.earlier, EARLIER_ID, "Synthetic", "ENOENT", "Error"]) assert.ok(!line.includes(leak), leak);
    // Every document, the printed decks, action lists and tickets too, as with no earlier health check at all.
    rmSync(w.earlier, { recursive: true });
    assert.deepEqual((await w.make()).texts, first.texts);
  });

  test("the same with the record deleted (a re-check drafted before decision 534): no document compares, and the log says so once a make", async () => {
    const w = await ahead();
    rmSync(join(w.dir, BASELINE_READ_FILE));
    const first = await w.make();
    assert.deepEqual(first.progress, NONE);
    assert.equal(first.report.input.baseline, null);
    assert.deepEqual(said(first.lines), [UNAPPROVED]);
    const second = await w.make();
    assert.deepEqual(second.progress, NONE);
    assert.deepEqual(said(w.deps.lines), [UNAPPROVED, UNAPPROVED]);
    rmSync(w.earlier, { recursive: true });
    assert.deepEqual((await w.make()).texts, first.texts);
  });

  test("record true, the progress section approved, the earlier folder readable: every file byte for byte as a build with no check of the approved sections, and nothing logged", async () => {
    const w = await worker();
    await w.draft();
    assert.ok(w.draftHasProgress());
    const made = await w.make();
    assert.deepEqual(made.progress, EVERY);
    assert.ok(made.report.input.baseline);
    assert.deepEqual(said(made.lines), []);
    const unchecked = await uncheckedBuild(w.dir, w.handed.at(-1)!, join(w.deps.runsDir, "unchecked"));
    assert.deepEqual(unchecked, made.files);
  });

  test("record true, the progress section approved, the earlier folder gone: decision 534's sentence once a make, and not this one", async () => {
    const w = await worker();
    await w.draft();
    rmSync(w.earlier, { recursive: true });
    const first = await w.make();
    assert.deepEqual(first.progress, ONLY_THE_NOTES);
    assert.deepEqual(said(first.lines), [GONE]);
    const second = await w.make();
    assert.deepEqual(said(second.lines), [GONE]);
  });

  test("record true, no progress section approved, the earlier folder gone: neither sentence, and no progress page", async () => {
    const w = await ahead();
    rmSync(w.earlier, { recursive: true });
    const made = await w.make();
    assert.deepEqual(made.progress, NONE);
    assert.equal(made.report.input.baseline, null);
    assert.deepEqual(said(made.lines), []);
  });

  test("record false with the progress section approved (the first delivery read it, a second refused at the save didn't): no comparison, the decks have no progress page, the Word notes hold the approved section, and nothing of either is logged", async () => {
    const w = await worker();
    await w.draft();
    assert.ok(w.draftHasProgress());
    const whole = await w.make();
    const restore = UNREADABLE_AT_DRAFT[0]![1](w);
    await w.refused();
    restore();
    assert.equal(w.record(), '{"read":false}');
    assert.ok(w.draftHasProgress(), "the console holds the first delivery's sections");
    const made = await w.make();
    // The record wins (decision 534): what was approved, and no comparison printed.
    assert.deepEqual(made.progress, ONLY_THE_NOTES);
    assert.equal(made.report.input.baseline, null);
    assert.equal(made.texts["raw notes"], whole.texts["raw notes"]);
    assert.deepEqual(said(made.lines), []);
  });

  test("a progress section of another output is not the Word notes' comparison: no document compares, and the log says so", async () => {
    const w = await ahead();
    const made = await w.make({
      plant: (sections, versions) => {
        const like = sections.find((s) => s.output === "executive_summary")!;
        const version = versions.find((v) => v.id === like.currentVersionId)!;
        sections.push({ ...like, id: "planted-progress-0001", key: "progress", title: "Since your last health check", order: 999, currentVersionId: "planted-progress-0001-v1" });
        versions.push({ ...version, id: "planted-progress-0001-v1", sectionId: "planted-progress-0001", body: "Planted." });
      },
    });
    assert.ok(made.approved.sections.some((s) => s.output === "executive_summary" && s.key === "progress"));
    assert.deepEqual(made.progress, NONE);
    assert.equal(made.report.input.baseline, null);
    assert.deepEqual(said(made.lines), [UNAPPROVED]);
  });

  test("the org handbook's change log follows neither the record nor the approved sections: read false, the earlier folder readable at the make, it still compares while no report does", async () => {
    const w = await worker();
    const path = join(w.earlier, HANDBOOK_FILE);
    const handbook = Handbook.parse(JSON.parse(readFileSync(path, "utf8")));
    earlierHandbook(handbook);
    writeFileSync(path, JSON.stringify(handbook));
    const restore = UNREADABLE_AT_DRAFT[0]![1](w);
    await w.draft();
    restore();
    assert.equal(w.record(), '{"read":false}');
    const made = await w.make();
    assert.deepEqual(made.progress, NONE);
    assert.match(made.handbook, new RegExp(`\\n## ${SINCE_TITLE}\\n\\nThis compares your org on `));
    assert.match(made.handbook, new RegExp(`\\nRemoved: 1 flow\\nLead Nurture \\(old\\) \\(${REMOVED_FLOW}\\)\\n`));
    assert.deepEqual(said(made.lines), []);
  });

  test("the benchmark contribution and the case study draft hold nothing of the earlier health check the approved sections don't", async () => {
    const consenting = { ...INTAKE, customerName: "Synthetic Co", industry: "technology" as const, headcountBand: "51-200" as const, caseStudyConsent: true };
    const w = await ahead();
    const made = await w.make({ intake: consenting });
    assert.deepEqual(made.progress, NONE);
    assert.equal(made.caseStudy, false, "no case study draft without a comparison");
    assert.ok(w.deps.store.benchmarkRows.length > 0);
    // As for a re-check with no earlier health check at all.
    const none = await worker();
    rmSync(none.earlier, { recursive: true });
    await none.draft();
    assert.equal((await none.make({ intake: consenting })).caseStudy, false);
    assert.deepEqual(w.deps.store.benchmarkRows, none.deps.store.benchmarkRows);
    // And the consent is enough for one where the approved sections compare.
    const compared = await worker();
    await compared.draft();
    assert.equal((await compared.make({ intake: consenting })).caseStudy, true);
  });
});
