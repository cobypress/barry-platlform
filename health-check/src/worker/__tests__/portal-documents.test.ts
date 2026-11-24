import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { approvedReport, renderReleased, type ApprovedRun, type DocumentKey, type HeldFigure, type ReleasedDocuments } from "../../render/render-released.ts";
import { loadRun, OUTPUT_FILES } from "../../render/render-run.ts";
import { buildReport } from "../../narrative/sections.ts";
import { executiveHtml } from "../../render/executive.ts";
import { fullReportHtml } from "../../render/full-report.ts";
import { onePagerHtml } from "../../render/one-pager.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers } from "../../testing/replay.ts";
import { AUTOMATIC_DOCUMENTS_NOT_BUILT } from "../../render/render-released.ts";
import { ORG_ID } from "../../testing/synthetic-org.ts";
import { AutomaticReplay, automaticSyntheticCalibration } from "../../testing/automatic.ts";
import type { NarrativeClient } from "../../narrative/client.ts";
import { AUTOMATIC_CLAIM } from "../../narrative/draft.ts";
import { AUTOMATIC_FORBIDDEN, claimsAPerson, readerText } from "../../narrative/prompt.ts";
import { AUTOMATIC_NEXT_STEPS, CONSULTANT_NEXT_STEPS } from "../../render/exec-pages.ts";
import { ENGINE_TEXT_DIRS } from "../../digest/engine-texts-scan.ts";
import ts from "typescript";
import { SERVICE_MODULE } from "../../domains/index.ts";
import type { DocumentDrive, FolderSharing } from "../drive.ts";
import { processDocumentsJob, PORTAL_REFUSALS } from "../documents-job.ts";
import { readWorkerEnv } from "../env.ts";
import { createJobHandler } from "../worker.ts";
import { BadJobError, parseJob, type DocumentsJobInput } from "../types.ts";
import {
  documentsKey,
  DocumentsKeyError,
  forbiddenInDraft,
  forbiddenInFrame,
  HELD_PLACEHOLDER,
  openDocument,
  PORTAL_OUTPUTS,
  PORTAL_PDF_MAX_BYTES,
  SEAL_OVERHEAD,
  SEALED_MAX_BYTES,
  sealDocument,
  type PortalDocumentsSwitch,
} from "../portal-documents.ts";
import { gatedProcessor, lostRecorder, portalDocumentsJobId, type QueueJob } from "../bullmq.ts";
import { FAILURE_FLAG_SQL, FLAG_CONFLICT, HELD_REASON_SQL, HOLD_RUN_SQL, PORTAL_DOCUMENT_SQL, PORTAL_EXPIRED_SQL, PORTAL_SWEEP_LOCK_TIMEOUT_SQL, PORTAL_SWEEP_STATEMENT_TIMEOUT_SQL, PostgresWorkerStore, RAISE_FLAG_SQL, type SqlClient, type SqlPool, type SqlResult } from "../postgres-store.ts";
import { CONSOLE_CHECKS, passesCheck } from "../console-checks.ts";
import { baseDeps, consoleRun, RUN_ID, runInput, syntheticConnection } from "./helpers.ts";
import type { MemoryWorkerStore } from "../memory-store.ts";

// An automatic run's three PDFs, made when the portal asks, sealed into the
// console's database and put nowhere else (DECISIONS.md 698). A throwaway key
// made here stands in for HC_DOCUMENTS_KEY: no real key is ever used.

const KEY = randomBytes(32);
const ON: PortalDocumentsSwitch = { on: true, key: KEY, keyProblem: null };
const PORTAL = { kind: "documents" as const, runId: RUN_ID, deliver: "portal" as const };
const ORG15 = ORG_ID.slice(0, 15);
const FILE_OF: Record<(typeof PORTAL_OUTPUTS)[number], string> = { executive_summary: OUTPUT_FILES.executive, full_health_check: OUTPUT_FILES.full, ceo_one_pager: OUTPUT_FILES.ceo };

/** Any call to Drive or sharing fails the test: the portal's documents go nowhere but the database. */
const untouchable = <T extends object>(what: string): T =>
  new Proxy({} as T, {
    get: (_t, prop) => {
      if (prop === "then") return undefined;
      if (prop === "rootFolderId") return "root-folder-0001";
      return () => assert.fail(`${what}.${String(prop)} was called for a portal make`);
    },
  });

/** Claude writing a claim nobody reviewed, for every section it's asked to draft. */
const claiming = (text: string): NarrativeClient => ({
  complete: async ({ prompt, model }) => {
    const ids = (JSON.parse(prompt.slice(prompt.indexOf("<sources>") + 9, prompt.indexOf("</sources>"))) as Array<{ id: string }>).map((s) => s.id).slice(0, 1);
    return { stopReason: "end_turn", text: JSON.stringify({ paragraphs: [{ text, sources: ids }] }), model };
  },
});

const CLAIM = "A consultant reviewed it, and will walk you through it.";

/**
 * The synthetic org's automatic run, end to end with Claude writing a claim in
 * every draft, then released by code. Its run folder is the one the documents
 * are made from.
 */
async function releasedAutomaticRun(modules: string[] = consoleRun().modules) {
  const deps = baseDeps({ dataTransport: new AutomaticReplay(), automatic: { on: true, orgs: new Set([ORG15]) }, narrative: claiming(CLAIM) });
  deps.store.addRun(runInput({ run: consoleRun({ reviewMode: "automatic", modules }), calibration: await automaticSyntheticCalibration(), intake: null, answers: [] }));
  await deps.tokens.save(syntheticConnection());
  const outcome = await createJobHandler(deps)({ kind: "run", runId: RUN_ID }, { final: false });
  assert.deepEqual(outcome, { outcome: "done", detail: `Run ${RUN_ID} is released` });
  return deps;
}

/** What the console's documents job reads for the released run: its checked sections and their current versions. */
function inputOf(store: MemoryWorkerStore, change: (input: DocumentsJobInput) => void = () => {}): DocumentsJobInput {
  const stored = store.runs.get(RUN_ID)!;
  const out = stored.output!;
  const current = new Set(out.sections.map((s) => s.currentVersionId));
  // A consultant's name on the row is never printed: an automatic run has nobody who prepared it.
  const input: DocumentsJobInput = { run: structuredClone(stored.run), intake: null, sections: structuredClone(out.sections), versions: structuredClone(out.versions.filter((v) => current.has(v.id))), overrides: [], preparedBy: "consultant@example.com", state: null, driveFolderId: null, files: [] };
  change(input);
  return input;
}

/** The deps a portal make runs with: everything it must not touch is there and fails if touched. */
function portalDeps(from: Awaited<ReturnType<typeof releasedAutomaticRun>>, pages: Record<string, string>, made: { last?: ReleasedDocuments }) {
  return Object.assign(from, {
    portalDocuments: ON,
    drive: untouchable<DocumentDrive>("drive"),
    sharing: untouchable<FolderSharing>("sharing"),
    micrositeKey: randomBytes(32),
    alert: async () => {},
    // The real renderer, watched: what each page printed, and what the make returned.
    renderReleased: async (dir: string, approved: ApprovedRun, out: string) => {
      const result = await renderReleased(dir, { ...approved, portal: { onPrinted: (doc, html) => ((pages[doc] = html), approved.portal?.onPrinted(doc, html)) } }, out);
      if (!("refused" in result)) made.last = result;
      return result;
    },
  });
}

const rowsOf = (store: MemoryWorkerStore) => PORTAL_OUTPUTS.map((o) => store.portalDocuments.get(`${RUN_ID}|${o}`));

describe("the portal's documents for a released automatic run (DECISIONS.md 698)", async () => {
  const deps = await releasedAutomaticRun();
  const store = deps.store;
  const pages: Record<string, string> = {};
  const made: { last?: ReleasedDocuments } = {};
  portalDeps(deps, pages, made);
  // The benchmark would be added to on a consultant's make of a consenting run: not on this one.
  store.addDocumentsJob(inputOf(store, (i) => (i.run.benchmarkOptIn = true)));
  const writesBefore = store.writes.length;
  const handle = createJobHandler(deps);
  const outcome = await handle(PORTAL, { final: true });
  const first = rowsOf(store).map((r) => r && { ...r, sealed: Buffer.from(r.sealed) });
  const dir = store.runs.get(RUN_ID)!.runDir!;

  test("Claude's drafts that claimed a review fell back at draft time, so none reached a page", () => {
    const rejections = JSON.parse(readFileSync(join(dir, OUTPUT_FILES.rejections), "utf8")) as Array<{ reasons: string[] }>;
    assert.ok(rejections.length > 0 && rejections.every((r) => r.reasons.includes(AUTOMATIC_CLAIM)));
    const out = store.runs.get(RUN_ID)!.output!;
    const current = new Set(out.sections.map((s) => s.currentVersionId));
    assert.ok(out.versions.filter((v) => current.has(v.id)).every((v) => v.source === "engine"));
    for (const doc of ["executive", "full", "ceo"]) {
      assert.ok(pages[doc], doc);
      assert.ok(!pages[doc]!.includes(CLAIM.slice(0, 20)), doc);
      assert.deepEqual(forbiddenInFrame(pages[doc]!), [], doc);
    }
  });

  test("the three PDFs are sealed, and open again under the key to the bytes printed", () => {
    assert.equal(outcome.outcome, "done", outcome.detail);
    for (const [i, output] of PORTAL_OUTPUTS.entries()) {
      const row = first[i]!;
      assert.equal(row.heldForUs, false);
      const pdf = readFileSync(join(dir, "portal-documents", FILE_OF[output]));
      assert.ok(pdf.subarray(0, 5).toString("latin1") === "%PDF-");
      assert.ok(openDocument(row.sealed, RUN_ID, output, KEY).equals(pdf), output);
      assert.equal(row.sealed.length, pdf.length + SEAL_OVERHEAD);
      assert.ok(pdf.length < PORTAL_PDF_MAX_BYTES / 4, `${output} weighs ${pdf.length}`);
      // Kept 7 days from the make, as the console's CHECK allows.
      assert.equal(Date.parse(row.expiresAt) - Date.parse(row.madeAt), 7 * 86_400_000);
    }
    assert.equal(made.last?.repairs, "none");
  });

  test("the renderer still refuses this run's folder to anything but the portal's make", () => {
    const input = inputOf(store);
    assert.throws(() => approvedReport(dir, { intake: null, sections: input.sections, versions: input.versions, overrides: [] }), new RegExp(AUTOMATIC_DOCUMENTS_NOT_BUILT));
  });

  test("a row moved to another run or another output, or opened with another key, doesn't open", () => {
    const row = first[0]!;
    assert.throws(() => openDocument(row.sealed, "hc-2026-10-31-00000002", "executive_summary", KEY));
    assert.throws(() => openDocument(row.sealed, RUN_ID, "full_health_check", KEY));
    assert.throws(() => openDocument(row.sealed, RUN_ID, "executive_summary", randomBytes(32)));
    const tampered = Buffer.from(row.sealed);
    tampered[tampered.length - 1] ^= 1;
    assert.throws(() => openDocument(tampered, RUN_ID, "executive_summary", KEY));
  });

  test("nothing is uploaded, shared, published, contributed or written but the portal's rows", () => {
    const since = store.writes.slice(writesBefore).map((w) => w.method);
    assert.deepEqual(since, ["savePortalDocuments"]);
    assert.equal(store.microsites.size, 0);
    assert.equal(store.benchmarkRows.length, 0);
    assert.equal(store.documents.get(RUN_ID)!.made, null);
    assert.equal(store.documents.get(RUN_ID)!.input.state, null);
    assert.deepEqual(store.flagsOf(RUN_ID), []);
    // No page names who prepared it, though the row named a consultant.
    for (const doc of ["executive", "full", "ceo"]) assert.ok(!/consultant@example\.com|Prepared by/i.test(pages[doc]!), doc);
  });

  describe("made again, with a planted claim and a planted figure in the checked sections", async () => {
    const PLANTED = "We counted 98,765 records nobody owns.";
    const input = inputOf(store, (i) => {
      const plant = (output: string, text: string) => {
        const section = i.sections.find((s) => s.output === output)!;
        const v = i.versions.find((x) => x.id === section.currentVersionId)!;
        Object.assign(v, { body: `${text}\n\n${v.body}`, source: "claude_draft" });
      };
      // As if a draft had slipped past its guard: every check stored as passed.
      plant("executive_summary", "A consultant reviewed it.");
      plant("full_health_check", PLANTED);
    });
    store.addDocumentsJob(input);
    const again = await handle(PORTAL, { final: true });
    const second = rowsOf(store);

    test("a redelivery replaces the three rows, never adds to them", () => {
      assert.equal(again.outcome, "done", again.detail);
      assert.equal([...store.portalDocuments.keys()].filter((k) => k.startsWith(`${RUN_ID}|`)).length, 3);
      assert.ok(!second[1]!.sealed.equals(first[1]!.sealed));
    });

    test("the rendered-text check holds the document whose page holds the claim, with no servable bytes and one flag", () => {
      // Claude's planted words are on the page, where its section prints.
      assert.ok(readerText(pages.executive!).includes("A consultant reviewed it."));
      const exec = second[0]!;
      assert.equal(exec.heldForUs, true);
      assert.ok(exec.sealed.equals(HELD_PLACEHOLDER));
      assert.throws(() => openDocument(exec.sealed, RUN_ID, "executive_summary", KEY));
      assert.deepEqual(store.flagsOf(RUN_ID), ["documents_held"]);
      const flag = [...store.flags.values()].find((f) => f.runId === RUN_ID)!;
      assert.equal(flag.counts.held, 1);
      assert.equal(flag.counts.words, 1);
      // The rest are sealed as before.
      assert.equal(second[2]!.heldForUs, false);
      assert.ok(openDocument(second[2]!.sealed, RUN_ID, "ceo_one_pager", KEY).subarray(0, 5).toString("latin1") === "%PDF-");
    });

    test("the planted untraceable figure is repaired as decision 682 does: its section prints our own wording", () => {
      assert.equal(made.last?.held.length, 0);
      assert.ok(made.last?.fellBack?.some((f) => f.output === "full_health_check"));
      assert.notEqual(made.last?.repairs, "held for us");
      assert.ok(!pages.full!.includes("98,765"));
      assert.equal(second[1]!.heldForUs, false);
    });

    test("held again, the flag is raised once", async () => {
      await handle(PORTAL, { final: true });
      assert.equal(store.flagsOf(RUN_ID).length, 1);
    });

    test("expired rows go at the start of the next documents job while the switch is on, whatever the job", async () => {
      const at = Date.parse(second[0]!.expiresAt) + 1000;
      deps.now = () => new Date(at);
      // The switch on, the key gone: refused, and the clean-up still runs first.
      deps.portalDocuments = { on: true, key: null, keyProblem: "HC_DOCUMENTS_KEY isn't set on the worker" };
      const refused = await handle(PORTAL, { final: true });
      assert.equal(refused.outcome, "refused");
      assert.equal(rowsOf(store).filter(Boolean).length, 0);
      assert.ok(store.writes.some((w) => w.method === "deleteExpiredPortalDocuments" && w.detail === 3));
    });
  });
});

/** Prints placeholder files and no pages, so a gate's tests need no browser; counts its calls. */
function fakeRender(result: Partial<ReleasedDocuments> = {}, pagesHtml: string | null = null) {
  const calls: ApprovedRun[] = [];
  const render = async (_dir: string, approved: ApprovedRun, out: string) => {
    calls.push(approved);
    mkdirSync(out, { recursive: true });
    const files = {} as Record<DocumentKey, string>;
    for (const output of ["raw_notes", "executive_summary", "full_health_check", "ceo_one_pager", "action_lists", "tickets", "readout_kit"] as DocumentKey[]) {
      files[output] = join(out, `${output}.pdf`);
      writeFileSync(files[output], `%PDF- synthetic ${output}`);
    }
    if (pagesHtml !== null) for (const doc of ["executive", "full", "ceo"] as const) approved.portal?.onPrinted(doc, pagesHtml);
    return { files, warnings: [], held: [] as HeldFigure[], repairs: "none" as const, ...result };
  };
  return { render, calls };
}

function gateDeps(input: Partial<DocumentsJobInput["run"]>, options: { portal?: PortalDocumentsSwitch; held?: string; render?: ReturnType<typeof fakeRender> } = {}) {
  const render = options.render ?? fakeRender({}, "<p>Your score is good.</p>");
  const deps = baseDeps({ renderReleased: render.render, portalDocuments: options.portal ?? ON, drive: untouchable<DocumentDrive>("drive"), sharing: untouchable<FolderSharing>("sharing") });
  const run = consoleRun({ status: "released", reviewMode: "automatic", ...input });
  deps.store.addDocumentsJob({ run, intake: null, sections: [], versions: [], overrides: [], preparedBy: null, state: null, driveFolderId: null, files: [] });
  if (options.held) deps.store.held.set(RUN_ID, options.held);
  mkdirSync(join(deps.runsDir, RUN_ID), { recursive: true });
  writeFileSync(join(deps.runsDir, RUN_ID, "run.json"), "{}");
  return { deps, render };
}

describe("the portal's gate: each refusal reads nothing more and makes nothing (DECISIONS.md 698)", () => {
  const refused = async (setup: ReturnType<typeof gateDeps>, reason: RegExp | string) => {
    const outcome = await processDocumentsJob(RUN_ID, setup.deps, { final: true }, "portal");
    assert.equal(outcome.outcome, "refused", outcome.detail);
    if (typeof reason === "string") assert.equal(outcome.detail, reason);
    else assert.match(outcome.detail, reason);
    assert.equal(setup.render.calls.length, 0);
    assert.equal(setup.deps.store.portalDocuments.size, 0);
    assert.deepEqual(setup.deps.store.flagsOf(RUN_ID), []);
  };

  test("the switch off", () => refused(gateDeps({}, { portal: { on: false, key: KEY, keyProblem: null } }), PORTAL_REFUSALS.off));
  test("no switch at all", () => {
    const setup = gateDeps({});
    delete setup.deps.portalDocuments;
    return refused(setup, PORTAL_REFUSALS.off);
  });
  test("the key missing", () => refused(gateDeps({}, { portal: { on: true, key: null, keyProblem: "HC_DOCUMENTS_KEY isn't set on the worker" } }), /^HC_DOCUMENTS_KEY isn't set on the worker, so the portal's documents can't be sealed/));
  test("the key bad", () => refused(gateDeps({}, { portal: { on: true, key: null, keyProblem: "HC_DOCUMENTS_KEY must be 32 bytes, base64" } }), /^HC_DOCUMENTS_KEY must be 32 bytes, base64, so/));
  test("a consultant-led run", () => refused(gateDeps({ reviewMode: "consultant" }), PORTAL_REFUSALS.consultant));
  test("an automatic run not released", () => refused(gateDeps({ status: "in_review" }), PORTAL_REFUSALS.notReleased));
  test("a held automatic run", () => refused(gateDeps({ status: "in_review" }, { held: "score_moved" }), PORTAL_REFUSALS.notReleased));
  test("a hold on a run that reads as released", () => refused(gateDeps({}, { held: "score_moved" }), PORTAL_REFUSALS.held));

  test("sections that don't fit the run: a checked section naming an approver", async () => {
    const setup = gateDeps({});
    const section = { id: "s-0001", runId: RUN_ID, output: "executive_summary", key: "verdict", title: "Verdict", order: 0, status: "checked", currentVersionId: "v-0001", findingIds: [], metricIds: [], manualStepIds: [], approval: { approvedBy: "someone@example.com", approvedAt: "2026-11-01T09:00:00.000Z" } };
    setup.deps.store.documents.get(RUN_ID)!.input.sections = [section as unknown as DocumentsJobInput["sections"][number]];
    await refused(setup, /approved by no one/);
  });

  test("a released automatic run passes the gate, and a run released by a person after its hold (its hold cleared) too", async () => {
    const setup = gateDeps({});
    const outcome = await processDocumentsJob(RUN_ID, setup.deps, { final: true }, "portal");
    assert.equal(outcome.outcome, "done", outcome.detail);
    assert.equal(setup.render.calls.length, 1);
    // No consultant, no benchmark, no digest, and the portal's own way through the renderer.
    const approved = setup.render.calls[0]!;
    assert.equal(approved.preparedBy, null);
    assert.equal(approved.benchmark, null);
    assert.equal(approved.digest, undefined);
    assert.ok(approved.portal);
    assert.deepEqual(rowsOf(setup.deps.store).map((r) => r?.heldForUs), [false, false, false]);
  });

  test("an automatic run asked for the old way is still refused, as before", async () => {
    const setup = gateDeps({});
    setup.deps.store.documents.get(RUN_ID)!.input.state = "queued";
    const outcome = await processDocumentsJob(RUN_ID, setup.deps, { final: true });
    assert.equal(outcome.outcome, "failed");
    assert.equal(setup.deps.store.documents.get(RUN_ID)!.error, AUTOMATIC_DOCUMENTS_NOT_BUILT);
    assert.equal(setup.render.calls.length, 0);
    assert.equal(setup.deps.store.portalDocuments.size, 0);
  });
});

describe("a make held for us, exactly as decision 682 holds one (DECISIONS.md 698)", () => {
  const held = async (result: Partial<ReleasedDocuments>, counts: Record<string, number>) => {
    const setup = gateDeps({}, { render: fakeRender(result, "<p>Your score is good.</p>") });
    const outcome = await processDocumentsJob(RUN_ID, setup.deps, { final: true }, "portal");
    assert.equal(outcome.outcome, "done", outcome.detail);
    const rows = rowsOf(setup.deps.store);
    for (const row of rows) {
      assert.equal(row!.heldForUs, true);
      assert.ok(row!.sealed.equals(HELD_PLACEHOLDER));
    }
    assert.deepEqual(setup.deps.store.flagsOf(RUN_ID), ["documents_held"]);
    const flag = [...setup.deps.store.flags.values()][0]!;
    for (const [k, v] of Object.entries(counts)) assert.equal(flag.counts[k], v, k);
    // Counts only: every value a number.
    assert.ok(Object.values(flag.counts).every((v) => typeof v === "number"));
  };

  test("the repair's held-for-us ceiling holds all three", () => held({ repairs: "held for us", masks: { executive: 11, full: 0, ceo: 0, microsite: 0 } }, { held: 3, masks: 11 }));
  test("a figure still held after both masks holds all three", () => held({ held: [{ document: "The full health check", page: 3, figure: "98,765", reason: "isn't in the data" }] }, { held: 3, untraced: 1 }));
});

describe("a make that fails writes nothing, and leaves good documents in date as they were (DECISIONS.md 698)", () => {
  /** A good make first, then the failing one, on the same store. */
  const remake = async (failing: (dir: string, approved: ApprovedRun, out: string) => Promise<unknown>, final = true) => {
    const setup = gateDeps({});
    assert.equal((await processDocumentsJob(RUN_ID, setup.deps, { final: true }, "portal")).outcome, "done");
    const before = rowsOf(setup.deps.store).map((r) => ({ ...r!, sealed: Buffer.from(r!.sealed) }));
    setup.deps.renderReleased = failing as typeof renderReleased;
    const writes = setup.deps.store.writes.length;
    const outcome = await processDocumentsJob(RUN_ID, setup.deps, { final }, "portal").catch((err: Error) => ({ outcome: "thrown", detail: err.message }));
    const after = rowsOf(setup.deps.store);
    for (const [i, row] of before.entries()) {
      assert.equal(after[i]!.heldForUs, false);
      assert.ok(after[i]!.sealed.equals(row.sealed));
      assert.equal(after[i]!.madeAt, row.madeAt);
    }
    assert.deepEqual(setup.deps.store.flagsOf(RUN_ID), []);
    assert.deepEqual(setup.deps.store.writes.slice(writes), []);
    return outcome;
  };
  test("a render that fails on the last delivery: failed, nothing replaced, no flag", async () => {
    const outcome = await remake(async () => Promise.reject(new Error("the browser closed")));
    assert.equal(outcome.outcome, "failed");
    assert.match(outcome.detail, /the browser closed/);
  });
  test("before the last delivery, it goes back to the queue, with nothing replaced", async () => {
    assert.equal((await remake(async () => Promise.reject(new Error("the browser closed")), false)).outcome, "thrown");
  });
  test("a correction that can't be carried: failed, nothing replaced", async () => {
    const outcome = await remake(async () => ({ refused: [{ title: "A finding", field: "value.amount", asRead: 10, last: 12, why: { kind: "stale", from: 10 } }] }));
    assert.equal(outcome.outcome, "failed");
  });
  test("a page never seen printed: failed, never sealed unread, nothing replaced", async () => {
    const outcome = await remake(fakeRender({}, null).render);
    assert.equal(outcome.outcome, "failed");
    assert.match(outcome.detail, /weren't seen as printed/);
  });
  test("a content hold, by contrast, does replace them with held rows", async () => {
    const setup = gateDeps({});
    await processDocumentsJob(RUN_ID, setup.deps, { final: true }, "portal");
    setup.deps.renderReleased = fakeRender({ repairs: "held for us" }, "<p>Fine.</p>").render;
    await processDocumentsJob(RUN_ID, setup.deps, { final: true }, "portal");
    assert.deepEqual(rowsOf(setup.deps.store).map((r) => r?.heldForUs), [true, true, true]);
    assert.deepEqual(setup.deps.store.flagsOf(RUN_ID), ["documents_held"]);
  });
  test("a PDF over the limit is held for us, the others sealed", async () => {
    const big = fakeRender({}, "<p>Your score is good.</p>");
    const render = async (dir: string, approved: ApprovedRun, out: string) => {
      const result = await big.render(dir, approved, out);
      writeFileSync(result.files.full_health_check, Buffer.alloc(PORTAL_PDF_MAX_BYTES + 1));
      return result;
    };
    const setup = gateDeps({}, { render: { render, calls: big.calls } as ReturnType<typeof fakeRender> });
    await processDocumentsJob(RUN_ID, setup.deps, { final: true }, "portal");
    assert.deepEqual(rowsOf(setup.deps.store).map((r) => r?.heldForUs), [false, true, false]);
    assert.equal([...setup.deps.store.flags.values()][0]!.counts.tooLarge, 1);
  });
  test("a console without the table: the make fails plainly on the last delivery, with nothing written", async () => {
    const setup = gateDeps({});
    setup.deps.store.portalTable = false;
    const outcome = await processDocumentsJob(RUN_ID, setup.deps, { final: true }, "portal");
    assert.equal(outcome.outcome, "failed");
    assert.match(outcome.detail, /HealthCheckPortalDocument/);
    assert.ok(setup.deps.lines.some((l) => /no HealthCheckPortalDocument table yet/.test(l)));
  });
});

describe("the words check: Claude's text and the page's frame only (DECISIONS.md 698)", () => {
  test("Claude's text is read as its reader sees it: soft hyphens, zero-width characters, tags inside a word and alt text hide nothing", () => {
    for (const text of [
      "A consultant can help.",
      "con­sultant",
      "con&shy;sultant",
      "consul‍tant",
      "consul​tant",
      "reviewed⁠",
      "﻿reviewed",
      "re<span>viewed</span>",
      "<b>Prepared</b> by us",
      'See <img alt="Reviewed by a consultant" src="x.png">',
      "We&#39;ll walk you through it.",
      "Book a call with us.",
    ]) assert.ok(forbiddenInDraft(text).length > 0, JSON.stringify(text));
  });
  test("the draft guard reads the same way", () => {
    for (const text of ["con­sultant", "con&shy;sultant", "We'll talk​ you through it", "reviewed by <i>our</i> team", 'x <img alt="checked by our team">']) assert.ok(claimsAPerson(text), JSON.stringify(text));
    for (const text of ["Review your sharing settings.", "We found 3 flows without fault paths."]) assert.ok(!claimsAPerson(text), text);
  });
  test("covers every word the draft guard refuses, the reviewer's phrasings among them", () => {
    for (const text of ["Our team has looked over these findings.", "Your Black Cloud contact can help with the next steps.", "One of our specialists will be in touch.", "We have verified these results by hand.", "The findings below were checked by our team."]) {
      assert.ok(AUTOMATIC_FORBIDDEN.some((p) => p.test(text)));
      assert.ok(forbiddenInDraft(text).length > 0, text);
    }
  });
  test("the frame: a Prepared by line, the draft mark and a consultant's next steps, however they're marked up", () => {
    for (const html of [
      '<div class="who"><b>31 Oct 2026</b>Prepared by Black Cloud</div>',
      "<p>Prepared&nbsp;by Someone Example, Black Cloud</p>",
      '<p class="for">Prepared for</p><p>Synthetic Co</p><p>by Someone Example, Black Cloud</p>',
      '<div class="footer" data-chrome><span>Black Cloud Barometer | Draft, not approved for release</span></div>',
      `<h3>${CONSULTANT_NEXT_STEPS.first.title}</h3><p>We&#39;ll take your team through what we found, answer their questions, and agree what matters most to you.</p>`,
      `<p>${CONSULTANT_NEXT_STEPS.closing}</p>`,
      "<p>We&#39;ll look at your Salesforce again after 90 days, so you can see what has improved and what still needs work.</p>",
    ]) assert.ok(forbiddenInFrame(html).length > 0, html);
  });
  test("the engine's words and the org's never trip the frame", () => {
    for (const text of ["Most knowledge articles haven't been reviewed in a year", "Forecast changed by hand", "Check this by hand", "Draft a description of what each field holds", "Profile: Account Manager (12 users)", "Sales Analyst profile", "Quote status Draft", "Prepared By field on Quote", AUTOMATIC_NEXT_STEPS.first.title, AUTOMATIC_NEXT_STEPS.first.text, AUTOMATIC_NEXT_STEPS.recheck]) {
      assert.deepEqual(forbiddenInFrame(`<p>${text}</p>`), [], text);
    }
  });
  test("an automatic run's own next steps say nothing the draft guard would refuse", () => {
    for (const text of [AUTOMATIC_NEXT_STEPS.first.title, AUTOMATIC_NEXT_STEPS.first.text, AUTOMATIC_NEXT_STEPS.recheck]) assert.deepEqual(forbiddenInDraft(text), [], text);
  });

  test("every text the engine writes in the folders that reach a page: none trips the frame but the frame's own consultant-led and draft lines", () => {
    const root = join(import.meta.dirname, "../..");
    const files = (dir: string): string[] => readdirSync(dir, { withFileTypes: true }).flatMap((e) => (e.isDirectory() ? (e.name === "__tests__" ? [] : files(join(dir, e.name))) : e.name.endsWith(".ts") ? [join(dir, e.name)] : []));
    const hits: string[] = [];
    let texts = 0;
    for (const file of ENGINE_TEXT_DIRS.flatMap((d) => files(join(root, d)))) {
      const source = ts.createSourceFile(file, readFileSync(file, "utf8"), ts.ScriptTarget.Latest, true);
      const visit = (node: ts.Node): void => {
        // Every literal, and every template with what it puts in read as a word, so nothing a template writes around a value escapes.
        let text: string | null = null;
        if (ts.isStringLiteral(node) || ts.isNoSubstitutionTemplateLiteral(node)) text = node.text;
        else if (ts.isTemplateExpression(node)) text = node.head.text + node.templateSpans.map((span) => `X${span.literal.text}`).join("");
        if (text !== null && /\s/.test(text.trim())) {
          texts++;
          if (forbiddenInFrame(text).length) hits.push(readerText(text).trim());
        }
        ts.forEachChild(node, visit);
      };
      visit(source);
    }
    assert.ok(texts > 3000, `${texts} texts`);
    const frame = [/Draft, not approved for release/, /^Prepared by X, Black Cloud$/, /^Prepared by Black Cloud$/, /^by X, Black Cloud$/, /Walk through it together/, /We'll take your team through what we found/, /Your consultant will talk you through/, /We'll look at your Salesforce again after 90 days/];
    for (const hit of hits) assert.ok(frame.some((f) => f.test(hit)), `an engine text trips the frame: ${hit.slice(0, 160)}`);
    // And a section of the engine's own text is never read for Claude's words, whatever it says: only a version Claude drafted is.
  });
});

describe("the seal (DECISIONS.md 698)", () => {
  const pdf = Buffer.from("%PDF-1.7 synthetic");
  test("a fixed vector: version 1, the IV, the tag, then ciphertext as long as the PDF", () => {
    const iv = Buffer.alloc(12, 7);
    const sealed = sealDocument(pdf, RUN_ID, "executive_summary", Buffer.alloc(32, 1), iv);
    assert.equal(sealed[0], 1);
    assert.ok(sealed.subarray(1, 13).equals(iv));
    assert.equal(sealed.length, pdf.length + SEAL_OVERHEAD);
    assert.ok(openDocument(sealed, RUN_ID, "executive_summary", Buffer.alloc(32, 1)).equals(pdf));
    // The same inputs seal the same, so the console can test against this vector.
    assert.ok(sealDocument(pdf, RUN_ID, "executive_summary", Buffer.alloc(32, 1), iv).equals(sealed));
  });
  test("a fresh IV each time", () => assert.ok(!sealDocument(pdf, RUN_ID, "executive_summary", KEY).equals(sealDocument(pdf, RUN_ID, "executive_summary", KEY))));
  test("the placeholder a held row stores is one byte, inside the console's CHECK, and opens under no key", () => {
    assert.equal(HELD_PLACEHOLDER.length, 1);
    assert.ok(HELD_PLACEHOLDER.length >= 1 && HELD_PLACEHOLDER.length <= SEALED_MAX_BYTES);
    assert.throws(() => openDocument(HELD_PLACEHOLDER, RUN_ID, "executive_summary", KEY));
    assert.ok(PORTAL_PDF_MAX_BYTES + SEAL_OVERHEAD <= SEALED_MAX_BYTES);
  });
  test("the key: 32 bytes of base64, different from every other key, and never repeated in an error", () => {
    const b64 = (b: Buffer) => b.toString("base64");
    assert.equal(documentsKey({}), null);
    assert.ok(documentsKey({ HC_DOCUMENTS_KEY: b64(KEY) })!.equals(KEY));
    for (const env of [{ HC_DOCUMENTS_KEY: "not base64!" }, { HC_DOCUMENTS_KEY: b64(randomBytes(16)) }, { HC_DOCUMENTS_KEY: b64(KEY), HC_MICROSITE_KEY: b64(KEY) }, { HC_DOCUMENTS_KEY: b64(KEY), HC_CONNECT_KEY: b64(KEY) }, { HC_DOCUMENTS_KEY: b64(KEY), HC_TOKEN_KEY: b64(KEY) }]) {
      assert.throws(() => documentsKey(env), (err: Error) => err instanceof DocumentsKeyError && !err.message.includes(env.HC_DOCUMENTS_KEY));
    }
  });
  test("the worker starts with the key bad, and keeps why for every request", () => {
    const base = { REDIS_URL: "redis://localhost:6379", HC_WORKER_DATABASE_URL: "postgres://localhost/hc", HC_CONNECT_KEY: randomBytes(32).toString("base64"), HC_TOKEN_KEY: randomBytes(32).toString("base64"), HC_CONNECTIONS_DIR: "/tmp/c", HC_RUNS_DIR: "/tmp/r", HC_SF_CLIENT_ID: "id", HC_SF_CLIENT_SECRET: "secret", HC_SF_REDIRECT_URI: "https://example.com/cb" };
    const bad = readWorkerEnv({ ...base, HC_DOCUMENTS_ON_DEMAND: "on", HC_DOCUMENTS_KEY: "short" });
    assert.ok(bad.ok);
    assert.deepEqual(bad.config.portalDocuments, { on: true, key: null, keyProblem: "HC_DOCUMENTS_KEY must be 32 bytes, base64" });
    const none = readWorkerEnv(base);
    assert.ok(none.ok);
    assert.deepEqual(none.config.portalDocuments, { on: false, key: null, keyProblem: "HC_DOCUMENTS_KEY isn't set on the worker" });
    const good = readWorkerEnv({ ...base, HC_DOCUMENTS_ON_DEMAND: "on ", HC_DOCUMENTS_KEY: KEY.toString("base64") });
    assert.ok(good.ok);
    // "on " isn't "on": a switch that puts something before a customer doesn't guess.
    assert.equal(good.config.portalDocuments.on, false);
    assert.ok(good.config.portalDocuments.key!.equals(KEY));
  });
});

describe("the queue and the portal's job (DECISIONS.md 698)", () => {
  test("a documents job may ask for the portal, and for nothing else", () => {
    assert.deepEqual(parseJob({ kind: "documents", runId: RUN_ID, deliver: "portal" }), PORTAL);
    assert.deepEqual(parseJob({ kind: "documents", runId: RUN_ID }), { kind: "documents", runId: RUN_ID });
    assert.throws(() => parseJob({ kind: "documents", runId: RUN_ID, deliver: "drive" }), BadJobError);
    assert.throws(() => parseJob({ kind: "documents", runId: RUN_ID, deliver: "portal", email: "a@example.com" }), BadJobError);
  });

  test("handed to the heavy slot under its fixed id, and kept there by nothing once it ends", async () => {
    const added: Array<{ data: unknown; opts: Record<string, unknown> }> = [];
    const heavy = { add: async (_n: string, data: unknown, opts: Record<string, unknown>) => void added.push({ data, opts }) };
    const process = gatedProcessor(async () => ({ outcome: "done", detail: "" }), { UnrecoverableError: Error }, heavy, () => {});
    const job = (data: unknown, id: string): QueueJob => ({ id, name: "health-check-documents", data, attemptsMade: 0, opts: { attempts: 3, removeOnComplete: true, removeOnFail: 200 } });
    await process(job(PORTAL, portalDocumentsJobId(RUN_ID)));
    await process(job({ kind: "documents", runId: RUN_ID }, `documents-${RUN_ID}-1`));
    assert.equal(portalDocumentsJobId(RUN_ID), `documents-portal-${RUN_ID}`);
    assert.deepEqual(added[0]!.opts, { attempts: 3, removeOnComplete: true, removeOnFail: true, jobId: `documents-portal-${RUN_ID}` });
    // A Drive make keeps its id for a day, as before.
    assert.deepEqual(added[1]!.opts, { attempts: 3, removeOnComplete: { age: 86_400 }, removeOnFail: 200, jobId: `documents-${RUN_ID}-1` });
  });

  test("a portal make BullMQ gave up on never fails the Drive make's row", async () => {
    const failed: string[] = [];
    const record = lostRecorder({ recordRunFailure: async () => {}, failDocuments: async (runId) => void failed.push(runId) });
    await record(PORTAL, "lost");
    assert.deepEqual(failed, []);
    await record({ kind: "documents", runId: RUN_ID }, "lost");
    assert.deepEqual(failed, [RUN_ID]);
  });
});

type Handler = (text: string, values: unknown[]) => SqlResult | undefined;
class FakePool implements SqlPool {
  readonly statements: Array<{ text: string; values: unknown[] }> = [];
  readonly handlers: Handler[] = [];
  async query(text: string, values: unknown[] = []): Promise<SqlResult> {
    this.statements.push({ text, values });
    for (const h of this.handlers) {
      const r = h(text, values);
      if (r) return r;
    }
    return { rows: [], rowCount: 1 };
  }
  async connect(): Promise<SqlClient & { release(): void }> {
    return { query: (t, v) => this.query(t, v), release: () => {} };
  }
}

describe("the Postgres store's portal statements (DECISIONS.md 698)", () => {
  const row = (output: (typeof PORTAL_OUTPUTS)[number], heldForUs = false) => ({ runId: RUN_ID, output, sealed: heldForUs ? HELD_PLACEHOLDER : Buffer.from([1, 2, 3]), madeAt: "2026-11-24T09:00:00.000Z", expiresAt: "2026-12-01T09:00:00.000Z", heldForUs });

  test("all three rows and the flag in one transaction, each only while the run is a released, unheld automatic run", async () => {
    const pool = new FakePool();
    await new PostgresWorkerStore(pool).savePortalDocuments(RUN_ID, [row("executive_summary", true), row("full_health_check"), row("ceo_one_pager")], { documents: 3, held: 1 });
    const texts = pool.statements.map((s) => s.text);
    assert.deepEqual(texts, ["BEGIN", PORTAL_DOCUMENT_SQL, PORTAL_DOCUMENT_SQL, PORTAL_DOCUMENT_SQL, RAISE_FLAG_SQL, "COMMIT"]);
    assert.match(PORTAL_DOCUMENT_SQL, /r\."status" = 'released' AND r\."reviewMode" = 'automatic' AND r\."heldReason" IS NULL/);
    assert.match(PORTAL_DOCUMENT_SQL, /ON CONFLICT \("runId", "output"\) DO UPDATE/);
    assert.ok(RAISE_FLAG_SQL.endsWith(FLAG_CONFLICT));
    assert.deepEqual(pool.statements[4]!.values, [RUN_ID, "documents_held", "2026-11-24T09:00:00.000Z", JSON.stringify({ documents: 3, held: 1 })]);
    assert.ok((pool.statements[1]!.values[2] as Buffer).equals(HELD_PLACEHOLDER));
    assert.equal(pool.statements[1]!.values[5], true);
  });

  test("a run that isn't released any more writes nothing: the transaction rolls back", async () => {
    const pool = new FakePool();
    pool.handlers.push((text) => (text === PORTAL_DOCUMENT_SQL ? { rows: [], rowCount: 0 } : undefined));
    await assert.rejects(new PostgresWorkerStore(pool).savePortalDocuments(RUN_ID, [row("executive_summary")], null), /isn't a released, unheld automatic run/);
    assert.deepEqual(pool.statements.map((s) => s.text), ["BEGIN", PORTAL_DOCUMENT_SQL, "ROLLBACK"]);
  });

  test("the clean-up compares in UTC, and a console without the table reads as none", async () => {
    assert.match(PORTAL_EXPIRED_SQL, /"expiresAt" < \(now\(\) AT TIME ZONE 'UTC'\)/);
    const pool = new FakePool();
    pool.handlers.push((text) => (text === PORTAL_EXPIRED_SQL ? { rows: [], rowCount: 2 } : undefined));
    assert.equal(await new PostgresWorkerStore(pool).deleteExpiredPortalDocuments("2026-11-24T09:00:00.000Z"), 2);
    const missing = new FakePool();
    missing.handlers.push(() => {
      throw Object.assign(new Error("relation does not exist"), { code: "42P01" });
    });
    assert.equal(await new PostgresWorkerStore(missing).deleteExpiredPortalDocuments("2026-11-24T09:00:00.000Z"), null);
  });

  test("the migration's CHECKs are the ones the store and the memory store keep, where the console's checkout is beside this one", () => {
    // As release.test.ts finds them: from a worktree, or from the main checkout.
    const name = "20261212_health_check_portal_documents.sql";
    const file = [join(process.cwd(), "..", "..", "hc-console-merge", "migrations", name), join(process.cwd(), "..", "..", "worktrees", "hc-console-merge", "migrations", name)].find((f) => existsSync(f));
    if (!file) return;
    const sql = readFileSync(file, "utf8");
    assert.ok(sql.includes(`CHECK ("output" IN ('executive_summary', 'full_health_check', 'ceo_one_pager'))`));
    assert.ok(sql.includes(`CHECK (octet_length("sealed") BETWEEN 1 AND ${SEALED_MAX_BYTES})`));
    assert.ok(sql.includes(`CHECK ("expiresAt" > "madeAt" AND "expiresAt" <= "madeAt" + interval '7 days')`));
    assert.ok(sql.includes(`WHERE NOT d."heldForUs"`));
  });
});

describe("the hold, as the Postgres store reads it for a documents job (DECISIONS.md 698)", () => {
  const row = (reviewMode: string | null) => {
    const r: Record<string, unknown> = { ...consoleRun({ status: "released" }), reviewMode, analysisFrom: "2024-10-31", analysisTo: "2026-10-31", connectionName: "synthetic-co", preparedBy: null };
    delete r.analysisWindow;
    delete r.steps;
    return r;
  };
  const load = async (reviewMode: string | null, heldReason: string | null) => {
    const pool = new FakePool();
    pool.handlers.push((text) => {
      if (text === HELD_REASON_SQL) return { rows: [{ heldReason }], rowCount: 1 };
      if (text.includes(`FROM "HealthCheckRun" WHERE`)) return { rows: [row(reviewMode)], rowCount: 1 };
      return { rows: [], rowCount: 0 };
    });
    const job = (await new PostgresWorkerStore(pool).loadDocumentsJob(RUN_ID))!;
    return { job, asked: pool.statements.some((s) => s.text === HELD_REASON_SQL) };
  };
  test("an automatic run's hold is read, and its absence reads as none", async () => {
    assert.equal((await load("automatic", "score_moved")).job.heldReason, "score_moved");
    assert.equal((await load("automatic", null)).job.heldReason, null);
  });
  test("a consultant-led run's reads are as they were: no hold asked for", async () => {
    const { job, asked } = await load(null, "score_moved");
    assert.equal(asked, false);
    assert.equal("heldReason" in job, false);
  });
});

describe("a flag raised again (DECISIONS.md 693, 698)", () => {
  test("every flag statement leaves an open flag as it is and opens a cleared one again, unposted, with the new counts", () => {
    for (const sql of [RAISE_FLAG_SQL, HOLD_RUN_SQL, FAILURE_FLAG_SQL]) {
      assert.ok(sql.includes(FLAG_CONFLICT), sql.slice(0, 80));
      assert.doesNotMatch(sql, /DO NOTHING/);
    }
    assert.match(FLAG_CONFLICT, /^ON CONFLICT \("runId", "kind"\) DO UPDATE SET "raisedAt" = EXCLUDED\."raisedAt", "counts" = EXCLUDED\."counts", "postedAt" = NULL, "clearedBy" = NULL, "clearedAt" = NULL WHERE "HealthCheckRunFlag"\."clearedAt" IS NOT NULL$/);
    // Both clear columns go null together, which the console's cleared check asks.
    assert.ok(passesCheck(CONSOLE_CHECKS.flagCleared, { clearedBy: null, clearedAt: null }));
  });

  test("the memory store the same: a redelivery adds nothing to an open flag; a cleared one is opened again", async () => {
    const setup = gateDeps({}, { render: fakeRender({ repairs: "held for us", masks: { executive: 11, full: 0, ceo: 0, microsite: 0 } }, "<p>Fine.</p>") });
    const store = setup.deps.store;
    await processDocumentsJob(RUN_ID, setup.deps, { final: true }, "portal");
    const first = structuredClone([...store.flags.values()][0]!);
    setup.deps.renderReleased = fakeRender({ repairs: "held for us", masks: { executive: 12, full: 0, ceo: 0, microsite: 0 } }, "<p>Fine.</p>").render;
    setup.deps.now = () => new Date(Date.parse(first.raisedAt) + 60_000);
    await processDocumentsJob(RUN_ID, setup.deps, { final: true }, "portal");
    assert.deepEqual([...store.flags.values()][0], first);
    store.postAndClearFlag(RUN_ID, "documents_held", "staff@example.com", new Date(Date.parse(first.raisedAt) + 120_000).toISOString());
    setup.deps.now = () => new Date(Date.parse(first.raisedAt) + 180_000);
    await processDocumentsJob(RUN_ID, setup.deps, { final: true }, "portal");
    const reopened = [...store.flags.values()][0]!;
    assert.equal(reopened.clearedAt, null);
    assert.equal(reopened.clearedBy, null);
    assert.equal(reopened.postedAt, null);
    assert.equal(reopened.counts.masks, 12);
    assert.equal(reopened.raisedAt, new Date(Date.parse(first.raisedAt) + 180_000).toISOString());
  });
});

describe("the expired-row clean-up (DECISIONS.md 698)", () => {
  const consultant = () => {
    const setup = gateDeps({ reviewMode: "consultant" });
    const input = setup.deps.store.documents.get(RUN_ID)!.input;
    input.state = "queued";
    // No Drive: a make that gets past the clean-up stops at its own first gate, which says so.
    setup.deps.drive = null;
    return setup;
  };
  test("off with the switch off: the table is never named", async () => {
    const setup = consultant();
    setup.deps.portalDocuments = { on: false, key: KEY, keyProblem: null };
    let called = 0;
    setup.deps.store.deleteExpiredPortalDocuments = async () => (called++, 0);
    await processDocumentsJob(RUN_ID, setup.deps, { final: true });
    assert.equal(called, 0);
  });
  for (const [what, err] of [
    ["a missing table", Object.assign(new Error("relation does not exist"), { code: "42P01" })],
    ["a missing right", Object.assign(new Error("permission denied"), { code: "42501" })],
    ["a lock timeout", Object.assign(new Error("canceling statement due to lock timeout"), { code: "55P03" })],
    ["a statement timeout", Object.assign(new Error("canceling statement due to statement timeout"), { code: "57014" })],
  ] as const) {
    test(`${what} never fails or holds up a consultant's make`, async () => {
      const setup = consultant();
      setup.deps.store.deleteExpiredPortalDocuments = async () => Promise.reject(err);
      // The make reaches its own gate (here, no Drive): the clean-up's error never became the job's outcome.
      const outcome = await processDocumentsJob(RUN_ID, setup.deps, { final: true });
      assert.equal(outcome.outcome, "failed");
      assert.match(outcome.detail, /^Google Drive isn't set up on the worker/);
      assert.ok(setup.deps.lines.some((l) => l.startsWith("Expired portal documents couldn't be cleared")));
    });
  }
  test("the Postgres store's clean-up runs under its own short limits, in one transaction", async () => {
    const pool = new FakePool();
    await new PostgresWorkerStore(pool).deleteExpiredPortalDocuments("2026-11-24T09:00:00.000Z");
    assert.deepEqual(pool.statements.map((s) => s.text), ["BEGIN", PORTAL_SWEEP_LOCK_TIMEOUT_SQL, PORTAL_SWEEP_STATEMENT_TIMEOUT_SQL, PORTAL_EXPIRED_SQL, "COMMIT"]);
    assert.equal(PORTAL_SWEEP_LOCK_TIMEOUT_SQL, "SET LOCAL lock_timeout = '2s'");
    assert.equal(PORTAL_SWEEP_STATEMENT_TIMEOUT_SQL, "SET LOCAL statement_timeout = '5s'");
  });
});

describe("Sales Cloud and Service Cloud, automatic, end to end: the engine's own words hold nothing (DECISIONS.md 698)", async () => {
  const deps = await releasedAutomaticRun([...consoleRun().modules, SERVICE_MODULE]);
  const pages: Record<string, string> = {};
  const made: { last?: ReleasedDocuments } = {};
  portalDeps(deps, pages, made);
  deps.store.addDocumentsJob(inputOf(deps.store));
  const outcome = await processDocumentsJob(RUN_ID, deps, { final: true }, "portal");

  test("all three are sealed, none held, no flag", () => {
    assert.equal(outcome.outcome, "done", outcome.detail);
    assert.deepEqual(rowsOf(deps.store).map((r) => r?.heldForUs), [false, false, false]);
    assert.deepEqual(deps.store.flagsOf(RUN_ID), []);
  });
  test("though the pages hold the engine's own words the old check held them for", () => {
    const text = readerText(pages.full!);
    assert.match(text, /by hand|reviewed/i);
  });
  test("an automatic run's pages say none of a consultant-led run's next steps, and its own instead", () => {
    for (const doc of ["executive", "full", "ceo"]) {
      const text = readerText(pages[doc]!);
      for (const said of [CONSULTANT_NEXT_STEPS.first.title, readerText(CONSULTANT_NEXT_STEPS.first.text), CONSULTANT_NEXT_STEPS.closing, "Black Cloud can deliver this plan", readerText(CONSULTANT_NEXT_STEPS.recheck), readerText(CONSULTANT_NEXT_STEPS.recheckFull), "Prepared by"]) assert.ok(!text.includes(said), `${doc}: ${said}`);
      assert.ok(text.includes(AUTOMATIC_NEXT_STEPS.recheck), doc);
    }
    for (const doc of ["executive", "full"]) assert.ok(readerText(pages[doc]!).includes(AUTOMATIC_NEXT_STEPS.first.title), doc);
  });
  test("a section in the engine's own words is never read for Claude's, whatever it says", async () => {
    deps.store.addDocumentsJob(
      inputOf(deps.store, (i) => {
        const section = i.sections.find((s) => s.output === "executive_summary")!;
        const v = i.versions.find((x) => x.id === section.currentVersionId)!;
        assert.equal(v.source, "engine");
        Object.assign(v, { body: `Check this by hand: most knowledge articles haven't been reviewed in a year.\n\n${v.body}` });
      }),
    );
    const again = await processDocumentsJob(RUN_ID, deps, { final: true }, "portal");
    assert.equal(again.outcome, "done", again.detail);
    assert.deepEqual(rowsOf(deps.store).map((r) => r?.heldForUs), [false, false, false]);
  });
});

describe("a consultant-led run's documents keep their words (DECISIONS.md 698)", async () => {
  const { dir } = await runFromRecording({ answers: loadAnswers() });
  const { input, pseudonyms } = loadRun(dir);
  const report = buildReport({ ...input, preparedBy: null });
  const pages = { executive: readerText(executiveHtml(report, pseudonyms)), full: readerText(fullReportHtml(report, pseudonyms)), ceo: readerText(onePagerHtml(report, pseudonyms)) };
  test("its next steps walk through it together, with the consultant's closing line and the 90-day promise", () => {
    for (const doc of ["executive", "full"] as const) {
      assert.ok(pages[doc].includes(CONSULTANT_NEXT_STEPS.first.title), doc);
      assert.ok(pages[doc].includes(readerText(CONSULTANT_NEXT_STEPS.first.text)), doc);
      assert.ok(pages[doc].includes(CONSULTANT_NEXT_STEPS.closing), doc);
    }
    assert.ok(pages.executive.includes(readerText(CONSULTANT_NEXT_STEPS.recheck)));
    assert.ok(pages.full.includes(readerText(CONSULTANT_NEXT_STEPS.recheckFull)));
    assert.ok(pages.ceo.includes(readerText(CONSULTANT_NEXT_STEPS.recheck)));
  });
  test("its one-pager says Prepared by Black Cloud with nobody named", () => assert.ok(pages.ceo.includes("Prepared by Black Cloud")));
});
