import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { cpSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { basename, dirname, join } from "node:path";
import { tmpdir } from "node:os";
import { buildReport, currentBody, PROGRESS as PROGRESS_SOURCE, type BuiltReport, type ReportInput } from "../../narrative/sections.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { scoreDomains } from "../../domains/scoring.ts";
import { technicalHealthModule } from "../../domains/technical-health/index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { withEarlier } from "../../testing/earlier-handbook.ts";
import { executiveHtml } from "../../render/executive.ts";
import { fullReportHtml } from "../../render/full-report.ts";
import { onePagerHtml } from "../../render/one-pager.ts";
import { progressPage } from "../../render/progress.ts";
import { renderContext } from "../../render/html.ts";
import { loadRun } from "../../render/render-run.ts";
import { buildMicrositePages } from "../../microsite/pages.ts";
import { buildPulseResult } from "../../pulse/result.ts";
import { checkNumbers, scopeDataset } from "../../numbercheck/index.ts";
import { limitsReadAt, progressFigures, PROGRESS_LISTED, STORAGE_READ_ALIKE, STORAGE_RULE, storageSentences } from "../compare.ts";
import type { Finding, NumberOverride, Run } from "../../schemas/index.ts";

// Storage growth on a re-check (DECISIONS.md 500): the synthetic org's storage
// finding, set beside an earlier one whose figures and reading time we plant.

const { output, run, dir } = await runFromRecording({ answers: loadAnswers() });
const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
const base: ReportInput = { run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps, scores: scoreDomains(run.modules, output!.findings) };
const storageFinding = base.findings.find((f) => f.ruleId === STORAGE_RULE)!;

const T0 = Date.parse("2026-07-14T12:00:00.000Z");
const DAY = 86_400_000;
const at = (days: number): string => new Date(T0 + days * DAY).toISOString();

type Kind = [used: number, max: number];
type Edit = (f: Finding) => Finding | null;
const share = ([used, max]: Kind): number => Math.round((used / max) * 1000) / 10;
/** The storage finding holding these kinds' figures. A kind left null isn't reported. */
const kinds = (data: Kind | null, file: Kind | null): Edit => (f) => ({
  ...f,
  facts: {
    ...(data ? { DataStorageMBUsed: data[0], DataStorageMBMax: data[1], DataStorageMBShare: share(data) } : {}),
    ...(file ? { FileStorageMBUsed: file[0], FileStorageMBMax: file[1], FileStorageMBShare: share(file) } : {}),
  },
});
const notMeasurable: Edit = (f) => ({ ...f, status: "not_measurable", notMeasurableReason: "The org's limits couldn't be read: test.", facts: {} });
const gone: Edit = () => null;
const withStorage = (findings: Finding[], edit: Edit): Finding[] => findings.flatMap((f) => (f.ruleId === STORAGE_RULE ? (edit(f) ?? []) : [f]));
const stamped = (r: Run, finishedAt: string | null, extra: Partial<Run> = {}): Run => ({ ...r, ...extra, steps: r.steps.map((s) => (s.id === "api-limits" ? { ...s, finishedAt } : s)) });

type Side = { edit?: Edit; at?: string | null; asOf?: string; overrides?: NumberOverride[] };
const THEN = kinds([8_940, 10_240], [4_480, 20_480]);
const NOW = kinds([9_410, 10_240], [5_000, 20_480]);

/** A re-check 118 days after its earlier health check, both storage kinds read each time unless an edit says otherwise. */
function recheck(then: Side = {}, now: Side = {}): BuiltReport {
  const earlier: ReportInput = { ...base, run: stamped(run, then.at === undefined ? at(0) : then.at, { asOf: then.asOf ?? at(0) }), findings: withStorage(base.findings, then.edit ?? THEN), overrides: then.overrides ?? [] };
  const findings = withStorage(base.findings, now.edit ?? NOW);
  return buildReport({ ...base, run: stamped(run, now.at === undefined ? at(118) : now.at, { id: `${run.id}-recheck`, baselineRunId: run.id, asOf: now.asOf ?? at(118) }), findings, scores: scoreDomains(run.modules, findings), baseline: earlier });
}

const DATA_118 = "Data storage: 8,940 of 10,240 MB used then, 9,410 of 10,240 MB now, up 470 MB in 118 days (an average of 119 MB every 30 days).";
const FILE_118 = "File storage: 4,480 of 20,480 MB used then, 5,000 of 20,480 MB now, up 520 MB in 118 days (an average of 132 MB every 30 days).";
const NOT_READ_THEN = "Your last health check didn't read storage, so we can't say how it has changed.";
const UNREADABLE_NOW = "Storage couldn't be read this time, so we can't say how it has changed since your last health check.";
const UNREADABLE_THEN = "Storage couldn't be read last time, so we can't say how it has changed since your last health check.";
const NOT_ALIKE = "We can't set the two health checks' storage readings side by side, so we don't compare them.";

const notes = (report: BuiltReport): string => currentBody(report.outputs.raw_notes, "progress");
const deckPage = (report: BuiltReport): string => progressPage(renderContext(pseudonyms, report.input.calibration.currency, false), report);
const micrositePage = (report: BuiltReport): string => buildMicrositePages(report, pseudonyms).pages.find((p) => p.slug === "progress")!.html;
const ENTITIES: Record<string, string> = { "&#39;": "'", "&quot;": '"', "&amp;": "&", "&lt;": "<", "&gt;": ">" };
const decoded = (html: string): string => html.replace(/&#39;|&quot;|&amp;|&lt;|&gt;/g, (e) => ENTITIES[e]!);

describe("storage on a re-check (DECISIONS.md 500)", () => {
  test("both kinds risen over 118 days: one sentence each, with an average every 30 days", () => {
    const report = recheck();
    assert.deepEqual(storageSentences(report.progress!), [DATA_118, FILE_118]);
    assert.ok(notes(report).includes(`${DATA_118}\n${FILE_118}`), notes(report));
  });

  test("a fall and no change have no average, and neither has a rise over 29 days", () => {
    const fall = recheck({}, { edit: kinds([9_410, 10_240], [4_480, 20_480]) });
    assert.deepEqual(storageSentences(fall.progress!), [
      "Data storage: 8,940 of 10,240 MB used then, 9,410 of 10,240 MB now, up 470 MB in 118 days (an average of 119 MB every 30 days).",
      "File storage: 4,480 of 20,480 MB used then, 4,480 of 20,480 MB now, the same as 118 days ago.",
    ]);
    const down = recheck({ edit: kinds([9_410, 10_240], null) }, { edit: kinds([8_940, 10_240], null) });
    assert.deepEqual(storageSentences(down.progress!), ["Data storage: 9,410 of 10,240 MB used then, 8,940 of 10,240 MB now, down 470 MB in 118 days."]);
    const short = recheck({}, { at: at(29) });
    assert.deepEqual(storageSentences(short.progress!), [
      "Data storage: 8,940 of 10,240 MB used then, 9,410 of 10,240 MB now, up 470 MB in 29 days.",
      "File storage: 4,480 of 20,480 MB used then, 5,000 of 20,480 MB now, up 520 MB in 29 days.",
    ]);
  });

  test("exactly 30 days has an average, and one day reads as 1 day", () => {
    const thirty = recheck({}, { at: at(30) });
    assert.equal(storageSentences(thirty.progress!)[0], "Data storage: 8,940 of 10,240 MB used then, 9,410 of 10,240 MB now, up 470 MB in 30 days (an average of 470 MB every 30 days).");
    // A day and a half is one whole day.
    const one = recheck({}, { at: at(1.5) });
    assert.equal(storageSentences(one.progress!)[0], "Data storage: 8,940 of 10,240 MB used then, 9,410 of 10,240 MB now, up 470 MB in 1 day.");
    const same = recheck({}, { at: at(1), edit: THEN });
    assert.equal(storageSentences(same.progress!)[0], "Data storage: 8,940 of 10,240 MB used then, 8,940 of 10,240 MB now, the same as 1 day ago.");
  });

  test("an allowance that changed between the runs is printed as each run read it", () => {
    // The earlier allowance is no longer the org's, so only the earlier reading backs it.
    const report = recheck({ edit: kinds([8_940, 10_000], null) }, { edit: kinds([9_410, 15_360], null) });
    const [sentence] = storageSentences(report.progress!);
    assert.equal(sentence, "Data storage: 8,940 of 10,000 MB used then, 9,410 of 15,360 MB now, up 470 MB in 118 days (an average of 119 MB every 30 days).");
    const section = report.outputs.raw_notes.sections.find((s) => s.key === "progress")!;
    assert.deepEqual(checkNumbers(sentence!, scopeDataset(report.dataset, report.scopes.get(section.id)!)).unmatched, []);
  });

  test("the days are between the two limits readings, not the runs' dates", () => {
    // Resumed late: the run's date is 10 days before its limits were read.
    const report = recheck({ asOf: at(-5) }, { asOf: at(108) });
    assert.deepEqual(storageSentences(report.progress!), [DATA_118, FILE_118]);
  });

  test("a reused checkpoint keeps the first reading's time, so a resumed run is dated by its first connection", async () => {
    const first = await runFromRecording({ answers: loadAnswers() });
    const path = join(first.dir, "run.json");
    // Planted far back, so a step run again would show as a new time.
    writeFileSync(path, JSON.stringify(stamped(JSON.parse(readFileSync(path, "utf8")) as Run, at(0))));
    const resumed = await runFromRecording({ answers: loadAnswers(), outDir: dirname(first.dir), resume: basename(first.dir) });
    assert.equal(resumed.run.steps.find((s) => s.id === "api-limits")?.status, "done");
    assert.equal(limitsReadAt(resumed.run), at(0));
  });

  describe("when they can't be compared, one sentence says why and nothing else about storage", () => {
    const cases: Array<[string, Side, Side, string]> = [
      ["the earlier run has no storage finding", { edit: gone }, {}, NOT_READ_THEN],
      ["the earlier finding wasn't measurable", { edit: notMeasurable }, {}, UNREADABLE_THEN],
      ["this run's finding wasn't measurable", {}, { edit: notMeasurable }, UNREADABLE_NOW],
      ["the earlier finding was made by a rule version not on the list, reading the same figures", { edit: (f) => ({ ...THEN(f)!, ruleVersion: 3 }) }, {}, NOT_ALIKE],
      ["no kind is held by both", { edit: kinds(null, [4_480, 20_480]) }, { edit: kinds([9_410, 10_240], null) }, NOT_ALIKE],
      ["the earlier reading has no time", { at: null }, {}, NOT_ALIKE],
      ["this reading has no time", {}, { at: null }, NOT_ALIKE],
      ["the later reading isn't after the earlier", { at: at(118) }, {}, NOT_ALIKE],
      ["the later reading is before the earlier", { at: at(120) }, {}, NOT_ALIKE],
      ["a used figure isn't a whole number", { edit: kinds([8_940.5, 10_240], null) }, {}, NOT_ALIKE],
      ["an allowance is 0", {}, { edit: kinds([9_410, 0], null) }, NOT_ALIKE],
    ];
    for (const [name, then, now, sentence] of cases) {
      test(name, () => {
        const report = recheck(then, now);
        assert.deepEqual(storageSentences(report.progress!), [sentence]);
        for (const text of [notes(report), decoded(deckPage(report)), decoded(micrositePage(report))]) {
          assert.equal(text.split(sentence).length - 1, 1, text);
          // The storage issue's own title can be listed as still open; nothing else may mention storage.
          assert.doesNotMatch(text.replace(sentence, "").replaceAll(storageFinding.title, ""), /storage/i, text);
        }
      });
    }
  });

  test("a re-check that didn't read storage itself says nothing about it, anywhere", () => {
    // The earlier one passed, so no list names it either.
    const report = recheck({ edit: (f) => ({ ...THEN(f)!, status: "passed" }) }, { edit: gone });
    assert.equal(report.progress!.storage, null);
    assert.deepEqual(storageSentences(report.progress!), []);
    for (const text of [notes(report), deckPage(report), micrositePage(report)]) assert.doesNotMatch(text, /storage/i);
  });

  test("a kind only one run holds isn't mentioned; the shared one is compared", () => {
    const report = recheck({}, { edit: kinds([9_410, 10_240], null) });
    assert.deepEqual(storageSentences(report.progress!), [DATA_118]);
    for (const text of [notes(report), decoded(deckPage(report)), decoded(micrositePage(report))]) assert.doesNotMatch(text, /File storage/);
  });

  test("a consultant's correction to the earlier figure is the one compared", () => {
    const override: NumberOverride = { id: "o-1", runId: run.id, target: { type: "finding", id: storageFinding.id, field: "facts.DataStorageMBUsed" }, previousValue: 8_940, newValue: 9_000, reason: "Corrected from Setup.", overriddenBy: "Synthetic Consultant", overriddenAt: at(1) };
    const report = recheck({ overrides: [override] }, { edit: kinds([9_410, 10_240], null) });
    assert.deepEqual(storageSentences(report.progress!), ["Data storage: 9,000 of 10,240 MB used then, 9,410 of 10,240 MB now, up 410 MB in 118 days (an average of 104 MB every 30 days)."]);
  });

  test("the number check passes the section's storage figures, and catches one changed by hand", () => {
    const report = recheck();
    const section = report.outputs.raw_notes.sections.find((s) => s.key === "progress")!;
    const version = report.outputs.raw_notes.versions.find((v) => v.id === section.currentVersionId)!;
    const storage = `${DATA_118}\n${FILE_118}`;
    assert.ok(version.body.includes(storage));
    // Checked as the section is: against its own scope, nothing wider.
    const scope = scopeDataset(report.dataset, report.scopes.get(section.id)!);
    assert.deepEqual(checkNumbers(storage, scope).unmatched, []);
    assert.equal(checkNumbers(storage, scope).checked, 16, "eight figures a sentence");
    // The sentences add nothing the check can't trace. Some rules' titles in the section's lists carry figures
    // of their own ("12 months") that its scope doesn't hold; they fail the same with or without storage.
    const without = checkNumbers(version.body.replace(storage, ""), scope).unmatched;
    assert.deepEqual(version.numberCheck.unmatched, without.map((u) => `${u.raw}: ${u.reason}`));
    for (const [from, to] of [["8,940 of", "8,950 of"], ["of 10,240 MB used", "of 10,250 MB used"], ["9,410 of", "9,411 of"], ["of 10,240 MB now", "of 10,250 MB now"], ["up 470 MB", "up 480 MB"], ["in 118 days (", "in 117 days ("], ["of 119 MB", "of 129 MB"], ["every 30 days", "every 40 days"]] as const) {
      const altered = storage.replace(from, to);
      assert.notEqual(altered, storage, from);
      assert.equal(checkNumbers(altered, scope).passed, false, `${to} passed`);
    }
  });

  test("the pulse result and the one-pager don't change", () => {
    const report = recheck();
    assert.equal(storageSentences(report.progress!).length, 2);
    const without: BuiltReport = { ...report, progress: { ...report.progress!, storage: null } };
    assert.deepEqual(buildPulseResult(report), buildPulseResult(without));
    assert.equal(onePagerHtml(report, pseudonyms), onePagerHtml(without, pseudonyms));
    assert.doesNotMatch(JSON.stringify(buildPulseResult(report)), /MB|storage readings/);
  });

  test("the storage rule's version is one we know reads storage the same way", () => {
    const rule = technicalHealthModule.rules.find((r) => r.id === STORAGE_RULE);
    assert.ok(rule, `${STORAGE_RULE} is gone: point STORAGE_RULE in src/progress/compare.ts at the rule that reads storage now.`);
    assert.ok(
      STORAGE_READ_ALIKE.includes(rule.version),
      `${STORAGE_RULE} is at version ${rule.version}, which isn't on STORAGE_READ_ALIKE in src/progress/compare.ts. Decide whether version ${rule.version} reads storage as versions ${STORAGE_READ_ALIKE.join(" and ")} do (the same fact keys, used as the allowance less what remains, in MB). If it does, add it to the list. If not, leave it off, and a re-check against an earlier version will say it can't compare them (DECISIONS.md 500).`,
    );
  });
});

describe("storage on a re-check, corrected after review (DECISIONS.md 501 and 502)", () => {
  const HOUR = 1 / 24;
  const section = (report: BuiltReport) => report.outputs.raw_notes.sections.find((s) => s.key === "progress")!;
  const sectionScope = (report: BuiltReport) => scopeDataset(report.dataset, report.scopes.get(section(report).id)!);

  for (const [name, days] of [["5 hours", 5 * HOUR], ["23 hours 59 minutes", (23 * 60 + 59) / (24 * 60)]] as const) {
    test(`readings ${name} apart aren't compared: the side by side sentence and nothing else about storage`, () => {
      const report = recheck({}, { at: at(days) });
      assert.deepEqual(storageSentences(report.progress!), [NOT_ALIKE]);
      for (const text of [notes(report), decoded(deckPage(report)), decoded(micrositePage(report))]) {
        assert.equal(text.split(NOT_ALIKE).length - 1, 1, text);
        assert.doesNotMatch(text.replace(NOT_ALIKE, "").replaceAll(storageFinding.title, ""), /storage|in 0 days|0 days ago/i, text);
      }
    });
  }

  test("each sentence saying why storage isn't compared passes the section's number check", () => {
    // "The two health checks'" reads as a figure: the side by side sentence, now also under a day apart, carries it.
    for (const [then, now, sentence] of [[{ edit: gone }, {}, NOT_READ_THEN], [{ edit: notMeasurable }, {}, UNREADABLE_THEN], [{}, { edit: notMeasurable }, UNREADABLE_NOW], [{}, { at: at(5 * HOUR) }, NOT_ALIKE]] as Array<[Side, Side, string]>) {
      const report = recheck(then, now);
      assert.ok(notes(report).includes(sentence));
      assert.deepEqual(checkNumbers(sentence, sectionScope(report)).unmatched, [], sentence);
      assert.equal(section(report).status, "draft", sentence);
    }
  });

  test("exactly 24 hours apart is 1 day", () => {
    const report = recheck({}, { at: at(1) });
    assert.equal(storageSentences(report.progress!)[0], "Data storage: 8,940 of 10,240 MB used then, 9,410 of 10,240 MB now, up 470 MB in 1 day.");
  });

  const pastNow = "Data storage: 9,000 of 10,240 MB used then, 10,500 MB used now, over the 10,240 MB allowance, up 1,500 MB in 118 days (an average of 381 MB every 30 days).";
  const pastThen = "Data storage: 10,500 MB used then, over the 10,240 MB allowance, 9,000 of 10,240 MB now, down 1,500 MB in 118 days.";
  const pastBoth = "Data storage: 10,500 MB used then, over the 10,240 MB allowance, 10,700 MB used now, over the 10,240 MB allowance, up 200 MB in 118 days (an average of 51 MB every 30 days).";
  const past: Array<[string, Kind, Kind, string, Array<[string, string]>]> = [
    ["now", [9_000, 10_240], [10_500, 10_240], pastNow, [["10,500 MB used now", "10,600 MB used now"], ["over the 10,240 MB allowance", "over the 10,250 MB allowance"]]],
    ["then", [10_500, 10_240], [9_000, 10_240], pastThen, [["10,500 MB used then", "10,400 MB used then"], ["over the 10,240 MB allowance", "over the 10,340 MB allowance"]]],
    ["both", [10_500, 10_240], [10_700, 10_240], pastBoth, [["10,700 MB used now", "10,800 MB used now"], ["10,500 MB used then", "10,600 MB used then"]]],
  ];
  for (const [when, then, now, sentence, alterations] of past) {
    test(`past the allowance ${when}: that side isn't written as X of Y, and every check passes it`, () => {
      const report = recheck({ edit: kinds(then, null) }, { edit: kinds(now, null) });
      assert.deepEqual(storageSentences(report.progress!), [sentence]);
      // The section as a whole passes, so it's a draft, not waiting for input.
      const version = report.outputs.raw_notes.versions.find((v) => v.id === section(report).currentVersionId)!;
      assert.ok(version.body.includes(sentence));
      assert.deepEqual(version.numberCheck.unmatched, []);
      assert.equal(section(report).status, "draft");
      assert.deepEqual(checkNumbers(sentence, sectionScope(report)).unmatched, []);
      assert.deepEqual(checkNumbers(sentence, report.dataset).unmatched, []);
      // The microsite checks every page against the whole dataset and throws on a miss.
      assert.ok(decoded(micrositePage(report)).includes(`<p>${sentence}</p>`));
      for (const [from, to] of alterations) {
        const altered = sentence.replace(from, to);
        assert.notEqual(altered, sentence, from);
        assert.equal(checkNumbers(altered, sectionScope(report)).passed, false, `${to} passed`);
      }
    });
  }

  test("an average that rounds to nothing isn't printed; one that rounds to 1 MB is", () => {
    const slow = recheck({ edit: kinds([8_940, 10_240], null) }, { edit: kinds([8_941, 10_240], null), at: at(400) });
    assert.deepEqual(storageSentences(slow.progress!), ["Data storage: 8,940 of 10,240 MB used then, 8,941 of 10,240 MB now, up 1 MB in 400 days."]);
    const one = recheck({ edit: kinds([8_940, 10_240], null) }, { edit: kinds([8_942, 10_240], null) });
    assert.deepEqual(storageSentences(one.progress!), ["Data storage: 8,940 of 10,240 MB used then, 8,942 of 10,240 MB now, up 2 MB in 118 days (an average of 1 MB every 30 days)."]);
    assert.equal(section(one).status, "draft");
  });

  test("with a storage sentence the deck's lists name four, and the count left to the raw notes is the number checked", () => {
    const report = recheck();
    const p = report.progress!;
    assert.ok(p.remaining.length > PROGRESS_LISTED, "a list longer than it names");
    const rest = p.remaining.length - (PROGRESS_LISTED - 1);
    const page = decoded(deckPage(report));
    const still = page.split("Still open")[1]!.split("</ul>")[0]!;
    assert.equal(still.split("<li>").length - 1, PROGRESS_LISTED, "four titles and the count");
    assert.ok(still.includes(`<li>And ${rest} more in the raw notes.</li>`), still);
    assert.ok(progressFigures(p).includes(rest));
    const scope = scopeDataset(report.dataset, [PROGRESS_SOURCE]);
    assert.ok(scope.figures.some((f) => f.kind === "number" && f.value === rest));
    // Without storage the page names five, as before, and that count is the one checked.
    const without: BuiltReport = { ...report, progress: { ...p, storage: null } };
    const plain = decoded(deckPage(without)).split("Still open")[1]!.split("</ul>")[0]!;
    assert.equal(plain.split("<li>").length - 1, PROGRESS_LISTED + 1);
    assert.ok(plain.includes(`<li>And ${rest - 1} more in the raw notes.</li>`), plain);
    assert.ok(progressFigures(without.progress!).includes(rest - 1));
    // The microsite lists every one.
    assert.equal(decoded(micrositePage(report)).split("<li>").length - 1, p.fixed.length + p.remaining.length + p.added.length + p.unknown.length);
  });
});

describe("storage on a re-check made on disk", () => {
  test("the deck, the microsite and the raw notes print the same sentences", () => {
    const root = mkdtempSync(join(tmpdir(), "hc-storage-"));
    const recheckDir = join(root, "hc-2026-11-09-recheck");
    cpSync(dir, recheckDir, { recursive: true });
    const earlierDir = withEarlier(recheckDir);
    const edit = (folder: string, finishedAt: string, used: number): void => {
      const runPath = join(folder, "run.json");
      writeFileSync(runPath, JSON.stringify(stamped(JSON.parse(readFileSync(runPath, "utf8")) as Run, finishedAt)));
      const findingsPath = join(folder, "findings.json");
      const findings = JSON.parse(readFileSync(findingsPath, "utf8")) as Finding[];
      writeFileSync(findingsPath, JSON.stringify(findings.map((f) => (f.ruleId === STORAGE_RULE ? { ...f, facts: { ...f.facts, DataStorageMBUsed: used } } : f))));
    };
    edit(earlierDir, at(0), 8_470);
    edit(recheckDir, at(118), 8_940);
    const { input, pseudonyms: names } = loadRun(recheckDir);
    const report = buildReport(input);
    const sentences = storageSentences(report.progress!);
    assert.deepEqual(sentences, [
      "Data storage: 8,470 of 10,240 MB used then, 8,940 of 10,240 MB now, up 470 MB in 118 days (an average of 119 MB every 30 days).",
      "File storage: 4,480 of 20,480 MB used then, 4,480 of 20,480 MB now, the same as 118 days ago.",
    ]);
    // The microsite refuses a page whose figures the number check can't trace, so making it is a check too.
    const page = buildMicrositePages(report, names).pages.find((p) => p.slug === "progress")!.html;
    // The decks set both in one paragraph, a space between (DECISIONS.md 501); the microsite and the raw notes keep one a line.
    for (const [where, text] of [["executive", executiveHtml(report, names)], ["full report", fullReportHtml(report, names)]] as const) {
      assert.ok(decoded(text).includes(`<p class="note">${sentences.join(" ")}</p>`), where);
    }
    assert.ok(decoded(page).includes(sentences.map((s) => `<p>${s}</p>`).join("")), "microsite");
    assert.ok(notes(report).includes(sentences.join("\n")), "raw notes");
  });
});
