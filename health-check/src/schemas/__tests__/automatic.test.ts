import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { assertSectionsFitRun, AUTOMATIC_NOT_ALL_CHECKED, Calibration, calibrationConfirmation, canRelease, CHECKED_OUTSIDE_AUTOMATIC, Release, ReportSection, Run, SectionsOfRun } from "../index.ts";
import { loadCalibration } from "../../testing/replay.ts";
import { consoleRun } from "../../worker/__tests__/helpers.ts";

// The shapes for automatic runs (DECISIONS.md 688): each has a default that
// every row and file stored before it reads as, and nothing in them lets an
// automatic run be released yet, or names a person who didn't act.

const at = "2026-11-23T10:00:00.000Z";
const section = { id: "s1", runId: "r1", output: "executive_summary", key: "verdict", title: "Verdict", order: 1, status: "draft", currentVersionId: null, findingIds: [], metricIds: [], manualStepIds: [], approval: null } as const;
const approval = { approvedBy: "consultant@example.com", approvedAt: at };

describe("a run's review mode", () => {
  test("a run stored before it reads as consultant-led, and both modes parse", () => {
    const { reviewMode: _m, ...stored } = consoleRun();
    assert.ok(!("reviewMode" in stored));
    assert.equal(Run.parse(stored).reviewMode, "consultant");
    assert.equal(Run.parse({ ...stored, reviewMode: "automatic" }).reviewMode, "automatic");
    assert.ok(!Run.safeParse({ ...stored, reviewMode: "reviewed" }).success);
    assert.ok(!Run.safeParse({ ...stored, reviewMode: null }).success, "null is read as absent by the store, never by the schema");
  });
});

describe("a checked section", () => {
  test("every status stored before it still parses, as before", () => {
    for (const status of ["draft", "needs_input", "in_review"]) assert.ok(ReportSection.parse({ ...section, status }));
    assert.ok(ReportSection.parse({ ...section, status: "approved", approval }));
  });

  test("passed the machine checks with no approver, and is refused if it names one", () => {
    assert.equal(ReportSection.parse({ ...section, status: "checked" }).approval, null);
    const named = ReportSection.safeParse({ ...section, status: "checked", approval });
    assert.ok(!named.success);
    assert.match(named.error.issues[0]!.message, /approved by no one/);
  });

  test("is refused on a consultant-led run, wherever the run and its sections are read together", () => {
    const checked = ReportSection.parse({ ...section, status: "checked" });
    const approved = ReportSection.parse({ ...section, key: "scorecard", status: "approved", approval });
    assert.ok(SectionsOfRun.safeParse({ reviewMode: "automatic", sections: [checked] }).success);
    assert.ok(SectionsOfRun.safeParse({ reviewMode: "consultant", sections: [approved] }).success);
    const refused = SectionsOfRun.safeParse({ reviewMode: "consultant", sections: [approved, checked] });
    assert.ok(!refused.success);
    assert.equal(refused.error.issues[0]!.message, CHECKED_OUTSIDE_AUTOMATIC);
    assert.throws(() => assertSectionsFitRun("consultant", [checked]), new RegExp(CHECKED_OUTSIDE_AUTOMATIC));
    assert.doesNotThrow(() => assertSectionsFitRun("automatic", [checked]));
  });
});

describe("a release", () => {
  const row = { id: "rel1", runId: "r1", output: "executive_summary", sectionIds: ["s1"], releasedAt: at };

  test("a row stored before the kind is a person's, and still needs their name", () => {
    assert.deepEqual(Release.parse({ ...row, releasedBy: "consultant@example.com" }), { ...row, kind: "person", releasedBy: "consultant@example.com" });
    assert.ok(!Release.safeParse(row).success);
    assert.ok(!Release.safeParse({ ...row, kind: "person", releasedBy: null }).success);
  });

  test("an automatic release names no one, and is refused if it names a person", () => {
    assert.equal(Release.parse({ ...row, kind: "automatic" }).releasedBy, null);
    assert.equal(Release.parse({ ...row, kind: "automatic", releasedBy: null }).releasedBy, null);
    assert.ok(!Release.safeParse({ ...row, kind: "automatic", releasedBy: "consultant@example.com" }).success);
    assert.ok(!Release.safeParse({ ...row, kind: "machine" }).success);
  });
});

describe("canRelease", () => {
  const approved = ReportSection.parse({ ...section, status: "approved", approval });
  const checked = ReportSection.parse({ ...section, status: "checked" });

  test("a consultant-led run: exactly rule 10, every section approved with who and when", () => {
    assert.deepEqual(canRelease([approved], "consultant"), { ok: true });
    assert.deepEqual(canRelease([approved, { ...checked, key: "scorecard" }], "consultant"), { ok: false, blockedBy: ["scorecard"] });
    assert.deepEqual(canRelease([], "consultant"), { ok: false, blockedBy: ["no sections"] });
  });

  test("an automatic run is released only when every section is checked with no approver: an approved section, any other status, or none refuses (DECISIONS.md 693)", () => {
    assert.deepEqual(canRelease([checked, { ...checked, key: "scorecard" }], "automatic"), { ok: true });
    for (const sections of [[approved], [approved, checked], [{ ...checked, status: "in_review" as const }], [{ ...checked, status: "needs_input" as const }], [{ ...checked, status: "draft" as const }]]) {
      const result = canRelease(sections, "automatic");
      assert.equal(result.ok, false);
      assert.equal(!result.ok && result.reason, AUTOMATIC_NOT_ALL_CHECKED);
    }
    // A checked section that somehow names an approver is refused here too, whatever its parse said.
    assert.equal(canRelease([{ ...checked, approval }], "automatic").ok, false);
    assert.deepEqual(canRelease([], "automatic"), { ok: false, blockedBy: ["no sections"] });
  });

  test("a consultant-led run still refuses a checked section, alone or beside approved ones", () => {
    assert.deepEqual(canRelease([checked], "consultant"), { ok: false, blockedBy: [checked.key] });
    assert.equal(canRelease([approved, checked], "consultant").ok, false);
  });
});

describe("a calibration's source", () => {
  test("the fixture, written before it, reads as confirmed, and both sources parse", () => {
    const raw = JSON.parse(readFileSync("fixtures/synthetic-org/calibration.json", "utf8")) as Record<string, unknown>;
    assert.ok(!("source" in raw));
    assert.equal(Calibration.parse(raw).source, "confirmed");
    assert.equal(Calibration.parse({ ...raw, source: "org_settings" }).source, "org_settings");
    assert.ok(!Calibration.safeParse({ ...raw, source: "guessed" }).success);
  });

  test("only a confirmed one is printed as a person's confirmation", () => {
    const c = loadCalibration();
    assert.equal(calibrationConfirmation(c), `Calibration confirmed by ${c.confirmedBy} on ${c.confirmedAt.slice(0, 10)}`);
    const read = calibrationConfirmation({ ...c, source: "org_settings" });
    assert.equal(read, `Calibration read from the org's own settings on ${c.confirmedAt.slice(0, 10)}, with no person's confirmation`);
    assert.ok(!read.includes(c.confirmedBy));
  });
});
