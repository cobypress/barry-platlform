import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { buildDigest } from "../build.ts";
import { marksOf, TEXT_SLOTS } from "../checked.ts";
import { SALES_CLOUD_MODULES } from "../../domains/index.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { buildReport, READINESS_CAVEAT, readinessOpening, type BuiltReport, type ReportInput } from "../../narrative/sections.ts";
import { extractFigures } from "../../numbercheck/extract.ts";
import { loadRun } from "../../render/render-run.ts";
import { MASK } from "../../render/repair.ts";
import { DIGEST_KEYS, RunDigest } from "../../schemas/digest.ts";
import { tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { AS_OF, CONNECTED_USER } from "../../testing/synthetic-org.ts";
import { READINESS_CODE_PATHS, STRUCTURAL_KEYS } from "../build.ts";
import { assertSameAsWithout, automatic, base, calibration, check, ENGINE, MADE_AT, scenario, SCENARIOS, withoutReadiness } from "./readiness-scenarios.ts";

// The readiness caveat in the digest (KAN-95, DECISIONS.md 762): the raw notes
// say under the readiness opening that each verdict is decided by code and that
// there's no money figure. The digest serves that same sentence, from the one
// constant, as `readiness.caveat`, marked as ours by the scanned engine texts,
// settled with the block's other texts and never in a count or a hold.

/** The raw notes' AI readiness section, as the documents print it. */
function rawNotesReadiness(report: BuiltReport): string[] {
  const out = report.outputs.raw_notes;
  const section = out.sections.find((s) => s.key === "ai_readiness")!;
  const body = out.versions.find((v) => v.id === section.currentVersionId)!.body;
  return body.split("\n\n");
}

/** A value the org chose equal to the caveat (a segment key), so the run can't vouch for it as ours (DECISIONS.md 693). */
function hostile(input: ReportInput): ReportInput {
  const at = input.metrics.findIndex((m) => m.status === "measured" && !m.id.startsWith("ai."));
  return {
    ...input,
    metrics: input.metrics.map((m, i) => (i === at && m.status === "measured" ? { ...m, segments: [...m.segments, { dimension: "Stage", key: READINESS_CAVEAT, n: 0, value: null, notMeasurableReason: null }] } : m)),
  };
}

const run = (overrides: Partial<Parameters<typeof runHealthCheck>[0]>) =>
  runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), answers: loadAnswers(), modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport: new Simulator(), connectedUser: CONNECTED_USER, log: () => {}, ...overrides });

describe("the caveat is the documents' sentence, from one place (DECISIONS.md 762)", () => {
  test("the raw notes print it under the opening, before the pillars, and the digest serves exactly that text", () => {
    const report = buildReport(automatic.input);
    const paragraphs = rawNotesReadiness(report);
    assert.equal(paragraphs[0], readinessOpening(report.readiness));
    assert.equal(paragraphs[1], READINESS_CAVEAT);
    const d = check(automatic.input).digest!;
    assert.equal(d.readiness!.caveat, paragraphs[1]);
  });

  test("it holds no number, so there is nothing for it to trace", () => {
    assert.deepEqual(extractFigures(READINESS_CAVEAT).figures.filter((f) => /\d/.test(f.raw)), []);
  });

  test("its words are written once in the engine's source, and the digest builder imports them", () => {
    const src = fileURLToPath(new URL("../../", import.meta.url));
    const files = (dir: string): string[] => readdirSync(dir, { withFileTypes: true }).flatMap((e) => (e.isDirectory() ? (e.name === "__tests__" ? [] : files(join(dir, e.name))) : e.name.endsWith(".ts") ? [join(dir, e.name)] : []));
    const holding = files(src).filter((f) => readFileSync(f, "utf8").includes("decided by code from the checks and figures named with it"));
    assert.deepEqual(holding.map((f) => f.slice(src.length)), ["narrative/sections.ts"]);
    assert.match(readFileSync(join(src, "digest/build.ts"), "utf8"), /caveat: READINESS_CAVEAT,/);
  });

  test("it is a text to the guard and the checks, never a code skipped by key or path", () => {
    assert.ok(DIGEST_KEYS.has("caveat"));
    assert.equal(STRUCTURAL_KEYS.has("caveat"), false);
    assert.equal(READINESS_CODE_PATHS.has("readiness.caveat"), false);
    assert.ok(TEXT_SLOTS["readiness.caveat"]);
  });

  test("the schema refuses marks that don't fit it, and marks with no caveat", () => {
    const d = check(automatic.input).digest!;
    const r = d.readiness!;
    assert.equal(RunDigest.safeParse({ ...d, readiness: { ...r, orgWords: { ...r.orgWords, caveat: { orgWordsMarked: true, orgWords: [{ start: 0, end: READINESS_CAVEAT.length + 1 }] } } } }).success, false);
    const { caveat: _c, ...bare } = r;
    assert.equal(RunDigest.safeParse({ ...d, readiness: bare }).success, false, "marks left beside no caveat");
    const { caveat: _w, ...noMarks } = r.orgWords!;
    assert.equal(RunDigest.safeParse({ ...d, readiness: { ...bare, orgWords: noMarks } }).success, true, "an older digest, with no caveat, parses");
  });
});

describe("what each product's digest carries (DECISIONS.md 762)", () => {
  const products: Array<[string, Partial<Parameters<typeof runHealthCheck>[0]>]> = [
    ["a consultant-led check", {}],
    ["an automatic check", { calibration, answers: [], reviewMode: "automatic" }],
    ["a monthly pulse", { product: "pulse" }],
    ["a pulse through an automatic check's gate", { calibration, answers: [], product: "pulse", gated: true }],
    ["a set-up review", { scope: "essentials" }],
    ["an automatic set-up review", { calibration, answers: [], reviewMode: "automatic", scope: "essentials" }],
  ];
  for (const [name, options] of products) {
    test(`${name}: the caveat, marked ours, as its raw notes print it`, async () => {
      const l = loadRun((await run(options)).dir);
      const report = buildReport(l.input);
      const d = buildDigest({ report, pseudonyms: l.pseudonyms, inventory: null, madeAt: MADE_AT });
      assert.deepEqual([d.readiness!.caveat, d.readiness!.orgWords?.caveat], [READINESS_CAVEAT, ENGINE]);
      assert.equal(rawNotesReadiness(report)[1], d.readiness!.caveat);
    });
  }

  test("a report without readiness: no readiness key, so no caveat", () => {
    const { readiness: _r, ...rest } = buildReport(automatic.input);
    const d = buildDigest({ report: rest as BuiltReport, pseudonyms: automatic.pseudonyms, inventory: null, madeAt: MADE_AT });
    assert.ok(!("readiness" in d));
    assert.ok(!JSON.stringify(d).includes(READINESS_CAVEAT));
    assert.ok(!JSON.stringify(withoutReadiness(automatic.input).digest).includes(READINESS_CAVEAT));
  });
});

describe("the caveat never moves a count or a hold (DECISIONS.md 742, 762)", () => {
  test("served on an automatic run: marked, nothing withheld", () => {
    const c = check(base);
    const t = { path: "readiness.caveat", at: [], text: c.digest!.readiness!.caveat! };
    assert.deepEqual([t.text, marksOf(c.digest!, t)], [READINESS_CAVEAT, ENGINE]);
    assert.equal(c.readinessWithheld, 0);
  });

  test("on every generated scenario, with the caveat as made and with it unknown: the same hold, masks, unmatched and unmarked as without readiness", () => {
    for (let s = 0; s < SCENARIOS; s++) {
      const input = scenario(s).input;
      const made = check(input);
      assertSameAsWithout(made, withoutReadiness(input), `scenario ${s}`);
      assert.equal(made.digest!.readiness!.caveat, READINESS_CAVEAT, `scenario ${s}`);
      const unknown = check(hostile(input));
      assertSameAsWithout(unknown, withoutReadiness(hostile(input)), `scenario ${s}, caveat unknown`);
      // Withheld whole, marked ours, and counted only apart.
      assert.deepEqual([unknown.digest!.readiness!.caveat, unknown.digest!.readiness!.orgWords?.caveat], [MASK, ENGINE], `scenario ${s}`);
      assert.equal(unknown.readinessWithheld, made.readinessWithheld + 1, `scenario ${s}`);
      assert.deepEqual({ masks: unknown.masks, unmatched: unknown.unmatched, unmarked: unknown.unmarked, held: unknown.held }, { masks: made.masks, unmatched: made.unmatched, unmarked: made.unmarked, held: made.held }, `scenario ${s}`);
    }
  });
});
