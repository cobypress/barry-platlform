import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { z } from "zod";
import { ID_MAPS, STRUCTURAL_KEYS } from "../build.ts";
import { checkedDigest, digestTextsAt, marksOf, TEXT_SLOTS, type CheckedDigest } from "../checked.ts";
import { ENGINE_TEXTS_FILE, engineTextsSource, scanEngineTexts } from "../engine-texts-scan.ts";
import { buildReport, type ReportInput } from "../../narrative/sections.ts";
import { loadRun } from "../../render/render-run.ts";
import { MASK } from "../../render/repair.ts";
import { RunDigest } from "../../schemas/digest.ts";
import type { Finding, Metric } from "../../schemas/index.ts";
import { AutomaticReplay, automaticSyntheticCalibration } from "../../testing/automatic.ts";
import { runFromRecording } from "../../testing/harness.ts";

// Rule 10's "words from the customer's org are marked apart from ours", held
// over every text an automatic run's digest serves (DECISIONS.md 693).

const made = await runFromRecording({ calibration: await automaticSyntheticCalibration(), reviewMode: "automatic", transport: new AutomaticReplay() });
const loaded = loadRun(made.dir);
const NOW = "2026-11-24T09:00:00.000Z";
const check = (input: ReportInput, build?: (i: ReportInput) => ReturnType<typeof buildReport>): CheckedDigest => checkedDigest({ input, pseudonyms: loaded.pseudonyms, inventory: null, madeAt: NOW, ...(build ? { build } : {}) });

/** Every string field of a schema, by path with [] for each array, as digestTextsAt names them. Fixed words (enums, literals) aren't text. */
function stringPaths(schema: z.ZodType, path = "", key = ""): string[] {
  if (STRUCTURAL_KEYS.has(key) || ID_MAPS.has(key) || key === "orgWords") return [];
  const def = (schema as unknown as { _zod: { def: Record<string, unknown> & { type: string } } })._zod.def;
  switch (def.type) {
    case "string":
      return [path];
    case "object":
      return Object.entries(def.shape as Record<string, z.ZodType>).flatMap(([k, v]) => stringPaths(v, path ? `${path}.${k}` : k, k));
    case "array":
      return stringPaths(def.element as z.ZodType, `${path}[]`, key);
    case "optional":
    case "nullable":
    case "default":
    case "readonly":
      return stringPaths(def.innerType as z.ZodType, path, key);
    case "union":
      return [...new Set((def.options as z.ZodType[]).flatMap((o) => stringPaths(o, path, key)))];
    case "pipe":
      return stringPaths(def.in as z.ZodType, path, key);
    case "record":
      return stringPaths(def.valueType as z.ZodType, `${path}{}`, key);
    default:
      return [];
  }
}

describe("every text the digest can serve has a place for its marks", () => {
  test("read from the schema itself, so a field added later is held until it's marked", () => {
    const paths = [...new Set(stringPaths(RunDigest))].sort();
    assert.ok(paths.length > 15);
    const missing = paths.filter((p) => !(p in TEXT_SLOTS));
    assert.deepEqual(missing, []);
  });

  test("the engine's own texts are listed as the source writes them: regenerate with node src/digest/engine-texts-scan.ts --write", () => {
    const src = fileURLToPath(new URL("../../", import.meta.url));
    assert.equal(readFileSync(ENGINE_TEXTS_FILE, "utf8"), engineTextsSource(scanEngineTexts(src)));
  });
});

describe("the synthetic org as an automatic run reads it (DECISIONS.md 693)", () => {
  const c = check(loaded.input);

  test("every text it serves is marked, nothing withheld or held: it may be released", () => {
    assert.deepEqual({ masks: c.masks, unmatched: c.unmatched, unmarked: c.unmarked, held: c.held }, { masks: 0, unmatched: 0, unmarked: 0, held: null });
    const texts = digestTextsAt(c.digest!);
    assert.ok(texts.length > 1000);
    for (const t of texts) assert.ok(marksOf(c.digest!, t), t.path);
    // The metrics' marks are the run's own, a stage named as the org's.
    const marked = c.digest!.metrics.flatMap((m) => (m.status === "measured" ? m.orgWords?.exclusions ?? [] : [])).filter((x) => x && x.orgWords.length > 0);
    assert.ok(marked.length > 0);
  });

  test("a metric's own marks are stored only on an automatic run", () => {
    assert.ok(loaded.input.metrics.every((m) => m.orgWords));
  });
});

describe("a text that can't be marked", () => {
  const withMetric = (change: (m: Metric) => Metric): ReportInput => ({ ...loaded.input, metrics: loaded.input.metrics.map((m, i) => (i === 0 ? change(m) : m)) });
  const unknownWords = ["Quokka", "Wombat"].join(" ");

  test("a metric's label with no marks is withheld whole and counted as a mask; the rest is served", () => {
    const c = check(withMetric((m) => ({ ...m, label: `Counted by ${unknownWords}`, orgWords: undefined })));
    assert.equal(c.digest!.metrics[0]!.label, MASK);
    assert.equal(c.masks, 1);
    assert.equal(c.unmarked, 0);
    assert.equal(c.held, null);
  });

  test("a metric's assumption and exclusion with no marks are withheld the same way", () => {
    const c = check(withMetric((m) => ({ ...m, assumptions: [...m.assumptions, `Seen by ${unknownWords}`], exclusions: [...m.exclusions, `Left by ${unknownWords}`], orgWords: undefined })));
    const json = JSON.stringify(c.digest);
    assert.ok(!json.includes(unknownWords));
    assert.ok(c.masks >= 2);
  });

  test("more than ten metric texts withheld is held for us", () => {
    const input: ReportInput = { ...loaded.input, metrics: loaded.input.metrics.map((m, i) => (i < 11 ? { ...m, label: `Counted by ${unknownWords} ${i}`, orgWords: undefined } : m)) };
    assert.equal(check(input).held, "digest_held");
  });

  test("any other text with no marks is held: a finding's value reason", () => {
    const f = loaded.input.findings.find((x) => x.value.kind === "not_sized")!;
    const input: ReportInput = { ...loaded.input, findings: loaded.input.findings.map((x) => (x.id === f.id ? ({ ...x, value: { kind: "not_sized", reason: `Sized by ${unknownWords}` }, orgWords: { ...x.orgWords, valueReason: undefined } } as Finding) : x)) };
    const c = check(input);
    assert.ok(c.unmarked > 0);
    assert.equal(c.held, "digest_held");
  });
});

describe("every text is number-checked, wherever it is (the reviewer's M6)", () => {
  const PLANTED = 987654;
  const without = (i: ReportInput) => {
    const r = buildReport(i);
    r.dataset = { ...r.dataset, figures: r.dataset.figures.filter((f) => f.value !== PLANTED) };
    return r;
  };
  const metric = (change: (m: Metric) => Partial<Metric>): ReportInput => ({ ...loaded.input, metrics: loaded.input.metrics.map((m, i) => (i === 3 ? ({ ...m, ...change(m) } as Metric) : m)) });
  const finding = (change: (f: Finding) => Partial<Finding>): ReportInput => {
    const id = loaded.input.findings.find((f) => f.status === "issue" && f.fixPrompt)!.id;
    return { ...loaded.input, findings: loaded.input.findings.map((f) => (f.id === id ? ({ ...f, ...change(f) } as Finding) : f)) };
  };
  const cases: Array<[string, ReportInput]> = [
    ["a metric's assumption", metric((m) => ({ assumptions: [...m.assumptions, `It took ${PLANTED} steps`] }))],
    ["a metric's exclusion", metric((m) => ({ exclusions: [...m.exclusions, `It took ${PLANTED} steps`] }))],
    ["a metric's label", metric((m) => ({ label: `${m.label} ${PLANTED}` }))],
    ["a planned issue's title, the plan's item", finding((f) => ({ title: `${f.title} ${PLANTED}` }))],
    ["a fix prompt", finding((f) => ({ fixPrompt: `${f.fixPrompt} Step ${PLANTED}.` }))],
  ];
  for (const [name, input] of cases) {
    test(`${name}: a figure the data doesn't hold never reaches the customer`, () => {
      const c = check(input, without);
      const json = JSON.stringify(c.digest);
      assert.ok(c.held !== null || !json.includes(String(PLANTED)), name);
      assert.ok(c.held !== null || c.masks > 0 || !json.includes(String(PLANTED)));
    });
  }
});
