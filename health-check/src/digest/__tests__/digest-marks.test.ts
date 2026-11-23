import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { z } from "zod";
import { checkedDigest, digestTextsAt, marksOf, STRUCTURAL_PATHS, TEXT_SLOTS, type CheckedDigest } from "../checked.ts";
import { metricMarks, rewordFinding, rewordMetric } from "../../engine/automatic-words.ts";
import { org, said, withOrgWords } from "../../engine/org-words.ts";
import { automaticCalibration } from "../../connect/auto-calibration.ts";
import { stagesOf } from "../../testing/automatic.ts";
import { loadRecording, ReplayTransport } from "../../testing/replay.ts";
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
  // By full path, as the checked digest skips them: a new free-text field under a structural key's name is text (the reviewer's N10).
  if (STRUCTURAL_PATHS.has(path) || key === "orgWords") return [];
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

  test("a metric whose label has no marks is left out whole, its value with it, and counted as a mask; the rest is served", () => {
    const id = loaded.input.metrics[0]!.id;
    const c = check(withMetric((m) => ({ ...m, label: `Counted by ${unknownWords}`, orgWords: undefined })));
    assert.ok(!c.digest!.metrics.some((m) => m.id === id));
    assert.ok(!JSON.stringify(c.digest).includes(unknownWords));
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

describe("the marks machinery's edges (fix round 2)", () => {
  test("a text said recorded as unknown stays unknown, even where the engine's source writes the same words (the reviewer's N8)", () => {
    assert.ok(metricMarks("Win rate"), "the engine writes this label whole");
    withOrgWords(() => {
      // The same words made twice with different marks: said can't vouch for them.
      said`${org("Win")} rate`;
      said`Win ${org("rate")}`;
      assert.equal(metricMarks("Win rate"), null);
    });
  });

  test("a value the org chose is never read as the engine's, however the source writes the same words", () => {
    assert.ok(metricMarks("Win rate"));
    assert.equal(metricMarks("Win rate", new Set(["Win rate"])), null);
  });

  test("unknown marks stay unknown through the rewording (the reviewer's N5)", () => {
    const m = loaded.input.metrics.find((x) => x.status === "measured")!;
    const reworded = rewordMetric({ ...m, label: "The calibration needs at least two ordered open stages." }, { assumptions: m.assumptions.map(() => null), exclusions: m.exclusions.map(() => null) });
    assert.equal(reworded.label, "We need at least two open stages in a known order.");
    assert.equal(reworded.orgWords?.label, undefined);
    assert.ok((reworded.orgWords?.assumptions ?? []).every((x) => x === null));
    const f = loaded.input.findings.find((x) => x.status === "not_measurable")!;
    const unmarked = rewordFinding({ ...f, notMeasurableReason: "No targets were given in the calibration.", orgWords: undefined });
    assert.equal(unmarked.notMeasurableReason, "You haven't told us your sales targets yet.");
    assert.equal(unmarked.orgWords?.notMeasurableReason, undefined);
  });

  test("a text at a path with no place for marks is unknown (the reviewer's N6)", () => {
    const d = check(loaded.input).digest!;
    assert.equal(marksOf(d, { path: "findings[].novelText", at: [0], text: "Something new" }), null);
    assert.equal(marksOf(d, { path: "plan.phases[].key", at: [0], text: "fix" }), null);
  });
});

describe("adversarial stage names, end to end (the reviewer's probe E)", async () => {
  // The synthetic org with four stages renamed to what could pass for our words or a figure; the same run with the names as they were.
  const SWAPS: Array<[string, string]> = [["Negotiation", "2024"], ["Legal Review", "Win rate"], ["Discovery", "Disc (detail withheld)"], ["Proposal", "<b>Prop</b>"]];
  const swapped = loadRecording();
  const esc = (s: string) => JSON.stringify(s).slice(1, -1);
  for (const e of swapped.exchanges) for (const [a, b] of SWAPS) e.text = e.text.split(esc(a)).join(esc(b));
  const keyed = structuredClone(swapped);
  for (const e of keyed.exchanges) for (const [a, b] of SWAPS) e.key = e.key.split(encodeURIComponent(a)).join(encodeURIComponent(b));
  const first = await runFromRecording({ transport: new ReplayTransport(structuredClone(swapped)) });
  const built = automaticCalibration(stagesOf(first.dir), { orgId: first.run.orgId, at: "2026-10-31T09:00:00.000Z", currency: "GBP" });
  assert.ok(built.ok);
  const run = await runFromRecording({ calibration: built.calibration, reviewMode: "automatic", transport: new AutomaticReplay(keyed) });
  const adv = loadRun(run.dir);
  const c = check(adv.input);
  const plain = check(loaded.input);
  /** Each text by where it is, owned by its finding's or metric's id, so a metric left out doesn't move the rest. */
  const byPlace = (d: RunDigest) => new Map(digestTextsAt(d).map((t) => {
    const owner = t.path.startsWith("findings") ? d.findings[t.at[0]!]!.id : t.path.startsWith("metrics") ? d.metrics[t.at[0]!]!.id : String(t.at[0] ?? "");
    return [`${t.path}|${owner}|${t.at.slice(1).join(",")}`, t] as const;
  }));
  const was = byPlace(plain.digest!);

  test("every place an org's stage name reaches the customer, it sits inside an org span", () => {
    assert.equal(c.held, null);
    let checked = 0;
    for (const [place, t] of byPlace(c.digest!)) {
      const before = was.get(place)?.text ?? "";
      for (const [, word] of SWAPS) {
        // Where our own text at the same place holds the same characters, the occurrences can't be told apart.
        if (before.includes(word)) continue;
        for (let i = t.text.indexOf(word); i >= 0; i = t.text.indexOf(word, i + 1)) {
          // A date of ours ("2024-10-31") is no stage.
          if (word === "2024" && /^2024-\d\d-\d\d/.test(t.text.slice(i))) continue;
          const marks = marksOf(c.digest!, t);
          assert.ok(marks?.orgWords.some((s) => s.start <= i && s.end >= i + word.length), `${place}: ${word} in ${t.text.slice(0, 140)}`);
          checked++;
        }
      }
    }
    assert.ok(checked >= 2, `only ${checked}: ${JSON.stringify(digestTextsAt(c.digest!).filter((t) => SWAPS.some(([, w]) => t.text.includes(w))).slice(0, 3).map((t) => t.path))}`);
  });

  test("a metric text equal to an org's stage name is never served as ours", () => {
    for (const m of c.digest!.metrics) {
      assert.notEqual(m.label, "Win rate");
    }
  });
});
