import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { applyOverrides, grouped, refusalLines, refusalReason, type OverrideRefusal } from "../overrides.ts";
import { tokenSource } from "../../privacy/pseudonymiser.ts";
import type { Finding, Metric } from "../../schemas/index.ts";
import { asText, correction, OVERRIDES_GOLDEN, overridable, overridesGolden, REMAKE, verdictsOf, type GoldenCase, type OverridesGolden, type Overridable } from "../../testing/overrides-golden.ts";

// The console judges a correction with a copy of overrides.ts, and both repos
// run every case of fixtures/overrides-golden.json (DECISIONS.md 532, 533). The
// file is read here as parsed from disk, as the console reads its copy.

const text = readFileSync(OVERRIDES_GOLDEN, "utf8");
const golden = JSON.parse(text) as OverridesGolden;
const memory = await overridesGolden();
const STALE = `fixtures/overrides-golden.json is out of date. Make it again from health-check/ with \`${REMAKE}\`, and copy it to the console's docs/health-check/overrides-golden.json`;

const CLOSE = "A finding's wording and a measure's chart or table can't be edited, so a figure we can't carry changes only with a new health check. The console refuses a correction like this before it is saved and holds a release while one stands. If this run is already released its corrections can no longer be changed there, so tell whoever looks after the health check.";

const judged = (c: Pick<GoldenCase, "findings" | "metrics" | "overrides">) => applyOverrides(c.findings, c.metrics, c.overrides);
const linesOf = (refused: OverrideRefusal[]): string[] => (refused.length > 0 ? refusalLines(refused) : []);
const kindOf = (r: OverrideRefusal): string => `${r.findingId === undefined ? "measure" : "finding"}:${r.why.kind}`;

/** What moved between the file on disk and the file made again, named, so a stale file says why. */
function moved(was: OverridesGolden, now: OverridesGolden): string[] {
  const out: string[] = [];
  if (was.source !== now.source) out.push("source");
  const names = new Set([...was.cases.map((c) => c.name), ...now.cases.map((c) => c.name)]);
  for (const name of names) {
    const [a, b] = [was.cases.find((c) => c.name === name), now.cases.find((c) => c.name === name)];
    if (!a || !b) out.push(`case "${name}" ${a ? "is no longer made" : "is new"}`);
    else for (const key of ["findings", "metrics", "overrides", "expected"] as const) if (JSON.stringify(a[key]) !== JSON.stringify(b[key])) out.push(`case "${name}": ${key}`);
  }
  for (const key of ["findings", "metrics"] as const) if (JSON.stringify(was.synthetic[key]) !== JSON.stringify(now.synthetic[key])) out.push(`the synthetic run's ${key}`);
  const verdict = (v: OverridesGolden["synthetic"]["verdicts"][number]): string => `${v.type} ${v.id} ${v.field}`;
  const before = new Map(was.synthetic.verdicts.map((v) => [verdict(v), JSON.stringify(v)]));
  const after = new Map(now.synthetic.verdicts.map((v) => [verdict(v), JSON.stringify(v)]));
  for (const key of new Set([...before.keys(), ...after.keys()])) if (before.get(key) !== after.get(key)) out.push(`verdict ${key}: ${before.get(key) ?? "none"} now ${after.get(key) ?? "none"}`);
  if (out.length === 0 && asText(was) !== asText(now)) out.push("the order of the file's keys or entries");
  return out;
}

describe("the golden overrides file shared with the console (decision 532, item 3)", () => {
  test("is current: made again from the code and the synthetic run, it is the same bytes", () => {
    const remade = asText(memory);
    assert.ok(remade === text, `${STALE}. What moved: ${moved(golden, memory).slice(0, 20).join("; ")}`);
  });

  test("is JSON only: parsed back it is the objects in memory, and judges as they do", () => {
    assert.deepEqual(JSON.parse(asText(memory)), memory, "no undefined, -0 or NaN");
    for (const [parsed, made] of golden.cases.map((c, i) => [c, memory.cases[i]!] as const)) assert.deepEqual(judged(parsed), judged(made), parsed.name);
    assert.deepEqual(verdictsOf(golden.synthetic.findings, golden.synthetic.metrics), verdictsOf(memory.synthetic.findings, memory.synthetic.metrics));
  });

  for (const c of golden.cases) {
    test(`case: ${c.name}`, () => {
      const result = judged(c);
      assert.deepEqual({ findings: result.findings, metrics: result.metrics, refused: result.refused }, { findings: c.expected.findings, metrics: c.expected.metrics, refused: c.expected.refused });
      assert.deepEqual(linesOf(result.refused), c.expected.lines);
    });
  }

  test("every synthetic verdict, each correction alone, agrees with the file", () => {
    const { findings, metrics, verdicts } = golden.synthetic;
    const wrong = verdicts.flatMap((v) => {
      const asRead = overridable(findings, metrics).find((x) => x.type === v.type && x.id === v.id && x.field === v.field)!.asRead;
      const [refused] = applyOverrides(findings, metrics, [correction(v, asRead, v.newValue)]).refused;
      const kind = refused ? refused.why.kind : null;
      return kind === v.refused ? [] : [`${v.type} ${v.id} ${v.field}: ${v.refused} in the file, ${kind} now`];
    });
    assert.deepEqual(wrong, [], STALE);
  });

  test("the verdicts are every figure the console can correct: each finding's facts and sized amount, each measured metric's value", () => {
    const { findings, metrics, verdicts } = golden.synthetic;
    assert.deepEqual(
      verdicts.map(({ type, id, field, newValue }) => ({ type, id, field, newValue })),
      overridable(findings, metrics).map(({ type, id, field, asRead }) => ({ type, id, field, newValue: asRead + 1 })),
    );
    const count = (field: (v: string) => boolean, type: string): number => verdicts.filter((v) => v.type === type && field(v.field)).length;
    assert.deepEqual([findings.length, metrics.length, verdicts.length], [103, 78, 419]);
    assert.deepEqual([count((f) => f.startsWith("facts."), "finding"), count((f) => f === "value.amount", "finding"), count((f) => f === "value", "metric")], [319, 24, 76]);
  });

  test("holds every kind of refusal for a finding and for a measure, by name, and each where a measure's part can be", () => {
    const refused = golden.cases.flatMap((c) => c.expected.refused);
    const kinds = (measure: boolean): string[] =>
      [...new Set([...refused.filter((r) => (r.metricId !== undefined) === measure).map((r) => r.why.kind), ...golden.synthetic.verdicts.filter((v) => (v.type === "metric") === measure).flatMap((v) => (v.refused ? [v.refused] : []))])].sort();
    assert.deepEqual(kinds(false), ["repeated", "rounded", "shared", "stale", "unsure"]);
    assert.deepEqual(kinds(true), ["part", "rounded", "stale", "worded"]);
    const where = [...new Set(refused.flatMap((r) => (r.why.kind === "part" ? [r.why.where] : [])))].sort();
    assert.deepEqual(where, ["a point in its series", "a segment", "an evidence count", "n", "the stat median", "the stat upper", "value"]);
  });

  // Each hand case's refusals by kind, so a case that stops doing what its name says fails here, not only in the console.
  const INTENT: Record<string, string[]> = {
    "a finding: another figure holds the value as read (shared)": ["finding:shared"],
    "a finding: the value as read is printed twice (repeated)": ["finding:repeated"],
    "a finding: the value as read is printed rounded (rounded)": ["finding:rounded"],
    "a finding: the value as read is only part of a colon form (unsure)": ["finding:unsure"],
    "a finding: the correction was made from another value (stale)": ["finding:stale"],
    "a finding: a grouped figure printed once is rewritten in the form found (carried)": [],
    "a finding: a figure in no text changes and no text does (carried)": [],
    "a finding: put back after two rows, nothing changes": [],
    "a finding: two corrections in one finding are carried in one pass": [],
    "a finding: the sized amount in no text changes (carried)": [],
    "a title holding a person's token prints it as a named person": ["finding:repeated"],
    "a title over 120 characters with a line break is printed on one line and cut": ["finding:stale"],
    "a correction of a measure that isn't measured is ignored": [],
    "rows naming a finding or a fact the run doesn't hold are ignored": [],
    "a finding: a figure written as a number word is unsure": ["finding:unsure"],
    "a finding: a figure written as a full-width digit is unsure": ["finding:unsure"],
    "a measure: another of its figures (value) holds the value as read (part)": ["measure:part"],
    "a measure: another of its figures (n) holds the value as read (part)": ["measure:part"],
    "a measure: another of its figures (the stat median) holds the value as read (part)": ["measure:part"],
    "a measure: another of its figures (a segment) holds the value as read (part)": ["measure:part"],
    "a measure: another of its figures (a point in its series) holds the value as read (part)": ["measure:part"],
    "a measure: another of its figures (an evidence count) holds the value as read (part)": ["measure:part"],
    "a finding: two refused figures are named and listed in the order of its facts": ["finding:shared", "finding:shared"],
    "a measure: of two stats that hold the value, the first in its stats' order is named": ["measure:part"],
    "a measure: its own wording prints the value as read (worded)": ["measure:worded"],
    "a measure: its own wording prints the value rounded (rounded)": ["measure:rounded"],
    "a measure: the correction was made from another value (stale)": ["measure:stale"],
    "a measure: a value no part or text holds changes (carried)": [],
    "twelve refusals: findings' first, ten listed, then how many more": [...Array<string>(11).fill("finding:repeated"), "measure:worded"],
  };

  test("each hand case does what its name says", () => {
    assert.deepEqual(golden.cases.map((c) => c.name).sort(), Object.keys(INTENT).sort());
    for (const c of golden.cases) assert.deepEqual(c.expected.refused.map(kindOf), INTENT[c.name], c.name);
    const of = (name: string): GoldenCase => golden.cases.find((c) => c.name === name)!;
    const findingOut = (name: string): Finding => of(name).expected.findings[0]!;
    // Carried: the text in the form found, or no text, and the figure.
    const pipeline = findingOut("a finding: a grouped figure printed once is rewritten in the form found (carried)");
    assert.deepEqual([pipeline.summary, pipeline.facts.pipeline], ["£9,800 of open pipeline has no next step.", 9800]);
    const hidden = of("a finding: a figure in no text changes and no text does (carried)");
    assert.deepEqual(hidden.expected.findings[0], { ...hidden.findings[0]!, facts: { hidden: 70 } });
    const two = findingOut("a finding: two corrections in one finding are carried in one pass");
    assert.deepEqual([two.summary, two.facts], ["15 of 95 users logged in this month.", { active: 15, users: 95 }]);
    const sized = findingOut("a finding: the sized amount in no text changes (carried)");
    assert.equal(sized.value.kind !== "not_sized" && sized.value.amount, 2700);
    const carried = of("a measure: a value no part or text holds changes (carried)").expected.metrics[0]!;
    assert.equal(carried.status === "measured" && carried.value, 70);
    // Nothing changes.
    for (const name of ["a finding: put back after two rows, nothing changes", "a correction of a measure that isn't measured is ignored", "rows naming a finding or a fact the run doesn't hold are ignored"]) {
      assert.deepEqual([of(name).expected.findings, of(name).expected.metrics], [of(name).findings, of(name).metrics], name);
    }
    // A title as a line prints it.
    assert.ok(of("a title holding a person's token prints it as a named person").expected.lines[1]!.startsWith('"Win rate for rep a named person is well below the overall figure", deals: 43 as read'));
    const long = of("a title over 120 characters with a line break is printed on one line and cut").expected.lines[1]!;
    assert.equal(long.slice(1, long.indexOf('", seats:')).length, 120);
    assert.ok(!long.includes("\n") && long.includes("runs on well past"));
    // Named and listed in the order of the facts.
    assert.deepEqual(of("a finding: two refused figures are named and listed in the order of its facts").expected.refused.map((r) => [r.field, r.why]), [["facts.zeta", { kind: "shared", other: "alpha" }], ["facts.alpha", { kind: "shared", other: "zeta" }]]);
    // Twelve: the measure's row came first, and its line is past the ten.
    const twelve = of("twelve refusals: findings' first, ten listed, then how many more");
    assert.equal(twelve.overrides[0]!.target.type, "metric");
    assert.deepEqual([twelve.expected.lines.length, twelve.expected.lines[0], twelve.expected.lines[11]], [12, "Held back: 12 corrected figures can't be carried into the documents.", "And 2 more."]);
    assert.ok(twelve.expected.lines.slice(1, 11).every((line) => line.startsWith('"Seats on team ')));
  });
});

// refusalReason as it was before decision 532, copied as it stood (83e9d1e), so
// the listed lines can be shown not to have moved by a character.
const OLD_TOKEN = new RegExp(tokenSource(), "g");
const oldCountOf = (n: number): string => `${n} corrected ${n === 1 ? "figure" : "figures"}`;
const oldNameOf = (field: string): string => (field === "value.amount" ? "the sized amount" : field.slice("facts.".length));
const oldMetricNameOf = (field: string): string => (field.startsWith("stats.") ? field.slice("stats.".length) : field);
function oldShownTitle(title: string): string {
  return Array.from(title.replace(OLD_TOKEN, "a named person").replace(/\s+/g, " ").trim()).slice(0, 120).join("");
}
function oldWhy(r: OverrideRefusal): string {
  const measure = r.metricId !== undefined;
  switch (r.why.kind) {
    case "part":
      if (r.why.where === "an evidence count") return `the number of records this measure was read from is also ${grouped(r.asRead)}, and our check of printed figures would go on passing the figure as read`;
      return `another figure of this measure (${r.why.where}) is also ${grouped(r.asRead)}, and a chart, table or sentence prints it as read`;
    case "worded":
      return `the measure's own wording has ${grouped(r.asRead)}, which a correction doesn't rewrite`;
    case "shared":
      return `another figure of this finding (${r.why.other}) is also ${grouped(r.asRead)}, so we can't tell which one the wording prints`;
    case "repeated":
      return `the wording has ${grouped(r.asRead)} in ${r.why.places} places, and we can't tell which of them are this figure`;
    case "rounded":
      return `${measure ? "the measure's own wording" : "the wording"} doesn't print ${grouped(r.asRead)} itself and has ${r.why.found}, which may be this figure rounded`;
    case "unsure":
      return `the wording has ${grouped(r.asRead)} only as ${r.why.found}, which we can't be sure is this figure and can't rewrite`;
    case "stale":
      return `the correction was made from ${grouped(r.why.from)}, which this run no longer reads`;
  }
}
function oldRefusalReason(refused: OverrideRefusal[]): string {
  const ordered = [...refused.filter((r) => r.metricId === undefined), ...refused.filter((r) => r.metricId !== undefined)];
  const head = `Held back: ${oldCountOf(refused.length)} can't be carried into the documents.`;
  const listed = ordered.slice(0, 10).map((r) => `"${oldShownTitle(r.title)}", ${r.metricId === undefined ? oldNameOf(r.field) : oldMetricNameOf(r.field)}: ${grouped(r.asRead)} as read, corrected to ${grouped(r.last)}, but ${oldWhy(r)}.`);
  const more = refused.length > 10 ? [`And ${refused.length - 10} more.`] : [];
  const close = "Override each one back to its figure as read, then make the documents again. A finding's wording and a measure's chart or table can't be edited, so a figure we can't carry changes only with a new health check.";
  return [head, ...listed, ...more, close].map((line) => line.replace(/\s+/g, " ").trim()).join("\n");
}
const oldLines = (refused: OverrideRefusal[]): string[] => oldRefusalReason(refused).split("\n").slice(0, -1);

/** Every figure of the synthetic run the engine judges, the console's and a measure's n and stats too, in the run's order. */
function engineFigures(findings: Finding[], metrics: Metric[]): Overridable[] {
  const measures = metrics.flatMap((m) =>
    m.status === "measured" ? [{ type: "metric" as const, id: m.id, field: "value", asRead: m.value }, { type: "metric" as const, id: m.id, field: "n", asRead: m.n }, ...Object.entries(m.stats).map(([key, asRead]) => ({ type: "metric" as const, id: m.id, field: `stats.${key}`, asRead }))] : [],
  );
  return [...overridable(findings, []), ...measures];
}
const { findings: runFindings, metrics: runMetrics } = golden.synthetic;
const runFigures = engineFigures(runFindings, runMetrics);
const alone = (findings: Finding[], metrics: Metric[], x: Overridable): OverrideRefusal | undefined => applyOverrides(findings, metrics, [correction(x, x.asRead, x.asRead + 1)]).refused[0];
const runRefusals = runFigures.flatMap((x) => alone(runFindings, runMetrics, x) ?? []);
const handRefusals = golden.cases.flatMap((c) => c.expected.refused);

describe("refusalLines (decision 532, item 1)", () => {
  test("refusalReason is the lines, then the close", () => {
    for (const refused of [runRefusals.slice(0, 1), runRefusals.slice(0, 10), runRefusals, handRefusals, [...handRefusals].reverse()]) {
      assert.equal(refusalReason(refused), [...refusalLines(refused), CLOSE].join("\n"));
    }
  });

  test("the listed lines are the old function's, by the character, for every refusal of the synthetic run and the hand cases, alone and together", () => {
    assert.deepEqual([runFigures.length, runRefusals.length, handRefusals.length], [603, 237, 33]);
    for (const r of [...runRefusals, ...handRefusals]) {
      assert.deepEqual(refusalLines([r]), oldLines([r]));
      assert.equal(refusalLines([r])[1], oldLines([r])[1]);
    }
    // Together, and in reverse, so a measure's refusal given before a finding's is still listed after it.
    for (const refused of [runRefusals, [...runRefusals].reverse(), handRefusals, [...handRefusals].reverse(), [...runRefusals.filter((r) => r.metricId !== undefined).slice(0, 3), ...runRefusals.slice(0, 3)]]) {
      assert.deepEqual(refusalLines(refused), oldLines(refused));
    }
  });

  test("for 1, 10, 11 and 25 refusals, the lines number 2, 11, 12 and 12", () => {
    assert.deepEqual([1, 10, 11, 25].map((n) => refusalLines(runRefusals.slice(0, n)).length), [2, 11, 12, 12]);
    assert.deepEqual(refusalLines(runRefusals.slice(0, 25)).at(-1), "And 15 more.");
  });
});

// A seeded shuffle, so the order test runs the same orders every time.
function shuffler(seed: number): <T>(items: T[]) => T[] {
  let state = seed;
  const next = (): number => {
    state = (state + 0x6d2b79f5) | 0;
    let t = Math.imul(state ^ (state >>> 15), 1 | state);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
  return <T>(items: T[]): T[] => {
    const out = items.slice();
    for (let i = out.length - 1; i > 0; i--) {
      const j = Math.floor(next() * (i + 1));
      [out[i], out[j]] = [out[j]!, out[i]!];
    }
    return out;
  };
}
function shuffled(findings: Finding[], metrics: Metric[], shuffle: <T>(items: T[]) => T[]): { findings: Finding[]; metrics: Metric[] } {
  return {
    findings: findings.map((f) => ({ ...f, facts: Object.fromEntries(shuffle(Object.entries(f.facts))) })),
    metrics: metrics.map((m) => (m.status === "measured" ? { ...m, stats: Object.fromEntries(shuffle(Object.entries(m.stats))) } : m)),
  };
}
const refusedOrNot = (r: OverrideRefusal | undefined): string | null => (r ? r.why.kind : null);
const whatOf = (r: OverrideRefusal): string => `${r.findingId ?? r.metricId} ${r.field} ${r.why.kind}`;
const SEEDS = [532, 533, 534];

describe("no verdict depends on the order of a finding's facts or a measure's stats (decision 532, item 4)", () => {
  test("the hand cases, keys shuffled: the same findings and measures, and the same figures refused for the same kind", () => {
    for (const seed of SEEDS) {
      const shuffle = shuffler(seed);
      for (const c of golden.cases) {
        const keys = shuffled(c.findings, c.metrics, shuffle);
        const result = applyOverrides(keys.findings, keys.metrics, c.overrides);
        assert.deepEqual([result.findings, result.metrics], [c.expected.findings, c.expected.metrics], c.name);
        assert.deepEqual(result.refused.map(whatOf).sort(), c.expected.refused.map(whatOf).sort(), c.name);
      }
    }
  });

  test("the synthetic run, keys shuffled: each correction alone is refused, or not, for the same kind", () => {
    for (const seed of SEEDS) {
      const run = shuffled(runFindings, runMetrics, shuffler(seed));
      const moved = runFigures.filter((x) => refusedOrNot(alone(run.findings, run.metrics, x)) !== refusedOrNot(alone(runFindings, runMetrics, x)));
      assert.deepEqual(moved, [], `seed ${seed}`);
      const all = runFigures.map((x, i) => correction(x, x.asRead, x.asRead + 1, i + 1));
      const [was, now] = [applyOverrides(runFindings, runMetrics, all), applyOverrides(run.findings, run.metrics, all)];
      assert.deepEqual([now.findings, now.metrics], [was.findings, was.metrics], `seed ${seed}`);
      assert.deepEqual(now.refused.map(whatOf).sort(), was.refused.map(whatOf).sort(), `seed ${seed}`);
    }
  });
});

// Measured before the build (session 25, lane A): with every finding's facts and
// every measure's stats shuffled, no correction's verdict changes in kind, or in
// being refused or not. What changes is which other figure a "shared" refusal
// names, or which stat a "part" refusal names, and the order of a finding's
// refusals among themselves: the order the console may see from Postgres.
const NAMED_OTHERWISE: Record<number, { finding: number; measure: number; consoleOnly: number; reordered: number }> = {
  532: { finding: 6, measure: 4, consoleOnly: 6, reordered: 70 },
  533: { finding: 5, measure: 3, consoleOnly: 5, reordered: 87 },
  534: { finding: 3, measure: 1, consoleOnly: 3, reordered: 80 },
};

describe("measured: what the order of facts and stats does change, on the synthetic run (decision 532, item 4)", () => {
  test("the other figure named, each seed, and the order of refusals listed when every figure is corrected at once", () => {
    const consoleFields = new Set(overridable(runFindings, runMetrics).map((x) => `${x.type} ${x.id} ${x.field}`));
    const seen: typeof NAMED_OTHERWISE = {};
    for (const seed of SEEDS) {
      const run = shuffled(runFindings, runMetrics, shuffler(seed));
      const renamed = runFigures.filter((x) => JSON.stringify(alone(run.findings, run.metrics, x)?.why) !== JSON.stringify(alone(runFindings, runMetrics, x)?.why));
      const all = runFigures.map((x, i) => correction(x, x.asRead, x.asRead + 1, i + 1));
      const [was, now] = [applyOverrides(runFindings, runMetrics, all).refused.map(whatOf), applyOverrides(run.findings, run.metrics, all).refused.map(whatOf)];
      seen[seed] = {
        finding: renamed.filter((x) => x.type === "finding").length,
        measure: renamed.filter((x) => x.type === "metric").length,
        consoleOnly: renamed.filter((x) => consoleFields.has(`${x.type} ${x.id} ${x.field}`)).length,
        reordered: was.filter((w, i) => now[i] !== w).length,
      };
    }
    assert.deepEqual(seen, NAMED_OTHERWISE);
  });
});

// Decision 533's premise: a new row can't turn another figure's verdict.
describe("measured: a figure's verdict is the same with another figure of its finding or measure corrected beside it (decision 533)", () => {
  test("every ordered pair of figures of one finding or measure, on the synthetic run", () => {
    const groups = new Map<string, Overridable[]>();
    for (const x of runFigures) groups.set(`${x.type} ${x.id}`, [...(groups.get(`${x.type} ${x.id}`) ?? []), x]);
    const consoleFields = new Set(overridable(runFindings, runMetrics).map((x) => `${x.type} ${x.id} ${x.field}`));
    let [items, pairs, consoleItems, consolePairs] = [0, 0, 0, 0];
    const differ: string[] = [];
    for (const figures of groups.values()) {
      if (figures.length < 2) continue;
      items++;
      const onConsole = figures.filter((x) => consoleFields.has(`${x.type} ${x.id} ${x.field}`));
      if (onConsole.length >= 2) consoleItems++;
      for (const a of figures) {
        const was = alone(runFindings, runMetrics, a);
        for (const b of figures) {
          if (a === b) continue;
          pairs++;
          if (onConsole.includes(a) && onConsole.includes(b)) consolePairs++;
          const both = applyOverrides(runFindings, runMetrics, [correction(a, a.asRead, a.asRead + 1, 1), correction(b, b.asRead, b.asRead + 1, 2)]).refused;
          const now = both.find((r) => r.field === a.field);
          if (JSON.stringify(now) !== JSON.stringify(was)) differ.push(`${a.id} ${a.field} beside ${b.field}: ${JSON.stringify(was?.why ?? null)} alone, ${JSON.stringify(now?.why ?? null)} beside`);
        }
      }
    }
    assert.deepEqual(differ, []);
    assert.deepEqual({ items, pairs, consoleItems, consolePairs }, { items: 167, pairs: 2586, consoleItems: 91, consolePairs: 1176 });
  });
});
