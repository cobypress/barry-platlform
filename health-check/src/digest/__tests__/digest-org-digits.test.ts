import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { checkedDigest, digestDataset, type CheckedDigest } from "../checked.ts";
import { textSha256 } from "../../engine/org-words.ts";
import { buildReport, type BuiltReport, type ReportInput } from "../../narrative/sections.ts";
import { buildDataset, checkNumbers, extractFigures } from "../../numbercheck/index.ts";
import { inOrgWords } from "../../numbercheck/extract.ts";
import { orgFree } from "../../numbercheck/dataset.ts";
import { loadRun } from "../../render/render-run.ts";
import type { Finding } from "../../schemas/index.ts";
import { automatic, base, MADE_AT } from "./readiness-scenarios.ts";
import { deals, orgWith, SKIPS, USUAL } from "./paths-orgs.ts";

// Digits in words the org chose never make a number in our words count as
// traced in the digest (DECISIONS.md 764). A stage named "987654" put 987654
// into the set the digest is checked against, as a label and as a figure read
// from every text that names the stage, so a planted 987654 in a sentence of
// ours traced. Now a figure inside the org's words is the org's name and isn't
// checked, and the set holds nothing read from the org's words.

const check = (input: ReportInput, build?: (i: ReportInput) => BuiltReport): CheckedDigest =>
  checkedDigest({ input, pseudonyms: automatic.pseudonyms, inventory: null, madeAt: MADE_AT, ...(build ? { build } : {}) });

/** Appends org words to a finding's sentence, marked as the org's, the marks made again for the new text. */
function withOrgWords(f: Finding, key: "summary", words: string, at: "end" | { after: string } = "end"): Finding {
  const held = f.orgWords?.[key]?.spans ?? [];
  const text = f[key];
  const p = at === "end" ? text.length : text.indexOf(at.after) + at.after.length;
  const lead = at === "end" ? " It names " : " in ";
  const tail = at === "end" ? "." : "";
  const inserted = lead.length + words.length + tail.length;
  const start = p + lead.length;
  const moved = held.map((s) => (s.start >= p ? { start: s.start + inserted, end: s.end + inserted } : s));
  const next = `${text.slice(0, p)}${lead}${words}${tail}${text.slice(p)}`;
  const spans = [...moved, { start, end: start + words.length }].sort((a, b) => a.start - b.start);
  return { ...f, [key]: next, orgWords: { ...f.orgWords, [key]: { spans, textSha256: textSha256(next) } } };
}

/** Appends words of ours to a finding's summary, marks kept and made again for the new text. */
function withOurWords(f: Finding, words: string): Finding {
  const next = `${f.summary} ${words}`;
  return { ...f, summary: next, orgWords: { ...f.orgWords, summary: { spans: f.orgWords?.summary?.spans ?? [], textSha256: textSha256(next) } } };
}

const A = base.findings.find((f) => f.ruleId === "cfg.unused-custom-fields" && f.orgWords?.summary?.spans.length)!;
const B = base.findings.find((f) => f.ruleId === "utl.unused-reports")!;
const C = base.findings.find((f) => f.ruleId === "utl.dashboards-inactive-running-user")!;

/** The org's name in a finding's subjects, in a sentence of its marked as the org's, in a note with no marks, and as a segment's key. */
function named(input: ReportInput, type: string, name: string): ReportInput {
  const findings = input.findings.map((f) => {
    if (f.id === A.id) return { ...withOrgWords(f, "summary", name), subjects: [...f.subjects, { type, label: name }] };
    if (f.id === C.id) return { ...f, subjects: [...f.subjects, { type, label: name }], evidence: f.evidence.map((e, i) => (i === 0 ? { ...e, note: `Seen on ${name}` } : e)) };
    return f;
  });
  let segmented = false;
  const metrics = input.metrics.map((m) => {
    if (segmented || m.status !== "measured" || m.segments.length === 0) return m;
    segmented = true;
    return { ...m, segments: m.segments.map((s, i) => (i === 0 ? { ...s, key: name } : s)) };
  });
  return { ...input, findings, metrics };
}

/** A build in which nothing but the planted sentence itself holds the planted number for its finding: as if no data computed it. */
const plantedOnly = (n: number, id: string) => (i: ReportInput): BuiltReport => {
  const r = buildReport(i);
  return { ...r, dataset: { ...r.dataset, figures: r.dataset.figures.filter((f) => !(f.source === id && f.value === n)) } };
};

// Every kind of name the org chooses, each with digits (or number words) a sentence of ours could repeat.
const KINDS: Array<{ kind: string; type: string; name: string; planted: string; value: number }> = [
  { kind: "stage name", type: "stage", name: "Stage 913 of 917", planted: "917", value: 917 },
  { kind: "field label", type: "field", name: "Score 7341", planted: "7341", value: 7341 },
  { kind: "field API name", type: "field", name: "Score_7342__c", planted: "7342", value: 7342 },
  { kind: "object label", type: "object", name: "Q4 2061 Pipeline", planted: "2061", value: 2061 },
  { kind: "record type name", type: "record type", name: "Tier 3187", planted: "3187", value: 3187 },
  { kind: "flow name", type: "flow", name: "Renewals 8423 Flow", planted: "8423", value: 8423 },
  { kind: "class name", type: "apex class", name: "Batch 6619 Job", planted: "6619", value: 6619 },
  { kind: "user-facing label", type: "dashboard", name: "Top 5527 Accounts", planted: "5527", value: 5527 },
  { kind: "picklist value", type: "picklist value", name: "Band 4471", planted: "4471", value: 4471 },
  { kind: "licence name", type: "licence", name: "Platform 3391 Pack", planted: "3391", value: 3391 },
  { kind: "name in number words", type: "stage", name: "Stage Nine Hundred Eleven", planted: "911", value: 911 },
  { kind: "name with a percentage", type: "dashboard", name: "Top 37.25% Accounts", planted: "37.25%", value: 37.25 },
];

const servedSummary = (c: CheckedDigest, id: string): string | undefined => c.digest?.findings.find((f) => f.id === id)?.summary;

describe("digits in the org's words, on the synthetic org's automatic run", () => {
  const clean = check(base);

  test("the run as made: released, nothing masked, unmatched or unmarked", () => {
    assert.deepEqual([clean.held, clean.masks, clean.unmatched, clean.unmarked], [null, 0, 0, 0]);
  });

  for (const k of KINDS) {
    describe(`a ${k.kind}, "${k.name}"`, () => {
      const input = named(base, k.type, k.name);

      test("the name alone changes nothing: released with no mask, the name served whole as the org's words", () => {
        const c = check(input);
        assert.deepEqual([c.held, c.masks, c.unmatched, c.unmarked], [null, 0, 0, 0]);
        assert.ok(servedSummary(c, A.id)?.includes(k.name));
      });

      test("the set the digest is checked against holds nothing from the name", () => {
        const set = orgFree(digestDataset(buildReport(input), automatic.pseudonyms));
        assert.equal(set.labels.length, 0);
        assert.ok(!set.figures.some((f) => Math.abs(f.value - k.value) < 1e-9), `${k.value} isn't a figure of the run`);
      });

      test(`a planted ${k.planted} in our words elsewhere doesn't trace: it is withheld, never served as traced`, () => {
        const plant = `It took ${k.planted} steps.`;
        const planted = { ...input, findings: input.findings.map((f) => (f.id === B.id ? withOurWords(f, plant) : f)) };
        const c = check(planted, plantedOnly(k.value, B.id));
        assert.ok(!servedSummary(c, B.id)?.includes(plant), "the planted figure isn't served");
        assert.ok(c.masks > 0 || c.unmatched > 0 || c.held !== null);
      });

      test("the same number, computed by the run, still traces: nothing masked", () => {
        // The unused reports' count, 59, written into the name: the finding's own "59 of 79" is ours and traces to its facts.
        const real = B.facts.unused!;
        const withReal = named(base, k.type, k.name.replace(k.planted.replace("%", ""), String(real)));
        const c = check(withReal);
        assert.deepEqual([c.held, c.masks, c.unmatched, c.unmarked], [null, 0, 0, 0]);
        assert.equal(servedSummary(c, B.id), B.summary);
      });
    });
  }

  test("a name holding a real figure, in the same sentence as that figure: both served, nothing masked", () => {
    const real = B.facts.unused!;
    const name = `Top ${real} Reports`;
    const at = `${real} of ${B.facts.reports} reports`;
    assert.ok(B.summary.includes(at));
    const input = { ...base, findings: base.findings.map((f) => (f.id === B.id ? { ...withOrgWords(f, "summary", name, { after: at }), subjects: [...f.subjects, { type: "dashboard", label: name }] } : f)) };
    const c = check(input);
    assert.deepEqual([c.held, c.masks, c.unmatched, c.unmarked], [null, 0, 0, 0]);
    assert.ok(servedSummary(c, B.id)?.includes(`${at} in ${name}`));
  });
});

describe("digits in a stage's name, through the real analysis (paths-orgs.ts)", async () => {
  const skip = [...deals(60, USUAL, 30, 60), ...deals(30, SKIPS, 25, 60)];
  const plain = loadRun((await orgWith(skip)).dir);
  const planted = loadRun((await orgWith(skip, { stages: { Discovery: "987654" } })).dir);
  // The insight's own counts: 30 skipped Discovery, of 90 considered.
  const thirty = loadRun((await orgWith(skip, { stages: { Discovery: "30" } })).dir);
  const ninety = loadRun((await orgWith(skip, { stages: { Discovery: "Stage 90" } })).dir);
  const run = (l: typeof plain, input = l.input, build?: (i: ReportInput) => BuiltReport) => checkedDigest({ input, pseudonyms: l.pseudonyms, inventory: null, madeAt: MADE_AT, ...(build ? { build } : {}) });
  const counts = (c: CheckedDigest) => [c.held, c.masks, c.unmatched, c.unmarked, c.pathsWithheld];

  test("a stage named 987654, 30 or Stage 90 is released as the plain org is, with the same counts", () => {
    const want = counts(run(plain));
    assert.deepEqual(want, [null, 0, 0, 0, 0]);
    for (const l of [planted, thirty, ninety]) assert.deepEqual(counts(run(l)), want);
  });

  test("the insight that names the stage 30 says its own count of 30 and serves it, traced", () => {
    const c = run(thirty);
    const insight = c.digest!.paths!.insights[0]!;
    assert.match(insight.summary, /the 30 that skipped 30 were won/);
    assert.ok(insight.figures?.some((f) => f.name === "groupDeals" && f.value === 30));
  });

  test("the review's case: a planted 987654 in our words beside a stage named 987654 is withheld", () => {
    const target = planted.input.findings.find((f) => f.status === "issue" && f.ruleId !== "pipe.path-insights" && f.orgWords?.summary)!;
    const plant = "It took 987654 steps.";
    const input = { ...planted.input, findings: planted.input.findings.map((f) => (f.id === target.id ? withOurWords(f, plant) : f)) };
    const c = run(planted, input, plantedOnly(987654, target.id));
    assert.ok(!servedSummary(c, target.id)?.includes(plant));
    assert.ok(c.masks > 0 || c.unmatched > 0 || c.held !== null);
  });
});

describe("a guard's detail and an evidence note: never a source the digest traces to", () => {
  // The guard's own shape on the synthetic run is "Used in 1 report: reports/Marketing/Report_8.report.":
  // a report's path is the org's, named freely, and no subject or segment need name it.
  const PL = 987654;
  const host = base.findings.find((f) => f.ruleId === "cfg.unused-custom-fields")!;
  const plant = `It took ${PL} steps.`;
  const planted = (place: "guard" | "note" | "exclusion"): ReportInput => ({
    ...base,
    findings: base.findings.map((f) => {
      const text = `Used in 1 report: reports/Pipeline/Top ${PL} Deals.report.`;
      if (f.id === host.id && place === "guard") return { ...f, guards: [...f.guards, { ...f.guards[0]!, detail: text }] };
      if (f.id === host.id) return { ...f, evidence: f.evidence.map((e, i) => (i === 0 ? (place === "note" ? { ...e, note: text } : { ...e, exclusions: [...e.exclusions, text] }) : e)) };
      if (f.id === B.id) return withOurWords(f, plant);
      return f;
    }),
  });
  for (const place of ["guard", "note", "exclusion"] as const) {
    test(`a report's path in a ${place} doesn't make a planted ${PL} in our words trace`, () => {
      const c = check(planted(place), plantedOnly(PL, B.id));
      assert.ok(!servedSummary(c, B.id)?.includes(plant));
      assert.ok(c.masks > 0 || c.unmatched > 0 || c.held !== null);
    });
  }

  test("every figure read from any guard's detail, evidence note or exclusion is the org's in the set", () => {
    const set = buildDataset({ findings: base.findings, metrics: [] });
    const texts = base.findings.flatMap((f) => [...f.guards.map((g) => g.detail), ...f.evidence.flatMap((e) => [e.note ?? "", ...e.exclusions])]).filter((t) => /\d/.test(t));
    assert.ok(texts.length > 0);
    const only = buildDataset({ findings: base.findings.map((f) => ({ ...f, title: "x", summary: "x", recommendation: "", notMeasurableReason: f.notMeasurableReason ? "x" : f.notMeasurableReason, effort: { ...f.effort, note: undefined }, dependencies: [], value: { kind: "not_sized", reason: "x" }, subjects: [], facts: {}, segmentsChecked: [], orgWords: undefined, evidence: f.evidence.map((e) => ({ ...e, count: null, dateRange: null })) })) as Finding[], metrics: [] });
    // The run's summary counts aside, every row left is read from those texts.
    const read = only.figures.filter((x) => x.source !== "summary");
    assert.ok(read.length > 0 && read.every((x) => x.org));
    assert.ok(set.figures.some((x) => x.org));
  });
});

describe("the number check's reading of the org's words", () => {
  test("a figure is the org's when every digit lies in its words, or a figure in words starts there; a unit of theirs leaves it ours", () => {
    const text = "Stage 9 of 12 holds 1200 GBP and Tier Three has 4 deals.";
    const spans = [{ start: 0, end: 13 }, { start: 25, end: 28 }, { start: 33, end: 43 }];
    const { figures } = extractFigures(text);
    const read = Object.fromEntries(figures.map((f, i) => [f.raw, inOrgWords(text, figures, spans)[i]]));
    assert.deepEqual(read, { "9": true, "12": true, "1200 GBP": false, Three: true, "4": false });
  });

  test("a number in words is the org's only when every letter of every number word lies in its words", () => {
    const read = (text: string, org: string) => {
      const at = text.indexOf(org);
      const { figures } = extractFigures(text);
      return Object.fromEntries(figures.map((f, i) => [f.raw, inOrgWords(text, figures, [{ start: at, end: at + org.length }])[i]]));
    };
    // The reviewer's three: a name ending in a number word never takes the rest of a number of ours with it.
    assert.deepEqual(read("Deals in Tier Three hundred and twelve days late.", "Tier Three"), { "Three hundred and twelve days": false });
    assert.deepEqual(read("Region One million in pipeline", "Region One"), { "One million": false });
    assert.deepEqual(read("Deals at forty-two thousand lost.", "f"), { "forty-two thousand": false });
    // A name holding the whole number in words is still the org's, a unit of ours after it too.
    assert.deepEqual(read("Team Seven 7 deals", "Team Seven"), { Seven: true, "7": false });
    assert.deepEqual(read("Tier Three days late", "Tier Three"), { "Three days": true });
  });

  test("checked with its marks, a figure in the org's words is never checked, and one of ours is never read as 'of' it", () => {
    const set = { figures: [{ source: "f", kind: "number" as const, value: 5 }], dates: [], identifiers: [], tokens: [], labels: [] };
    const text = "Stage 9 of 12: 5 deals";
    assert.equal(checkNumbers(text, set, { orgWords: [{ start: 0, end: 13 }] }).unmatched.length, 0);
    assert.deepEqual(checkNumbers(text, set).unmatched.map((u) => u.raw), ["9", "12"]);
    // "3 of" a count the org's words hold: 3 must trace on its own.
    assert.deepEqual(checkNumbers("3 of 12 Stage", set, { orgWords: [{ start: 5, end: 13 }] }).unmatched.map((u) => u.raw), ["3"]);
  });

  test("the set: what a text's marks or a known name hold is the org's; a fact written only there is added as a figure of ours; a label never reaches the digest's set", () => {
    const f = structuredClone(A);
    f.facts = { ...f.facts, special: 4242 };
    f.subjects = [{ type: "stage", label: "Stage 4242" }];
    f.summary = "Deals in Stage 4242 waited.";
    f.orgWords = { summary: { spans: [{ start: 9, end: 19 }], textSha256: textSha256(f.summary) } };
    f.dependencies = ["Also Stage 4242 and 77 more."];
    const set = buildDataset({ findings: [f], metrics: [], orgNames: ["Record 555"], extraText: [{ source: "x", text: "Record 555 then 556." }, { source: "y", text: "ACME 557", org: true }] });
    const free = orgFree(set);
    const has = (s: typeof set, v: number, org?: boolean) => s.figures.some((x) => x.value === v && (org === undefined || !!x.org === org));
    assert.ok(has(set, 4242, true), "the name's 4242 is in the documents' set, as the org's");
    assert.ok(has(set, 4242, false), "and the fact, as ours");
    assert.ok(has(free, 77) && has(free, 556) && !has(free, 555) && !has(free, 557));
    assert.ok(set.labels.length > 0 && free.labels.length === 0);
  });
});
