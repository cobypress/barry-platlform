import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { checkedDigest, digestDataset, type CheckedDigest } from "../checked.ts";
import { buildDigest } from "../build.ts";
import { strictDataset, strictScope, strictShadow, strictUntraced, STRICT_SHADOW_FILE, type StrictShadow } from "../strict.ts";
import { markSentence } from "../../engine/org-words.ts";
import { named, namedIn, NAMED_VALUES_FILE, withNamedValues } from "../../engine/named.ts";
import { buildReport, type ReportInput } from "../../narrative/sections.ts";
import { scopeDataset } from "../../numbercheck/index.ts";
import type { Finding } from "../../schemas/index.ts";
import { keepStrictShadow, namedValuesOf } from "../../worker/run-job.ts";
import { automatic, base, MADE_AT } from "./readiness-scenarios.ts";

// The strict per-finding check, in shadow (DECISIONS.md 768, KAN-102): each
// finding's figures trace to its own computed values alone, never to its own
// prose or another finding's; and the shadow changes nothing a run serves,
// stores or decides, even when it fails.

const PLANTED = "We counted 4,321 widgets.";
const issues = base.findings.filter((f) => f.status === "issue" && f.orgWords?.summary);
const [first, second] = [issues[0]!, issues[1]!];

/** A finding whose summary is `text`, marked as all ours, with these facts. */
const withSummary = (f: Finding, text: string, facts: Record<string, number> = f.facts): Finding => ({ ...f, summary: text, facts, orgWords: { ...f.orgWords, summary: markSentence(text, [])! } });
const inputWith = (change: (f: Finding) => Finding): ReportInput => ({ ...base, findings: base.findings.map(change) });

/** The strict check's untraced figures in one finding's summary, as the shadow scopes it. */
function summaryUntraced(input: ReportInput, id: string, named = {}): string[] {
  const report = buildReport(input);
  const digest = buildDigest({ report, pseudonyms: automatic.pseudonyms, inventory: null, madeAt: MADE_AT });
  const at = digest.findings.findIndex((f) => f.id === id);
  const t = { path: "findings[].summary", at: [at], text: digest.findings[at]!.summary };
  const strict = strictDataset(report, digestDataset(report, automatic.pseudonyms), named);
  return strictUntraced(t.text, digest.findings[at]!.orgWords!.summary!.orgWords, scopeDataset(strict, strictScope(digest, report, t)!)).unmatched;
}

const check = (input: ReportInput, more: Partial<Parameters<typeof checkedDigest>[0]> = {}): CheckedDigest => checkedDigest({ input, pseudonyms: automatic.pseudonyms, inventory: null, madeAt: MADE_AT, ...more });
const served = ({ strictShadow: _s, ...rest }: CheckedDigest) => rest;

describe("the strict check traces a finding's figures to its own computed values alone", () => {
  test("a figure the finding's own prose writes, and no computed value holds, is untraced, though today's check traces it", () => {
    const input = inputWith((f) => (f.id === first.id ? withSummary(f, PLANTED) : f));
    assert.deepEqual(summaryUntraced(input, first.id), ["4,321"]);
    // Today's check reads the sentence into its own set, so it traces (DECISIONS.md 764's circularity).
    assert.equal(check(input).unmatched, 0);
  });

  test("the same figure as a fact of the finding traces", () => {
    const input = inputWith((f) => (f.id === first.id ? withSummary(f, PLANTED, { ...f.facts, widgets: 4321 }) : f));
    assert.deepEqual(summaryUntraced(input, first.id), []);
  });

  test("and as a value named for the finding, in the run folder's named values", () => {
    const input = inputWith((f) => (f.id === first.id ? withSummary(f, PLANTED) : f));
    assert.deepEqual(summaryUntraced(input, first.id, { [first.id]: { widgets: 4321 } }), []);
  });

  test("another finding's fact or named value never backs it", () => {
    const input = inputWith((f) => (f.id === first.id ? withSummary(f, PLANTED) : f.id === second.id ? { ...f, facts: { ...f.facts, widgets: 4321 } } : f));
    assert.deepEqual(summaryUntraced(input, first.id, { [second.id]: { widgets: 4321 } }), ["4,321"]);
  });

  test("a named value takes the unit its name says, and no other", () => {
    const text = "Raw exposure: £807,000 over 19 deals, weighted at 61.9%.";
    const input = inputWith((f) => (f.id === first.id ? withSummary(f, text, {}) : f));
    assert.deepEqual(summaryUntraced(input, first.id, { [first.id]: { exposureMoney: 807_000, deals: 19, winPct: 61.9 } }), []);
    assert.deepEqual(summaryUntraced(input, first.id, { [first.id]: { exposure: 807_000, deals: 19, win: 61.9 } }), ["£807,000", "61.9%"]);
  });
});

describe("named values", () => {
  test("give the text back unchanged, and are found only in a text that holds the sentence whole", () => {
    withNamedValues(() => {
      const text = named("Planted deals with no widgets in 91 days", { widgetDays: 91, missing: null });
      assert.equal(text, "Planted deals with no widgets in 91 days");
      assert.deepEqual(namedIn([`${text}: 18 of 76.`]), { widgetDays: 91 });
      assert.equal(namedIn(["Planted deals with no widgets in 61 days"]), null);
    });
  });

  test("two sentences' values under one name are both kept", () => {
    withNamedValues(() => {
      named("3 planted widgets", { widgets: 3 });
      named("and 5 planted widgets", { widgets: 5 });
      assert.deepEqual(namedIn(["3 planted widgets and 5 planted widgets"]), { widgets: 3, "widgets#2": 5 });
    });
  });

  test("the run folder's file is read, and a missing or broken one reads as none", () => {
    const dir = mkdtempSync(join(tmpdir(), "hc-named-"));
    assert.deepEqual(namedValuesOf(dir), {});
    writeFileSync(join(dir, NAMED_VALUES_FILE), "{ not json");
    assert.deepEqual(namedValuesOf(dir), {});
    writeFileSync(join(dir, NAMED_VALUES_FILE), JSON.stringify({ "f-1": { days: 90 } }));
    assert.deepEqual(namedValuesOf(dir), { "f-1": { days: 90 } });
  });
});

describe("the shadow changes nothing a run serves, stores or decides", () => {
  const input = inputWith((f) => (f.id === first.id ? withSummary(f, PLANTED) : f));
  const real = check(input);

  test("it counts the planted figure, by rule and kind, as written", () => {
    assert.ok(real.strictShadow?.ok);
    const shadow = (real.strictShadow as { shadow: StrictShadow }).shadow;
    assert.ok(shadow.figures.some((f) => f.ruleId === first.ruleId && f.kind === "summary" && f.figure === "4,321"));
    assert.ok(shadow.untraced > 0 && shadow.byKind.summary!.untraced > 0);
  });

  test("the digest, masks, unmatched, unmarked, withheld and hold are those of a run with no shadow at all", () => {
    const none = check(input, { strict: () => ({ version: 1, checked: 0, untraced: 0, byKind: {}, figures: [] }) });
    const loud = check(input, { strict: () => ({ version: 1, checked: 9999, untraced: 9999, byKind: { summary: { checked: 9999, untraced: 9999 } }, figures: [] }) });
    assert.deepEqual(served(real), served(none));
    assert.deepEqual(served(loud), served(none));
  });

  test("a shadow that reaches into the digest it is given can't change the one served", () => {
    const meddling = check(input, {
      strict: (digest, report, set, named) => {
        const out = strictShadow(digest, report, set, named);
        for (const f of digest.findings) f.summary = "(detail withheld)";
        return out;
      },
    });
    assert.deepEqual(served(meddling), served(real));
  });

  test("a shadow that throws costs the run nothing: the same result, and the failure by its name", () => {
    const failing = check(input, {
      strict: () => {
        throw new RangeError("planted failure naming Acme Ltd");
      },
    });
    assert.deepEqual(served(failing), served(real));
    assert.deepEqual(failing.strictShadow, { ok: false, failed: "RangeError" });
  });

  test("the worker logs one line of counts and keeps the figures in the run folder only; a failure, one line and no file", () => {
    const dir = mkdtempSync(join(tmpdir(), "hc-shadow-"));
    const lines: string[] = [];
    keepStrictShadow(dir, "hc-run", real.strictShadow, (l) => lines.push(l));
    assert.equal(lines.length, 1);
    assert.match(lines[0]!, /^Run hc-run: the strict check \(shadow\) would mask \d+ of \d+ figures/);
    assert.doesNotMatch(lines[0]!, /4,321/);
    const kept = JSON.parse(readFileSync(join(dir, STRICT_SHADOW_FILE), "utf8")) as StrictShadow;
    assert.ok(kept.figures.some((f) => f.figure === "4,321"));
    keepStrictShadow(dir, "hc-run", { ok: false, failed: "RangeError" }, (l) => lines.push(l));
    assert.equal(lines.length, 2);
    assert.equal(lines[1], "Run hc-run: the strict check (shadow) couldn't be worked out (RangeError), and the run went on without it.");
    assert.equal(existsSync(join(dir, STRICT_SHADOW_FILE)), false);
  });

  test("a figure is recorded as written, and a word with digits never is", () => {
    const text = "We counted 4,321 widgets on Invoice2__c.";
    const shadow = (check(inputWith((f) => (f.id === first.id ? withSummary(f, text) : f))).strictShadow as { shadow: StrictShadow }).shadow;
    const mine = shadow.figures.filter((f) => f.ruleId === first.ruleId && f.kind === "summary").map((f) => f.figure);
    assert.ok(mine.includes("4,321"));
    assert.ok(mine.includes("(identifier)"));
    assert.ok(mine.every((f) => !/invoice/i.test(f)));
  });
});
