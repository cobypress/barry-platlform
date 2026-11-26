import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { checkedDigest, digestDataset, type CheckedDigest } from "../checked.ts";
import { buildDigest } from "../build.ts";
import { RUN_DATES, ShadowTimeout, strictDataset, strictScope, strictShadow, strictUntraced, STRICT_SHADOW_FILE, unitOf, type StrictShadow } from "../strict.ts";
import { markSentence } from "../../engine/org-words.ts";
import { named, namedFor, namedIn, NAMED_VALUES_FILE, withNamedValues } from "../../engine/named.ts";
import { buildReport, type ReportInput } from "../../narrative/sections.ts";
import { scopeDataset } from "../../numbercheck/index.ts";
import type { Finding } from "../../schemas/index.ts";
import { keepStrictShadow, namedValuesOf } from "../../worker/run-job.ts";
import { END_OF_SUPPORT, SUPPORT_ENDED_ON } from "../../domains/automation/notes.ts";
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
function summaryUntraced(input: ReportInput, id: string, named = {}, field: "summary" | "fixPrompt" = "summary"): string[] {
  const report = buildReport(input);
  const digest = buildDigest({ report, pseudonyms: automatic.pseudonyms, inventory: null, madeAt: MADE_AT });
  const at = digest.findings.findIndex((f) => f.id === id);
  const t = { path: `findings[].${field}`, at: [at], text: digest.findings[at]![field]! };
  const strict = strictDataset(report, digestDataset(report, automatic.pseudonyms), named);
  return strictUntraced(t.text, digest.findings[at]!.orgWords![field]!.orgWords, scopeDataset(strict, strictScope(digest, report, t)!)).unmatched;
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

  test("the run's numbers (its API calls, budget share, API version) back no finding's figure; its dates do", () => {
    const { run } = base;
    const calls = run.budget.callsMade;
    const text = `There are ${calls} scheduled jobs. Half your jobs, ${Math.round(run.budget.stopAtShare * 100)}%, failed. The analysis covers ${run.analysisWindow.from.slice(0, 10)} to ${run.analysisWindow.to.slice(0, 10)}.`;
    const input = inputWith((f) => (f.id === first.id ? withSummary(f, text, {}) : f));
    const untraced = summaryUntraced(input, first.id);
    assert.ok(untraced.includes(String(calls)) || untraced.includes(calls.toLocaleString("en-GB")), String(untraced));
    assert.ok(untraced.includes(`${Math.round(run.budget.stopAtShare * 100)}%`));
    assert.equal(untraced.filter((u) => /^\d{4}-\d\d-\d\d$/.test(u)).length, 0);
    // Whatever a run-wide line may take, a finding's scope holds the run's dates and nothing else of it.
    const report = buildReport(input);
    const digest = buildDigest({ report, pseudonyms: automatic.pseudonyms, inventory: null, madeAt: MADE_AT });
    const scope = strictScope(digest, report, { path: "findings[].summary", at: [0], text: "" })!;
    assert.ok(scope.includes(RUN_DATES) && !scope.includes("run"));
    assert.equal(scopeDataset(strictDataset(report, digestDataset(report, automatic.pseudonyms)), [RUN_DATES]).figures.length, 0);
  });

  test("a fix prompt's own fixed text never holds the finding's sentences: a figure only the summary writes is untraced in the prompt too", () => {
    const prompt = `What we found: ${PLANTED}`;
    const input = inputWith((f) => (f.id === first.id ? { ...withSummary(f, PLANTED), fixPrompt: prompt, orgWords: { ...withSummary(f, PLANTED).orgWords, fixPrompt: markSentence(prompt, [])! } } : f));
    assert.deepEqual(summaryUntraced(input, first.id, {}, "fixPrompt"), ["4,321"]);
  });

  test("nothing of the calibration's text, the intake, the cover or a correction is in the strict set", () => {
    const report = buildReport(base);
    const set = digestDataset(report, automatic.pseudonyms);
    const prose = ["intake", "calibration", "overrides", "cover"];
    // The calibration's text is in the documents' set, so this would catch it.
    assert.ok(set.figures.some((r) => r.source === "calibration") || set.dates.some((r) => r.source === "calibration"));
    const strict = strictDataset(report, set);
    for (const rows of [strict.figures, strict.dates, strict.identifiers, strict.tokens]) assert.deepEqual(rows.filter((r) => prose.includes(r.source)), []);
  });

  test("a named value takes the unit its name says, and no other", () => {
    const text = "Raw exposure: £807,000 over 19 deals, weighted at 61.9%.";
    const input = inputWith((f) => (f.id === first.id ? withSummary(f, text, {}) : f));
    assert.deepEqual(summaryUntraced(input, first.id, { [first.id]: { exposureMoney: 807_000, deals: 19, winPct: 61.9 } }), []);
    assert.deepEqual(summaryUntraced(input, first.id, { [first.id]: { exposure: 807_000, deals: 19, win: 61.9 } }), ["£807,000", "61.9%"]);
  });

  test("a name's last whole word says its unit, case aside; or the unit is given beside the value", () => {
    const text = "Jobs failed in the last 7 days. 25% is our working limit. 26.7% of leads convert overall, against £2,561,000 won.";
    const input = inputWith((f) => (f.id === first.id ? withSummary(f, text, {}) : f));
    // As the run folder keeps them: a name, or "name@unit" where named() was given the unit.
    assert.deepEqual(summaryUntraced(input, first.id, { [first.id]: { days: 7, "limit@percent": 25, "overall@percent": 26.7, "wonFromLeads@money": 2_561_000 } }), []);
    assert.deepEqual(summaryUntraced(input, first.id, { [first.id]: { days: 7, limit: 25, overall: 26.7, wonFromLeads: 2_561_000 } }), ["25%", "26.7%", "£2,561,000"]);
  });

  test("a named date traces the day it names, in the finding's own material alone (DECISIONS.md 772)", () => {
    const text = "Source: Salesforce, checked 2026-11-02. Support ended on 31 December 2025.";
    const input = inputWith((f) => (f.id === first.id ? withSummary(f, text, {}) : f));
    assert.deepEqual(summaryUntraced(input, first.id), ["2026-11-02", "31 December 2025"]);
    assert.deepEqual(summaryUntraced(input, first.id, { [first.id]: { "checkedOn@date": 20261102, "supportEnded@date": 20251231 } }), []);
    // Another finding's date backs nothing here, and a day that isn't the one named stays untraced.
    assert.deepEqual(summaryUntraced(input, first.id, { [second.id]: { "checkedOn@date": 20261102, "supportEnded@date": 20251231 } }), ["2026-11-02", "31 December 2025"]);
    assert.deepEqual(summaryUntraced(input, first.id, { [first.id]: { "checkedOn@date": 20261103, "supportEnded@date": 20251231 } }), ["2026-11-02"]);
  });

  test("named() keeps a date as yyyymmdd under name@date, and refuses what isn't one", () => {
    withNamedValues(() =>
      namedFor("rule:a", () => {
        named("Planted check on 2026-11-02 and in 2026-02", { checkedOn: ["2026-11-02", "date"], month: ["2026-02", "date"], not: ["soon", "date"] });
        assert.deepEqual(namedIn(["Planted check on 2026-11-02 and in 2026-02"], "rule:a"), { "checkedOn@date": 20261102, "month@date": 20260200 });
      }),
    );
  });

  test("the end-of-support sentence is made from the one constant its date is named with", () => {
    assert.equal(SUPPORT_ENDED_ON, "2025-12-31");
    assert.equal(END_OF_SUPPORT, "Salesforce ended support for Workflow Rules and Process Builder on 31 December 2025. They keep running, but any bug is yours to work around.");
  });

  test("a word that only holds a unit's letters says nothing", () => {
    const input = inputWith((f) => (f.id === first.id ? withSummary(f, "About 25% of them, 30% and 35%.", {}) : f));
    assert.deepEqual(summaryUntraced(input, first.id, { [first.id]: { generated: 25, separate: 30, operator: 35 } }), ["25%", "30%", "35%"]);
    assert.deepEqual([unitOf("generated"), unitOf("separate"), unitOf("operator"), unitOf("worstSegmentRate"), unitOf("DataStorageMBShare"), unitOf("days"), unitOf("limit@percent"), unitOf("dealsMoney#2")], [null, null, null, "percent", "percent", "duration:day", "percent", "currency"]);
  });
});

describe("named values", () => {
  test("give the text back unchanged, and are found only in a text that holds the sentence whole", () => {
    withNamedValues(() => {
      const text = namedFor("rule:a", () => named("Planted deals with no widgets in 91 days", { widgetDays: 91, missing: null }));
      assert.equal(text, "Planted deals with no widgets in 91 days");
      assert.deepEqual(namedIn([`${text}: 18 of 76.`], "rule:a"), { widgetDays: 91 });
      assert.equal(namedIn(["Planted deals with no widgets in 61 days"], "rule:a"), null);
    });
  });

  test("two sentences' values under one name are both kept, and a unit given goes with its value", () => {
    withNamedValues(() => {
      namedFor("rule:a", () => {
        named("3 planted widgets", { widgets: 3 });
        named("and 5 planted widgets at 25%", { widgets: 5, limit: [25, "percent"] });
      });
      assert.deepEqual(namedIn(["3 planted widgets and 5 planted widgets at 25%"], "rule:a"), { widgets: 3, "widgets#2": 5, "limit@percent": 25 });
    });
  });

  test("a finding takes only what its own rule named, never another rule's same sentence", () => {
    withNamedValues(() => {
      namedFor("rule:a", () => named("Weighted by your own planted rate of 47%", { winPct: 47 }));
      assert.equal(namedIn(["Weighted by your own planted rate of 47%, more."], "rule:b"), null);
      assert.deepEqual(namedIn(["Weighted by your own planted rate of 47%, more."], "rule:a"), { winPct: 47 });
    });
  });

  test("a module-level sentence lends only the values it writes", () => {
    // Outside an analysis, as a rule's fixed title is made when its module loads.
    named("Planted fixed title in 77 days", { titleDays: 77, other: 5 });
    withNamedValues(() => assert.deepEqual(namedIn(["Planted fixed title in 77 days"], "rule:b"), { titleDays: 77 }));
  });

  test("an empty or blank text names nothing", () => {
    withNamedValues(() =>
      namedFor("rule:a", () => {
        assert.equal(named("", { n: 4 }), "");
        assert.equal(named("   ", { n: 4 }), "   ");
        assert.equal(namedIn(["", "   ", "any text"], "rule:a"), null);
      }),
    );
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

  test("nor the report or the figure set it is given", () => {
    const meddling = check(input, {
      strict: (digest, report, set, named) => {
        const out = strictShadow(digest, report, set, named);
        report.teamFindings.length = 0;
        report.metrics.length = 0;
        set.figures.length = 0;
        set.dates.length = 0;
        return out;
      },
    });
    assert.deepEqual(served(meddling), served(real));
  });

  test("a shadow past its budget gives up with \"timeout\" inside its loop, and costs the run nothing", () => {
    const report = buildReport(input);
    const digest = buildDigest({ report, pseudonyms: automatic.pseudonyms, inventory: null, madeAt: MADE_AT });
    let t = 0;
    assert.throws(() => strictShadow(digest, report, digestDataset(report, automatic.pseudonyms), {}, 5, () => (t += 1)), ShadowTimeout);
    const slow = check(input, { strict: (d, r, s, n) => strictShadow(d, r, s, n, 0, () => (t += 1)) });
    assert.deepEqual(slow.strictShadow, { ok: false, failed: "timeout" });
    assert.deepEqual(served(slow), served(real));
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
    // A file kept by an earlier delivery goes when this one's can't be written.
    writeFileSync(join(dir, STRICT_SHADOW_FILE), "{}");
    const unwritable = { ok: true as const, shadow: { ...(real.strictShadow as { shadow: StrictShadow }).shadow, checked: 1n as unknown as number } };
    keepStrictShadow(dir, "hc-run", unwritable, (l) => lines.push(l));
    assert.equal(existsSync(join(dir, STRICT_SHADOW_FILE)), false);
    assert.match(lines.at(-1)!, /couldn't be kept \(TypeError\)/);
  });

  test("an email, a link, a record id or a word with digits is recorded as \"(identifier)\", by its kind, never as written", () => {
    const empty = { figures: [], dates: [], identifiers: [], tokens: [], labels: [] };
    for (const t of ["Ask jo.smith@acme.com or see https://acme.my.salesforce.com/home today.", "Record 0065g00000AbCdE is owned by Jo.", "See acme.com and ACME-UK and v2.", "Email ops@example.com now.", "The field Revenue_Q3 is blank.", "Stage Discovery9 is used."]) {
      const out = strictUntraced(t, [], empty).unmatched;
      assert.ok(out.length > 0 && out.every((u) => u === "(identifier)" || u === "(token)"), `${t}: ${out}`);
    }
    assert.deepEqual(strictUntraced("Rep owns 5 deals.", [], empty).unmatched, ["5"]);
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
