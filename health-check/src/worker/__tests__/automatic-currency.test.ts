import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { orgProfile } from "../../benchmark/export.ts";
import { automaticCalibration } from "../../connect/auto-calibration.ts";
import { checkedDigest } from "../../digest/checked.ts";
import { CURRENCY_NOT_NAMED, CURRENCY_SIGNS, noCurrencyCode, CURRENCY_UNKNOWN, NO_CURRENCY, NO_MONEY_FIGURE } from "../../engine/currency.ts";
import type { OpportunityExtract } from "../../extract/opportunities.ts";
import { buildReport } from "../../narrative/sections.ts";
import { applyNarrative } from "../../narrative/apply.ts";
import type { NarrativeClient } from "../../narrative/client.ts";
import { keepAmountsNote, NO_CURRENCY_CODE } from "../../narrative/draft.ts";
import { executiveHtml } from "../../render/executive.ts";
import { AMOUNT_MARK, escapeXml, probingAmounts } from "../../render/format.ts";
import type { DomainModule } from "../../domains/types.ts";
import { said } from "../../engine/org-words.ts";
import { moneySaid } from "../../engine/said-forms.ts";
import { fullReportHtml } from "../../render/full-report.ts";
import { onePagerHtml } from "../../render/one-pager.ts";
import { loadRun } from "../../render/render-run.ts";
import { Calibration, type Finding } from "../../schemas/index.ts";
import { checkNumbers, type FigureSet } from "../../numbercheck/index.ts";
import { AutomaticReplay, stagesOf } from "../../testing/automatic.ts";
import { analyseExtracts, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import { buildValueModel, HOURS_IN_POUNDS, HOURS_NOT_NAMED, valueCurrency } from "../../value/model.ts";

// An automatic run's currency over the synthetic org (DECISIONS.md 700): read
// from the org (a code); one currency not known by name (amounts shown as
// plain numbers, every place that shows one saying so, nothing guessed); or
// can't tell (no money figure, the rest measured as before).

const first = await runFromRecording();
const stages = stagesOf(first.dir);
const calibrate = (currency: Parameters<typeof automaticCalibration>[1]["currency"]): Calibration => {
  const built = automaticCalibration(stages, { orgId: first.run.orgId, at: "2026-10-31T09:00:00.000Z", currency });
  assert.ok(built.ok);
  return built.calibration;
};
const run = async (calibration: Calibration) => {
  const { dir } = await runFromRecording({ calibration, reviewMode: "automatic", transport: new AutomaticReplay() });
  return { ...loadRun(dir), dir };
};

const read = await run(calibrate("GBP"));
const single = await run(calibrate({ kind: "single", suggested: "GBP" }));
const unread = await run(calibrate(null));
const NOTE = CURRENCY_NOT_NAMED;
const NOTE_HTML = escapeXml(NOTE);
const MADE = "2026-11-24T09:00:00.000Z";
type Loaded = Pick<typeof read, "input" | "pseudonyms">;

/** Every text a run serves the customer, by where it is: never a currency's code, the codes kept in structural fields apart. */
function served(loaded: Loaded): Array<[string, string]> {
  const report = buildReport(loaded.input);
  const digest = checkedDigest({ input: loaded.input, pseudonyms: loaded.pseudonyms, inventory: null, madeAt: MADE }).digest!;
  // Codes held to their shape, never printed as text: a value's unit, the model's currency, the locale's suggestion (the console's contract).
  const STRUCTURAL = new Set(["unit", "currency", "currencySuggested"]);
  const walk = (v: unknown, path: string): Array<[string, string]> =>
    typeof v === "string" ? [[path, v]] : Array.isArray(v) ? v.flatMap((x, i) => walk(x, `${path}/${i}`)) : v && typeof v === "object" ? Object.entries(v).flatMap(([k, x]) => (STRUCTURAL.has(k) ? [] : walk(x, `${path}/${k}`))) : [];
  const bodies = Object.values(report.outputs).flatMap((o) => o.versions.map((v): [string, string] => [`section ${v.sectionId}`, v.body]));
  const pages = ([["full", fullReportHtml(report, loaded.pseudonyms)], ["executive", executiveHtml(report, loaded.pseudonyms)], ["one-pager", onePagerHtml(report, loaded.pseudonyms)]] as const).flatMap(([name, html]) =>
    pagesOf(html).map((p, i): [string, string] => [`${name} page ${i + 1}`, p]),
  );
  return [...walk(loaded.input.findings, "findings"), ...walk(loaded.input.metrics, "metrics"), ...walk(digest, "digest"), ...bodies, ...pages];
}

/** A document's pages, as printed, with the styles taken out. */
const pagesOf = (html: string): string[] => html.replace(/<style[\s\S]*?<\/style>/g, "").split(/(?=<section class="page)/).slice(1);
const money = (f: Finding) => f.status === "issue" && f.value.kind !== "not_sized" && /^[A-Z]{3}$/.test(f.value.unit);
const byId = <T extends { id: string }>(xs: T[]) => new Map(xs.map((x) => [x.id, x]));

describe("an automatic run on a single-currency org: amounts shown, no currency named", () => {
  test("no code or symbol anywhere a customer reads, in any text, section or page", () => {
    for (const [where, text] of served(single)) assert.ok(!CURRENCY_SIGNS.test(text), `${where}: ${text.slice(Math.max(0, text.search(CURRENCY_SIGNS) - 100), text.search(CURRENCY_SIGNS) + 30)}`);
  });

  test("the same amounts as a run that read the currency, measured at the same confidence, each finding with an amount saying so", () => {
    const r = byId(read.input.findings);
    assert.equal(single.input.findings.length, read.input.findings.length);
    let said = 0;
    for (const f of single.input.findings) {
      const was = r.get(f.id)!;
      assert.equal(f.status, was.status, f.ruleId);
      assert.equal(f.confidence, was.confidence, f.ruleId);
      assert.equal(f.severity, was.severity, f.ruleId);
      if (money(was)) {
        assert.ok(money(f) && f.value.kind !== "not_sized" && was.value.kind !== "not_sized" && f.value.amount === was.value.amount && f.value.unit === NO_CURRENCY, f.ruleId);
        assert.equal(f.amountsUnnamed, true, f.ruleId);
        said++;
      }
      if (/£/.test(JSON.stringify(was))) assert.equal(f.amountsUnnamed, true, `${f.ruleId} says an amount`);
      // The line is never the finding's own text, so never its fix prompt's (fix round 2).
      assert.ok(!f.summary.includes(NOTE) && !(f.fixPrompt ?? "").includes(NOTE), f.ruleId);
    }
    assert.ok(said >= 12, `${said}`);
    const rm = byId(read.input.metrics);
    for (const m of single.input.metrics) {
      const was = rm.get(m.id)!;
      assert.equal(m.status, was.status, m.id);
      if (m.status === "measured" && was.status === "measured") {
        assert.equal(m.value, was.value, m.id);
        if (m.unit === "currency") assert.ok(m.assumptions.includes(NOTE), m.id);
      }
    }
  });

  test("the score and every confidence are exactly a named currency's: nothing is guessed, so nothing moves", () => {
    const count = (fs: Finding[]) => fs.map((f) => `${f.status}/${f.confidence}`).sort();
    assert.deepEqual(count(single.input.findings), count(read.input.findings));
    assert.deepEqual(single.input.scores, read.input.scores);
    assert.deepEqual(buildReport(single.input).overall, buildReport(read.input).overall);
  });

  test("every page and section that shows an amount carries the line, once", () => {
    const report = buildReport(single.input);
    let noted = 0;
    for (const make of [() => fullReportHtml(report, single.pseudonyms), () => executiveHtml(report, single.pseudonyms), () => onePagerHtml(report, single.pseudonyms)]) {
      // Made with every amount marked, to find each page that prints one.
      const marked = pagesOf(probingAmounts(make));
      const pages = pagesOf(make());
      assert.equal(marked.length, pages.length);
      pages.forEach((page, i) => {
        if (marked[i]!.includes(AMOUNT_MARK)) {
          assert.ok(page.includes(NOTE_HTML), `page ${i + 1}: ${page.replace(/<[^>]+>/g, " ").slice(0, 160)}`);
          noted++;
        }
        // The page's own line at most once (a finding's text on it may say it too, as its own).
        if (marked[i]!.includes(AMOUNT_MARK)) assert.equal(page.split(NOTE_HTML).length - 1, 1, `page ${i + 1} says it exactly once`);
        // No amount printed on the page itself: no line of the page's own (a section's text on it may end with its own).
        else assert.ok(!page.includes("amounts-note"), `page ${i + 1} shows no amount, so adds no line`);
        assert.ok(!page.includes(AMOUNT_MARK));
      });
    }
    assert.ok(noted >= 20, `${noted}`);
    // The pages the review found without it, each by what it shows: found by the marked amounts, and each says it.
    const all = [fullReportHtml, executiveHtml].flatMap((make) => {
      const marked = pagesOf(probingAmounts(() => make(report, single.pseudonyms)));
      return pagesOf(make(report, single.pseudonyms)).map((page, i) => ({ page, amount: marked[i]!.includes(AMOUNT_MARK) }));
    });
    for (const shows of ["Top issues", "What it costs", "Open pipeline by stage", "Quarter by quarter", "What fixing this is worth"]) {
      const with_ = all.filter((p) => p.amount && p.page.includes(shows));
      assert.ok(with_.length > 0, shows);
      for (const p of with_) assert.ok(p.page.includes(NOTE_HTML), shows);
    }
    // Sections: each whose text prints an amount ends with the line; the same section where the currency is named shows a pound sign.
    const keyOf = (sectionId: string) => sectionId.split(":").slice(1).join(":");
    const before = new Map(Object.values(buildReport(read.input).outputs).flatMap((o) => o.versions.map((v) => [keyOf(v.sectionId), v.body] as const)));
    let sections = 0;
    for (const v of Object.values(report.outputs).flatMap((o) => o.versions)) {
      if (/£\d/.test(before.get(keyOf(v.sectionId)) ?? "")) {
        assert.ok(v.body.endsWith(NOTE) && v.body.split(NOTE).length === 2, v.sectionId);
        sections++;
      }
    }
    assert.ok(sections >= 3, `${sections}`);
  });

  test("released, with the line and the locale's suggestion in the digest, each marked", () => {
    const checked = checkedDigest({ input: single.input, pseudonyms: single.pseudonyms, inventory: null, madeAt: MADE });
    assert.equal(checked.held, null);
    const value = checked.digest!.value;
    assert.equal(value.currency, NO_CURRENCY);
    assert.equal(value.currencyNote, NOTE);
    assert.equal(value.currencySuggested, "GBP");
    assert.deepEqual(value.orgWords, { currencyNote: { orgWordsMarked: true, orgWords: [] }, currencySuggested: { orgWordsMarked: true, orgWords: [{ start: 0, end: 3 }] } });
    assert.ok(fullReportHtml(buildReport(single.input), single.pseudonyms).includes(escapeXml("Your Salesforce org's own currency, which you haven't told us yet")));
  });
});

describe("an automatic run whose currency read failed", () => {
  test("opportunities that show one currency: amounts shown as for a single-currency org", () => {
    assert.deepEqual(unread.input.findings.map((f) => [f.id, f.status, f.summary]), single.input.findings.map((f) => [f.id, f.status, f.summary]));
  });

  test("can't tell (the opportunities couldn't be described): every amount a rule or metric would give is taken out, for the plain reason, nothing named, nothing else moved", () => {
    const extracts = loadExtracts(unread.dir);
    const opps = extracts.get("opportunities") as OpportunityExtract;
    extracts.set("opportunities", { ...opps, opportunities: { status: "unavailable", reason: "refused", source: (opps.opportunities as unknown as { source: never }).source } });
    // A rule and a metric that put amounts in money, as any rule may.
    const ev = (ctx: { ev: (s: never) => never }) => [ctx.ev({ kind: "soql", api: "rest", query: "SELECT Id FROM Account" } as never)];
    const probe: DomainModule = {
      id: "data_quality",
      cloud: "sales",
      label: "Probe",
      extractors: [],
      metrics: [(ctx) => [{ status: "measured", id: "probe.value", domain: "data_quality", label: "Value", definition: "A value.", unit: "currency", evidence: ev(ctx as never), exclusions: [], assumptions: [], value: 1200, n: 3, dateRange: null, segments: [], segmentExemptReason: "One figure.", series: [], stats: {} }]],
      rules: [
        { id: "probe.money", version: 1, domain: "data_quality", title: "Money", evaluate: (ctx) => ({ outcome: "issue", title: "Some deals", summary: said`Three deals worth ${moneySaid(1200, ctx.calibration.currency)}.`, recommendation: "Look.", recommendationType: "investigate", severity: "medium", confidence: "high", evidence: ev(ctx as never), value: { kind: "revenue", amount: 1200, unit: ctx.calibration.currency, period: "open pipeline today", assumptions: [said`A sum`] }, effort: { size: "S" }, owner: "Sales", segmentExemptReason: "x" }) },
        { id: "probe.count", version: 1, domain: "data_quality", title: "Count", evaluate: (ctx) => ({ outcome: "issue", title: "Some deals", summary: said`Three deals.`, recommendation: "Look.", recommendationType: "investigate", severity: "medium", confidence: "high", evidence: ev(ctx as never), value: { kind: "revenue", amount: 1200, unit: ctx.calibration.currency, period: "open pipeline today", assumptions: [said`A sum`] }, effort: { size: "S" }, owner: "Sales" }) },
      ],
    };
    const out = analyseExtracts(extracts, { calibration: calibrate(null), modules: [probe] });
    const [money_, count] = out.findings;
    assert.equal(money_!.status, "not_measurable");
    assert.equal(money_!.notMeasurableReason, CURRENCY_UNKNOWN);
    assert.equal(money_!.notMeasurableKind, "needs_input");
    assert.equal(count!.status, "issue");
    assert.deepEqual(count!.value, { kind: "not_sized", reason: CURRENCY_UNKNOWN });
    assert.equal(count!.confidence, "high");
    assert.ok(out.metrics[0]!.status === "not_measurable" && out.metrics[0]!.reason === CURRENCY_UNKNOWN);
    assert.ok(!CURRENCY_SIGNS.test(JSON.stringify([out.findings, out.metrics])));
    // The same rules where the opportunities show one currency: amounts plain, said, confidence as it was.
    extracts.set("opportunities", opps);
    const plain = analyseExtracts(extracts, { calibration: calibrate(null), modules: [probe] });
    assert.equal(plain.findings[0]!.summary, "Three deals worth 1,200.");
    assert.equal(plain.findings[0]!.amountsUnnamed, true);
    assert.equal(plain.findings[1]!.amountsUnnamed, true);
    assert.equal(out.findings[1]!.amountsUnnamed, undefined);
    assert.equal(plain.findings[0]!.confidence, "high");
    assert.ok(plain.metrics[0]!.status === "measured" && plain.metrics[0]!.assumptions.includes(NOTE));
  });
});

describe("money beyond the findings", () => {
  test("hours: our pounds benchmark only beside pounds; a currency not known by name leaves them as hours, naming none", () => {
    const person = { ...read.input.calibration, source: "confirmed" as const };
    assert.deepEqual(valueCurrency(person), {});
    assert.deepEqual(valueCurrency({ ...person, currency: "EUR" }), {});
    assert.deepEqual(valueCurrency(calibrate("GBP")), {});
    assert.deepEqual(valueCurrency(calibrate("EUR")), { unpricedHours: HOURS_IN_POUNDS("EUR") });
    assert.deepEqual(valueCurrency(calibrate("EUR"), 40), {});
    assert.deepEqual(valueCurrency(calibrate({ kind: "single", suggested: null })), { unpricedHours: HOURS_NOT_NAMED, note: NOTE });
    assert.deepEqual(valueCurrency(calibrate({ kind: "single", suggested: null }), 40), { note: NOTE });
    assert.ok(!CURRENCY_SIGNS.test(HOURS_NOT_NAMED.replace("pounds", "")));
  });

  test("a consultant-led run in another currency prices hours exactly as before", () => {
    const hours = { ...read.input.findings.find((f) => f.status === "issue")!, value: { kind: "hours" as const, amount: 10, unit: "hours", period: "per year", assumptions: ["a"] } };
    for (const currency of ["USD", "EUR"]) {
      const person = { ...read.input.calibration, source: "confirmed" as const, currency };
      const model = buildValueModel([hours], read.input.run, currency, null, null, valueCurrency(person));
      assert.deepEqual(model, buildValueModel([hours], read.input.run, currency, null, null));
      assert.equal(model.left.length, 0);
      assert.ok(model.total.expected > 0);
    }
  });

  test("the survey's hours are left out of the money when hours are unpriced, and never listed as a finding left out", () => {
    const team = { hoursPerWeek: 2, users: 10, usersFrom: "active users" };
    const plain = buildValueModel([], read.input.run, NO_CURRENCY, null, team as never, valueCurrency(calibrate({ kind: "single", suggested: null })));
    assert.deepEqual(plain.groups, []);
    assert.deepEqual(plain.left, []);
    assert.ok(buildValueModel([], read.input.run, "GBP", null, team as never, valueCurrency(calibrate("GBP"))).total.expected > 0);
  });

  test("where money can't be shown, the page says why, not that it matters for its risk", () => {
    const findings = single.input.findings.map((f) => (f.status === "issue" ? { ...f, value: { kind: "not_sized" as const, reason: CURRENCY_UNKNOWN } } : f));
    const report = buildReport({ ...single.input, findings });
    const page = onePagerHtml(report, single.pseudonyms) + fullReportHtml(report, single.pseudonyms);
    assert.ok(page.includes(escapeXml(NO_MONEY_FIGURE)));
    assert.ok(!page.includes("They matter for the risk they carry"));
    // Where money is simply absent, the sentence stands as it was.
    const none = buildReport({ ...read.input, findings: read.input.findings.map((f) => (f.status === "issue" ? { ...f, value: { kind: "not_sized" as const, reason: "x" } } : f)) });
    assert.ok(onePagerHtml(none, read.pseudonyms).includes("They matter for the risk they carry"));
  });

  test("the benchmark keeps out the deal size band of a run whose currency isn't known by name", () => {
    for (const loaded of [single, unread]) assert.throws(() => orgProfile(loaded.dir, loaded.input.run, loaded.input.calibration), /currency/);
  });
});

describe("Claude's drafts on a run whose currency isn't known by name", () => {
  const sourceIds = (prompt: string): string[] => (JSON.parse(prompt.slice(prompt.indexOf("<sources>") + 9, prompt.indexOf("</sources>"))) as Array<{ id: string }>).map((x) => x.id).slice(0, 1);
  const writing = (text: (prompt: string) => string, prompts: string[] = []): NarrativeClient => ({
    complete: async ({ prompt, model }) => (prompts.push(prompt), { stopReason: "end_turn", text: JSON.stringify({ paragraphs: [{ text: text(prompt), sources: sourceIds(prompt) }] }), model }),
  });

  test("a draft naming a currency, by code or symbol, is sent back", async () => {
    for (const claim of ["Your pipeline is worth £3m.", "Your pipeline is worth 3m GBP.", "Your pipeline is worth $3m."]) {
      const report = buildReport(single.input);
      const rejections = await applyNarrative(report, writing(() => claim), single.pseudonyms);
      assert.ok(rejections.length > 0 && rejections.every((r) => r.reasons.includes(NO_CURRENCY_CODE)), claim);
    }
  });

  test("a section whose engine text carries the line keeps it, or its draft goes back; the source tells Claude to write plain numbers", async () => {
    const report = buildReport(single.input);
    const prompts: string[] = [];
    const rejections = await applyNarrative(report, writing(() => "We looked at your pipeline.", prompts), single.pseudonyms);
    assert.ok(rejections.some((r) => r.reasons.includes(keepAmountsNote(NOTE))));
    assert.ok(prompts.some((p) => p.includes("write amounts as plain numbers with no symbol or code")));
    const kept = buildReport(single.input);
    await applyNarrative(kept, writing(() => `We looked at your pipeline. ${NOTE}`), single.pseudonyms);
    assert.ok(Object.values(kept.outputs).flatMap((o) => o.versions).some((v) => v.source === "claude_draft" && v.body.includes(NOTE)));
  });
});

describe("the currency line, said once where it belongs (DECISIONS.md 700, fix round 2)", () => {
  const count = (text: string): number => text.split(NOTE).length - 1;

  test("in the synthetic single-currency run: never in a finding, once as the digest's note, once per money metric, once per page and section with an amount", () => {
    const report = buildReport(single.input);
    const digest = checkedDigest({ input: single.input, pseudonyms: single.pseudonyms, inventory: null, madeAt: MADE }).digest!;
    const flagged = single.input.findings.filter((f) => f.amountsUnnamed);
    assert.equal(flagged.length, 14);
    assert.equal(count(JSON.stringify(single.input.findings)), 0);
    assert.equal(count(JSON.stringify(digest.findings)), 0);
    assert.equal(digest.findings.filter((f) => f.amountsUnnamed).length, report.teamFindings.filter((f) => f.amountsUnnamed).length);
    assert.equal(count(digest.value.currencyNote ?? ""), 1);
    const moneyMetrics = digest.metrics.filter((m) => m.status === "measured" && m.unit === "currency");
    assert.equal(moneyMetrics.length, 10);
    assert.equal(count(JSON.stringify(digest.metrics)), 10);
    assert.equal(count(JSON.stringify(digest)), 11);
    // Pages: the line once on each page with an amount, as the test above checks page by page; in all:
    const pages = [fullReportHtml, executiveHtml, onePagerHtml].map((make) => make(report, single.pseudonyms).split(NOTE_HTML).length - 1);
    assert.deepEqual(pages, PAGE_LINES);
  });

  test("a page whose only amount is said in a flagged finding's own text still carries the line", () => {
    // No formatted amount anywhere: every sized figure taken away and every metric in money left out.
    const findings = single.input.findings.map((f) => (f.amountsUnnamed && f.status === "issue" ? { ...f, value: { kind: "not_sized" as const, reason: "Not sized here." } } : f));
    const metrics = single.input.metrics.filter((m) => m.unit !== "currency");
    const report = buildReport({ ...single.input, findings, metrics });
    assert.equal(report.value.currencyNote, NOTE);
    const pages = pagesOf(fullReportHtml(report, single.pseudonyms));
    let seen = 0;
    for (const f of report.teamFindings.filter((x) => x.amountsUnnamed && /\d{3}/.test(x.summary))) {
      const at = escapeXml(f.summary.slice(0, 40));
      for (const page of pages.filter((p) => p.includes(at))) {
        assert.ok(page.includes(NOTE_HTML), f.ruleId);
        seen++;
      }
    }
    assert.ok(seen > 0);
  });

  test("a run that can't tell its currency shows no amount, so carries no line", () => {
    const extracts = loadExtracts(unread.dir);
    const opps = extracts.get("opportunities") as OpportunityExtract;
    extracts.set("opportunities", { ...opps, opportunities: { status: "unavailable", reason: "refused", source: (opps.opportunities as unknown as { source: never }).source } });
    const out = analyseExtracts(extracts, { calibration: calibrate(null) });
    const report = buildReport({ ...unread.input, findings: out.findings, metrics: out.metrics });
    assert.equal(report.value.currencyNote, undefined);
    assert.ok(!fullReportHtml(report, unread.pseudonyms).includes(NOTE_HTML));
  });

  test("a person's calibration never has no currency, so never prints plain numbers", () => {
    const person = { ...read.input.calibration, source: "confirmed" as const };
    for (const code of [NO_CURRENCY, "XTS"]) {
      assert.equal(Calibration.safeParse({ ...person, currency: code }).success, false, code);
      assert.equal(noCurrencyCode({ source: "confirmed", currency: code }), false, code);
      assert.equal(Calibration.safeParse({ ...person, source: "org_settings", currency: code }).success, true, code);
    }
    assert.equal(noCurrencyCode({ source: "org_settings", currency: NO_CURRENCY }), true);
  });

  test("a plain number never traces to an amount in the testing code, only in no currency", () => {
    const set = (currency: string): FigureSet => ({ figures: [{ source: "f1", kind: "currency", value: 445, currency }], dates: [], identifiers: [], tokens: [], labels: [] }) as unknown as FigureSet;
    assert.equal(checkNumbers("They are worth 445.", set("XTS")).passed, false);
    assert.equal(checkNumbers("They are worth 445.", set("GBP")).passed, false);
    assert.equal(checkNumbers("They are worth 445.", set(NO_CURRENCY)).passed, true);
  });

  test("a draft naming a currency in words, with or without a number, is sent back", async () => {
    const sourceIds = (prompt: string): string[] => (JSON.parse(prompt.slice(prompt.indexOf("<sources>") + 9, prompt.indexOf("</sources>"))) as Array<{ id: string }>).map((x) => x.id).slice(0, 1);
    for (const claim of ["Your figures are in pounds.", "Worth 3m dollars.", "In euros.", "In sterling.", "In yen.", "In rupees.", "A few quid."]) {
      const report = buildReport(single.input);
      const rejections = await applyNarrative(report, { complete: async ({ prompt, model }) => ({ stopReason: "end_turn", text: JSON.stringify({ paragraphs: [{ text: claim, sources: sourceIds(prompt) }] }), model }) }, single.pseudonyms);
      assert.ok(rejections.length > 0 && rejections.every((r) => r.reasons.includes(NO_CURRENCY_CODE)), claim);
    }
  });
});

/** The line in all, in the full report, the executive summary and the one-pager: one per page that prints an amount. */
const PAGE_LINES: number[] = [21, 6, 1];

