import { issueUnder } from "../../engine/loosened.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { buildDigest, digestText, inventoryNameNamesPerson, inventoryNames, withheldNames } from "../build.ts";
import { Pseudonymiser } from "../../privacy/pseudonymiser.ts";
import { buildReport, VALUE_TOTAL, type BuiltReport } from "../../narrative/sections.ts";
import { buildPlan } from "../../narrative/select.ts";
import { loadRun } from "../../render/render-run.ts";
import { NAMED_PEOPLE_SENTENCE, RunDigest } from "../../schemas/digest.ts";
import type { NumberOverride } from "../../schemas/index.ts";
import type { Inventory } from "../../schemas/inventory.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { SIZED_RULE } from "../../testing/recheck.ts";
import { loadAnswers } from "../../testing/replay.ts";
import { HEADLINE_KINDS } from "../../value/model.ts";
import { executiveHtml } from "../../render/executive.ts";
import { compactMoney, money } from "../../render/format.ts";

// The run digest (DECISIONS.md 632): made by code from the built report the
// documents print, every figure under what it traces to. These are the
// design's six engine tests, the exit test in miniature (every number traces),
// and the text guard.

const { dir } = await runFromRecording({ answers: loadAnswers() });
const MADE_AT = "2026-11-21T09:00:00.000Z";
const loaded = loadRun(dir);
const { pseudonyms } = loaded;
const reportWith = (overrides: NumberOverride[] = []): BuiltReport => buildReport({ ...loaded.input, overrides });
const report = reportWith();
const digest = buildDigest({ report, pseudonyms, inventory: null, madeAt: MADE_AT });

const round2 = (n: number): number => Math.round(n * 100) / 100;
const near = (a: number, b: number): boolean => Math.abs(a - b) < 0.011;

/** Every number in a value, with its path. A sentence's org-word spans are positions in its text, not figures (DECISIONS.md 655). */
function numbers(value: unknown, path: Array<string | number> = []): Array<{ path: Array<string | number>; value: number }> {
  if (typeof value === "number") return [{ path, value }];
  if (Array.isArray(value)) return value.flatMap((v, i) => numbers(v, [...path, i]));
  if (value && typeof value === "object") return Object.entries(value).flatMap(([k, v]) => (k === "orgWords" ? [] : numbers(v, [...path, k])));
  return [];
}

/** Every string in a value, keys included. */
function strings(value: unknown): string[] {
  if (typeof value === "string") return [value];
  if (Array.isArray(value)) return value.flatMap(strings);
  if (value && typeof value === "object") return Object.entries(value).flatMap(([k, v]) => [k, ...strings(v)]);
  return [];
}

/**
 * What a digest figure names, read back from the built report or the stored
 * rows it came from, by its own path and the id beside it. Undefined when the
 * path names nothing we can resolve, which fails the test.
 */
function resolve(d: RunDigest, r: BuiltReport, path: Array<string | number>): number | undefined {
  const [top, i, field, sub] = path;
  const plan = buildPlan(r.teamFindings, r.added);
  const lineById = new Map(d.value.lines.map((l) => [l.id, l]));
  const reportLine = (id: string) => r.value.groups.flatMap((g) => g.lines).find((l) => l.findingId === id);
  const sumOf = (ids: string[], c: "low" | "expected" | "high") => round2(ids.reduce((s, id) => s + lineById.get(id)![c], 0));
  if (top === "version") return 1;
  if (top === "overall" && i === "score") return r.overall.status === "measured" ? r.overall.score : undefined;
  if (top === "areas" && field === "score") {
    const area = d.areas[i as number]!;
    const stored = (r.input.scores ?? []).find((s) => s.domain === area.domain);
    return stored?.status === "measured" ? stored.score : undefined;
  }
  if (top === "findings" && field === "value" && sub === "amount") {
    const f = d.findings[i as number]!;
    const built = r.teamFindings.find((x) => x.id === f.id);
    return built?.value.kind === "not_sized" ? undefined : built?.value.amount;
  }
  if (top === "metrics" && (field === "value" || field === "n")) {
    const m = d.metrics[i as number]!;
    const built = r.metrics.find((x) => x.id === m.id);
    return built?.status === "measured" ? built[field] : undefined;
  }
  // A duration's average beside its median (DECISIONS.md 709): the built measure's own stat.
  if (top === "metrics" && field === "average") {
    const built = r.metrics.find((x) => x.id === d.metrics[i as number]!.id);
    return built?.status === "measured" ? built.stats.mean : undefined;
  }
  if (top === "plan" && i === "phases" && (sub === "from" || sub === "to")) return plan.phases[field as number]![sub];
  // An issue's hours (DECISIONS.md 628, 636): the built finding's own.
  // A loosened rule's version, and a pass's verdict on the old line (DECISIONS.md 732): the finding's own version, and issueUnder's.
  if (top === "findings" && field === "ruleVersion") return r.teamFindings.find((x) => x.id === d.findings[i as number]!.id)?.ruleVersion;
  if (top === "findings" && field === "carriedFrom") return r.progress?.carried?.[d.findings[i as number]!.ruleId];
  if (top === "findings" && field === "issueUnder") {
    const own = r.teamFindings.find((x) => x.id === d.findings[i as number]!.id);
    return own ? issueUnder(own, r.input.run.budget.callsMade)?.[sub as number] : undefined;
  }
  if (top === "findings" && field === "effortHours" && (sub === "low" || sub === "high")) return r.teamFindings.find((x) => x.id === d.findings[i as number]!.id)?.effortHours?.[sub];
  // The plan's hours, built independently of the builder's walk: every issue in the digest with hours that the plan names anywhere.
  if (top === "plan" && i === "hoursTotal" && (field === "low" || field === "high")) {
    const total = d.plan.hoursTotal!;
    const named = new Set([...d.plan.quickWins, ...d.plan.phases.flatMap((p) => p.findingIds), ...d.plan.later, ...Object.keys(d.plan.alsoFor), ...Object.values(d.plan.alsoFor).flat()]);
    const held = d.findings.filter((f) => f.status === "issue" && f.effortHours && named.has(f.id));
    const same = (a: string[], b: string[]) => [...a].sort().join() === [...b].sort().join();
    if (!same(total.findingIds, held.map((f) => f.id))) return undefined;
    if (!same(total.partialFindingIds, held.filter((f) => f.effortHours!.partial).map((f) => f.id))) return undefined;
    if (!same(total.withoutHoursFindingIds, d.findings.filter((f) => f.status === "issue" && !f.effortHours && named.has(f.id)).map((f) => f.id))) return undefined;
    return held.reduce((sum, f) => sum + r.teamFindings.find((x) => x.id === f.id)!.effortHours![field], 0);
  }
  // AI readiness's figures (DECISIONS.md 742): the built metric's own stat, headline or working line, by the metric and stat the figure names.
  if (top === "readiness" && i === "pillars" && sub === "figures" && path[5] === "value") {
    const fig = d.readiness!.pillars[field as number]!.figures![path[4] as number]!;
    if (!d.readiness!.pillars[field as number]!.metricIds.includes(fig.metricId)) return undefined;
    const built = r.metrics.find((m) => m.id === fig.metricId);
    if (built?.status !== "measured") return undefined;
    if (fig.stat === "value") return built.value;
    if (fig.stat === "workingLine:ready") return built.segments.find((s) => s.dimension === "working line" && s.key === "Ready up to")?.value ?? undefined;
    return Object.hasOwn(built.stats, fig.stat) ? built.stats[fig.stat] : undefined;
  }
  if (top !== "value") return undefined;
  // value.total.low, value.byKind.<kind>.low, value.exposure.low: the report's own, and the sum of the lines named.
  const c = (top === "value" && typeof field === "string" ? field : sub) as "low" | "expected" | "high";
  if (i === "total") {
    const own = r.value.total[c];
    return near(own, sumOf(d.value.total.lineIds, c)) ? own : undefined;
  }
  if (i === "byKind") {
    const kind = field as keyof typeof d.value.byKind;
    const own = r.value.byKind[kind][sub as "low" | "expected" | "high"];
    return near(own, sumOf(d.value.byKind[kind].lineIds, sub as "low")) ? own : undefined;
  }
  if (i === "exposure") {
    const own = r.value.byKind.exposure[c];
    return near(own, sumOf(d.value.exposure.lineIds, c)) ? own : undefined;
  }
  if (i === "groups" && sub === "realisation") return r.value.groups.find((g) => g.id === d.value.groups[field as number]!.id)?.realisation[path[4] as "low"];
  if (i === "lines") {
    const line = d.value.lines[field as number]!;
    if (sub === "added") return r.added.get(line.id) ?? 0;
    const own = reportLine(line.id);
    return own ? own[sub as "annual" | "low" | "expected" | "high"] : undefined;
  }
  return undefined;
}

describe("the run digest (DECISIONS.md 632)", () => {
  test("parses as the console's schema, names its run, and holds every finding and measure the report does", () => {
    assert.ok(RunDigest.safeParse(digest).success);
    assert.equal(digest.runId, report.input.run.id);
    assert.equal(digest.product, "barometer");
    assert.deepEqual(digest.findings.map((f) => f.id), report.teamFindings.map((f) => f.id));
    assert.deepEqual(digest.metrics.map((m) => m.id), report.metrics.map((m) => m.id));
  });

  // This reads the same report fields the builder copies, so it proves each figure sits under the right id and each total adds up the lines it lists,
  // not that the report is right. The next test reads the rendered documents instead.
  test("every number traces to the built report it was copied from: each figure equals the built or stored figure its path and id name, and the totals equal the lines they list", () => {
    const all = numbers(digest);
    assert.ok(all.length > 300, `${all.length} figures`);
    const untraced = all.filter(({ path, value }) => {
      const expected = resolve(digest, report, path);
      return expected === undefined || expected !== value;
    });
    assert.deepEqual(untraced.map((u) => u.path.join(".")), []);
    // The resolver isn't vacuous: a figure moved by a penny is caught.
    const moved = structuredClone(digest);
    moved.value.total.expected += 0.01;
    assert.notEqual(resolve(moved, report, ["value", "total", "expected"]), moved.value.total.expected);
    const line = moved.value.lines.find((l) => l.annual > 0)!;
    line.annual += 1;
    assert.notEqual(resolve(moved, report, ["value", "lines", moved.value.lines.indexOf(line), "annual"]), line.annual);
  });

  test("independently of the report object: the value total, a finding's value and an area's score are the figures the rendered executive summary prints", () => {
    const text = executiveHtml(report, pseudonyms).replace(/<[^>]+>/g, " ").replace(/\s+/g, " ");
    assert.ok(text.includes(compactMoney(digest.value.total.expected, digest.value.currency)), compactMoney(digest.value.total.expected, digest.value.currency));
    const f = digest.findings.find((x) => x.ruleId === SIZED_RULE && x.status === "issue")!;
    assert.ok(f.value.kind !== "not_sized" && text.includes(money(f.value.amount, digest.value.currency)), f.id);
    const area = digest.areas.find((a) => a.status === "measured")!;
    // The scorecard prints each area as its label, its weight, its score and its issues.
    assert.ok(area.status === "measured" && new RegExp(`${area.label} [\\d.]+ ${area.score} \\d+ issues?`).test(text), area.label);
  });

  test("its text is the report's team-level text, or the one fixed sentence, so its figures are the documents' own", () => {
    for (const f of digest.findings) {
      const built = report.teamFindings.find((x) => x.id === f.id)!;
      for (const key of ["title", "summary", "recommendation"] as const) assert.ok(f[key] === built[key] || f[key] === NAMED_PEOPLE_SENTENCE, `${f.id} ${key}`);
    }
  });

  test("the synthetic run's digest plan equals the report's plan, phase by phase", () => {
    const plan = buildPlan(report.teamFindings, report.added);
    assert.deepEqual(digest.plan.phases.map((p) => [p.key, p.from, p.to, p.findingIds]), plan.phases.map((p) => [p.key, p.from, p.to, p.findings.map((f) => f.id)]));
    assert.deepEqual(digest.plan.later, plan.later.map((f) => f.id));
    assert.deepEqual(digest.plan.quickWins, plan.quickWins.map((f) => f.id));
    assert.deepEqual(Object.keys(digest.plan.alsoFor).sort(), Object.keys(plan.alsoFor).sort());
    assert.ok(digest.plan.phases.every((p) => p.findingIds.length > 0), "the synthetic plan has an action in each phase");
  });

  test("the digest value totals equal the executive summary's figures", () => {
    const { total, byKind } = report.value;
    assert.deepEqual((({ lineIds: _ids, ...rest }) => rest)(digest.value.total), total);
    for (const kind of HEADLINE_KINDS) assert.deepEqual((({ lineIds: _ids, ...rest }) => rest)(digest.value.byKind[kind as keyof typeof digest.value.byKind]), byKind[kind]);
    assert.deepEqual((({ lineIds: _ids, ...rest }) => rest)(digest.value.exposure), byKind.exposure);
    assert.ok(total.expected > 0);
    // The figures the executive summary is number-checked against hold each of them as money.
    const held = report.dataset.figures.filter((f) => f.source === VALUE_TOTAL && f.kind === "currency").map((f) => f.value);
    for (const n of [total.low, total.expected, total.high]) assert.ok(held.includes(n), String(n));
    // What each issue adds, as the ranking reads it.
    for (const line of digest.value.lines) assert.equal(line.added, report.added.get(line.id) ?? 0);
  });

  test("a corrected figure appears corrected, in the finding, its value line and the totals; a measure's too", () => {
    const sized = report.teamFindings.find((f) => f.ruleId === SIZED_RULE && f.status === "issue" && f.value.kind !== "not_sized")!;
    const asRead = sized.value.kind === "not_sized" ? 0 : sized.value.amount;
    const row = (id: string, type: "finding" | "metric", field: string, previousValue: number, newValue: number, target: string): NumberOverride => ({ id, runId: report.input.run.id, target: { type, id: target, field }, previousValue, newValue, reason: "Checked with the customer.", overriddenBy: "Synthetic Consultant", overriddenAt: "2026-11-04T12:00:00.000Z" });
    // A measure whose correction the report can carry: one quoted in a finding's wording that can't be is refused, and holds the documents.
    const metric = report.metrics.find((m) => m.status === "measured" && m.unit === "count" && m.value > 10 && reportWith([row("o-2", "metric", "value", m.value, 7, m.id)]).refused.length === 0)!;
    const corrected = asRead + 100_000;
    const fixedReport = reportWith([row("o-1", "finding", "value.amount", asRead, corrected, sized.id), row("o-2", "metric", "value", metric.status === "measured" ? metric.value : 0, 7, metric.id)]);
    assert.deepEqual(fixedReport.refused, []);
    const fixed = buildDigest({ report: fixedReport, pseudonyms, inventory: null, madeAt: MADE_AT });
    const f = fixed.findings.find((x) => x.id === sized.id)!;
    assert.equal(f.value.kind !== "not_sized" && f.value.amount, corrected);
    assert.equal(fixed.metrics.find((m) => m.id === metric.id)!.status === "measured" && (fixed.metrics.find((m) => m.id === metric.id) as { value: number }).value, 7);
    const before = digest.value.lines.find((l) => l.id === sized.id)!;
    const after = fixed.value.lines.find((l) => l.id === sized.id)!;
    assert.ok(after.annual > before.annual, "the value line moves with the correction");
    assert.ok(fixed.value.total.expected > digest.value.total.expected, "and so does the total");
    // Still every number traces, against the corrected report.
    assert.deepEqual(numbers(fixed).filter(({ path, value }) => resolve(fixed, fixedReport, path) !== value).map((u) => u.path.join(".")), []);
  });

  test("a name holding a known person's word is in withheldNames by hash only", () => {
    const person = pseudonyms.entries().find((e) => e.token.startsWith("[[user:") && /^\p{Lu}\p{Ll}+ \p{Lu}\p{Ll}+$/u.test(e.name))!;
    const surname = person.name.split(" ")[1]!;
    const named = `${surname}ExportJob`;
    const inventory: Inventory = {
      version: 1,
      madeAt: MADE_AT,
      scope: "complete",
      fillRates: { rows: [{ object: "Account", records: 10, fields: [{ field: `${surname}_Notes__c`, filled: 3, share: 0.3 }], skipped: [] }], notes: [] },
      packages: { rows: [], notes: [] },
      connectedApps: { rows: [{ name: named, installed: true, adminApprovedOnly: true, firstAuthorised: null, lastUsed: null, users: 1, uses: 1 }, { name: "Data Loader", installed: true, adminApprovedOnly: true, firstAuthorised: null, lastUsed: null, users: 1, uses: 1 }], notes: [] },
      riskyPermissions: { rows: [], notes: [], checked: [], notInOrg: [] },
      limits: { unavailable: "No access" },
    };
    const d = buildDigest({ report, pseudonyms, inventory, madeAt: MADE_AT });
    const hash = (s: string) => createHash("sha256").update(s, "utf8").digest("hex");
    assert.deepEqual(d.withheldNames, [hash(named), hash(`${surname}_Notes__c`)].sort());
    assert.ok(!d.withheldNames.includes(hash("Data Loader")));
    assert.ok(!d.withheldNames.includes(hash("Account")));
    const text = JSON.stringify(d);
    for (const part of [named, surname, person.name]) assert.ok(!text.includes(part), part);
    assert.deepEqual(inventoryNames(inventory).sort(), ["Account", "Data Loader", named, `${surname}_Notes__c`].sort());
    assert.deepEqual(withheldNames(null, pseudonyms), []);
  });

  test("text that holds nothing is kept as written, and an empty recommendation stays empty", () => {
    assert.equal(digestText("Add a weekly check to pipeline reviews.", pseudonyms), "Add a weekly check to pipeline reviews.");
    assert.equal(digestText("", pseudonyms), "");
  });

  test("a scan has no digest", () => {
    const scan = { ...report, input: { ...report.input, run: { ...report.input.run, product: "scan" as const } } };
    assert.throws(() => buildDigest({ report: scan, pseudonyms, inventory: null, madeAt: MADE_AT }), /scan/);
  });

  test("never holds subjects, evidence, guards, segments or facts", () => {
    const keys = new Set(strings(digest.findings.map((f) => Object.keys(f))).concat(strings(digest.metrics.map((m) => Object.keys(m)))));
    for (const key of ["subjects", "evidence", "guards", "segments", "series", "stats", "facts", "segmentsChecked"]) assert.ok(!keys.has(key), key);
    assert.ok(report.teamFindings.some((f) => f.subjects.length > 0), "the synthetic run has subjects to leave out");
  });
  test("carries each issue's hours, time band and fix complexity, and the plan's hours total, every figure traced (DECISIONS.md 636)", () => {
    const issues = digest.findings.filter((f) => f.status === "issue");
    assert.ok(issues.length > 30);
    for (const f of issues) {
      const built = report.teamFindings.find((x) => x.id === f.id)!;
      assert.deepEqual(f.effortHours, built.effortHours, f.id);
      assert.equal(f.timeBand, built.timeBand, f.id);
      assert.equal(f.fixComplexity, built.fixComplexity, f.id);
      assert.ok(f.effortHours && f.timeBand && f.fixComplexity, f.id);
    }
    for (const f of digest.findings.filter((x) => x.status !== "issue")) assert.ok(f.effortHours === undefined && f.timeBand === undefined && f.fixComplexity === undefined, f.id);
    // Each issue's fix prompt is carried only with its org words marked (DECISIONS.md 672; before it, none was carried).
    for (const f of issues) assert.ok(f.fixPrompt === report.teamFindings.find((x) => x.id === f.id)!.fixPrompt && f.orgWords?.fixPrompt, f.id);
    const total = digest.plan.hoursTotal!;
    assert.ok(total.findingIds.length > 10 && total.high >= total.low && total.low > 0);
    // Every issue with hours is in the total, the quick wins among them; every issue without is named beside it.
    for (const f of issues) assert.ok(f.effortHours ? total.findingIds.includes(f.id) : total.withoutHoursFindingIds.includes(f.id), f.id);
    assert.ok(digest.plan.quickWins.length > 0 && digest.plan.quickWins.every((id) => total.findingIds.includes(id)), "the quick wins are counted");
    assert.equal(new Set(total.findingIds).size, total.findingIds.length, "each finding once");
    assert.deepEqual(total.partialFindingIds, total.findingIds.filter((id) => digest.findings.find((f) => f.id === id)!.effortHours!.partial));
    assert.ok(Array.isArray(total.withoutHoursFindingIds));
    assert.equal(total.high, total.findingIds.reduce((s, id) => s + digest.findings.find((f) => f.id === id)!.effortHours!.high, 0));
    // The resolver catches a moved hour, in a finding and in the total.
    const moved = structuredClone(digest);
    const at = moved.findings.findIndex((f) => f.effortHours);
    moved.findings[at]!.effortHours!.high += 1;
    assert.notEqual(resolve(moved, report, ["findings", at, "effortHours", "high"]), moved.findings[at]!.effortHours!.high);
    moved.plan.hoursTotal!.findingIds.pop();
    assert.equal(resolve(moved, report, ["plan", "hoursTotal", "low"]), undefined);
    // Dropping a quick win from the total, as the first builder did, is caught.
    const noQuick = structuredClone(digest);
    noQuick.plan.hoursTotal!.findingIds = noQuick.plan.hoursTotal!.findingIds.filter((id) => !noQuick.plan.quickWins.includes(id));
    assert.equal(resolve(noQuick, report, ["plan", "hoursTotal", "high"]), undefined);
  });

  test("withheldNames uses decision 628's word match: a lower-case run-together name in an API name is withheld (DECISIONS.md 636)", () => {
    const person = pseudonyms.entries().find((e) => e.token.startsWith("[[user:") && /^\p{Lu}\p{Ll}+ \p{Lu}\p{Ll}+$/u.test(e.name))!;
    const [first, surname] = person.name.toLowerCase().split(" ") as [string, string];
    const named = `${first[0]}${surname}_nightly_sync`;
    const inventory: Inventory = {
      version: 1,
      madeAt: MADE_AT,
      scope: "complete",
      fillRates: { rows: [], notes: [] },
      packages: { rows: [], notes: [] },
      connectedApps: { rows: [{ name: named, installed: true, adminApprovedOnly: true, firstAuthorised: null, lastUsed: null, users: 1, uses: 1 }, { name: "nightly_sync", installed: true, adminApprovedOnly: true, firstAuthorised: null, lastUsed: null, users: 1, uses: 1 }], notes: [] },
      riskyPermissions: { rows: [], notes: [], checked: [], notInOrg: [] },
      limits: { unavailable: "No access" },
    };
    const hash = (x: string) => createHash("sha256").update(x, "utf8").digest("hex");
    assert.deepEqual(withheldNames(inventory, pseudonyms), [hash(named)]);
    assert.ok(!JSON.stringify(buildDigest({ report, pseudonyms, inventory, madeAt: MADE_AT })).includes(named));
  });
  test("withheldNames matches a person only as whole words: ordinary field names stay, a full or run-together name goes (DECISIONS.md 636)", () => {
    const people = new Pseudonymiser();
    for (const [n, name] of [[1, "Ann Mount"], [2, "Paul Rice"], [3, "Tim Evans"], [4, "Dan Day"], [5, "Sam Reed"]] as const) people.label("user", `005${String(n).padStart(12, "0")}AAA`, name, [`user${n}@example.com`]);
    const names = ["Amount__c", "Unit_Price__c", "Response_Time__c", "Days_Open__c", "Sam_Reed_Queue", "samreed_export", "sreed", "Reed Sam Report", "Opportunity"];
    const inventory: Inventory = {
      version: 1,
      madeAt: MADE_AT,
      scope: "complete",
      fillRates: { rows: [], notes: [] },
      packages: { rows: [], notes: [] },
      connectedApps: { rows: names.map((name) => ({ name, installed: true, adminApprovedOnly: true, firstAuthorised: null, lastUsed: null, users: 1, uses: 1 })), notes: [] },
      riskyPermissions: { rows: [], notes: [], checked: [], notInOrg: [] },
      limits: { unavailable: "No access" },
    };
    const hash = (x: string) => createHash("sha256").update(x, "utf8").digest("hex");
    const withheld = new Set(withheldNames(inventory, people));
    for (const ok of ["Amount__c", "Unit_Price__c", "Response_Time__c", "Days_Open__c", "Opportunity"]) assert.ok(!withheld.has(hash(ok)), `${ok} is withheld`);
    for (const named of ["Sam_Reed_Queue", "samreed_export", "sreed", "Reed Sam Report"]) assert.ok(withheld.has(hash(named)), `${named} is kept`);
    assert.equal(inventoryNameNamesPerson("Sreed_Flow", people), false, "a capitalised initial and surname is a word, not a name");
  });
});
