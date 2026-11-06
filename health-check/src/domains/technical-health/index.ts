import { apexExtractor, metadataExtractor, type ApexClassRow, type ApexTriggerRow } from "../../extract/automation.ts";
import { customObjectsExtractor, orgExtractor, recordCountsExtractor } from "../../extract/configuration.ts";
import { apiLimitsExtractor } from "../../extract/integrations.ts";
import { classicExtractor } from "../../extract/technical.ts";
import { notSized, pct, plural, type Rule, type RuleResult } from "../../engine/rule.ts";
import { apexGap, metadataOr, UNNAMED, unnamedNote } from "../automation/shared.ts";
import type { DomainModule } from "../types.ts";
import { labelsOf } from "../labels.ts";
import { labelsExtractor } from "../../extract/labels.ts";
import { isObjectId, objectResolver } from "../object-names.ts";

// Technical health (roadmap 3.2): how sound the code is, read from set-up only.
// Coverage, API versions and trigger counts moved here from automation with
// their ids, so a re-check still lines them up (DECISIONS.md 169). The new
// checks read the code as text while the extractor has it (src/extract/apex-scan.ts),
// so they say so and sit at medium confidence.

const DOMAIN = "technical_health";
const COVERAGE_MINIMUM = 75;
const CLASS_COVERAGE_FLOOR = 50;
const MIN_CLASS_LINES = 20;
const API_VERSIONS_BEHIND = 15;
/** Salesforce's limit on an org's own Apex, in characters without comments. Test classes don't count (Apex Developer Guide). */
export const APEX_CODE_LIMIT = 6_000_000;
// Our judgement: room for a year's growth below the limit.
const CODE_SIZE_WARNING = 75;
const CODE_SIZE_HIGH = 90;
// Our judgement: past this share, assertion-free tests are a habit rather than a slip.
const SILENT_TEST_SHARE = 20;
// Salesforce's per-transaction limits for synchronous Apex (Apex Developer Guide, Execution Governors and Limits).
const SOQL_LIMIT = 100;
const DML_LIMIT = 150;

type Code = (ApexClassRow | ApexTriggerRow) & { kind: "apex class" | "apex trigger" };

/** Active code of the org's own that isn't a test, with what the scan found. Hidden bodies are counted apart. */
function liveCode(classes: ApexClassRow[], triggers: ApexTriggerRow[]): { code: Code[]; hidden: number } {
  const all: Code[] = [...classes.map((c) => ({ ...c, kind: "apex class" as const })), ...triggers.map((t) => ({ ...t, kind: "apex trigger" as const }))].filter((c) => c.Status === "Active");
  return { code: all.filter((c) => c.scan !== null && !c.isTest), hidden: all.filter((c) => c.scan === null).length };
}

const hiddenNote = (hidden: number): string[] => (hidden ? [`Classes and triggers whose code is hidden: ${hidden}`] : []);

const oldApiVersions: Rule = {
  id: "aut.old-api-versions",
  version: 1,
  domain: DOMAIN,
  title: "Code and flows on old API versions",
  evaluate(ctx): RuleResult | RuleResult[] {
    const result = metadataOr(ctx);
    if ("outcome" in result) return result;
    const { classes, triggers } = ctx.data(apexExtractor);
    if (classes.status !== "ok") return { outcome: "not_measurable", reason: apexGap("classes", classes.reason), evidence: [ctx.ev(classes.source, { count: null })] };
    if (triggers.status !== "ok") return { outcome: "not_measurable", reason: apexGap("triggers", triggers.reason), evidence: [ctx.ev(triggers.source, { count: null })] };
    const newest = Math.max(0, ...classes.data.map((c) => c.ApiVersion), ...triggers.data.map((t) => t.ApiVersion), ...result.bundle.flows.map((f) => f.apiVersion));
    const floor = newest - API_VERSIONS_BEHIND;
    const old = {
      "Apex classes": classes.data.filter((c) => c.ApiVersion < floor).length,
      "Apex triggers": triggers.data.filter((t) => t.ApiVersion < floor).length,
      "Active flows": result.bundle.flows.filter((f) => f.status === "Active" && f.apiVersion < floor).length,
    };
    const total = Object.values(old).reduce((a, b) => a + b, 0);
    const evidence = [
      ctx.ev(classes.source, { count: old["Apex classes"], note: `Older than version ${floor}` }),
      ctx.ev(triggers.source, { count: old["Apex triggers"], note: `Older than version ${floor}` }),
      ctx.ev({ kind: "metadata", path: "flows/*.flow" }, { count: old["Active flows"], note: `Older than version ${floor}` }),
    ];
    const segments = [{ dimension: "component type", segments: 3 }];
    if (total === 0) return { outcome: "pass", summary: `Nothing is more than ${API_VERSIONS_BEHIND} versions behind the newest in this org.`, evidence, segmentsChecked: segments };
    return {
      outcome: "issue",
      title: "Some automation is on very old API versions",
      summary: `Components on an API version older than ${floor}, more than ${API_VERSIONS_BEHIND} versions behind the newest used here: ${Object.entries(old).filter(([, n]) => n > 0).map(([k, n]) => `${k}: ${n}`).join(", ")}. Salesforce retires old versions over time.`,
      recommendation: "Bring these components forward as part of your next release cycle.",
      recommendationType: "change",
      severity: "medium",
      confidence: "high",
      evidence,
      value: notSized("The cost depends on what breaks on upgrade, which needs testing."),
      effort: { size: "M" },
      owner: "Salesforce developer",
      segmentsChecked: segments,
      facts: { total, floor, ...old },
    };
  },
};

const apexCoverage: Rule = {
  id: "aut.apex-coverage",
  version: 2,
  domain: DOMAIN,
  title: "Apex test coverage",
  evaluate(ctx): RuleResult | RuleResult[] {
    const { classes, orgCoverage, classCoverage } = ctx.data(apexExtractor);
    if (classes.status !== "ok") return { outcome: "not_measurable", reason: apexGap("classes", classes.reason), evidence: [ctx.ev(classes.source, { count: null })] };
    if (classes.data.length === 0) {
      return { outcome: "pass", summary: "This org has no Apex of its own.", evidence: [ctx.ev(classes.source, { count: 0 })], segmentExemptReason: "There's no code to segment." };
    }
    // A coverage read that failed isn't an org with no figure. A read-only
    // connection reads the classes but not their coverage (DECISIONS.md 73).
    if (orgCoverage.status !== "ok") return { outcome: "not_measurable", reason: apexGap("coverage", orgCoverage.reason), evidence: [ctx.ev(orgCoverage.source, { count: null })] };
    const percent = orgCoverage.data[0]?.PercentCovered;
    if (percent === null || percent === undefined) {
      return { outcome: "not_measurable", reason: "The org holds no coverage figure, which usually means tests haven't been run recently. We don't run tests, because that would change the org.", evidence: [ctx.ev(orgCoverage.source, { count: null })] };
    }
    // Production refuses Apex below 75% at deploy, so an org with Apex can't really
    // be at 0%: the stored figures were cleared or only part-run. A real org stored
    // coverage for 1 class of 108 and 0% overall.
    if (percent === 0) {
      return { outcome: "not_measurable", reason: "The org stores 0% coverage overall, which means the stored figures were cleared or only some tests have run since, rather than untested code. We don't run tests, because that would change the org.", evidence: [ctx.ev(orgCoverage.source, { count: 1 })] };
    }
    if (classCoverage.status !== "ok") {
      return { outcome: "not_measurable", reason: `The org-wide figure is ${percent}%, but coverage by class couldn't be read, so we can't say the whole is healthy: ${classCoverage.reason}`, evidence: [ctx.ev(classCoverage.source, { count: null })] };
    }
    const measured = classCoverage.data.filter((c) => c.covered + c.uncovered >= MIN_CLASS_LINES);
    const weak = measured.filter((c) => (c.covered / (c.covered + c.uncovered)) * 100 < CLASS_COVERAGE_FLOOR);
    const evidence = [ctx.ev(orgCoverage.source, { count: 1 }), ctx.ev(classCoverage.source, { count: measured.length, exclusions: [`Classes under ${MIN_CLASS_LINES} lines`] })];
    const segments = [{ dimension: "class", segments: measured.length }];
    if (percent >= COVERAGE_MINIMUM && weak.length === 0) {
      return { outcome: "pass", summary: `Org-wide coverage is ${percent}%, and no class is below ${CLASS_COVERAGE_FLOOR}%.`, evidence, segmentsChecked: segments, facts: { percent } };
    }
    return {
      outcome: "issue",
      title: percent < COVERAGE_MINIMUM ? "Apex coverage is below what a deployment needs" : "A healthy coverage total is hiding weak classes",
      summary:
        percent < COVERAGE_MINIMUM
          ? `Org-wide coverage is ${percent}%, below the ${COVERAGE_MINIMUM}% Salesforce requires to deploy to production.`
          : `Org-wide coverage is ${percent}%, but of ${plural(measured.length, "class", "classes")} we can measure, ${weak.length} ${weak.length === 1 ? "is" : "are"} below ${CLASS_COVERAGE_FLOOR}%.`,
      recommendation: "Add meaningful tests to the weakest classes first.",
      recommendationType: "change",
      severity: percent < COVERAGE_MINIMUM ? "high" : "low",
      confidence: "high",
      evidence,
      value: notSized("The cost is deployment risk rather than a direct figure."),
      effort: { size: "M" },
      owner: "Salesforce developer",
      segmentsChecked: segments,
      subjects: weak.map((c) => ({ type: "apex class", label: c.name })),
      facts: { percent, weakClasses: weak.length, measuredClasses: measured.length },
    };
  },
};

const multipleTriggers: Rule = {
  id: "aut.multiple-triggers",
  version: 3,
  domain: DOMAIN,
  title: "More than one trigger on an object",
  evaluate(ctx): RuleResult | RuleResult[] {
    const { triggers } = ctx.data(apexExtractor);
    if (triggers.status !== "ok") return { outcome: "not_measurable", reason: apexGap("triggers", triggers.reason), evidence: [ctx.ev(triggers.source, { count: null })] };
    const ids = ctx.data(customObjectsExtractor);
    const objectOf = objectResolver(ids);
    // Grouped by name where we have one. An id we can't name still says which
    // triggers share an object, in either length, so it's grouped by id and never shown.
    const keyOf = (value: string): { key: string; label: string | null } => {
      const name = objectOf(value);
      if (name) return { key: name, label: labelsOf(ctx).object(name) };
      return { key: isObjectId(value) ? value.slice(0, 15) : value, label: null };
    };
    const byObject = new Map<string, { label: string | null; n: number }>();
    for (const t of triggers.data) {
      if (t.Status !== "Active") continue;
      const { key, label } = keyOf(t.TableEnumOrId);
      byObject.set(key, { label, n: (byObject.get(key)?.n ?? 0) + 1 });
    }
    const unnamed = [...byObject.values()].filter((o) => o.label === null).reduce((sum, o) => sum + o.n, 0);
    const multiple = [...byObject.values()].filter((o) => o.n > 1).sort((a, b) => (a.label === null ? 1 : 0) - (b.label === null ? 1 : 0) || (a.label ?? "").localeCompare(b.label ?? ""));
    const note = unnamedNote(unnamed, ids.status === "ok", unnamed === 1 ? "it's grouped by the object's id instead" : "they're grouped by the object's id instead");
    const evidence = [
      ctx.ev(triggers.source, { count: triggers.data.length, exclusions: ["Inactive triggers", "Packaged triggers"] }),
      ctx.ev(ids.source, { count: ids.status === "ok" ? ids.data.length : null, note: "Names the custom objects triggers refer to by id" }),
    ];
    const segments = [{ dimension: "object", segments: byObject.size }];
    if (multiple.length === 0) {
      return byObject.size === 0
        ? { outcome: "pass", summary: "This org has no active triggers of its own.", evidence, segmentExemptReason: "There are no triggers to segment." }
        : { outcome: "pass", summary: `No object has more than one active trigger, across ${byObject.size} objects.${note}`, evidence, segmentsChecked: segments, facts: { unnamedTriggers: unnamed } };
    }
    return {
      outcome: "issue",
      title: "Some objects have several triggers",
      summary: `${multiple.map((o) => `${o.label ?? UNNAMED} has ${o.n}`).join("; ")}. Salesforce doesn't guarantee the order they run in.${note}`,
      recommendation: "Move to one trigger per object, calling a handler class.",
      recommendationType: "change",
      severity: "medium",
      confidence: "high",
      evidence,
      value: notSized("The cost is unpredictable behaviour rather than a direct figure."),
      effort: { size: "M" },
      owner: "Salesforce developer",
      segmentsChecked: segments,
      subjects: multiple.map((o) => ({ type: "object", label: o.label ?? "Custom object we couldn't name" })),
      facts: { objects: multiple.length, unnamedTriggers: unnamed },
    };
  },
};

const testQuality: Rule = {
  id: "tech.test-quality",
  version: 1,
  domain: DOMAIN,
  title: "Apex tests that check nothing",
  evaluate(ctx): RuleResult | RuleResult[] {
    const { classes } = ctx.data(apexExtractor);
    if (classes.status !== "ok") return { outcome: "not_measurable", reason: apexGap("classes", classes.reason), evidence: [ctx.ev(classes.source, { count: null })] };
    const active = classes.data.filter((c) => c.Status === "Active");
    // A test data factory is @isTest with no test methods, and has nothing to assert.
    const tests = active.filter((c) => c.isTest && c.scan !== null && c.scan.testMethods > 0);
    const hidden = active.filter((c) => c.scan === null).length;
    const evidence = [ctx.ev(classes.source, { count: tests.length, exclusions: ["Test classes with no test methods, such as data factories", "Installed packages' classes", ...hiddenNote(hidden)], note: "Read from the code: test methods, assertions and SeeAllData" })];
    if (active.length === 0) return { outcome: "pass", summary: "This org has no Apex of its own.", evidence, segmentExemptReason: "There's no code to segment." };
    if (tests.length === 0) return { outcome: "not_measurable", reason: "We found no test methods we could read, so there's nothing to judge. The coverage check covers whether the code is tested.", evidence };
    const silent = tests.filter((c) => c.scan!.asserts === 0);
    const seeAll = tests.filter((c) => c.scan!.seeAllData);
    const segments = [{ dimension: "test class", segments: tests.length }];
    const facts = { testClasses: tests.length, withoutAssertions: silent.length, seeAllData: seeAll.length };
    if (silent.length === 0 && seeAll.length === 0) {
      return { outcome: "pass", summary: `All ${plural(tests.length, "test class", "test classes")} check their results, and none reads the org's own data.`, evidence, segmentsChecked: segments, facts };
    }
    const parts = [
      silent.length ? `${silent.length} of ${plural(tests.length, "test class", "test classes")} ${silent.length === 1 ? "has" : "have"} no assertion, so ${silent.length === 1 ? "it raises" : "they raise"} coverage without proving the code does what it should.` : "",
      seeAll.length ? `${plural(seeAll.length, "test class", "test classes")} ${seeAll.length === 1 ? "reads" : "read"} the org's live data (SeeAllData=true), so ${seeAll.length === 1 ? "it" : "they"} can pass or fail depending on what's in the org.` : "",
    ];
    return {
      outcome: "issue",
      title: "Some Apex tests run code without checking what it does",
      summary: parts.filter(Boolean).join(" "),
      recommendation: "Add assertions that check each test's outcome, and have tests create the data they need rather than read the org's.",
      recommendationType: "change",
      severity: pct(silent.length, tests.length) >= SILENT_TEST_SHARE ? "medium" : "low",
      // Read from the text: a helper that asserts under another name would be missed.
      confidence: "medium",
      evidence,
      value: notSized("The cost is code that passes its tests and still breaks, which we can't size from the code."),
      effort: { size: "M" },
      owner: "Salesforce developer",
      segmentsChecked: segments,
      subjects: [...new Map([...silent, ...seeAll].map((c) => [c.Name, { type: "apex class", label: c.Name }])).values()],
      facts,
    };
  },
};

const limitRisk: Rule = {
  id: "tech.governor-limit-risk",
  version: 1,
  domain: DOMAIN,
  title: "Queries and saves inside loops",
  evaluate(ctx): RuleResult | RuleResult[] {
    const { classes, triggers } = ctx.data(apexExtractor);
    if (classes.status !== "ok") return { outcome: "not_measurable", reason: apexGap("classes", classes.reason), evidence: [ctx.ev(classes.source, { count: null })] };
    if (triggers.status !== "ok") return { outcome: "not_measurable", reason: apexGap("triggers", triggers.reason), evidence: [ctx.ev(triggers.source, { count: null })] };
    const { code, hidden } = liveCode(classes.data, triggers.data);
    const risky = code.filter((c) => c.scan!.queriesInLoops + c.scan!.dmlInLoops > 0).sort((a, b) => a.Name.localeCompare(b.Name));
    const queries = risky.reduce((sum, c) => sum + c.scan!.queriesInLoops, 0);
    const saves = risky.reduce((sum, c) => sum + c.scan!.dmlInLoops, 0);
    const inTriggers = risky.filter((c) => c.kind === "apex trigger").length;
    const evidence = [
      ctx.ev(classes.source, { count: code.filter((c) => c.kind === "apex class").length, exclusions: ["Test classes", "Inactive code", "Installed packages' code", ...hiddenNote(hidden)], note: "Read from the code: a query or save inside a loop's body" }),
      ctx.ev(triggers.source, { count: code.filter((c) => c.kind === "apex trigger").length, exclusions: ["Inactive triggers"] }),
    ];
    const segments = [{ dimension: "component type", segments: 2 }, { dimension: "class or trigger", segments: code.length }];
    const facts = { risky: risky.length, code: code.length, queries, saves, triggers: inTriggers, soqlLimit: SOQL_LIMIT, dmlLimit: DML_LIMIT };
    if (code.length === 0) return { outcome: "pass", summary: "There's no active Apex of the org's own to read.", evidence, segmentExemptReason: "There's no code to segment." };
    if (risky.length === 0) return { outcome: "pass", summary: `None of the ${plural(code.length, "active class or trigger", "active classes and triggers")} we read runs a query or a save inside a loop.`, evidence, segmentsChecked: segments, facts };
    return {
      outcome: "issue",
      title: "Some code queries or saves records inside loops",
      summary: `${risky.length} of ${plural(code.length, "active class or trigger", "active classes and triggers")} ${risky.length === 1 ? "runs" : "run"} a query or a save inside a loop: ${plural(queries, "query", "queries")} and ${plural(saves, "save")} in all${inTriggers ? `, ${inTriggers === 1 ? "one of them a trigger" : `${inTriggers} of them triggers`}` : ""}. Salesforce allows ${SOQL_LIMIT} queries and ${DML_LIMIT} saves in one transaction, so code like this fails once enough records arrive together, as they do in a data load or an integration's batch.`,
      recommendation: "Move each query and save out of the loop: gather what the loop needs first, query once, and save the records together.",
      recommendationType: "change",
      // A trigger runs for up to 200 records at a time, so its loops are where the limit is met.
      severity: inTriggers ? "high" : "medium",
      confidence: "medium",
      evidence,
      value: notSized("The cost is saves that fail when volumes rise, which we can't size before it happens."),
      effort: { size: risky.length > 10 ? "L" : "M" },
      owner: "Salesforce developer",
      dependencies: ["Test each change with a bulk load in a sandbox"],
      segmentsChecked: segments,
      subjects: risky.map((c) => ({ type: c.kind, label: c.Name })),
      facts,
    };
  },
};

const codeSize: Rule = {
  id: "tech.apex-code-size",
  version: 1,
  domain: DOMAIN,
  title: "Apex against the org's size limit",
  evaluate(ctx): RuleResult | RuleResult[] {
    const { classes, triggers } = ctx.data(apexExtractor);
    if (classes.status !== "ok") return { outcome: "not_measurable", reason: apexGap("classes", classes.reason), evidence: [ctx.ev(classes.source, { count: null })] };
    if (triggers.status !== "ok") return { outcome: "not_measurable", reason: apexGap("triggers", triggers.reason), evidence: [ctx.ev(triggers.source, { count: null })] };
    // Only a class marked @isTest is left out. One we can't read is counted, so the figure errs high.
    const counted = [...classes.data.filter((c) => !c.scan?.testClass), ...triggers.data].filter((c) => c.Status === "Active");
    if (counted.some((c) => typeof c.LengthWithoutComments !== "number")) return { outcome: "not_measurable", reason: "The size of some code wasn't read.", evidence: [ctx.ev(triggers.source, { count: null })] };
    const used = counted.reduce((sum, c) => sum + c.LengthWithoutComments!, 0);
    const share = pct(used, APEX_CODE_LIMIT);
    const evidence = [
      ctx.ev(classes.source, { count: counted.length, exclusions: ["Classes marked @isTest, which don't count against the limit", "Inactive code", "Installed packages' code", "Comments"], note: `LengthWithoutComments, against Salesforce's limit of ${APEX_CODE_LIMIT.toLocaleString("en-GB")} characters` }),
    ];
    const facts = { used, limit: APEX_CODE_LIMIT, share };
    const text = `Your own Apex is ${used.toLocaleString("en-GB")} characters without comments, ${share}% of the ${APEX_CODE_LIMIT.toLocaleString("en-GB")} Salesforce allows an org.`;
    // The limit is one figure for the org, so the only cut is classes against triggers.
    const segments = [{ dimension: "component type", segments: 2, note: "The limit applies to the org as a whole" }];
    if (share < CODE_SIZE_WARNING) return { outcome: "pass", summary: text, evidence, segmentsChecked: segments, facts };
    return {
      outcome: "issue",
      title: "Your Apex is close to Salesforce's size limit",
      summary: `${text} At the limit, no new code can be saved until some is removed.`,
      recommendation: "Remove classes nobody calls, mark test classes and test helpers @isTest so they don't count, and plan larger features with the limit in mind.",
      recommendationType: "change",
      severity: share >= CODE_SIZE_HIGH ? "high" : "medium",
      confidence: "high",
      evidence,
      value: notSized("The cost is work that can't ship once the limit is reached, which we can't size."),
      effort: { size: "M" },
      owner: "Salesforce developer",
      segmentsChecked: segments,
      facts,
    };
  },
};

const classicLeftovers: Rule = {
  id: "tech.classic-leftovers",
  version: 2,
  domain: DOMAIN,
  title: "Set-up that only works in Salesforce Classic",
  evaluate(ctx): RuleResult | RuleResult[] {
    const { links, scontrols, pages } = ctx.data(classicExtractor);
    if (links.status !== "ok") return { outcome: "not_measurable", reason: `Custom buttons and links couldn't be read: ${links.reason}`, evidence: [ctx.ev(links.source, { count: null })] };
    if (pages.status !== "ok") return { outcome: "not_measurable", reason: `Visualforce pages couldn't be read: ${pages.reason}`, evidence: [ctx.ev(pages.source, { count: null })] };
    const javascript = links.data.filter((l) => l.LinkType?.toLowerCase() === "javascript").sort((a, b) => a.Name.localeCompare(b.Name));
    const controls = scontrols.status === "ok" ? scontrols.data : [];
    // Marked not available in Lightning Experience, Experience sites and the mobile app (ApexPage.IsAvailableInTouch).
    const classicPages = pages.data.filter((p) => p.IsAvailableInTouch === false).sort((a, b) => a.Name.localeCompare(b.Name));
    const evidence = [
      ctx.ev(links.source, { count: javascript.length, exclusions: ["Installed packages' buttons and links", "Buttons and links that open a URL, page or flow"] }),
      ctx.ev(scontrols.source, { count: scontrols.status === "ok" ? controls.length : null }),
      ctx.ev(pages.source, { count: classicPages.length, exclusions: ["Installed packages' pages", "Pages marked available in Lightning Experience"] }),
    ];
    const segments = [{ dimension: "component type", segments: 3 }];
    // Unread s-controls aren't a count of none (DECISIONS.md 291).
    const facts = { javascriptButtons: javascript.length, ...(scontrols.status === "ok" ? { scontrols: controls.length } : {}), classicPages: classicPages.length, pages: pages.data.length };
    if (javascript.length + controls.length + classicPages.length === 0) {
      if (scontrols.status !== "ok") return { outcome: "not_measurable", reason: `There are no JavaScript buttons or Classic-only pages, but s-controls couldn't be read: ${scontrols.reason}`, evidence };
      return { outcome: "pass", summary: `No JavaScript buttons or s-controls, and all ${plural(pages.data.length, "Visualforce page")} ${pages.data.length === 1 ? "is" : "are"} marked available in Lightning Experience.`, evidence, segmentsChecked: segments, facts };
    }
    const parts = [
      javascript.length ? plural(javascript.length, "JavaScript button or link", "JavaScript buttons and links") : "",
      controls.length ? plural(controls.length, "s-control") : "",
      classicPages.length ? `${plural(classicPages.length, "Visualforce page")} not marked available in Lightning Experience` : "",
    ].filter(Boolean);
    const breaks = javascript.length + controls.length > 0;
    return {
      outcome: "issue",
      title: "Parts of the org still depend on Salesforce Classic",
      summary: `${parts.length === 1 ? parts[0] : `${parts.slice(0, -1).join(", ")} and ${parts.at(-1)}`}.${breaks ? " Lightning Experience doesn't run JavaScript buttons or s-controls, so anyone who works in it can't use them." : ""}${scontrols.status !== "ok" ? ` S-controls couldn't be read: ${scontrols.reason}` : ""}`,
      recommendation: "Replace JavaScript buttons and s-controls with quick actions, flows or Lightning components, and check whether each Classic-only Visualforce page is still used before moving or retiring it.",
      recommendationType: "change",
      severity: breaks ? "medium" : "low",
      confidence: "high",
      evidence,
      value: notSized("The cost is work people can't do in Lightning Experience, which the set-up doesn't show."),
      effort: { size: javascript.length + controls.length > 10 ? "L" : "M" },
      owner: "Salesforce admin",
      dependencies: ["Check who still uses each one before replacing it"],
      segmentsChecked: segments,
      subjects: [
        ...javascript.map((l) => ({ type: "javascript button", label: l.PageOrSobjectType ? `${l.PageOrSobjectType}: ${l.Name}` : l.Name })),
        ...controls.map((c) => ({ type: "s-control", label: c.Name })),
        ...classicPages.map((p) => ({ type: "visualforce page", label: p.Name })),
      ],
      facts,
    };
  },
};


// Storage against the allowance (roadmap 3.2). Salesforce reports both in
// /limits, read as the connection was made. How fast it grows isn't read: the
// org keeps no history of it, and a guess from record dates would ignore what's
// deleted (DECISIONS.md 204).
const STORAGE_WARNING = 80;
const STORAGE_HIGH = 95;
const STORAGE: Record<string, string> = { DataStorageMB: "data storage", FileStorageMB: "file storage" };
const LARGEST_OBJECTS = 5;

const storageUse: Rule = {
  id: "tech.storage-use",
  version: 2,
  domain: DOMAIN,
  title: "Data and file storage against the allowance",
  evaluate(ctx): RuleResult | RuleResult[] {
    const limits = ctx.data(apiLimitsExtractor);
    if (limits.status !== "ok") return { outcome: "not_measurable", reason: `The org's limits couldn't be read: ${limits.reason}`, evidence: [ctx.ev(limits.source, { count: null })] };
    const read = Object.entries(STORAGE).flatMap(([key, label]) => {
      const limit = limits.data[key];
      if (!limit || limit.Max <= 0) return [];
      const used = Math.max(0, limit.Max - limit.Remaining);
      return [{ key, label, used, max: limit.Max, share: pct(used, limit.Max) }];
    });
    const counts = ctx.data(recordCountsExtractor);
    const evidence = [ctx.ev(limits.source, { count: read.length, note: "In MB, as Salesforce reported them when the connection was made" })];
    if (read.length === 0) return { outcome: "not_measurable", reason: "The org reported no storage allowance.", evidence };
    const largest = counts.status === "ok" ? [...counts.data].sort((a, b) => b.count - a.count || a.name.localeCompare(b.name)).slice(0, LARGEST_OBJECTS).filter((o) => o.count > 0) : [];
    if (counts.status === "ok") evidence.push(ctx.ev(counts.source, { count: counts.data.length, note: "Record counts by object, for the largest" }));
    const segments = [{ dimension: "storage kind", segments: read.length }];
    const facts = Object.fromEntries(read.flatMap((r) => [[`${r.key}Used`, r.used], [`${r.key}Max`, r.max], [`${r.key}Share`, r.share]]));
    const text = (r: (typeof read)[number]): string => `${r.label} is ${r.share}% used (${r.used.toLocaleString("en-GB")} of ${r.max.toLocaleString("en-GB")} MB)`;
    const list = (items: string[]): string => (items.length <= 1 ? (items[0] ?? "") : `${items.slice(0, -1).join(", ")} and ${items.at(-1)}`);
    const opening = list(read.map(text));
    const sentence = `${opening.charAt(0).toUpperCase()}${opening.slice(1)}.`;
    const over = read.filter((r) => r.share >= STORAGE_WARNING);
    if (over.length === 0) return { outcome: "pass", summary: sentence, evidence, segmentsChecked: segments, facts };
    return {
      outcome: "issue",
      title: "Storage is close to the allowance",
      summary: `${sentence} Past the allowance, Salesforce can stop new records or files being saved until space is freed or more is bought.${largest.length ? ` The objects with the most records: ${largest.map((o) => `${labelsOf(ctx).object(o.name)} (${o.count.toLocaleString("en-GB")})`).join(", ")}.` : ""}`,
      recommendation: "Archive or delete what you no longer need, starting with the largest objects and old files, and plan any extra storage before you reach the limit.",
      recommendationType: "change",
      severity: over.some((r) => r.share >= STORAGE_HIGH) ? "high" : "medium",
      confidence: "high",
      evidence,
      value: notSized("We hold no price for extra storage, so we don't put a figure on it."),
      effort: { size: "M" },
      owner: "Salesforce admin",
      dependencies: ["Agree with the business how long each kind of record must be kept before archiving any"],
      segmentsChecked: segments,
      subjects: over.map((r) => ({ type: "storage", label: r.label })),
      facts,
    };
  },
};

export const technicalHealthModule: DomainModule = {
  id: DOMAIN,
  cloud: "sales",
  label: "Technical health",
  extractors: [labelsExtractor, orgExtractor, metadataExtractor, apexExtractor, customObjectsExtractor, classicExtractor, apiLimitsExtractor, recordCountsExtractor],
  rules: [apexCoverage, testQuality, limitRisk, codeSize, storageUse, oldApiVersions, multipleTriggers, classicLeftovers],
};
