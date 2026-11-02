import { apexExtractor, metadataExtractor, scheduledJobsExtractor } from "../../extract/automation.ts";
import type { MetadataBundle } from "../../extract/metadata-parse.ts";
import { notSized, plural, type NotMeasurableResult, type Rule, type RuleContext, type RuleResult } from "../../engine/rule.ts";
import { MIGRATE_TO_FLOW_NOTE, END_OF_SUPPORT } from "./notes.ts";
import type { DomainModule } from "../types.ts";

const COVERAGE_MINIMUM = 75;
const CLASS_COVERAGE_FLOOR = 50;
const MIN_CLASS_LINES = 20;
const API_VERSIONS_BEHIND = 15;
const MIXED_AUTOMATION_LIMIT = 3;

function metadataOr(ctx: RuleContext): { bundle: MetadataBundle } | NotMeasurableResult {
  const metadata = ctx.data(metadataExtractor);
  if (metadata.status === "ok") return { bundle: metadata.data };
  return { outcome: "not_measurable", reason: `Metadata couldn't be retrieved: ${metadata.reason}`, evidence: [ctx.ev(metadata.source, { count: null })] };
}

const groupCount = (values: string[]): number => new Set(values).size;

const legacyWorkflowRules: Rule = {
  id: "aut.legacy-workflow-rules",
  version: 1,
  domain: "automation",
  title: "Workflow Rules still active",
  evaluate(ctx): RuleResult | RuleResult[] {
    const result = metadataOr(ctx);
    if ("outcome" in result) return result;
    const active = result.bundle.workflows.flatMap((w) => w.rules.filter((r) => r.active).map((r) => ({ object: w.object, ...r })));
    const evidence = [ctx.ev({ kind: "metadata", path: "workflows/*.workflow" }, { count: active.length, exclusions: ["Inactive rules"] })];
    const segments = [{ dimension: "object", segments: result.bundle.workflows.length }];
    if (active.length === 0) return { outcome: "pass", summary: "No Workflow Rules are active.", evidence, segmentsChecked: segments };
    const objects = groupCount(active.map((r) => r.object));
    return {
      outcome: "issue",
      title: "Automation still runs on Workflow Rules",
      summary: `${plural(active.length, "active Workflow Rule")} across ${plural(objects, "object")}. ${END_OF_SUPPORT}`,
      recommendation: `Plan the move to Flow, starting with the objects that have the most rules. ${MIGRATE_TO_FLOW_NOTE}`,
      recommendationType: "change",
      severity: "high",
      confidence: "high",
      evidence,
      value: notSized("The migration effort depends on how complex each rule is, which needs a closer read."),
      effort: { size: active.length > 20 ? "L" : "M" },
      owner: "Salesforce admin",
      dependencies: ["Test each migrated rule in a sandbox before switching it off"],
      segmentsChecked: segments,
      subjects: active.map((r) => ({ type: "workflow rule", label: `${r.object}.${r.name}` })),
      facts: { activeRules: active.length, objects },
    };
  },
};

const legacyProcessBuilder: Rule = {
  id: "aut.legacy-process-builder",
  version: 1,
  domain: "automation",
  title: "Process Builders still active",
  evaluate(ctx): RuleResult | RuleResult[] {
    const result = metadataOr(ctx);
    if ("outcome" in result) return result;
    const active = result.bundle.flows.filter((f) => f.processType === "Workflow" && f.status === "Active");
    const evidence = [ctx.ev({ kind: "metadata", path: "flows/*.flow (processType Workflow)" }, { count: active.length, exclusions: ["Inactive versions"] })];
    const segments = [{ dimension: "process type", segments: groupCount(result.bundle.flows.map((f) => f.processType)) }];
    if (active.length === 0) return { outcome: "pass", summary: "No Process Builders are active.", evidence, segmentsChecked: segments };
    return {
      outcome: "issue",
      title: "Automation still runs on Process Builder",
      summary: `${plural(active.length, "active Process Builder")}. ${END_OF_SUPPORT}`,
      recommendation: `Plan the move to Flow alongside any Workflow Rules on the same objects. ${MIGRATE_TO_FLOW_NOTE}`,
      recommendationType: "change",
      severity: "high",
      confidence: "high",
      evidence,
      value: notSized("The migration effort depends on how complex each process is, which needs a closer read."),
      effort: { size: active.length > 10 ? "L" : "M" },
      owner: "Salesforce admin",
      dependencies: ["Test each migrated process in a sandbox before switching it off"],
      segmentsChecked: segments,
      subjects: active.map((f) => ({ type: "process", label: f.name })),
      facts: { activeProcesses: active.length },
    };
  },
};

const mixedAutomation: Rule = {
  id: "aut.mixed-automation-on-object",
  version: 1,
  domain: "automation",
  title: "Several automation tools acting on the same object",
  evaluate(ctx): RuleResult | RuleResult[] {
    const result = metadataOr(ctx);
    if ("outcome" in result) return result;
    const { triggers } = ctx.data(apexExtractor);
    if (triggers.status !== "ok") return { outcome: "not_measurable", reason: `Apex triggers couldn't be read: ${triggers.reason}`, evidence: [ctx.ev(triggers.source, { count: null })] };
    const tools = new Map<string, Set<string>>();
    const add = (object: string | null, tool: string): void => {
      if (!object) return;
      if (!tools.has(object)) tools.set(object, new Set());
      tools.get(object)!.add(tool);
    };
    for (const t of triggers.data) if (t.Status === "Active") add(t.TableEnumOrId, "Apex trigger");
    for (const f of result.bundle.flows) {
      if (f.status !== "Active") continue;
      if (f.processType === "Workflow") add(f.object, "Process Builder");
      else if (f.triggerType?.startsWith("RecordAfter") || f.triggerType?.startsWith("RecordBefore")) add(f.object, "Record-triggered flow");
    }
    for (const w of result.bundle.workflows) if (w.rules.some((r) => r.active)) add(w.object, "Workflow Rule");

    const mixed = [...tools].filter(([, set]) => set.size >= MIXED_AUTOMATION_LIMIT).sort(([a], [b]) => a.localeCompare(b));
    const evidence = [ctx.ev({ kind: "metadata", path: "flows/*.flow, workflows/*.workflow" }, { count: mixed.length }), ctx.ev(triggers.source, { count: triggers.data.length })];
    const segments = [{ dimension: "object", segments: tools.size }];
    if (mixed.length === 0) return { outcome: "pass", summary: `No object runs ${MIXED_AUTOMATION_LIMIT} or more automation tools.`, evidence, segmentsChecked: segments };
    return {
      outcome: "issue",
      title: "Automation on some objects is split across several tools",
      summary: `${mixed.map(([o, set]) => `${o} runs ${[...set].sort().join(", ")}`).join("; ")}. Salesforce runs these at different points when a record saves, so the outcome can be hard to predict.`,
      recommendation: "Consolidate each of these objects on one tool, with a defined order of execution.",
      recommendationType: "change",
      severity: "medium",
      confidence: "high",
      evidence,
      value: notSized("The cost shows up as slower changes and harder debugging, which we can't measure from metadata."),
      effort: { size: "L" },
      owner: "Salesforce developer",
      segmentsChecked: segments,
      subjects: mixed.map(([o]) => ({ type: "object", label: o })),
      facts: { objects: mixed.length },
    };
  },
};

const hardcodedIds: Rule = {
  id: "aut.hardcoded-ids-in-flows",
  version: 1,
  domain: "automation",
  title: "Record ids or emails hardcoded in flows",
  evaluate(ctx): RuleResult | RuleResult[] {
    const result = metadataOr(ctx);
    if ("outcome" in result) return result;
    const active = result.bundle.flows.filter((f) => f.status === "Active");
    const affected = active.filter((f) => f.hardcodedUserIds + f.hardcodedOtherIds + f.hardcodedEmails > 0);
    const evidence = [ctx.ev({ kind: "metadata", path: "flows/*.flow" }, { count: affected.length, exclusions: ["Inactive versions"] })];
    const segments = [{ dimension: "flow", segments: active.length }];
    if (affected.length === 0) return { outcome: "pass", summary: `None of the ${active.length} active flows contain hardcoded ids or emails.`, evidence, segmentsChecked: segments };
    return {
      outcome: "issue",
      title: "Flows depend on hardcoded ids or email addresses",
      summary: `Of ${plural(active.length, "active flow")}, ${affected.length} ${affected.length === 1 ? "contains" : "contain"} hardcoded user ids, record ids or email addresses. These break when someone leaves or when the flow moves between orgs.`,
      recommendation: "Replace them with custom metadata, custom labels or queue lookups.",
      recommendationType: "change",
      severity: "high",
      confidence: "high",
      evidence,
      value: notSized("The cost is a future outage rather than a present one."),
      effort: { size: "S" },
      owner: "Salesforce admin",
      segmentsChecked: segments,
      subjects: affected.map((f) => ({ type: "flow", label: f.name })),
      facts: { affected: affected.length, activeFlows: active.length },
    };
  },
};

const missingFaultPaths: Rule = {
  id: "aut.flows-without-fault-paths",
  version: 1,
  domain: "automation",
  title: "Flows that write data without handling failure",
  evaluate(ctx): RuleResult | RuleResult[] {
    const result = metadataOr(ctx);
    if ("outcome" in result) return result;
    const active = result.bundle.flows.filter((f) => f.status === "Active" && f.processType !== "Workflow" && f.dmlOrActions > 0);
    const affected = active.filter((f) => f.withFault < f.dmlOrActions);
    const missing = affected.reduce((sum, f) => sum + (f.dmlOrActions - f.withFault), 0);
    const evidence = [ctx.ev({ kind: "metadata", path: "flows/*.flow" }, { count: affected.length, exclusions: ["Inactive versions", "Process Builders, which have no fault paths"] })];
    const segments = [{ dimension: "flow", segments: active.length }];
    if (affected.length === 0) return { outcome: "pass", summary: `All ${active.length} active flows that write data have fault paths.`, evidence, segmentsChecked: segments };
    return {
      outcome: "issue",
      title: "Flow failures aren't handled",
      summary: `Of ${plural(active.length, "active flow")} that write data, ${affected.length} ${affected.length === 1 ? "is" : "are"} missing fault paths on ${plural(missing, "element")}. When one of those fails, the user sees a generic error and the failure isn't logged anywhere useful.`,
      recommendation: "Add a fault path to each element, with a shared way of logging failures.",
      recommendationType: "change",
      severity: "medium",
      confidence: "high",
      evidence,
      value: notSized("We can't size this until we can see how often these flows fail."),
      effort: { size: "M" },
      owner: "Salesforce admin",
      segmentsChecked: segments,
      subjects: affected.map((f) => ({ type: "flow", label: f.name })),
      facts: { affected: affected.length, flowsWritingData: active.length, missingFaultPaths: missing },
    };
  },
};

const oldApiVersions: Rule = {
  id: "aut.old-api-versions",
  version: 1,
  domain: "automation",
  title: "Code and flows on old API versions",
  evaluate(ctx): RuleResult | RuleResult[] {
    const result = metadataOr(ctx);
    if ("outcome" in result) return result;
    const { classes, triggers } = ctx.data(apexExtractor);
    if (classes.status !== "ok") return { outcome: "not_measurable", reason: `Apex classes couldn't be read: ${classes.reason}`, evidence: [ctx.ev(classes.source, { count: null })] };
    if (triggers.status !== "ok") return { outcome: "not_measurable", reason: `Apex triggers couldn't be read: ${triggers.reason}`, evidence: [ctx.ev(triggers.source, { count: null })] };
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
  version: 1,
  domain: "automation",
  title: "Apex test coverage",
  evaluate(ctx): RuleResult | RuleResult[] {
    const { classes, orgCoverage, classCoverage } = ctx.data(apexExtractor);
    if (classes.status !== "ok") return { outcome: "not_measurable", reason: `Apex classes couldn't be read: ${classes.reason}`, evidence: [ctx.ev(classes.source, { count: null })] };
    if (classes.data.length === 0) {
      return { outcome: "pass", summary: "This org has no Apex of its own.", evidence: [ctx.ev(classes.source, { count: 0 })], segmentExemptReason: "There's no code to segment." };
    }
    const percent = orgCoverage.status === "ok" ? orgCoverage.data[0]?.PercentCovered : null;
    if (percent === null || percent === undefined) {
      return { outcome: "not_measurable", reason: "The org holds no coverage figure, which usually means tests haven't been run recently. We don't run tests, because that would change the org.", evidence: [ctx.ev(orgCoverage.source, { count: null })] };
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
  version: 1,
  domain: "automation",
  title: "More than one trigger on an object",
  evaluate(ctx): RuleResult | RuleResult[] {
    const { triggers } = ctx.data(apexExtractor);
    if (triggers.status !== "ok") return { outcome: "not_measurable", reason: `Apex triggers couldn't be read: ${triggers.reason}`, evidence: [ctx.ev(triggers.source, { count: null })] };
    const byObject = new Map<string, number>();
    for (const t of triggers.data) if (t.Status === "Active") byObject.set(t.TableEnumOrId, (byObject.get(t.TableEnumOrId) ?? 0) + 1);
    const multiple = [...byObject].filter(([, n]) => n > 1).sort(([a], [b]) => a.localeCompare(b));
    const evidence = [ctx.ev(triggers.source, { count: triggers.data.length, exclusions: ["Inactive triggers", "Packaged triggers"] })];
    const segments = [{ dimension: "object", segments: byObject.size }];
    if (multiple.length === 0) {
      return byObject.size === 0
        ? { outcome: "pass", summary: "This org has no active triggers of its own.", evidence, segmentExemptReason: "There are no triggers to segment." }
        : { outcome: "pass", summary: `No object has more than one active trigger, across ${byObject.size} objects.`, evidence, segmentsChecked: segments };
    }
    return {
      outcome: "issue",
      title: "Some objects have several triggers",
      summary: `${multiple.map(([o, n]) => `${o} has ${n}`).join("; ")}. Salesforce doesn't guarantee the order they run in.`,
      recommendation: "Move to one trigger per object, calling a handler class.",
      recommendationType: "change",
      severity: "medium",
      confidence: "high",
      evidence,
      value: notSized("The cost is unpredictable behaviour rather than a direct figure."),
      effort: { size: "M" },
      owner: "Salesforce developer",
      segmentsChecked: segments,
      subjects: multiple.map(([o]) => ({ type: "object", label: o })),
      facts: { objects: multiple.length },
    };
  },
};

export const automationModule: DomainModule = {
  id: "automation",
  cloud: "sales",
  label: "Automation and code",
  extractors: [metadataExtractor, apexExtractor, scheduledJobsExtractor],
  rules: [legacyWorkflowRules, legacyProcessBuilder, mixedAutomation, hardcodedIds, missingFaultPaths, oldApiVersions, apexCoverage, multipleTriggers],
};
