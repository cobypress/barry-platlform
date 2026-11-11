import { apexExtractor, metadataExtractor, scheduledJobsExtractor } from "../../extract/automation.ts";
import { componentChangesExtractor } from "../../extract/component-changes.ts";
import { customObjectsExtractor, recordCountsExtractor } from "../../extract/configuration.ts";
import { notSized, plural, type Rule, type RuleResult } from "../../engine/rule.ts";
import { MIGRATE_TO_FLOW_NOTE, END_OF_SUPPORT } from "./notes.ts";
import { apexGap, groupCount, metadataOr, UNNAMED, unnamedNote } from "./shared.ts";
import { labelsOf } from "../labels.ts";
import { labelsExtractor } from "../../extract/labels.ts";
import type { DomainModule } from "../types.ts";
import { objectResolver } from "../object-names.ts";

// Apex coverage, API versions and trigger counts moved to technical health
// (DECISIONS.md 169). This area keeps how the automation is built.

const MIXED_AUTOMATION_LIMIT = 3;
// Our judgement: past this, nobody new can tell what the automation does or why.
const UNDOCUMENTED_LIMIT = 50;

const legacyWorkflowRules: Rule = {
  id: "aut.legacy-workflow-rules",
  version: 3,
  domain: "automation",
  title: "Workflow Rules still active",
  evaluate(ctx): RuleResult | RuleResult[] {
    const result = metadataOr(ctx, ["workflowRules"]);
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
      recommendation: `Plan the move from Workflow Rules to Flow, starting with the objects that have the most rules. ${MIGRATE_TO_FLOW_NOTE}`,
      recommendationType: "change",
      severity: "high",
      confidence: "high",
      evidence,
      value: notSized("The migration effort depends on how complex each rule is, which needs a closer read."),
      effort: { size: active.length > 20 ? "L" : "M" },
      owner: "Salesforce admin",
      dependencies: ["Test each migrated rule in a sandbox before switching it off"],
      segmentsChecked: segments,
      // The rule's own name, and its object's label (DECISIONS.md 411).
      subjects: active.map((r) => ({ type: "workflow rule", label: `${r.name} (${labelsOf(ctx).object(r.object)})` })),
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
      recommendation: `Move each Process Builder to Flow in the same piece of work as any Workflow Rules on its object, so each object is migrated once. ${MIGRATE_TO_FLOW_NOTE}`,
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
  version: 4,
  domain: "automation",
  title: "Several automation tools acting on the same object",
  evaluate(ctx): RuleResult | RuleResult[] {
    const result = metadataOr(ctx, ["workflowRules"]);
    if ("outcome" in result) return result;
    const { triggers } = ctx.data(apexExtractor);
    if (triggers.status !== "ok") return { outcome: "not_measurable", reason: apexGap("triggers", triggers.reason), evidence: [ctx.ev(triggers.source, { count: null })] };
    // A trigger on a custom object can name it by id, and a flow names it by API name.
    const ids = ctx.data(customObjectsExtractor);
    const objectOf = objectResolver(ids);
    const tools = new Map<string, Set<string>>();
    const add = (object: string | null, tool: string): void => {
      if (!object) return;
      if (!tools.has(object)) tools.set(object, new Set());
      tools.get(object)!.add(tool);
    };
    const active = triggers.data.filter((t) => t.Status === "Active");
    const unnamed = active.filter((t) => objectOf(t.TableEnumOrId) === null).length;
    for (const t of active) add(objectOf(t.TableEnumOrId), "Apex trigger");
    for (const f of result.bundle.flows) {
      if (f.status !== "Active") continue;
      if (f.processType === "Workflow") add(f.object, "Process Builder");
      else if (f.triggerType?.startsWith("RecordAfter") || f.triggerType?.startsWith("RecordBefore")) add(f.object, "Record-triggered flow");
    }
    for (const w of result.bundle.workflows) if (w.rules.some((r) => r.active)) add(w.object, "Workflow Rule");

    const mixed = [...tools].filter(([, set]) => set.size >= MIXED_AUTOMATION_LIMIT).sort(([a], [b]) => a.localeCompare(b));
    const note = unnamedNote(unnamed, ids.status === "ok", unnamed === 1 ? "it isn't counted against any object" : "they aren't counted against any object");
    const evidence = [
      ctx.ev({ kind: "metadata", path: "flows/*.flow, workflows/*.workflow" }, { count: mixed.length }),
      ctx.ev(triggers.source, { count: triggers.data.length, exclusions: unnamed ? [`Active triggers on custom objects we couldn't name: ${unnamed}`] : [] }),
      ctx.ev(ids.source, { count: ids.status === "ok" ? ids.data.length : null, note: "Names the custom objects triggers refer to by id" }),
    ];
    const segments = [{ dimension: "object", segments: tools.size }];
    if (mixed.length === 0) return { outcome: "pass", summary: `No object runs ${MIXED_AUTOMATION_LIMIT} or more automation tools.${note}`, evidence, segmentsChecked: segments, facts: { unnamedTriggers: unnamed } };
    return {
      outcome: "issue",
      title: "Automation on some objects is split across several tools",
      summary: `${mixed.map(([o, set]) => `${labelsOf(ctx).object(o)} runs ${[...set].sort().join(", ")}`).join("; ")}. Salesforce runs these at different points when a record saves, so the outcome can be hard to predict.${note}`,
      recommendation: "Decide for each object what belongs in Apex and what in Flow, then retire the older tools into that split.",
      recommendationType: "change",
      severity: "medium",
      confidence: "high",
      evidence,
      value: notSized("The cost shows up as slower changes and harder debugging, which we can't measure from metadata."),
      effort: { size: "L" },
      owner: "Salesforce developer",
      segmentsChecked: segments,
      subjects: mixed.map(([o]) => ({ type: "object", label: labelsOf(ctx).object(o) })),
      facts: { objects: mixed.length, unnamedTriggers: unnamed },
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

// A real customer's main complaint was an org nobody could pick up: a lot of
// automation and no record of what it does. Salesforce keeps a description on
// flows and rules, and code keeps comments, so their absence is measurable.
const undocumentedAutomation: Rule = {
  id: "aut.undocumented-automation",
  version: 2,
  domain: "automation",
  title: "Automation with no description of what it does",
  evaluate(ctx): RuleResult | RuleResult[] {
    const result = metadataOr(ctx, ["workflowRules", "validationRules"]);
    if ("outcome" in result) return result;
    const { classes, triggers } = ctx.data(apexExtractor);
    if (classes.status !== "ok") return { outcome: "not_measurable", reason: apexGap("classes", classes.reason), evidence: [ctx.ev(classes.source, { count: null })] };
    if (triggers.status !== "ok") return { outcome: "not_measurable", reason: apexGap("triggers", triggers.reason), evidence: [ctx.ev(triggers.source, { count: null })] };
    const { flows, workflows, validationRules } = result.bundle;
    // Test classes document nothing a maintainer needs.
    const code = [...classes.data, ...triggers.data].filter((c) => c.Status === "Active" && !c.isTest && c.commented !== null);
    const groups = [
      { kind: "Flows", items: flows.filter((f) => f.status === "Active" && f.processType !== "Workflow"), undocumented: (f: { described: boolean }) => !f.described },
      { kind: "Process Builders", items: flows.filter((f) => f.status === "Active" && f.processType === "Workflow"), undocumented: (f: { described: boolean }) => !f.described },
      { kind: "Workflow Rules", items: workflows.flatMap((w) => w.rules.filter((r) => r.active)), undocumented: (r: { described: boolean }) => !r.described },
      { kind: "Validation rules", items: validationRules.filter((v) => v.active), undocumented: (v: { described: boolean }) => !v.described },
      { kind: "Apex classes and triggers", items: code, undocumented: (c: { commented: boolean | null }) => !c.commented },
    ].map((g) => ({ kind: g.kind, total: g.items.length, missing: (g.items as Array<never>).filter(g.undocumented).length }));
    const present = groups.filter((g) => g.total > 0);
    const total = present.reduce((sum, g) => sum + g.total, 0);
    const missing = present.reduce((sum, g) => sum + g.missing, 0);
    const evidence = [
      ctx.ev({ kind: "metadata", path: "flows, workflows, objects" }, { count: total - code.length, exclusions: ["Inactive flows and rules"], note: "Description element" }),
      ctx.ev(classes.source, { count: code.length, exclusions: ["Test classes", "Inactive or hidden code"], note: "Any comment in the code" }),
    ];
    const segments = [{ dimension: "component type", segments: present.length }];
    if (total === 0) return { outcome: "pass", summary: "There's no active automation to document.", evidence, segmentExemptReason: "Nothing to segment." };
    const rate = Math.round((missing / total) * 1000) / 10;
    const worst = [...present].sort((a, b) => b.missing / b.total - a.missing / a.total)[0]!;
    const facts = { total, missing, rate };
    const breakdown = present.map((g) => `${g.kind}: ${g.missing} of ${g.total}`).join(", ");
    if (rate <= UNDOCUMENTED_LIMIT && worst.missing / worst.total <= 0.8) {
      return { outcome: "pass", summary: `Automation with no description or comment: ${missing} of ${total} (${rate}%). ${breakdown}.`, evidence, segmentsChecked: segments, facts };
    }
    return {
      outcome: "issue",
      title: "Most automation has no description of what it does",
      summary: `Active automation with no description or comment: ${missing} of ${total} (${rate}%). ${breakdown}. Without one, anyone new has to read each component to learn what it does and why, and changing it safely depends on whoever built it.`,
      recommendation: "Describe what each flow, rule and class does and why, starting with your busiest objects, and require it for anything new.",
      recommendationType: "change",
      severity: "medium",
      confidence: "high",
      evidence,
      value: notSized("The cost is the time it takes to understand, change and hand over the org, which we can't measure from metadata."),
      effort: { size: "L" },
      owner: "Salesforce admin",
      segmentsChecked: segments,
      facts,
    };
  },
};

export const automationModule: DomainModule = {
  id: "automation",
  cloud: "sales",
  label: "Automation and code",
  // Record counts first: the metadata step reads them to know which standard objects to name.
  extractors: [labelsExtractor, recordCountsExtractor, metadataExtractor, apexExtractor, componentChangesExtractor, scheduledJobsExtractor, customObjectsExtractor],
  rules: [legacyWorkflowRules, legacyProcessBuilder, mixedAutomation, hardcodedIds, missingFaultPaths, undocumentedAutomation],
};
