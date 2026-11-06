import { metadataExtractor, apexExtractor } from "../../extract/automation.ts";
import {
  CORE_OBJECTS,
  customObjectsExtractor,
  duplicateRulesExtractor,
  fieldUsageExtractor,
  isOwnCustomField,
  layoutsExtractor,
  recordCountsExtractor,
  schemaExtractor,
  setupAuditExtractor,
} from "../../extract/configuration.ts";
import { opportunitiesExtractor, stagesExtractor } from "../../extract/opportunities.ts";
import { reportDefinitionsExtractor } from "../../extract/report-definitions.ts";
import { loginsExtractor, usersExtractor } from "../../extract/users.ts";
import { fieldGuards } from "../../guards/field-guards.ts";
import { notSized, pct, plural, type Rule, type RuleResult } from "../../engine/rule.ts";
import type { DomainModule } from "../types.ts";
import { calibratedHours } from "../value.ts";
import { objectInventoryMetric } from "./inventory.ts";
import { documentedMetrics } from "./documented.ts";
import { orgDocumentationExtractor } from "../../extract/documentation.ts";
import { accessExtractor } from "../../extract/security.ts";
import { loginActivityExtractor } from "../../extract/adoption.ts";

const FIELD_SPRAWL_LIMIT = 150;
const UNUSED_FILL_LIMIT = 1;
const MIN_RECORDS_FOR_UNUSED = 100;
const STAGE_LIMIT = 10;
const PICKLIST_LIMIT = 25;
const PICKLISTS: Array<[string, string]> = [["Lead", "LeadSource"], ["Account", "Industry"], ["Opportunity", "Type"]];
const CONCENTRATION_SHARE = 80;
// Our judgement, as for automation: past half, a newcomer is guessing at most fields.
const UNDOCUMENTED_FIELD_LIMIT = 50;
const MIN_CHANGES_FOR_CONCENTRATION = 20;

const customFieldSprawl: Rule = {
  id: "cfg.custom-field-sprawl",
  version: 1,
  domain: "configuration",
  title: "Too many custom fields on core objects",
  evaluate(ctx): RuleResult | RuleResult[] {
    const schema = ctx.data(schemaExtractor);
    const results: RuleResult[] = [];
    const counts: Array<[string, number]> = [];
    for (const object of CORE_OBJECTS) {
      const summary = schema[object]!;
      if (summary.status !== "ok") {
        results.push({ outcome: "not_measurable", key: object, reason: `${object} couldn't be described: ${summary.reason}`, evidence: [ctx.ev(summary.source, { count: null })] });
      } else {
        counts.push([object, summary.data.fields.filter(isOwnCustomField).length]);
      }
    }
    const over = counts.filter(([, n]) => n >= FIELD_SPRAWL_LIMIT);
    const evidence = counts.map(([object, n]) => ctx.ev({ kind: "rest", path: `sobjects/${object}/describe` }, { count: n, note: `Own custom fields on ${object}` }));
    if (over.length > 0) {
      results.push({
        outcome: "issue",
        title: "Core objects carry a heavy custom field load",
        summary: `${over.map(([o, n]) => `${o} has ${n} custom fields`).join("; ")}. We use ${FIELD_SPRAWL_LIMIT} as a working limit: past it, page layouts and reports get harder to keep clean and users stop filling fields in.`,
        recommendation: "Review these objects with the field usage findings and agree which fields still earn their place.",
        recommendationType: "investigate",
        severity: "medium",
        confidence: "high",
        evidence,
        value: notSized("The cost of field sprawl depends on how many fields users really see, which we don't measure yet."),
        effort: { size: "M" },
        owner: "Salesforce admin",
        segmentsChecked: [{ dimension: "object", segments: counts.length }],
        facts: Object.fromEntries(over),
      });
    } else if (counts.length > 0) {
      results.push({
        outcome: "pass",
        summary: `All ${counts.length} core objects are under ${FIELD_SPRAWL_LIMIT} custom fields.`,
        evidence,
        segmentsChecked: [{ dimension: "object", segments: counts.length }],
        facts: Object.fromEntries(counts),
      });
    }
    return results;
  },
};

const unusedCustomFields: Rule = {
  id: "cfg.unused-custom-fields",
  version: 2,
  records: true,
  domain: "configuration",
  title: "Custom fields that are almost never filled in",
  evaluate(ctx): RuleResult | RuleResult[] {
    const usage = ctx.data(fieldUsageExtractor);
    const schema = ctx.data(schemaExtractor);
    const results: RuleResult[] = [];
    for (const object of CORE_OBJECTS) {
      const scan = usage[object]!;
      if (scan.status !== "ok") {
        results.push({ outcome: "not_measurable", key: object, reason: `Field usage on ${object} couldn't be read: ${scan.reason}`, evidence: [ctx.ev(scan.source, { count: null })] });
        continue;
      }
      const describe = schema[object]!;
      const summary = describe.status === "ok" ? describe.data : null;
      const total = scan.data.fields[0]?.total ?? 0;
      const range = { from: scan.data.recentFrom, to: ctx.window.to };
      const exclusions = [`Not scanned, because their type can't be counted: ${scan.data.notScanned.length} fields`, "Packaged and formula fields"];
      if (scan.data.fields.length > 0 && total < MIN_RECORDS_FOR_UNUSED) {
        results.push({ outcome: "not_measurable", key: object, reason: `${object} has ${total} records, below the ${MIN_RECORDS_FOR_UNUSED} needed to call a field unused.`, evidence: [ctx.ev(scan.source, { count: total, exclusions })] });
        continue;
      }
      // Both tests must agree: a field empty overall but filling up this year is a new field.
      const unused = scan.data.fields.filter((f) => pct(f.filled, f.total) < UNUSED_FILL_LIMIT && pct(f.recentFilled, f.recentTotal) < UNUSED_FILL_LIMIT);
      const segments = [{ dimension: "all records", segments: scan.data.fields.length }, { dimension: "records created in the last 12 months", segments: scan.data.fields.length }];
      if (unused.length === 0) {
        results.push({ outcome: "pass", key: object, summary: `Every scanned custom field on ${object} is filled on at least ${UNUSED_FILL_LIMIT}% of records.`, evidence: [ctx.ev(scan.source, { count: scan.data.fields.length, exclusions })], segmentsChecked: segments });
        continue;
      }
      const fields = unused.flatMap((u) => summary?.fields.find((f) => f.name === u.field) ?? []);
      results.push({
        outcome: "issue",
        key: object,
        title: `Custom fields on ${object} are almost never filled in`,
        summary: `Of ${plural(scan.data.fields.length, "scanned custom field")} on ${object}, ${unused.length} ${unused.length === 1 ? "is" : "are"} filled on under ${UNUSED_FILL_LIMIT}% of records, both overall and on records from the last 12 months.`,
        recommendation: `Remove ${plural(unused.length, "field")} from ${object} once the data is archived.`,
        blockedRecommendation: `Review ${plural(unused.length, "field")} with the people who own ${object}. We aren't recommending removal, because not every dependency check could pass.`,
        recommendationType: "remove",
        severity: "low",
        confidence: "medium",
        evidence: [ctx.ev(scan.source, { count: unused.length, dateRange: range, exclusions })],
        value: calibratedHours(ctx, "custom-field-upkeep", unused.length, "unused custom field"),
        effort: { size: "M" },
        owner: "Salesforce admin",
        dependencies: ["Archive any data that must be kept before a field is deleted"],
        guards: fields.flatMap((f) => fieldGuards(ctx, object, f)),
        segmentsChecked: segments,
        subjects: unused.map((u) => ({ type: "field", label: `${object}.${u.field}` })),
        facts: { unusedFields: unused.length, scannedFields: scan.data.fields.length },
      });
    }
    return results;
  },
};

const unassignedLayouts: Rule = {
  id: "cfg.unassigned-layouts",
  version: 1,
  domain: "configuration",
  title: "Page layouts no profile uses",
  evaluate(ctx): RuleResult | RuleResult[] {
    const { layouts, assignments } = ctx.data(layoutsExtractor);
    if (layouts.status !== "ok") return { outcome: "not_measurable", reason: `Layouts couldn't be read: ${layouts.reason}`, evidence: [ctx.ev(layouts.source, { count: null })] };
    if (assignments.status !== "ok") return { outcome: "not_measurable", reason: `Layout assignments couldn't be read: ${assignments.reason}`, evidence: [ctx.ev(assignments.source, { count: null })] };
    const assigned = new Set(assignments.data.map((a) => a.LayoutId));
    const unused = layouts.data.filter((l) => !assigned.has(l.Id));
    const objects = new Set(layouts.data.map((l) => l.EntityDefinitionId));
    const evidence = [ctx.ev(layouts.source, { count: layouts.data.length }), ctx.ev(assignments.source, { count: assigned.size })];
    if (unused.length === 0) return { outcome: "pass", summary: `All ${layouts.data.length} page layouts are assigned to a profile.`, evidence, segmentsChecked: [{ dimension: "object", segments: objects.size }] };
    return {
      outcome: "issue",
      title: "Some page layouts aren't assigned to any profile",
      summary: `Of ${plural(layouts.data.length, "page layout")}, ${unused.length} ${unused.length === 1 ? "isn't" : "aren't"} assigned to any profile, so nobody sees ${unused.length === 1 ? "it" : "them"}.`,
      recommendation: "Review the unassigned layouts and retire the ones nobody needs.",
      recommendationType: "investigate",
      severity: "low",
      confidence: "high",
      evidence,
      value: notSized("Unused layouts are a tidiness cost, not a measurable one."),
      effort: { size: "S" },
      owner: "Salesforce admin",
      segmentsChecked: [{ dimension: "object", segments: objects.size }],
      subjects: unused.map((l) => ({ type: "layout", label: l.Name })),
      facts: { unassigned: unused.length, layouts: layouts.data.length },
    };
  },
};

const undocumentedFields: Rule = {
  id: "cfg.undocumented-fields",
  version: 1,
  domain: "configuration",
  title: "Custom fields with no help text or description",
  evaluate(ctx): RuleResult | RuleResult[] {
    const metadata = ctx.data(metadataExtractor);
    const source = { kind: "metadata", path: "objects/*.object" } as const;
    if (metadata.status !== "ok") return { outcome: "not_measurable", reason: `Object metadata couldn't be read: ${metadata.reason}`, evidence: [ctx.ev(source, { count: null })] };
    const fields = metadata.data.customFields;
    const objects = new Set(fields.map((f) => f.object));
    const evidence = [ctx.ev(source, { count: fields.length, exclusions: ["Standard fields", "Fields from installed packages"] })];
    const segments = [{ dimension: "object", segments: objects.size }];
    if (fields.length === 0) return { outcome: "pass", summary: "There are no custom fields of your own to document.", evidence, segmentExemptReason: "Nothing to segment." };
    const bare = fields.filter((f) => !f.described && !f.helpText);
    const rate = pct(bare.length, fields.length);
    const noHelp = fields.filter((f) => !f.helpText).length;
    const facts = { fields: fields.length, bare: bare.length, rate, noHelpText: noHelp };
    if (rate <= UNDOCUMENTED_FIELD_LIMIT) return { outcome: "pass", summary: `Custom fields with neither help text nor a description: ${bare.length} of ${fields.length} (${rate}%).`, evidence, segmentsChecked: segments, facts };
    const byObject = [...objects].map((o) => ({ o, n: bare.filter((f) => f.object === o).length })).filter((x) => x.n > 0).sort((a, b) => b.n - a.n || a.o.localeCompare(b.o));
    return {
      outcome: "issue",
      title: "Most custom fields don't say what they're for",
      summary: `Custom fields with neither help text nor a description: ${bare.length} of ${fields.length} (${rate}%), most on ${byObject.slice(0, 3).map((x) => `${x.o} (${x.n})`).join(", ")}. Users see no explanation when they fill these in, and whoever maintains the org can't tell what each one is for.`,
      recommendation: "Add help text to the fields people fill in and a description to every field you keep, and retire the ones nobody can explain.",
      recommendationType: "change",
      severity: "low",
      confidence: "high",
      evidence,
      value: notSized("The cost is mistakes and time spent working out what a field means, which we can't measure."),
      effort: { size: "M" },
      owner: "Salesforce admin",
      segmentsChecked: segments,
      subjects: byObject.slice(0, 10).map((x) => ({ type: "object", label: x.o })),
      facts,
    };
  },
};

const stageSprawl: Rule = {
  id: "cfg.stage-sprawl",
  version: 1,
  records: true,
  domain: "configuration",
  title: "Too many opportunity stages, or stages nobody uses",
  evaluate(ctx): RuleResult | RuleResult[] {
    const stages = ctx.data(stagesExtractor);
    if (stages.status !== "ok") return { outcome: "not_measurable", reason: `Opportunity stages couldn't be read: ${stages.reason}`, evidence: [ctx.ev(stages.source, { count: null })] };
    const { history } = ctx.data(opportunitiesExtractor);
    if (history.status !== "ok") return { outcome: "not_measurable", reason: `Stage history couldn't be read: ${history.reason}`, evidence: [ctx.ev(history.source, { count: null })] };
    const active = stages.data.filter((s) => s.IsActive);
    const used = new Set(history.data.map((h) => h.StageName));
    const unused = active.filter((s) => !used.has(s.ApiName));
    const evidence = [ctx.ev(stages.source, { count: active.length }), ctx.ev(history.source, { count: history.data.length, dateRange: ctx.window })];
    const segments = [{ dimension: "stage", segments: active.length }];
    if (active.length <= STAGE_LIMIT && unused.length === 0) {
      return { outcome: "pass", summary: `${active.length} active stages, all used in the analysis window.`, evidence, segmentsChecked: segments, facts: { activeStages: active.length } };
    }
    const parts = [
      active.length > STAGE_LIMIT ? `${plural(active.length, "active stage")}, against our working limit of ${STAGE_LIMIT}` : null,
      unused.length > 0 ? `${plural(unused.length, "active stage")} with no deals in the analysis window` : null,
    ].filter(Boolean);
    return {
      outcome: "issue",
      title: "The sales process has more stages than deals really use",
      summary: `${parts.join(", and ")}. Extra stages blur conversion figures and make forecasting harder.`,
      recommendation: "Agree the stages that mark real buyer progress, then map the rest to them.",
      recommendationType: "change",
      severity: "medium",
      confidence: "high",
      evidence,
      value: notSized("Stage cleanup improves forecast quality, which we can't size from configuration alone."),
      effort: { size: "M", note: "Needs sales leadership to agree the process" },
      owner: "Sales operations",
      dependencies: ["Sales leadership agreement on the stage model"],
      segmentsChecked: segments,
      subjects: unused.map((s) => ({ type: "stage", label: s.MasterLabel })),
      facts: { activeStages: active.length, unusedStages: unused.length },
    };
  },
};

const picklistSprawl: Rule = {
  id: "cfg.picklist-sprawl",
  version: 1,
  domain: "configuration",
  title: "Key picklists have too many values",
  evaluate(ctx): RuleResult | RuleResult[] {
    const schema = ctx.data(schemaExtractor);
    const sized: Array<{ label: string; n: number }> = [];
    const evidence = [];
    for (const [object, name] of PICKLISTS) {
      const summary = schema[object]!;
      if (summary.status !== "ok") continue;
      const field = summary.data.fields.find((f) => f.name === name);
      if (!field) continue;
      sized.push({ label: `${object}.${name}`, n: field.activePicklistValues.length });
      evidence.push(ctx.ev(summary.source, { count: field.activePicklistValues.length, note: `Active values on ${object}.${name}` }));
    }
    if (sized.length === 0) return { outcome: "not_measurable", reason: "None of the picklists we check could be read.", evidence: [ctx.ev(schema.Lead!.source, { count: null })] };
    const over = sized.filter((p) => p.n > PICKLIST_LIMIT);
    const segments = [{ dimension: "picklist", segments: sized.length }];
    if (over.length === 0) return { outcome: "pass", summary: `All ${sized.length} picklists we check are at or under ${PICKLIST_LIMIT} values.`, evidence, segmentsChecked: segments };
    return {
      outcome: "issue",
      title: "Reporting picklists have grown too long",
      summary: `${over.map((p) => `${p.label} has ${p.n} active values`).join("; ")}. Long lists split the same answer across several values, which weakens every report built on them.`,
      recommendation: "Consolidate these lists and move old values out of use.",
      recommendationType: "change",
      severity: "low",
      confidence: "high",
      evidence,
      value: notSized("The benefit is cleaner reporting, which has no direct figure."),
      effort: { size: "S" },
      owner: "Salesforce admin",
      segmentsChecked: segments,
      subjects: over.map((p) => ({ type: "picklist", label: p.label })),
      facts: Object.fromEntries(over.map((p) => [p.label, p.n])),
    };
  },
};

const setupChangeConcentration: Rule = {
  id: "cfg.setup-change-concentration",
  version: 1,
  domain: "configuration",
  title: "Setup changes rest on one person or on people who have left",
  evaluate(ctx): RuleResult | RuleResult[] {
    const audit = ctx.data(setupAuditExtractor);
    const users = ctx.data(usersExtractor);
    if (audit.status !== "ok") return { outcome: "not_measurable", reason: `The setup audit trail couldn't be read: ${audit.reason}`, evidence: [ctx.ev(audit.source, { count: null })] };
    if (users.status !== "ok") return { outcome: "not_measurable", reason: `Users couldn't be read: ${users.reason}`, evidence: [ctx.ev(users.source, { count: null })] };
    const { configChangesByUser, excludedUserAdmin, earliest, rows } = audit.data;
    const changes = Object.values(configChangesByUser).reduce((a, b) => a + b, 0);
    const range = earliest ? { from: earliest.slice(0, 10), to: ctx.window.to } : null;
    const evidence = [ctx.ev(audit.source, { count: rows, dateRange: range, exclusions: [`${excludedUserAdmin} Manage Users changes`], note: audit.truncated ? "Only the most recent rows were read" : undefined })];
    const people = Object.keys(configChangesByUser);
    const segments = [{ dimension: "user", segments: people.length }, { dimension: "section", segments: Object.keys(audit.data.bySection).length }];
    if (changes < MIN_CHANGES_FOR_CONCENTRATION) {
      return { outcome: "not_measurable", reason: `Only ${changes} setup changes in the period, below the ${MIN_CHANGES_FOR_CONCENTRATION} needed to judge who makes them.`, evidence };
    }
    const inactive = new Set(users.data.filter((u) => !u.IsActive).map((u) => u.Id));
    const byInactive = people.filter((id) => inactive.has(id)).reduce((sum, id) => sum + configChangesByUser[id]!, 0);
    const [topUser, topCount] = Object.entries(configChangesByUser).sort(([, a], [, b]) => b - a)[0]!;
    const topShare = pct(topCount, changes);
    if (topShare < CONCENTRATION_SHARE && byInactive === 0) {
      return { outcome: "pass", summary: `${people.length} people made the ${changes} setup changes in the period, and the busiest made ${topShare}%.`, evidence, segmentsChecked: segments, facts: { changes, topShare } };
    }
    return {
      outcome: "issue",
      title: "Org knowledge is concentrated",
      summary:
        topShare >= CONCENTRATION_SHARE
          ? `${ctx.userLabel(topUser)} made ${topShare}% of ${changes} setup changes in the period, so what they know about the org isn't shared.`
          : `${byInactive} of ${changes} setup changes in the period were made by users who are now inactive.`,
      recommendation: "Document how the org is changed, and make sure a second person can do the work.",
      recommendationType: "change",
      severity: "medium",
      confidence: "medium",
      evidence,
      value: notSized("This is a continuity risk rather than a cost."),
      effort: { size: "S" },
      owner: "Salesforce admin",
      segmentsChecked: segments,
      facts: { changes, topShare, byInactive },
    };
  },
};

export const configurationModule: DomainModule = {
  id: "configuration",
  cloud: "sales",
  label: "Configuration",
  extractors: [
    schemaExtractor,
    fieldUsageExtractor,
    recordCountsExtractor,
    layoutsExtractor,
    customObjectsExtractor,
    duplicateRulesExtractor,
    setupAuditExtractor,
    metadataExtractor,
    apexExtractor,
    stagesExtractor,
    usersExtractor,
    opportunitiesExtractor,
    reportDefinitionsExtractor,
    loginsExtractor,
    loginActivityExtractor,
    orgDocumentationExtractor,
    accessExtractor,
  ],
  metrics: [objectInventoryMetric, documentedMetrics],
  rules: [customFieldSprawl, unusedCustomFields, unassignedLayouts, undocumentedFields, stageSprawl, picklistSprawl, setupChangeConcentration],
};
