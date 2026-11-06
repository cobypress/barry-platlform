import { notSized, pct, plural, type Rule, type RuleResult } from "../../engine/rule.ts";
import { missingFieldReason } from "../../extract/fields.ts";
import { cpqSetupExtractor, type FieldType } from "../../extract/cpq.ts";
import { licencesExtractor } from "../../extract/users.ts";
import type { DomainModule } from "../types.ts";
import { listed, presenceGap } from "./shared.ts";

// How CPQ is set up: its price and product rules, and the twin fields that carry
// values from quote lines to opportunity and order products. None of it reads a
// quote, so a set-up review runs it (DECISIONS.md 217).

const DOMAIN = "cpq_setup";
const OWNER = "Salesforce admin";
// Our judgement: a few inactive rules are history; past this they're clutter.
const INACTIVE_MIN = 3;
const INACTIVE_SHARE = 20;
const MAX_LISTED = 6;

const inactiveRules: Rule = {
  id: "cpq.inactive-rules",
  version: 1,
  domain: DOMAIN,
  title: "Inactive price and product rules",
  evaluate(ctx): RuleResult {
    const gap = presenceGap(ctx);
    if (gap) return gap;
    const { priceRules, productRules } = ctx.data(cpqSetupExtractor);
    if (priceRules.status !== "ok") return { outcome: "not_measurable", reason: `Price rules couldn't be read: ${priceRules.reason}`, evidence: [ctx.ev(priceRules.source, { count: null })] };
    if (productRules.status !== "ok") return { outcome: "not_measurable", reason: `Product rules couldn't be read: ${productRules.reason}`, evidence: [ctx.ev(productRules.source, { count: null })] };
    const price = priceRules.data.filter((r) => !r.SBQQ__Active__c);
    const product = productRules.data.filter((r) => !r.SBQQ__Active__c);
    const total = priceRules.data.length + productRules.data.length;
    const inactive = price.length + product.length;
    const evidence = [ctx.ev(priceRules.source, { count: priceRules.data.length }), ctx.ev(productRules.source, { count: productRules.data.length })];
    const segments = [{ dimension: "rule type", segments: 2 }];
    const facts = { inactive, total, priceInactive: price.length, productInactive: product.length, share: pct(inactive, total) };
    if (total === 0) return { outcome: "pass", summary: "There are no price or product rules.", evidence, segmentExemptReason: "There are no rules to segment." };
    if (inactive < INACTIVE_MIN || facts.share < INACTIVE_SHARE) return { outcome: "pass", summary: `Inactive price and product rules: ${inactive} of ${total} (${facts.share}%).`, evidence, segmentsChecked: segments, facts };
    const names = [...price, ...product].map((r) => r.Name).sort();
    return {
      outcome: "issue",
      title: "Inactive pricing rules are cluttering the set-up",
      summary: `${inactive} of ${total} price and product rules are inactive (${facts.share}%): ${plural(price.length, "price rule")} and ${plural(product.length, "product rule")}. Everyone who changes pricing has to work out whether each one still matters, and an inactive rule can be switched back on without anyone knowing why it was off.`,
      recommendation: "Check each inactive rule with whoever owns pricing, and delete the ones nobody will use again.",
      recommendationType: "investigate",
      severity: "low",
      confidence: "high",
      evidence,
      value: notSized("The cost is slower, riskier pricing changes, which we can't size from the set-up."),
      effort: { size: "S" },
      owner: OWNER,
      segmentsChecked: segments,
      subjects: names.slice(0, MAX_LISTED).map((label) => ({ type: "pricing rule", label })),
      facts,
    };
  },
};

const events = (value: string | null | undefined): Set<string> => new Set((value ?? "").split(";").map((e) => e.trim().toLowerCase()).filter(Boolean));

// Two active rules that set the same field, at the same event and in the same
// evaluation order, leave the result to the order CPQ happens to run them in
// (QUESTIONS.md). Their conditions may never both be true, which code can't
// tell, so this is medium confidence.
const priceRuleConflicts: Rule = {
  id: "cpq.price-rule-conflicts",
  version: 1,
  domain: DOMAIN,
  title: "Price rules that set the same field at the same point",
  evaluate(ctx): RuleResult {
    const gap = presenceGap(ctx);
    if (gap) return gap;
    const { priceRules, priceActions } = ctx.data(cpqSetupExtractor);
    if (priceRules.status !== "ok") return { outcome: "not_measurable", reason: `Price rules couldn't be read: ${priceRules.reason}`, evidence: [ctx.ev(priceRules.source, { count: null })] };
    if (priceActions.status !== "ok") return { outcome: "not_measurable", reason: `Price actions couldn't be read: ${priceActions.reason}`, evidence: [ctx.ev(priceActions.source, { count: null })] };
    const missing = missingFieldReason(priceRules, "SBQQ__PriceRule__c.SBQQ__EvaluationEvent__c", "SBQQ__PriceRule__c.SBQQ__EvaluationOrder__c");
    if (missing) return { outcome: "not_measurable", reason: missing, evidence: [ctx.ev(priceRules.source, { count: priceRules.data.length })] };
    const active = priceRules.data.filter((r) => r.SBQQ__Active__c && r.SBQQ__EvaluationOrder__c !== null && r.SBQQ__EvaluationOrder__c !== undefined).sort((a, b) => a.Name.localeCompare(b.Name));
    const fields = new Map<string, Set<string>>();
    for (const a of priceActions.data) if (a.SBQQ__Field__c) fields.set(a.SBQQ__Rule__c, (fields.get(a.SBQQ__Rule__c) ?? new Set()).add(`${(a.SBQQ__TargetObject__c ?? "").toLowerCase()}|${a.SBQQ__Field__c.toLowerCase()}`));
    const pairs: Array<{ a: string; b: string; field: string }> = [];
    for (let i = 0; i < active.length; i++) {
      for (let j = i + 1; j < active.length; j++) {
        const a = active[i]!;
        const b = active[j]!;
        if (a.SBQQ__EvaluationOrder__c !== b.SBQQ__EvaluationOrder__c || (a.SBQQ__TargetObject__c ?? "") !== (b.SBQQ__TargetObject__c ?? "")) continue;
        const ea = events(a.SBQQ__EvaluationEvent__c);
        if (![...events(b.SBQQ__EvaluationEvent__c)].some((e) => ea.has(e))) continue;
        const shared = [...(fields.get(a.Id) ?? [])].find((f) => fields.get(b.Id)?.has(f));
        if (shared) pairs.push({ a: a.Name, b: b.Name, field: priceActions.data.find((x) => x.SBQQ__Rule__c === a.Id && `${(x.SBQQ__TargetObject__c ?? "").toLowerCase()}|${x.SBQQ__Field__c?.toLowerCase()}` === shared)!.SBQQ__Field__c! });
      }
    }
    const evidence = [ctx.ev(priceRules.source, { count: active.length, exclusions: ["Inactive rules", "Rules with no evaluation order"] }), ctx.ev(priceActions.source, { count: priceActions.data.length })];
    const segments = [{ dimension: "evaluation order", segments: new Set(active.map((r) => r.SBQQ__EvaluationOrder__c)).size }];
    const facts = { pairs: pairs.length, activeRules: active.length };
    if (active.length < 2) return { outcome: "pass", summary: `There ${active.length === 1 ? "is one active price rule" : "are no active price rules"} to compare.`, evidence, segmentExemptReason: "There are too few rules to compare.", facts };
    if (pairs.length === 0) return { outcome: "pass", summary: `No two of the ${active.length} active price rules set the same field at the same evaluation event and order.`, evidence, segmentsChecked: segments, facts };
    return {
      outcome: "issue",
      title: "Some price rules compete to set the same field",
      summary: `${plural(pairs.length, "pair")} of active price rules set the same field, at the same evaluation event and in the same evaluation order: ${listed(pairs.slice(0, MAX_LISTED).map((p) => `${p.a} and ${p.b} (${p.field})`))}${pairs.length > MAX_LISTED ? `, and ${pairs.length - MAX_LISTED} more` : ""}. When both apply, which one wins depends on the order CPQ happens to run them in, so a price can change without anyone changing a rule.`,
      recommendation: "Give each pair a different evaluation order, or merge them into one rule, and compare quote totals before and after.",
      recommendationType: "change",
      severity: "medium",
      confidence: "medium",
      evidence,
      value: notSized("The cost is prices that differ from what the rules intend, which we can't size without the quotes they touched."),
      effort: { size: "S" },
      owner: OWNER,
      dependencies: ["Test in a sandbox with real quotes before changing the order"],
      segmentsChecked: segments,
      subjects: pairs.slice(0, MAX_LISTED).map((p) => ({ type: "price rules", label: `${p.a} and ${p.b}` })),
      facts,
    };
  },
};

// CPQ copies a quote line's custom field to the opportunity product, and the
// order product, with the same API name, only when the types match (QUESTIONS.md).
const twinFieldMismatch: Rule = {
  id: "cpq.twin-field-mismatch",
  version: 1,
  domain: DOMAIN,
  title: "Twin fields whose types don't match",
  evaluate(ctx): RuleResult {
    const gap = presenceGap(ctx);
    if (gap) return gap;
    const { twins } = ctx.data(cpqSetupExtractor);
    if (twins.status !== "ok") return { outcome: "not_measurable", reason: `The quote line and product fields couldn't be described: ${twins.reason}`, evidence: [ctx.ev(twins.source, { count: null })] };
    const own = (f: FieldType): boolean => !f.name.toLowerCase().startsWith("sbqq__");
    const lines = twins.data.quoteLine.filter(own);
    const compare = [["opportunity product", twins.data.opportunityProduct], ...(twins.data.orderProduct ? [["order product", twins.data.orderProduct] as const] : [])] as const;
    const mismatches = lines.flatMap((f) =>
      compare.flatMap(([where, others]) => {
        const twin = others.find((o) => o.name.toLowerCase() === f.name.toLowerCase());
        return twin && twin.type !== f.type ? [{ name: f.name, where, line: f.type, other: twin.type }] : [];
      }),
    );
    const evidence = [ctx.ev(twins.source, { count: lines.length, exclusions: ["CPQ's own fields"], note: "Custom fields' names and types, from each object's describe" })];
    const segments = [{ dimension: "object", segments: compare.length }];
    const facts = { mismatches: mismatches.length, quoteLineFields: lines.length };
    if (mismatches.length === 0) return { outcome: "pass", summary: `None of the ${plural(lines.length, "custom field")} on quote lines shares its name with a field of a different type on ${listed(compare.map(([w]) => `${w}s`))}.`, evidence, segmentsChecked: segments, facts };
    return {
      outcome: "issue",
      title: "Some twin fields won't copy",
      summary: `${plural(mismatches.length, "custom field")} on quote lines ${mismatches.length === 1 ? "shares" : "share"} an API name with a field of a different type: ${listed(mismatches.slice(0, MAX_LISTED).map((m) => `${m.name} (${m.line} on the quote line, ${m.other} on the ${m.where})`))}. CPQ copies twin fields only when the name and type match, so these values don't carry over.`,
      recommendation: "Give each pair the same type, or rename one of them if they aren't meant to be twins.",
      recommendationType: "change",
      severity: "medium",
      confidence: "medium",
      evidence,
      value: notSized("The cost is values missing downstream, which the set-up doesn't show."),
      effort: { size: "S" },
      owner: OWNER,
      dependencies: ["Check what reports and automation read each field before changing its type"],
      segmentsChecked: segments,
      subjects: mismatches.map((m) => ({ type: "field", label: m.name })),
      facts,
    };
  },
};

export const cpqSetupModule: DomainModule = {
  id: DOMAIN,
  cloud: "cpq",
  label: "CPQ set-up",
  extractors: [licencesExtractor, cpqSetupExtractor],
  rules: [priceRuleConflicts, twinFieldMismatch, inactiveRules],
};
