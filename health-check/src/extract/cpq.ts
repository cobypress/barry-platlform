import { isUnavailable } from "../salesforce/client.ts";
import { describeObject, planFrom, type FieldSpec } from "./fields.ts";
import { readKeyset } from "./opportunities.ts";
import { readSoql, soqlDate, soqlDateTime, unavailable, type ExtractContext, type Extractor, type Sourced, type SourceInfo } from "./types.ts";
import { licencesExtractor } from "./users.ts";

// Salesforce CPQ's reads (roadmap Stage 6, DECISIONS.md 216 to 219). CPQ is a
// managed package, so its objects and fields aren't in the Object Reference:
// every object is described first, and a field the org doesn't have makes only
// the checks that need it not measurable. Nothing is queried unless the
// package is installed, so a run with CPQ switched on in an org without it
// spends one describe and says so.

export const CPQ_NAMESPACE = "SBQQ";
/** Salesforce's Advanced Approvals package, whose approvals we don't read yet. */
export const ADVANCED_APPROVALS_NAMESPACE = "sbaa";
/** How far ahead a contract's end counts as coming up for renewal. Our judgement: a quarter and a month. */
export const RENEWAL_DAYS = 120;
const QUOTE_ROWS = 200_000;
const APPROVAL_ROWS = 50_000;

export type CpqPresence = { status: "installed"; advancedApprovals: boolean } | { status: "absent" | "unknown"; reason: string };

/**
 * Installed when the package has a licence record or its quote object can be
 * described. A free package has no licence record, so a missing one alone isn't
 * proof, and a describe that failed for another reason is "unknown", not absent.
 */
export async function cpqPresence(ctx: ExtractContext): Promise<CpqPresence> {
  const { packageLicenses } = ctx.get(licencesExtractor);
  const has = (ns: string): boolean => packageLicenses.status === "ok" && packageLicenses.data.some((p) => (p.NamespacePrefix ?? "").toLowerCase() === ns.toLowerCase());
  if (has(CPQ_NAMESPACE)) return { status: "installed", advancedApprovals: has(ADVANCED_APPROVALS_NAMESPACE) };
  const quote = await describeObject(ctx, "SBQQ__Quote__c");
  if (quote.status === "ok") return { status: "installed", advancedApprovals: has(ADVANCED_APPROVALS_NAMESPACE) };
  if (quote.status === "absent") return { status: "absent", reason: "Salesforce CPQ (the SBQQ package) isn't installed in this org, so there's nothing of it to check." };
  return { status: "unknown", reason: `We couldn't tell whether Salesforce CPQ is installed: ${quote.reason}` };
}

const notRead = <T>(reason: string, query: string): Sourced<T> => ({ status: "unavailable", reason, source: { kind: "soql", api: "rest", query } });

/** A planned query: described first, so a field the org lacks is named rather than failing the read. */
async function planned<T>(ctx: ExtractContext, spec: FieldSpec, where: string, options: { maxRecords?: number } = {}): Promise<Sourced<T[]>> {
  const plan = planFrom(await describeObject(ctx, spec.object), spec);
  const soql = (select: string) => `SELECT ${select} FROM ${spec.object}${where ? ` WHERE ${where}` : ""}`;
  if (!plan.ok) return notRead(plan.reason, soql(spec.required.join(", ")));
  return readSoql<T>(ctx, soql(plan.select), { missingFields: plan.missing, ...options });
}

export type PriceRuleRow = { Id: string; Name: string; SBQQ__Active__c: boolean; SBQQ__EvaluationEvent__c?: string | null; SBQQ__EvaluationOrder__c?: number | null; SBQQ__TargetObject__c?: string | null };
export type PriceActionRow = { Id: string; SBQQ__Rule__c: string; SBQQ__Field__c: string | null; SBQQ__TargetObject__c?: string | null };
export type ProductRuleRow = { Id: string; Name: string; SBQQ__Active__c: boolean; SBQQ__Type__c?: string | null };
/** A custom field's name and type, from a describe: no data. */
export type FieldType = { name: string; type: string };
export type TwinFields = { quoteLine: FieldType[]; opportunityProduct: FieldType[]; orderProduct: FieldType[] | null };

export type CpqSetup = {
  presence: CpqPresence;
  priceRules: Sourced<PriceRuleRow[]>;
  priceActions: Sourced<PriceActionRow[]>;
  productRules: Sourced<ProductRuleRow[]>;
  twins: Sourced<TwinFields>;
};

const PRICE_RULE: FieldSpec = { object: "SBQQ__PriceRule__c", required: ["Id", "Name", "SBQQ__Active__c"], optional: ["SBQQ__EvaluationEvent__c", "SBQQ__EvaluationOrder__c", "SBQQ__TargetObject__c"] };
const PRICE_ACTION: FieldSpec = { object: "SBQQ__PriceAction__c", required: ["Id", "SBQQ__Rule__c", "SBQQ__Field__c"], optional: ["SBQQ__TargetObject__c"] };
const PRODUCT_RULE: FieldSpec = { object: "SBQQ__ProductRule__c", required: ["Id", "Name", "SBQQ__Active__c"], optional: ["SBQQ__Type__c"] };

type Describe = { fields: Array<{ name: string; type: string; custom?: boolean }> };

/** The custom fields of an object, by name and type. Null when Salesforce says the object isn't there. */
async function customFields(ctx: ExtractContext, object: string): Promise<FieldType[] | null> {
  try {
    const d = await ctx.client.describe<Describe>(object);
    return d.fields.filter((f) => f.custom || f.name.endsWith("__c")).map((f) => ({ name: f.name, type: f.type })).sort((a, b) => a.name.localeCompare(b.name));
  } catch (err) {
    if (isUnavailable(err) && (err.code === "NOT_FOUND" || err.code === "INVALID_TYPE")) return null;
    throw err;
  }
}

export const cpqSetupExtractor: Extractor<CpqSetup> = {
  id: "cpq-setup",
  version: 1,
  async run(ctx) {
    const presence = await cpqPresence(ctx);
    const twinSource: SourceInfo = { kind: "rest", path: "sobjects/{SBQQ__QuoteLine__c, OpportunityLineItem, OrderItem}/describe" };
    if (presence.status !== "installed") {
      return { presence, priceRules: notRead(presence.reason, PRICE_RULE.object), priceActions: notRead(presence.reason, PRICE_ACTION.object), productRules: notRead(presence.reason, PRODUCT_RULE.object), twins: { status: "unavailable", reason: presence.reason, source: twinSource } };
    }
    let twins: Sourced<TwinFields>;
    try {
      const quoteLine = await customFields(ctx, "SBQQ__QuoteLine__c");
      const opportunityProduct = await customFields(ctx, "OpportunityLineItem");
      // An org without orders has no OrderItem to compare.
      const orderProduct = await customFields(ctx, "OrderItem");
      twins =
        quoteLine && opportunityProduct
          ? { status: "ok", data: { quoteLine, opportunityProduct, orderProduct }, source: twinSource, truncated: false }
          : { status: "unavailable", reason: `${quoteLine ? "OpportunityLineItem" : "SBQQ__QuoteLine__c"} couldn't be described.`, source: twinSource };
    } catch (err) {
      twins = unavailable(twinSource, err);
    }
    return {
      presence,
      priceRules: await planned<PriceRuleRow>(ctx, PRICE_RULE, ""),
      priceActions: await planned<PriceActionRow>(ctx, PRICE_ACTION, ""),
      productRules: await planned<ProductRuleRow>(ctx, PRODUCT_RULE, ""),
      twins,
    };
  },
};

export type QuoteRow = { Id: string; Name: string; OwnerId: string; SBQQ__Opportunity2__c: string | null; SBQQ__ListAmount__c: number | null; SBQQ__NetAmount__c: number | null; SBQQ__Ordered__c?: boolean | null; CreatedDate: string };
export type ApprovalRow = { Id: string; TargetObjectId: string; Status: string };
export type ContractRow = { Id: string; ContractNumber: string; OwnerId: string; EndDate: string; Status: string; SBQQ__RenewalOpportunity__c: string | null };

export type CpqQuotes = { quotes: Sourced<QuoteRow[]>; approvals: Sourced<ApprovalRow[]>; contracts: Sourced<ContractRow[]> };

const QUOTE: FieldSpec = { object: "SBQQ__Quote__c", required: ["Id", "Name", "OwnerId", "SBQQ__Primary__c", "SBQQ__Opportunity2__c", "SBQQ__ListAmount__c", "SBQQ__NetAmount__c", "CreatedDate"], optional: ["SBQQ__Ordered__c"] };
const CONTRACT: FieldSpec = { object: "Contract", required: ["Id", "ContractNumber", "OwnerId", "EndDate", "Status", "SBQQ__RenewalOpportunity__c"] };

// Primary quotes made in the window, the approvals started in it, and the
// contracts ending soon. Records, so a set-up review never reads them.
export const cpqQuotesExtractor: Extractor<CpqQuotes> = {
  id: "cpq-quotes",
  version: 1,
  records: true,
  partialKeys: ["cpq-quotes"],
  async run(ctx) {
    const { presence } = ctx.get(cpqSetupExtractor);
    const from = soqlDateTime(ctx.windowStart);
    const quoteWhere = `SBQQ__Primary__c = true AND CreatedDate >= ${from}`;
    if (presence.status !== "installed") {
      return { quotes: notRead(presence.reason, QUOTE.object), approvals: notRead(presence.reason, "ProcessInstance"), contracts: notRead(presence.reason, CONTRACT.object) };
    }
    const plan = planFrom(await describeObject(ctx, QUOTE.object), QUOTE);
    const quoteSource: SourceInfo = { kind: "soql", api: "rest", query: `SELECT ${plan.ok ? plan.select : QUOTE.required.join(", ")} FROM ${QUOTE.object} WHERE ${quoteWhere}` };
    let quotes: Sourced<QuoteRow[]>;
    if (!plan.ok) quotes = { status: "unavailable", reason: plan.reason, source: quoteSource };
    else {
      try {
        const rows = await readKeyset<QuoteRow>(ctx, "cpq-quotes", plan.select, QUOTE.object, quoteWhere, QUOTE_ROWS);
        quotes = { status: "ok", data: rows, source: quoteSource, truncated: rows.length >= QUOTE_ROWS, ...(plan.missing.length ? { missingFields: plan.missing } : {}) };
      } catch (err) {
        quotes = unavailable(quoteSource, err);
      }
    }
    // Every approval started in the window. Matched to quotes by id, which avoids a filter on the polymorphic TargetObject.
    const approvals = await readSoql<ApprovalRow>(ctx, `SELECT Id, TargetObjectId, Status FROM ProcessInstance WHERE CreatedDate >= ${from}`, { maxRecords: APPROVAL_ROWS });
    const today = soqlDate(ctx.now);
    const until = soqlDate(new Date(ctx.now.getTime() + RENEWAL_DAYS * 86_400_000));
    const contracts = await planned<ContractRow>(ctx, CONTRACT, `Status = 'Activated' AND EndDate >= ${today} AND EndDate <= ${until}`);
    return { quotes, approvals, contracts };
  },
};
