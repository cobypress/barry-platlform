import type { Rule } from "../engine/rule.ts";
import type { Extractor } from "../extract/types.ts";
import type { Metric } from "../schemas/index.ts";
import type { RuleContext } from "../engine/rule.ts";

// A domain is a plug-in: extractors to gather data, metric builders to calculate,
// and rules to conclude. Service Cloud adds modules to src/domains/index.ts and
// changes nothing in src/engine.
export type DomainModule = {
  id: string;
  cloud: "sales" | "service" | "cpq";
  label: string;
  /** In dependency order. Shared extractors are listed by every module that reads them; the run dedupes. */
  extractors: Extractor<any>[];
  metrics?: Array<(ctx: Omit<RuleContext, "metrics">) => Metric[]>;
  rules: Rule[];
};
