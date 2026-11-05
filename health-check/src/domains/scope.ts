import type { RunScope } from "../schemas/index.ts";
import type { DomainModule } from "./types.ts";

// Essentials, the set-up review (roadmap 2.2), reads the org's set-up and never
// its CRM records. Its areas are the ones a set-up can answer for. Within them,
// an extract that reads records doesn't run, and a rule that needs one is left
// out: a check we chose not to make isn't a finding, not even "not measurable".

export const ESSENTIALS_DOMAINS = ["configuration", "automation", "security", "best_practice", "utilisation"];

/** The modules a run of this scope runs, with record extracts and the rules that need them taken out. */
export function modulesFor(scope: RunScope, modules: DomainModule[]): DomainModule[] {
  if (scope === "complete") return modules;
  return modules
    .filter((m) => ESSENTIALS_DOMAINS.includes(m.id))
    .map((m) => ({ ...m, extractors: m.extractors.filter((e) => !e.records), rules: m.rules.filter((r) => !r.records) }));
}
