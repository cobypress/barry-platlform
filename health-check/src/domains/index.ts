import { activityModule } from "./activity/index.ts";
import { adoptionModule } from "./adoption/index.ts";
import { automationModule } from "./automation/index.ts";
import { bestPracticeModule } from "./best-practice/index.ts";
import { configurationModule } from "./configuration/index.ts";
import { currentPipelineModule } from "./current-pipeline/index.ts";
import { dataQualityModule } from "./data-quality/index.ts";
import { forecastModule } from "./forecast/index.ts";
import { historicPipelineModule } from "./historic-pipeline/index.ts";
import { integrationsModule } from "./integrations/index.ts";
import { leadFunnelModule } from "./lead-funnel/index.ts";
import { revenueOpportunitiesModule } from "./revenue-opportunities/index.ts";
import { securityModule } from "./security/index.ts";
import { SERVICE_CLOUD_MODULES } from "./service/index.ts";
import { technicalHealthModule } from "./technical-health/index.ts";
import type { DomainModule } from "./types.ts";
import { utilisationModule } from "./utilisation/index.ts";

export const SALES_CLOUD_MODULES: DomainModule[] = [configurationModule, automationModule, technicalHealthModule, utilisationModule, adoptionModule, dataQualityModule, historicPipelineModule, securityModule, integrationsModule, bestPracticeModule, currentPipelineModule, forecastModule, activityModule, leadFunnelModule, revenueOpportunitiesModule];

export { SERVICE_CLOUD_MODULES };

/** Every module the engine knows, Sales Cloud first. */
export const ALL_MODULES: DomainModule[] = [...SALES_CLOUD_MODULES, ...SERVICE_CLOUD_MODULES];

/** The console's module for both Service Cloud areas (DECISIONS.md 156). */
export const SERVICE_MODULE = "service";

/** The module ids a run can name: each area's own, and "service" for both Service Cloud areas. */
export const knownModuleIds = (modules: DomainModule[] = ALL_MODULES): Set<string> => new Set([...modules.map((m) => m.id), ...(modules.some((m) => m.cloud === "service") ? [SERVICE_MODULE] : [])]);

/**
 * Areas made from checks that used to sit in another (DECISIONS.md 171). A run
 * that names the older area gets the newer one too, so a re-check of an older
 * baseline still runs every check it had, and the console's lists needn't change.
 */
export const AREAS_WITHIN: Record<string, string> = { technical_health: "automation", integrations: "security" };

/** The modules a run's list selects, in the engine's order. "service" brings in both Service Cloud areas. */
export function modulesForRun(ids: string[], modules: DomainModule[] = ALL_MODULES): DomainModule[] {
  const named = (id: string): boolean => ids.includes(id) || (AREAS_WITHIN[id] !== undefined && ids.includes(AREAS_WITHIN[id]));
  return modules.filter((m) => named(m.id) || (m.cloud === "service" && ids.includes(SERVICE_MODULE)));
}
