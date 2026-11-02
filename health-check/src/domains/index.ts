import { automationModule } from "./automation/index.ts";
import { bestPracticeModule } from "./best-practice/index.ts";
import { configurationModule } from "./configuration/index.ts";
import { currentPipelineModule } from "./current-pipeline/index.ts";
import { dataQualityModule } from "./data-quality/index.ts";
import { forecastModule } from "./forecast/index.ts";
import { historicPipelineModule } from "./historic-pipeline/index.ts";
import { securityModule } from "./security/index.ts";
import type { DomainModule } from "./types.ts";
import { utilisationModule } from "./utilisation/index.ts";

export const SALES_CLOUD_MODULES: DomainModule[] = [configurationModule, automationModule, utilisationModule, dataQualityModule, historicPipelineModule, securityModule, bestPracticeModule, currentPipelineModule, forecastModule];
