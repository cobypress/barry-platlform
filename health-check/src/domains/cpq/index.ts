import type { DomainModule } from "../types.ts";
import { cpqQuotingModule } from "./quoting.ts";
import { cpqSetupModule } from "./setup.ts";

export { CPQ_MODULE } from "./shared.ts";

/** Salesforce CPQ (roadmap Stage 6): how quotes are priced and handed on, and how CPQ is set up. Set-up first, since quoting reads what it found. */
export const CPQ_MODULES: DomainModule[] = [cpqSetupModule, cpqQuotingModule];
