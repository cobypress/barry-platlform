import type { DomainModule } from "../types.ts";
import { serviceOperationsModule } from "./operations.ts";
import { serviceSetupModule } from "./setup.ts";

export { SERVICE_OPERATIONS_METRIC_IDS } from "./operations.ts";

/** Service Cloud (roadmap Stage 6): how cases are handled, and how the org is set up to handle them. */
export const SERVICE_CLOUD_MODULES: DomainModule[] = [serviceOperationsModule, serviceSetupModule];
