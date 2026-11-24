import { APEX_POINTER } from "../salesforce/refusal-words.ts";
import type { Sourced } from "../extract/types.ts";

/**
 * Whether the org's own Apex was searched for a reference (DECISIONS.md 710):
 * read, or asked and answered by a Professional Edition org that it has none
 * of its own, which Salesforce documents. Anything else unread is unknown, and
 * blocks a removal (rule 3).
 */
export const apexSearched = (part: Sourced<unknown>): boolean => part.status === "ok" || part.reason === APEX_POINTER;
