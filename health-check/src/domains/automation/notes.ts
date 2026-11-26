import { eng, said } from "../../engine/org-words.ts";
/** The day Salesforce ended support for Workflow Rules and Process Builder, written once: the sentence says it, and the strict check names it (DECISIONS.md 772). */
export const SUPPORT_ENDED_ON = "2025-12-31";
const MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
/** "31 December 2025", from an ISO day. */
const onDay = (iso: string): string => `${Number(iso.slice(8, 10))} ${MONTHS[Number(iso.slice(5, 7)) - 1]} ${Number(iso.slice(0, 4))}`;
export const END_OF_SUPPORT = said`Salesforce ended support for Workflow Rules and Process Builder on ${eng(onDay(SUPPORT_ENDED_ON))}. They keep running, but any bug is yours to work around.`;

export const MIGRATE_TO_FLOW_NOTE = "Salesforce's Migrate to Flow tool handles the simple cases, but it isn't always a like-for-like move.";
