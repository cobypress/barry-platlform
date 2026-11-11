import { cpSync, readFileSync, writeFileSync } from "node:fs";
import { basename, join } from "node:path";
import { HANDBOOK_FILE } from "../handbook/build.ts";
import { Handbook, type HandbookFlowItem } from "../handbook/schema.ts";
import { Run } from "../schemas/index.ts";

// A re-check of the synthetic org, for the tests and the sample (DECISIONS.md
// 483): its run folder, and its earlier health check's beside it, both made up.
// Nothing here is from a real org.

/** When the earlier health check read the synthetic org. */
export const EARLIER_AS_OF = "2026-07-14T12:00:00.000Z";

/**
 * The earlier health check's folder beside `dir`, a copy of it with its
 * handbook edited, and `dir`'s run.json naming it as its baseline. Returns the
 * earlier folder. The edit gets the handbook as this run's file holds it.
 */
export function withEarlier(dir: string, edit: (h: Handbook) => unknown = plantEarlier, over: Partial<Pick<Run, "id" | "orgId" | "asOf">> = {}): string {
  const run = Run.parse(JSON.parse(readFileSync(join(dir, "run.json"), "utf8")));
  const id = over.id ?? `${basename(dir)}-earlier`;
  const earlier = join(dir, "..", id);
  cpSync(dir, earlier, { recursive: true });
  const handbook = Handbook.parse(JSON.parse(readFileSync(join(earlier, HANDBOOK_FILE), "utf8")));
  edit(handbook);
  writeFileSync(join(earlier, HANDBOOK_FILE), JSON.stringify(handbook));
  writeFileSync(join(earlier, "run.json"), JSON.stringify({ ...run, id, asOf: over.asOf ?? EARLIER_AS_OF, orgId: over.orgId ?? run.orgId, baselineRunId: null }));
  writeFileSync(join(dir, "run.json"), JSON.stringify({ ...run, baselineRunId: id }));
  return earlier;
}

/**
 * The earlier health check as one read before which reading made its file was
 * kept (decision 484), as every handbook made before then is: a re-check set
 * against it compares only what was added or removed and when each was last
 * changed. Without this, the earlier file holds this run's readings, so the
 * part names which details changed.
 */
export function olderReading(h: Handbook): void {
  h.readBy = { metadata: null, apex: null };
}

/** A flow the synthetic org had at its earlier health check and has since removed. */
export const REMOVED_FLOW = "Lead_Nurture_Old";

/**
 * The synthetic org as it stood at its earlier health check, so that since then
 * one flow was added (Draft_Idea), one removed and one changed in its status
 * and a step; one validation rule added (Needs_Competitor), one removed, one
 * changed in its formula and one only in its message; one Apex class removed
 * and one changed only in when it was last changed; and one trigger changed in
 * its events.
 */
export function plantEarlier(h: Handbook): void {
  if (h.flows.status !== "ok" || h.validationRules.status !== "ok" || h.apexClasses.status !== "ok" || h.apexTriggers.status !== "ok") throw new Error("The synthetic handbook should have every part");
  const flows = h.flows.items;
  const route = flows.find((f) => f.name === "Opportunity_Route_Deal")!;
  route.status = "Draft";
  route.steps[0]!.fields = ["OwnerId"];
  const removed: HandbookFlowItem = { ...structuredClone(route), name: REMOVED_FLOW, label: "Lead Nurture (old)", status: "Obsolete" };
  h.flows.items = [...flows.filter((f) => f.name !== "Draft_Idea"), removed];
  const rules = h.validationRules.items;
  const due = rules.find((r) => r.name === "Due_Date_Required")!;
  due.formula = "OR(ISBLANK(Due_Date__c), ISBLANK(Amount__c))";
  due.formulaLength = due.formula.length;
  rules.find((r) => r.name === "Old_Check")!.message = "Check the invoice.";
  const competitor = rules.find((r) => r.name === "Needs_Competitor")!;
  h.validationRules.items = [...rules.filter((r) => r !== competitor), { ...competitor, name: "Old_Discount_Check", formula: "Discount__c > 0.5", formulaLength: 17 }];
  const classes = h.apexClasses.items;
  classes.find((c) => c.name === "FinanceSync")!.lastChanged = { at: "2026-05-02T09:30:00.000+0000", by: "[[user:U0001]]" };
  h.apexClasses.items = [...classes, { ...structuredClone(classes[0]!), name: "QuoteSync", lastChanged: null }].sort((a, b) => (a.name < b.name ? -1 : 1));
  h.apexClasses.total = h.apexClasses.items.length;
  h.apexTriggers.items.find((t) => t.name === "InvoiceTrigger")!.events = ["before insert"];
}
