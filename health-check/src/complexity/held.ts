import type { Complexity } from "../schemas/complexity.ts";
import type { OrgCount, OrgCountId } from "../scan/result.ts";

// The complexity score's counts the free scan shows under "Found in your org"
// (DECISIONS.md 623). The score has already checked each component was read
// whole (614), so a component not read leaves its counts out rather than show
// part of the org as all of it (rule 5). Read here, beside the score, so nothing
// that scores, finds or prints reads the complexity score itself.

const SHOWN: Array<{ id: OrgCountId; label: string; component: string; counts: string[] }> = [
  { id: "customObjects", label: "Custom objects of your own", component: "dataModel", counts: ["customObjects"] },
  { id: "customFields", label: "Custom fields of your own on custom objects, accounts, contacts, leads and opportunities", component: "dataModel", counts: ["customFields"] },
  { id: "flows", label: "Active flows of your own", component: "declarativeAutomation", counts: ["recordTriggeredFlows", "otherFlows"] },
  { id: "apexClasses", label: "Apex classes, not counting tests", component: "code", counts: ["apexClasses"] },
  { id: "packages", label: "Installed packages with a namespace", component: "integrations", counts: ["packages"] },
];

export function heldCounts(complexity: Complexity | null): OrgCount[] {
  const out: OrgCount[] = [];
  for (const c of SHOWN) {
    const component = complexity?.components.find((x) => x.id === c.component);
    if (component?.status !== "measured") continue;
    const counts = c.counts.map((id) => component.counts.find((n) => n.id === id)?.count);
    if (counts.some((n) => n === undefined)) continue;
    out.push({ id: c.id, label: c.label, count: counts.reduce((a: number, n) => a + n!, 0) });
  }
  return out;
}
