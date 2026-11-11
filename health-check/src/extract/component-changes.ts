import type { LastChange } from "./automation.ts";
import { readSoql, type ExtractContext, type Extractor } from "./types.ts";

// When each flow and validation rule was last changed, and by whom, for the
// customer's own documents only (DECISIONS.md 482). The retrieve's files carry
// neither, and listMetadata is refused (decision 4), so two Tooling list
// queries read them on every run, on either route. A step of its own, so a
// refusal here can never cost the automation findings, and a run already in
// flight reads nothing again but this. No rule reads it.

// No filter on the namespace: there's less in the query for an org to refuse,
// and the managed rows are dropped below.
export const FLOW_CHANGES_QUERY =
  "SELECT Id, DefinitionId, Definition.DeveloperName, Definition.NamespacePrefix, VersionNumber, Status, LastModifiedDate, LastModifiedById, LastModifiedBy.Name FROM Flow";
export const RULE_CHANGES_QUERY = "SELECT Id, ValidationName, EntityDefinition.QualifiedApiName, NamespacePrefix, LastModifiedDate, LastModifiedById, LastModifiedBy.Name FROM ValidationRule";

/** A flow's last change, under its developer name. */
export type FlowChange = LastChange & { name: string };
/** A validation rule's, under its object's API name and its own. */
export type RuleChange = LastChange & { object: string; name: string };

/**
 * Each kind's last changes, or why they weren't read: Salesforce refused the
 * query. The reason is Salesforce's, and stays in the run folder.
 */
export type ComponentChanges<T> = { status: "read"; items: T[] } | { status: "refused"; reason: string };

export type ComponentChangesData = { flows: ComponentChanges<FlowChange>; validationRules: ComponentChanges<RuleChange> };

type ChangeFields = { LastModifiedDate?: string | null; LastModifiedById?: string | null; LastModifiedBy?: { Name?: string | null } | null };
export type FlowVersionRow = ChangeFields & { Id: string; Definition?: { DeveloperName?: string | null; NamespacePrefix?: string | null } | null; VersionNumber?: number | string | null; Status?: string | null };
export type RuleChangeRow = ChangeFields & { Id: string; ValidationName?: string | null; EntityDefinition?: { QualifiedApiName?: string | null } | null; NamespacePrefix?: string | null };

const lastChange = (row: ChangeFields): LastChange => ({ at: row.LastModifiedDate ?? null, byId: row.LastModifiedById ?? null, byName: row.LastModifiedBy?.Name ?? null });

const byText = (a: string, b: string): number => (a < b ? -1 : a > b ? 1 : 0);

const isActive = (row: FlowVersionRow): boolean => row.Status === "Active";
const versionOf = (row: FlowVersionRow): number => Number(row.VersionNumber) || 0;

/**
 * One line per flow of the org's own: its active version's, or its
 * highest-numbered version's where none is active, as the retrieve and the
 * Tooling route read the flow itself. Sorted by name, so the checkpoint is the
 * same whatever order Salesforce gives the versions in.
 */
export function flowChanges(rows: FlowVersionRow[]): FlowChange[] {
  const chosen = new Map<string, FlowVersionRow>();
  for (const row of rows) {
    const name = row.Definition?.DeveloperName;
    if (!name || row.Definition?.NamespacePrefix) continue;
    const held = chosen.get(name);
    const wins = !held || (isActive(row) !== isActive(held) ? isActive(row) : versionOf(row) > versionOf(held));
    if (wins) chosen.set(name, row);
  }
  return [...chosen].sort(([a], [b]) => byText(a, b)).map(([name, row]) => ({ name, ...lastChange(row) }));
}

/** One line per validation rule of the org's own, by object and then name. */
export function ruleChanges(rows: RuleChangeRow[]): RuleChange[] {
  return rows
    .flatMap((row) => {
      const object = row.EntityDefinition?.QualifiedApiName;
      const name = row.ValidationName;
      return object && name && !row.NamespacePrefix ? [{ object, name, ...lastChange(row) }] : [];
    })
    .sort((a, b) => byText(a.object, b.object) || byText(a.name, b.name));
}

// Which queries Salesforce refused, kept as soon as it does, so a run stopped
// at the budget on the other one doesn't make the refused query again when it
// resumes. It holds the kind and Salesforce's reason, nothing read from the org.
const REFUSED_PARTIAL = "component-changes-refused";
type Kind = keyof ComponentChangesData;
type Refused = { kind: Kind; reason: string };

/** One kind's query, or the refusal a resumed run already met. A budget stop or a network error stops the step. */
async function readKind<R, T>(ctx: ExtractContext, kind: Kind, soql: string, lines: (rows: R[]) => T[]): Promise<ComponentChanges<T>> {
  const known = ctx.partial.read<Refused>(REFUSED_PARTIAL).find((r) => r.kind === kind);
  if (known) return { status: "refused", reason: known.reason };
  const read = await readSoql<R>(ctx, soql, { api: "tooling" });
  if (read.status === "ok") return { status: "read", items: lines(read.data) };
  ctx.partial.append<Refused>(REFUSED_PARTIAL, [{ kind, reason: read.reason }]);
  return { status: "refused", reason: read.reason };
}

export const componentChangesExtractor: Extractor<ComponentChangesData> = {
  id: "component-changes",
  version: 1,
  partialKeys: [REFUSED_PARTIAL],
  // Through Tooling, one query each: composite batch refuses Tooling, and the
  // client pages each as it does any other list.
  async run(ctx) {
    // Each on its own, so one refused leaves the other read.
    const flows = await readKind(ctx, "flows", FLOW_CHANGES_QUERY, flowChanges);
    const validationRules = await readKind(ctx, "validationRules", RULE_CHANGES_QUERY, ruleChanges);
    return { flows, validationRules };
  },
};
