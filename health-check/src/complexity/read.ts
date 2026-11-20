import { apexExtractor, metadataExtractor, type ApexData } from "../extract/automation.ts";
import { customObjectsExtractor, type CustomObjectRow } from "../extract/configuration.ts";
import { orgDocumentationExtractor, type OrgDocumentation } from "../extract/documentation.ts";
import { labelsExtractor, type Labels } from "../extract/labels.ts";
import { unreadReason, type MetadataBundle } from "../extract/metadata-parse.ts";
import { accessExtractor, type AccessData } from "../extract/security.ts";
import type { Extractor, Sourced } from "../extract/types.ts";
import { objectResolver } from "../domains/object-names.ts";
import type { Complexity } from "../schemas/complexity.ts";
import { scoreComplexity, type ComponentInput } from "./formula.ts";

// The counts behind the complexity score (DECISIONS.md 614), from extracts
// every set-up only scan already makes, so it reads no records and a complete
// run of the same org gives the same counts. A component counts only when
// every list in it was read whole: a part read would show the org as simpler
// than it is (rule 5).

export const COMPLEXITY_FILE = "complexity.json";

class NotRead extends Error {}

function step<T>(extracts: Map<string, unknown>, extractor: Extractor<T>, what: string): T {
  if (!extracts.has(extractor.id)) throw new NotRead(`This run didn't read ${what}.`);
  return extracts.get(extractor.id) as T;
}

function whole<T>(s: Sourced<T> | undefined, what: string): T {
  if (!s) throw new NotRead(`This run didn't read ${what}.`);
  if (s.status !== "ok") throw new NotRead(`${what.charAt(0).toUpperCase()}${what.slice(1)} couldn't be read: ${s.reason}`);
  if (s.truncated) throw new NotRead(`Salesforce gave only part of the ${what}.`);
  return s.data;
}

const component = (read: () => ComponentInput): ComponentInput => {
  try {
    return read();
  } catch (err) {
    if (err instanceof NotRead) return { reason: err.message };
    throw err;
  }
};

const recordTriggered = (triggerType: string | null): boolean => !!triggerType && (triggerType.startsWith("RecordBefore") || triggerType.startsWith("RecordAfter"));

export function complexityInputs(extracts: Map<string, unknown>): Record<string, ComponentInput> {
  const bundle = (needs: Array<"workflowRules" | "validationRules"> = []): MetadataBundle => {
    const b = whole(step<Sourced<MetadataBundle>>(extracts, metadataExtractor, "the org's metadata"), "the org's metadata");
    const unread = unreadReason(b, needs);
    if (unread) throw new NotRead(unread);
    return b;
  };
  const apex = (): ApexData => step(extracts, apexExtractor, "the Apex code");
  const doc = (): OrgDocumentation => step(extracts, orgDocumentationExtractor, "the org's connected apps, roles and permission sets");
  const objects = (): Sourced<CustomObjectRow[]> => step(extracts, customObjectsExtractor, "the custom objects");

  return {
    dataModel: component(() => {
      const own = whole(objects(), "custom objects").filter((o) => !o.NamespacePrefix);
      const b = bundle();
      return { counts: { customObjects: own.length, customFields: b.customFields.length, recordTypes: b.recordTypes.filter((r) => r.active).length } };
    }),
    declarativeAutomation: component(() => {
      const b = bundle(["validationRules"]);
      const flows = b.flows.filter((f) => f.status === "Active" && f.processType !== "Workflow");
      const triggered = flows.filter((f) => recordTriggered(f.triggerType)).length;
      return { counts: { recordTriggeredFlows: triggered, otherFlows: flows.length - triggered, validationRules: b.validationRules.filter((v) => v.active).length } };
    }),
    legacyAutomation: component(() => {
      const b = bundle(["workflowRules"]);
      return { counts: { workflowRules: b.workflows.reduce((n, w) => n + w.rules.filter((r) => r.active).length, 0), processBuilders: b.flows.filter((f) => f.status === "Active" && f.processType === "Workflow").length } };
    }),
    code: component(() => {
      const { classes, triggers } = apex();
      const own = whole(classes, "Apex classes").filter((c) => c.Status === "Active" && !c.scan?.testClass);
      const active = whole(triggers, "Apex triggers").filter((t) => t.Status === "Active");
      const counted = [...own, ...active];
      // As the code-size check: a size we didn't read isn't taken as nothing.
      if (counted.some((c) => typeof c.LengthWithoutComments !== "number")) throw new NotRead("The size of some code wasn't read.");
      return { counts: { apexClasses: own.length, apexTriggers: active.length, apexCharacters: counted.reduce((n, c) => n + c.LengthWithoutComments!, 0) } };
    }),
    automationOverlap: component(() => {
      // The kinds as the mixed-automation check finds them, counted from two kinds up.
      const b = bundle(["workflowRules"]);
      const triggers = whole(apex().triggers, "Apex triggers");
      const objectOf = objectResolver(objects());
      const kinds = new Map<string, Set<string>>();
      const add = (object: string | null, kind: string): void => {
        if (object) kinds.set(object, (kinds.get(object) ?? new Set()).add(kind));
      };
      let unnamed = 0;
      for (const t of triggers.filter((x) => x.Status === "Active")) {
        const object = objectOf(t.TableEnumOrId);
        if (!object) unnamed++;
        add(object, "trigger");
      }
      for (const f of b.flows.filter((x) => x.status === "Active")) {
        if (f.processType === "Workflow") add(f.object, "process");
        else if (recordTriggered(f.triggerType)) add(f.object, "flow");
      }
      for (const w of b.workflows) if (w.rules.some((r) => r.active)) add(w.object, "workflow");
      const notes = unnamed ? [`${unnamed} active Apex ${unnamed === 1 ? "trigger is" : "triggers are"} on a custom object we couldn't name, so not counted against any object`] : [];
      return { counts: { objectsWithSeveralKinds: [...kinds.values()].filter((k) => k.size >= 2).length }, notes };
    }),
    integrations: component(() => {
      const d = doc();
      const labels: Labels = step(extracts, labelsExtractor, "the installed packages");
      const apps = whole(d.connectedApps, "connected apps");
      const credentials = whole(d.namedCredentials, "named credentials").filter((c) => !c.NamespacePrefix);
      const sites = whole(d.remoteSites, "remote site settings").filter((r) => !r.NamespacePrefix && r.IsActive);
      const packages = whole(labels.packages, "installed packages");
      return { counts: { connectedApps: apps.length, endpoints: credentials.length + sites.length, packages: Object.keys(packages).length } };
    }),
    accessModel: component(() => {
      const access: AccessData = step(extracts, accessExtractor, "the profiles");
      const d = doc();
      const profiles = whole(access.profiles, "profiles").filter((p) => p.IsCustom);
      const sets = whole(d.permissionSets, "permission sets").filter((s) => s.IsCustom && !s.NamespacePrefix);
      return { counts: { customProfiles: profiles.length, customPermissionSets: sets.length, roles: whole(d.roles, "roles").length } };
    }),
  };
}

/** The org's complexity, dated as the run reads the org, so every delivery writes the same file. */
export const buildComplexity = (extracts: Map<string, unknown>, madeAt: Date): Complexity => scoreComplexity(complexityInputs(extracts), madeAt);
