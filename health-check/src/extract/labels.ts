import { readSoql, unavailable, type Extractor, type Sourced, type SourceInfo } from "./types.ts";

// What the org calls each object and each installed app, so customer text says
// "Invoice", not "Invoice__c", and "Acme CPQ", not "acmecpq" (DECISIONS.md 411,
// 413). API names and namespaces stay in the raw notes' working.

export type Labels = {
  /** Every object the connection can see, by API name, from one call to the global describe. */
  objects: Sourced<Record<string, string>>;
  /** Installed packages' names by namespace. A licence record holds only the namespace. */
  packages: Sourced<Record<string, string>>;
};

export const labelsExtractor: Extractor<Labels> = {
  id: "labels",
  version: 2,
  async run(ctx) {
    const source: SourceInfo = { kind: "rest", path: "sobjects" };
    let objects: Labels["objects"];
    try {
      const global = await ctx.client.describeGlobal<{ sobjects: Array<{ name: string; label?: string }> }>();
      objects = { status: "ok", source, truncated: false, data: Object.fromEntries(global.sobjects.filter((o) => o.label).map((o) => [o.name, o.label!])) };
    } catch (err) {
      objects = unavailable(source, err);
    }
    const installed = await readSoql<{ SubscriberPackage: { Name: string | null; NamespacePrefix: string | null } | null }>(ctx, "SELECT SubscriberPackage.Name, SubscriberPackage.NamespacePrefix FROM InstalledSubscriberPackage", { api: "tooling" });
    const packages: Labels["packages"] =
      installed.status === "ok"
        ? { ...installed, data: Object.fromEntries(installed.data.flatMap((p) => (p.SubscriberPackage?.NamespacePrefix && p.SubscriberPackage.Name ? [[p.SubscriberPackage.NamespacePrefix, p.SubscriberPackage.Name]] : []))) }
        : installed;
    return { objects, packages };
  },
};
