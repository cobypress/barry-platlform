import { unavailable, type Extractor, type Sourced, type SourceInfo } from "./types.ts";

// What the org calls each object, so customer text says "Invoice", not
// "Invoice__c" (night 3 reading pass, item 13). One call lists every object the
// connection can see with its label. API names stay in the raw notes' working.

export type ObjectLabels = Record<string, string>;

export const labelsExtractor: Extractor<Sourced<ObjectLabels>> = {
  id: "labels",
  version: 1,
  async run(ctx) {
    const source: SourceInfo = { kind: "rest", path: "sobjects" };
    try {
      const global = await ctx.client.describeGlobal<{ sobjects: Array<{ name: string; label?: string }> }>();
      return { status: "ok", source, truncated: false, data: Object.fromEntries(global.sobjects.filter((o) => o.label).map((o) => [o.name, o.label!])) };
    } catch (err) {
      return unavailable(source, err);
    }
  },
};
