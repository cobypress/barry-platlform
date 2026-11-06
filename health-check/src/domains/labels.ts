import { schemaExtractor } from "../extract/configuration.ts";
import { labelsExtractor } from "../extract/labels.ts";
import type { RuleContext } from "../engine/rule.ts";

// Customer text names objects and fields by their labels, as the org shows them
// (DECISIONS.md 411). Where a label couldn't be read, the API name is the only
// name we have, so it's used. Evidence and facts keep API names, for the working.

export type Labels = {
  object: (name: string) => string;
  /** A field's label, from the describe the run already read for its object. */
  field: (object: string, name: string) => string;
};

export function labelsOf(ctx: Pick<RuleContext, "data">): Labels {
  const objects = ctx.data(labelsExtractor);
  const schema = ctx.data(schemaExtractor);
  return {
    object: (name) => (objects?.status === "ok" ? objects.data[name] : undefined) ?? name,
    field: (object, name) => {
      const described = schema?.[object];
      return (described?.status === "ok" ? described.data.fields.find((f) => f.name === name)?.label : undefined) ?? name;
    },
  };
}
