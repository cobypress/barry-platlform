import type { CustomObjectRow } from "../extract/configuration.ts";
import type { Sourced } from "../extract/types.ts";

// Layout and ApexTrigger can name a custom object by its id rather than its API
// name, so anything that groups by object resolves the id first. Shared by the
// object inventory and the automation rules, so they always agree.

// A Custom Object Definition id, in its 15 or 18 character form.
const OBJECT_ID = /^01I[A-Za-z0-9]{12}(?:[A-Za-z0-9]{3})?$/;

export const isObjectId = (value: string): boolean => OBJECT_ID.test(value);

/**
 * Ids to API names. Tooling's DeveloperName drops the suffix, so the name is
 * rebuilt as a custom object's. Two ids that would make the same name (a custom
 * object and a custom metadata type of the same name, say) are both left unnamed
 * rather than guessed.
 */
function objectNames(rows: CustomObjectRow[]): Map<string, string> {
  const named = rows.map((r) => [r.Id.slice(0, 15), `${r.NamespacePrefix ? `${r.NamespacePrefix}__` : ""}${r.DeveloperName}__c`] as const);
  const seen = new Map<string, number>();
  for (const [, name] of named) seen.set(name, (seen.get(name) ?? 0) + 1);
  return new Map(named.filter(([, name]) => seen.get(name) === 1));
}

/** The object a layout or trigger belongs to, or null when it's an id we can't name. */
export function objectResolver(ids: Sourced<CustomObjectRow[]>): (value: string) => string | null {
  const names = ids.status === "ok" ? objectNames(ids.data) : new Map<string, string>();
  return (value) => (isObjectId(value) ? (names.get(value.slice(0, 15)) ?? null) : value);
}
