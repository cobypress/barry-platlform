import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import { redactSecrets } from "../privacy/secrets.ts";

// Rule 7: nothing that leaves for Claude may hold a real name. Findings already
// hold tokens, so this is the backstop, and every prompt passes through it.
//
// scrub() only catches full names, usernames, emails and ids. A first name on its
// own ("Sam's pipeline") gets through it, and metadata labels can hold exactly
// that, so name parts are masked here as well.

// Parts of shared-user names that are ordinary words. Masking "Sales" would turn
// "Sales Cloud" into a token.
const ORDINARY = new Set(["sales", "shared", "desk", "integration", "user", "users", "admin", "system", "service", "support", "marketing", "automated", "process", "platform", "unknown", "team", "ops", "account", "lead", "data", "test", "api", "default", "workflow", "site", "guest", "the", "and", "chatter", "expert"]);

const escapeRegex = (value: string): string => value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

function maskNameParts(text: string, pseudonyms: Pseudonymiser): string {
  let out = text;
  for (const entry of pseudonyms.entries()) {
    for (const part of entry.name.split(/\s+/)) {
      if (part.length < 2 || !/^\p{Lu}/u.test(part) || ORDINARY.has(part.toLowerCase())) continue;
      out = out.replace(new RegExp(`(?<![\\p{L}\\p{N}_-])${escapeRegex(part)}(?![\\p{L}\\p{N}_]|-\\p{L})`, "gu"), entry.token);
    }
  }
  return out;
}

/** Everything that goes to Claude goes through here. */
export function guardPrompt(text: string, pseudonyms: Pseudonymiser): string {
  return redactSecrets(maskNameParts(pseudonyms.scrub(text), pseudonyms));
}

/**
 * Field by field, before anything is JSON encoded. Escaped quotes in encoded text
 * would slip past the secret patterns, which is why the whole prompt isn't enough.
 */
export function guardDeep<T>(value: T, pseudonyms: Pseudonymiser): T {
  if (typeof value === "string") return guardPrompt(value, pseudonyms) as T;
  if (Array.isArray(value)) return value.map((v) => guardDeep(v, pseudonyms)) as T;
  if (value && typeof value === "object") return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, guardDeep(v, pseudonyms)])) as T;
  return value;
}
