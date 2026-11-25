// Writes fixtures/rule-where.json: every rule's version and, where it has one,
// where its subject is in Setup, from the rules' own definitions (DECISIONS.md
// 626). The customer portal shows the labels, and a link where a path has been
// checked by hand, from a copy of this file in the console
// (docs/health-check/engine-rules/where.json), so it never imports a rule.
// Made here, never by hand: a test fails when the file and the rules differ.
//
//   npm run rule-where        (from health-check/)

import { writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { ALL_MODULES } from "../src/domains/index.ts";
import type { Rule } from "../src/engine/rule.ts";
import { loosenedFrom } from "../src/engine/loosened.ts";

export const RULE_WHERE_FILE = fileURLToPath(new URL("../fixtures/rule-where.json", import.meta.url));
/** How the file is made again, from the health-check folder. */
export const REMAKE = "npm run rule-where";

/**
 * `loosenedFrom` (DECISIONS.md 732), only for a rule we have loosened: by version, the
 * earlier versions whose issues may no longer be issues under it (src/engine/loosened.ts).
 */
export type RuleWhere = Record<string, { version: number; where?: string[]; loosenedFrom?: Record<string, number[]> }>;

/** Each rule once, by id in code-point order, so the file is the same bytes wherever it's made. */
export function ruleWhere(rules: readonly Rule[] = ALL_MODULES.flatMap((m) => m.rules)): RuleWhere {
  const out: RuleWhere = {};
  for (const rule of [...rules].sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0))) {
    if (Object.hasOwn(out, rule.id)) throw new Error(`Two rules have the id ${rule.id}`);
    const loosened = loosenedFrom(rule.id);
    out[rule.id] = { version: rule.version, ...(rule.where ? { where: [...rule.where] } : {}), ...(loosened ? { loosenedFrom: loosened } : {}) };
  }
  return out;
}

export const ruleWhereText = (rules?: readonly Rule[]): string => `${JSON.stringify(ruleWhere(rules), null, 2)}\n`;

if (import.meta.main) {
  writeFileSync(RULE_WHERE_FILE, ruleWhereText());
  console.log(`Wrote ${Object.keys(ruleWhere()).length} rules to fixtures/rule-where.json. Copy it to the console's docs/health-check/engine-rules/where.json.`);
}
