import { test } from "node:test";
import { eng, org, said, type Said } from "../org-words.ts";

// What the type check lets into `said` (DECISIONS.md 655): `npm run typecheck`
// holds every line marked below to be refused. A value read at run time passes
// only as org words, or through eng, which review signs off.

declare const label: string;
declare const anything: any;
declare const kind: "Flow" | "Apex";
declare const n: number;

/** Never called: the expressions are here for the type check. */
export function typed(): Said[] {
  return [
    // Allowed: literals, unions of literals, numbers, org words, sentences, eng.
    said`${kind} ${n} ${org(label)} ${said`inner`} ${eng("2 flows")} ${n === 1 ? "is" : "are"}`,
    // @ts-expect-error a plain string read at run time
    said`Object ${label} runs`,
    // @ts-expect-error a template around a value read at run time: its type is the pattern `${string}!`
    said`Object ${`${label}!`} runs`,
    // @ts-expect-error the same with as const
    said`Object ${`x${label}` as const} runs`,
    // @ts-expect-error an intrinsic string type has endless members
    said`Object ${label.toUpperCase() as Uppercase<string>} runs`,
    // @ts-expect-error any
    said`Object ${anything} runs`,
    // @ts-expect-error String(x) is a string
    said`Object ${String(label)} runs`,
  ];
}

test("the type check refuses run-time strings in said (held by npm run typecheck)", () => {});
