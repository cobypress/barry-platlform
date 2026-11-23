import { extractFigures, type FigureSet } from "../numbercheck/index.ts";
import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";

/**
 * The dataset a customer-facing output is checked against: the report's, with
 * each name its tokens stand for as a label (DECISIONS.md 682). A section is
 * checked with a person's or an account's token in it, "[[account:A0001]]", and
 * the page prints the name the token restores to, which the data holds only as
 * the token: a name with a digit in it ("Unit4 Ltd") was then held on the page
 * though its token passed.
 *
 * A name is a label only when the number check reads nothing in it but words
 * of letters and digits, at least one: "Unit4 Ltd" is one, while "3M", "24
 * Seven" and "Studio 54" read as figures, so they stay figures to trace and
 * can never mask a figure of the same shape elsewhere on the page.
 */
export function pageDataset(set: FigureSet, pseudonyms: Pseudonymiser): FigureSet {
  const tokens = new Set(set.tokens.map((t) => t.value));
  const names = pseudonyms.entries().filter((e) => {
    if (!tokens.has(e.token)) return false;
    const figures = extractFigures(e.name).figures;
    return figures.length > 0 && figures.every((f) => f.kind === "identifier" && f.word);
  });
  return { ...set, labels: [...set.labels, ...names.map((e) => ({ source: e.token, value: e.name }))] };
}
