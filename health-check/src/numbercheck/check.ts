import { extractFigures, type Figure, type PartialDate } from "./extract.ts";
import type { DatasetFigure, FigureSet } from "./dataset.ts";

// Rule 1. Every figure in generated text must trace to the dataset, in the same
// unit, or the text is rejected. It errs on the side of blocking: a false alarm
// costs a redraft, and a miss puts an invented number in front of a customer.
//
// What it can't do: prove a true number is attached to the right claim. "Win rate
// is 20.6%" passes if 20.6% is the no decision rate in the same scope. That's why
// sections are checked against their own sources, and why a person approves them.

/** A rounded figure may drift this far from the real one. "£1m" for £1.4m doesn't pass. */
const MAX_ROUNDING_ERROR = 0.05;
/** "half" and "a third" are exact claims: 49% to 51%, not "nearly half" of 44%. */
const FRACTION_TOLERANCE = 1;

export type UnmatchedFigure = { raw: string; reason: string };
export type CheckOutcome = { passed: boolean; checked: number; unmatched: UnmatchedFigure[] };

/** The figure as written is the real value rounded to the precision it shows, within limits. */
export function agrees(written: number, step: number, real: number): boolean {
  if (Math.sign(written) !== Math.sign(real) && written !== 0 && real !== 0) return false;
  const tiny = 1e-9 * Math.max(1, Math.abs(real));
  if (Math.abs(written - real) < tiny) return true;
  if (written === 0 || real === 0) return false;
  const rounded = Math.round(Math.abs(real) / step + 1e-9) * step;
  if (Math.abs(rounded - Math.abs(written)) > tiny + step * 1e-6) return false;
  return Math.abs(Math.abs(written) - Math.abs(real)) / Math.abs(real) <= MAX_ROUNDING_ERROR;
}

function candidates(figure: Figure, set: FigureSet): DatasetFigure[] {
  return set.figures.filter((f) => {
    if (figure.kind === "number") return f.kind === "number";
    if (f.kind !== figure.kind) return false;
    if (figure.kind === "currency") return f.currency === figure.currency;
    if (figure.kind === "duration") return f.unit === figure.unit;
    return true;
  });
}

const dateMatches = (written: PartialDate, known: PartialDate): boolean => {
  if (written.y !== undefined && written.y !== known.y) return false;
  if (written.m !== undefined && written.m !== known.m) return false;
  if (written.d !== undefined && written.d !== known.d) return false;
  if (written.q !== undefined) {
    if (known.m === undefined) return known.q === written.q;
    if (Math.ceil(known.m / 3) !== written.q) return false;
  }
  return true;
};

function sourcesFor(figure: Figure, set: FigureSet): Set<string> {
  return new Set(candidates(figure, set).filter((f) => agrees(figure.value!, figure.step!, f.value)).map((f) => f.source));
}

function checkOne(figure: Figure, figures: Figure[], set: FigureSet): string | null {
  switch (figure.kind) {
    case "token":
      return set.tokens.some((t) => t.value === figure.raw) ? null : "this name token isn't in the data";
    case "identifier":
      return set.identifiers.some((i) => i.value === figure.raw.toLowerCase()) ? null : "this id isn't in the data";
    case "date":
      return set.dates.some((d) => dateMatches(figure.date!, d)) ? null : "this date isn't in the data";
    case "magnitude":
      return "a vague scale isn't a figure from the data";
    case "malformed":
      return "this can't be read as a figure";
    case "fraction": {
      const hit = set.figures.some((f) => f.kind === "percent" && Math.abs(f.value - figure.value!) <= FRACTION_TOLERANCE);
      return hit ? null : `no percentage near ${Math.round(figure.value! * 10) / 10}% in the data`;
    }
    default: {
      const linked = figure.linkedTo === undefined ? undefined : figures[figure.linkedTo];
      if (linked) {
        // "1 in 5" also reads as 20%, which is how the engine usually holds it.
        const share = (figure.value! / linked.value!) * 100;
        if (Number.isFinite(share) && share <= 100 && set.figures.some((f) => f.kind === "percent" && Math.abs(f.value - share) <= FRACTION_TOLERANCE)) return null;
      }
      const sources = sourcesFor(figure, set);
      if (sources.size === 0) {
        const wrongUnit = set.figures.some((f) => agrees(figure.value!, figure.step!, f.value));
        return wrongUnit ? `this value exists, but not as a ${describe(figure)}` : "not in the data";
      }
      if (linked && linked.value !== undefined) {
        if (figure.value! > linked.value) return `can't be ${figure.raw} of ${linked.raw}`;
        const other = sourcesFor(linked, set);
        if (![...sources].some((s) => other.has(s))) return `${figure.raw} and ${linked.raw} don't come from the same finding`;
      }
      return null;
    }
  }
}

function describe(figure: Figure): string {
  if (figure.kind === "currency") return `${figure.currency} amount`;
  if (figure.kind === "duration") return `${figure.unit} count`;
  return figure.kind === "number" ? "plain count" : figure.kind;
}

/** Checks text against a dataset. Anything that goes wrong in the check itself blocks. */
export function checkNumbers(text: string, set: FigureSet): CheckOutcome {
  try {
    const { figures } = extractFigures(text, { labels: set.labels.map((l) => l.value) });
    const unmatched: UnmatchedFigure[] = [];
    for (const figure of figures) {
      const reason = checkOne(figure, figures, set);
      if (reason) unmatched.push({ raw: figure.raw, reason });
    }
    return { passed: unmatched.length === 0, checked: figures.length, unmatched };
  } catch (err) {
    return { passed: false, checked: 0, unmatched: [{ raw: "(whole text)", reason: `The number check failed to run: ${err instanceof Error ? err.message : String(err)}` }] };
  }
}

/** In the shape `SectionVersion.numberCheck` stores. */
export function toNumberCheckResult(outcome: CheckOutcome, now: Date): { passed: boolean; checkedAt: string; unmatched: string[] } {
  return { passed: outcome.passed, checkedAt: now.toISOString(), unmatched: outcome.unmatched.map((u) => `${u.raw}: ${u.reason}`) };
}
