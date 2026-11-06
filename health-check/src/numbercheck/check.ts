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
/** A figure that traces only to what someone told us, with where it was said. */
export type SaidFigure = { raw: string; source: string };
export type CheckOutcome = { passed: boolean; checked: number; unmatched: UnmatchedFigure[]; said: SaidFigure[] };

// A figure only someone said is their statement, not a measure (rule 1), so its
// sentence has to say it was said. These are the words that do.
export const ATTRIBUTION = /\b(?:said|say|says|saying|told us|tell us|tells us|telling us|rated|rate it|put it at|puts it at|putting it at|estimated?|estimates|reckon(?:s|ed)?|think|thinks|thought|believe[sd]?|feel|feels|felt|according to|in (?:an |the |our |their )?interviews?|guess(?:es|ed)?|recall(?:s|ed)?)\b/i;

/** The sentence a figure sits in. A full stop inside a number ("2.5") doesn't end one. */
function sentenceAround(text: string, start: number, end: number): string {
  let from = 0;
  for (const m of text.slice(0, start).matchAll(/[.!?](?=\s)|\n/g)) from = m.index + 1;
  const stop = text.slice(end).search(/[.!?](?=\s|$)|\n/);
  return text.slice(from, stop === -1 ? text.length : end + stop);
}

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

function matchesFor(figure: Figure, set: FigureSet): DatasetFigure[] {
  return candidates(figure, set).filter((f) => agrees(figure.value!, figure.step!, f.value));
}

function sourcesFor(figure: Figure, set: FigureSet): Set<string> {
  return new Set(matchesFor(figure, set).map((f) => f.source));
}

/**
 * Where a figure was said, when nothing but a person's words backs it. Null
 * when the data backs it too, or nothing does.
 */
function saidOnly(figure: Figure, set: FigureSet): string | null {
  const hits =
    figure.kind === "fraction"
      ? set.figures.filter((f) => f.kind === "percent" && Math.abs(f.value - figure.value!) <= FRACTION_TOLERANCE)
      : figure.value === undefined || figure.step === undefined
        ? []
        : matchesFor(figure, set);
  if (hits.length === 0 || hits.some((h) => !h.said)) return null;
  return [...new Set(hits.map((h) => h.said!))].join("; ");
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
    const { figures, normalised: read } = extractFigures(text, { labels: set.labels.map((l) => l.value) });
    const unmatched: UnmatchedFigure[] = [];
    const said: SaidFigure[] = [];
    for (const figure of figures) {
      const reason = checkOne(figure, figures, set);
      if (reason) {
        unmatched.push({ raw: figure.raw, reason });
        continue;
      }
      const where = saidOnly(figure, set);
      if (!where) continue;
      if (ATTRIBUTION.test(sentenceAround(read, figure.start, figure.end))) said.push({ raw: figure.raw, source: where });
      else unmatched.push({ raw: figure.raw, reason: `this is only what was ${where}, so write it as what they told us, not as a figure we measured` });
    }
    return { passed: unmatched.length === 0, checked: figures.length, unmatched, said };
  } catch (err) {
    return { passed: false, checked: 0, unmatched: [{ raw: "(whole text)", reason: `The number check failed to run: ${err instanceof Error ? err.message : String(err)}` }], said: [] };
  }
}

/** In the shape `SectionVersion.numberCheck` stores. `said` traces each figure only someone told us. */
export function toNumberCheckResult(outcome: CheckOutcome, now: Date): { passed: boolean; checkedAt: string; unmatched: string[]; said?: string[] } {
  return {
    passed: outcome.passed,
    checkedAt: now.toISOString(),
    unmatched: outcome.unmatched.map((u) => `${u.raw}: ${u.reason}`),
    ...(outcome.said.length ? { said: outcome.said.map((s) => `${s.raw}: ${s.source}`) } : {}),
  };
}
