// Reads every figure out of a piece of text. This is the last line of defence
// against an invented number, so it fails closed: anything that looks numeric
// and can't be understood comes back as "malformed", and the check blocks it.
// A final sweep guarantees no digit leaves here unaccounted for.

export type FigureKind = "number" | "percent" | "points" | "currency" | "ratio" | "fraction" | "duration" | "date" | "identifier" | "token" | "magnitude" | "malformed";

export type PartialDate = { y?: number; m?: number; d?: number; q?: number };

export type Figure = {
  raw: string;
  start: number;
  end: number;
  kind: FigureKind;
  /** Present on every numeric kind. A fraction's value is a percentage. */
  value?: number;
  /** The precision it was written to: 1 for "35", 0.1 for "35.2", 100000 for "£2.9m". */
  step?: number;
  currency?: string;
  /** For a duration: day, week, month, quarter, year, hour or minute. */
  unit?: string;
  date?: PartialDate;
  /** Index of the figure this one is "of", in "15 of 70". */
  linkedTo?: number;
  /** The multiplier it was written with: 1e6 for "£2.9m". */
  scale?: number;
};

const MONTHS = ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"];
const MONTH = String.raw`jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|june?|july?|aug(?:ust)?|sept?(?:ember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?`;
const monthNumber = (name: string): number => MONTHS.findIndex((m) => m.startsWith(name.toLowerCase().slice(0, 3))) + 1;

const CURRENCY_SYMBOL: Record<string, string> = { "£": "GBP", $: "USD", "€": "EUR" };
const CURRENCY_WORD: Record<string, string> = { gbp: "GBP", usd: "USD", eur: "EUR", pound: "GBP", pounds: "GBP", sterling: "GBP", quid: "GBP", dollar: "USD", dollars: "USD", euro: "EUR", euros: "EUR" };

const UNITS: Record<string, number> = { zero: 0, one: 1, two: 2, three: 3, four: 4, five: 5, six: 6, seven: 7, eight: 8, nine: 9, ten: 10, eleven: 11, twelve: 12, thirteen: 13, fourteen: 14, fifteen: 15, sixteen: 16, seventeen: 17, eighteen: 18, nineteen: 19 };
const TENS: Record<string, number> = { twenty: 20, thirty: 30, forty: 40, fourty: 40, fifty: 50, sixty: 60, seventy: 70, eighty: 80, ninety: 90 };
const LARGE: Record<string, number> = { hundred: 100, thousand: 1e3, million: 1e6, billion: 1e9, trillion: 1e12 };
const NUMBER_WORD = [...Object.keys(UNITS), ...Object.keys(TENS), ...Object.keys(LARGE), "dozen"].join("|");

const DENOMINATOR: Record<string, number> = { half: 2, halves: 2, third: 3, thirds: 3, quarter: 4, quarters: 4, fifth: 5, fifths: 5, sixth: 6, sixths: 6, seventh: 7, sevenths: 7, eighth: 8, eighths: 8, ninth: 9, ninths: 9, tenth: 10, tenths: 10 };
const ORDINAL: Record<string, number> = { third: 3, fourth: 4, fifth: 5, sixth: 6, seventh: 7, eighth: 8, ninth: 9, tenth: 10, eleventh: 11, twelfth: 12 };
const MULTIPLE: Record<string, number> = { double: 2, doubled: 2, doubles: 2, doubling: 2, twice: 2, triple: 3, tripled: 3, triples: 3, tripling: 3, treble: 3, trebled: 3, quadruple: 4, quadrupled: 4, halved: 0.5, halves: 0.5, halving: 0.5 };

const VULGAR: Record<string, string> = { "½": " one half ", "¼": " one quarter ", "¾": " three quarters ", "⅓": " one third ", "⅔": " two thirds ", "⅕": " one fifth ", "⅖": " two fifths ", "⅗": " three fifths ", "⅘": " four fifths ", "⅙": " one sixth ", "⅛": " one eighth ", "⅒": " one tenth " };

/**
 * Removes the ways a figure can be hidden from a reader of the raw string:
 * full-width digits, zero-width joiners, emphasis around a number, unusual dashes.
 */
export function normalise(text: string): string {
  return text
    .replace(/[½¼¾⅓⅔⅕⅖⅗⅘⅙⅛⅒]/g, (c) => VULGAR[c]!)
    .normalize("NFKC")
    .replace(/[\u00AD\u200B-\u200F\u202A-\u202E\u2060-\u2064\uFEFF]/g, "")
    .replace(/[\u2212\u2010-\u2015]/g, "-")
    .replace(/[*`]/g, "")
    .replace(/\s/g, " ");
}

const pow10 = (dp: number): number => Number(`1e-${dp}`);

function parseDigits(raw: string): { value: number; step: number } {
  const clean = raw.replace(/[, ]/g, "");
  const dp = clean.includes(".") ? clean.length - clean.indexOf(".") - 1 : 0;
  return { value: Number(clean), step: pow10(dp) };
}

/** Whole-number words only. Returns null if the sequence doesn't read as one number. */
function parseWords(words: string[]): number | null {
  let total = 0;
  let current = 0;
  let seen = false;
  for (const raw of words) {
    const word = raw.toLowerCase();
    if (word === "and") continue;
    seen = true;
    if (word in UNITS) current += UNITS[word]!;
    else if (word in TENS) current += TENS[word]!;
    else if (word === "dozen") current = (current || 1) * 12;
    else if (word === "hundred") current = (current || 1) * 100;
    else if (word in LARGE) {
      total += (current || 1) * LARGE[word]!;
      current = 0;
    } else return null;
  }
  return seen ? total + current : null;
}

const wordStep = (value: number): number => (value === 0 ? 1 : value % 1e6 === 0 ? 1e6 : value % 1e3 === 0 ? 1e3 : 1);

type Unit = { length: number; kind: FigureKind; currency?: string; unit?: string };

function readUnit(after: string): Unit | null {
  let m: RegExpExecArray | null;
  if ((m = /^ ?(?:%|per ?cent\b|pct\b|pc\b)/i.exec(after))) return { length: m[0].length, kind: "percent" };
  if ((m = /^[ -]?(?:basis[ -]points?|bps)\b/i.exec(after))) return { length: m[0].length, kind: "malformed" };
  if ((m = /^[ -]?(?:percentage[ -]points?|ppts?\b|pts?\b|points?\b|pp\b)/i.exec(after))) return { length: m[0].length, kind: "points" };
  if ((m = /^[ -]?(?:x\b|times\b|fold\b)/i.exec(after))) return { length: m[0].length, kind: "ratio" };
  if ((m = /^ ?(GBP|USD|EUR|pounds?|sterling|quid|dollars?|euros?)\b/i.exec(after))) return { length: m[0].length, kind: "currency", currency: CURRENCY_WORD[m[1]!.toLowerCase()]! };
  if ((m = /^[ -]?(day|week|month|quarter|year|hour|minute)s?\b/i.exec(after))) return { length: m[0].length, kind: "duration", unit: m[1]!.toLowerCase() };
  return null;
}

function readScale(after: string): { length: number; factor: number } | null {
  let m: RegExpExecArray | null;
  if ((m = /^ ?(hundred|thousand|million|billion|trillion)\b/i.exec(after))) return { length: m[0].length, factor: LARGE[m[1]!.toLowerCase()]! };
  if ((m = /^(k|mn|m|bn|b)\b/i.exec(after))) {
    const suffix = m[1]!.toLowerCase();
    return { length: m[0].length, factor: suffix === "k" ? 1e3 : suffix === "m" || suffix === "mn" ? 1e6 : 1e9 };
  }
  return null;
}

// "90-day", "12k", "3rd", "4x": digits with a tail that the numeric pass reads.
const NUMERIC_TAIL = /^\d[\d,.]*(?:-\d[\d,.]*)?(?:k|mn|m|bn|b|x|st|nd|rd|th|pc|pts?|ppts?|pp|bps)$|^\d[\d,.]*-[a-z]+$/i;

const YEAR_LEADS = /\b(?:in|during|since|from|to|until|till|by|of|before|after|through|throughout|year|and|or|for|early|mid|late|end|start|fy|calendar)\s*-?$/i;

class Scanner {
  readonly source: string;
  #work: string;
  figures: Figure[] = [];

  constructor(source: string) {
    this.source = source;
    this.#work = source;
  }

  get work(): string {
    return this.#work;
  }

  claim(start: number, end: number): void {
    this.#work = `${this.#work.slice(0, start)}${" ".repeat(end - start)}${this.#work.slice(end)}`;
  }

  add(start: number, end: number, figure: Omit<Figure, "raw" | "start" | "end">): void {
    this.figures.push({ ...figure, raw: this.source.slice(start, end), start, end });
    this.claim(start, end);
  }

  /** Runs a pattern over what's still unclaimed. The handler returns false to leave a match alone. */
  each(pattern: RegExp, handle: (m: RegExpExecArray, start: number, end: number) => boolean | void): void {
    for (const m of [...this.#work.matchAll(pattern)] as RegExpExecArray[]) {
      const start = m.index;
      const end = start + m[0].length;
      // An earlier match in this pass may have widened over this one.
      if (this.#work.slice(start, end) !== m[0]) continue;
      handle(m, start, end);
    }
  }
}

function scanDates(s: Scanner): void {
  const date = (start: number, end: number, value: PartialDate): void => s.add(start, end, { kind: "date", date: value });
  s.each(/\b(\d{4})-(\d{2})-(\d{2})(?:T[\d:.]+(?:Z|[+-]\d{2}:?\d{2})?)?(?![\w-])/g, (m, a, b) => date(a, b, { y: +m[1]!, m: +m[2]!, d: +m[3]! }));
  s.each(new RegExp(String.raw`\b(\d{1,2})(?:st|nd|rd|th)? (?:of )?(${MONTH})\.?,? (\d{4})\b`, "gi"), (m, a, b) => date(a, b, { d: +m[1]!, m: monthNumber(m[2]!), y: +m[3]! }));
  s.each(new RegExp(String.raw`\b(${MONTH})\.? (\d{1,2})(?:st|nd|rd|th)?,? (\d{4})\b`, "gi"), (m, a, b) => date(a, b, { m: monthNumber(m[1]!), d: +m[2]!, y: +m[3]! }));
  s.each(new RegExp(String.raw`\b(\d{1,2})(?:st|nd|rd|th)? (?:of )?(${MONTH})\b(?![\w-])`, "gi"), (m, a, b) => date(a, b, { d: +m[1]!, m: monthNumber(m[2]!) }));
  s.each(new RegExp(String.raw`\b(${MONTH})\.? (\d{1,2})(?:st|nd|rd|th)\b`, "gi"), (m, a, b) => date(a, b, { m: monthNumber(m[1]!), d: +m[2]! }));
  s.each(new RegExp(String.raw`\b(${MONTH})\.?,? (\d{4})\b`, "gi"), (m, a, b) => date(a, b, { m: monthNumber(m[1]!), y: +m[2]! }));
  s.each(/\bQ([1-4])(?: ?(?:FY)? ?(\d{4}|\d{2}))?\b/gi, (m, a, b) => date(a, b, { q: +m[1]!, ...(m[2] ? { y: fullYear(m[2]) } : {}) }));
  s.each(/\bFY ?(\d{4}|\d{2})\b/gi, (m, a, b) => date(a, b, { y: fullYear(m[1]!) }));
  // UK order. "12/31/2025" has no reading in it, so it's blocked rather than guessed.
  s.each(/\b(\d{1,2})\/(\d{1,2})\/(\d{4}|\d{2})\b/g, (m, a, b) => {
    const [d, mo] = [+m[1]!, +m[2]!];
    if (mo > 12 || d > 31 || mo === 0 || d === 0) s.add(a, b, { kind: "malformed" });
    else date(a, b, { d, m: mo, y: fullYear(m[3]!) });
  });
}

const fullYear = (raw: string): number => (raw.length === 2 ? 2000 + Number(raw) : Number(raw));

function scanIdentifiers(s: Scanner): void {
  s.each(/\[\[[^\]]*\]\]/g, (_m, a, b) => s.add(a, b, { kind: "token" }));
  s.each(/\b(?:https?:\/\/|www\.)[^\s)]+/gi, (_m, a, b) => s.add(a, b, { kind: "identifier" }));
  s.each(/[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+/g, (_m, a, b) => s.add(a, b, { kind: "identifier" }));
  // Before dates: a run id holds a date that isn't a claim about anything.
  s.each(/\bhc-\d{4}-\d{2}-\d{2}-[0-9a-f]{8}\b/g, (_m, a, b) => s.add(a, b, { kind: "identifier" }));
  s.each(/\bf-[0-9a-f]{12}\b/g, (_m, a, b) => s.add(a, b, { kind: "identifier" }));
}

function scanAlphanumeric(s: Scanner): void {
  s.each(/(?<![\w.-])(?=[\w.-]*[A-Za-z_])(?=[\w.-]*\d)[A-Za-z0-9_]+(?:[._-][A-Za-z0-9_]+)*/g, (m, a, b) => {
    if (NUMERIC_TAIL.test(m[0])) return;
    s.add(a, b, { kind: "identifier" });
  });
}

function scanMalformed(s: Scanner): void {
  // "12,5" and "1.234.567" are European forms that would otherwise be misread.
  s.each(/(?<![\w.,])\d+(?:[.,]\d+)+(?![\w])/g, (m, a, b) => {
    if (/^\d{1,3}(?:,\d{3})+(?:\.\d+)?$/.test(m[0]) || /^\d+\.\d+$/.test(m[0])) return;
    s.add(a, b, { kind: "malformed" });
  });
  s.each(/&#?\w+;/g, (_m, a, b) => s.add(a, b, { kind: "malformed" }));
  s.each(/[^\x00-\x7F]/g, (m, a, b) => {
    if (/\p{Nd}/u.test(m[0])) s.add(a, b, { kind: "malformed" });
  });
}

function pushNumeric(s: Scanner, start: number, numberEnd: number, base: { value: number; step: number }, currency: string | undefined, kindHint?: FigureKind): void {
  let end = numberEnd;
  let { value, step } = base;
  const scale = readScale(s.work.slice(end));
  if (scale) {
    value *= scale.factor;
    step *= scale.factor;
    end += scale.length;
  }
  const ordinal = /^(?:st|nd|rd|th)\b/.exec(s.work.slice(end));
  if (ordinal) end += ordinal[0].length;
  const unit = readUnit(s.work.slice(end));
  if (unit) end += unit.length;
  const kind: FigureKind = currency ? "currency" : (unit?.kind ?? kindHint ?? "number");
  s.add(start, end, { kind, value, step, ...(scale ? { scale: scale.factor } : {}), ...(currency || unit?.currency ? { currency: currency ?? unit?.currency } : {}), ...(unit?.unit && !currency ? { unit: unit.unit } : {}) });
}

function scanDigits(s: Scanner): void {
  // A ratio or a time. Neither is a count, so neither may match a count.
  s.each(/(?<![\w.,:])(\d+(?:\.\d+)?):(\d+(?:\.\d+)?)(?![\w:])/g, (m, a, b) => {
    const right = Number(m[2]);
    s.add(a, b, right === 0 ? { kind: "malformed" } : { kind: "ratio", value: Number(m[1]) / right, step: 0.1 });
  });
  const number = String.raw`\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d{1,3}(?: \d{3})+(?![\d.,])|\d+(?:\.\d+)?|\.\d+`;
  const pattern = new RegExp(String.raw`(?:(?<=^|[\s(\[:=])(?<sign>[-+]))?(?:(?<cur>£|\$|€|(?:GBP|USD|EUR) ?)(?<sign2>-)?)?(?<![\w.,])(?<num>${number})`, "g");
  s.each(pattern, (m, a, b) => {
    const group = m.groups!;
    const parsed = parseDigits(group.num!);
    const negative = group.sign === "-" || group.sign2 === "-";
    const symbol = group.cur?.trim();
    const currency = symbol ? (CURRENCY_SYMBOL[symbol] ?? symbol) : undefined;
    pushNumeric(s, a, b, { value: negative ? -parsed.value : parsed.value, step: parsed.step }, currency);
  });
}

function scanFractions(s: Scanner): void {
  const leads = `a|an|one|two|three|four|five|six|seven|eight|nine`;
  const denominators = Object.keys(DENOMINATOR).join("|");
  // A large number in fraction form, before "half" and "quarter" read alone.
  s.each(/\b(half|quarter) (?:of )?a (million|billion|thousand)\b/gi, (m, a, b) => {
    const factor = (m[1]!.toLowerCase() === "half" ? 0.5 : 0.25) * LARGE[m[2]!.toLowerCase()]!;
    pushNumeric(s, a, b, { value: factor, step: factor }, undefined);
  });
  s.each(new RegExp(String.raw`\b(${leads})[ -](${denominators})\b(?![ -]part(?:y|ies)\b)(?![ -](?:year|day)\b)`, "gi"), (m, a, b) => {
    const denominator = m[2]!.toLowerCase();
    const before = s.work.slice(0, a);
    const after = s.work.slice(b);
    // "in a quarter" is a period of time. "a quarter of" never is.
    if (denominator === "quarter" && !/^ of\b/i.test(after) && /\b(?:in|per|each|every|within|over|across|for|during|last|this|next)\s*$/i.test(before)) return;
    const lead = m[1]!.toLowerCase();
    const n = lead === "a" || lead === "an" ? 1 : UNITS[lead]!;
    s.add(a, b, { kind: "fraction", value: (n / DENOMINATOR[denominator]!) * 100, step: 1 });
  });
  s.each(/\b(?:first|second|1st|2nd) half\b/gi, (_m, a, b) => s.claim(a, b));
  s.each(/\bhalf\b(?![ -](?:year|day|hour)\b|-yearly)/gi, (_m, a, b) => s.add(a, b, { kind: "fraction", value: 50, step: 1 }));
}

function scanWords(s: Scanner): void {
  s.each(/\b(?:hundreds|thousands|millions|billions|dozens|tens of|scores of)\b/gi, (_m, a, b) => s.add(a, b, { kind: "magnitude" }));
  s.each(/\b(?:double|triple)[ -](?:check|checks|checked|checking|count|counts|counted|counting|entry|click|clicks)\b/gi, (_m, a, b) => s.claim(a, b));
  s.each(new RegExp(String.raw`\b(?:${Object.keys(MULTIPLE).join("|")})\b`, "gi"), (m, a, b) => s.add(a, b, { kind: "ratio", value: MULTIPLE[m[0].toLowerCase()]!, step: 0.1 }));
  s.each(/\b(?:no|any|every|each|another) one\b|\bone[ -](?:off|another|by one|or more|of the|of these|of those|of your|of our)\b|\bone-to-one\b/gi, (_m, a, b) => s.claim(a, b));
  s.each(/\ba (?:couple|pair) of\b/gi, (_m, a, b) => s.add(a, b, { kind: "number", value: 2, step: 1 }));
  s.each(/\bthird[ -]part(?:y|ies)\b/gi, (_m, a, b) => s.claim(a, b));
  s.each(new RegExp(String.raw`\b(?:a |an )?(?:${NUMBER_WORD})(?:[ -](?:and )?(?:${NUMBER_WORD}))*\b`, "gi"), (m, a, b) => {
    const words = m[0].split(/[ -]+/).filter((w) => !/^an?$/i.test(w));
    const value = parseWords(words);
    if (value === null) return void s.add(a, b, { kind: "malformed" });
    pushNumeric(s, a, b, { value, step: wordStep(value) }, undefined);
  });
  s.each(new RegExp(String.raw`\b(?:${Object.keys(ORDINAL).join("|")})\b`, "gi"), (m, a, b) => s.add(a, b, { kind: "number", value: ORDINAL[m[0].toLowerCase()]!, step: 1 }));
}

function scanBareYears(s: Scanner): void {
  s.each(/(?<![\w.,£$€-])(?:19|20)\d{2}(?![\w%]|[.,]\d| ?(?:GBP|USD|EUR))/g, (m, a, b) => {
    if (!YEAR_LEADS.test(s.work.slice(0, a).trimEnd()) && !YEAR_LEADS.test(s.source.slice(0, a).trimEnd())) return;
    s.add(a, b, { kind: "date", date: { y: +m[0] } });
  });
}

const NUMERIC: ReadonlySet<FigureKind> = new Set(["number", "percent", "points", "currency", "ratio", "duration"]);

/** "10-20%" and "£1-2m": each end of a range takes the unit and scale the other one shows. */
function spreadRanges(figures: Figure[], source: string): void {
  for (let i = 0; i + 1 < figures.length; i++) {
    const [a, b] = [figures[i]!, figures[i + 1]!];
    if (!NUMERIC.has(a.kind) || !NUMERIC.has(b.kind)) continue;
    const gap = source.slice(a.end, b.start);
    const between = /^ *and *$/i.test(gap) && /\bbetween *$/i.test(source.slice(0, a.start));
    if (!/^ *(?:-|to) *$/i.test(gap) && !between) continue;
    for (const [from, to] of [[b, a], [a, b]] as const) {
      if (to.kind !== "number" || from.kind === "number") continue;
      to.kind = from.kind;
      if (from.unit) to.unit = from.unit;
      if (from.currency) to.currency = from.currency;
    }
    if (b.scale && !a.scale && Math.abs(a.value!) < Math.abs(b.value! / b.scale)) {
      a.value! *= b.scale;
      a.step! *= b.scale;
      a.scale = b.scale;
    }
  }
}

function linkPairs(figures: Figure[], source: string): void {
  for (let i = 0; i + 1 < figures.length; i++) {
    const [a, b] = [figures[i]!, figures[i + 1]!];
    if (a.kind !== "number" || b.kind !== "number") continue;
    if (/^(?: out of | of | in |\/)(?:the |all |your |a total of )?$/i.test(source.slice(a.end, b.start))) a.linkedTo = i + 1;
  }
}

export type ExtractOptions = {
  /** Names that contain digits and aren't claims, such as a dashboard called "Q3 Pipeline". */
  labels?: string[];
};

export function extractFigures(text: string, options: ExtractOptions = {}): { normalised: string; figures: Figure[] } {
  const s = new Scanner(normalise(text));
  scanIdentifiers(s);
  for (const label of [...(options.labels ?? [])].sort((a, b) => b.length - a.length)) {
    const norm = normalise(label);
    if (!norm.trim()) continue;
    let at = s.work.indexOf(norm);
    while (at !== -1) {
      const end = at + norm.length;
      if (!/[\w]/.test(s.work[at - 1] ?? "") && !/[\w]/.test(s.work[end] ?? "")) s.claim(at, end);
      at = s.work.indexOf(norm, end);
    }
  }
  scanDates(s);
  scanMalformed(s);
  scanAlphanumeric(s);
  scanBareYears(s);
  scanDigits(s);
  scanFractions(s);
  scanWords(s);
  // Nothing numeric leaves unaccounted for, whatever the patterns above missed.
  s.each(/\d+/g, (_m, a, b) => s.add(a, b, { kind: "malformed" }));

  const figures = s.figures.sort((a, b) => a.start - b.start);
  spreadRanges(figures, s.source);
  linkPairs(figures, s.source);
  return { normalised: s.source, figures };
}
