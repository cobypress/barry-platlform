import { CURRENCY_UNKNOWN, NO_MONEY_FIGURE } from "../engine/currency.ts";
import type { Finding } from "../schemas/index.ts";
import { HOURS_NOT_NAMED, type ValueModel } from "../value/model.ts";
import { AMOUNT_MARK, escapeXml, probingAmounts } from "./format.ts";

// Amounts in a currency not known by name are plain numbers (DECISIONS.md 700),
// so every page that prints one says so: the document is made once with each
// such amount marked, to find those pages, and again for real, with the line
// put on each, above its footer. A page that already says it gets it once.

/** The document `make` builds, with `note` on every page that prints an amount. With no note, exactly as `make` builds it. */
export function withAmountNotes(note: string | undefined, make: () => string): string {
  if (!note) return make();
  const probe = probingAmounts(make).split(PAGE);
  const real = make().split(PAGE);
  const line = `<p class="note amounts-note">${escapeXml(note)}</p>`;
  const said = escapeXml(note);
  // The same document twice, so the same pages: if not, every page says it rather than any going without.
  const marked = (i: number): boolean => (probe.length === real.length ? probe[i]!.includes(AMOUNT_MARK) : true);
  return real.map((part, i) => (i > 0 && marked(i) && !part.includes(said) ? noted(part, line) : part)).join("");
}

const PAGE = /(?=<section class="page)/;

/** The line put last on the page, before its footer if it has one. */
function noted(part: string, line: string): string {
  const end = part.indexOf("</section>");
  if (end === -1) return part;
  const footer = part.lastIndexOf('<div class="footer"', end);
  const at = footer === -1 ? end : footer;
  return `${part.slice(0, at)}${line}${part.slice(at)}`;
}

/**
 * Why there's no money figure where one would be (DECISIONS.md 700), when it's
 * that the currency couldn't be told or named, for these findings (all, if
 * none are given); else null, and the page says what it always said.
 */
export function noMoneyReason(value: Pick<ValueModel, "left">, findings: Finding[], ids?: Set<string>): string | null {
  const own = (id: string) => !ids || ids.has(id);
  if (findings.some((f) => own(f.id) && f.status === "issue" && f.value.kind === "not_sized" && f.value.reason.endsWith(CURRENCY_UNKNOWN))) return NO_MONEY_FIGURE;
  if (value.left.some((l) => own(l.findingId) && l.reason === HOURS_NOT_NAMED)) return HOURS_UNPRICED;
  return null;
}

/** Said where a run's only money is time, and its currency isn't known by name. */
export const HOURS_UNPRICED = "We don't know your currency yet, so we haven't put a money figure on the time these take. Tell us and your next health check can show it.";
