import type { Finding, Metric, NumberOverride } from "../schemas/index.ts";

// An override is the only way a number changes without a re-run. The raw notes
// list it as it was, with the reason. Reports show the figure as it now stands,
// so an executive reader never sees the number a consultant has corrected.

const escapeRegex = (value: string): string => value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

function swap(text: string, from: number, to: number): string {
  const pattern = new RegExp(`(?<![\\d.,])${escapeRegex(String(from))}(?![\\d]|[.,]\\d)`, "g");
  return text.replace(pattern, String(to));
}

function overrideFinding(f: Finding, o: NumberOverride): Finding {
  const [root, key] = o.target.field.split(".");
  const next: Finding = { ...f, summary: swap(f.summary, o.previousValue, o.newValue), title: swap(f.title, o.previousValue, o.newValue), facts: { ...f.facts } };
  if (root === "facts" && key && key in next.facts) next.facts[key] = o.newValue;
  if (o.target.field === "value.amount" && next.value.kind !== "not_sized") next.value = { ...next.value, amount: o.newValue };
  return next;
}

function overrideMetric(m: Metric, o: NumberOverride): Metric {
  if (m.status !== "measured") return m;
  const stat = /^stats\.(.+)$/.exec(o.target.field)?.[1];
  if (o.target.field === "value") return { ...m, value: o.newValue };
  if (o.target.field === "n") return { ...m, n: o.newValue };
  return stat ? { ...m, stats: { ...m.stats, [stat]: o.newValue } } : m;
}

export function applyOverrides(findings: Finding[], metrics: Metric[], overrides: NumberOverride[]): { findings: Finding[]; metrics: Metric[] } {
  let [outF, outM] = [findings, metrics];
  for (const o of overrides) {
    if (o.target.type === "finding") outF = outF.map((f) => (f.id === o.target.id ? overrideFinding(f, o) : f));
    else outM = outM.map((m) => (m.id === o.target.id ? overrideMetric(m, o) : m));
  }
  return { findings: outF, metrics: outM };
}
