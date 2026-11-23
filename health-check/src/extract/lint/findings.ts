// What PMD and ESLint found, as the run keeps it (DECISIONS.md 670). Never a
// line of the code, a snippet or the tool's own message: both tools' messages
// quote names from the code. A finding is where it is and which of our rules it
// broke, and each rule carries our own fixed description instead.

export type LintTool = "pmd" | "eslint";
/** What a component is: Apex today; a Lightning bundle's JavaScript once a later lane reads one. */
export type LintKind = "class" | "trigger" | "lwc" | "aura";

export type LintFinding = {
  tool: LintTool;
  toolVersion: string;
  /** The tool's rule id, one of our pinned set. */
  rule: string;
  /** PMD's category for the rule ("Security", "Performance"), as PMD gives it. Null for ESLint. */
  category: string | null;
  /** As the tool gives it: PMD's priority, 1 (worst) to 5; ESLint's 1 (warn) or 2 (error) from our configuration. */
  severity: number;
  kind: LintKind;
  /** The class's, trigger's or bundle's API name. */
  component: string;
  line: number;
  column: number | null;
  /** Hidden by the code's own @SuppressWarnings or NOPMD: kept, and marked, so it isn't silently lost. */
  suppressed?: true;
};

/** How many a component had, beside how many are kept. Only components with a finding are listed. */
export type ComponentCount = { kind: LintKind; component: string; found: number; stored: number };

export type ToolRun =
  | {
      status: "ran";
      tool: LintTool;
      toolVersion: string;
      /** Components the tool was given and read. */
      checked: number;
      /** Components not checked, by why. None of these is "no findings". */
      skipped: { tooLarge: number; unparsable: number; unreadable: number; failed: number; timeLimit: number };
      found: number;
      stored: number;
      components: ComponentCount[];
      findings: LintFinding[];
      /** Our own fixed description of each rule that has a finding here. */
      rules: Record<string, string>;
      /** Findings the tool reported under a rule outside our pinned set: dropped, and counted. */
      outsideRules: number;
      /** PMD only: findings whose line, column or priority couldn't be true of the file, dropped and counted (DECISIONS.md 670). */
      malformed?: number;
      /** ESLint only: inline configuration comments in the code, each ignored. */
      inlineConfigIgnored?: number;
      /** Anything else the reader should know: a part of the code that couldn't be read, and why. */
      limits: string[];
    }
  | { status: "not_run"; tool: LintTool; reason: string };

export const MAX_FINDINGS_PER_COMPONENT = 50;
export const MAX_FINDINGS_PER_RUN = 2000;
/** A body or file longer than this isn't handed to a tool. Salesforce's own limit on a class is 1,000,000 characters. */
export const MAX_SOURCE_CHARS = 200_000;

export const notRun = (tool: LintTool, reason: string): ToolRun => ({ status: "not_run", tool, reason });

/**
 * Worst first, so a bound keeps what matters most: PMD's priority 1 is its
 * worst, and ESLint's severity 2 is an error.
 */
const rank = (f: LintFinding): number => (f.tool === "pmd" ? f.severity : 3 - f.severity);

const byPlace = (a: LintFinding, b: LintFinding): number =>
  a.kind.localeCompare(b.kind) || a.component.localeCompare(b.component) || a.line - b.line || (a.column ?? 0) - (b.column ?? 0) || a.rule.localeCompare(b.rule);

/**
 * At most MAX_FINDINGS_PER_COMPONENT a component and MAX_FINDINGS_PER_RUN in
 * all, worst first, with each component's count found beside its count kept,
 * so what was left out is said in numbers. The same findings in any order give
 * the same result.
 */
export function bound(all: LintFinding[]): { findings: LintFinding[]; found: number; stored: number; components: ComponentCount[] } {
  const groups = new Map<string, LintFinding[]>();
  for (const f of all) {
    const key = `${f.kind}\u0000${f.component}`;
    const list = groups.get(key);
    if (list) list.push(f);
    else groups.set(key, [f]);
  }
  const kept: LintFinding[] = [];
  for (const list of groups.values()) kept.push(...[...list].sort((a, b) => rank(a) - rank(b) || byPlace(a, b)).slice(0, MAX_FINDINGS_PER_COMPONENT));
  const findings = kept
    .sort((a, b) => rank(a) - rank(b) || byPlace(a, b))
    .slice(0, MAX_FINDINGS_PER_RUN)
    .sort(byPlace);
  const storedBy = new Map<string, number>();
  for (const f of findings) storedBy.set(`${f.kind}\u0000${f.component}`, (storedBy.get(`${f.kind}\u0000${f.component}`) ?? 0) + 1);
  const components = [...groups.entries()]
    .map(([key, list]) => ({ kind: list[0]!.kind, component: list[0]!.component, found: list.length, stored: storedBy.get(key) ?? 0 }))
    .sort((a, b) => a.kind.localeCompare(b.kind) || a.component.localeCompare(b.component));
  return { findings, found: all.length, stored: findings.length, components };
}

/** The descriptions of the rules that have a finding, from our own table. */
export function rulesSeen(findings: LintFinding[], table: Readonly<Record<string, string>>): Record<string, string> {
  const out: Record<string, string> = {};
  for (const f of [...findings].sort((a, b) => a.rule.localeCompare(b.rule))) out[f.rule] = table[f.rule]!;
  return out;
}
