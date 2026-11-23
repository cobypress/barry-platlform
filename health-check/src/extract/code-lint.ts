import { BudgetStopError } from "../salesforce/budget.ts";
import { lintJavaScript, type EslintOptions } from "./lint/eslint.ts";
import { notRun, type ToolRun } from "./lint/findings.ts";
import { PMD_NOT_INSTALLED, runPmd, type ApexFile, type PmdInstall, type PmdOptions } from "./lint/pmd.ts";
import { readSoql, type Extractor, type Sourced } from "./types.ts";

// PMD and ESLint beside our own code scan (DECISIONS.md 670). Six groups of
// PMD's rules are shown in technical health, unscored, and every rule's counts
// are kept for the benchmark (673). It reads the Apex code again
// itself, and only when PMD is on the worker, because the Apex step drops each
// body as it reads it and a resumed run reuses that step's checkpoint.

/** What the worker gives a health check: PMD where it found it, or why not. Never read from the environment here. */
export type CodeLintTools = {
  pmd: PmdInstall | null;
  /** Why there's no PMD, when there isn't. */
  pmdMissing?: string;
  /** Tests only. */
  pmdOptions?: PmdOptions;
  eslintOptions?: EslintOptions;
};

export type CodeLintData = { pmd: ToolRun; eslint: ToolRun };

const CLASSES = "SELECT Id, Name, Body FROM ApexClass WHERE NamespacePrefix = null";
const TRIGGERS = "SELECT Id, Name, Body FROM ApexTrigger WHERE NamespacePrefix = null";
/** Where the code PMD read came from, for the findings' evidence. */
export const LINT_SOURCES = { classes: { kind: "soql", api: "rest", query: CLASSES }, triggers: { kind: "soql", api: "rest", query: TRIGGERS } } as const;

/**
 * The calls the step must have room for before it reads anything: this many
 * more must fit under both the run's cap and the org's stop share. Reading the
 * code takes one call a page, a few for most orgs; the rest is headroom, so a
 * step that only stores never brings a customer's run near its stop for the
 * facts step after it (DECISIONS.md 670).
 */
export const LINT_BUDGET_ROOM = 200;
const BUDGET_SHORT = "The run's API budget hadn't clear room for reading the Apex code again, so PMD didn't run. Nothing else in the run is affected.";
const BUDGET_HIT = "The run's API budget ran short while the Apex code was read again, so PMD didn't run. Nothing else in the run is affected.";

/** The error's class name only: its message could quote what a tool read. */
const className = (err: unknown): string => (err instanceof Error ? err.constructor.name : typeof err);

export const codeLintExtractor: Extractor<CodeLintData> = {
  id: "code-lint",
  // 2: every rule's count in every component, uncapped, and what PMD read (DECISIONS.md 673),
  // so a version 1 checkpoint, which has neither, is read again rather than reused.
  version: 2,
  async run(ctx) {
    // Nothing reads JavaScript today, so ESLint is handed none and says so.
    const eslint = await lintJavaScript([], ctx.codeLint?.eslintOptions);
    const tools = ctx.codeLint;
    if (!tools?.pmd) return { pmd: notRun("pmd", tools?.pmdMissing ?? PMD_NOT_INSTALLED), eslint };
    // A step that only stores never pauses a customer's run at its budget:
    // without clear room it doesn't read, and a stop part way is its own not run.
    try {
      ctx.client.budget.assertCanSpend(LINT_BUDGET_ROOM);
    } catch (err) {
      if (err instanceof BudgetStopError) return { pmd: notRun("pmd", BUDGET_SHORT), eslint };
      throw err;
    }
    let classes: Sourced<Array<{ Name: string; Body?: string }>>;
    let triggers: Sourced<Array<{ Name: string; Body?: string }>>;
    try {
      classes = await readSoql<{ Name: string; Body?: string }>(ctx, CLASSES);
      triggers = await readSoql<{ Name: string; Body?: string }>(ctx, TRIGGERS);
    } catch (err) {
      if (err instanceof BudgetStopError) return { pmd: notRun("pmd", BUDGET_HIT), eslint };
      throw err;
    }
    if (classes.status !== "ok" && triggers.status !== "ok") return { pmd: notRun("pmd", `The Apex code couldn't be read: ${classes.reason}`), eslint };
    const files: ApexFile[] = [
      ...(classes.status === "ok" ? classes.data.map((r) => ({ kind: "class" as const, name: r.Name, body: r.Body ?? "" })) : []),
      ...(triggers.status === "ok" ? triggers.data.map((r) => ({ kind: "trigger" as const, name: r.Name, body: r.Body ?? "" })) : []),
    ];
    const limits = [
      ...(classes.status !== "ok" ? [`Apex classes couldn't be read, so only triggers were checked: ${classes.reason}`] : []),
      ...(triggers.status !== "ok" ? [`Apex triggers couldn't be read, so only classes were checked: ${triggers.reason}`] : []),
      ...(classes.status === "ok" && classes.truncated ? ["Not every Apex class could be read, so some weren't checked."] : []),
      ...(triggers.status === "ok" && triggers.truncated ? ["Not every Apex trigger could be read, so some weren't checked."] : []),
    ];
    let pmd: ToolRun;
    try {
      pmd = await runPmd(files, tools.pmd, tools.pmdOptions);
    } catch (err) {
      // The run never fails because a linter did.
      pmd = notRun("pmd", `PMD couldn't be run here (${className(err)}).`);
    }
    if (pmd.status !== "ran") return { pmd, eslint };
    // A kind is unread when its query failed, or the org has some and PMD read none of them (DECISIONS.md 673, fix round 1).
    const had = { class: classes.status === "ok" ? classes.data.length : null, trigger: triggers.status === "ok" ? triggers.data.length : null };
    const readOf = { class: pmd.read?.classes ?? 0, trigger: pmd.read?.triggers ?? 0 };
    const unread = (["class", "trigger"] as const).filter((k) => had[k] === null || (had[k]! > 0 && readOf[k] === 0));
    return { pmd: { ...pmd, limits: [...pmd.limits, ...limits], unread }, eslint };
  },
};
