import { BudgetStopError } from "../salesforce/budget.ts";
import { lintJavaScript, type EslintOptions } from "./lint/eslint.ts";
import { notRun, type ToolRun } from "./lint/findings.ts";
import { PMD_NOT_INSTALLED, runPmd, type ApexFile, type PmdInstall, type PmdOptions } from "./lint/pmd.ts";
import { readSoql, type Extractor } from "./types.ts";

// PMD and ESLint beside our own code scan (DECISIONS.md 670). It only stores:
// no finding, score or report text reads it yet. It reads the Apex code again
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

/** The error's class name only: its message could quote what a tool read. */
const className = (err: unknown): string => (err instanceof Error ? err.constructor.name : typeof err);

export const codeLintExtractor: Extractor<CodeLintData> = {
  id: "code-lint",
  version: 1,
  async run(ctx) {
    // Nothing reads JavaScript today, so ESLint is handed none and says so.
    const eslint = await lintJavaScript([], ctx.codeLint?.eslintOptions);
    const tools = ctx.codeLint;
    if (!tools?.pmd) return { pmd: notRun("pmd", tools?.pmdMissing ?? PMD_NOT_INSTALLED), eslint };
    const classes = await readSoql<{ Name: string; Body?: string }>(ctx, CLASSES);
    const triggers = await readSoql<{ Name: string; Body?: string }>(ctx, TRIGGERS);
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
      if (err instanceof BudgetStopError) throw err;
      pmd = notRun("pmd", `PMD couldn't be run here (${className(err)}).`);
    }
    return { pmd: pmd.status === "ran" ? { ...pmd, limits: [...pmd.limits, ...limits] } : pmd, eslint };
  },
};
