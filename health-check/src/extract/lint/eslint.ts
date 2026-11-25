import { eslintTimeLimit } from "../../salesforce/refusal-words.ts";
import { bound, MAX_SOURCE_CHARS, notRun, rulesSeen, type LintFinding, type LintKind, type ToolRun } from "./findings.ts";

// ESLint over a Lightning component's JavaScript (DECISIONS.md 670). In
// process, through its Linter class, with the configuration written here: it
// never looks for a configuration file, loads no plugin and no parser but its
// own, and runs nothing in the code. Inline configuration is off, so the code
// can neither switch our rules off nor switch others on. Today no run reads any
// JavaScript, so the step hands it nothing and says so.

export const ESLINT_VERSION = "10.14.0";
export const NO_JAVASCRIPT = "No JavaScript is read from the org today: the health check doesn't retrieve Lightning web components or Aura components, and reads JavaScript buttons by name and type only.";
export const ESLINT_TIME_LIMIT_MS = 60_000;

/** Our own description of each rule, and the severity we give it. */
export const ESLINT_RULES: Readonly<Record<string, { severity: 1 | 2; description: string }>> = {
  "no-eval": { severity: 2, description: "Text run as code with eval." },
  "no-implied-eval": { severity: 2, description: "Text run as code through a timer." },
  "no-new-func": { severity: 2, description: "Text run as code through new Function." },
  "no-script-url": { severity: 2, description: "A javascript: address." },
  "no-unsafe-finally": { severity: 2, description: "A finally block that overrides what the try returned or threw." },
  "no-unsafe-negation": { severity: 2, description: "A negation that applies to the wrong part of a test." },
  "no-unsafe-optional-chaining": { severity: 2, description: "Optional chaining where an undefined result throws." },
  "no-async-promise-executor": { severity: 2, description: "An async function given to new Promise, which loses its errors." },
  "no-cond-assign": { severity: 2, description: "An assignment where a comparison was probably meant." },
  "no-dupe-keys": { severity: 2, description: "An object with the same key twice." },
  "no-dupe-else-if": { severity: 2, description: "An else-if that repeats an earlier condition." },
  "no-duplicate-case": { severity: 2, description: "A switch with the same case twice." },
  "no-self-assign": { severity: 2, description: "A variable assigned to itself." },
  "no-self-compare": { severity: 2, description: "A value compared with itself." },
  "no-unreachable": { severity: 2, description: "Code that can never run." },
  "use-isnan": { severity: 2, description: "A comparison with NaN, which is never equal to anything." },
  "valid-typeof": { severity: 2, description: "typeof compared with a type name that doesn't exist." },
  "no-debugger": { severity: 1, description: "A debugger statement left in." },
  "no-alert": { severity: 1, description: "A browser alert, confirm or prompt." },
  "no-unused-vars": { severity: 1, description: "A variable that is never used." },
  "no-constant-condition": { severity: 1, description: "A condition that is always true or always false." },
  "no-empty": { severity: 1, description: "A block that does nothing." },
  "no-await-in-loop": { severity: 1, description: "Waiting inside a loop, one call at a time." },
  eqeqeq: { severity: 1, description: "A loose == or != comparison." },
  "no-var": { severity: 1, description: "var instead of let or const." },
};

export type JsFile = { kind: LintKind; component: string; source: string };

type Message = { ruleId: string | null; severity: number; fatal?: boolean; line?: number; column?: number };
/** What we use of ESLint's Linter, so a test can stand in a linter that crashes. */
export type LinterLike = { verify(code: string, config: unknown, options: { filename: string }): Message[] };

export type EslintOptions = {
  timeLimitMs?: number;
  clock?: () => number;
  /** Tests only: stands in for ESLint's own Linter. */
  linter?: LinterLike;
};

/** The whole configuration ESLint is given. Nothing else ever reaches it. */
export const ESLINT_CONFIG = [
  {
    languageOptions: { ecmaVersion: "latest", sourceType: "module" },
    linterOptions: { noInlineConfig: true, reportUnusedDisableDirectives: "off", reportUnusedInlineConfigs: "off" },
    rules: Object.fromEntries(Object.entries(ESLINT_RULES).map(([id, r]) => [id, r.severity])),
  },
];

async function loadLinter(): Promise<LinterLike | string> {
  const { Linter } = await import("eslint");
  // Nothing but the version we pinned: another one's rules may differ.
  if (Linter.version !== ESLINT_VERSION) return `The ESLint on this worker is version ${Linter.version}, not ${ESLINT_VERSION}, the one we run.`;
  return new Linter() as unknown as LinterLike;
}

/** ESLint over each file, with our rules alone. Never throws for anything ESLint does. */
export async function lintJavaScript(files: JsFile[], options: EslintOptions = {}): Promise<ToolRun> {
  if (files.length === 0) return notRun("eslint", NO_JAVASCRIPT);
  let linter: LinterLike;
  try {
    const loaded = options.linter ?? (await loadLinter());
    if (typeof loaded === "string") return notRun("eslint", loaded);
    linter = loaded;
  } catch {
    return notRun("eslint", "ESLint couldn't be loaded on this worker.");
  }
  const clock = options.clock ?? Date.now;
  const limit = options.timeLimitMs ?? ESLINT_TIME_LIMIT_MS;
  const started = clock();
  const skipped = { tooLarge: 0, unparsable: 0, unreadable: 0, failed: 0, timeLimit: 0 };
  const raw: LintFinding[] = [];
  let checked = 0;
  let outsideRules = 0;
  let inlineConfigIgnored = 0;
  for (const [i, file] of files.entries()) {
    // ESLint runs in this process and can't be stopped part way through a
    // file, so the limit is checked between files, and the size bound keeps any one short.
    if (clock() - started > limit) {
      skipped.timeLimit = files.length - i;
      break;
    }
    if (!file.source) {
      skipped.unreadable++;
      continue;
    }
    if (file.source.length > MAX_SOURCE_CHARS) {
      skipped.tooLarge++;
      continue;
    }
    let messages: Message[];
    try {
      // A fixed file name: the component's name never reaches the tool.
      messages = linter.verify(file.source, ESLINT_CONFIG, { filename: "component.js" });
    } catch {
      skipped.failed++;
      continue;
    }
    if (messages.some((m) => m.fatal)) {
      skipped.unparsable++;
      continue;
    }
    checked++;
    for (const m of messages) {
      // With inline configuration off, ESLint says so for each comment that tried.
      if (m.ruleId === null) {
        inlineConfigIgnored++;
        continue;
      }
      if (!Object.hasOwn(ESLINT_RULES, m.ruleId)) {
        outsideRules++;
        continue;
      }
      if (typeof m.line !== "number" || m.line < 1) continue;
      raw.push({ tool: "eslint", toolVersion: ESLINT_VERSION, rule: m.ruleId, category: null, severity: m.severity, kind: file.kind, component: file.component, line: m.line, column: typeof m.column === "number" ? m.column : null });
    }
  }
  const kept = bound(raw);
  const descriptions = Object.fromEntries(Object.entries(ESLINT_RULES).map(([id, r]) => [id, r.description]));
  return {
    status: "ran",
    tool: "eslint",
    toolVersion: ESLINT_VERSION,
    checked,
    skipped,
    found: kept.found,
    stored: kept.stored,
    components: kept.components,
    findings: kept.findings,
    rules: rulesSeen(kept.findings, descriptions),
    outsideRules,
    inlineConfigIgnored,
    limits: skipped.timeLimit ? [eslintTimeLimit(Math.round(limit / 1000), skipped.timeLimit)] : [],
  };
}
