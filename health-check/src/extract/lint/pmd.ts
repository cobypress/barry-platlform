import { spawn } from "node:child_process";
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { fileURLToPath } from "node:url";
import { redactSecrets } from "../../privacy/secrets.ts";
import { bound, MAX_SOURCE_CHARS, notRun, rulesSeen, type LintFinding, type ToolRun } from "./findings.ts";

// PMD over the org's Apex, when the worker has it (DECISIONS.md 670). PMD is
// Java, and Render's Node runtime has none, so the worker finds both through
// its own variables, and a worker without them says so instead of reporting
// nothing found. PMD sees the code only in a private folder that never outlives
// the call, with our ruleset alone and none of the worker's environment.

export const PMD_VERSION = "7.29.0";
export const PMD_NOT_INSTALLED = "PMD isn't installed on this worker.";
/** Our ruleset, the only one PMD is ever given. */
export const PMD_RULESET = fileURLToPath(new URL("./pmd-apex.xml", import.meta.url));
export const PMD_TIME_LIMIT_MS = 5 * 60_000;
export const PMD_MAX_REPORT_BYTES = 20 * 1024 * 1024;
/** Each temporary folder's name starts with this, so a sweep after a crash finds it. */
export const LINT_TEMP_PREFIX = "hc-lint-";

/** Our own description of each rule in the ruleset, in its order. A test holds this to the XML. */
export const PMD_RULES: Readonly<Record<string, string>> = {
  ApexUnitTestClassShouldHaveAsserts: "A test method that checks nothing with an assertion.",
  ApexUnitTestMethodShouldHaveIsTestAnnotation: "A test method without @isTest.",
  ApexUnitTestShouldNotUseSeeAllDataTrue: "A test that reads the org's live data (SeeAllData=true).",
  AvoidGlobalModifier: "Code marked global, which can't be changed freely once published.",
  AvoidLogicInTrigger: "Logic written in the trigger itself, rather than in a handler class.",
  UnusedLocalVariable: "A local variable that is never used.",
  AvoidDeeplyNestedIfStmts: "If statements nested too deeply to follow easily.",
  CognitiveComplexity: "A method that is hard to follow.",
  CyclomaticComplexity: "A method or class with many paths through it.",
  ExcessiveParameterList: "A method with too many parameters.",
  NcssCount: "A method or class with too many statements.",
  ApexCSRF: "Data changed while a page loads, open to cross-site request forgery.",
  AvoidDirectAccessTriggerMap: "Trigger records read by fixed position, so only the first of a batch is handled.",
  AvoidHardcodingId: "A record id written into the code, which differs between orgs.",
  AvoidNonExistentAnnotations: "An annotation Apex doesn't have.",
  EmptyCatchBlock: "An error caught and ignored.",
  EmptyIfStmt: "An if statement that does nothing.",
  EmptyStatementBlock: "A block of code that does nothing.",
  EmptyTryOrFinallyBlock: "A try or finally block that does nothing.",
  EmptyWhileStmt: "A while loop that does nothing.",
  MethodWithSameNameAsEnclosingClass: "A method named like its class, easily mistaken for a constructor.",
  OverrideBothEqualsAndHashcode: "equals or hashCode overridden without the other.",
  TestMethodsMustBeInTestClasses: "A test method outside a test class.",
  AvoidNonRestrictiveQueries: "A query with no filter and no limit.",
  EagerlyLoadedDescribeSObjectResult: "A describe that loads every field up front.",
  OperationWithHighCostInLoop: "A costly operation inside a loop.",
  OperationWithLimitsInLoop: "A query, DML or other limited operation inside a loop.",
  ApexBadCrypto: "Encryption with a fixed key or vector written into the code.",
  ApexCRUDViolation: "Data read or changed without checking the user's object permissions.",
  ApexDangerousMethods: "A method that can weaken security if misused.",
  ApexInsecureEndpoint: "A callout over plain http.",
  ApexOpenRedirect: "A redirect to an address the user can supply.",
  ApexSharingViolations: "A class that reads or changes data without declaring its sharing.",
  ApexSOQLInjection: "A query built from text the user can supply.",
  ApexSuggestUsingNamedCred: "Credentials handled in code instead of a named credential.",
  ApexXSSFromEscapeFalse: "Output with escaping turned off.",
  ApexXSSFromURLParam: "A page parameter used without escaping.",
};

export type PmdInstall = { bin: string; javaHome: string };

/**
 * PMD as the worker's variables point to it: HC_PMD_HOME is the unpacked
 * pmd-bin folder, which must hold exactly our version's jars, and
 * HC_JAVA_HOME the Java it runs on. Files are only looked at, never run.
 */
export function findPmd(pmdHome: string | null | undefined, javaHome: string | null | undefined): { found: PmdInstall } | { missing: string } {
  if (!pmdHome) return { missing: PMD_NOT_INSTALLED };
  const bin = join(pmdHome, "bin", "pmd");
  const jars = [`pmd-core-${PMD_VERSION}.jar`, `pmd-apex-${PMD_VERSION}.jar`].map((j) => join(pmdHome, "lib", j));
  if (!existsSync(bin)) return { missing: `${PMD_NOT_INSTALLED} HC_PMD_HOME has no bin/pmd.` };
  if (!jars.every((j) => existsSync(j))) return { missing: `The PMD on this worker isn't version ${PMD_VERSION}, the one we run.` };
  if (!javaHome || !existsSync(join(javaHome, "bin", "java"))) return { missing: "PMD is on this worker, but Java isn't: HC_JAVA_HOME has no bin/java." };
  return { found: { bin, javaHome } };
}

/** A class's or trigger's code, as the step read it. */
export type ApexFile = { kind: "class" | "trigger"; name: string; body: string };

export type PmdOptions = {
  timeLimitMs?: number;
  maxReportBytes?: number;
  /** Where the temporary folder goes. The system's by default. */
  tempBase?: string;
};

/**
 * Secrets out, every line kept where it was, so PMD's line numbers are the
 * code's: a private key block becomes one marker and the blank lines it spanned.
 */
export function redactKeepingLines(body: string): string {
  const blocks = body.replace(/-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----/g, (m) => `[REDACTED]${"\n".repeat(m.split("\n").length - 1)}`);
  return blocks.split("\n").map(redactSecrets).join("\n");
}

/**
 * Deletes temporary folders a crashed step left: only those older than
 * `olderThanMs`, so a run beside this one keeps the folder it is using.
 */
export function sweepLintTemp(base: string, olderThanMs: number, now = Date.now()): number {
  let swept = 0;
  let names: string[];
  try {
    names = readdirSync(base);
  } catch {
    return 0;
  }
  for (const name of names) {
    if (!name.startsWith(LINT_TEMP_PREFIX)) continue;
    const path = join(base, name);
    try {
      if (now - statSync(path).mtimeMs > olderThanMs) {
        rmSync(path, { recursive: true, force: true });
        swept++;
      }
    } catch {
      // Gone already, or not ours to read.
    }
  }
  return swept;
}

type PmdViolation = { beginline?: unknown; begincolumn?: unknown; rule?: unknown; ruleset?: unknown; priority?: unknown };
type PmdFile = { filename?: unknown; violations?: unknown };
type PmdReport = { pmdVersion?: unknown; files?: unknown; suppressedViolations?: unknown; processingErrors?: unknown; configurationErrors?: unknown };

class PmdStopped extends Error {}

/** Runs PMD and resolves when it has exited, or rejects with why it was stopped. Its console is drained, never kept. */
function runProcess(install: PmdInstall, args: string[], dir: string, report: string, timeLimitMs: number, maxReportBytes: number): Promise<number | null> {
  const javaTmp = join(dir, "java-tmp");
  mkdirSync(javaTmp, { mode: 0o700 });
  // PMD's script splits these on spaces, so a folder with a space keeps Java's default.
  const tmpOption = /\s/.test(javaTmp) ? [] : [`-Djava.io.tmpdir=${javaTmp}`];
  // A connection PMD tries goes to a closed local port and fails.
  const noNetwork = ["socksProxyHost", "http.proxyHost", "https.proxyHost"].flatMap((p) => [`-D${p}=127.0.0.1`, `-D${p.replace("Host", "Port")}=9`]);
  const env = {
    PATH: `${join(install.javaHome, "bin")}:/usr/bin:/bin`,
    HOME: dir,
    TMPDIR: dir,
    LANG: "C.UTF-8",
    PMD_JAVA_OPTS: ["-Xmx512m", ...tmpOption, ...noNetwork].join(" "),
  };
  return new Promise((resolve, reject) => {
    const child = spawn(install.bin, args, { cwd: dir, env, detached: true, stdio: ["ignore", "pipe", "pipe"] });
    child.stdout.resume();
    child.stderr.resume();
    let stopped: string | null = null;
    const stop = (why: string): void => {
      if (stopped) return;
      stopped = why;
      try {
        // The whole group: the script, Java, and anything either started.
        process.kill(-child.pid!, "SIGKILL");
      } catch {
        child.kill("SIGKILL");
      }
    };
    const timer = setTimeout(() => stop(`PMD didn't finish within ${Math.round(timeLimitMs / 1000)} seconds, so it was stopped.`), timeLimitMs);
    const watch = setInterval(() => {
      try {
        if (statSync(report).size > maxReportBytes) stop(`PMD's report grew past ${Math.round(maxReportBytes / 1024)} KB, so it was stopped.`);
      } catch {
        // Not written yet.
      }
    }, 100);
    child.on("error", (err) => {
      clearTimeout(timer);
      clearInterval(watch);
      reject(new PmdStopped(`PMD couldn't be started (${(err as NodeJS.ErrnoException).code ?? err.name}).`));
    });
    child.on("close", (code) => {
      clearTimeout(timer);
      clearInterval(watch);
      if (stopped) reject(new PmdStopped(stopped));
      else resolve(code);
    });
  });
}

const whole = (v: unknown): number | null => (typeof v === "number" && Number.isInteger(v) && v > 0 ? v : null);

/**
 * PMD over the files, with our ruleset, in a private folder deleted before
 * this returns, whatever happened. Never throws for anything PMD does: it is
 * then not run, with why.
 */
export async function runPmd(files: ApexFile[], install: PmdInstall, options: PmdOptions = {}): Promise<ToolRun> {
  const timeLimitMs = options.timeLimitMs ?? PMD_TIME_LIMIT_MS;
  const maxReportBytes = options.maxReportBytes ?? PMD_MAX_REPORT_BYTES;
  const base = options.tempBase ?? tmpdir();
  sweepLintTemp(base, 2 * timeLimitMs);
  const skipped = { tooLarge: 0, unparsable: 0, unreadable: 0, failed: 0, timeLimit: 0 };
  const handed: ApexFile[] = [];
  for (const file of files) {
    if (!file.body || file.body === "(hidden)") skipped.unreadable++;
    else if (file.body.length > MAX_SOURCE_CHARS) skipped.tooLarge++;
    else handed.push(file);
  }
  if (handed.length === 0) return notRun("pmd", files.length === 0 ? "There's no Apex code in this org to check." : "None of the org's Apex code could be handed to PMD: each class and trigger was unreadable or too large.");

  const dir = mkdtempSync(join(base, LINT_TEMP_PREFIX));
  try {
    chmodSync(dir, 0o700);
    const src = join(dir, "src");
    mkdirSync(src, { mode: 0o700 });
    // Numbered, never named: what PMD sees of a name is the code itself.
    handed.forEach((file, i) => writeFileSync(join(src, `${i}.${file.kind === "class" ? "cls" : "trigger"}`), redactKeepingLines(file.body), { mode: 0o600 }));
    const report = join(dir, "report.json");
    const args = ["check", "--dir", src, "--rulesets", PMD_RULESET, "--format", "json", "--report-file", report, "--no-cache", "--no-progress", "--show-suppressed", "--no-fail-on-violation", "--no-fail-on-error", "--threads", "1"];
    let code: number | null;
    try {
      code = await runProcess(install, args, dir, report, timeLimitMs, maxReportBytes);
    } catch (err) {
      if (err instanceof PmdStopped) return notRun("pmd", err.message);
      throw err;
    }
    if (code !== 0) return notRun("pmd", `PMD stopped with exit code ${code ?? "none"}, so its findings weren't read.`);
    if (!existsSync(report)) return notRun("pmd", "PMD finished without writing its report.");
    if (statSync(report).size > maxReportBytes) return notRun("pmd", `PMD's report was larger than ${Math.round(maxReportBytes / 1024)} KB, so it wasn't read.`);
    let parsed: PmdReport;
    try {
      parsed = JSON.parse(readFileSync(report, "utf8")) as PmdReport;
    } catch {
      return notRun("pmd", "PMD's report couldn't be read.");
    }
    if (parsed.pmdVersion !== PMD_VERSION) return notRun("pmd", `The report came from a PMD other than version ${PMD_VERSION}, so it was set aside.`);
    if (Array.isArray(parsed.configurationErrors) && parsed.configurationErrors.length > 0) return notRun("pmd", "PMD didn't accept our ruleset, so its findings weren't read.");

    const fileOf = (name: unknown): ApexFile | undefined => {
      if (typeof name !== "string") return undefined;
      const m = /^(\d+)\.(cls|trigger)$/.exec(basename(name));
      return m ? handed[Number(m[1])] : undefined;
    };
    const raw: LintFinding[] = [];
    let outsideRules = 0;
    const take = (groups: unknown, suppressed: boolean): void => {
      if (!Array.isArray(groups)) return;
      for (const group of groups as PmdFile[]) {
        const file = fileOf(group?.filename);
        if (!file || !Array.isArray(group.violations)) continue;
        for (const v of group.violations as PmdViolation[]) {
          const line = whole(v?.beginline);
          if (typeof v?.rule !== "string" || line === null) continue;
          // Only our rules: anything else PMD reports is dropped, and counted.
          if (!Object.hasOwn(PMD_RULES, v.rule)) {
            outsideRules++;
            continue;
          }
          raw.push({
            tool: "pmd",
            toolVersion: PMD_VERSION,
            rule: v.rule,
            category: typeof v.ruleset === "string" ? v.ruleset : null,
            severity: whole(v.priority) ?? 3,
            kind: file.kind,
            component: file.name,
            line,
            column: whole(v.begincolumn),
            ...(suppressed ? { suppressed: true as const } : {}),
          });
        }
      }
    };
    take(parsed.files, false);
    take(parsed.suppressedViolations, true);
    // A file PMD couldn't parse is counted; its error text quotes the code, so it goes no further.
    const unparsable = new Set<ApexFile>();
    if (Array.isArray(parsed.processingErrors)) for (const e of parsed.processingErrors as Array<{ filename?: unknown }>) {
      const file = fileOf(e?.filename);
      if (file) unparsable.add(file);
    }
    skipped.unparsable = unparsable.size;
    const kept = bound(raw);
    return {
      status: "ran",
      tool: "pmd",
      toolVersion: PMD_VERSION,
      checked: handed.length - unparsable.size,
      skipped,
      found: kept.found,
      stored: kept.stored,
      components: kept.components,
      findings: kept.findings,
      rules: rulesSeen(kept.findings, PMD_RULES),
      outsideRules,
      limits: [],
    };
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}
