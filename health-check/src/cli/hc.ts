#!/usr/bin/env node
import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { parseArgs } from "node:util";
import { z } from "zod";
import { CONSENT_FILE, defaultBenchmarkDir, exportBenchmark, Industry } from "../benchmark/export.ts";
import { formatSummary, summariseBenchmarks } from "../benchmark/summary.ts";
import { BUDGET_DEFAULTS, envNumber } from "../config.ts";
import { ACCOUNT_ENGAGEMENT_MODULES, CPQ_MODULES, SALES_CLOUD_MODULES, SERVICE_CLOUD_MODULES } from "../domains/index.ts";
import { runHealthCheck } from "../engine/run.ts";
import { defaultRunsDir, purgeRun, sweepRuns } from "../retention/purge.ts";
import { Calibration, HeadcountBand, ManualAnswer } from "../schemas/index.ts";
import { BudgetStopError } from "../salesforce/budget.ts";
import { SfCliConnectionProvider } from "../salesforce/sf-cli-provider.ts";
import type { ConnectionProvider } from "../salesforce/connection.ts";
import { OAuthConnectionProvider } from "../salesforce/oauth-provider.ts";
import { CONNECT_USAGE, connectCommand, connectionStore, oauthClient } from "./connect.ts";
import { WORKER_USAGE, workerCommand } from "./worker.ts";

const USAGE = `Black Cloud Barometer

  hc run --org <alias> --calibration <file> [options]
  hc run --connection <name> --calibration <file> [options]
      --service adds Service Cloud's two areas to the Sales Cloud ones
      --cpq adds Salesforce CPQ's two areas
      --pardot adds Marketing Cloud Account Engagement's area
  hc render --run <run folder> [--narrative]
  hc benchmark --run <run folder> --consent "<who agreed and how>" [--industry <industry>] [--headcount <band>] [--out <dir>]
  hc benchmark --summary [--out <dir>]
  hc purge --run <run folder>
  hc purge --older-than <days> [--runs <dir>]
${CONNECT_USAGE.trimEnd()}
${WORKER_USAGE.trimEnd()}

Options:
  --connection <name>  Run through a stored OAuth connection instead of a CLI alias
  --answers <file>     Manual step answers (JSON array)
  --out <dir>          Where runs are written. Default: ~/.barometer/runs
                       For benchmark, the benchmark set's folder. Default: ~/.barometer/benchmarks
  --resume <run id>    Carry on a run that stopped at its API budget
  --budget-share <n>   Stop at this share of the org's daily API limit, 0 to 1
  --api-version <v>    Pin a Salesforce API version, such as 66.0
  --run <folder>       The run folder to render, export or purge
  --narrative          Have Claude draft sections. Needs ANTHROPIC_API_KEY
  --industry <name>    Overrides the intake. ${Industry.options.join(", ")}
  --headcount <band>   Overrides the intake. LinkedIn company size: ${HeadcountBand.options.join(", ")}
  --consent <text>     Who at the customer agreed to share anonymised figures, and how
  --summary            Count benchmark rows by metric and group. No values are shown
  --older-than <days>  Purge every run whose run date is more than this many days ago
  --runs <dir>         The runs folder to sweep. Default: ~/.barometer/runs

The org alias or connection name must be listed in HC_APPROVED_ORG_ALIASES.

benchmark adds one run's figures to the benchmark set, once, and only with the
customer's consent, which is recorded in the run folder. It needs the run's raw
extracts, so export before the run is purged.

purge deletes a run's raw extracts (checkpoints/) and keeps findings, metrics,
scores, reports and the pseudonym map, so reports can still be rendered.

Exit codes: 0 finished, 2 refused or failed, 3 stopped at the API budget.`;

export const EXIT = { ok: 0, failed: 2, budget: 3 } as const;

function optionalClient(): ReturnType<typeof oauthClient> | undefined {
  try {
    return oauthClient();
  } catch {
    return undefined;
  }
}

function readJsonFile(path: string): unknown {
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch (err) {
    throw new Error(`Couldn't read ${path}: ${err instanceof Error ? err.message : err}`);
  }
}

async function main(argv: string[]): Promise<number> {
  const { values, positionals } = parseArgs({
    args: argv,
    allowPositionals: true,
    options: {
      org: { type: "string" },
      connection: { type: "string" },
      "login-url": { type: "string" },
      "callback-url": { type: "string" },
      config: { type: "string" },
      calibration: { type: "string" },
      answers: { type: "string" },
      out: { type: "string" },
      resume: { type: "string" },
      "budget-share": { type: "string" },
      service: { type: "boolean" },
      cpq: { type: "boolean" },
      pardot: { type: "boolean" },
      "api-version": { type: "string" },
      run: { type: "string" },
      narrative: { type: "boolean" },
      industry: { type: "string" },
      headcount: { type: "string" },
      consent: { type: "string" },
      summary: { type: "boolean" },
      "older-than": { type: "string" },
      runs: { type: "string" },
      help: { type: "boolean", short: "h" },
    },
  });

  if (positionals[0] === "benchmark" && !values.help) {
    if (values.summary) {
      for (const line of formatSummary(summariseBenchmarks(values.out ?? defaultBenchmarkDir()))) console.log(line);
      return EXIT.ok;
    }
    if (!values.run) {
      console.log(USAGE);
      return EXIT.failed;
    }
    const result = exportBenchmark({ runDir: values.run, industry: values.industry, headcount: values.headcount, consent: values.consent, storeDir: values.out });
    const band = result.rows[0]!;
    console.log(`Added ${result.rows.length} rows to ${result.storePath}: users ${band.userBand}, deal size ${band.dealSizeBand}, ${band.period}.`);
    for (const s of result.skipped) console.log(`  Left out ${s.metricKey}: ${s.reason}`);
    console.log(`Consent recorded in ${join(values.run, CONSENT_FILE)}`);
    return EXIT.ok;
  }

  if (positionals[0] === "purge" && !values.help) {
    if (values.run) {
      const result = purgeRun(values.run);
      console.log(result.purged ? `Purged ${result.runId}: ${result.filesDeleted} raw extract files deleted.` : `${result.runId} was ${result.note}. Nothing deleted.`);
      return EXIT.ok;
    }
    if (values["older-than"] === undefined) {
      console.log(USAGE);
      return EXIT.failed;
    }
    const days = Number(values["older-than"]);
    const sweep = sweepRuns(values.runs ?? defaultRunsDir(), days);
    for (const p of sweep.purged) console.log(`Purged ${p.runId}: ${p.filesDeleted} raw extract files deleted.`);
    for (const s of sweep.skipped) console.log(`Skipped ${s.folder}: ${s.reason}`);
    console.log(`${sweep.purged.length} purged. ${sweep.kept} left alone, being newer than ${days} days or already purged.`);
    return EXIT.ok;
  }

  if (positionals[0] === "worker" && !values.help) return workerCommand();

  if (positionals[0] === "connect") return connectCommand(values.help ? undefined : positionals[1], values);

  if (positionals[0] === "render" && values.run) {
    // Loaded here so `hc run` never needs Chromium or the Claude SDK.
    const { renderRun } = await import("../render/render-run.ts");
    const client = values.narrative ? (await import("../narrative/client.ts")).anthropicClient() : undefined;
    const result = await renderRun(values.run, { client });
    for (const [name, layout] of Object.entries(result.layout)) {
      console.log(`${name}: ${layout.pages} pages`);
      for (const p of layout.problems) console.log(`  page ${p.page}, ${p.check}: ${p.detail}`);
    }
    for (const r of result.rejections) console.log(`Kept the engine's text for ${r.sectionId}: ${r.reasons.join(" ")}`);
    console.log(`Written to ${values.run}`);
    return result.passed ? EXIT.ok : EXIT.failed;
  }

  // Exactly one of --org and --connection.
  if (values.help || positionals[0] !== "run" || !values.org === !values.connection || !values.calibration) {
    console.log(USAGE);
    return values.help ? EXIT.ok : EXIT.failed;
  }

  const calibration = Calibration.parse(readJsonFile(values.calibration));
  const answers = values.answers ? z.array(ManualAnswer).parse(readJsonFile(values.answers)) : [];
  const stopAtShare = values["budget-share"] ? Number(values["budget-share"]) : envNumber("HC_API_BUDGET_SHARE", BUDGET_DEFAULTS.stopAtShare);

  // A web server connection refreshes with the app's secret, so the OAuth client
  // is read if there is one. A JWT bearer connection doesn't need it.
  const provider: ConnectionProvider = values.connection
    ? new OAuthConnectionProvider(values.connection, { store: connectionStore(), client: optionalClient() })
    : new SfCliConnectionProvider(values.org!);
  const target = values.connection ? `--connection ${values.connection}` : `--org ${values.org}`;

  const { run, dir, output } = await runHealthCheck({
    provider,
    calibration,
    modules: [...SALES_CLOUD_MODULES, ...(values.service ? SERVICE_CLOUD_MODULES : []), ...(values.cpq ? CPQ_MODULES : []), ...(values.pardot ? ACCOUNT_ENGAGEMENT_MODULES : [])],
    outDir: values.out ?? join(homedir(), ".barometer", "runs"),
    answers,
    resume: values.resume,
    stopAtShare,
    apiVersion: values["api-version"] ?? process.env.HC_SF_API_VERSION,
    log: (message) => console.log(message),
  });

  if (run.status === "stopped_budget") {
    console.log(`\nStopped: ${run.budget.stoppedReason}\nResume with: hc run ${target} --calibration ${values.calibration} --resume ${run.id}`);
    return EXIT.budget;
  }
  const issues = output!.findings.filter((f) => f.status === "issue").length;
  const blocked = output!.findings.filter((f) => f.status === "not_measurable").length;
  const pending = output!.manualSteps.filter((s) => s.status === "pending").length;
  console.log(`\nRun ${run.id}: ${issues} issues, ${blocked} not measurable, ${pending} manual steps outstanding.\nWritten to ${dir}`);
  return EXIT.ok;
}

main(process.argv.slice(2)).then(
  (code) => {
    process.exitCode = code;
  },
  (err) => {
    // Our own errors only ever hold codes and messages, never a token.
    console.error(err instanceof BudgetStopError ? `Stopped: ${err.message}` : `Failed: ${err instanceof Error ? err.message : err}`);
    process.exitCode = err instanceof BudgetStopError ? EXIT.budget : EXIT.failed;
  },
);
