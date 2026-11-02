#!/usr/bin/env node
import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { parseArgs } from "node:util";
import { z } from "zod";
import { BUDGET_DEFAULTS, envNumber } from "../config.ts";
import { SALES_CLOUD_MODULES } from "../domains/index.ts";
import { runHealthCheck } from "../engine/run.ts";
import { Calibration, ManualAnswer } from "../schemas/index.ts";
import { BudgetStopError } from "../salesforce/budget.ts";
import { SfCliConnectionProvider } from "../salesforce/sf-cli-provider.ts";

const USAGE = `Black Cloud Barometer

  hc run --org <alias> --calibration <file> [options]

Options:
  --answers <file>     Manual step answers (JSON array)
  --out <dir>          Where runs are written. Default: ~/.barometer/runs
  --resume <run id>    Carry on a run that stopped at its API budget
  --budget-share <n>   Stop at this share of the org's daily API limit, 0 to 1
  --api-version <v>    Pin a Salesforce API version, such as 66.0

The org alias must be listed in HC_APPROVED_ORG_ALIASES.

Exit codes: 0 finished, 2 refused or failed, 3 stopped at the API budget.`;

export const EXIT = { ok: 0, failed: 2, budget: 3 } as const;

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
      calibration: { type: "string" },
      answers: { type: "string" },
      out: { type: "string" },
      resume: { type: "string" },
      "budget-share": { type: "string" },
      "api-version": { type: "string" },
      help: { type: "boolean", short: "h" },
    },
  });

  if (values.help || positionals[0] !== "run" || !values.org || !values.calibration) {
    console.log(USAGE);
    return values.help ? EXIT.ok : EXIT.failed;
  }

  const calibration = Calibration.parse(readJsonFile(values.calibration));
  const answers = values.answers ? z.array(ManualAnswer).parse(readJsonFile(values.answers)) : [];
  const stopAtShare = values["budget-share"] ? Number(values["budget-share"]) : envNumber("HC_API_BUDGET_SHARE", BUDGET_DEFAULTS.stopAtShare);

  const { run, dir, output } = await runHealthCheck({
    provider: new SfCliConnectionProvider(values.org),
    calibration,
    modules: SALES_CLOUD_MODULES,
    outDir: values.out ?? join(homedir(), ".barometer", "runs"),
    answers,
    resume: values.resume,
    stopAtShare,
    apiVersion: values["api-version"] ?? process.env.HC_SF_API_VERSION,
    log: (message) => console.log(message),
  });

  if (run.status === "stopped_budget") {
    console.log(`\nStopped: ${run.budget.stoppedReason}\nResume with: hc run --org ${values.org} --calibration ${values.calibration} --resume ${run.id}`);
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
