import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, readdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { assertOutsideGit, runHealthCheck } from "../run.ts";
import { SALES_CLOUD_MODULES } from "../../domains/index.ts";
import { runFromRecording, tempDir } from "../../testing/harness.ts";
import { FIXTURE_DIR, loadAnswers, loadCalibration, loadRecording, ReplayTransport, syntheticProvider } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { AS_OF, usersTable } from "../../testing/synthetic-org.ts";
import { Finding, ManualStep, Metric, Run } from "../../schemas/index.ts";
import { z } from "zod";

const read = <T>(dir: string, file: string): T => JSON.parse(readFileSync(join(dir, file), "utf8")) as T;
const service = dirname(dirname(FIXTURE_DIR.replace(/\/$/, "")));

describe("a full run", () => {
  test("writes the four outputs, and each validates", async () => {
    const { dir, run } = await runFromRecording();
    assert.ok(Run.parse(run));
    assert.ok(z.array(Finding).parse(read(dir, "findings.json")).length > 0);
    assert.ok(z.array(Metric).parse(read(dir, "metrics.json")).length > 0);
    assert.ok(z.array(ManualStep).parse(read(dir, "manual-steps.json")).length === 2);
    assert.ok(existsSync(join(dir, "raw-notes.md")));
  });

  test("names the customer from the org's company information, and masks it like a person", async () => {
    const { dir, run } = await runFromRecording();
    assert.equal(run.customerRef, "Synthetic Co");
    const map = read(dir, "pseudonym-map.json") as Array<{ token: string; name: string }>;
    assert.ok(map.some((e) => e.token.startsWith("[[customer:") && e.name === "Synthetic Co"));
  });

  test("waits on manual steps when any are outstanding", async () => {
    assert.equal((await runFromRecording()).run.status, "awaiting_manual");
    assert.equal((await runFromRecording({ answers: loadAnswers() })).run.status, "in_review");
  });

  test("no real name reaches findings, metrics or manual steps (rule 7)", async () => {
    const { dir } = await runFromRecording({ answers: loadAnswers() });
    const output = ["findings.json", "metrics.json", "manual-steps.json"].map((f) => readFileSync(join(dir, f), "utf8")).join("\n");
    for (const user of usersTable) {
      assert.ok(!output.includes(user.Id), `${user.Name}'s id leaked`);
      assert.ok(!output.includes(user.Username), `${user.Name}'s username leaked`);
      if (user.Name.includes(" ")) assert.ok(!output.includes(user.Name), `${user.Name} leaked`);
    }
    assert.doesNotMatch(output, /@example\.com/);
    assert.match(output, /\[\[user:U\d{4}\]\]/);
  });

  test("names come back only in the rendered notes", async () => {
    const { dir } = await runFromRecording({ answers: loadAnswers() });
    const notes = readFileSync(join(dir, "raw-notes.md"), "utf8");
    assert.match(notes, /Sam Reed/);
    assert.doesNotMatch(notes, /\[\[user:/);
  });

  test("the raw notes carry what the brief requires", async () => {
    const { dir } = await runFromRecording({ answers: loadAnswers() });
    const notes = readFileSync(join(dir, "raw-notes.md"), "utf8");
    for (const heading of ["## Assumptions", "## Manual steps", "## Issues", "## Pipeline metrics", "## Not measurable", "## Checks passed"]) assert.ok(notes.includes(heading), heading);
    assert.match(notes, /Query: `SELECT/);
    assert.match(notes, /excludes:/);
    assert.match(notes, /Calibration confirmed by Synthetic Consultant/);
    assert.match(notes, /Manual step ms-eac-advanced-sync \(answered, by Synthetic Consultant on 2026-10-30\)/);
    assert.match(notes, /\| owns-no-records \| failed \|/);
  });

  test("the same recording gives the same findings", async () => {
    const a = await runFromRecording({ answers: loadAnswers() });
    const b = await runFromRecording({ answers: loadAnswers() });
    assert.equal(readFileSync(join(a.dir, "findings.json"), "utf8"), readFileSync(join(b.dir, "findings.json"), "utf8"));
    assert.equal(readFileSync(join(a.dir, "metrics.json"), "utf8"), readFileSync(join(b.dir, "metrics.json"), "utf8"));
  });

  test("uses only what the allowlist permits, with no write method on the wire", async () => {
    const transport = new ReplayTransport(loadRecording());
    await runFromRecording({ transport });
    assert.ok(transport.requests.length > 20);
    for (const request of transport.requests) {
      assert.ok(request.method === "GET" || request.url.includes("/composite/batch") || request.url.includes("/Soap/m/"), request.url);
    }
  });

  test("Tooling queries never go through composite batch", async () => {
    const transport = new ReplayTransport(loadRecording());
    await runFromRecording({ transport });
    const batches = transport.requests.filter((r) => r.url.includes("/composite/batch"));
    assert.ok(batches.length > 0);
    for (const batch of batches) assert.doesNotMatch(batch.body!, /tooling/);
    assert.ok(transport.requests.some((r) => r.url.includes("/tooling/query")));
  });

  test("setup audit and login filters happen in code, not SOQL", async () => {
    const transport = new ReplayTransport(loadRecording());
    await runFromRecording({ transport });
    const queries = transport.requests.map((r) => decodeURIComponent(r.url));
    const audit = queries.find((q) => q.includes("FROM SetupAuditTrail"))!;
    const logins = queries.find((q) => q.includes("FROM LoginHistory"))!;
    assert.doesNotMatch(audit.split("WHERE")[1]!, /Section|Display|Action/);
    assert.doesNotMatch(logins.split("WHERE")[1]!, /Status|Application/);
  });

  test("the extract reaches a deal closed in the window with an old close date", async () => {
    const transport = new ReplayTransport(loadRecording());
    await runFromRecording({ transport });
    const query = transport.requests.map((r) => decodeURIComponent(r.url)).find((q) => q.includes("FROM Opportunity "))!;
    assert.match(query, /LastModifiedDate >= /);
    assert.match(query, /StageName IN \('Prospecting', 'Qualified', 'Discovery', 'Proposal', 'Negotiation', 'Legal Review'\)/);
  });

  test("run data is written privately", async () => {
    const { dir } = await runFromRecording();
    assert.equal(statSync(join(dir, "pseudonym-map.json")).mode & 0o077, 0);
    assert.equal(statSync(join(dir, "findings.json")).mode & 0o077, 0);
  });

  test("a recording with no matching query fails loudly", async () => {
    await assert.rejects(runFromRecording({ transport: new ReplayTransport({ note: "", exchanges: [] }) }), /No recording for/);
  });
});

describe("safety checks at the start of a run", () => {
  test("refuses a calibration written for another org", async () => {
    const calibration = { ...loadCalibration(), orgId: "00D000000000999AAA" };
    await assert.rejects(runFromRecording({ calibration }), /different org/);
  });

  test("won't write run data inside a git repository (rule 8)", async () => {
    await assert.rejects(runFromRecording({ outDir: join(service, "runs") }), /inside the git repository/);
  });

  test("assertOutsideGit walks up the folder tree", () => {
    const root = tempDir();
    mkdirSync(join(root, ".git"));
    assert.throws(() => assertOutsideGit(join(root, "a", "b")), /git repository/);
    assert.doesNotThrow(() => assertOutsideGit(tempDir()));
  });
});

describe("budget and resume (rule 9)", () => {
  const simulatorRun = (transport: Simulator, outDir: string, overrides = {}) =>
    runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir, now: new Date(AS_OF), transport, ...overrides });

  test("an org already over the share stops cleanly, with its place saved", async () => {
    const result = await simulatorRun(new Simulator({ used: 60_000 }), tempDir());
    assert.equal(result.run.status, "stopped_budget");
    assert.match(result.run.budget.stoppedReason!, /stops at 50%/);
    assert.equal(result.output, null);
    assert.equal(read<{ status: string }>(result.dir, "run.json").status, "stopped_budget");
  });

  test("the share is configurable", async () => {
    const result = await simulatorRun(new Simulator({ used: 60_000 }), tempDir(), { stopAtShare: 0.9 });
    assert.notEqual(result.run.status, "stopped_budget");
  });

  test("stops part-way, then resumes without repeating finished steps", async () => {
    const out = tempDir();
    const first = new Simulator({ used: 49_990, dailyLimit: 100_000 });
    const stopped = await simulatorRun(first, out);
    assert.equal(stopped.run.status, "stopped_budget");
    const done = stopped.run.steps.filter((s) => s.status === "done").map((s) => s.id);
    assert.ok(done.length > 0 && done.length < stopped.run.steps.length, `${done.length} of ${stopped.run.steps.length} steps done`);

    const second = new Simulator({ used: 1000 });
    const resumed = await simulatorRun(second, out, { resume: stopped.run.id, answers: loadAnswers() });
    assert.equal(resumed.run.status, "in_review");
    assert.equal(resumed.run.id, stopped.run.id);
    assert.ok(resumed.run.steps.every((s) => s.status === "done"));

    const orgQueries = second.requests.filter((r) => decodeURIComponent(r.url).includes("FROM Organization"));
    assert.equal(orgQueries.length, 0, "the finished org step wasn't repeated");
    assert.ok(resumed.run.budget.callsMade > stopped.run.budget.callsMade, "calls accumulate across sessions");
  });

  test("a resumed run gives the same answer as an uninterrupted one", async () => {
    const out = tempDir();
    const stopped = await simulatorRun(new Simulator({ used: 49_990 }), out);
    const resumed = await simulatorRun(new Simulator({ used: 1000 }), out, { resume: stopped.run.id, answers: loadAnswers() });
    const straight = await simulatorRun(new Simulator(), tempDir(), { answers: loadAnswers() });
    assert.equal(readFileSync(join(resumed.dir, "findings.json"), "utf8"), readFileSync(join(straight.dir, "findings.json"), "utf8"));
    assert.equal(readFileSync(join(resumed.dir, "metrics.json"), "utf8"), readFileSync(join(straight.dir, "metrics.json"), "utf8"));
  });

  test("a big extract resumes from its last page, not its first", async () => {
    const out = tempDir();
    // Stop partway through Opportunity paging by budgeting just past its start.
    const probe = new Simulator();
    await simulatorRun(probe, tempDir(), { pageSize: 50 });
    const firstOpportunityCall = probe.requests.findIndex((r) => decodeURIComponent(r.url).includes("FROM Opportunity ") && r.url.includes("query"));
    const billable = probe.requests.slice(0, firstOpportunityCall + 3).filter((r) => !r.url.endsWith("/limits") && !r.url.endsWith("/services/data/")).length;
    const startingUsed = 50_000 - billable;

    const first = new Simulator({ used: startingUsed });
    const stopped = await simulatorRun(first, out, { pageSize: 50 });
    assert.equal(stopped.run.status, "stopped_budget");
    const partial = join(stopped.dir, "checkpoints", "opportunities.partial.jsonl");
    assert.ok(existsSync(partial), "pages so far were saved");
    const saved = readFileSync(partial, "utf8").trim().split("\n").length;
    assert.ok(saved >= 50);

    const second = new Simulator();
    const resumed = await simulatorRun(second, out, { pageSize: 50, resume: stopped.run.id });
    const queries = second.requests.map((r) => decodeURIComponent(r.url)).filter((q) => q.includes("FROM Opportunity "));
    assert.match(queries[0]!, /Id > '/, "the first query after resuming starts past the saved rows");
    assert.ok(!existsSync(partial), "the partial file is gone once the step finishes");

    const straight = await simulatorRun(new Simulator(), tempDir(), { pageSize: 50 });
    assert.equal(read<{ id: string }[]>(resumed.dir, "metrics.json").length, read<{ id: string }[]>(straight.dir, "metrics.json").length);
    assert.equal(readFileSync(join(resumed.dir, "metrics.json"), "utf8"), readFileSync(join(straight.dir, "metrics.json"), "utf8"));
  });

  test("refuses to resume with a changed calibration", async () => {
    const out = tempDir();
    const stopped = await simulatorRun(new Simulator({ used: 60_000 }), out);
    const calibration = { ...loadCalibration(), minSample: 5 };
    await assert.rejects(simulatorRun(new Simulator(), out, { resume: stopped.run.id, calibration }), /calibration has changed/);
  });

  test("refuses a run id that doesn't exist", async () => {
    await assert.rejects(simulatorRun(new Simulator(), tempDir(), { resume: "hc-nope" }), /No run "hc-nope"/);
  });
});

describe("extraction failures", () => {
  const run = (failQuery: ConstructorParameters<typeof Simulator>[0]) =>
    runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport: new Simulator(failQuery) });

  test("an object we can't read becomes not measurable, and the run finishes", async () => {
    const { output, run: finished } = await run({ failQuery: (soql) => (soql.includes("FROM UserLicense") ? { errorCode: "INSUFFICIENT_ACCESS", message: "no" } : null) });
    assert.equal(finished.status, "awaiting_manual");
    const finding = output!.findings.find((f) => f.ruleId === "utl.unassigned-licence-seats");
    assert.equal(finding?.status, "not_measurable");
  });

  test("an unreadable Opportunity doesn't crash the run", async () => {
    const { output } = await run({ failQuery: (soql) => (soql.includes("FROM Opportunity ") ? { errorCode: "INSUFFICIENT_ACCESS", message: "no" } : null) });
    // Every figure built from deals; AI readiness's never reads them (DECISIONS.md 237).
    assert.ok(output!.metrics.filter((m) => !m.id.startsWith("ai.")).every((m) => m.status === "not_measurable"));
    assert.ok(output!.metrics.filter((m) => m.id.startsWith("ai.")).every((m) => m.status === "measured"));
  });

  test("a failed ownership object blocks removal rather than reading as nothing owned", async () => {
    const { output } = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(AS_OF), answers: loadAnswers(), transport: new Simulator({ failQuery: (soql) => (soql.startsWith("SELECT OwnerId") && soql.includes("Project__c") ? { errorCode: "QUERY_TIMEOUT", message: "slow" } : null) }) });
    assert.deepEqual(output!.findings.filter((f) => f.recommendationType === "deactivate"), []);
  });

  test("a detail object with no owner field isn't a gap in the scan", async () => {
    const { output } = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(AS_OF), answers: loadAnswers(), transport: new Simulator() });
    assert.ok(output!.findings.some((f) => f.recommendationType === "deactivate"));
  });
});

describe("the hc command", () => {
  const hc = (args: string[], env: Record<string, string> = {}) => spawnSync(process.execPath, ["src/cli/hc.ts", ...args], { cwd: service, encoding: "utf8", env: { PATH: process.env.PATH, HOME: tempDir(), ...env } });

  test("prints usage", () => {
    const result = hc(["--help"]);
    assert.equal(result.status, 0);
    assert.match(result.stdout, /hc run --org <alias> --calibration <file>/);
  });

  test("exits 2 on missing arguments", () => assert.equal(hc(["run"]).status, 2));

  test("refuses an org that isn't approved, before touching it", () => {
    const fakeBin = tempDir();
    const marker = join(fakeBin, "sf-was-called");
    writeFileSync(join(fakeBin, "sf"), `#!/bin/sh\ntouch "${marker}"\n`, { mode: 0o755 });
    const result = hc(["run", "--org", "some-customer", "--calibration", join(FIXTURE_DIR, "calibration.json")], { PATH: `${fakeBin}:${process.env.PATH}` });
    assert.equal(result.status, 2);
    assert.match(result.stderr, /isn't approved/);
    assert.ok(!existsSync(marker), "the sf CLI wasn't called");
  });

  test("rejects a calibration that doesn't validate", () => {
    const bad = join(tempDir(), "bad.json");
    writeFileSync(bad, JSON.stringify({ version: 1 }));
    const result = hc(["run", "--org", "x", "--calibration", bad]);
    assert.equal(result.status, 2);
  });

  test("the service never reads a .env file", () => {
    const src = join(service, "src");
    const files = (dir: string): string[] => readdirSync(dir, { withFileTypes: true }).flatMap((e) => (e.isDirectory() ? files(join(dir, e.name)) : [join(dir, e.name)]));
    for (const file of files(src)) assert.doesNotMatch(readFileSync(file, "utf8"), /dotenv\/config/, file);
  });
});

describe("a run started by someone else (the worker)", () => {
  test("takes the run id it's given, and refuses one that isn't a plain name", async () => {
    const { run, dir } = await runFromRecording({ runId: "hc-2026-11-03-00000001" });
    assert.equal(run.id, "hc-2026-11-03-00000001");
    assert.match(dir, /hc-2026-11-03-00000001$/);
    await assert.rejects(runFromRecording({ runId: "../escape" }), /letters, digits, hyphens and underscores/);
    await assert.rejects(runFromRecording({ resume: "a", runId: "b" }), /different runs/);
  });

  test("won't start a second run over a folder that already holds one", async () => {
    const outDir = tempDir();
    await runFromRecording({ outDir, runId: "hc-2026-11-03-00000002" });
    await assert.rejects(runFromRecording({ outDir, runId: "hc-2026-11-03-00000002" }), /already exists.*Resume it/);
    const resumed = await runFromRecording({ outDir, runId: "hc-2026-11-03-00000002", resume: "hc-2026-11-03-00000002" });
    assert.equal(resumed.run.id, "hc-2026-11-03-00000002");
  });

  test("reports each save of run.json, in order, as a copy", async () => {
    const seen: Run[] = [];
    const { run } = await runFromRecording({
      onProgress: async (r) => {
        r.status = "failed";
        seen.push(structuredClone(r));
      },
    });
    assert.notEqual(run.status, "failed", "changing the copy changes nothing");
    assert.ok(seen.length >= run.steps.length);
    const doneCounts = seen.map((r) => r.steps.filter((s) => s.status === "done").length);
    assert.deepEqual(doneCounts, [...doneCounts].sort((a, b) => a - b), "steps only ever finish");
  });
});
