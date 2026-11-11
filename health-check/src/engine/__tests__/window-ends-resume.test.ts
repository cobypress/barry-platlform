import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { ALL_MODULES } from "../../domains/index.ts";
import { fingerprint } from "../../testing/fingerprint.ts";
import { tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator, SYNTHETIC_RECORDS, type OrgRecords } from "../../testing/simulator.ts";
import { AS_OF } from "../../testing/synthetic-org.ts";
import { runHealthCheck } from "../run.ts";

// A step read again on a resumed run gives what it gave the first time, though
// the org has gained records since the run's date (DECISIONS.md 480). Without
// the end on its reads, the resumed step would take them in.

/** The steps whose reads gained an end. */
const STEPS = ["activity", "async-jobs", "leads", "pardot-leads", "cpq-quotes", "logins", "login-activity", "field-usage", "service-setup", "service-cases", "service-activity"];

// The org as it was at the run's date: none of what it has gained since, each
// record by its own date as well as by its case's (DECISIONS.md 481), so a
// reply or an article link made since on an earlier case is gone too.
// AsyncApexJob isn't among the records a test can swap, so its later job is
// there both times, and the first read's end leaves it out too.
const stamp = new Date(AS_OF).toISOString().replace("Z", "+0000");
const by = <T>(rows: T[], field: keyof T): T[] => rows.filter((r) => !(typeof r[field] === "string" && (r[field] as string) > stamp));
const { supportCases } = SYNTHETIC_RECORDS;
const lateCases = new Set(supportCases.cases.filter((c) => c.CreatedDate > stamp).map((c) => c.Id));
const onEarlier = <T extends { CaseId: string } | { ParentId: string }>(rows: T[]): T[] => rows.filter((r) => !lateCases.has("CaseId" in r ? r.CaseId : r.ParentId));
const AT_THE_RUN: OrgRecords = {
  ...SYNTHETIC_RECORDS,
  activity: { tasks: by(SYNTHETIC_RECORDS.activity.tasks, "CreatedDate"), events: by(SYNTHETIC_RECORDS.activity.events, "StartDateTime"), emails: by(SYNTHETIC_RECORDS.activity.emails, "MessageDate") },
  leadFunnel: { ...SYNTHETIC_RECORDS.leadFunnel, leads: by(SYNTHETIC_RECORDS.leadFunnel.leads, "CreatedDate"), statusChanges: by(SYNTHETIC_RECORDS.leadFunnel.statusChanges, "CreatedDate") },
  loginHistory: by(SYNTHETIC_RECORDS.loginHistory, "LoginTime"),
  supportCases: {
    cases: by(supportCases.cases, "CreatedDate"),
    history: by(onEarlier(supportCases.history), "CreatedDate"),
    milestones: by(onEarlier(supportCases.milestones), "StartDate"),
    replies: by(onEarlier(supportCases.replies), "MessageDate"),
    work: by(supportCases.work, "RequestDateTime"),
    articles: by(onEarlier(supportCases.articles), "CreatedDate"),
  },
};

const read = (dir: string, file: string): string => readFileSync(join(dir, file), "utf8");
/** Everything the run concluded. The raw notes count the API calls made, which a resumed run adds to, so that line is left out. */
const concluded = (dir: string) => {
  const { "raw-notes.md": _notes, ...files } = fingerprint(dir);
  return { files, notes: read(dir, "raw-notes.md").replace(/^- API calls: .*$/m, "").replace(/hc-\d{4}-\d{2}-\d{2}-[0-9a-f]{8}/g, "") };
};

test("a resumed run that reads its windowed steps again, after the org has gained records, gives the same steps, findings and metrics", async () => {
  const outDir = tempDir();
  const options = { provider: syntheticProvider, calibration: loadCalibration(), answers: loadAnswers(), modules: ALL_MODULES, outDir, log: () => {} };
  assert.ok(lateCases.size > 0 && AT_THE_RUN.activity.tasks.length < SYNTHETIC_RECORDS.activity.tasks.length, "the org has gained records since");
  assert.ok(AT_THE_RUN.supportCases.cases.some((c) => !c.IsClosed) && supportCases.cases.some((c) => lateCases.has(c.Id) && !c.IsClosed), "an open case since");
  assert.ok(AT_THE_RUN.supportCases.replies.length < onEarlier(supportCases.replies).length, "a reply since on an earlier case");
  assert.ok(AT_THE_RUN.supportCases.articles.length < onEarlier(supportCases.articles).length, "an article link since on an earlier case");
  const first = await runHealthCheck({ ...options, now: new Date(AS_OF), transport: new Simulator({ records: AT_THE_RUN }) });
  const was = { steps: Object.fromEntries(STEPS.map((id) => [id, read(first.dir, `checkpoints/${id}.json`)])), concluded: concluded(first.dir) };

  // As a run stopped at the budget before these steps leaves them.
  const run = JSON.parse(read(first.dir, "run.json")) as { steps: Array<{ id: string; status: string }> };
  for (const step of run.steps) if (STEPS.includes(step.id)) step.status = "pending";
  writeFileSync(join(first.dir, "run.json"), JSON.stringify(run));
  for (const id of STEPS) rmSync(join(first.dir, "checkpoints", `${id}.json`));

  // Resumed later, from the org as it is now. The run keeps its own date.
  const now = new Simulator();
  await runHealthCheck({ ...options, now: new Date(), transport: now, resume: first.run.id });
  const sent = now.requests.map((r) => decodeURIComponent(r.url));
  for (const object of ["FROM Task", "FROM Case", "FROM LoginHistory", "FROM Lead", "FROM AsyncApexJob"]) assert.ok(sent.some((u) => u.includes(object)), `read again: ${object}`);

  for (const id of STEPS) assert.equal(read(first.dir, `checkpoints/${id}.json`), was.steps[id], id);
  const is = concluded(first.dir);
  assert.deepEqual(is.files, was.concluded.files);
  assert.equal(is.notes, was.concluded.notes);
});
