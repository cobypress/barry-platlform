import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { join } from "node:path";
import { ALL_MODULES } from "../../domains/index.ts";
import { readCheckpoint } from "../../engine/checkpoint.ts";
import { collectExtractors, runHealthCheck } from "../../engine/run.ts";
import { tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import * as org from "../../testing/synthetic-org.ts";
import type { ActivityExtract } from "../activity.ts";
import type { AsyncJobRow } from "../integrations.ts";
import type { LeadExtract } from "../leads.ts";
import type { ServiceActivity, ServiceCases } from "../service.ts";
import { soqlDateTime, upToItsSecond, type Sourced } from "../types.ts";
import type { LoginSummary } from "../users.ts";

// Every read with a window on when something happened stops at the run's own
// date (DECISIONS.md 463 and 480), so a step read late, or again on a resumed
// run, doesn't take in what was made since. Two reads are left as they are, by
// decision 480, and named here.

const RUN_END = soqlDateTime(upToItsSecond(new Date(org.AS_OF)));
const LATER = org.LATER_THAN_THE_RUN;

/** Every SOQL query a run sent, through the query route or a composite batch. */
function queriesOf(sim: Simulator): string[] {
  return sim.requests.flatMap((r) => {
    const url = new URL(r.url);
    if (url.pathname.endsWith("/composite/batch")) {
      return (JSON.parse(r.body!) as { batchRequests: Array<{ url: string }> }).batchRequests.map((s) => new URLSearchParams(s.url.split("?")[1]).get("q") ?? "");
    }
    const q = url.searchParams.get("q");
    return q ? [q] : [];
  });
}

/** The object a filter at `index` belongs to: the nearest FROM before it, which is a subquery's own where it has one. */
const objectAt = (soql: string, index: number): string => [...soql.slice(0, index).matchAll(/\bFROM\s+([A-Za-z0-9_]+)/g)].at(-1)?.[1] ?? "?";

// The opportunities scope keeps every open deal, with an OR (opportunities.ts):
// its end needs its own change, since it moves which deals every pipeline
// figure counts (DECISIONS.md 480).
const OPPORTUNITY_SCOPE = /\(CreatedDate >= \S+ OR CloseDate >= \S+ OR LastModifiedDate >= \S+ OR IsClosed = false/g;

describe("a full synthetic run's queries", () => {
  let queries: string[];
  let dir: string;
  before(async () => {
    const sim = new Simulator();
    dir = (await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), answers: loadAnswers(), modules: ALL_MODULES, outDir: tempDir(), now: new Date(org.AS_OF), transport: sim, log: () => {} })).dir;
    queries = queriesOf(sim);
  });

  test("every filter from a date-time to now ends at the run's date, on the same field, but the opportunities scope", () => {
    const ended = new Set<string>();
    const unended: string[] = [];
    let scopes = 0;
    for (const query of queries) {
      const soql = query.replace(OPPORTUNITY_SCOPE, () => (scopes++, "(scope"));
      for (const m of soql.matchAll(/\b([A-Za-z_]+)\s*>=\s*(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)/g)) {
        const read = `${objectAt(soql, m.index)}.${m[1]}`;
        if (soql.includes(`${m[1]} <= ${RUN_END}`)) ended.add(read);
        else unended.push(`${read} in: ${query}`);
      }
    }
    assert.deepEqual(unended, []);
    assert.ok(scopes >= 2, "the opportunities and their history were read with the scope");
    // The reads decision 480 names, and the trail's (463): if one stopped being read, this would pass for nothing.
    const named = [
      "Task.CreatedDate", "Event.StartDateTime", "EmailMessage.MessageDate", "AsyncApexJob.CreatedDate",
      "Lead.CreatedDate", "LeadHistory.CreatedDate", "CampaignMember.CreatedDate", "SBQQ__Quote__c.CreatedDate", "ProcessInstance.CreatedDate",
      // Field usage's recent side, with Lead's above: the synthetic Contact has no custom field to count.
      "LoginHistory.LoginTime", "Account.CreatedDate", "Opportunity.CreatedDate",
      "MacroUsage.CreatedDate", "QuickTextUsage.LoggedTime", "Case.CreatedDate", "Case.ClosedDate",
      "CaseHistory.CreatedDate", "CaseMilestone.StartDate", "AgentWork.RequestDateTime",
      "MessagingSession.StartTime", "LiveChatTranscript.StartTime", "VoiceCall.CallStartDateTime", "SetupAuditTrail.CreatedDate",
    ];
    assert.deepEqual(named.filter((r) => !ended.has(r)), []);
  });

  test("a read from a date alone is ForecastingQuota's, the opportunities scope's, or a date still to come", () => {
    const fromADate = new Set<string>();
    for (const query of queries) {
      for (const m of query.matchAll(/\b([A-Za-z_]+)\s*>=\s*(\d{4}-\d{2}-\d{2})(?!T)/g)) fromADate.add(`${objectAt(query, m.index)}.${m[1]}`);
    }
    // ForecastingQuota.StartDate is the period a quota is for, not when it was
    // made, so a quota for a later period is part of the plan as it stood
    // (DECISIONS.md 480). The Contract and Period reads look forward from today.
    assert.deepEqual([...fromADate].sort(), ["Contract.EndDate", "ForecastingQuota.StartDate", "Opportunity.CloseDate", "Period.EndDate"]);
    assert.ok(queries.some((q) => /FROM ForecastingQuota WHERE StartDate >= \d{4}-\d{2}-\d{2}$/.test(q)), "ForecastingQuota is read from its start alone");
  });

  test("the records made after the run's date are in the org, and none of them is read", () => {
    const after = (value: string | null | undefined) => !!value && value > new Date(org.AS_OF).toISOString().replace("Z", "+0000");
    const checkpoint = <T>(id: string): T => readCheckpoint(join(dir, "checkpoints", `${id}.json`)) as T;
    const ok = <T>(s: Sourced<T>): T => (s.status === "ok" ? s.data : assert.fail(`unavailable: ${s.reason}`));

    // Task, Event and EmailMessage.
    assert.ok(after(org.activity.tasks.find((t) => t.Id === LATER.task)?.CreatedDate));
    assert.ok(after(org.activity.events.find((e) => e.Id === LATER.event)?.StartDateTime));
    assert.ok(after(org.activity.emails.find((e) => e.Id === LATER.email)?.MessageDate));
    const activity = checkpoint<ActivityExtract>("activity");
    assert.equal(ok(activity.tasks).length, org.activity.tasks.length - 1);
    assert.ok(!ok(activity.tasks).some((t) => t.Id === LATER.task));
    assert.ok(!ok(activity.events).some((e) => e.Id === LATER.event));
    assert.ok(!ok(activity.emails).some((e) => e.Id === LATER.email));

    // Lead and LeadHistory.
    assert.ok(after(org.leadFunnel.leads.find((l) => l.Id === LATER.lead)?.CreatedDate));
    assert.ok(after(org.leadFunnel.statusChanges.find((s) => s.Id === LATER.leadStatus)?.CreatedDate));
    const leads = checkpoint<LeadExtract>("leads");
    assert.equal(ok(leads.leads).length, org.leadFunnel.leads.length - 1);
    assert.ok(!ok(leads.leads).some((l) => l.Id === LATER.lead));
    assert.ok(!ok(leads.statusChanges).some((s) => s.Id === LATER.leadStatus));

    // AsyncApexJob.
    assert.ok(after(org.ASYNC_JOBS.find((j) => j.Id === LATER.asyncJob)?.CreatedDate));
    assert.ok(!ok(checkpoint<Sourced<AsyncJobRow[]>>("async-jobs")).some((j) => j.Id === LATER.asyncJob));

    // LoginHistory: the clean dormant user's only successful login is after the run, so neither login read has them.
    assert.ok(org.LOGIN_HISTORY.some((r) => r.UserId === LATER.login.UserId && r.LoginTime === LATER.login.LoginTime && after(r.LoginTime)));
    assert.equal(ok(checkpoint<Sourced<LoginSummary>>("logins")).byUser[LATER.login.UserId], undefined);
    const days = ok(checkpoint<Sourced<{ days: Record<string, string[]> }>>("login-activity")).days;
    assert.ok(!Object.keys(days).some((day) => day > org.AS_OF.slice(0, 10)), "no login day after the run's");

    // Case: opened and closed after the run, with what happened to it.
    const late = org.supportCases.cases.find((c) => c.Id === LATER.case)!;
    assert.ok(after(late.CreatedDate) && after(late.ClosedDate) && late.IsClosed);
    const cases = checkpoint<ServiceCases>("service-cases");
    assert.ok(!ok(cases.closedCases).some((c) => c.Id === LATER.case));
    const counted = (rows: Sourced<Array<{ n: number }>>) => ok(rows).reduce((sum, r) => sum + r.n, 0);
    const year = (value: string | null) => !!value && value >= new Date(new Date(org.AS_OF).setUTCMonth(new Date(org.AS_OF).getUTCMonth() - 12)).toISOString().replace("Z", "+0000") && !after(value);
    assert.equal(counted(cases.created), org.supportCases.cases.filter((c) => year(c.CreatedDate)).length);
    assert.equal(counted(cases.closed), org.supportCases.cases.filter((c) => c.IsClosed && year(c.ClosedDate)).length);
    assert.equal(counted(cases.origins), counted(cases.created));
    const activityOfCases = checkpoint<ServiceActivity>("service-activity");
    assert.ok(!ok(activityOfCases.statusChanges).some((h) => h.CaseId === LATER.case));
    assert.ok(!ok(activityOfCases.milestones).some((m) => m.CaseId === LATER.case));
    assert.ok(!ok(activityOfCases.replies).some((r) => r.ParentId === LATER.case));
    assert.ok(!ok(activityOfCases.agentWork).some((w) => after(w.RequestDateTime)));
    assert.equal(ok(activityOfCases.caseArticles).length, org.supportCases.articles.length - 1);
  });
});

test("no step whose read gained an end changed its version, so runs in flight keep their checkpoints", () => {
  // DECISIONS.md 480: what each step keeps has the same shape as before.
  const versions = Object.fromEntries(collectExtractors(ALL_MODULES).map((e) => [e.id, e.version]));
  const touched = { activity: 2, "async-jobs": 1, leads: 3, "pardot-leads": 2, "cpq-quotes": 1, logins: 4, "login-activity": 1, "field-usage": 1, "service-setup": 1, "service-cases": 2, "service-activity": 2, "setup-audit": 2 };
  assert.deepEqual(Object.fromEntries(Object.keys(touched).map((id) => [id, versions[id]])), touched);
});
