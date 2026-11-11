import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { join } from "node:path";
import { ALL_MODULES } from "../../domains/index.ts";
import { readCheckpoint } from "../../engine/checkpoint.ts";
import { collectExtractors, runHealthCheck } from "../../engine/run.ts";
import type { Finding } from "../../schemas/index.ts";
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
// run, doesn't take in what was made since. ForecastingQuota is left as it is,
// by decision 480, and the opportunities scope's ways in by 465, named here.

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

// The opportunities scope keeps every open deal, with an OR (opportunities.ts),
// and ends on CreatedDate (DECISIONS.md 465). Its LastModifiedDate is a way
// into the scope, not a window: a deal changed since the run's date may have
// been in the scope at it, and it's wound back from its history, so that
// clause has no end of its own.
const WAY_IN = "Opportunity.LastModifiedDate";
const SCOPE = String.raw`\(CreatedDate >= \S+ OR CloseDate >= \S+ OR LastModifiedDate >= \S+ OR IsClosed = false(?: OR StageName IN \([^)]*\))?\)`;

describe("a full synthetic run's queries", () => {
  let queries: string[];
  let dir: string;
  let findings: Finding[];
  before(async () => {
    const sim = new Simulator();
    const run = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), answers: loadAnswers(), modules: ALL_MODULES, outDir: tempDir(), now: new Date(org.AS_OF), transport: sim, log: () => {} });
    ({ dir } = run);
    findings = run.output!.findings;
    queries = queriesOf(sim);
  });

  test("every filter from a date-time to now ends at the run's date, on the same field, but the opportunities scope's way in", () => {
    const ended = new Set<string>();
    const unended: string[] = [];
    let waysIn = 0;
    for (const soql of queries) {
      for (const m of soql.matchAll(/\b([A-Za-z_]+)\s*>=\s*(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)/g)) {
        const read = `${objectAt(soql, m.index)}.${m[1]}`;
        if (soql.includes(`${m[1]} <= ${RUN_END}`)) ended.add(read);
        else if (read === WAY_IN) waysIn++;
        else unended.push(`${read} in: ${soql}`);
      }
    }
    assert.deepEqual(unended, []);
    assert.ok(waysIn >= 2, "the opportunities and their history were read with the scope");
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
    // (DECISIONS.md 480). Opportunity.CloseDate is a way into the opportunities
    // scope, which ends on CreatedDate (465). The Contract and Period reads look
    // forward from today.
    assert.deepEqual([...fromADate].sort(), ["Contract.EndDate", "ForecastingQuota.StartDate", "Opportunity.CloseDate", "Period.EndDate"]);
    assert.ok(queries.some((q) => /FROM ForecastingQuota WHERE StartDate >= \d{4}-\d{2}-\d{2}$/.test(q)), "ForecastingQuota is read from its start alone");
  });

  test("the opportunities, their history and the next steps end on the deal's CreatedDate at the run's date (DECISIONS.md 465)", () => {
    const end = ` AND CreatedDate <= ${RUN_END}`;
    const deals = queries.filter((q) => new RegExp(`^SELECT Id, Name, .* FROM Opportunity WHERE ${SCOPE}${end}( AND Id > '\\w+')? ORDER BY Id`).test(q));
    // The history's own rows have no end in the query: the later ones are how a deal changed since is found.
    const history = queries.filter((q) => new RegExp(`FROM OpportunityHistory WHERE OpportunityId IN \\(SELECT Id FROM Opportunity WHERE ${SCOPE}${end}\\)( AND Id > '\\w+')? ORDER BY Id`).test(q));
    const steps = queries.filter((q) => new RegExp(`^SELECT Id FROM Opportunity WHERE \\(IsClosed = false(?: OR StageName IN \\([^)]*\\))?\\) AND NextStep != null${end}( AND Id > '\\w+')? ORDER BY Id`).test(q));
    assert.ok(deals.length > 0 && history.length > 0 && steps.length > 0, `deals ${deals.length}, history ${history.length}, next steps ${steps.length}`);
    // Every other read of a deal is left without the end on purpose, and named:
    // the wins before the window, and reads of how things stand with no window
    // (DECISIONS.md 481), which are as they were when read.
    const left: Array<[string, RegExp]> = [
      ["prior wins", /^SELECT Id, AccountId, CloseDate FROM Opportunity WHERE .* AND CloseDate < \d{4}-\d{2}-\d{2} ORDER BY Id/],
      ["won accounts", /^SELECT Id, OwnerId, LastActivityDate FROM Account WHERE Id IN \(SELECT AccountId FROM Opportunity WHERE IsWon = true\)/],
      ["ownership count", /^SELECT OwnerId, COUNT\(Id\) c FROM Opportunity GROUP BY OwnerId$/],
      ["field usage, all time", /^SELECT COUNT\(Id\) total, .* FROM Opportunity$/],
    ];
    const unended = queries.filter((q) => /\bFROM Opportunity(History)?\b/.test(q) && !q.includes(end));
    assert.deepEqual(unended.filter((q) => !left.some(([, read]) => read.test(q))), []);
    assert.deepEqual([...new Set(unended.flatMap((q) => left.filter(([, read]) => read.test(q)).map(([name]) => name)))].sort(), left.map(([name]) => name).sort());
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

    // Each read that hangs off a case ends on its own date too (DECISIONS.md 481).
    // An open email case opened since, with its reply.
    const open = org.supportCases.cases.find((c) => c.Id === LATER.openCase)!;
    assert.ok(after(open.CreatedDate) && !open.IsClosed && open.Origin === "Email");
    assert.ok(org.supportCases.replies.some((r) => r.ParentId === LATER.openCase && !r.Incoming));
    assert.ok(ok(cases.open).length > 0 && !ok(cases.open).some((c) => c.Id === LATER.openCase || after(c.CreatedDate)));
    // A reply and an incoming email since, on an email case of the year that had no reply at the run's date.
    const earlier = org.supportCases.cases.find((c) => c.Id === LATER.unansweredCase)!;
    assert.ok(year(earlier.CreatedDate) && earlier.Origin === "Email");
    assert.ok(!org.supportCases.replies.some((r) => r.ParentId === LATER.unansweredCase && !r.Incoming && !after(r.MessageDate)), "no reply at the run's date");
    for (const id of [LATER.reply, LATER.incoming]) assert.ok(after(org.supportCases.replies.find((r) => r.Id === id)?.MessageDate));
    assert.ok(!ok(activityOfCases.replies).some((r) => r.ParentId === LATER.unansweredCase || after(r.MessageDate)));
    const inYearCases = new Set(org.supportCases.cases.filter((c) => year(c.CreatedDate)).map((c) => c.Id));
    const email = activityOfCases.channels.find((c) => c.channel === "Email")!;
    assert.equal(ok(email.count), org.supportCases.replies.filter((r) => r.Incoming && inYearCases.has(r.ParentId) && !after(r.MessageDate)).length);
    // An article attached since to a case closed in the year.
    const closed = org.supportCases.cases.find((c) => c.Id === LATER.closedCase)!;
    assert.ok(closed.IsClosed && year(closed.ClosedDate));
    assert.ok(after(org.supportCases.articles.find((a) => a.Id === LATER.article)?.CreatedDate));
    assert.ok(!ok(activityOfCases.caseArticles).some((a) => a.CaseId === LATER.closedCase));
    const closedInYear = new Set(org.supportCases.cases.filter((c) => c.IsClosed && year(c.ClosedDate)).map((c) => c.Id));
    assert.equal(ok(activityOfCases.caseArticles).length, org.supportCases.articles.filter((a) => closedInYear.has(a.CaseId) && !after(a.CreatedDate)).length);
    assert.equal(ok(activityOfCases.caseArticles).length, org.supportCases.articles.length - 2);
  });

  test("the first reply on email cases counts the cases opened, and the replies sent, by the run's date (DECISIONS.md 481)", () => {
    // Worked out from the org as it stood at the run's date, apart from the read:
    // an email case opened since, or a reply sent since, would move these.
    const at = new Date(org.AS_OF).toISOString().replace("Z", "+0000");
    const from = new Date(new Date(org.AS_OF).setUTCMonth(new Date(org.AS_OF).getUTCMonth() - 12)).toISOString().replace("Z", "+0000");
    const inYear = (value: string | null) => !!value && value >= from && value <= at;
    const emailCases = org.supportCases.cases.filter((c) => c.Origin === "Email" && inYear(c.CreatedDate) && (!c.IsClosed || inYear(c.ClosedDate)));
    const first = new Map<string, string>();
    for (const r of org.supportCases.replies) if (!r.Incoming && r.MessageDate <= at && (!first.has(r.ParentId) || r.MessageDate < first.get(r.ParentId)!)) first.set(r.ParentId, r.MessageDate);
    const answered = emailCases.filter((c) => first.has(c.Id) && first.get(c.Id)! >= c.CreatedDate).length;
    const facts = findings.find((f) => f.ruleId === "svc.first-reply-email")!.facts as { cases: number; answered: number; unanswered: number };
    assert.deepEqual({ cases: facts.cases, answered: facts.answered, unanswered: facts.unanswered }, { cases: emailCases.length, answered, unanswered: emailCases.length - answered });
    // The planted cases are among those the figure is about: one left out, one still unanswered.
    assert.ok(!emailCases.some((c) => c.Id === LATER.openCase) && emailCases.some((c) => c.Id === LATER.unansweredCase) && !first.has(LATER.unansweredCase));
  });

  test("every read of Case, and of an email or article link on a case, ends at the run's date, but the ownership count", () => {
    // The ownership scan counts the records each user owns now, for the guard
    // on deactivating them: a case made since is still theirs, so it isn't a
    // window on when something happened, and cutting it would only hide work.
    const ownership = /^SELECT OwnerId, COUNT\(Id\) c FROM Case GROUP BY OwnerId$/;
    const read = new Set<string>();
    const unended: string[] = [];
    for (const query of queries) {
      if (ownership.test(query)) {
        read.add("ownership");
        continue;
      }
      const ends = new Set([...query.matchAll(new RegExp(`\\b[A-Za-z_]+ <= ${RUN_END}`, "g"))].map((m) => objectAt(query, m.index)));
      for (const m of query.matchAll(/\bFROM (Case|EmailMessage|CaseArticle)\b/g)) {
        read.add(m[1]!);
        if (!ends.has(m[1]!)) unended.push(`${m[1]} in: ${query}`);
      }
    }
    assert.deepEqual(unended, []);
    assert.deepEqual([...read].sort(), ["Case", "CaseArticle", "EmailMessage", "ownership"]);
    assert.ok(queries.some((q) => q.endsWith(`FROM Case WHERE IsClosed = false AND CreatedDate <= ${RUN_END}`)), "the open cases are read to the run's date");
  });
});

test("no step whose read gained an end changed its version, so runs in flight keep their checkpoints, but the opportunities", () => {
  // DECISIONS.md 480 and 465: what each step keeps has the same shape as before.
  // The opportunities went from 4 to 5 on purpose (465): a checkpoint from 4
  // may hold deals read late as they are now, which a re-read winds back.
  const versions = Object.fromEntries(collectExtractors(ALL_MODULES).map((e) => [e.id, e.version]));
  const touched = { opportunities: 5, "next-steps": 1, activity: 2, "async-jobs": 1, leads: 3, "pardot-leads": 2, "cpq-quotes": 1, logins: 4, "login-activity": 1, "field-usage": 1, "service-setup": 1, "service-cases": 2, "service-activity": 2, "setup-audit": 2 };
  assert.deepEqual(Object.fromEntries(Object.keys(touched).map((id) => [id, versions[id]])), touched);
});
