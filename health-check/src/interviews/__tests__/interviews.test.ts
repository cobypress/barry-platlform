import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { MODELS } from "../../config.ts";
import type { NarrativeClient, NarrativeReply, NarrativeRequest } from "../../narrative/client.ts";
import { draftSection } from "../../narrative/draft.ts";
import { buildReport, currentBody, type ReportInput } from "../../narrative/sections.ts";
import { buildDataset, checkNumbers, scopeDataset, toNumberCheckResult } from "../../numbercheck/index.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { executiveHtml } from "../../render/executive.ts";
import { fullReportHtml } from "../../render/full-report.ts";
import { interviewSlides, INTERVIEW_ROWS_PER_SLIDE } from "../../render/interviews.ts";
import { INTERVIEWS_FILE, loadRun } from "../../render/render-run.ts";
import { renderContext } from "../../render/html.ts";
import { buildMicrositePages } from "../../microsite/pages.ts";
import type { Finding, Metric } from "../../schemas/index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { SYNTHETIC_NOTE, syntheticInterviews } from "../../testing/interviews.ts";
import { INTERVIEW_SQL, interviewsFromRows, PostgresWorkerStore, type SqlClient, type SqlPool, type SqlResult } from "../../worker/postgres-store.ts";
import { INTERVIEW_QUESTIONS, INTERVIEW_ROLES, INTERVIEW_VERSION, questionsFor, readInterviewAnswers, ROLE_QUESTIONS, type Interview } from "../questions.ts";
import { buildInterviewView, buildThemes, executiveThemes, interviewSource, rolesPhrase, strongThemes, SHARE_WITHIN_POINTS } from "../readings.ts";

const interview = (over: Partial<Interview> = {}): Interview => ({ id: "ia-0000000001", role: "sales_leader", heldOn: "2026-10-27", notes: "", answers: {}, version: 1, ...over });

const issue = (domain: string, ruleId = `${domain}.synthetic`, status: Finding["status"] = "issue"): Finding => ({ id: `${ruleId}:${status}`, domain, ruleId, status, title: `${domain} issue`, severity: "high" }) as unknown as Finding;
const metric = (id: string, value: number): Metric => ({ id, status: "measured", value, n: 50, domain: "historic_pipeline" }) as unknown as Metric;

describe("the interview questions", () => {
  test("are versioned, short for every role, ask no name, and every role's question exists", () => {
    assert.equal(INTERVIEW_VERSION, 1);
    for (const { id } of INTERVIEW_ROLES) {
      assert.ok(ROLE_QUESTIONS[id].length >= 3 && ROLE_QUESTIONS[id].length <= 5, `${id} asks 3 to 5`);
      assert.equal(questionsFor(id).length, ROLE_QUESTIONS[id].length, `${id}'s questions all exist`);
    }
    assert.ok(INTERVIEW_QUESTIONS.every((q) => !/\bname\b/i.test(q.text)));
  });

  test("keep only the role's own answers, in their shape, from this version", () => {
    const read = readInterviewAnswers(interview({ answers: { trust: 2, win_rate: 140, sales_cycle: 61.5, ease: 4, fix_first: "  One view.  ", forecast_hit: "70" } }));
    assert.deepEqual(read, { trust: 2, fix_first: "One view." }, "ease isn't the director's; 140% and 61.5 days don't fit; a string isn't a share");
    assert.deepEqual(readInterviewAnswers(interview({ role: "sales_rep", answers: { rekeying: 1.5, admin_hours: 1.2 } })), { rekeying: 1.5 }, "hours go in half hours");
    assert.deepEqual(readInterviewAnswers(interview({ answers: { trust: 2 }, version: 2 })), {}, "another version's answers aren't read");
  });
});

describe("what people told us, against the data", () => {
  test("a rating is a problem felt at 3 or less, and agrees when the checks found issues", () => {
    const one = (trust: number, findings: Finding[]) => buildThemes([interview({ answers: { trust } })], findings, [])[0]!;
    assert.equal(one(2, [issue("data_quality")]).reading, "agrees");
    assert.equal(one(2, [issue("forecast", "fct.x", "passed")]).reading, "disagrees");
    assert.equal(one(2, [issue("forecast", "fct.x", "passed")]).why, "We didn't find a cause in what we read.");
    assert.equal(one(5, [issue("data_quality")]).reading, "disagrees");
    assert.equal(one(5, [issue("data_quality", "dq.x", "passed")]).reading, "agrees");
    assert.equal(one(2, []).reading, "cant_tell", "nothing checked");
    assert.equal(one(2, [issue("data_quality", "dq.x", "not_measurable")]).reading, "cant_tell", "nothing measurable");
  });

  test(`a share agrees within ${SHARE_WITHIN_POINTS} points, a count or days within a quarter, and says which way it's off`, () => {
    const one = (answers: Record<string, number>, metrics: Metric[]) => buildThemes([interview({ answers })], [], metrics)[0]!;
    assert.equal(one({ win_rate: 40 }, [metric("historic.win_rate", 47)]).reading, "agrees");
    const high = one({ win_rate: 60 }, [metric("historic.win_rate", 47)]);
    assert.deepEqual([high.reading, high.why], ["disagrees", "Higher than the data shows."]);
    assert.equal(one({ sales_cycle: 60 }, [metric("historic.sales_cycle_won", 77)]).reading, "agrees");
    assert.equal(one({ sales_cycle: 30 }, [metric("historic.sales_cycle_won", 77)]).why, "Lower than the data shows.");
    assert.equal(one({ win_rate: 40 }, []).reading, "cant_tell", "nothing measured to set it against");
    const admin = buildThemes([interview({ role: "admin", answers: { integrations: 6 } })], [], [metric("documented.integrations", 8)])[0]!;
    assert.equal(admin.reading, "agrees", "two either way always agrees, however small");
  });

  test("time on admin is shown, and we can't tell, because Salesforce doesn't record it", () => {
    const t = buildThemes([interview({ role: "sales_rep", answers: { admin_hours: 3 } })], [], [])[0]!;
    assert.equal(t.reading, "cant_tell");
    assert.match(t.said, /told us about 3 hours a week/);
  });

  test("every sentence that quotes what someone said says so, and names a role, never a person", () => {
    const themes = buildThemes(syntheticInterviews(), [issue("data_quality")], [metric("historic.win_rate", 47)]);
    assert.ok(themes.length > 10);
    for (const t of themes) assert.match(t.said, /^(Your|A|Someone) .*(rated|told us)/, t.said);
    assert.equal(rolesPhrase(["sales_rep", "sales_leader", "sales_rep", "admin"]), "your sales director, your Salesforce admin and 2 sales reps");
  });

  test("a theme is strong only when two or more read the same way and none the other", () => {
    const two = [interview({ answers: { trust: 2 } }), interview({ id: "ia-0000000002", role: "finance", answers: { trust: 1 } })];
    const strong = strongThemes(buildThemes(two, [issue("data_quality")], []));
    assert.equal(strong.length, 1);
    assert.match(strong[0]!.sentence, /your sales director and someone in finance told us this is a problem, and what we found backs them up/);
    const mixed = [...two, interview({ id: "ia-0000000003", role: "sales_ops", answers: { trust: 5 } })];
    assert.deepEqual(strongThemes(buildThemes(mixed, [issue("data_quality")], [])), [], "one says it works, so it isn't one message");
    const fine = [interview({ answers: { trust: 5 } }), interview({ id: "ia-0000000002", role: "finance", answers: { trust: 4 } })];
    const view = buildInterviewView(fine, [issue("data_quality", "dq.x", "passed")], [])!;
    assert.equal(view.strong.length, 1, "agreeing that it works is strong");
    assert.deepEqual(executiveThemes(view), [], "but it isn't news for an executive");
  });

  test("no interviews is no view", () => {
    assert.equal(buildInterviewView([], [], []), null);
    assert.equal(buildInterviewView(null, [], []), null);
  });
});

describe("the number check, on what someone said", () => {
  const set = buildDataset({ findings: [], metrics: [], extraText: [{ source: interviewSource(1), text: "Thinks the win rate is around 40%.", said: "said in interview, Sales director" }, { source: "measured", text: "Open deals: 76." }] });
  const all = scopeDataset(set, [interviewSource(1), "measured"]);

  test("passes a figure only someone said when its sentence says it was said, and traces it", () => {
    const outcome = checkNumbers("Your sales director told us the win rate is about 40%. You have 76 open deals.", all);
    assert.equal(outcome.passed, true);
    assert.deepEqual(outcome.said, [{ raw: "40%", source: "said in interview, Sales director" }]);
    assert.deepEqual(toNumberCheckResult(outcome, new Date("2026-11-06T00:00:00Z")).said, ["40%: said in interview, Sales director"]);
  });

  test("rejects it written as a figure we measured", () => {
    const outcome = checkNumbers("Your win rate is 40%.", all);
    assert.equal(outcome.passed, false);
    assert.match(outcome.unmatched[0]!.reason, /only what was said in interview, Sales director/);
    assert.equal(checkNumbers("The win rate is 40%. Your sales director told us so.", all).passed, false, "the attribution has to be in the same sentence");
  });

  test("leaves a figure the data backs as ours, said or not", () => {
    const both = buildDataset({ findings: [], metrics: [], extraText: [{ source: interviewSource(1), text: "Open deals are about 76.", said: "said in interview, Sales director" }, { source: "measured", text: "Open deals: 76." }] });
    const outcome = checkNumbers("You have 76 open deals.", both);
    assert.deepEqual([outcome.passed, outcome.said], [true, []]);
    assert.equal(toNumberCheckResult(outcome, new Date()).said, undefined, "nothing said, nothing stored");
  });
});

const { output, run, dir } = await runFromRecording({ answers: loadAnswers() });
const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
const base: ReportInput = { run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps };
const withInterviews = buildReport({ ...base, interviews: syntheticInterviews() });
const sectionOf = (out: keyof typeof withInterviews.outputs, key: string) => withInterviews.outputs[out].sections.find((s) => s.key === key);

describe("the report, with interviews", () => {
  test("has the chapter in all three outputs, and every one passes its own number check", () => {
    for (const out of ["raw_notes", "executive_summary", "full_health_check"] as const) {
      const section = sectionOf(out, "interviews");
      assert.ok(section, `${out} has it`);
      const version = withInterviews.outputs[out].versions.find((v) => v.id === section!.currentVersionId)!;
      assert.deepEqual(version.numberCheck.unmatched, [], out);
      assert.notEqual(section!.status, "needs_input", out);
    }
    const raw = withInterviews.outputs.raw_notes.versions.find((v) => v.id === sectionOf("raw_notes", "interviews")!.currentVersionId)!;
    assert.ok(raw.numberCheck.said?.some((s) => /said in interview, Sales (director|operations|rep)/.test(s)), "the figures people gave us are traced to them");
  });

  test("never prints the notes, a name or the interviewer, in the sections, the deck, the summary or the microsite", () => {
    const names = pseudonyms.entries().map((e) => e.name).filter((n) => n.includes(" "));
    const texts = [
      ...Object.values(withInterviews.outputs).flatMap((o) => o.versions.map((v) => v.body)),
      fullReportHtml(withInterviews, pseudonyms),
      executiveHtml(withInterviews, pseudonyms),
      ...buildMicrositePages(withInterviews, pseudonyms).pages.map((p) => p.html),
    ];
    for (const text of texts) assert.ok(!text.includes(SYNTHETIC_NOTE), "a note was printed");
    // The chapter itself names nobody: every section, and every slide of it.
    const chapter = [
      ...Object.values(withInterviews.outputs).flatMap((o) => o.versions.filter((v) => v.sectionId.endsWith(":interviews")).map((v) => v.body)),
      ...interviewSlides(renderContext(pseudonyms, "GBP", false), withInterviews.interviews, "").map((sl) => sl.html),
    ];
    assert.ok(chapter.length >= 4);
    for (const text of chapter) for (const name of names) assert.ok(!text.includes(name), name);
    assert.match(fullReportHtml(withInterviews, pseudonyms), /What your people told us/);
    assert.match(executiveHtml(withInterviews, pseudonyms), /What your people told us/);
    assert.match(executiveHtml(withInterviews, pseudonyms), /it's their view, not a measure/);
  });

  test("gives Claude the notes under each interview's source, and refuses a draft that names a person", async () => {
    const brief = withInterviews.briefs.get(sectionOf("full_health_check", "interviews")!.id)!;
    assert.equal(brief.rolesOnly, true);
    assert.deepEqual(brief.sources.map((s) => s.id), ["interviews", ...syntheticInterviews().map((_, i) => interviewSource(i + 1))]);
    assert.ok(JSON.stringify(brief.sources).includes(SYNTHETIC_NOTE), "the notes go, guarded");
    const user = pseudonyms.entries().find((e) => e.token.startsWith("[[user:"))!;
    const requests: NarrativeRequest[] = [];
    const reply = (text: string): NarrativeReply => ({ stopReason: "end_turn", text: JSON.stringify({ paragraphs: [{ text, sources: [interviewSource(1)] }] }), model: MODELS.synthesis });
    const replies = [reply(`${user.token} told us the forecast is built in a spreadsheet.`), reply("Your sales director told us they put the win rate at about 40%.")];
    const client: NarrativeClient = { async complete(request) { requests.push(request); return replies.shift()!; } };
    const result = await draftSection({ brief, dataset: withInterviews.dataset, client, pseudonyms });
    assert.equal(result.status, "accepted");
    assert.match(requests[1]!.prompt, /Refer to people by their role/);
    const measured = await draftSection({ brief, dataset: withInterviews.dataset, client: { async complete() { return reply("The win rate is about 40%."); } }, pseudonyms });
    assert.equal(measured.status, "rejected", "a said figure written as ours is refused");
  });

  test("puts only strong themes in the executive summary, and nothing without interviews", () => {
    const exec = currentBody(withInterviews.outputs.executive_summary, "interviews");
    assert.ok(exec.split("\n\n").length <= 3);
    assert.ok(withInterviews.interviews!.strong.length >= 1);
    const without = buildReport(base);
    assert.equal(without.interviews, null);
    for (const o of Object.values(without.outputs)) assert.ok(!o.sections.some((s) => s.key === "interviews"));
    const weak = buildReport({ ...base, interviews: [syntheticInterviews()[0]!] });
    assert.ok(!weak.outputs.executive_summary.sections.some((s) => s.key === "interviews"), "one person isn't a strong theme");
    assert.ok(weak.outputs.full_health_check.sections.some((s) => s.key === "interviews"), "but the chapter is there");
  });

  test(`puts ${INTERVIEW_ROWS_PER_SLIDE} readings to a slide, by role, after the chapter's opening`, () => {
    const r = renderContext(pseudonyms, "GBP", false);
    const slides = interviewSlides(r, withInterviews.interviews, "<p>Body</p>");
    assert.equal(slides.length, 1 + Math.ceil(withInterviews.interviews!.themes.length / INTERVIEW_ROWS_PER_SLIDE));
    assert.match(slides[1]!.html, /They told us/);
    assert.deepEqual(interviewSlides(r, null, ""), []);
  });
});

describe("the worker, with interviews", () => {
  test("reads them in the order they were held, answers and version through the row as JSON, and leaves out what it can't read", async () => {
    assert.equal(INTERVIEW_SQL, `SELECT "id", "role", "heldOn"::text AS "heldOn", "notes", to_jsonb("HealthCheckInterview") -> 'answers' AS "answers", to_jsonb("HealthCheckInterview") ->> 'version' AS "version" FROM "HealthCheckInterview" WHERE "runId" = $1 ORDER BY "heldOn", "id"`);
    const read = interviewsFromRows([
      { id: "ia-0000000001", role: "sales_leader", heldOn: "2026-10-27", notes: "Notes.", answers: { trust: 2 }, version: "1" },
      { id: "ia-0000000002", role: "chief_of_vibes", heldOn: "2026-10-28", notes: "", answers: null, version: null },
      { id: "ia-0000000003", role: "admin", heldOn: "2026-10-29", notes: null, answers: null, version: null },
    ])!;
    assert.deepEqual(read.map((i) => [i.id, i.notes, i.answers, i.version]), [["ia-0000000001", "Notes.", { trust: 2 }, 1], ["ia-0000000003", "", {}, undefined]]);
    assert.equal(interviewsFromRows([]), null);
  });

  test("treats a database without the table as no interviews", async () => {
    const pool: SqlPool = {
      async query(text: string): Promise<SqlResult> {
        if (text.includes(`FROM "HealthCheckInterview"`)) throw Object.assign(new Error("relation does not exist"), { code: "42P01" });
        if (text.includes(`FROM "HealthCheckRun" WHERE`)) return { rows: [{ id: run.id, product: "barometer", customerRef: null, orgId: run.orgId, isSandbox: false, connectionKind: run.connectionKind, status: "extracting", modules: run.modules, ruleSetVersion: run.ruleSetVersion, calibrationHash: run.calibrationHash, baselineRunId: null, asOf: run.asOf, analysisFrom: run.analysisWindow.from, analysisTo: run.analysisWindow.to, apiVersion: run.apiVersion, budget: run.budget, benchmarkOptIn: false, createdBy: run.createdBy, startedAt: run.startedAt, finishedAt: null, connectionName: null, scope: null }], rowCount: 1 };
        return { rows: [], rowCount: 0 };
      },
      async connect(): Promise<SqlClient & { release(): void }> {
        return { query: (t, v) => pool.query(t, v), release: () => {} };
      },
    };
    const job = await new PostgresWorkerStore(pool).loadRunJob(run.id);
    assert.equal(job?.interviews, null);
  });

  test("the run folder keeps them, and the documents print from there", () => {
    const folder = mkdtempSync(join(tmpdir(), "hc-interviews-"));
    for (const name of ["run.json", "calibration.json", "findings.json", "metrics.json", "manual-steps.json", "pseudonym-map.json"]) writeFileSync(join(folder, name), readFileSync(join(dir, name)));
    assert.equal(loadRun(folder).input.interviews, null);
    writeFileSync(join(folder, INTERVIEWS_FILE), JSON.stringify({ version: INTERVIEW_VERSION, interviews: syntheticInterviews() }));
    assert.equal(loadRun(folder).input.interviews!.length, 5);
  });
});
