import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { ALL_MODULES } from "../../domains/index.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { Pseudonymiser } from "../../privacy/pseudonymiser.ts";
import { Finding, FIX_PROMPT_MAX } from "../../schemas/index.ts";
import { runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { buildScaleOrg, FIVE_HUNDRED_USERS, scaled } from "../../testing/scale-org.ts";
import { Simulator, SYNTHETIC_RECORDS, type OrgRecords } from "../../testing/simulator.ts";
import { AS_OF, CONNECTED_USER, ORG_ID } from "../../testing/synthetic-org.ts";
import { buildReport } from "../../narrative/sections.ts";
import { renderRawNotes } from "../../notes/raw-notes.ts";
import { executiveHtml } from "../../render/executive.ts";
import { fullReportHtml } from "../../render/full-report.ts";
import { onePagerHtml } from "../../render/one-pager.ts";
import { readoutKit } from "../../render/readout-kit.ts";
import { buildRawNotes } from "../../render/raw-notes-docx.ts";
import type { PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { effortHours } from "../hours.ts";
import { assemblePrompt, fixPrompt, promptIsSafe, withheldLabels } from "../prompt.ts";
import { FIX_TABLE, NEVER_RAISES } from "../table.ts";

// Every issue carries hours to fix and a fix prompt (DECISIONS.md 628). The hours
// come from one table, and the prompt is assembled by code and never names a
// person or a record.

const rules = ALL_MODULES.flatMap((m) => m.rules);
const generatedRecords = buildScaleOrg(scaled(FIVE_HUNDRED_USERS, 0.005));
const readFindings = (dir: string): Finding[] => (JSON.parse(readFileSync(join(dir, "findings.json"), "utf8")) as unknown[]).map((f) => Finding.parse(f));
const recordedRun = await runFromRecording({ answers: loadAnswers(), modules: ALL_MODULES });
const recorded = readFindings(recordedRun.dir);
const generated = readFindings(
  (await runHealthCheck({ provider: syntheticProvider, connectedUser: CONNECTED_USER, calibration: loadCalibration(), answers: loadAnswers(), modules: ALL_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport: new Simulator({ records: generatedRecords, pageSize: 2000 }) })).dir,
);
const issues = recorded.filter((f) => f.status === "issue");
const one = (ruleId: string, key?: (f: Finding) => boolean): Finding => {
  const found = issues.filter((f) => f.ruleId === ruleId && (key?.(f) ?? true));
  assert.equal(found.length, 1, ruleId);
  return found[0]!;
};

/** Every person's and record's name, username, email and id planted in an org's records, in each form a prompt could hold. */
function planted(records: OrgRecords): string[] {
  const out = new Set<string>([ORG_ID, ORG_ID.slice(0, 15)]);
  const walk = (value: unknown): void => {
    if (Array.isArray(value)) return value.forEach(walk);
    if (!value || typeof value !== "object") return;
    const row = value as Record<string, unknown>;
    if (typeof row.Id === "string") {
      for (const key of ["Id", "Name", "Username", "Email"]) {
        const v = row[key];
        if (typeof v !== "string" || !v) continue;
        out.add(v);
        if (key === "Id") out.add(v.slice(0, 15));
        // A person's parts too: "Sam" or "Reed" alone is still a name.
        if (key === "Name" && /^[A-Z][a-z]+ [A-Z][a-z]+$/.test(v) && !/^(Integration|Synthetic|Shared)/.test(v)) v.split(" ").forEach((part) => out.add(part));
      }
    }
    for (const v of Object.values(row)) if (v && typeof v === "object") walk(v);
  };
  walk(records);
  return [...out].filter((s) => s.length >= 2);
}
const word = (s: string): RegExp => new RegExp(`(?<![\\w@.])${s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}(?![\\w@])`);

describe("every rule that can raise an issue declares its hours and its prompt (decision 628)", () => {
  test("each rule has an hours declaration and a prompt template, and the table holds no rule that doesn't exist", () => {
    const missing = rules.filter((r) => !NEVER_RAISES.has(r.id) && (!FIX_TABLE[r.id]?.hours || !FIX_TABLE[r.id]?.prompt.steps.length || !FIX_TABLE[r.id]?.prompt.verify.length)).map((r) => r.id);
    assert.deepEqual(missing, [], "rules that can raise an issue with no hours declaration or prompt template");
    const ids = new Set(rules.map((r) => r.id));
    assert.deepEqual(Object.keys(FIX_TABLE).filter((id) => !ids.has(id) || NEVER_RAISES.has(id)), []);
  });

  test("the rules said never to raise an issue can't: none of their code returns one", () => {
    for (const id of NEVER_RAISES) {
      const rule = rules.find((r) => r.id === id);
      assert.ok(rule, id);
      assert.doesNotMatch(rule.evaluate.toString(), /outcome:\s*"issue"/, id);
    }
  });

  test("every declaration is a sane range: whole hours from 1, high at least low, and a unit rate with a cap", () => {
    for (const [id, { hours }] of Object.entries(FIX_TABLE)) {
      if (hours.kind === "fixed") assert.ok(Number.isInteger(hours.low) && hours.low >= 1 && Number.isInteger(hours.high) && hours.high >= hours.low && hours.basis.length > 10, id);
      else assert.ok(hours.minutes[0] >= 1 && hours.minutes[1] >= hours.minutes[0] && Number.isInteger(hours.cap) && hours.cap >= 1 && hours.capReason.length > 10, id);
    }
  });
});

describe("hours to fix", () => {
  const flows = FIX_TABLE["aut.flows-without-fault-paths"]!.hours;
  const fields = FIX_TABLE["cfg.unused-custom-fields"]!.hours;

  test("a unit rate is the count times the minutes, rounded up to whole hours, with the count in the basis", () => {
    assert.deepEqual(effortHours(flows, { affected: 6 }), { low: 6, high: 12, basis: "About 1 to 2 hours for each flow, for 6 flows." });
    assert.deepEqual(effortHours(flows, { affected: 1 }), { low: 1, high: 2, basis: "About 1 to 2 hours for each flow, for 1 flow." });
    // 3 fields at 30 to 60 minutes: 1.5 hours rounds up to 2, and 3 hours.
    assert.deepEqual(effortHours(fields, { unusedFields: 3 }), { low: 2, high: 3, basis: "About 30 to 60 minutes for each field, for 3 fields." });
  });

  test("the cap: 300 fields claim the cap, not 300 hours, and the basis says why", () => {
    const h = effortHours(fields, { unusedFields: 300 })!;
    assert.equal(h.high, 16);
    assert.equal(h.low, 16);
    assert.match(h.basis, /for 300 fields, held at 16 hours at most: past that the work is done in bulk/);
    // Just under the cap, nothing is held.
    assert.deepEqual(effortHours(fields, { unusedFields: 16 }), { low: 8, high: 16, basis: "About 30 to 60 minutes for each field, for 16 fields." });
  });

  test("no count the finding holds, no range: never a guess", () => {
    assert.equal(effortHours(flows, {}), null);
    assert.equal(effortHours(flows, { affected: 0 }), null);
    assert.equal(effortHours(flows, { affected: 2.5 }), null);
    assert.equal(effortHours(flows, { flowsWritingData: 9 }), null);
  });

  test("a fixed range is the declaration as it stands", () => {
    assert.deepEqual(effortHours(FIX_TABLE["cfg.stage-sprawl"]!.hours, { activeStages: 40 }), { low: 4, high: 8, basis: "Preparing the analysis and options for the people who decide, and making the agreed change; their time deciding isn't counted." });
  });
});

describe("the synthetic org's findings carry both", () => {
  test("every issue in the recorded and the generated run has hours and a prompt; no pass or not measurable finding has either", () => {
    for (const findings of [recorded, generated]) {
      const issuesHere = findings.filter((f) => f.status === "issue");
      assert.ok(issuesHere.length > 30);
      assert.deepEqual(issuesHere.filter((f) => !f.effortHours).map((f) => f.ruleId), [], "issues with no hours");
      assert.deepEqual(issuesHere.filter((f) => !f.fixPrompt).map((f) => f.ruleId), [], "issues with no prompt");
      const others = findings.filter((f) => f.status !== "issue");
      assert.ok(others.some((f) => f.status === "passed") && others.some((f) => f.status === "not_measurable"));
      for (const f of others) assert.ok(f.effortHours === undefined && f.fixPrompt === undefined, f.ruleId);
    }
  });

  test("worked by hand: 2 flows, 20 articles, and 59 reports", () => {
    // 2 flows at 1 to 2 hours each.
    assert.deepEqual(one("aut.hardcoded-ids-in-flows").effortHours, { low: 2, high: 4, basis: "About 1 to 2 hours for each flow, for 2 flows." });
    // 20 articles at 15 to 30 minutes: 5 to 10 hours.
    assert.deepEqual(one("svc.knowledge-current").effortHours, { low: 5, high: 10, basis: "About 15 to 30 minutes for each article, for 20 articles." });
    // 59 reports at 2 to 5 minutes: 118 minutes rounds up to 2 hours, 295 to 5.
    assert.deepEqual(one("utl.unused-reports").effortHours, { low: 2, high: 5, basis: "About 2 to 5 minutes for each report, for 59 reports." });
  });

  test("each prompt has the one shape, under the length limit", () => {
    for (const f of [...recorded, ...generated]) {
      if (!f.fixPrompt) continue;
      const p = f.fixPrompt;
      assert.ok(p.length <= FIX_PROMPT_MAX, `${f.ruleId}: ${p.length}`);
      const at = ["What we found: ", "What good looks like: ", "Steps:\n1. ", "Do not:\n- ", "To check it worked:\n- "].map((h) => p.indexOf(h));
      assert.ok(at.every((i, n) => i > 0 && (n === 0 || i > at[n - 1]!)), f.ruleId);
      assert.ok(p.includes("Work in a sandbox") || p.includes("work in a sandbox"), f.ruleId);
      assert.ok(p.includes("Delete or deactivate anything until you have checked what depends on it"), f.ruleId);
    }
  });

  test("a decision for people says so, and asks for the analysis, not a change", () => {
    const p = one("cfg.stage-sprawl").fixPrompt!;
    assert.match(p, /This is a decision for people, not a change to the org\. Do not change the org/);
    assert.match(p, /- Change any setting, metadata or record in the org: this task is analysis only\./);
    assert.doesNotMatch(one("aut.hardcoded-ids-in-flows").fixPrompt!, /decision for people/);
  });

  test("a remove that a guard held back says not to remove anything", () => {
    const blocked = issues.filter((f) => f.ruleId === "cfg.unused-custom-fields");
    assert.ok(blocked.length > 0 && blocked.every((f) => f.recommendationType === "investigate"));
    for (const f of blocked) assert.match(f.fixPrompt!, /1\. Not every safety check passed, so do not remove, deactivate or merge anything/);
    // A deactivation every guard cleared has no such line.
    const clear = one("utl.never-logged-in");
    assert.equal(clear.recommendationType, "deactivate");
    assert.doesNotMatch(clear.fixPrompt!, /Not every safety check passed/);
  });
});

describe("a fix prompt never names a person or a record (rule 7)", () => {
  test("no prompt in either run holds a planted name, username, email or id, or a token", () => {
    for (const [findings, records] of [[recorded, SYNTHETIC_RECORDS], [generated, generatedRecords]] as const) {
      const names = planted(records);
      assert.ok(names.includes("Sam Reed") || names.some((n) => n.startsWith("Synthetic User")), "the planted names were collected");
      for (const f of findings) {
        if (!f.fixPrompt) continue;
        for (const name of names) assert.doesNotMatch(f.fixPrompt, word(name), `${f.ruleId} holds a planted name or id`);
        assert.doesNotMatch(f.fixPrompt, /\[\[|@/, f.ruleId);
        assert.doesNotMatch(f.fixPrompt, /\b(?=[a-zA-Z0-9]*\d)[a-zA-Z0-9]{15}(?:[a-zA-Z0-9]{3})?\b/, f.ruleId);
      }
    }
  });

  test("a prompt names a subject only where the finding's title or summary shows it, and never a person or a record", () => {
    let named = 0;
    for (const f of [...recorded, ...generated]) {
      if (!f.fixPrompt) continue;
      const shown = `${f.title}\n${f.summary}`;
      for (const s of f.subjects) {
        if (!word(s.label).test(f.fixPrompt)) continue;
        assert.ok(shown.includes(s.label), `${f.ruleId} names ${s.type} "${s.label}", which its title and summary don't show`);
        assert.ok(!["user", "rep", "owner", "account", "dashboard", "report", "scheduled job"].includes(s.type), `${f.ruleId} names a ${s.type}`);
        named++;
      }
    }
    assert.ok(named > 0, "some prompt names set-up its finding shows");
    // The flows aren't in the title or summary, so the prompt refers to them.
    const flows = one("aut.hardcoded-ids-in-flows");
    assert.ok(flows.subjects.length > 0 && flows.subjects.every((s) => !flows.fixPrompt!.includes(s.label)));
    assert.match(flows.fixPrompt!, /Work on the flows listed in this finding\./);
    // A person in the summary is "one rep", as the executive report says it.
    assert.match(one("dq.open-opps-past-close").fixPrompt!, /The worst rep is at 50%\./);
  });

  test("the guard: a name, username, email, id or token, or a withheld subject, keeps the prompt off", () => {
    const pseudonyms = new Pseudonymiser();
    pseudonyms.label("user", "005000000000005AAA", "Sam Reed", ["sam.reed@example.com"]);
    const base = { title: "Flows break", summary: "2 flows hold ids.", recommendation: "Replace them.", subjects: [{ type: "flow", label: "Route_Deal" }] };
    const rule = FIX_TABLE["aut.hardcoded-ids-in-flows"]!.prompt;
    assert.ok(fixPrompt(base, rule, pseudonyms));
    for (const leak of ["Ask Sam Reed.", "Ask sam.reed@example.com.", "See 005000000000005AAA.", "See 005000000000005.", "Ask [[user:U0001]] and [[email]].", "Fix Route_Deal first."]) {
      const text = `${assemblePrompt(base, rule)}\n${leak}`;
      assert.equal(promptIsSafe(text, base, pseudonyms), false, leak);
    }
    assert.equal(promptIsSafe("x".repeat(FIX_PROMPT_MAX + 1), base, pseudonyms), false);
    // A name a rule let into its recommendation keeps the whole prompt off.
    assert.equal(fixPrompt({ ...base, recommendation: "Ask Sam Reed to replace them." }, rule, pseudonyms), null);
  });

  test("tokens become words, a person's subject is never named, and set-up the summary doesn't show is withheld from the recommendation", () => {
    const f = {
      title: "Dashboards run as people who've left",
      summary: "Worst by rep: [[user:U0003]], at 50%. Account [[account:A0001]] holds most. Dashboard EU Pipeline runs as [[user:U0004]].",
      recommendation: "Move Old_Flow and EU Pipeline to a current user.",
      subjects: [
        { type: "dashboard", label: "EU Pipeline" },
        { type: "flow", label: "Old_Flow" },
        { type: "user", label: "[[user:U0004]]" },
      ],
    };
    assert.deepEqual(withheldLabels(f).sort(), ["EU Pipeline", "Old_Flow", "[[user:U0004]]"]);
    const text = assemblePrompt(f, FIX_TABLE["utl.dashboards-inactive-running-user"]!.prompt);
    assert.match(text, /The worst rep is at 50%\. Account a record listed in this finding holds most\. Dashboard the dashboard listed in this finding runs as one person\./);
    assert.match(text, /What good looks like: Move the flow listed in this finding and the dashboard listed in this finding to a current user\./);
    assert.ok(promptIsSafe(text, f, new Pseudonymiser()));
  });
});

describe("the schema (decision 628)", () => {
  const issue = issues[0]!;
  test("a result written before this change, with neither field, still parses and reads as before", () => {
    const { effortHours: _h, fixPrompt: _p, ...older } = issue;
    const parsed = Finding.parse(older);
    assert.equal(parsed.effortHours, undefined);
    assert.equal(parsed.fixPrompt, undefined);
    assert.deepEqual(Object.keys(parsed), Object.keys(older));
  });

  test("the range runs low to high in whole hours from 1, and a prompt keeps to its length", () => {
    for (const bad of [{ low: 0, high: 2, basis: "x" }, { low: 3, high: 2, basis: "x" }, { low: 1.5, high: 2, basis: "x" }, { low: 1, high: 2, basis: "" }]) assert.equal(Finding.safeParse({ ...issue, effortHours: bad }).success, false, JSON.stringify(bad));
    assert.equal(Finding.safeParse({ ...issue, fixPrompt: "x".repeat(FIX_PROMPT_MAX + 1) }).success, false);
    assert.equal(Finding.safeParse({ ...issue, fixPrompt: "x".repeat(FIX_PROMPT_MAX) }).success, true);
  });
});

describe("nothing is printed: every document and figure is as it was (decision 628)", () => {
  test("the report, its figures and scores, and every document built from it are the same with the hours and prompts or without them", () => {
    const { output, run, dir } = recordedRun;
    const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
    assert.ok(output!.findings.some((f) => f.fixPrompt && f.effortHours));
    const without = output!.findings.map(({ effortHours: _h, fixPrompt: _p, ...rest }) => rest as Finding);
    const base = { run, calibration: loadCalibration(), metrics: output!.metrics, manualSteps: output!.manualSteps };
    const withFix = buildReport({ ...base, findings: output!.findings });
    const before = buildReport({ ...base, findings: without });
    const printed = (report: typeof withFix) => ({
      outputs: JSON.stringify(report.outputs),
      dataset: JSON.stringify(report.dataset),
      briefs: JSON.stringify([...report.briefs]),
      scores: JSON.stringify({ overall: report.overall, value: report.value, added: report.added }),
      executive: executiveHtml(report, pseudonyms),
      full: fullReportHtml(report, pseudonyms),
      onePager: onePagerHtml(report, pseudonyms),
      kit: JSON.stringify(readoutKit(report)),
      notes: JSON.stringify(buildRawNotes(report, pseudonyms)),
    });
    const a = printed(withFix);
    const b = printed(before);
    for (const key of Object.keys(a) as Array<keyof typeof a>) assert.ok(a[key] === b[key], `${key} moved`);
    for (const [key, text] of Object.entries(a)) assert.doesNotMatch(text, /What good looks like: |About \d+ to \d+ (minutes|hours) for each/, key);
    const notes = (findings: Finding[]) => renderRawNotes({ run, calibration: loadCalibration(), findings, metrics: output!.metrics, manualSteps: output!.manualSteps, pseudonyms });
    assert.equal(notes(output!.findings), notes(without));
  });
});
