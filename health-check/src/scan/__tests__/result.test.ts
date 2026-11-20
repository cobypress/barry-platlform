import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { z } from "zod";
import { ALL_MODULES } from "../../domains/index.ts";
import { modulesFor } from "../../domains/scope.ts";
import { DOCUMENTED } from "../../domains/configuration/documented.ts";
import { customObjectsExtractor } from "../../extract/configuration.ts";
import { buildComplexity } from "../../complexity/read.ts";
import { loadRun } from "../../render/render-run.ts";
import { buildReport, type BuiltReport } from "../../narrative/sections.ts";
import { clone, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import { AS_OF, ORG_ID, usersTable } from "../../testing/synthetic-org.ts";
import { complexityOf } from "../../worker/run-job.ts";
import type { Complexity } from "../../schemas/complexity.ts";
import type { Finding } from "../../schemas/index.ts";
import { buildScanResult, type ScanResult } from "../result.ts";

// The free scan's result gains, as optional fields, each shown finding's fix and
// where it is in Setup, each area's issues by severity, and what the org holds
// (DECISIONS.md 623). Nothing it had before changes for the same run.

let dir: string;
let report: BuiltReport;
let complexity: Complexity | null;
let result: ScanResult;

before(async () => {
  dir = (await runFromRecording({ scope: "essentials", inventory: true, modules: ALL_MODULES })).dir;
  report = buildReport(loadRun(dir).input);
  complexity = complexityOf(dir, () => {});
  result = buildScanResult(report, complexity);
});

/** The result with only the fields it had before decision 623. */
const before623 = (r: ScanResult) => ({
  score: r.score,
  grade: r.grade,
  areas: r.areas.map(({ domain, label, score, issues, high }) => ({ domain, label, score, issues, high })),
  shown: r.shown.map(({ title, summary, severity, area }) => ({ title, summary, severity, area })),
  lockedIssues: r.lockedIssues,
});

/** A copy of the report whose team findings are changed, as a run of another org would give them. */
const withFindings = (edit: (findings: Finding[]) => Finding[]): BuiltReport => ({ ...report, teamFindings: edit(clone(report.teamFindings)) });

describe("the synthetic org's scan result (decision 623)", () => {
  test("keeps every field it had, byte for byte as the engine wrote them before 623", () => {
    // Written down from buildScanResult at 6d8f5fd, the tip before 623, for this run.
    assert.deepEqual(before623(result), {
      score: 69,
      grade: "C",
      areas: [
        { domain: "configuration", label: "Configuration", score: 79, issues: 5, high: 0 },
        { domain: "automation", label: "Automation", score: 53, issues: 7, high: 3 },
        { domain: "technical_health", label: "Technical health", score: 73, issues: 7, high: 0 },
        { domain: "security", label: "Security and access", score: 64, issues: 7, high: 2 },
        { domain: "integrations", label: "Integrations", score: 63, issues: 8, high: 1 },
        { domain: "best_practice", label: "Best practice", score: 74, issues: 3, high: 0 },
        { domain: "utilisation", label: "Utilisation", score: 74, issues: 7, high: 1 },
        { domain: "adoption", label: "Adoption and usage", score: 68, issues: 3, high: 1 },
        { domain: "account_engagement", label: "Marketing handover", score: null, issues: 1, high: 0 },
        { domain: "cpq_setup", label: "CPQ set-up", score: 77, issues: 3, high: 0 },
        { domain: "service_setup", label: "Service set-up", score: 71, issues: 6, high: 1 },
      ],
      shown: [
        {
          title: "Any user can authorise some connected apps",
          summary: "1 of 2 connected apps let users authorise them for themselves, without an admin approving who can: Data Loader. Data Loader has had API logins since 2 Aug 2026, so it carries data now.",
          severity: "high",
          area: "Integrations",
        },
        {
          title: "Automation still runs on Workflow Rules",
          summary: "3 active Workflow Rules across 3 objects. Salesforce ended support for Workflow Rules and Process Builder on 31 December 2025. They keep running, but any bug is yours to work around.",
          severity: "high",
          area: "Automation",
        },
        { title: "High-risk security settings need changing", summary: "Of 5 settings Salesforce checks, 2 are rated high risk and 1 medium risk.", severity: "high", area: "Security and access" },
        {
          title: "Escalations go to people who can't act on them",
          summary: "2 of 2 escalation actions in active rules assign cases to an inactive user or a queue with no members, so an escalated case lands where nobody will see it.",
          severity: "high",
          area: "Service set-up",
        },
      ],
      lockedIssues: 53,
    });
  });

  test("with or without the complexity file, the fields from before are the same", () => {
    assert.deepEqual(before623(buildScanResult(report)), before623(result));
  });

  test("each new field, as worked out by hand", () => {
    // Each area's issues in the synthetic org, counted from the findings listed by severity.
    assert.deepEqual(
      result.areas.map((a) => [a.domain, a.bySeverity]),
      [
        ["configuration", { critical: 0, high: 0, medium: 2, low: 3 }],
        ["automation", { critical: 0, high: 3, medium: 4, low: 0 }],
        ["technical_health", { critical: 0, high: 0, medium: 6, low: 1 }],
        ["security", { critical: 0, high: 2, medium: 3, low: 2 }],
        ["integrations", { critical: 0, high: 1, medium: 7, low: 0 }],
        ["best_practice", { critical: 0, high: 0, medium: 2, low: 1 }],
        ["utilisation", { critical: 0, high: 1, medium: 2, low: 4 }],
        ["adoption", { critical: 0, high: 1, medium: 1, low: 1 }],
        ["account_engagement", { critical: 0, high: 0, medium: 0, low: 1 }],
        ["cpq_setup", { critical: 0, high: 0, medium: 2, low: 1 }],
        ["service_setup", { critical: 0, high: 1, medium: 3, low: 2 }],
      ],
    );
    // `high` keeps its meaning: critical and high together.
    for (const a of result.areas) assert.equal(a.high, a.bySeverity!.critical + a.bySeverity!.high, a.domain);
    assert.deepEqual(
      result.shown.map((f) => [f.fix, f.where]),
      [
        ["Set each app to admin-approved users only, then give access through a permission set to the people and integration accounts that need it.", ["Setup", "Apps", "Connected Apps", "Manage Connected Apps"]],
        [
          "Plan the move from Workflow Rules to Flow, starting with the objects that have the most rules. Salesforce's Migrate to Flow tool handles the simple cases, but it isn't always a like-for-like move.",
          ["Setup", "Process Automation", "Workflow Rules"],
        ],
        ["Bring the high-risk settings up to Salesforce's standard value, then plan the medium-risk ones.", ["Setup", "Security", "Health Check"]],
        ["Point each escalation at a queue with members, not a named person, and add escalation targets to your leaver checklist.", ["Setup", "Feature Settings", "Service", "Escalation Rules"]],
      ],
    );
    // The synthetic org: 4 custom objects of its own, 8 custom fields, 3 record-triggered flows and 1 other,
    // 4 Apex classes that aren't tests, 3 installed packages and 14 active standard users.
    assert.deepEqual(result.foundInOrg, [
      { id: "customObjects", label: "Custom objects of your own", count: 4 },
      { id: "customFields", label: "Custom fields of your own, on core and custom objects", count: 8 },
      { id: "flows", label: "Active flows", count: 4 },
      { id: "apexClasses", label: "Apex classes, not counting tests", count: 4 },
      { id: "packages", label: "Installed packages", count: 3 },
      { id: "activeUsers", label: "Active users", count: 14 },
    ]);
    assert.equal(usersTable.filter((u) => u.IsActive).length, 14);
  });

  test("each fix is the finding's own recommendation, as the report prints it", () => {
    const issues = report.teamFindings.filter((f) => f.status === "issue");
    for (const shown of result.shown) {
      const f = issues.find((x) => x.title === shown.title)!;
      assert.equal(shown.fix, f.recommendation);
    }
  });

  test("locked findings gain nothing: there are still only the shown ones, and no other recommendation is in the result", () => {
    const text = JSON.stringify(result);
    const shownTitles = new Set(result.shown.map((f) => f.title));
    const locked = report.teamFindings.filter((f) => f.status === "issue" && !shownTitles.has(f.title));
    assert.equal(locked.length, result.lockedIssues);
    for (const f of locked) {
      assert.ok(!text.includes(f.title), f.title);
      // A locked finding can share its recommendation's words with a shown one (one rule's several findings); none does here.
      if (!result.shown.some((s) => s.fix === f.recommendation)) assert.ok(!text.includes(f.recommendation), f.ruleId);
    }
  });
});

describe("where a finding is in Setup", () => {
  test("a finding whose rule has no Setup location has none", () => {
    const first = result.shown[0]!;
    const changed = buildScanResult(
      withFindings((fs) => fs.map((f) => (f.title === first.title ? { ...f, ruleId: "utl.eac-connections" } : f))),
      complexity,
    );
    assert.equal(changed.shown[0]!.title, first.title);
    assert.equal("where" in changed.shown[0]!, false);
    assert.equal(changed.shown[0]!.fix, first.fix, "the fix is the finding's own, whatever its rule");
  });

  test("every Setup location is plain labels from Setup: no address, no id, no punctuation a name would bring", () => {
    const rules = ALL_MODULES.flatMap((m) => m.rules).filter((r) => r.where);
    assert.ok(rules.length >= 30, `${rules.length} rules have one`);
    for (const r of rules) {
      assert.equal(r.where![0], "Setup", r.id);
      assert.ok(r.where!.length >= 2, r.id);
      for (const label of r.where!) assert.match(label, /^[A-Z][A-Za-z ]+$/, `${r.id}: ${label}`);
    }
  });

  test("the rules a set-up-only scan can raise that have one, and those that don't", () => {
    const scan = modulesFor("essentials", ALL_MODULES).flatMap((m) => m.rules);
    assert.deepEqual(scan.filter((r) => r.where).map((r) => r.id).sort(), [
      "aut.apex-coverage",
      "aut.flows-without-fault-paths",
      "aut.hardcoded-ids-in-flows",
      "aut.legacy-process-builder",
      "aut.legacy-workflow-rules",
      "aut.multiple-triggers",
      "bp.after-save-same-record-updates",
      "bp.jobs-on-inactive-owners",
      "bp.users-without-role",
      "cfg.custom-field-sprawl",
      "cfg.setup-change-concentration",
      "cfg.unassigned-layouts",
      "cfg.undocumented-fields",
      "cpq.twin-field-mismatch",
      "int.failed-async-jobs",
      "mkt.campaign-influence",
      "sec.health-check-risks",
      "sec.health-check-score",
      "sec.insecure-remote-sites",
      "sec.self-authorised-connected-apps",
      "sec.sharing-defaults",
      "svc.escalation-rules",
      "svc.escalation-targets",
      "svc.queue-sprawl",
      "tech.storage-use",
      "tech.test-quality",
      "utl.dormant-licensed-users",
      "utl.never-logged-in",
      "utl.package-seats",
      "utl.unassigned-licence-seats",
    ]);
    assert.equal(scan.length, 61);
  });
});

describe("a fix that could name someone or something is left out", () => {
  const shownWith = (recommendation: string, subjects: Finding["subjects"] = []) => {
    const first = result.shown[0]!;
    return buildScanResult(withFindings((fs) => fs.map((f) => (f.title === first.title ? { ...f, recommendation, subjects } : f))), complexity).shown[0]!;
  };

  test("a token, an email, an id or a subject's name: no fix, and the rest of the finding as before", () => {
    for (const [recommendation, subjects] of [
      ["Ask [[user:U0003]] to approve it.", []],
      ["Ask admin@example.com to approve it.", []],
      [`Reassign what ${"005000000000003AAA"} owns.`, []],
      ["Reassign what 005000000000003 owns.", []],
      ["Deactivate Old_Lead_Router first.", [{ type: "flow", label: "Old_Lead_Router" }]],
    ] as Array<[string, Finding["subjects"]]>) {
      const shown = shownWith(recommendation, subjects);
      assert.equal("fix" in shown, false, recommendation);
      assert.equal(shown.title, result.shown[0]!.title);
      assert.deepEqual(shown.where, result.shown[0]!.where);
    }
  });

  test("an empty recommendation gives no fix", () => {
    assert.equal("fix" in shownWith(""), false);
  });

  test("a fix in plain words, with a date as the reports print it", () => {
    assert.equal(shownWith("Review it before 2026-12-31.").fix, "Review it before 31 Dec 2026.");
  });
});

describe("what the org holds", () => {
  test("a count that wasn't read whole isn't there, and the others are", () => {
    const extracts = loadExtracts(dir);
    const objects = extracts.get(customObjectsExtractor.id) as { truncated: boolean };
    objects.truncated = true;
    const cut = buildScanResult(report, buildComplexity(extracts, new Date(AS_OF)));
    // The data model's counts go together: the custom objects were cut short, so neither is shown.
    assert.deepEqual(cut.foundInOrg!.map((c) => c.id), ["flows", "apexClasses", "packages", "activeUsers"]);
  });

  test("a component not read, or no complexity file at all, leaves its counts out", () => {
    const notRead: Complexity = clone(complexity!);
    notRead.components = notRead.components.map((c) => (c.id === "code" ? { status: "not_read", id: c.id, label: c.label, weight: c.weight, reason: "The size of some code wasn't read." } : c));
    assert.deepEqual(buildScanResult(report, notRead).foundInOrg!.map((c) => c.id), ["customObjects", "customFields", "flows", "packages", "activeUsers"]);
    assert.deepEqual(buildScanResult(report).foundInOrg, [{ id: "activeUsers", label: "Active users", count: 14 }]);
  });

  test("active users only when the users were read", () => {
    const metrics = report.input.metrics.map((m) => (m.id === DOCUMENTED.access ? { ...m, status: "not_measurable" as const, value: null, reason: "Users couldn't be read: refused." } : m));
    const noUsers = buildScanResult({ ...report, input: { ...report.input, metrics: metrics as typeof report.input.metrics } }, complexity);
    assert.equal(noUsers.foundInOrg!.some((c) => c.id === "activeUsers"), false);
    assert.equal(noUsers.foundInOrg!.length, 5);
  });
});

describe("privacy: the result is shown to whoever holds its link", () => {
  test("nothing in it holds a planted user's name, username, email or id, the org's id, or any id or address", () => {
    const text = JSON.stringify(result);
    const planted = usersTable.flatMap((u) => [u.Id, u.Id.slice(0, 15), u.Name, u.Username, u.Email]);
    assert.deepEqual(planted.filter((p) => text.includes(p)), []);
    assert.equal(text.includes(ORG_ID), false);
    assert.equal(text.includes(ORG_ID.slice(0, 15)), false);
    assert.doesNotMatch(text, /@|\[\[[a-z]+:|\b[a-zA-Z0-9]{3}0{6,}\d+(?:AAA)?\b|https?:/);
  });
});

describe("the console reads either shape", () => {
  // The console's schema as it stands (blackcloud-next lib/health-check/scan/records.ts,
  // ScanResultBody): zod objects there strip keys they don't know rather than refuse them.
  const ConsoleArea = z.object({ domain: z.string().min(1), label: z.string().min(1), score: z.number().min(0).max(100).nullable(), issues: z.number().int().nonnegative(), high: z.number().int().nonnegative() });
  const ConsoleFinding = z.object({ title: z.string().min(1), summary: z.string().min(1), severity: z.string().min(1), area: z.string().min(1) });
  const ConsoleBody = z.object({ score: z.number().min(0).max(100).nullable(), grade: z.string().min(1).nullable(), areas: z.array(ConsoleArea), shown: z.array(ConsoleFinding).max(4), lockedIssues: z.number().int().nonnegative() });
  // What the console would add to read 623's fields: each optional, so an older result still parses.
  const Count = z.number().int().nonnegative();
  const NewBody = ConsoleBody.extend({
    areas: z.array(ConsoleArea.extend({ bySeverity: z.object({ critical: Count, high: Count, medium: Count, low: Count }).optional() })),
    shown: z.array(ConsoleFinding.extend({ fix: z.string().min(1).optional(), where: z.array(z.string().min(1)).min(1).optional() })).max(4),
    foundInOrg: z.array(z.object({ id: z.string().min(1), label: z.string().min(1), count: Count })).optional(),
  });

  test("today's console reads the new result, and keeps every field it knows", () => {
    assert.deepEqual(ConsoleBody.parse(result), before623(result));
  });

  test("a result without the new fields still parses, with the schema that knows them", () => {
    const old = before623(result);
    assert.deepEqual(NewBody.parse(old), old);
    assert.deepEqual(NewBody.parse(result), result);
  });
});
