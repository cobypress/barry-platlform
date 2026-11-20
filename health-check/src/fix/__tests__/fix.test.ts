import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { ALL_MODULES } from "../../domains/index.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { buildReport } from "../../narrative/sections.ts";
import { applyOverrides } from "../../narrative/overrides.ts";
import { findNames } from "../../privacy/names.ts";
import { renderRawNotes } from "../../notes/raw-notes.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { executiveHtml } from "../../render/executive.ts";
import { fullReportHtml } from "../../render/full-report.ts";
import { onePagerHtml } from "../../render/one-pager.ts";
import { readoutKit } from "../../render/readout-kit.ts";
import { buildRawNotes } from "../../render/raw-notes-docx.ts";
import { Finding, FIX_PROMPT_MAX, type NumberOverride, type RecommendationType } from "../../schemas/index.ts";
import { runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { buildScaleOrg, FIVE_HUNDRED_USERS, scaled } from "../../testing/scale-org.ts";
import { Simulator, SYNTHETIC_RECORDS, type OrgRecords } from "../../testing/simulator.ts";
import { AS_OF, CONNECTED_USER, ORG_ID } from "../../testing/synthetic-org.ts";
import { effortHours } from "../hours.ts";
import { assemblePrompt, fixPrompt, FRAMING, isHeldBack, labelHoldsPersonName, labelWords, MIN_NAME_PART, promptIsSafe, withheldLabels, type PromptFinding } from "../prompt.ts";
import { FIX_TABLE, NEVER_RAISES } from "../table.ts";

// Every issue carries hours to fix and a fix prompt (DECISIONS.md 628). The hours
// come from one table, and the prompt is assembled by code, never names a person
// or a record, never asks the assistant to change records (principle A), and
// never takes access away before its replacement is in place (principle B).

const rules = ALL_MODULES.flatMap((m) => m.rules);
const generatedRecords = buildScaleOrg(scaled(FIVE_HUNDRED_USERS, 0.005));
const readFindings = (dir: string): Finding[] => (JSON.parse(readFileSync(join(dir, "findings.json"), "utf8")) as unknown[]).map((f) => Finding.parse(f));
const recordedRun = await runFromRecording({ answers: loadAnswers(), modules: ALL_MODULES });
const recorded = readFindings(recordedRun.dir);
const generated = readFindings(
  (await runHealthCheck({ provider: syntheticProvider, connectedUser: CONNECTED_USER, calibration: loadCalibration(), answers: loadAnswers(), modules: ALL_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport: new Simulator({ records: generatedRecords, pageSize: 2000 }) })).dir,
);
const issues = recorded.filter((f) => f.status === "issue");
const allPrompts = [...recorded, ...generated].filter((f) => f.fixPrompt);
const one = (ruleId: string, key?: (f: Finding) => boolean, findings: Finding[] = issues): Finding => {
  const found = findings.filter((f) => f.status === "issue" && f.ruleId === ruleId && (key?.(f) ?? true));
  assert.equal(found.length, 1, ruleId);
  return found[0]!;
};

/** The reviewer's pseudonymiser: two people, one with a surname that is also a word in an API name. */
function reviewerPseudonyms(): Pseudonymiser {
  const p = new Pseudonymiser();
  p.label("user", "005000000000005AAA", "Sam Reed", ["sam.reed@example.com"]);
  p.label("user", "005000000000006AAA", "Priya Natarajan", ["priya@example.com"]);
  return p;
}

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

/**
 * The whole prompt but three parts: what we found, the one framed line holding
 * the report's recommendation (the finding's own words, for the owners, framed
 * as not the assistant's task), and the "Do not" list.
 */
function instructions(prompt: string): string {
  return prompt
    .split("\n\n")
    .filter((s) => !s.startsWith("What we found: ") && !s.startsWith(FRAMING) && !s.startsWith("Do not:"))
    .join("\n\n");
}

/** A record change, as principle A names them. */
const RECORD_CHANGE = /\bdeactivat\w*\s+(?:the\s+|each\s+|these\s+|this\s+|a\s+)?users?\b|\bmerg\w*|\breassign\w*|\btransfer\w*|\bmass[- ]?updat\w*|\barchiv\w*|\bdelet\w*|\bcreat\w* (?:the |each |a |an )?(?:renewal|opportunit\w*)|\bmake (?:the |each |a |an )?(?:renewal )?opportunit\w*/i;

/** A made-up finding for any rule, in each recommendation type, so a rule the synthetic org never fires is held too. */
const sample = (recommendationType: RecommendationType): PromptFinding => ({ title: "A planted finding", summary: "2 items are affected.", recommendation: "Look into it.", recommendationType, subjects: [] });

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
      else assert.ok(hours.minutes[0] >= 1 && hours.minutes[1] >= hours.minutes[0] && Number.isInteger(hours.cap) && hours.cap >= 1 && (hours.facts === "all" || hours.facts.length > 0), id);
    }
  });

  test("the analysis-only rules are exactly these: each a decision for people or a change to records (finding 7)", () => {
    const analysis = Object.entries(FIX_TABLE).filter(([, r]) => r.prompt.decision).map(([id]) => id).sort();
    assert.deepEqual(analysis, ANALYSIS_ONLY);
    // Every rule that can remove, deactivate or merge is analysis only.
    for (const [id, r] of Object.entries(FIX_TABLE)) if (r.prompt.removes) assert.ok(r.prompt.decision, id);
  });
});

/** The final set of analysis-only rules (decision 628, finding 7). */
const ANALYSIS_ONLY = [
  "act.deals-without-activity", "act.rep-activity-spread", "act.won-vs-lost-activity", "adp.classic-users", "adp.falling-adoption", "adp.low-engagement-group",
  "bp.jobs-on-inactive-owners", "bp.ownership-skew", "bp.users-without-role", "cfg.custom-field-sprawl", "cfg.picklist-sprawl", "cfg.setup-change-concentration",
  "cfg.stage-sprawl", "cfg.unassigned-layouts", "cfg.unused-custom-fields", "cpq.discount-leakage", "cpq.inactive-rules", "cpq.price-rule-conflicts",
  "cpq.twin-field-mismatch", "cur.account-concentration", "cur.coverage", "cur.rep-concentration", "dq.contact-completeness", "dq.duplicate-accounts",
  "dq.opps-inactive-owner", "dq.record-type-use", "dq.records-inactive-owner", "dq.stale-leads", "fct.manager-adjustments", "fct.quota-gaps",
  "int.api-usage", "int.integration-user-licences", "lead.campaign-return", "lead.conversion-by-source", "lead.converted-no-opportunity", "lead.source-return",
  "mkt.scoring-grading", "mkt.unowned-prospects", "mkt.unworked-handovers", "pipe.calibration-contradicts-org", "pipe.going-backwards", "pipe.segment-divergence",
  "pipe.skipped-stages", "pipe.unmapped-stages", "rev.dormant-accounts", "rev.stalled-deals", "rev.win-backs", "sec.health-check-risks", "sec.health-check-score", "sec.peer-outlier-access",
  "sec.personal-integration-accounts", "sec.sharing-defaults", "sec.unused-access", "svc.agent-licences", "svc.backlog-growth", "svc.knowledge-current",
  "svc.macros-and-quick-text", "svc.queue-sprawl", "svc.unassigned-cases", "tech.apex-code-size", "tech.storage-use", "utl.dashboards-inactive-running-user",
  "utl.dormant-licensed-users", "utl.eac-connections", "utl.never-logged-in", "utl.package-seats", "utl.unassigned-licence-seats", "utl.unused-reports",
];

describe("hours to fix", () => {
  const flows = FIX_TABLE["aut.flows-without-fault-paths"]!.hours;
  const fields = FIX_TABLE["cfg.unused-custom-fields"]!.hours;
  const reports = FIX_TABLE["utl.unused-reports"]!.hours;

  test("a unit rate is the count times the minutes, rounded up to whole hours, with what the count counts in the basis", () => {
    assert.deepEqual(effortHours(flows, { affected: 6 }), { low: 6, high: 12, basis: "About 1 to 2 hours for each flow with no fault path, for 6 flows with no fault path." });
    assert.deepEqual(effortHours(flows, { affected: 1 }), { low: 1, high: 2, basis: "About 1 to 2 hours for each flow with no fault path, for 1 flow with no fault path." });
    // 3 fields at 30 to 60 minutes: 1.5 hours rounds up to 2, and 3 hours.
    assert.deepEqual(effortHours(fields, { unusedFields: 3 }), { low: 2, high: 3, basis: "About 30 to 60 minutes for each field almost never filled in, for 3 fields almost never filled in." });
  });

  test("the cap where work grows with every item: the range covers the first items the cap allows and says the rest is scoped separately (finding 6b)", () => {
    const h = effortHours(fields, { unusedFields: 300 })!;
    assert.deepEqual(h, { low: 8, high: 16, basis: "About 30 to 60 minutes for each field almost never filled in, for 300 fields almost never filled in. This range covers the first 16 fields almost never filled in; the other 284 are scoped separately." });
    assert.doesNotMatch(JSON.stringify(Object.values(FIX_TABLE).map((r) => r.hours)), /done in bulk, so it stops growing/);
    // Just under the cap, nothing is held.
    assert.deepEqual(effortHours(fields, { unusedFields: 16 }), { low: 8, high: 16, basis: "About 30 to 60 minutes for each field almost never filled in, for 16 fields almost never filled in." });
  });

  test("no rule holds its range at the cap: every per-item rule says how many items its range covers (round 3, finding 5)", () => {
    const held = Object.entries(FIX_TABLE).filter(([, r]) => r.hours.kind === "each" && r.hours.bulk !== undefined).map(([id]) => id);
    assert.deepEqual(held, []);
    assert.deepEqual(effortHours(reports, { unused: 300 }), { low: 4, high: 8, basis: "About 2 to 5 minutes for each unused report, for 300 unused reports. This range covers the first 96 unused reports; the other 204 are scoped separately." });
    // Rates a consultant would quote, production hand-over included.
    assert.deepEqual(effortHours(FIX_TABLE["sec.modify-all-data"]!.hours, { unexpected: 1 }), { low: 3, high: 6, basis: "About 3 to 6 hours for each person outside the admin team who holds it, for 1 person outside the admin team who holds it." });
    assert.deepEqual(effortHours(FIX_TABLE["sec.view-all-data"]!.hours, { unexpected: 10 })!.basis, "About 3 to 6 hours for each person outside the admin team who holds it, for 10 people outside the admin team who hold it. This range covers the first 4 people outside the admin team who hold it; the other 6 are scoped separately.");
    assert.deepEqual(effortHours(FIX_TABLE["sec.self-authorised-connected-apps"]!.hours, { open: 1 }), { low: 2, high: 4, basis: "About 2 to 4 hours for each connected app users can authorise, for 1 connected app users can authorise." });
    assert.deepEqual(effortHours(FIX_TABLE["svc.escalation-targets"]!.hours, { dead: 2 }), { low: 2, high: 4, basis: "About 1 to 2 hours for each escalation action with no one to act, for 2 escalation actions with no one to act." });
  });

  test("no count the finding holds, no range: never a guess", () => {
    assert.equal(effortHours(flows, {}), null);
    assert.equal(effortHours(flows, { affected: 0 }), null);
    assert.equal(effortHours(flows, { affected: 2.5 }), null);
    assert.equal(effortHours(flows, { flowsWritingData: 9 }), null);
  });

  test("a rule holding several counts sums them, and one holding a count per object sums every fact (finding 6a)", () => {
    const classic = FIX_TABLE["tech.classic-leftovers"]!.hours;
    assert.deepEqual(effortHours(classic, { javascriptButtons: 3, scontrols: 1, classicPages: 2, pages: 40 }), { low: 12, high: 36, basis: "About 2 to 6 hours for each JavaScript button, s-control or Classic-only page, for 6 JavaScript buttons, s-controls and Classic-only pages." });
    // S-controls not read: the others still count, and the total pages never do.
    assert.equal(effortHours(classic, { javascriptButtons: 1, classicPages: 1, pages: 9 })!.basis.includes("for 2 JavaScript"), true);
    assert.deepEqual(effortHours(FIX_TABLE["tech.test-quality"]!.hours, { testClasses: 40, withoutAssertions: 3, seeAllData: 2 })!.low, 5);
    assert.match(effortHours(FIX_TABLE["sec.unused-access"]!.hours, { emptyProfiles: 3, customProfiles: 9, emptySets: 5, ownSets: 30 })!.basis, /for 8 unused profiles and permission sets\.$/);
    assert.match(effortHours(FIX_TABLE["cfg.custom-field-sprawl"]!.hours, { Account: 156, Opportunity: 210 })!.basis, /for 366 custom fields on the objects named\. This range covers the first 240/);
  });

  test("integration hours count what the change touches (finding 6c)", () => {
    // 1 of 4 accounts isn't API only: one account's work, not four.
    assert.equal(effortHours(FIX_TABLE["int.integration-users-interactive"]!.hours, { accounts: 4, open: 1, loggedInByHand: 0 })!.basis, "About 1 to 3 hours for each integration account not set to API only, for 1 integration account not set to API only.");
    // 3 accounts on full licences and none spare: still the work for all 3.
    assert.deepEqual(effortHours(FIX_TABLE["int.integration-user-licences"]!.hours, { accounts: 3, onFull: 3, spare: 0, movable: 0 }), { low: 3, high: 6, basis: "About 1 to 2 hours for each integration account on a full licence, for 3 integration accounts on a full licence." });
    assert.match(effortHours(FIX_TABLE["sec.personal-integration-accounts"]!.hours, { accounts: 2 })!.basis, /for each personal account used for an integration, for 2 personal accounts/);
  });

  test("a fixed range is the declaration as it stands", () => {
    assert.deepEqual(effortHours(FIX_TABLE["cfg.stage-sprawl"]!.hours, { activeStages: 40 }), { low: 4, high: 8, basis: "Preparing the analysis and the options for the people who decide; their time deciding, and making the change, isn't counted." });
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
    assert.deepEqual(one("aut.hardcoded-ids-in-flows").effortHours, { low: 2, high: 4, basis: "About 1 to 2 hours for each flow with hardcoded values, for 2 flows with hardcoded values." });
    // 20 articles at 15 to 30 minutes: 5 to 10 hours.
    assert.deepEqual(one("svc.knowledge-current").effortHours, { low: 5, high: 10, basis: "About 15 to 30 minutes for each article not reviewed in the period, for 20 articles not reviewed in the period." });
    // 59 reports at 2 to 5 minutes: 118 minutes rounds up to 2 hours, 295 to 5.
    assert.deepEqual(one("utl.unused-reports").effortHours, { low: 2, high: 5, basis: "About 2 to 5 minutes for each unused report, for 59 unused reports." });
  });

  test("each prompt has the one shape, under the length limit", () => {
    for (const f of allPrompts) {
      const p = f.fixPrompt!;
      assert.ok(p.length <= FIX_PROMPT_MAX, `${f.ruleId}: ${p.length}`);
      const at = ["What we found: ", FRAMING, "Steps:\n1. ", "Do not:\n- ", "To check it worked:\n- "].map((h) => p.indexOf(h));
      assert.ok(at.every((i, n) => i > 0 && (n === 0 || i > at[n - 1]!)), f.ruleId);
      assert.ok(p.includes("use a sandbox, test, then deploy"), f.ruleId);
      assert.ok(p.includes("Switch anything off before checking what depends on it"), f.ruleId);
    }
  });

  test("a summary too long for the limit loses sentences from its end and says so, and the prompt stays", () => {
    const long = { ...sample("change"), summary: Array.from({ length: 40 }, (_, i) => `Sentence ${i + 1} of the summary says something.`).join(" ") };
    const text = assemblePrompt(long, FIX_TABLE["aut.hardcoded-ids-in-flows"]!.prompt);
    assert.ok(text.length <= FIX_PROMPT_MAX);
    assert.match(text, /Sentence 1 of the summary says something\..* \(The finding says more\.\)/);
  });

  test("a decision for people says so, and asks for the analysis, not a change", () => {
    const p = one("cfg.stage-sprawl").fixPrompt!;
    assert.match(p, /This is for people to decide, not a change to the org\. Do not change the org/);
    assert.match(p, /- Change any setting, metadata or record: this task is analysis only\./);
    assert.ok(p.includes(FRAMING));
    assert.doesNotMatch(one("aut.hardcoded-ids-in-flows").fixPrompt!, /for people to decide/);
  });
});

describe("a rule that can remove, deactivate or merge never walks the assistant to it (finding 1)", () => {
  const review = /1\. This task is a review for the people who own these items|1\. This task is a review for the accounts' owners/;

  test("the two user rules and duplicate accounts are analysis only in every variant the synthetic runs make", () => {
    const variants = [...recorded, ...generated].filter((f) => f.status === "issue" && ["utl.dormant-licensed-users", "utl.never-logged-in", "dq.duplicate-accounts"].includes(f.ruleId));
    // Deactivate (every guard clear) and investigate (held back, by a guard or by the rule) both appear.
    assert.ok(variants.some((f) => f.recommendationType === "deactivate") && variants.some((f) => f.recommendationType === "investigate" && f.ruleId !== "dq.duplicate-accounts"));
    for (const f of variants) {
      assert.match(f.fixPrompt!, /This is for people to decide, not a change to the org/, `${f.ruleId} ${f.recommendationType}`);
      assert.doesNotMatch(instructions(f.fixPrompt!), RECORD_CHANGE, `${f.ruleId} ${f.recommendationType}`);
      if (f.recommendationType === "investigate") assert.match(f.fixPrompt!, review, `${f.ruleId} held back`);
      else assert.doesNotMatch(f.fixPrompt!, review, `${f.ruleId} cleared`);
    }
  });

  test("whenever such a rule's finding isn't its destructive recommendation, it gets the review, never its own steps", () => {
    for (const [id, rule] of Object.entries(FIX_TABLE)) {
      if (!rule.prompt.removes) continue;
      for (const type of ["investigate", "change", "none"] as const) {
        assert.ok(isHeldBack(sample(type), rule.prompt), `${id} ${type}`);
        const text = assemblePrompt(sample(type), rule.prompt);
        for (const step of rule.prompt.steps) assert.ok(!text.includes(step), `${id} ${type}: ${step}`);
        assert.match(text, /- Remove, deactivate or merge anything: this task is a review\./);
      }
      for (const type of ["remove", "deactivate", "merge"] as const) assert.equal(isHeldBack(sample(type), rule.prompt), false, `${id} ${type}`);
    }
    // A rule that can't remove anything never gets the review for an investigate finding.
    assert.equal(isHeldBack(sample("investigate"), FIX_TABLE["aut.hardcoded-ids-in-flows"]!.prompt), false);
  });
});

describe("no prompt asks the assistant to change records (principle A, findings 1 and 4)", () => {
  test("no prompt in either run, nor any rule's prompt in any variant, holds a record change outside the finding's own words and the Do not list", () => {
    for (const f of allPrompts) assert.doesNotMatch(instructions(f.fixPrompt!), RECORD_CHANGE, `${f.ruleId} ${f.recommendationType}`);
    for (const [id, rule] of Object.entries(FIX_TABLE)) {
      for (const type of ["remove", "deactivate", "merge", "change", "investigate"] as const) assert.doesNotMatch(instructions(assemblePrompt(sample(type), rule.prompt)), RECORD_CHANGE, `${id} ${type}`);
    }
  });

  test("the verbs are caught where they would be steps", () => {
    for (const bad of ["1. Deactivate the user.", "2. Merge the true duplicates.", "1. Reassign the deals.", "1. Transfer the records.", "1. Mass update the field.", "1. Archive what nobody reads.", "1. Delete the field.", "1. Create the renewal opportunity.", "1. Create each renewal.", "1. Deactivate each user listed."]) assert.match(bad, RECORD_CHANGE, bad);
  });

  test("the rules the reviewer named are analysis only, and the CPQ renewal is set up in a sandbox, never made by hand", () => {
    for (const id of ["cfg.picklist-sprawl", "bp.ownership-skew", "svc.knowledge-current", "mkt.unworked-handovers", "mkt.unowned-prospects", "svc.queue-sprawl"]) assert.ok(FIX_TABLE[id]!.prompt.decision, id);
    const renewals = FIX_TABLE["cpq.renewals-not-started"]!.prompt.steps.join(" ");
    assert.match(renewals, /In a sandbox, find the setting on the contract that has CPQ produce its renewal opportunity/);
    assert.match(renewals, /check its label in your org/);
    assert.match(renewals, /their owners, who set it on each in production/);
  });
});

describe("a security fix grants before it tightens (principle B, finding 5)", () => {
  const steps = (id: string): string[] => FIX_TABLE[id]!.prompt.steps.slice();
  const at = (list: string[], pattern: RegExp): number => list.findIndex((s) => pattern.test(s));

  test("self-authorised connected apps: who uses it, then the permission set assigned, then the policy, and what switching first breaks", () => {
    const s = steps("sec.self-authorised-connected-apps");
    const who = at(s, /list who and what uses it today/);
    const grant = at(s, /build a permission set that gives access to the app, and assign it/);
    const tighten = at(s, /Only then set the app to admin-approved users are pre-authorised/);
    assert.ok(who >= 0 && who < grant && grant < tighten, s.join(" | "));
    assert.match(s.join(" "), /Switching the policy first cuts off everyone using the app now, integrations included\./);
    assert.match(s[who]!, /The finding says when an app carries data now/);
  });

  test("interactive integration accounts: who signs in checked first, API Only through a permission set on that account alone, never a shared profile", () => {
    const s = steps("int.integration-users-interactive");
    const check = at(s, /check first whether anyone signs in to it through the browser/);
    const grant = at(s, /permission set holding only the API Only User permission, and assign it to that integration account alone/);
    assert.ok(check >= 0 && check < grant, s.join(" | "));
    assert.match(s.join(" "), /Never change a profile other users hold\./);
    assert.doesNotMatch(s.join(" "), /through its profile/);
  });

  test("every other access change grants first: Modify and View All Data, personal integration accounts, licences and remote sites", () => {
    for (const id of ["sec.modify-all-data", "sec.view-all-data"]) {
      const s = steps(id);
      assert.ok(at(s, /assign it first/) < at(s, /Only then take/), id);
      assert.match(s.join(" "), /Taking it away first/, id);
    }
    assert.match(steps("sec.personal-integration-accounts").join(" "), /only then the personal account's API access taken away\. Taking it away first stops the integration\./);
    assert.match(steps("int.integration-user-licences").join(" "), /only then the full seat given up/);
    assert.match(steps("sec.insecure-remote-sites").join(" "), /Only then turn off the setting.*Turning it off first stops any callout still on HTTP\./);
  });
});

describe("a fix prompt never names a person or a record (rule 7, finding 2)", () => {
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
    for (const f of allPrompts) {
      const shown = `${f.title}\n${f.summary}`;
      for (const s of f.subjects) {
        if (!word(s.label).test(f.fixPrompt!)) continue;
        assert.ok(shown.includes(s.label), `${f.ruleId} names ${s.type} "${s.label}", which its title and summary don't show`);
        assert.ok(!["user", "rep", "owner", "account", "dashboard", "report", "scheduled job", "price rules", "pricing rule"].includes(s.type), `${f.ruleId} names a ${s.type}`);
        named++;
      }
    }
    assert.ok(named > 0, "some prompt names set-up its finding shows");
    const flows = one("aut.hardcoded-ids-in-flows");
    assert.ok(flows.subjects.length > 0 && flows.subjects.every((s) => !flows.fixPrompt!.includes(s.label)));
    assert.match(flows.fixPrompt!, /Work on the flows listed in this finding\./);
    assert.match(one("dq.open-opps-past-close").fixPrompt!, /The worst rep is at 50%\./);
  });

  // The reviewer's planted inputs (probes/s46u-adv/priv.ts), scrubbed as the engine scrubs a finding.
  const reviewer: Array<[string, PromptFinding, string[]]> = [
    ["svc.unassigned-cases", { title: "Cases wait in queues with nobody working them", summary: "Open cases still owned by a queue more than 2 days after they came in: 38 of 141 (27%). Worst by queue: Sam's Escalations, at 100%.", recommendation: "Make sure every case queue has members.", subjects: [{ type: "queue", label: "Sam's Escalations" }] }, ["Sam's Escalations"]],
    ["int.failed-async-jobs", { title: "Background jobs are failing", summary: "3 of 7 jobs failed: NatarajanSync 3, Reed_Export 1.", recommendation: "Fix them.", subjects: [{ type: "apex class", label: "NatarajanSync" }, { type: "apex class", label: "Reed_Export" }] }, ["NatarajanSync", "Reed_Export"]],
    ["pipe.segment-divergence", { title: "Win rate for the Sam Reed Team team is well below", summary: "Win rate is 11% for the Sam Reed Team team (88 deals).", recommendation: "Look.", subjects: [{ type: "team", label: "Sam Reed Team" }] }, ["Sam Reed Team"]],
    ["pipe.segment-divergence", { title: "Win rate for the Priya's Team team is well below", summary: "Win rate is 11% for the Priya's Team team (88 deals).", recommendation: "Look.", subjects: [{ type: "team", label: "Priya's Team" }] }, ["Priya's Team"]],
    ["cpq.price-rule-conflicts", { title: "Some price rules compete", summary: "1 pair: Globex Corp 40pct and Volume Discount (Discount on Quote Line).", recommendation: "Give each pair.", subjects: [{ type: "price rules", label: "Globex Corp 40pct and Volume Discount" }] }, ["Globex Corp 40pct and Volume Discount"]],
  ];

  test("the reviewer's set-up names holding part of a person's name, and a CPQ rule's name, are never named, and the prompt is kept", () => {
    const p = reviewerPseudonyms();
    for (const [id, given, labels] of reviewer) {
      const f = p.scrubDeep(given);
      const out = fixPrompt(f, FIX_TABLE[id]!.prompt, p);
      assert.ok(out, `${id}: the prompt is kept`);
      assert.deepEqual(findNames(out, p), [], id);
      for (const label of labels) assert.ok(!out.includes(label), `${id} names ${label}`);
      for (const part of ["Sam", "Reed", "Priya", "Natarajan", "Globex"]) assert.doesNotMatch(out, new RegExp(part), `${id} holds ${part}`);
      assert.match(out, /listed in this finding/, id);
    }
  });

  test("the guard is at least as strict as the one before Claude: a part of a name in any form keeps the prompt off", () => {
    const pseudonyms = reviewerPseudonyms();
    const base: PromptFinding = { title: "Flows break", summary: "2 flows hold ids.", recommendation: "Replace them.", recommendationType: "change", subjects: [{ type: "flow", label: "Route_Deal" }] };
    const rule = FIX_TABLE["aut.hardcoded-ids-in-flows"]!.prompt;
    assert.ok(fixPrompt(base, rule, pseudonyms));
    for (const leak of ["Ask Sam Reed.", "Ask sam.reed@example.com.", "See 005000000000005AAA.", "See 005000000000005.", "Ask [[user:U0001]] and [[email]].", "Fix Route_Deal first.", "Sam's queue.", "Ask Priya.", "Run ReedExport.", "Run NatarajanSync."]) {
      const text = `${assemblePrompt(base, rule, pseudonyms)}\n${leak}`;
      assert.equal(promptIsSafe(text, base, pseudonyms), false, leak);
    }
    assert.equal(promptIsSafe("x".repeat(FIX_PROMPT_MAX + 1), base, pseudonyms), false);
    assert.equal(fixPrompt({ ...base, recommendation: "Ask Sam Reed to replace them." }, rule, pseudonyms), null);
  });

  test("tokens become words, a person's or record's subject is never named, and set-up the summary doesn't show is withheld from the recommendation", () => {
    const f: PromptFinding = {
      title: "Dashboards run as people who've left",
      summary: "Worst by rep: [[user:U0003]], at 50%. Account [[account:A0001]] holds most. Dashboard EU Pipeline runs as [[user:U0004]].",
      recommendation: "Move Old_Flow and EU Pipeline to a current user.",
      recommendationType: "change",
      subjects: [
        { type: "dashboard", label: "EU Pipeline" },
        { type: "flow", label: "Old_Flow" },
        { type: "user", label: "[[user:U0004]]" },
      ],
    };
    assert.deepEqual(withheldLabels(f).sort(), ["EU Pipeline", "Old_Flow", "[[user:U0004]]"]);
    const text = assemblePrompt(f, FIX_TABLE["utl.dashboards-inactive-running-user"]!.prompt);
    assert.match(text, /The worst rep is at 50%\. Account a record listed in this finding holds most\. Dashboard the dashboard listed in this finding runs as one person\./);
    assert.match(text, /Move the flow listed in this finding and the dashboard listed in this finding to a current user\./);
    assert.ok(promptIsSafe(text, f, new Pseudonymiser()));
  });
});

describe("a consultant's correction leaves no stale hours or prompt (finding 3)", () => {
  const correct = (f: Finding, key: string, newValue: number, previousValue = f.facts[key]!): NumberOverride => ({ id: "ov-0001", runId: "run-0001", target: { type: "finding", id: f.id, field: `facts.${key}` }, previousValue, newValue, reason: "Checked with the customer.", overriddenBy: "Synthetic Consultant", overriddenAt: "2026-10-31T13:00:00.000Z" });

  test("a carried correction drops both; a refused one, or none, keeps both", () => {
    const flows = one("aut.hardcoded-ids-in-flows");
    const carried = applyOverrides(recorded, [], [correct(flows, "affected", 3)]);
    assert.deepEqual(carried.refused, []);
    const after = carried.findings.find((f) => f.id === flows.id)!;
    assert.equal(after.facts.affected, 3);
    assert.equal(after.effortHours, undefined);
    assert.equal(after.fixPrompt, undefined);
    // Every other finding keeps its own.
    assert.ok(carried.findings.filter((f) => f.id !== flows.id && f.status === "issue").every((f) => f.effortHours && f.fixPrompt));
    // A correction made from a value this run doesn't read is refused, and nothing changes.
    const refused = applyOverrides(recorded, [], [correct(flows, "affected", 3, 7)]);
    assert.equal(refused.refused.length, 1);
    assert.deepEqual(refused.findings.find((f) => f.id === flows.id), flows);
  });

  test("the shared golden file holds the same cases for the console's copy", () => {
    const golden = JSON.parse(readFileSync(new URL("../../../fixtures/overrides-golden.json", import.meta.url), "utf8")) as { cases: Array<{ name: string; expected: { findings: Finding[] } }> };
    const cases = golden.cases.filter((c) => c.name.startsWith("a finding with hours and a prompt"));
    assert.equal(cases.length, 4);
    for (const c of cases) {
      const kept = !/drops both/.test(c.name);
      for (const f of c.expected.findings) assert.equal(f.effortHours !== undefined && f.fixPrompt !== undefined, kept, c.name);
    }
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
    for (const [key, text] of Object.entries(a)) assert.doesNotMatch(text, /What our report recommends|About \d+ to \d+ (minutes|hours) for each/, key);
    const notes = (findings: Finding[]) => renderRawNotes({ run, calibration: loadCalibration(), findings, metrics: output!.metrics, manualSteps: output!.manualSteps, pseudonyms });
    assert.equal(notes(output!.findings), notes(without));
  });
});

describe("round 3: the second reviewer's findings (decision 628)", () => {
  test("[1] every prompt of both runs frames the report's recommendation as the owners' and not the assistant's task, in one line", () => {
    assert.equal(FRAMING, "What our report recommends, for the people who own this to decide and carry out. It is not your task: your task is only the steps below.");
    for (const f of allPrompts) {
      const framed = f.fixPrompt!.split("\n\n").filter((s) => s.startsWith(FRAMING));
      assert.equal(framed.length, 1, f.ruleId);
      assert.match(framed[0]!, /\nThe report says: /, f.ruleId);
    }
    // The recommendations the reviewer quoted sit only inside that line.
    for (const [rule, words] of [["cpq.renewals-not-started", "Create each renewal opportunity now"], ["dq.opps-without-account", "Link each deal to its account"], ["sec.self-authorised-connected-apps", "Set each app to admin-approved users only, then give access"]] as const) {
      const p = one(rule).fixPrompt!;
      assert.ok(p.includes(words), rule);
      assert.ok(!instructions(p).includes(words), rule);
    }
  });

  test("[2] Modify and View All Data never move a person to another profile: shared profiles are listed for an admin with what else they give", () => {
    for (const id of ["sec.modify-all-data", "sec.view-all-data"]) {
      const steps = FIX_TABLE[id]!.prompt.steps.join(" ");
      assert.doesNotMatch(steps, /move the person to a profile|to a profile without it|change (?:the|their|a) user's profile/i, id);
      assert.match(steps, /Never change a profile or a user's profile: where a person holds it through a profile others share, list for an admin each such person, with what else that profile gives them, so the owners can decide\./, id);
      assert.match(steps, /Hand the tested change, and its order, to the admin who owns access, to make in production in the same order\./, id);
    }
  });

  const VALIDATION = Object.entries(FIX_TABLE).filter(([, r]) => r.prompt.steps.some((s) => /build a validation rule that/.test(s))).map(([id]) => id).sort();

  test("[3] a validation rule is built in a sandbox for new records or a changed field only, and handed to the owners, never turned on by the assistant", () => {
    assert.deepEqual(VALIDATION, ["cur.missing-next-steps", "dq.loss-reason-missing", "dq.open-opps-missing-amount", "dq.open-opps-past-close", "dq.opps-without-account", "fct.early-stage-commits", "svc.case-classification", "svc.origin-missing"]);
    for (const id of VALIDATION) {
      const steps = FIX_TABLE[id]!.prompt.steps;
      const build = steps.find((s) => /build a validation rule that/.test(s))!;
      assert.match(build, /In (?:a|the) sandbox/, id);
      assert.match(build, /written so it applies only when .+, never to an edit of a record that already lacks the value/, id);
      assert.match(steps.join(" "), /Hand the owners the tested rule and a report of the existing records that would fail it; they decide when it is turned on in production\./, id);
    }
    // Nothing that changes what users can save or sign in to is switched on in production by the assistant.
    for (const [id, r] of Object.entries(FIX_TABLE)) {
      const steps = r.prompt.steps.join(" ");
      if (/validation rule that|approval process|permission set|admin-approved|mix HTTPS|duplicate rule/.test(steps)) assert.doesNotMatch(steps, /(?:then do the same|turn (?:it|them) on|then) in production(?! in alert mode)|do the same in production/i, id);
    }
    for (const id of ["sec.health-check-score", "sec.health-check-risks"]) assert.ok(FIX_TABLE[id]!.prompt.decision, id);
    assert.match(one("dq.open-opps-missing-amount").fixPrompt!, /written so it applies only when a deal is created or moves into that stage/);
  });

  test(`[4] a set-up label holding a known person's name in the forms admins use is never named; the shortest word matched is ${MIN_NAME_PART} letters`, () => {
    const p = new Pseudonymiser();
    p.label("user", "005000000000005AAA", "Sam Reed", ["sam.reed@example.com"]);
    p.label("user", "005000000000006AAA", "Priya Natarajan", ["priya@example.com"]);
    p.label("user", "005000000000007AAA", "Mark Bishop", ["mbishop@example.com"]);
    p.label("user", "005000000000008AAA", "Grace Hall", ["ghall@example.com"]);
    p.label("user", "005000000000009AAA", "Shared Sales Desk", ["sales@example.com"]);
    p.label("account", "001000000000001AAA", "001000000000001AAA");
    // The reviewer's planted labels: those that came out named before this round, then those already caught.
    const throughBefore = ["sreed_nightly_sync", "pnatarajan_admin", "Reeds Escalations"];
    const caughtBefore = ["Priya_Lead_Router", "reed_nightly_sync", "NATARAJAN_SYNC", "Natarajan-Sync", "SReedExport", "Sam’s Escalations", "Bishop Approval", "Mark Approval", "Hall Booking", "grace_hall_flow", "GHall_Flow", "PS_Priya", "Natarajan Admin", "Sam Reed Profile", "Team Sam", "Priya Group", "Sam Reed Reports"];
    for (const label of [...throughBefore, ...caughtBefore]) {
      const f: PromptFinding = { title: `One item has a problem: ${label}`, summary: `${label} was found. ${label} matters.`, recommendation: `Look at ${label}.`, subjects: [{ type: "flow", label }], recommendationType: "investigate" };
      const out = fixPrompt(f, FIX_TABLE["aut.flows-without-fault-paths"]!.prompt, p);
      assert.ok(out, `${label}: the prompt is kept`);
      assert.ok(!out.includes(label), `${label} is named`);
      assert.match(out, /the flow listed in this finding/, label);
    }
    for (const label of throughBefore) assert.ok(labelHoldsPersonName(label, p), label);
    assert.deepEqual(labelWords("SReedExport_v2-Sam’s"), ["s", "reed", "export", "v", "sam", "s"]);
    // Ordinary words in set-up names, and shared users' words, stay nameable.
    for (const label of ["Opportunity_Route_Deal", "Renewal_Notice", "Sales EU", "Lead Source on Lead", "Support Tier 1"]) assert.equal(labelHoldsPersonName(label, p), false, label);
    // Accepted limit: a customer's or account's name can't be known to the run, so it stays, as the finding's own title shows it.
    const globex = fixPrompt({ title: "One item: Globex Integration", summary: "Globex Integration was found.", recommendation: "Look.", subjects: [{ type: "connected app", label: "Globex Integration" }], recommendationType: "change" }, FIX_TABLE["sec.self-authorised-connected-apps"]!.prompt, p)!;
    assert.ok(globex.includes("Globex Integration"));
  });

  test("[6] instructions name only what exists: flows look up queues and groups by developer name, never a user's; quotes to order is a report", () => {
    const flows = FIX_TABLE["aut.hardcoded-ids-in-flows"]!.prompt.steps.join(" ");
    assert.match(flows, /replace a queue's or public group's id with a lookup of it by its developer name/);
    assert.match(flows, /for a person, use a custom setting, a custom metadata record or a role instead of a user id, and never type a username into the flow/);
    assert.doesNotMatch(flows, /user by its developer name/);
    const orders = FIX_TABLE["cpq.quote-to-order"]!.prompt.steps.join(" ");
    assert.match(orders, /Build a report of won quotes with no order, for their owners\./);
    assert.doesNotMatch(orders, /validation rule|build the check/);
  });
});
