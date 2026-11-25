import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { producerKey, textProducers } from "./text-producers.ts";
import { checkedDigest, digestTextsAt, marksOf } from "../checked.ts";
import { PLAN_HOURS_APART } from "../build.ts";
import { ENGINE_TEXT_DIRS } from "../engine-texts-scan.ts";
import { engineOwn } from "../../engine/automatic-words.ts";
import { UNSCORED_RULES } from "../../domains/scoring.ts";
import { OBJECT_INVENTORY } from "../../domains/configuration/inventory.ts";
import type { ReportInput } from "../../narrative/sections.ts";
import { loadRun } from "../../render/render-run.ts";
import { refusalSentence } from "../../salesforce/refusal-words.ts";
import type { Finding, Metric } from "../../schemas/index.ts";
import { AutomaticReplay, automaticSyntheticCalibration } from "../../testing/automatic.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording } from "../../testing/harness.ts";

// The sentences the digest's builder writes itself, and the metric texts a rule
// puts a count into, marked on an automatic run (DECISIONS.md 739). A real
// org's first automatic run was held for one: the plan's hours note, which the
// builder writes when a code checker finding is in the plan, and which the
// synthetic and scale orgs never reach, as no code checker finding is raised
// on them. Built here from the synthetic org, with no real data.

const calibration = await automaticSyntheticCalibration();
const made = await runFromRecording({ calibration, reviewMode: "automatic", transport: new AutomaticReplay() });
const loaded = loadRun(made.dir);
const check = (input: ReportInput) => checkedDigest({ input, pseudonyms: loaded.pseudonyms, inventory: null, madeAt: "2026-11-25T09:00:00.000Z" });

describe("the plan's hours note on an automatic run (DECISIONS.md 739)", () => {
  /** The synthetic org's findings with a code checker issue in the plan, as a real org with Apex has. */
  const withCodeChecker = (): ReportInput => {
    const like = loaded.input.findings.find((f) => f.status === "issue" && f.effortHours)!;
    const lint: Finding = { ...like, id: "f-00000000c0de", ruleId: [...UNSCORED_RULES][0]! };
    return { ...loaded.input, findings: [...loaded.input.findings, lint] };
  };

  test("is served marked as ours, and the run is released, not held", () => {
    const c = check(withCodeChecker());
    assert.equal(c.digest!.plan.hoursTotal?.note, PLAN_HOURS_APART);
    assert.deepEqual(c.digest!.plan.hoursTotal?.orgWords?.note, { orgWordsMarked: true, orgWords: [] });
    for (const t of digestTextsAt(c.digest!)) assert.ok(marksOf(c.digest!, t), t.path);
    assert.deepEqual({ unmatched: c.unmatched, unmarked: c.unmarked, held: c.held }, { unmatched: 0, unmarked: 0, held: null });
  });

  test("without a code checker finding there is no note, as on the synthetic org", () => {
    assert.equal(check(loaded.input).digest!.plan.hoursTotal?.note, undefined);
  });

  test("the builder's own sentences are known as the engine's, because its folder is scanned", () => {
    assert.ok(ENGINE_TEXT_DIRS.includes("digest"));
    for (const text of [PLAN_HOURS_APART, "The overall score couldn't be kept."]) assert.ok(engineOwn(text, new Set()), text);
  });

  test("never as ours where the org chose the same words, or where the note reads any other way", () => {
    assert.equal(engineOwn(PLAN_HOURS_APART, new Set([PLAN_HOURS_APART])), false);
    assert.equal(engineOwn(PLAN_HOURS_APART.replace("our code checker", "Invoice__c"), new Set()), false);
  });
});

describe("the object inventory's unnamed layouts and triggers on an automatic run (DECISIONS.md 739)", () => {
  const extracts = loadExtracts(made.dir);
  const inventory = (changed: Map<string, unknown>): Extract<Metric, { status: "measured" }> => {
    const m = analyseExtracts(changed, { calibration }).metrics.find((x) => x.id === OBJECT_INVENTORY)!;
    assert.equal(m.status, "measured");
    return m as Extract<Metric, { status: "measured" }>;
  };
  const marksAt = (m: Extract<Metric, { status: "measured" }>, pattern: RegExp) => {
    const i = m.exclusions.findIndex((e) => pattern.test(e));
    assert.ok(i >= 0, m.exclusions.join(" | "));
    return { text: m.exclusions[i]!, marks: m.orgWords?.exclusions?.[i] ?? null };
  };

  test("the counts of layouts and triggers on objects we couldn't name are marked, with none of the org's words", () => {
    const changed = clone(extracts);
    // A custom object Tooling doesn't list, as a real org's can be: its layouts and triggers can't be named.
    const ids = changed.get("custom-objects") as { data: Array<{ DeveloperName: string }> };
    ids.data = ids.data.filter((r) => r.DeveloperName !== "Invoice");
    const { text, marks } = marksAt(inventory(changed), /^Page layouts \(\d+\) and Apex triggers \(\d+\) on custom objects we couldn't name$/);
    assert.ok(marks, text);
    assert.deepEqual(marks.spans, []);
  });

  test("an unreadable list's reason is passed on in its own text, the object it names marked as the org's", () => {
    const changed = clone(extracts);
    const reason = refusalSentence("EntityDefinition", "no_permission");
    changed.set("custom-objects", { status: "unavailable", reason, source: { kind: "soql", api: "tooling", query: "x" } });
    const m = inventory(changed);
    assert.ok(marksAt(m, /^Page layouts \(\d+\) and Apex triggers \(\d+\) on custom objects we couldn't name$/).marks);
    const { text, marks } = marksAt(m, /^The names of custom objects, because the list of custom objects couldn't be read: /);
    assert.ok(marks, text);
    const start = text.indexOf("EntityDefinition");
    assert.deepEqual(marks.spans, [{ start, end: start + "EntityDefinition".length }]);
  });
});

describe("every producer of a digest text is one an automatic run knows (DECISIONS.md 739, 743)", () => {
  /**
   * Strings the classifier finds in a producer's place that the digest never
   * serves, by file and expression: each read where it goes, in review.
   */
  const NOT_SERVED: string[] = [
    // Evidence exclusions: kept in the run's evidence, never in the digest.
    'src/domains/account-engagement/index.ts: [...(open.truncated ? [`Open synced leads past the first ${open.data.length.toLocaleString("en-GB")}, in order of Salesforce id`] : []), "Converted leads"]',
    'src/domains/configuration/index.ts: [`Not scanned, because their type can\'t be counted: ${scan.data.notScanned.length} fields`, "Packaged and formula fields"]',
    'src/domains/integrations/index.ts: [ "Accounts calibrated as shared, system, integration or test", apiOnly ? `Accounts ${API_ONLY_ACCOUNT}` : "Who is API only couldn\'t be read, so an account set to API only may be counted as a person\'s", `People with fewer than ${MIN_API_LOGINS} API logins, which is normal for an occasional data load`, ]',
    // A quarter's point in a series: its reason is the series point's, which the digest doesn't hold.
    "src/domains/historic-pipeline/deep-dive.ts: `${n} ${noun} is below the calibrated minimum of ${minSample}`",
    "src/domains/historic-pipeline/deep-dive.ts: `${rows.filter((d) => d.Amount === null).length} of ${rows.length} won deals have no amount`",
    // The Metadata API's refusal code and class, kept for us in run.json (702); the raw notes leave it out.
    'src/extract/automation.ts: `${retrieveError.code.replace(/[^A-Za-z0-9_]/g, "").slice(0, 60)} (${classifyError(retrieveError).replace(/_/g, " ")})`',
    // A field the inventory couldn't count: the inventory's own row, which the digest doesn't hold.
    "src/extract/inventory.ts: reason",
  ];
  /**
   * AI readiness's pillar reasons (DECISIONS.md 742, another lane's): served marked by readinessWords in build.ts, ours
   * unless the pillar quotes a finding's title (that title's spans) or passes a metric's reason on (that reason's marks),
   * and its figures counted apart. Signed off by expression; readiness's own tests hold the marks.
   */
  const READINESS: string[] = [
    'src/narrative/readiness.ts: `${head}${as}.`',
    'src/narrative/readiness.ts: `${subject} has nothing above low severity.`',
    'src/narrative/readiness.ts: `${text}, within our working line of ${LEGACY_READY}%.`',
    'src/narrative/readiness.ts: `${text}. Our working line is ${LEGACY_READY}%.`',
    'src/narrative/readiness.ts: `${text}.`',
    'src/narrative/readiness.ts: `${what}. An agent chooses an action by its description, and too few have one.`',
    'src/narrative/readiness.ts: `${what}.`',
    'src/narrative/readiness.ts: `Einstein generative AI and Agentforce are both switched on.${tail}`',
    'src/narrative/readiness.ts: `Einstein generative AI is on, and Agentforce ${agentforce === 0 ? "isn\'t" : "couldn\'t be read"}.${tail}`',
    "src/narrative/readiness.ts: `Einstein generative AI isn't switched on, and Agentforce needs it.${tail}`",
    'src/narrative/readiness.ts: `None of the checks on ${noun} could be measured.`',
    'src/narrative/readiness.ts: `Salesforce Knowledge is on, with ${count}.`',
    'src/narrative/readiness.ts: `Salesforce Knowledge is on, with ${count}: fewer than ${ARTICLES_READY}, our working line for enough to ground most answers.`',
  ];
  /** Values handed on, signed off by file and expression as one of the known kinds, with where each is written. */
  const SIGNED_OFF: string[] = [
    // A rule's own title, a literal where each rule is declared: a pass's or not measurable title is known as the rule's (evaluate.ts).
    "src/domains/current-pipeline/index.ts: options.title",
    "src/domains/data-quality/index.ts: options.title",
    "src/domains/security/index.ts: options.title",
    "src/domains/technical-health/lint.ts: group.title",
    "src/domains/utilisation/index.ts: title",
    // A metric's label and definition from the file's own table of literals, which the engine-text scan reads.
    "src/domains/configuration/documented.ts: definition",
    "src/domains/configuration/documented.ts: label",
    "src/domains/historic-pipeline/deep-dive.ts: label",
    "src/domains/historic-pipeline/deep-dive.ts: text",
    "src/domains/historic-pipeline/metrics.ts: label",
    "src/domains/service/operations.ts: definition",
    "src/domains/service/operations.ts: label",
    // A helper's list of exclusions or assumptions, every caller's items written in its own object, read there as producers.
    "src/domains/adoption/index.ts: extra.assumptions ?? []",
    "src/domains/adoption/index.ts: extra.exclusions ?? EXCLUSIONS",
    "src/domains/configuration/documented.ts: extra.assumptions ?? []",
    "src/domains/configuration/documented.ts: extra.exclusions ?? []",
    "src/domains/utilisation/investment.ts: extra.assumptions ?? []",
    "src/domains/utilisation/investment.ts: extra.exclusions ?? [LICENCE_EXCLUSION]",
    "src/domains/current-pipeline/outlook.ts: [...data.exclusions, ...weighted.unweighted]",
    "src/domains/historic-pipeline/metrics.ts: [zoneNote(ctx), ...(fields.assumptions ?? [])]",
    // Said lists the classifier can't see through: each item made by said where it is pushed or written.
    "src/domains/technical-health/lint.ts: [ ...(pmd.skipped.tooLarge ? [said`Classes and triggers too large for our code checker: ${pmd.skipped.tooLarge}`] : []), ...(pmd.skipped.unparsable ? [said`Classes and triggers our code checker couldn't read: ${pmd.skipped.unparsable}`] : []), ...(pmd.skipped.unreadable ? [said`Classes and triggers whose code is hidden: ${pmd.skipped.unreadable}`] : []), \"Installed packages' code\", ...pmd.limits, ]",
    // A gate made by unmappedReason (said), passed to the metric builder.
    "src/domains/historic-pipeline/deep-dive.ts: gate",
    // A driver's reason, the segment's own, which the digest doesn't hold.
    "src/domains/historic-pipeline/drivers.ts: yes?.notMeasurableReason ?? null",
    // A score's reason: the literals scoreDomain passes, read by the engine-text scan.
    "src/domains/scoring.ts: reason",
    "src/domains/scoring.ts: words",
    // Guard phrases from USER_GUARD_BLOCKERS, our own table (signed off in org-words-eng.test.ts), for an action list's rows.
    "src/domains/utilisation/index.ts: [...new Set(a.guards.filter((g) => g.status === \"failed\").map((g) => USER_GUARD_BLOCKERS[g.guardId as keyof typeof USER_GUARD_BLOCKERS] ?? g.guardId))]",
    // The loss reason field's sentence, made by lossFieldUnusable and known by the recogniser (702).
    "src/domains/data-quality/index.ts: extract.lossReasonUnusable",
    // An extractor's reason, each traced to what can be put there (re-audited in review, DECISIONS.md 743):
    // automation.ts `why`: a ToolingGapError's message (tooManyFlows or flowWithoutDefinition, the recogniser's) or refusalReason's.
    "src/extract/automation.ts: why",
    // configuration.ts `failure`: batchRefused's or refusalReason's sentence, or "The field usage query failed" (a sentence, scanned).
    "src/extract/configuration.ts: failure",
    // integrations.ts and inventory.ts `unread`: the client's limitsUnread, LIMITS_NO_PERMISSION or LIMITS_REFUSED (the recogniser's whole sentences).
    "src/extract/integrations.ts: unread",
    "src/extract/inventory.ts: unread",
    // opportunities.ts and pardot.ts `reasons[i]`: batchRefused's sentences.
    "src/extract/opportunities.ts: reasons[i]!",
    "src/extract/pardot.ts: reasons[i]!",
    // record-types.ts: a read's reason (readSoql's refusal sentence) with the connected user's id and username hidden, which no
    // refusal sentence holds, so it's the sentence unchanged; and batchRefused's sentences handed to each count.
    "src/extract/record-types.ts: hide(read.reason)",
    "src/extract/record-types.ts: reason",
    // metadata-parse.ts: a kind's reason in `unread`, written only by markUnread in metadata-tooling.ts, whose every call is a
    // producer read here (refusalReason's sentence, tooManyRules or ruleWithoutDefinition, the recogniser's).
    "src/extract/metadata-parse.ts: bundle.unread![kind]!",
    // A code checker's not-run reason: kept in the evidence's note alone (lint.ts `cannot`), never a digest text.
    "src/extract/lint/findings.ts: reason",
  ];
  const producers = textProducers();

  test("none is a plain template, a join or a sum of strings: each is said, written whole, or a reason passed on", () => {
    const plain = [...new Set(producers.filter((p) => p.kind === "NOT RECOGNISED").map(producerKey))].filter((k) => !NOT_SERVED.includes(k) && !READINESS.includes(k)).sort();
    assert.deepEqual(plain, []);
    assert.ok(producers.length > 1500, `${producers.length} producers`);
  });

  test("every value handed on is signed off, and every sign-off still names one", () => {
    const refs = [...new Set(producers.filter((p) => p.kind === "ref").map(producerKey))].sort();
    assert.deepEqual(refs.filter((k) => !SIGNED_OFF.includes(k)), []);
    assert.deepEqual(SIGNED_OFF.filter((k) => !refs.includes(k)), []);
    const unrecognised = new Set(producers.filter((p) => p.kind === "NOT RECOGNISED").map(producerKey));
    assert.deepEqual([...NOT_SERVED, ...READINESS].filter((k) => !unrecognised.has(k)), []);
  });

  test("the ways a producer once slipped past are caught: a field set later, a computed key, a quoted key, a helper in an unlisted folder", () => {
    const at = "src/digest/__tests__/fixtures/";
    const found = textProducers({ fixtures: [`${at}unmarked-producers.ts`, `${at}unmarked-helper.ts`], unlisted: [`${at}unmarked-helper.ts`] }).filter((p) => p.at.startsWith(at) && p.kind === "NOT RECOGNISED");
    assert.deepEqual(found.map((p) => `${p.at.slice(at.length).replace(/:\d+$/, "")} ${p.slot} ${p.expression}`).sort(), [
      "unmarked-helper.ts field:reason `${n} things is too few.`",
      "unmarked-producers.ts field:reason `${name} couldn't be counted.`",
      "unmarked-producers.ts field:reason `${name} couldn't be listed.`",
      "unmarked-producers.ts set:reason `${name} couldn't be read.`",
    ]);
  });

  test("the 739 pushes are still held: each exclusion or assumption pushed in src/domains is a producer here", () => {
    const pushes = producers.filter((p) => p.slot.startsWith("push:") && p.at.startsWith("src/domains/"));
    assert.ok(pushes.length > 20, `${pushes.length} pushes`);
    assert.ok(pushes.every((p) => p.kind !== "NOT RECOGNISED"));
  });
});
