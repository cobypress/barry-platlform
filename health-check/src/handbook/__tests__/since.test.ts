import { test, describe, before, after as afterAll } from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { chmodSync, cpSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { basename, join } from "node:path";
import { SALES_CLOUD_MODULES } from "../../domains/index.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { APEX_READING } from "../../extract/apex-touches.ts";
import { loadPseudonymMap, loadRun } from "../../render/render-run.ts";
import { docxText } from "../../testing/docx-text.ts";
import { EARLIER_AS_OF, plantEarlier, REMOVED_FLOW, withEarlier } from "../../testing/earlier-handbook.ts";
import { runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { AS_OF } from "../../testing/synthetic-org.ts";
import { BUILD_READING, HANDBOOK_FILE } from "../build.ts";
import { compareHandbooks, routeOf, type Compared, type KindComparison } from "../compare.ts";
import { METADATA_READING } from "../detail.ts";
import { NO_EARLIER, renderHandbookDocx, SINCE_TITLE } from "../docx.ts";
import { readEarlierHandbook, readHandbook, type EarlierHandbook } from "../read.ts";
import { Handbook, type HandbookFlowItem, type HandbookStep } from "../schema.ts";
import { olderComponents } from "./older-files.ts";

// A re-check's handbook gains "Changed since your last health check"
// (DECISIONS.md 483): this run's handbook.json set beside the earlier health
// check's, worked out when the Word file is made. Nothing is called added,
// removed or changed that the two files can't show: each case is planted here,
// with the sentence the customer reads.

const { dir: recorded, run } = await runFromRecording({ answers: loadAnswers() });
const handbookIn = (dir: string): Handbook => {
  const read = readHandbook(dir);
  return read.ok ? read.handbook : assert.fail(read.why);
};
const after = handbookIn(recorded);
const pseudonyms = loadPseudonymMap(recorded);
const OPTIONS = { clientName: "Synthetic Co", runDate: run.asOf, pseudonyms };

/** The synthetic org's handbook, edited, as an earlier health check's. */
function earlierOf(edit: (h: Handbook) => unknown, from: Handbook = after): EarlierHandbook {
  const handbook = structuredClone(from);
  edit(handbook);
  return { handbook: Handbook.parse(handbook), asOf: EARLIER_AS_OF };
}

async function textOf(earlier: EarlierHandbook | null | undefined, h: Handbook = after): Promise<string> {
  return docxText(await renderHandbookDocx(h, { ...OPTIONS, earlier }));
}

/** The part, from its heading to the recent changes'. */
function partOf(text: string): string {
  const start = text.indexOf(`\n## ${SINCE_TITLE}\n`);
  assert.ok(start >= 0, "the part is there");
  return text.slice(start + 1, text.indexOf("\n## Recent changes\n"));
}

/** One kind's words in the part: under its heading, up to the next. */
function kindIn(part: string, title: string): string {
  const start = part.indexOf(`\n### ${title}\n`);
  assert.ok(start >= 0, title);
  const rest = part.slice(start + `\n### ${title}\n`.length);
  const next = rest.indexOf("\n### ");
  return (next < 0 ? rest : rest.slice(0, next)).trim();
}

const compared = (c: KindComparison): Compared => (c.status === "compared" ? c : assert.fail(`not compared: ${c.unreadIn}`));
const names = (c: KindComparison) => {
  const k = compared(c);
  return { added: k.added.map((i) => i.name), removed: k.removed.map((i) => i.name), changed: k.changed.map((i) => [i.name, i.details]) };
};
const ok = <P extends { status: string }>(part: P): Extract<P, { status: "ok" }> => (part.status === "ok" ? (part as Extract<P, { status: "ok" }>) : assert.fail("unavailable"));

const NOTHING = { added: [], removed: [], changed: [] };

/**
 * The synthetic org as an earlier health check that a later reading would see
 * otherwise (decision 484): a flow's step, a formula written with `&#34;` for a
 * quote (decision 471), what a class touches and a trigger's events, each
 * differing with nothing in the org changed; and beside those, a flow, a rule
 * and a class only it had, a flow only this run has, and three components whose
 * last-changed date moved.
 */
function plantReadings(h: Handbook): void {
  const flows = ok(h.flows);
  ok(h.flows).items.find((f) => f.name === "Opportunity_Stage_Review")!.steps[0]!.label = "Read otherwise";
  ok(h.flows).items.find((f) => f.name === "Invoice_Set_Status")!.lastChanged!.at = "2026-03-01T09:00:00.000+0000";
  flows.items = [...flows.items.filter((f) => f.name !== "Draft_Idea"), { ...structuredClone(flows.items[0]!), name: REMOVED_FLOW, label: "Lead Nurture (old)" }];
  const rules = ok(h.validationRules);
  const escalated = rules.items.find((r) => r.name === "Reason_When_Escalated")!;
  escalated.formula = escalated.formula!.replaceAll('"', "&#34;");
  const old = rules.items.find((r) => r.name === "Old_Check")!;
  old.formula = old.formula!.replaceAll('"', "&#34;");
  old.lastChanged!.at = "2024-01-01T09:00:00.000+0000";
  rules.items.push({ ...structuredClone(rules.items.find((r) => r.name === "Needs_Competitor")!), name: "Old_Discount_Check" });
  const classes = ok(h.apexClasses);
  classes.items.find((k) => k.name === "LeadRouter")!.touches!.queries = [];
  classes.items.find((k) => k.name === "FinanceSync")!.lastChanged = { at: "2026-05-02T09:30:00.000+0000", by: "[[user:U0001]]" };
  classes.items = [...classes.items, { ...structuredClone(classes.items[0]!), name: "QuoteSync" }].sort((a, b) => (a.name < b.name ? -1 : 1));
  classes.total = classes.items.length;
  ok(h.apexTriggers).items.find((t) => t.name === "InvoiceTrigger")!.events = ["before insert"];
}

/** A version 6 file as an engine of an earlier version wrote it: no readings, and what each version before didn't keep. */
function asVersion(file: Handbook, version: number): Record<string, unknown> {
  const { readBy: _none, ...five } = { ...file, version } as Record<string, unknown>;
  if (version === 5) return five;
  const older = (part: { status: string; items?: Array<Record<string, unknown>>; lastChanges?: string }) => {
    if (part.status !== "ok") return part;
    const { lastChanges: _said, ...rest } = part;
    return { ...rest, items: part.items!.map(({ lastChanged: _gone, ...item }) => item) };
  };
  const four = olderComponents<Record<string, unknown>>(five);
  if (version === 4) return four;
  const three: Record<string, unknown> = { ...four, apexClasses: older(four.apexClasses as never), apexTriggers: older(four.apexTriggers as never) };
  if (version === 3) return three;
  const { recentChanges: _gone, ...two } = three;
  return two;
}

/** The sentence for a kind whose earlier file says no reading. */
const unrecorded = (many: string, one: string): string =>
  `We can't tell which version of our reader read your ${many} in your earlier health check, and two versions can read the same ${one} differently, so we've compared only which were added or removed and when each was last changed.`;

/** The same, where either file holds no last-changed dates for the kind (decision 485). */
const addedOrRemoved = (many: string, one: string): string =>
  `We can't tell which version of our reader read your ${many} in your earlier health check, and two versions can read the same ${one} differently, so we've compared only which were added or removed.`;

describe("no baseline", () => {
  // The synthetic org's handbook as an engine at 17586d0, before decision 483,
  // printed it (src/handbook/sample.ts). No baseline, no part: not a word moves.
  const BEFORE_483 = "110aa430083eed85fb0a5c652b45bc119f714730169dc2d63bc6cf01c072350c";

  test("the synthetic org's document is word for word what it was", async () => {
    const text = await textOf(undefined);
    assert.equal(createHash("sha256").update(`${text}\n`).digest("hex"), BEFORE_483);
    assert.doesNotMatch(text, /Changed since|earlier handbook/);
  });

  test("a run that names no baseline reads as none, and so does one whose own run.json can't be read", () => {
    assert.equal(readEarlierHandbook(recorded), undefined);
    const dir = join(tempDir(), "hc-2026-10-31-norunjson");
    mkdirSync(dir);
    writeFileSync(join(dir, "run.json"), "{}");
    assert.equal(readEarlierHandbook(dir), undefined);
  });
});

describe("the synthetic org since its earlier health check", () => {
  const earlier = earlierOf(plantEarlier);

  test("lists what was added, removed and changed, kind by kind, with which details changed", () => {
    const c = compareHandbooks(earlier.handbook, after);
    assert.deepEqual(names(c.flows), { added: ["Draft_Idea"], removed: [REMOVED_FLOW], changed: [["Opportunity_Route_Deal", ["status", "steps"]]] });
    assert.deepEqual(names(c.validationRules), {
      added: ["Needs_Competitor"],
      removed: ["Old_Discount_Check"],
      changed: [
        ["Due_Date_Required", ["formula"]],
        ["Old_Check", ["message"]],
      ],
    });
    assert.deepEqual(names(c.apexClasses), { added: [], removed: ["QuoteSync"], changed: [["FinanceSync", ["lastChanged"]]] });
    assert.deepEqual(names(c.apexTriggers), { added: [], removed: [], changed: [["InvoiceTrigger", ["events"]]] });
    // Nothing left out: both runs read everything, by the same route.
    for (const kind of Object.values(c)) {
      const k = compared(kind);
      assert.deepEqual([k.unread, k.leftOutObjects, k.unnamed, k.range, k.stepsCut, k.routes], [0, [], 0, null, 0, null]);
    }
  });

  test("the customer reads them in plain words, every count the comparison's own, before the recent changes", async () => {
    const text = await textOf(earlier);
    const part = partOf(text);
    assert.ok(text.indexOf(`## ${SINCE_TITLE}`) < text.indexOf("## Recent changes") && text.indexOf("## Apex triggers") < text.indexOf(`## ${SINCE_TITLE}`));
    assert.match(part, /^## Changed since your last health check\n\nThis compares your org on 31 Oct 2026 with your health check of 14 Jul 2026\. For your flows, validation rules, Apex classes and Apex triggers, it lists what was added, what was removed and what changed, and for each one that changed, which of its details differ, not what they were before\. One that was renamed shows as one removed and one added\.\n/);
    assert.equal(kindIn(part, "Flows"), "Added: 1 flow\nDraft (Draft_Idea)\n\nRemoved: 1 flow\nLead Nurture (old) (Lead_Nurture_Old)\n\nChanged: 1 flow\nRoute Deal (Opportunity_Route_Deal): status and steps");
    assert.equal(
      kindIn(part, "Validation rules"),
      "Added: 1 validation rule\nNeeds_Competitor on Opportunity\n\nRemoved: 1 validation rule\nOld_Discount_Check on Opportunity\n\nChanged: 2 validation rules\nDue_Date_Required on Invoice__c: formula\nOld_Check on Invoice__c: error message",
    );
    // The date and who are this run's, the name restored from this run's map.
    assert.equal(kindIn(part, "Apex classes"), "Added: none\n\nRemoved: 1 Apex class\nQuoteSync\n\nChanged: 1 Apex class\nFinanceSync: last changed 13 Oct 2026 by Ash Rowe");
    assert.equal(kindIn(part, "Apex triggers"), "Added: none\n\nRemoved: none\n\nChanged: 1 Apex trigger\nInvoiceTrigger on Invoice__c: events");
    // Never the earlier text: not the old formula, message or step's field.
    for (const old of ["OR(ISBLANK(Due_Date__c), ISBLANK(Amount__c))", "Check the invoice.", "OwnerId"]) assert.ok(!part.includes(old), old);
    // And "What it covers" says what the part is and what it leaves out.
    assert.match(text, /\nChanged since your last health check: what was added, removed or changed in your flows, validation rules, Apex classes and Apex triggers since your health check of 14 Jul 2026\.\n\nWhere both health checks read them the same way, it compares every detail this handbook keeps of each one, apart from who last changed it, which we don't match from one health check to the next\. It doesn't compare the order of steps no path reaches, the part of a formula or a condition's value past the 4,000 characters this handbook keeps, or the recent changes, which have their own part\. Where the two health checks can't be compared fairly, because one couldn't read something or read less of it, or the two read it in different ways, the part says so and compares only what it can, rather than call anything added, removed or changed that it can't show\.\n\nRecent changes: /);
  });

  test("two identical files: every kind says nothing changed, and nothing more", async () => {
    const part = partOf(await textOf(earlierOf(() => {})));
    for (const [title, many] of [["Flows", "flows"], ["Validation rules", "validation rules"], ["Apex classes", "Apex classes"], ["Apex triggers", "Apex triggers"]]) assert.equal(kindIn(part, title!), `No ${many} were added, removed or changed.`);
  });

  test("who last changed one is never compared: the same date with another run's token is no change, and a new date names this run's person", async () => {
    // Tokens are numbered afresh in each run, so the earlier file's U0001 is nobody here.
    const c = compareHandbooks(
      earlierOf((h) => {
        for (const f of ok(h.flows).items) if (f.lastChanged?.by) f.lastChanged.by = "[[user:U0001]]";
        for (const k of ok(h.apexClasses).items) if (k.lastChanged?.by) k.lastChanged.by = "[[user:U0003]]";
      }).handbook,
      after,
    );
    assert.deepEqual([names(c.flows), names(c.apexClasses)], [NOTHING, NOTHING]);
    // The same instant written another way is the same date.
    const same = compareHandbooks(earlierOf((h) => (ok(h.flows).items[0]!.lastChanged!.at = "2026-10-01T12:00:00.000Z")).handbook, after);
    assert.deepEqual(names(same.flows), NOTHING);
  });
});

describe("never guessed", () => {
  test("a kind either run couldn't read isn't compared, and says which run couldn't", async () => {
    const unavailable = { status: "unavailable" as const, reason: "Metadata couldn't be retrieved: HB-REASON-0483" };
    const earlier = earlierOf((h) => {
      h.flows = unavailable;
      h.apexTriggers = unavailable;
    });
    const now = Handbook.parse({ ...after, apexClasses: unavailable, apexTriggers: unavailable });
    const part = partOf(await textOf(earlier, now));
    assert.equal(kindIn(part, "Flows"), "We couldn't read your flows in your earlier health check, so we haven't compared them.");
    assert.equal(kindIn(part, "Apex classes"), "We couldn't read your Apex classes in this health check, so we haven't compared them with your earlier one.");
    assert.equal(kindIn(part, "Apex triggers"), "We couldn't read your Apex triggers in either health check, so we haven't compared them.");
    assert.equal(kindIn(part, "Validation rules"), "No validation rules were added, removed or changed.");
    // Salesforce's reason is the parts' own, not this one's.
    assert.ok(!part.includes("HB-REASON-0483"));
  });

  test("a rule on an object only this run read is neither added nor removed, and the part names the object", async () => {
    // The earlier run's retrieve named the four and not Case, so it never read Case's rules.
    const earlier = earlierOf((h) => {
      const rules = ok(h.validationRules);
      rules.covers = { customObjects: "all", standardObjects: ["Account", "Contact", "Lead", "Opportunity"] };
      rules.items = rules.items.filter((r) => r.object !== "Case" && r.name !== "Due_Date_Required");
    });
    const c = compared(compareHandbooks(earlier.handbook, after).validationRules);
    assert.ok(!c.added.some((r) => r.object === "Case"), "Case's rule isn't added");
    // A custom object is read on every route, so its new rule is.
    assert.deepEqual(names(c), { added: ["Due_Date_Required"], removed: [], changed: [] });
    assert.deepEqual(c.leftOutObjects, ["Case"]);
    const words = kindIn(partOf(await textOf(earlier)), "Validation rules");
    assert.equal(words, "Added: 1 validation rule\nDue_Date_Required on Invoice__c\n\nRemoved: none\n\nChanged: none\n\nWe left out the rules on Case: we can't be sure both health checks read the rules on that object, so a rule there isn't called added or removed.");
    // The other way about, this run reading less: Case's rule isn't removed either.
    const back = compared(compareHandbooks(after, earlier.handbook).validationRules);
    assert.deepEqual([back.removed.map((r) => r.name), back.leftOutObjects], [["Due_Date_Required"], ["Case"]]);
    // Tooling reads every object, so against a retrieve that read Case too, Case is compared.
    const tooling = earlierOf((h) => (ok(h.validationRules).covers = { customObjects: "all", standardObjects: "all" }));
    assert.deepEqual(compared(compareHandbooks(tooling.handbook, after).validationRules).leftOutObjects, []);
  });

  describe("a rule on a custom object of another kind than __c (decision 484)", () => {
    /** A rule on a custom metadata type, or another custom object, as the synthetic org's rules are kept. */
    const ruleOn = (object: string, name = "Key_Required") => ({ ...structuredClone(ok(after.validationRules).items.find((r) => r.name === "Needs_Competitor")!), object, name });
    const withRules = (h: Handbook, ...rules: ReturnType<typeof ruleOn>[]) => ok(h.validationRules).items.push(...rules);
    const asTooling = (h: Handbook) => (ok(h.validationRules).covers = { customObjects: "all", standardObjects: "all" });

    test("by the same route, one removed or added there is listed, with no note", async () => {
      const earlier = earlierOf((h) => withRules(h, ruleOn("Setting__mdt")));
      const c = compared(compareHandbooks(earlier.handbook, after).validationRules);
      assert.deepEqual([names(c), c.leftOutObjects], [{ added: [], removed: ["Key_Required"], changed: [] }, []]);
      assert.equal(kindIn(partOf(await textOf(earlier)), "Validation rules"), "Added: none\n\nRemoved: 1 validation rule\nKey_Required on Setting__mdt\n\nChanged: none");
      const now = earlierOf((h) => withRules(h, ruleOn("Setting__mdt"))).handbook;
      assert.deepEqual(names(compareHandbooks(after, now).validationRules), { added: ["Key_Required"], removed: [], changed: [] });
      assert.equal(kindIn(partOf(await textOf({ handbook: after, asOf: EARLIER_AS_OF }, now)), "Validation rules"), "Added: 1 validation rule\nKey_Required on Setting__mdt\n\nRemoved: none\n\nChanged: none");
      // Tooling to Tooling too, and every kind of custom object.
      const suffixes = ["Setting__mdt", "Alert__e", "Archive__b", "Remote__x", "Article__kav"];
      const tooling = earlierOf((h) => {
        asTooling(h);
        withRules(h, ...suffixes.map((o) => ruleOn(o)));
      });
      const toolingNow = earlierOf(asTooling).handbook;
      assert.deepEqual(compared(compareHandbooks(tooling.handbook, toolingNow).validationRules).removed.map((r) => r.object), [...suffixes].sort((a, b) => a.localeCompare(b, "en-GB")));
    });

    test("across routes it's left out, and the sentence says we can't be sure; __c is still compared", async () => {
      const earlier = earlierOf((h) => {
        asTooling(h);
        withRules(h, ruleOn("Setting__mdt"), ruleOn("Invoice__c", "New_On_Custom"));
      });
      const c = compared(compareHandbooks(earlier.handbook, after).validationRules);
      assert.deepEqual([names(c), c.leftOutObjects], [{ added: [], removed: ["New_On_Custom"], changed: [] }, ["Setting__mdt"]]);
      assert.equal(
        kindIn(partOf(await textOf(earlier)), "Validation rules"),
        "Added: none\n\nRemoved: 1 validation rule\nNew_On_Custom on Invoice__c\n\nChanged: none\n\nWe left out the rules on Setting__mdt: we can't be sure both health checks read the rules on that object, so a rule there isn't called added or removed.",
      );
      // Two objects, and the other way about: the later file the Tooling one.
      const two = earlierOf((h) => {
        asTooling(h);
        withRules(h, ruleOn("Setting__mdt"), ruleOn("Alert__e"));
      }).handbook;
      assert.match(kindIn(partOf(await textOf({ handbook: after, asOf: EARLIER_AS_OF }, two)), "Validation rules"), /^No validation rules were added, removed or changed\.\n\nWe left out the rules on Alert__e and Setting__mdt: we can't be sure both health checks read the rules on those objects, so a rule there isn't called added or removed\.$/);
    });

    test("a file that says it read less than every custom object is taken at its word", () => {
      // Never written today: the schema holds "all". Built by hand, as a later version might write it.
      const earlier = earlierOf((h) => withRules(h, ruleOn("Setting__mdt"), ruleOn("Invoice__c", "New_On_Custom")));
      (ok(earlier.handbook.validationRules).covers as { customObjects: string }).customObjects = "some";
      const c = compared(compareHandbooks(earlier.handbook, after).validationRules);
      assert.deepEqual([names(c).removed, c.leftOutObjects], [[], ["Invoice__c", "Setting__mdt"]]);
    });
  });

  test("a cut Apex list: names are compared only as far as both lists go, and none past it is added or removed", async () => {
    // The earlier list held its first three by name, of nine.
    const earlier = earlierOf((h) => {
      const classes = ok(h.apexClasses);
      classes.items = classes.items.slice(0, 3);
      classes.total = 9;
      classes.cut = true;
    });
    assert.deepEqual(
      ok(earlier.handbook.apexClasses).items.map((c) => c.name),
      ["FinanceSync", "LeadRouter", "LeadRouterTest"],
    );
    const now = structuredClone(after);
    const classes = ok(now.apexClasses);
    const added = { ...structuredClone(classes.items[0]!), name: "FieldAudit", lastChanged: null };
    const beyond = { ...structuredClone(classes.items[0]!), name: "ZebraBeyondTheCut", lastChanged: null };
    classes.items = [...classes.items, added, beyond].sort((a, b) => (a.name < b.name ? -1 : 1));
    classes.total = classes.items.length;
    const c = compared(compareHandbooks(earlier.handbook, now).apexClasses);
    // FieldAudit sorts before the earlier list's last kept name, so it's new; the rest aren't called anything.
    assert.deepEqual(names(c), { added: ["FieldAudit"], removed: [], changed: [] });
    assert.deepEqual(c.range, { upTo: "LeadRouterTest", leftOut: 5 });
    assert.equal(
      kindIn(partOf(await textOf(earlier, now)), "Apex classes"),
      "Added: 1 Apex class\nFieldAudit\n\nRemoved: none\n\nChanged: none\n\nOne of the two health checks had more Apex classes than this handbook keeps, so we compared them only as far as both lists go, up to LeadRouterTest. This health check's 5 Apex classes after that aren't compared, so none is called added or removed.",
    );
    // And this run's list cut: the earlier one's classes past its end aren't removed.
    const cutNow = structuredClone(after);
    const mine = ok(cutNow.apexClasses);
    mine.items = mine.items.slice(0, 2);
    mine.cut = true;
    const back = compared(compareHandbooks(after, cutNow).apexClasses);
    assert.deepEqual([names(back), back.range], [NOTHING, { upTo: "LeadRouter", leftOut: 5 }]);
  });

  for (const version of [5, 4, 3, 2]) {
    test(`an earlier version ${version} file, which says no reading: only what was added or removed, and when each was last changed where both say, in one sentence`, async () => {
      // As an engine at 8344688 (version 5) or before wrote it, planted with a
      // change of each kind that a parser fix alone can make, and read back
      // through the one reader (decision 484).
      const current = structuredClone(JSON.parse(readFileSync(join(recorded, HANDBOOK_FILE), "utf8"))) as Handbook;
      plantReadings(current);
      const dir = join(tempDir(), "earlier");
      mkdirSync(dir);
      writeFileSync(join(dir, HANDBOOK_FILE), JSON.stringify(asVersion(current, version)));
      const earlier = handbookIn(dir);
      assert.deepEqual(earlier.readBy, { metadata: null, apex: null, build: null });
      const c = compareHandbooks(earlier, after);
      // Flows' and rules' dates are kept from version 5, Apex's from 4.
      const flowsDated = version >= 5;
      const apexDated = version >= 4;
      assert.deepEqual(names(c.flows), { added: ["Draft_Idea"], removed: [REMOVED_FLOW], changed: flowsDated ? [["Invoice_Set_Status", ["lastChanged"]]] : [] });
      assert.deepEqual(names(c.validationRules), { added: [], removed: ["Old_Discount_Check"], changed: flowsDated ? [["Old_Check", ["lastChanged"]]] : [] });
      assert.deepEqual(names(c.apexClasses), { added: [], removed: ["QuoteSync"], changed: apexDated ? [["FinanceSync", ["lastChanged"]]] : [] });
      assert.deepEqual(names(c.apexTriggers), NOTHING);
      for (const kind of Object.values(c)) assert.deepEqual([compared(kind).reading, compared(kind).unread, compared(kind).stepsCut], ["unknownBefore", 0, 0]);

      const part = partOf(await textOf({ handbook: earlier, asOf: EARLIER_AS_OF }));
      // Where the earlier file holds no dates, one sentence says only which were
      // added or removed were compared, with no Changed list (decision 485).
      const sentence = (many: string, one: string, isDated: boolean) => (isDated ? unrecorded(many, one) : addedOrRemoved(many, one));
      const set = ok(after.flows).items.find((f) => f.name === "Invoice_Set_Status")!;
      // The date and who are this run's, the name restored from this run's map.
      const words = {
        Flows: `Added: 1 flow\nDraft (Draft_Idea)\n\nRemoved: 1 flow\nLead Nurture (old) (Lead_Nurture_Old)\n\n${flowsDated ? `Changed: 1 flow\n${set.label} (Invoice_Set_Status): last changed 13 Oct 2026 by Ash Rowe\n\n` : ""}${sentence("flows", "flow", flowsDated)}`,
        "Validation rules": `Added: none\n\nRemoved: 1 validation rule\nOld_Discount_Check on Opportunity\n\n${flowsDated ? "Changed: 1 validation rule\nOld_Check on Invoice__c: last changed 18 Jun 2025\n\n" : ""}${sentence("validation rules", "validation rule", flowsDated)}`,
        "Apex classes": `Added: none\n\nRemoved: 1 Apex class\nQuoteSync\n\n${apexDated ? "Changed: 1 Apex class\nFinanceSync: last changed 13 Oct 2026 by Ash Rowe\n\n" : ""}${sentence("Apex classes", "Apex class", apexDated)}`,
        "Apex triggers": `${apexDated ? "No Apex triggers were added, removed or changed." : "No Apex triggers were added or removed."}\n\n${sentence("Apex triggers", "Apex trigger", apexDated)}`,
      };
      for (const [title, expected] of Object.entries(words)) {
        const kind = kindIn(part, title);
        assert.equal(kind, expected, title);
        // Never both that dates were compared and that they weren't.
        const onlyAddedOrRemoved = kind.includes("we've compared only which were added or removed.");
        assert.equal(onlyAddedOrRemoved, title.startsWith("Apex") ? !apexDated : !flowsDated, title);
        assert.doesNotMatch(kind, /was last changed, so that isn't compared/, title);
        assert.ok(!(kind.includes("and when each was last changed") && onlyAddedOrRemoved), title);
        if (onlyAddedOrRemoved) assert.doesNotMatch(kind, /Changed: |or changed\./, title);
      }
      // Nothing derived is named, nor the notes about details that weren't compared.
      assert.doesNotMatch(part, /what it touches|formula|steps|events|whether it's a test|couldn't read the code|had more steps/);
    });
  }

  test("stamps that differ for the flows and rules only: those two compared by their dates, Apex in full; and the other way about", async () => {
    const planted = earlierOf(plantReadings);
    const metadataOlder = earlierOf((h) => {
      plantReadings(h);
      h.readBy = { metadata: 1, apex: 1, build: 1 };
    });
    const now = Handbook.parse({ ...after, readBy: { metadata: 2, apex: 1, build: 1 } });
    const c = compareHandbooks(metadataOlder.handbook, now);
    assert.deepEqual([compared(c.flows).reading, compared(c.validationRules).reading, compared(c.apexClasses).reading, compared(c.apexTriggers).reading], ["older", "older", null, null]);
    assert.deepEqual(names(c.flows).changed, [["Invoice_Set_Status", ["lastChanged"]]]);
    assert.deepEqual(names(c.validationRules).changed, [["Old_Check", ["lastChanged"]]]);
    // Apex in full: what it touches, its events and its date.
    assert.deepEqual(names(c.apexClasses).changed, [["FinanceSync", ["lastChanged"]], ["LeadRouter", ["touches"]]]);
    assert.deepEqual(names(c.apexTriggers).changed, [["InvoiceTrigger", ["events"]]]);
    const part = partOf(await textOf(metadataOlder, now));
    assert.match(kindIn(part, "Flows"), /\n\nYour earlier health check read your flows with an earlier version of our reader, which can read the same flow differently, so we've compared only which were added or removed and when each was last changed\.$/);
    assert.match(kindIn(part, "Validation rules"), /\n\nYour earlier health check read your validation rules with an earlier version of our reader, which can read the same validation rule differently, so we've compared only which were added or removed and when each was last changed\.$/);
    assert.match(kindIn(part, "Apex classes"), /\nLeadRouter: what it touches$/);
    assert.doesNotMatch(kindIn(part, "Apex classes") + kindIn(part, "Apex triggers"), /reader/);

    // The other way about: Apex by its dates, flows and rules in full.
    const apexOlder = earlierOf((h) => {
      plantReadings(h);
      h.readBy = { metadata: 1, apex: 1, build: 1 };
    });
    const apexNow = Handbook.parse({ ...after, readBy: { metadata: 1, apex: 2, build: 1 } });
    const back = compareHandbooks(apexOlder.handbook, apexNow);
    assert.deepEqual([compared(back.flows).reading, compared(back.validationRules).reading, compared(back.apexClasses).reading, compared(back.apexTriggers).reading], [null, null, "older", "older"]);
    assert.deepEqual(names(back.flows).changed, [["Invoice_Set_Status", ["lastChanged"]], ["Opportunity_Stage_Review", ["steps"]]]);
    assert.deepEqual(names(back.validationRules).changed, [["Reason_When_Escalated", ["formula"]], ["Old_Check", ["formula", "lastChanged"]]]);
    assert.deepEqual([names(back.apexClasses).changed, names(back.apexTriggers).changed], [[["FinanceSync", ["lastChanged"]]], []]);
    const backPart = partOf(await textOf(apexOlder, apexNow));
    assert.match(kindIn(backPart, "Apex triggers"), /^No Apex triggers were added, removed or changed\.\n\nYour earlier health check read your Apex triggers with an earlier version of our reader, which can read the same Apex trigger differently, so we've compared only which were added or removed and when each was last changed\.$/);
    // A higher number in the earlier file says only that the two differ.
    const higher = compareHandbooks(Handbook.parse({ ...planted.handbook, readBy: { metadata: 3, apex: 1, build: 1 } }), Handbook.parse({ ...after, readBy: { metadata: 2, apex: 1, build: 1 } }));
    assert.equal(compared(higher.flows).reading, "differs");
    const higherPart = partOf(await textOf({ handbook: Handbook.parse({ ...planted.handbook, readBy: { metadata: 3, apex: 1, build: 1 } }), asOf: EARLIER_AS_OF }, Handbook.parse({ ...after, readBy: { metadata: 2, apex: 1, build: 1 } })));
    assert.match(kindIn(higherPart, "Flows"), /\n\nYour earlier health check read your flows with a different version of our reader from this one, which can read the same flow differently, so we've compared only which were added or removed and when each was last changed\.$/);
  });

  test("the same step numbers and another build (decision 485): no detail named in any kind, only what was added or removed and when each was last changed, with the earlier-reader sentence", async () => {
    // Both version 6, both read by the same steps' readings, built by different code.
    const earlier = earlierOf((h) => {
      plantReadings(h);
      h.readBy = { metadata: 1, apex: 1, build: 1 };
    });
    const now = Handbook.parse({ ...after, readBy: { metadata: 1, apex: 1, build: 2 } });
    const c = compareHandbooks(earlier.handbook, now);
    for (const kind of Object.values(c)) assert.equal(compared(kind).reading, "older");
    // Only dates: the planted step, formulas, what a class touches and a trigger's events aren't named.
    assert.deepEqual(names(c.flows), { added: ["Draft_Idea"], removed: [REMOVED_FLOW], changed: [["Invoice_Set_Status", ["lastChanged"]]] });
    assert.deepEqual(names(c.validationRules), { added: [], removed: ["Old_Discount_Check"], changed: [["Old_Check", ["lastChanged"]]] });
    assert.deepEqual(names(c.apexClasses), { added: [], removed: ["QuoteSync"], changed: [["FinanceSync", ["lastChanged"]]] });
    assert.deepEqual(names(c.apexTriggers), NOTHING);
    const part = partOf(await textOf(earlier, now));
    for (const [title, many, one] of [["Flows", "flows", "flow"], ["Validation rules", "validation rules", "validation rule"], ["Apex classes", "Apex classes", "Apex class"], ["Apex triggers", "Apex triggers", "Apex trigger"]] as const) {
      assert.ok(kindIn(part, title).endsWith(`\n\nYour earlier health check read your ${many} with an earlier version of our reader, which can read the same ${one} differently, so we've compared only which were added or removed and when each was last changed.`), title);
    }
    assert.doesNotMatch(part, /: steps|formula|what it touches|events/);
    // A higher build in the earlier file says only that the two differ.
    const higher = compareHandbooks(earlierOf((h) => (h.readBy = { metadata: 1, apex: 1, build: 3 })).handbook, now);
    for (const kind of Object.values(higher)) assert.equal(compared(kind).reading, "differs");
    // And with no build recorded in the earlier file, as one from before 485 would have.
    const unbuilt = compareHandbooks(earlierOf((h) => (h.readBy = { metadata: 1, apex: 1, build: null })).handbook, now);
    for (const kind of Object.values(unbuilt)) assert.equal(compared(kind).reading, "unknownBefore");
  });

  test("the same build and another step number: that kind by its dates alone, the others in full", async () => {
    const earlier = earlierOf((h) => {
      plantReadings(h);
      h.readBy = { metadata: 1, apex: 1, build: 1 };
    });
    const now = Handbook.parse({ ...after, readBy: { metadata: 1, apex: 2, build: 1 } });
    const c = compareHandbooks(earlier.handbook, now);
    assert.deepEqual(Object.values(c).map((k) => compared(k).reading), [null, null, "older", "older"]);
    assert.deepEqual(names(c.flows).changed, [["Invoice_Set_Status", ["lastChanged"]], ["Opportunity_Stage_Review", ["steps"]]]);
    assert.deepEqual(names(c.apexClasses).changed, [["FinanceSync", ["lastChanged"]]]);
    assert.deepEqual(names(c.apexTriggers).changed, []);
    const part = partOf(await textOf(earlier, now));
    assert.match(kindIn(part, "Apex classes"), /\n\nYour earlier health check read your Apex classes with an earlier version of our reader, which can read the same Apex class differently, so we've compared only which were added or removed and when each was last changed\.$/);
    assert.doesNotMatch(kindIn(part, "Flows"), /reader/);
  });

  test("this health check's reading not known, or neither's: the sentence names the right one", async () => {
    const earlier = earlierOf((h) => {
      plantReadings(h);
      h.readBy = { metadata: 1, apex: 1, build: 1 };
    });
    const unknownNow = Handbook.parse({ ...after, readBy: { metadata: null, apex: 1, build: 1 } });
    assert.match(kindIn(partOf(await textOf(earlier, unknownNow)), "Validation rules"), /\n\nWe can't tell which version of our reader read your validation rules in this health check, and two versions can read the same validation rule differently, so we've compared only which were added or removed and when each was last changed\.$/);
    const neither = earlierOf((h) => {
      plantReadings(h);
      h.readBy = { metadata: null, apex: null, build: 1 };
    });
    const both = compareHandbooks(neither.handbook, Handbook.parse({ ...after, readBy: { metadata: null, apex: null, build: 1 } }));
    for (const kind of Object.values(both)) assert.equal(compared(kind).reading, "unknownBoth");
    assert.match(kindIn(partOf(await textOf(neither, Handbook.parse({ ...after, readBy: { metadata: null, apex: null, build: 1 } }))), "Apex classes"), /\n\nWe can't tell which version of our reader read your Apex classes in either health check, and two versions can read the same Apex class differently, so we've compared only which were added or removed and when each was last changed\.$/);
  });

  test("flows by another route and another reading: one sentence gives both reasons and says what follows once", async () => {
    // The earlier run's route is the Tooling route's, told from its rules' cover.
    const earlier = earlierOf((h) => {
      ok(h.validationRules).covers = { customObjects: "all", standardObjects: "all" };
      h.readBy = { metadata: null, apex: 1, build: 1 };
    });
    const c = compared(compareHandbooks(earlier.handbook, after).flows);
    assert.deepEqual([c.routes, c.reading], ["differ", "unknownBefore"]);
    const words = kindIn(partOf(await textOf(earlier)), "Flows");
    assert.equal(
      words,
      "No flows were added, removed or changed.\n\nWe've compared only which flows were added or removed and when each was last changed, for two reasons: your earlier health check read your flows by a different route from this one, and the two routes can give a different version of a flow; and we can't tell which version of our reader read your flows in your earlier health check, and two versions can read the same flow differently.",
    );
    assert.equal(words.match(/we've compared only/gi)?.length, 1);
  });

  test("the same reading both sides: nothing is said of it, and every detail is compared", () => {
    const c = compareHandbooks(earlierOf(plantReadings).handbook, after);
    for (const kind of Object.values(c)) assert.equal(compared(kind).reading, null);
    assert.deepEqual(names(c.apexClasses).changed, [["FinanceSync", ["lastChanged"]], ["LeadRouter", ["touches"]]]);
  });

  test("a class or trigger whose code one run couldn't read isn't changed in what it touches, its test or its events", async () => {
    const earlier = earlierOf((h) => {
      const k = ok(h.apexClasses).items.find((c) => c.name === "LeadRouter")!;
      k.isTest = null;
      k.touches = null;
      ok(h.apexTriggers).items.find((t) => t.name === "AccountTrigger")!.events = null;
    });
    const now = earlierOf((h) => {
      ok(h.apexClasses).items.find((c) => c.name === "LeadRouter")!.isTest = true;
      ok(h.apexTriggers).items.find((t) => t.name === "AccountTrigger")!.events = ["after insert"];
    }).handbook;
    const c = compareHandbooks(earlier.handbook, now);
    assert.deepEqual([names(c.apexClasses), names(c.apexTriggers)], [NOTHING, NOTHING]);
    const part = partOf(await textOf(earlier, now));
    assert.equal(kindIn(part, "Apex classes"), "No Apex classes were added, removed or changed.\n\nFor 1 Apex class, one of the two health checks couldn't read the code, so whether it's a test and what it touches aren't compared.");
    assert.match(kindIn(part, "Apex triggers"), /For 1 Apex trigger, one of the two health checks couldn't read the code or the events, or could name the object only by its id, so those details aren't compared\.$/);
  });

  test("a trigger's object one run named and the other gave only as its id isn't a move, and a real move is", () => {
    const earlier = earlierOf((h) => {
      const triggers = ok(h.apexTriggers).items;
      triggers.find((t) => t.name === "InvoiceAudit")!.object = "01I000000000001";
      triggers.find((t) => t.name === "AccountTrigger")!.object = "Contact";
    });
    assert.deepEqual(names(compareHandbooks(earlier.handbook, after).apexTriggers), { added: [], removed: [], changed: [["AccountTrigger", ["object"]]] });
  });

  test("a long formula is compared by what both kept and its length; a flow's cut steps by their count and the steps both kept", async () => {
    const long = "A".repeat(4_000);
    const withFormula = (length: number, text = long) => (h: Handbook) => Object.assign(ok(h.validationRules).items.find((r) => r.name === "Needs_Competitor")!, { formula: text, formulaCut: true, formulaLength: length });
    const now = earlierOf(withFormula(5_000)).handbook;
    assert.deepEqual(names(compareHandbooks(earlierOf(withFormula(5_000)).handbook, now).validationRules), NOTHING);
    assert.deepEqual(names(compareHandbooks(earlierOf(withFormula(5_200)).handbook, now).validationRules).changed, [["Needs_Competitor", ["formula"]]]);
    assert.deepEqual(names(compareHandbooks(earlierOf(withFormula(5_000, `${"A".repeat(3_999)}B`)).handbook, now).validationRules).changed, [["Needs_Competitor", ["formula"]]]);

    // 600 steps, of which each run kept 500: a different 500, the 400 both kept the same.
    const step = (n: number): HandbookStep => ({ kind: "assignments", name: `S${String(n).padStart(3, "0")}`, label: null, object: null, fields: [], fieldsTotal: 0, detail: null, outcomes: [], next: null, fault: null, reached: true });
    const steps = (from: number, total = 600) => (h: Handbook) => Object.assign(ok(h.flows).items.find((f) => f.name === "Lead_Assign")! as HandbookFlowItem, { steps: Array.from({ length: 500 }, (_, i) => step(from + i)), stepsTotal: total, cut: true });
    const cutNow = earlierOf(steps(101)).handbook;
    const c = compared(compareHandbooks(earlierOf(steps(1)).handbook, cutNow).flows);
    assert.deepEqual([names(c), c.stepsCut], [NOTHING, 1]);
    assert.deepEqual(names(compareHandbooks(earlierOf(steps(1, 601)).handbook, cutNow).flows).changed, [["Lead_Assign", ["steps"]]]);
    const changedStep = earlierOf((h) => {
      steps(1)(h);
      ok(h.flows).items.find((f) => f.name === "Lead_Assign")!.steps[200]!.label = "Renamed";
    });
    assert.deepEqual(names(compareHandbooks(changedStep.handbook, cutNow).flows).changed, [["Lead_Assign", ["steps"]]]);
    assert.match(kindIn(partOf(await textOf(earlierOf(steps(1)), cutNow)), "Flows"), /\n\n1 flow had more steps than this handbook keeps in one of the two health checks, so for that one we compared how many steps each has and the steps both kept\.$/);
  });

  test("steps no path reaches, listed in another order, aren't a change", () => {
    const flowOf = (h: Handbook) => ok(h.flows).items.find((f) => f.name === "Opportunity_Stage_Review")!;
    const earlier = earlierOf((h) => {
      const f = flowOf(h);
      const unreached = f.steps.filter((s) => !s.reached);
      unreached.push({ ...unreached[0]!, name: "Old_Step_Two" });
      f.steps = [...f.steps.filter((s) => s.reached), ...unreached];
      f.stepsTotal = f.steps.length;
    });
    const now = earlierOf((h) => {
      const f = flowOf(h);
      const unreached = flowOf(earlier.handbook).steps.filter((s) => !s.reached).reverse();
      f.steps = [...f.steps.filter((s) => s.reached), ...unreached];
      f.stepsTotal = f.steps.length;
    }).handbook;
    assert.deepEqual(names(compareHandbooks(earlier.handbook, now).flows), NOTHING);
  });
});

describe("the two metadata routes", () => {
  type Built = { dir: string; handbook: Handbook };
  async function built(options: ConstructorParameters<typeof Simulator>[0] = {}): Promise<Built> {
    const result = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport: new Simulator(options), log: () => {} });
    return { dir: result.dir, handbook: handbookIn(result.dir) };
  }
  let retrieve: Built;
  let tooling: Built;
  before(async () => {
    retrieve = await built();
    tooling = await built({ readOnly: true });
  });

  /** Every leaf that differs between two values, by its path. */
  function differences(a: unknown, b: unknown, path = ""): string[] {
    if (a !== null && b !== null && typeof a === "object" && typeof b === "object" && Array.isArray(a) === Array.isArray(b)) {
      const keys = [...new Set([...Object.keys(a), ...Object.keys(b)])];
      return keys.flatMap((k) => differences((a as Record<string, unknown>)[k], (b as Record<string, unknown>)[k], path ? `${path}.${k}` : k));
    }
    return Object.is(a, b) ? [] : [path];
  }

  test("a fresh run on either route records the reading of each half it read", () => {
    for (const { handbook } of [retrieve, tooling]) assert.deepEqual(handbook.readBy, { metadata: METADATA_READING, apex: APEX_READING, build: BUILD_READING });
    assert.deepEqual([METADATA_READING, APEX_READING], [1, 1]);
  });

  test("measured on the synthetic org: the two files differ only in which objects' rules they cover", () => {
    assert.deepEqual([routeOf(retrieve.handbook), routeOf(tooling.handbook)], ["retrieve", "tooling"]);
    assert.deepEqual(differences(retrieve.handbook, tooling.handbook), ["validationRules.covers.standardObjects"]);
  });

  test("across routes, a flow is compared only in whether it was added or removed and when it was last changed, and the part says why", async () => {
    const c = compareHandbooks(retrieve.handbook, tooling.handbook);
    for (const kind of Object.values(c)) assert.deepEqual(names(kind), NOTHING);
    assert.equal(compared(c.flows).routes, "differ");
    // Same route, both ways: no such note.
    assert.equal(compared(compareHandbooks(tooling.handbook, tooling.handbook).flows).routes, null);
    assert.equal(compared(compareHandbooks(retrieve.handbook, retrieve.handbook).flows).routes, null);
    // A flow whose status differs by route isn't changed, but a new date is.
    const edited = structuredClone(tooling.handbook);
    const flow = ok(edited.flows).items.find((f) => f.name === "Lead_Assign")!;
    flow.status = "Obsolete";
    flow.steps = [];
    flow.lastChanged = { at: "2026-10-30T08:00:00.000+0000", by: null };
    assert.deepEqual(names(compareHandbooks(retrieve.handbook, edited).flows).changed, [["Lead_Assign", ["lastChanged"]]]);
    const words = kindIn(partOf(await textOf({ handbook: retrieve.handbook, asOf: EARLIER_AS_OF }, edited)), "Flows");
    assert.equal(words, "Added: none\n\nRemoved: none\n\nChanged: 1 flow\nAssign Lead (Lead_Assign): last changed 30 Oct 2026\n\nYour earlier health check read your flows by a different route from this one, and the two routes can give a different version of a flow, so we've compared only which were added or removed and when each was last changed.");
  });

  test("a run whose route can't be told compares flows the same way, and says so", async () => {
    const unknown = Handbook.parse({ ...retrieve.handbook, validationRules: { status: "unavailable", reason: "x" } });
    assert.equal(routeOf(unknown), null);
    assert.equal(compared(compareHandbooks(unknown, retrieve.handbook).flows).routes, "unknown");
    assert.match(kindIn(partOf(await textOf({ handbook: unknown, asOf: EARLIER_AS_OF }, retrieve.handbook)), "Flows"), /\n\nWe can't tell whether your earlier health check read your flows the same way as this one, and two ways of reading them can give a different version of a flow, so we've compared only which were added or removed and when each was last changed\.$/);
  });

  test("a formula Tooling couldn't read is not read, never a change, and nor are its message and where it shows", async () => {
    const unread = structuredClone(tooling.handbook);
    Object.assign(ok(unread.validationRules).items.find((r) => r.name === "Needs_Competitor")!, { formula: null, formulaLength: null, message: null, messageField: null });
    const c = compared(compareHandbooks(retrieve.handbook, unread).validationRules);
    assert.deepEqual([names(c), c.unread], [NOTHING, 1]);
    assert.equal(
      kindIn(partOf(await textOf({ handbook: retrieve.handbook, asOf: EARLIER_AS_OF }, unread)), "Validation rules"),
      "No validation rules were added, removed or changed.\n\nFor 1 validation rule, one of the two health checks couldn't read the formula, so its formula, error message and where the message shows aren't compared.",
    );
  });

  test("a rule whose object Tooling could name only by its id is neither added nor removed against the retrieve's named one", async () => {
    const ids = structuredClone(tooling.handbook);
    ok(ids.validationRules).items.find((r) => r.name === "Due_Date_Required")!.object = "01I000000000001AAA";
    const c = compared(compareHandbooks(ids, retrieve.handbook).validationRules);
    assert.deepEqual([names(c), c.unnamed], [NOTHING, 1]);
    assert.match(kindIn(partOf(await textOf({ handbook: ids, asOf: EARLIER_AS_OF }, retrieve.handbook)), "Validation rules"), /\n\nWe left out 1 validation rule whose object one of the two health checks could name only by its id, so it isn't called added, removed or changed\.$/);
    // Kept under the same id by both runs, it's compared as any other.
    assert.equal(compared(compareHandbooks(ids, ids).validationRules).unnamed, 0);
  });
});

describe("the earlier handbook's folder", () => {
  /** A copy of the synthetic run, a re-check of a planted earlier one beside it. */
  function recheck(over: Parameters<typeof withEarlier>[2] = {}): { dir: string; earlier: string } {
    const dir = join(tempDir(), run.id);
    cpSync(recorded, dir, { recursive: true });
    return { dir, earlier: withEarlier(dir, plantEarlier, over) };
  }
  const named = (dir: string, baselineRunId: string): void => {
    const own = JSON.parse(readFileSync(join(dir, "run.json"), "utf8")) as Record<string, unknown>;
    writeFileSync(join(dir, "run.json"), JSON.stringify({ ...own, baselineRunId }));
  };

  test("is read by the report's route: beside the run's own, the same org", () => {
    const { dir } = recheck();
    const earlier = readEarlierHandbook(dir);
    assert.ok(earlier);
    assert.equal(earlier.asOf, EARLIER_AS_OF);
    assert.ok(ok(earlier.handbook.flows).items.some((f) => f.name === REMOVED_FLOW));
    // The report's comparison takes the very same check, so it finds the same run.
    assert.equal(loadRun(dir).input.baseline?.run.asOf, EARLIER_AS_OF);
  });

  test("needs only the earlier run.json and handbook.json, not its findings", () => {
    const { dir, earlier } = recheck();
    for (const file of ["findings.json", "metrics.json", "pseudonym-map.json", "checkpoints"]) rmSync(join(earlier, file), { recursive: true });
    assert.equal(readEarlierHandbook(dir)?.asOf, EARLIER_AS_OF);
    // The report needs its findings, and has no comparison without them, as before.
    assert.equal(loadRun(dir).input.baseline, null);
  });

  test("an org id that differs only after its 15th character is the same org", () => {
    const { dir } = recheck({ orgId: `${run.orgId.slice(0, 15)}XYZ` });
    assert.ok(readEarlierHandbook(dir));
  });

  const cases: Array<[string, (dir: string, earlier: string) => void]> = [
    ["its folder is gone", (_dir, earlier) => rmSync(earlier, { recursive: true })],
    ["its run.json is missing", (_dir, earlier) => rmSync(join(earlier, "run.json"))],
    ["its handbook.json is missing", (_dir, earlier) => rmSync(join(earlier, HANDBOOK_FILE))],
    ["its handbook.json is damaged", (_dir, earlier) => writeFileSync(join(earlier, HANDBOOK_FILE), "{ not json")],
    ["its handbook.json is in no format we read", (_dir, earlier) => writeFileSync(join(earlier, HANDBOOK_FILE), JSON.stringify({ version: 1 }))],
    ["it's another org's", (_dir, earlier) => {
      const other = JSON.parse(readFileSync(join(earlier, "run.json"), "utf8")) as Record<string, unknown>;
      writeFileSync(join(earlier, "run.json"), JSON.stringify({ ...other, orgId: "00D000000000002AAA" }));
    }],
  ];
  for (const [what, spoil] of cases) {
    test(`named and ${what}: the one sentence, no lists, and the document is still made`, async () => {
      const { dir, earlier } = recheck();
      spoil(dir, earlier);
      assert.equal(readEarlierHandbook(dir), null);
      const text = await textOf(readEarlierHandbook(dir));
      assert.equal(partOf(text), `## ${SINCE_TITLE}\n\n${NO_EARLIER}\n`);
      assert.match(text, /\nChanged since your last health check: there was no earlier handbook to compare with\.\n\nRecent changes: /);
      assert.doesNotMatch(text, /Added: |Removed: |Lead_Nurture_Old/);
    });
  }

  test("named with a path in its id: never read, though a handbook waits there", async () => {
    const { dir, earlier } = recheck();
    // A real earlier folder at the place the path would reach, which must still not be read.
    const away = `hc-since-escape-${process.pid}`;
    const reached = join(dir, "..", "..", away);
    cpSync(earlier, reached, { recursive: true });
    afterAll(() => rmSync(reached, { recursive: true, force: true }));
    for (const id of [`../${away}`, "..", ".", `../${basename(dir)}-earlier`, `${away}/../../${away}`]) {
      named(dir, id);
      assert.equal(readEarlierHandbook(dir), null, id);
      assert.equal(partOf(await textOf(readEarlierHandbook(dir))), `## ${SINCE_TITLE}\n\n${NO_EARLIER}\n`, id);
    }
  });

  test("its pseudonym map is never opened, and no name from it reaches the document", async () => {
    const { dir, earlier } = recheck();
    // The earlier file's tokens point at its own people, whom this run numbered otherwise.
    const handbook = JSON.parse(readFileSync(join(earlier, HANDBOOK_FILE), "utf8")) as Handbook;
    for (const k of ok(handbook.apexClasses).items) if (k.lastChanged) k.lastChanged = { at: "2026-01-01T00:00:00.000+0000", by: "[[user:U0001]]" };
    writeFileSync(join(earlier, HANDBOOK_FILE), JSON.stringify(handbook));
    const path = join(earlier, "pseudonym-map.json");
    for (const map of ["a readable map of other people", "garbage no one can open"] as const) {
      if (map === "a readable map of other people") writeFileSync(path, JSON.stringify([{ token: "[[user:U0001]]", id: "005000000000901AAA", name: "Baseline Onlyname", aliases: ["Baseline Onlyname"] }]));
      else {
        writeFileSync(path, "HB-NOT-A-MAP-0483 Baseline Onlyname");
        // Opening it would throw, and nothing on the way to the document catches that.
        chmodSync(path, 0o000);
      }
      const text = await textOf(readEarlierHandbook(dir));
      assert.ok(!text.includes("Baseline Onlyname"), map);
      // Every class changed in its date, each named as this run names its person.
      assert.match(kindIn(partOf(text), "Apex classes"), /\nFinanceSync: last changed 13 Oct 2026 by Ash Rowe\n/);
      chmodSync(path, 0o600);
    }
  });
});
