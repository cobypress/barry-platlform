import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { checkedDigest, digestTextsAt, marksOf, type CheckedDigest } from "../checked.ts";
import { scoreDomains } from "../../domains/scoring.ts";
import { SALES_CLOUD_MODULES } from "../../domains/index.ts";
import { runHealthCheck } from "../../engine/run.ts";
import type { ReportInput } from "../../narrative/sections.ts";
import { loadRun } from "../../render/render-run.ts";
import { fieldsNotInOrg, refusalSentence, type RefusalKind } from "../../salesforce/refusal-words.ts";
import type { RunDigest } from "../../schemas/digest.ts";
import { AutomaticReplay, automaticSyntheticCalibration } from "../../testing/automatic.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording, tempDir } from "../../testing/harness.ts";
import { syntheticProvider } from "../../testing/replay.ts";
import { Simulator, type SimulatorOptions } from "../../testing/simulator.ts";
import { AS_OF, CONNECTED_USER } from "../../testing/synthetic-org.ts";

// The branches a real org reaches and the synthetic org doesn't (DECISIONS.md
// 743), driven through the real analysis, digest build and check of an
// automatic run. A real customer-shaped org was held twice by sentences of our
// own that only its shape reached; here every read the run makes is, in turn,
// unreadable (each way a read is refused or lost, an extractor's own sentence,
// a field the read needs) and empty; every sample is too thin for the
// calibrated minimum; the org's own words are hostile; and the simulator's
// connections a customer brings (read-only, Professional Edition, no CPQ or
// Account Engagement) run end to end. Each must be released, with no sentence
// unmarked, no figure untraced and nothing withheld, and the org's words marked
// as the org's exactly. No real data: the synthetic org, changed in place.

const MADE_AT = "2026-11-25T09:00:00.000Z";
const calibration = await automaticSyntheticCalibration();
const made = await runFromRecording({ calibration, reviewMode: "automatic", transport: new AutomaticReplay() });
const loaded = loadRun(made.dir);
const extracts = loadExtracts(made.dir);

/** The automatic run's digest check over these extracts, as the worker makes it. */
function checked(changed: Map<string, unknown>, cal = calibration): CheckedDigest {
  const analysis = analyseExtracts(changed, { calibration: cal });
  const input: ReportInput = { ...loaded.input, calibration: cal, findings: analysis.findings, metrics: analysis.metrics, scores: scoreDomains(loaded.input.run.modules, analysis.findings, loaded.input.run.ruleSetVersion) };
  return checkedDigest({ input, pseudonyms: loaded.pseudonyms, inventory: null, madeAt: MADE_AT });
}

/** What a failure names: every text served unmarked, by path. */
const unmarkedIn = (d: RunDigest | null): string[] => (d ? digestTextsAt(d).filter((t) => !marksOf(d, t)).map((t) => `${t.path}: ${t.text}`) : []);

function released(c: CheckedDigest, what: string): void {
  assert.deepEqual({ held: c.held, unmarked: c.unmarked, unmatched: c.unmatched, masks: c.masks }, { held: null, unmarked: 0, unmatched: 0, masks: 0 }, `${what}\n${unmarkedIn(c.digest).join("\n")}`);
}

/** Each way an extractor's reason is written: a refusal of each kind, one of the extractors' own sentences, and a field the read needs. */
const KINDS: RefusalKind[] = ["no_permission", "refused", "unanswered", "not_available"];
const REASONS = [...KINDS.map((k) => refusalSentence("Account", k)), "The org reported no limits.", fieldsNotInOrg(["Account.Region__c", "Account.Tier__c"])];

/** The extract with every read in it unreadable for this reason. A composite extract has each of its reads changed. */
function unreadable(value: unknown, reason: string): unknown {
  const v = value as Record<string, unknown>;
  const source = (v && "source" in v ? v.source : { kind: "soql", api: "rest", query: "x" }) as unknown;
  if ("status" in v) return { status: "unavailable", reason, source };
  const out = clone(v);
  for (const [k, part] of Object.entries(out)) if (part && typeof part === "object" && "status" in part) out[k] = { status: "unavailable", reason, source: (part as { source?: unknown }).source ?? source };
  return out;
}

describe("every read unreadable in turn, on an automatic run (DECISIONS.md 743)", () => {
  const ids = [...extracts.keys()];
  test("the run reads enough steps for this to mean something", () => assert.ok(ids.length > 40, `${ids.length}`));
  ids.forEach((id, i) => {
    // Each read refused (a different kind in turn, so every kind is met many times over), with an extractor's own
    // sentence, and with a field the read needs: the three ways a reason is known (DECISIONS.md 702, 743).
    for (const reason of [REASONS[i % KINDS.length]!, ...REASONS.slice(KINDS.length)]) {
      test(`${id}, ${reason.slice(0, 40)}`, () => {
        const changed = clone(extracts);
        changed.set(id, unreadable(extracts.get(id), reason));
        released(checked(changed), id);
      });
    }
  });
});

describe("every read empty in turn, on an automatic run (DECISIONS.md 743)", () => {
  for (const [id, value] of extracts) {
    const v = value as { status?: string; data?: unknown };
    if (v?.status !== "ok" || !Array.isArray(v.data)) continue;
    test(id, () => {
      const changed = clone(extracts);
      changed.set(id, { ...v, data: [] });
      released(checked(changed), id);
    });
  }
});

describe("every sample too thin, on an automatic run (DECISIONS.md 743)", () => {
  test("a calibrated minimum no org reaches leaves every metric and check not measurable, each reason marked", () => {
    const thin = { ...clone(calibration), minSample: 1_000_000 };
    const c = checked(extracts, thin);
    released(c, "thin");
    assert.ok(c.digest!.metrics.filter((m) => m.status === "not_measurable").length > 20);
  });
});

describe("the org's own words, hostile, on an automatic run (DECISIONS.md 743)", () => {
  // Stage names with quotes, digits and accents, one that is a word of ours, and one that is a whole sentence of ours.
  const HOSTILE: Record<string, string> = {
    "Closed Lost": 'Lost "hard" 2026',
    Qualified: "Qualifiée ✓ 3",
    "Closed No Decision": "deals",
    "Closed Lost - No Budget": "The org reported no limits.",
  };
  const renamed = (value: unknown): unknown =>
    typeof value === "string" ? (HOSTILE[value] ?? value) : Array.isArray(value) ? value.map(renamed) : value && typeof value === "object" ? Object.fromEntries(Object.entries(value).map(([k, v]) => [k, renamed(v)])) : value;
  // The deals and their history take the names; Salesforce's own stage list doesn't, so the calibration knows none of them.
  const changed = new Map([...extracts].map(([id, v]) => [id, id === "opportunity-stages" ? v : renamed(v)]));
  const c = checked(changed);

  test("is released, and the stages the calibration doesn't know are listed with each name marked exactly", () => {
    released(c, "hostile");
    const d = c.digest!;
    const listed = digestTextsAt(d).filter((t) => t.text.includes("what these stages mean: "));
    assert.ok(listed.length > 0);
    for (const t of listed) {
      const spans = marksOf(d, t)!.orgWords.map((s) => t.text.slice(s.start, s.end));
      const names = t.text.slice(t.text.indexOf(": ") + 2, -1).split(", ");
      assert.deepEqual(spans, names, t.text);
      for (const hostile of Object.values(HOSTILE)) assert.ok(names.includes(hostile), `${hostile} in ${t.text}`);
    }
  });

  test("a span is only ever on words the org's data holds, so none of ours is served as the org's, nor the org's as ours", () => {
    const orgText: string[] = [];
    const collect = (v: unknown): void => void (typeof v === "string" ? orgText.push(v) : Array.isArray(v) ? v.forEach(collect) : v && typeof v === "object" ? Object.values(v).forEach(collect) : undefined);
    [...changed.values(), calibration].forEach(collect);
    // Our words for a value the org left empty ("No role"), put where the org's value goes and marked with it: over-marking is safe.
    const OUR_EMPTY = /^No [a-z][a-z ]*$/;
    const d = c.digest!;
    for (const t of digestTextsAt(d)) {
      for (const s of marksOf(d, t)!.orgWords) {
        const span = t.text.slice(s.start, s.end);
        assert.ok(orgText.some((o) => o.includes(span)) || OUR_EMPTY.test(span) || /^\[\[user:U\d{4}\]\]$/.test(span), `${t.path}: "${span}" in ${t.text}`);
      }
      // The stage named as a sentence of ours is never read as ours where it stands.
      if (t.text === HOSTILE["Closed Lost - No Budget"]) assert.notDeepEqual(marksOf(d, t)!.orgWords, [], t.path);
    }
  });
});

describe("the connections a customer brings, end to end on the simulator (DECISIONS.md 743)", () => {
  const VARIANTS: Array<[string, SimulatorOptions]> = [
    ["read-only", { readOnly: true }],
    ["Professional Edition", { organizationType: "Professional Edition" }],
    ["read-only on Professional Edition", { readOnly: true, organizationType: "Professional Edition" }],
    ["no CPQ and no Account Engagement", { cpq: false, pardot: false }],
    ["no async Apex limit reported", { asyncApex: null }],
  ];
  for (const [name, options] of VARIANTS) {
    test(`${name}: released, nothing unmarked or withheld`, async () => {
      const run = await runHealthCheck({ provider: syntheticProvider, calibration, modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport: new Simulator(options), connectedUser: CONNECTED_USER, log: () => {}, reviewMode: "automatic" });
      const { input, pseudonyms } = loadRun(run.dir);
      released(checkedDigest({ input, pseudonyms, inventory: null, madeAt: MADE_AT }), name);
    });
  }
});
