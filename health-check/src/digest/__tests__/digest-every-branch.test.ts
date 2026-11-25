import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { checkedDigest, digestTextsAt, marksOf, type CheckedDigest } from "../checked.ts";
import { scoreDomains } from "../../domains/scoring.ts";
import { SALES_CLOUD_MODULES } from "../../domains/index.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { buildReport, type ReportInput } from "../../narrative/sections.ts";
import { loadRun } from "../../render/render-run.ts";
import { fieldsNotInOrg, refusalSentence, type RefusalKind } from "../../salesforce/refusal-words.ts";
import type { RunDigest } from "../../schemas/digest.ts";
import { AutomaticReplay, automaticSyntheticCalibration } from "../../testing/automatic.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording, tempDir } from "../../testing/harness.ts";
import { syntheticProvider } from "../../testing/replay.ts";
import { Simulator, type SimulatorOptions } from "../../testing/simulator.ts";
import type { Transport, TransportRequest, TransportResponse } from "../../salesforce/transport.ts";
import { MAX_ITEM_READS } from "../../extract/metadata-tooling.ts";
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
  // Each read alone, with one reason, every reason in turn: what an unreadable read reaches is the producer's head,
  // the same whatever follows it. And each reason with every read unreadable at once: the passed-on route knows a head
  // and what follows it apart (the rest is read on its own after a head of ours), so a reason every head passes on here
  // is known after each of them. Together they reach every head with every reason, at a sixth of the full cross.
  ids.forEach((id, i) => {
    const reason = REASONS[i % REASONS.length]!;
    test(`${id}, ${reason.slice(0, 40)}`, () => {
      const changed = clone(extracts);
      changed.set(id, unreadable(extracts.get(id), reason));
      released(checked(changed), id);
    });
  });
  for (const reason of REASONS) {
    test(`every read at once, ${reason.slice(0, 40)}`, () => {
      const changed = new Map([...extracts].map(([id, v]) => [id, unreadable(v, reason)]));
      released(checked(changed), "every read");
    });
  }
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

  test("readiness's withheld texts stay out of the hold counts: the run is released or held as it is without the block (742)", () => {
    const thin = { ...clone(calibration), minSample: 1_000_000 };
    for (const [cal, changed] of [[thin, extracts], [calibration, new Map([...extracts].map(([id, v]) => [id, unreadable(v, REASONS[0]!)]))]] as const) {
      const analysis = analyseExtracts(changed, { calibration: cal });
      const input: ReportInput = { ...loaded.input, calibration: cal, findings: analysis.findings, metrics: analysis.metrics, scores: scoreDomains(loaded.input.run.modules, analysis.findings, loaded.input.run.ruleSetVersion) };
      const withBlock = checkedDigest({ input, pseudonyms: loaded.pseudonyms, inventory: null, madeAt: MADE_AT });
      const without = checkedDigest({ input, pseudonyms: loaded.pseudonyms, inventory: null, madeAt: MADE_AT, build: (i) => ({ ...buildReport(i), readiness: undefined }) as unknown as ReturnType<typeof buildReport> });
      assert.ok(withBlock.digest?.readiness);
      const counts = (c: CheckedDigest) => ({ held: c.held, unmarked: c.unmarked, unmatched: c.unmatched, masks: c.masks });
      assert.deepEqual(counts(withBlock), counts(without));
    }
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

/** The simulator, with this many more Workflow Rules listed than it holds: as an org past the item reads' limit. */
class ManyRules implements Transport {
  readonly #inner: Simulator;
  readonly #extra: number;
  constructor(options: SimulatorOptions, extra: number) {
    this.#inner = new Simulator(options);
    this.#extra = extra;
  }
  async send(request: TransportRequest): Promise<TransportResponse> {
    const r = await this.#inner.send(request);
    if (!/FROM\+WorkflowRule(?!\+WHERE)/.test(request.url) && !/FROM WorkflowRule(?! WHERE)/.test(decodeURIComponent(request.url).replace(/\+/g, " "))) return r;
    const body = JSON.parse(r.text) as { totalSize: number; records: unknown[] };
    for (let i = 0; i < this.#extra; i++) body.records.push({ attributes: { type: "WorkflowRule" }, Id: `01Q${String(i).padStart(15, "0")}`, Name: `Rule_${i}`, TableEnumOrId: "Account", NamespacePrefix: null });
    body.totalSize = body.records.length;
    return { status: r.status, header: (name) => r.header(name), text: JSON.stringify(body) };
  }
}

describe("Workflow Rules unread on the Tooling route, end to end (DECISIONS.md 743)", () => {
  // The Tooling route is the one a read-only or Professional Edition connection takes; both of its ways to leave
  // Workflow Rules unread are driven through the real reads, never an `unread` put in by hand.
  const automatic = async (transport: Transport) => {
    const run = await runHealthCheck({ provider: syntheticProvider, calibration, modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport, connectedUser: CONNECTED_USER, log: () => {}, reviewMode: "automatic" });
    const { input, pseudonyms } = loadRun(run.dir);
    return checkedDigest({ input, pseudonyms, inventory: null, madeAt: MADE_AT });
  };
  const served = (c: CheckedDigest, words: RegExp) => digestTextsAt(c.digest!).filter((t) => words.test(t.text));

  test("more Workflow Rules and flows than we read one at a time: said, and released", async () => {
    const c = await automatic(new ManyRules({ readOnly: true }, MAX_ITEM_READS + 1));
    released(c, "too many rules");
    assert.ok(served(c, /Workflow Rules and \d+ flows, more than the \d+ we read one at a time/).length > 0);
  });

  test("a Workflow Rule without its definition: its name marked as the org's, and released", async () => {
    const c = await automatic(new Simulator({ organizationType: "Professional Edition", withholdMetadata: (soql) => /FROM WorkflowRule WHERE/.test(soql) }));
    released(c, "rule without definition");
    const texts = served(c, /Workflow Rule (.+) came back without its definition/);
    assert.ok(texts.length > 0);
    for (const t of texts) {
      const name = /Workflow Rule (.+) came back/.exec(t.text)![1]!;
      assert.ok(marksOf(c.digest!, t)!.orgWords.some((s) => t.text.slice(s.start, s.end) === name), t.text);
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
