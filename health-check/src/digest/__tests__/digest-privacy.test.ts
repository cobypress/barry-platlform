import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { buildDigest, guardDigest, holdsPersonOrSecret, ID_MAPS, READINESS_CODE_PATHS, STRUCTURAL_KEYS } from "../build.ts";
import { PATHS_CODE_PATHS } from "../paths.ts";
import { GROUPS } from "../../value/model.ts";
import { holdsNameOrSecret } from "../../narrative/guard.ts";
import { buildReport } from "../../narrative/sections.ts";
import { findNames } from "../../privacy/names.ts";
import { loadRun } from "../../render/render-run.ts";
import { NAMED_PEOPLE_SENTENCE } from "../../schemas/digest.ts";
import type { Finding, Metric, Score } from "../../schemas/index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers } from "../../testing/replay.ts";

// Rule 7 for the run digest (DECISIONS.md 632): the privacy case the design
// asks of the engine, "no digest string holds a token, an email, an id shape or
// any known name". It lives here because the privacy folder's own tests are
// held still. Names, emails, ids, tokens and a secret are planted in every kind
// of text the digest carries, and none reaches it.

const { dir } = await runFromRecording({ answers: loadAnswers() });
const { input, pseudonyms } = loadRun(dir);

const person = pseudonyms.entries().find((e) => e.token.startsWith("[[user:") && /^\p{Lu}\p{Ll}{3,} \p{Lu}\p{Ll}{3,}$/u.test(e.name))!;
const [first, surname] = person.name.split(" ") as [string, string];
const EMAIL = "someone.planted@example.com";
const RECORD_ID = "0065000000PLnTd";
const TOKEN = "[[account:A0001]]";
const SECRET = "AKIAPLANTED000000001";

const PLANTED = {
  title: `${person.name} owns most of these deals`,
  summary: `Ask ${first} why these slipped.`,
  recommendation: `Email ${EMAIL} for the list.`,
  assumption: `${TOKEN} holds the largest`,
  reason: `Record ${RECORD_ID} couldn't be read`,
  runTogether: `The ${surname}Approval flow sends these`,
  metricLabel: `${surname}'s pipeline`,
  metricDefinition: `Keyed with ${SECRET} from the integration`,
  areaReason: `Only ${first} could see this area`,
};

const TOKEN_SHAPE = /\[\[[a-z]+[:\]]/;
const ID_SHAPE = /\b(?=[a-zA-Z0-9]*\d)[a-zA-Z0-9]{15}(?:[a-zA-Z0-9]{3})?\b/;

function strings(value: unknown): string[] {
  if (typeof value === "string") return [value];
  if (Array.isArray(value)) return value.flatMap(strings);
  if (value && typeof value === "object") return Object.entries(value).flatMap(([k, v]) => [k, ...strings(v)]);
  return [];
}

function planted(): { findings: Finding[]; metrics: Metric[]; scores: Score[]; ids: { issue: string; sized: string; open: string; metric: string; area: string } } {
  const issue = input.findings.find((f) => f.status === "issue" && f.recommendation)!;
  const sized = input.findings.find((f) => f.status === "issue" && f.value.kind !== "not_sized" && f.id !== issue.id)!;
  const open = input.findings.find((f) => f.status === "not_measurable")!;
  const findings = input.findings.map((f): Finding => {
    if (f.id === issue.id) return { ...f, title: PLANTED.title, summary: PLANTED.summary, recommendation: PLANTED.recommendation };
    if (f.id === sized.id && f.value.kind !== "not_sized") return { ...f, summary: PLANTED.runTogether, value: { ...f.value, assumptions: [PLANTED.assumption, ...f.value.assumptions] } };
    if (f.id === open.id) return { ...f, notMeasurableReason: PLANTED.reason };
    return f;
  });
  const metric = input.metrics.find((m) => m.status === "measured")!;
  const metrics = input.metrics.map((m): Metric => (m.id === metric.id ? { ...m, label: PLANTED.metricLabel, definition: PLANTED.metricDefinition } : m));
  const scores = input.scores ?? [];
  const area = scores.find((s) => s.status === "measured")!;
  const withArea = scores.map((s): Score => (s.domain === area.domain ? { status: "not_measurable", domain: s.domain, ruleSetVersion: s.ruleSetVersion, reason: PLANTED.areaReason } : s));
  return { findings, metrics, scores: withArea, ids: { issue: issue.id, sized: sized.id, open: open.id, metric: metric.id, area: area.domain } };
}

describe("the run digest holds no name, email, id, token or secret (DECISIONS.md 632)", () => {
  const plant = planted();
  const report = buildReport({ ...input, findings: plant.findings, metrics: plant.metrics, scores: plant.scores });
  const digest = buildDigest({ report, pseudonyms, inventory: null, madeAt: "2026-11-21T09:00:00.000Z" });
  const all = strings(digest);

  test("every planted text is the one fixed sentence", () => {
    const f = (id: string) => digest.findings.find((x) => x.id === id)!;
    assert.equal(f(plant.ids.issue).title, NAMED_PEOPLE_SENTENCE);
    assert.equal(f(plant.ids.issue).summary, NAMED_PEOPLE_SENTENCE);
    assert.equal(f(plant.ids.issue).recommendation, NAMED_PEOPLE_SENTENCE);
    assert.equal(f(plant.ids.sized).summary, NAMED_PEOPLE_SENTENCE);
    const value = f(plant.ids.sized).value;
    assert.ok(value.kind !== "not_sized" && value.assumptions[0] === NAMED_PEOPLE_SENTENCE);
    assert.equal(f(plant.ids.open).notMeasurableReason, NAMED_PEOPLE_SENTENCE);
    const metric = digest.metrics.find((m) => m.id === plant.ids.metric)!;
    assert.equal(metric.label, NAMED_PEOPLE_SENTENCE);
    assert.equal(metric.definition, NAMED_PEOPLE_SENTENCE);
    const area = digest.areas.find((a) => a.domain === plant.ids.area)!;
    assert.ok(area.status === "not_measurable" && area.reason === NAMED_PEOPLE_SENTENCE);
  });

  test("no digest string holds a token, an email, an id shape or any known name", () => {
    for (const s of all) {
      assert.doesNotMatch(s, TOKEN_SHAPE, s);
      assert.ok(!s.includes("@"), s);
      // Finding ids are a prefix and twelve hex characters, so they never take an id's shape.
      assert.doesNotMatch(s, ID_SHAPE, s);
      assert.deepEqual(findNames(s, pseudonyms), [], s);
      assert.ok(!holdsNameOrSecret(s, pseudonyms), s);
    }
    const text = JSON.stringify(digest);
    for (const part of [person.name, EMAIL, RECORD_ID, TOKEN, SECRET, `${surname}Approval`]) assert.ok(!text.includes(part), part);
  });

  test("the guard is the engine's own, with the shapes it leaves to scrub", () => {
    for (const text of Object.values(PLANTED)) assert.ok(holdsPersonOrSecret(text, pseudonyms), text);
    assert.ok(!holdsPersonOrSecret("Add a weekly check to pipeline reviews.", pseudonyms));
  });
});

/** Every string leaf, with the key it sits under (an array's items under the array's key), and its path with indices as [] (`at`). */
function leaves(value: unknown, key = "", path: string[] = [], at = ""): Array<{ key: string; path: string; at: string }> {
  if (typeof value === "string") return [{ key, path: path.join("."), at }];
  if (Array.isArray(value)) return value.flatMap((v, i) => leaves(v, key, [...path, String(i)], `${at}[]`));
  if (value && typeof value === "object") return Object.entries(value).flatMap(([k, v]) => leaves(v, ID_MAPS.has(key) ? key : k, [...path, k], at ? `${at}.${k}` : k));
  return [];
}
/** A leaf the guard leaves as it is: under a structural key, or at one of readiness's or the paths block's code paths (DECISIONS.md 742, 758). */
const structuralLeaf = (l: { key: string; at: string }): boolean => STRUCTURAL_KEYS.has(l.key) || ID_MAPS.has(l.key) || READINESS_CODE_PATHS.has(l.at) || PATHS_CODE_PATHS.has(l.at);

/** The same value with every string outside the structural keys and paths replaced by `text`. */
function plantEverywhere(value: unknown, text: string, key = "", at = ""): unknown {
  if (STRUCTURAL_KEYS.has(key) || ID_MAPS.has(key) || READINESS_CODE_PATHS.has(at) || PATHS_CODE_PATHS.has(at)) return value;
  if (typeof value === "string") return text;
  if (Array.isArray(value)) return value.map((v) => plantEverywhere(v, text, key, `${at}[]`));
  if (value && typeof value === "object") return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, plantEverywhere(v, text, k, at ? `${at}.${k}` : k)]));
  return value;
}

describe("every string the digest can hold goes through the guard (DECISIONS.md 632, fix round)", () => {
  const plain = buildDigest({ report: buildReport(input), pseudonyms, inventory: null, madeAt: "2026-11-21T09:00:00.000Z" });

  test("a name planted in every text field of the whole built digest is replaced, so a field added later can't be missed", () => {
    const text = `${person.name} holds most of these`;
    const guarded = guardDigest(plantEverywhere(plain, text), pseudonyms);
    const all = leaves(guarded);
    const textLeaves = all.filter((l) => !structuralLeaf(l));
    assert.ok(textLeaves.length > 500, `${textLeaves.length} text fields`);
    assert.ok(!JSON.stringify(guarded).includes(person.name));
    // Every text field the digest holds today, among them the reviewer's: a group's label, a value's period and unit, a not-sized reason, a measure's exclusions and assumptions, a phase's title and an area's reason.
    const keys = new Set(textLeaves.map((l) => l.key));
    for (const key of ["label", "period", "unit", "reason", "exclusions", "assumptions", "title", "summary", "recommendation", "definition", "notMeasurableReason", "opening", "question"]) assert.ok(keys.has(key), key);
  });

  test("the structural keys hold only ids, codes, timestamps and hashes, never a sentence", () => {
    const structural = leaves(plain).filter(structuralLeaf);
    const values = structural.map((l) => l.path.split(".").reduce<unknown>((v, k) => (v as Record<string, unknown>)[k], plain) as string);
    for (const v of values) assert.match(v, /^(?:[A-Za-z0-9][A-Za-z0-9_.:-]*|\d{4}-\d{2}-\d{2}T[\d:.]+Z)$/, v);
    // And alsoFor's keys, which are finding ids.
    for (const k of Object.keys(plain.plan.alsoFor)) assert.match(k, /^f-[0-9a-f]+$/);
  });

  test("planted in a group's label, a value's period, a not-sized reason and a measure's exclusions and assumptions, through the builder, none reaches the digest", () => {
    // A group the model names after its finding: one with no label of its own.
    const named = buildReport(input).value.groups.find((g) => !GROUPS[g.id] && g.kind !== "exposure") ?? buildReport(input).value.groups.find((g) => !GROUPS[g.id])!;
    const sized = input.findings.find((f) => f.status === "issue" && f.value.kind !== "not_sized" && f.id !== named.counted.findingId)!;
    const unsized = input.findings.find((f) => f.value.kind === "not_sized")!;
    const metric = input.metrics.find((m) => m.status === "measured")!;
    const token = person.token;
    const findings = input.findings.map((f): Finding => {
      if (f.id === named.counted.findingId) return { ...f, title: `${person.name} holds most of these` };
      if (f.id === sized.id && f.value.kind !== "not_sized") return { ...f, value: { ...f.value, period: `${f.value.period}, as ${first} counts it` } };
      if (f.id === unsized.id) return { ...f, value: { kind: "not_sized", reason: `Only ${EMAIL} knows` } };
      return f;
    });
    const metrics = input.metrics.map((m): Metric => (m.id === metric.id ? { ...m, exclusions: [`Deals ${surname} owns`], assumptions: [`${token} set the targets`] } : m));
    const digest = buildDigest({ report: buildReport({ ...input, findings, metrics }), pseudonyms, inventory: null, madeAt: "2026-11-21T09:00:00.000Z" });
    const group = digest.value.groups.find((g) => g.id === named.id)!;
    assert.equal(group.label, NAMED_PEOPLE_SENTENCE);
    const v = digest.findings.find((f) => f.id === sized.id)!.value;
    assert.ok(v.kind !== "not_sized" && v.period === NAMED_PEOPLE_SENTENCE);
    const u = digest.findings.find((f) => f.id === unsized.id)!.value;
    assert.ok(u.kind === "not_sized" && u.reason === NAMED_PEOPLE_SENTENCE);
    const m = digest.metrics.find((x) => x.id === metric.id)!;
    assert.ok(m.status === "measured" && m.exclusions[0] === NAMED_PEOPLE_SENTENCE && m.assumptions[0] === NAMED_PEOPLE_SENTENCE);
    const text = JSON.stringify(digest);
    for (const part of [person.name, first, surname, EMAIL, token]) assert.ok(!text.includes(part), part);
  });

  test("a group named after its finding takes the title as the executive report says it, not as read", () => {
    const named = buildReport(input).value.groups.find((g) => !GROUPS[g.id])!;
    const findings = input.findings.map((f): Finding => (f.id === named.counted.findingId ? { ...f, title: `${person.token} holds most of these deals` } : f));
    const digest = buildDigest({ report: buildReport({ ...input, findings }), pseudonyms, inventory: null, madeAt: "2026-11-21T09:00:00.000Z" });
    assert.equal(digest.value.groups.find((g) => g.id === named.id)!.label, "A single person holds most of these deals");
  });
});
