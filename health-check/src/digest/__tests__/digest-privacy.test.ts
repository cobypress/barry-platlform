import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { buildDigest, holdsPersonOrSecret } from "../build.ts";
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
