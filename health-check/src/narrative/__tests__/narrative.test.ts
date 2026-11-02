import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { anthropicClient, buildMessageParams, type MessagesApi, type NarrativeClient, type NarrativeReply, type NarrativeRequest } from "../client.ts";
import { draftSection, DRAFT_DEFAULTS, type SectionBrief } from "../draft.ts";
import { guardPrompt } from "../guard.ts";
import { DRAFT_SCHEMA, findingSource, metricSource, SYSTEM_PROMPT } from "../prompt.ts";
import { MODELS } from "../../config.ts";
import { buildDataset, SOURCE } from "../../numbercheck/index.ts";
import { Pseudonymiser } from "../../privacy/pseudonymiser.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers } from "../../testing/replay.ts";
import { usersTable } from "../../testing/synthetic-org.ts";
import type { Finding, Metric } from "../../schemas/index.ts";

const { output } = await runFromRecording({ answers: loadAnswers() });
const findings: Finding[] = output!.findings;
const metrics: Metric[] = output!.metrics;
const dataset = buildDataset({ findings, metrics, currency: "GBP" });

const stale = findings.find((f) => f.ruleId.includes("past") || /should already have closed/.test(f.title))!;
const winRate = metrics.find((m) => m.id === "historic.win_rate")!;

const brief = (overrides: Partial<SectionBrief> = {}): SectionBrief => ({
  title: "Pipeline",
  purpose: "Explain the state of the pipeline.",
  sources: [findingSource(stale), metricSource(winRate)],
  ...overrides,
});

function pseudonyms(): Pseudonymiser {
  const p = new Pseudonymiser();
  for (const user of usersTable) p.label("user", user.Id, user.Name, [user.Username, user.Email]);
  return p;
}

const reply = (paragraphs: unknown, overrides: Partial<NarrativeReply> = {}): NarrativeReply => ({ stopReason: "end_turn", text: JSON.stringify({ paragraphs }), model: MODELS.synthesis, ...overrides });

function stub(...replies: Array<NarrativeReply | Error>): NarrativeClient & { requests: NarrativeRequest[] } {
  const requests: NarrativeRequest[] = [];
  return {
    requests,
    async complete(request) {
      requests.push(request);
      const next = replies.shift();
      if (!next) throw new Error("Stub ran out of replies");
      if (next instanceof Error) throw next;
      return next;
    },
  };
}

const good = [{ text: "Your win rate is 35%, and 15 of 70 open deals are past their close date.", sources: [stale.id, winRate.id] }];
const draft = (client: NarrativeClient, b = brief()) => draftSection({ brief: b, dataset, client, pseudonyms: pseudonyms() });

describe("a draft", () => {
  test("is accepted when every figure traces", async () => {
    const result = await draft(stub(reply(good)));
    assert.equal(result.status, "accepted");
    assert.equal(result.status === "accepted" && result.attempts, 1);
    assert.match(result.status === "accepted" ? result.body : "", /35%/);
  });

  test("is rejected when a figure is invented, and never reaches the body", async () => {
    const bad = [{ text: "Your win rate is 97%.", sources: [winRate.id] }];
    const client = stub(reply(bad), reply(bad));
    const result = await draft(client);
    assert.equal(result.status, "rejected");
    assert.equal(client.requests.length, DRAFT_DEFAULTS.maxAttempts);
    assert.match(result.status === "rejected" ? result.reasons.join() : "", /97%/);
  });

  test("gets one redraft, told what failed", async () => {
    const client = stub(reply([{ text: "Your win rate is 97%.", sources: [winRate.id] }]), reply(good));
    const result = await draft(client);
    assert.equal(result.status, "accepted");
    assert.equal(result.status === "accepted" && result.attempts, 2);
    assert.match(client.requests[1]!.prompt, /earlier draft was rejected/);
    assert.match(client.requests[1]!.prompt, /97%/);
  });

  test("a figure must come from a source its own paragraph cites", async () => {
    const wrongCitation = [{ text: "Your win rate is 35%.", sources: [stale.id] }];
    assert.equal((await draft(stub(reply(wrongCitation), reply(wrongCitation)))).status, "rejected");
  });

  test("a paragraph with no sources can't hold a figure", async () => {
    const noSources = [{ text: "Your win rate is 35%.", sources: [] }];
    assert.equal((await draft(stub(reply(noSources), reply(noSources)))).status, "rejected");
    assert.equal((await draft(stub(reply([{ text: "We looked closely at your pipeline.", sources: [] }])))).status, "accepted");
  });

  test("can't cite a source outside its section", async () => {
    const other = findings.find((f) => f.id !== stale.id)!;
    const outside = [{ text: other.summary, sources: [other.id] }];
    const result = await draft(stub(reply(outside), reply(outside)));
    assert.equal(result.status, "rejected");
    assert.match(result.status === "rejected" ? result.reasons.join() : "", /aren't in this section/);
  });

  test("a figure from another finding in the dataset doesn't pass", async () => {
    const seats = findings.find((f) => f.facts.annualSaving === 18000)!;
    assert.ok(seats, "the synthetic run has an unassigned seats finding");
    const bad = [{ text: "You could save £18,000 a year.", sources: [stale.id] }];
    assert.equal((await draft(stub(reply(bad), reply(bad)))).status, "rejected");
  });

  test("a refusal is rejected without a retry, and can't crash the run", async () => {
    const client = stub({ stopReason: "refusal", text: "", model: MODELS.synthesis });
    const result = await draft(client);
    assert.equal(result.status, "rejected");
    assert.equal(client.requests.length, 1);
  });

  test("a cut-off reply is rejected even if what arrived looks fine", async () => {
    const client = stub(reply(good, { stopReason: "max_tokens" }));
    assert.equal((await draft(client)).status, "rejected");
    assert.equal(client.requests.length, 1);
  });

  test("invalid JSON or the wrong shape is rejected", async () => {
    for (const text of ["not json", "{}", '{"paragraphs":[]}', '{"paragraphs":[{"text":"","sources":[]}]}', '{"paragraphs":"35%"}']) {
      const bad = { stopReason: "end_turn", text, model: MODELS.synthesis };
      assert.equal((await draft(stub(bad, bad))).status, "rejected", text);
    }
  });

  test("a failed call is a rejection, not a crash", async () => {
    const result = await draft(stub(new Error("connection reset")));
    assert.equal(result.status, "rejected");
    assert.match(result.status === "rejected" ? result.reasons[0]! : "", /connection reset/);
  });

  test("an emoji is rejected", async () => {
    const bad = [{ text: "Great news \u{1F680} about your pipeline.", sources: [] }];
    assert.equal((await draft(stub(reply(bad), reply(bad)))).status, "rejected");
  });

  test("a name in the reply is rejected", async () => {
    const bad = [{ text: "Sam Reed should fix this.", sources: [] }];
    const result = await draft(stub(reply(bad), reply(bad)));
    assert.equal(result.status, "rejected");
    assert.match(result.status === "rejected" ? result.reasons.join() : "", /name/);
    const first = [{ text: "Sam should fix this.", sources: [] }];
    assert.equal((await draft(stub(reply(first), reply(first)))).status, "rejected");
  });

  test("uses the synthesis model unless the brief says otherwise", async () => {
    const client = stub(reply(good), reply(good));
    await draft(client);
    await draft(client, brief({ model: MODELS.notesAndScreenshots }));
    assert.equal(client.requests[0]!.model, MODELS.synthesis);
    assert.equal(client.requests[1]!.model, MODELS.notesAndScreenshots);
  });
});

describe("a real name can't reach the prompt", () => {
  const leakyFinding = (text: string): Finding => ({ ...stale, summary: text, title: text, recommendation: text, subjects: [{ type: "user", label: text }] });

  test("no synthetic user's name, username, email or id reaches it", async () => {
    for (const user of usersTable) {
      const text = `Ask ${user.Name} (${user.Username}, ${user.Email}, ${user.Id}) about this.`;
      const client = stub(reply([{ text: "We looked at this.", sources: [] }]));
      await draft(client, brief({ sources: [findingSource(leakyFinding(text))] }));
      const sent = `${client.requests[0]!.system}\n${client.requests[0]!.prompt}`;
      if (user.Name.includes(" ")) assert.ok(!sent.includes(user.Name), `${user.Name} leaked`);
      assert.ok(!sent.includes(user.Username), `${user.Username} leaked`);
      assert.ok(!sent.includes(user.Id), `${user.Id} leaked`);
      assert.doesNotMatch(sent, /@example\.com/);
    }
  });

  test("a first or last name on its own doesn't reach it", async () => {
    const client = stub(reply([{ text: "We looked at this.", sources: [] }]));
    await draft(client, brief({ sources: [findingSource(leakyFinding("Sam's pipeline and the Reed account dashboard"))] }));
    assert.doesNotMatch(client.requests[0]!.prompt, /\bSam\b|\bReed\b/);
    assert.match(client.requests[0]!.prompt, /\[\[user:U\d{4}\]\]/);
  });

  test("feedback goes through the guard too", async () => {
    const client = stub(reply([{ text: "Sam Reed won 97%.", sources: [winRate.id] }]), reply(good));
    await draft(client);
    assert.doesNotMatch(client.requests[1]!.prompt, /Sam Reed/);
  });

  test("ordinary words that share a name part survive", () => {
    const text = "Sales Cloud integration, Cross-sell and the Default Workflow User.";
    assert.equal(guardPrompt(text, pseudonyms()), text);
  });

  test("a pasted secret doesn't reach it", async () => {
    const client = stub(reply([{ text: "We looked at this.", sources: [] }]));
    await draft(client, brief({ sources: [findingSource(leakyFinding('apiKey = "abc123secretvalue" and Bearer abcdefghijklmnop1234567890'))] }));
    assert.doesNotMatch(client.requests[0]!.prompt, /abc123secretvalue|abcdefghijklmnop1234567890/);
  });

  test("the whole synthetic dataset goes through clean", async () => {
    const client = stub(reply([{ text: "We looked at this.", sources: [] }]));
    await draft(client, brief({ sources: [...findings.map(findingSource), ...metrics.map(metricSource)] }));
    const sent = client.requests[0]!.prompt;
    for (const user of usersTable) if (user.Name.includes(" ")) assert.ok(!sent.includes(user.Name), user.Name);
    assert.ok(!/SELECT /.test(sent), "queries stay out of the prompt");
    assert.ok(findings.some((f) => /\[\[user:/.test(f.summary)));
    assert.match(sent, /\[\[user:U\d{4}\]\]/);
  });
});

describe("the Claude request", () => {
  const request: NarrativeRequest = { model: MODELS.synthesis, system: SYSTEM_PROMPT, prompt: "Write.", schema: DRAFT_SCHEMA, effort: "high", maxTokens: 16000 };

  test("follows the current API, avoiding what returns a 400", () => {
    const params = buildMessageParams(request) as unknown as Record<string, unknown>;
    for (const banned of ["temperature", "top_p", "top_k", "tool_choice", "tools", "thinking", "output_format"]) assert.ok(!(banned in params), banned);
    assert.deepEqual(params.output_config, { effort: "high", format: { type: "json_schema", schema: DRAFT_SCHEMA } });
    assert.equal(params.model, "claude-opus-5-5");
  });

  test("never ends on an assistant turn, which would be a prefill", () => {
    const { messages } = buildMessageParams(request);
    assert.equal(messages.length, 1);
    assert.equal(messages[0]!.role, "user");
  });

  test("every object in the schema is closed", () => {
    assert.equal(DRAFT_SCHEMA.additionalProperties, false);
    assert.equal(DRAFT_SCHEMA.properties.paragraphs.items.additionalProperties, false);
  });

  function fakeSdk(message: unknown): MessagesApi & { params: unknown[] } {
    const params: unknown[] = [];
    return {
      params,
      messages: {
        stream(p) {
          params.push(p);
          return { finalMessage: async () => message as never };
        },
      },
    };
  }

  test("streams, and reads only text blocks", async () => {
    const sdk = fakeSdk({ stop_reason: "end_turn", model: "claude-opus-5-5", content: [{ type: "thinking", thinking: "" }, { type: "text", text: "{}" }] });
    const result = await anthropicClient(sdk).complete(request);
    assert.deepEqual(result, { stopReason: "end_turn", text: "{}", model: "claude-opus-5-5" });
    assert.equal(sdk.params.length, 1);
  });

  test("passes a refusal through with no content", async () => {
    const result = await anthropicClient(fakeSdk({ stop_reason: "refusal", model: "claude-opus-5-5", content: [] })).complete(request);
    assert.equal(result.stopReason, "refusal");
    assert.equal(result.text, "");
  });
});

describe("the dataset carries what narrative needs", () => {
  test("the summary source is available by id", () => {
    assert.ok(dataset.figures.some((f) => f.source === SOURCE.summary));
  });
});
