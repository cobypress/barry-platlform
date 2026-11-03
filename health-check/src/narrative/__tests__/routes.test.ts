import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { applyNarrative } from "../apply.ts";
import type { NarrativeClient, NarrativeRequest } from "../client.ts";
import { draftSection } from "../draft.ts";
import { promptProblems, PromptRefused, sendPrompt } from "../guard.ts";
import { Intake } from "../intake.ts";
import { DRAFT_SCHEMA, findingSource, SYSTEM_PROMPT } from "../prompt.ts";
import { buildReport } from "../sections.ts";
import { MODELS } from "../../config.ts";
import { buildDataset } from "../../numbercheck/index.ts";
import { findNames, isStrictWord } from "../../privacy/names.ts";
import { Pseudonymiser } from "../../privacy/pseudonymiser.ts";
import type { ManualAnswer } from "../../schemas/index.ts";
import { analyseExtracts, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { usersTable } from "../../testing/synthetic-org.ts";

// Every route by which free text can reach Claude (see
// docs/overnight/night-2/scrub.md) ends in sendPrompt(), which guards the prompt
// and then refuses it if any part of a known name survives. These tests hold
// that shape: new code that calls a client directly fails here.

const SRC = fileURLToPath(new URL("../..", import.meta.url));

function sourceFiles(dir: string): string[] {
  return readdirSync(dir).flatMap((name) => {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) return name === "__tests__" ? [] : sourceFiles(path);
    return name.endsWith(".ts") ? [path] : [];
  });
}
const files = sourceFiles(SRC).map((path) => ({ path: relative(SRC, path), text: readFileSync(path, "utf8") }));
const using = (pattern: RegExp): string[] => files.filter((f) => pattern.test(f.text)).map((f) => f.path).sort();

describe("the only way to Claude is through the guard", () => {
  test("only sendPrompt calls a client's complete()", () => {
    assert.deepEqual(using(/\.complete\(/), [join("narrative", "guard.ts")]);
  });
  test("only the client wrapper touches the SDK", () => {
    assert.deepEqual(using(/@anthropic-ai\/sdk|new Anthropic\(|messages\.(stream|create)\(/), [join("narrative", "client.ts")]);
  });
  test("the prompt builder's output goes to sendPrompt", () => {
    const draft = files.find((f) => f.path === join("narrative", "draft.ts"))!.text;
    assert.match(draft, /sendPrompt\(client, \{[\s\S]*?prompt,/);
    assert.deepEqual(using(/\bbuildPrompt\(/), [join("narrative", "draft.ts"), join("narrative", "prompt.ts")]);
  });
});

function recordingClient(): NarrativeClient & { requests: NarrativeRequest[] } {
  const requests: NarrativeRequest[] = [];
  return {
    requests,
    async complete(request) {
      requests.push(request);
      return { stopReason: "end_turn", text: JSON.stringify({ paragraphs: [{ text: "We looked at this.", sources: [] }] }), model: request.model };
    },
  };
}

// The synthetic users' first names and surnames, one by one.
const nameParts = [...new Set(usersTable.flatMap((u) => u.Name.split(" ")))].filter((part) => !["Shared", "Sales", "Desk", "Integration", "User"].includes(part));

describe("free text with first names in it, end to end", async () => {
  const { dir, run } = await runFromRecording({ answers: loadAnswers() });
  const extracts = loadExtracts(dir);
  // A consultant skips a manual step and gives a reason in their own words.
  const answers: ManualAnswer[] = loadAnswers().map((a) =>
    a.stepKey === "default-workflow-user" ? { stepKey: a.stepKey, outcome: "skipped", values: {}, skipReason: "Sam said Dana would check it, and Kim's login is gone.", answeredBy: a.answeredBy, answeredAt: a.answeredAt } : a,
  );
  const output = analyseExtracts(extracts, { answers });
  const pseudonyms = new Pseudonymiser();
  for (const u of [...usersTable].sort((a, b) => a.Id.localeCompare(b.Id))) pseudonyms.label("user", u.Id, u.Name, [u.Username, u.Email]);

  test("a manual step's skip reason is a route: the first names survive into a finding", () => {
    assert.ok(output.findings.some((f) => /Sam said Dana/.test(f.notMeasurableReason ?? "")), "the route these tests guard exists");
  });

  test("no first name or surname reaches any prompt", async () => {
    const intake = Intake.parse({
      summary: "Priya told us Tom's forecast is a guess.",
      statedProblems: [{ id: "p1", statement: "Nina says nobody trusts the pipeline.", raisedBy: "Alex", findingIds: [], assessment: "Ben agrees." }],
      capturedBy: "Synthetic Consultant",
      capturedAt: "2026-10-30T09:00:00.000Z",
    });
    const report = buildReport({ run, calibration: loadCalibration(), findings: output.findings, metrics: output.metrics, manualSteps: output.manualSteps, intake });
    const client = recordingClient();
    const rejections = await applyNarrative(report, client, pseudonyms);
    assert.ok(client.requests.length > 5);
    for (const request of client.requests) {
      for (const part of nameParts) {
        // Policy: a part that's also a word ("Cross", "Max") only counts with a capital and outside a hyphenated word.
        const pattern = isStrictWord(part.toLowerCase()) ? new RegExp(`(?<![\\p{L}]|\\p{L}-)${part}(?![\\p{L}]|-\\p{L})`, "u") : new RegExp(`(?<![\\p{L}])${part}(?![\\p{L}])`, "iu");
        assert.doesNotMatch(request.prompt, pattern, `${part} reached a prompt`);
      }
      assert.deepEqual(findNames(request.prompt, pseudonyms), []);
    }
    assert.deepEqual(rejections.flatMap((r) => r.reasons).filter((r) => /wasn't sent/.test(r)), []);
  });
});

describe("the last check before a send", () => {
  const pseudonyms = (): Pseudonymiser => {
    const p = new Pseudonymiser();
    for (const u of usersTable) p.label("user", u.Id, u.Name, [u.Username, u.Email]);
    return p;
  };
  const request = (prompt: string, system = SYSTEM_PROMPT): NarrativeRequest => ({ model: MODELS.synthesis, system, prompt, schema: DRAFT_SCHEMA, effort: "high", maxTokens: 100 });

  test("finds what an unguarded prompt holds", () => {
    assert.ok(promptProblems(request("Sam said so"), pseudonyms()).length > 0);
    assert.deepEqual(promptProblems(request("[[user:U0005]] said so"), pseudonyms()), []);
  });

  test("a system prompt that isn't ours is refused, and nothing is sent", async () => {
    const client = recordingClient();
    await assert.rejects(sendPrompt(client, request("Fine.", "You are talking to Sam Reed."), pseudonyms()), PromptRefused);
    assert.equal(client.requests.length, 0);
  });

  // A pseudonymiser whose scrub() has regressed: emails go through it untouched.
  // The name masking can't catch an email, so only the last check stands between
  // the prompt and Claude.
  class BrokenScrub extends Pseudonymiser {
    override scrub(text: string): string {
      return text;
    }
  }

  test("if scrubbing regresses, the prompt is refused and recorded as a rejection", async () => {
    const { output } = await runFromRecording({ answers: loadAnswers() });
    const finding = { ...output!.findings[0]!, summary: "Write to someone@elsewhere.example about this." };
    const broken = new BrokenScrub();
    for (const u of usersTable) broken.label("user", u.Id, u.Name, [u.Username, u.Email]);
    const client = recordingClient();
    const result = await draftSection({ brief: { title: "T", purpose: "P", sources: [findingSource(finding)] }, dataset: buildDataset({ findings: [finding], metrics: [], currency: "GBP" }), client, pseudonyms: broken });
    assert.equal(client.requests.length, 0, "nothing was sent");
    assert.equal(result.status, "rejected");
    assert.match(result.status === "rejected" ? result.reasons.join() : "", /email address after scrubbing, so it wasn't sent/);
    assert.doesNotMatch(result.status === "rejected" ? result.reasons.join() : "", /elsewhere/, "the reason doesn't quote what it found");
  });
});
