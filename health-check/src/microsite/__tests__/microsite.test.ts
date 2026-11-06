import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { createDecipheriv } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { buildReport, type ReportInput } from "../../narrative/sections.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { scoreDomains } from "../../domains/scoring.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { checkNumbers } from "../../numbercheck/index.ts";
import { buildMicrositePages, pageText } from "../pages.ts";
import { syntheticInterviews } from "../../testing/interviews.ts";
import { micrositeKey, MicrositeKeyError, sealPages } from "../seal.ts";

// The console's fixed vector (blackcloud-next docs/health-check/microsite-crypto-vector.json). Synthetic.
const VECTOR = {
  keyBase64: "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=",
  ivBase64url: "oKGio6Slpqeoqaqr",
  runId: "run-vector-0001",
  plaintext: '{"pages":[{"slug":"summary","title":"Summary","html":"<h1>Synthetic Co: your Salesforce health check</h1><p>Score 64 out of 100.</p>"}]}',
  sealed:
    "v1.oKGio6Slpqeoqaqr.Es7_G-Gi6vBVNN6WVUb_BQ.wvNNB5rQKSZFVdlJgulV43XtkmotBQ3lVSL30XA6sUFdtLvyzYHFWUAXsuCEGmxPbdsc5va5wr152tCJrEJvDdgrwMTtRHbRo_8zhiKJmrOo7HAkCpU3VqGXp1aTr3XCbLthNn3OMJiatT5IIAWTtITyb0Pxv6ml5I4ultt3AvvGojbBE7i2jg",
};

const open = (sealed: string, runId: string, key: Buffer): string => {
  const [, iv, tag, ct] = sealed.split(".");
  const d = createDecipheriv("aes-256-gcm", key, Buffer.from(iv!, "base64url"));
  d.setAAD(Buffer.from(runId, "utf8"));
  d.setAuthTag(Buffer.from(tag!, "base64url"));
  return Buffer.concat([d.update(Buffer.from(ct!, "base64url")), d.final()]).toString("utf8");
};

describe("sealing the microsite", () => {
  test("matches the console's fixed vector exactly", () => {
    const key = Buffer.from(VECTOR.keyBase64, "base64");
    assert.equal(sealPages(VECTOR.plaintext, VECTOR.runId, key, Buffer.from(VECTOR.ivBase64url, "base64url")), VECTOR.sealed);
  });

  test("a value sealed for one run won't open as another's", () => {
    const key = Buffer.from(VECTOR.keyBase64, "base64");
    const sealed = sealPages("{}", "run-a", key);
    assert.equal(open(sealed, "run-a", key), "{}");
    assert.throws(() => open(sealed, "run-b", key));
  });

  test("the key is 32 bytes of base64, different from both connect keys, and optional", () => {
    assert.equal(micrositeKey({}), null);
    assert.throws(() => micrositeKey({ HC_MICROSITE_KEY: "not base64!" }), MicrositeKeyError);
    assert.throws(() => micrositeKey({ HC_MICROSITE_KEY: Buffer.alloc(16).toString("base64") }), MicrositeKeyError);
    const same = Buffer.alloc(32, 7).toString("base64");
    assert.throws(() => micrositeKey({ HC_MICROSITE_KEY: same, HC_CONNECT_KEY: same }), /differ from HC_CONNECT_KEY/);
    assert.equal(micrositeKey({ HC_MICROSITE_KEY: same })?.length, 32);
  });
});

describe("the microsite's pages", async () => {
  const { output, run, dir } = await runFromRecording({ answers: loadAnswers() });
  const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
  const input: ReportInput = { run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps, scores: scoreDomains(run.modules, output!.findings) };
  const report = buildReport(input);
  const { pages } = buildMicrositePages(report, pseudonyms);

  test("the overview lists every line behind the total, with its range, adding up to it (DECISIONS.md 326)", () => {
    const overview = pages.find((p) => p.slug === "overview")!.html;
    for (const g of report.value.groups.filter((x) => x.kind !== "exposure")) assert.ok(overview.includes(g.label.replace(/'/g, "&#39;")), g.label);
    assert.match(overview, /Total, each counted once/);
    assert.match(overview, /Kept apart from this total/);
  });

  test("references sit apart from their words, charts say what they show, and the interview table stacks on a phone (DECISIONS.md 329)", () => {
    const withInterviews = buildMicrositePages(buildReport({ ...input, interviews: syntheticInterviews() }), pseudonyms).pages;
    const overview = withInterviews.find((p) => p.slug === "overview")!.html;
    assert.match(overview, /\.bcr \.ref \{[^}]*margin-left: 4px/);
    assert.match(overview, /<table class="stack">/);
    assert.match(overview, /<td data-label="Role">/);
    assert.match(overview, /@media \(max-width: 600px\)/);
    const all = pages.map((p) => p.html).join("");
    if (all.includes("doc-cols charts")) assert.match(all, /<h3 class="cap">Monthly active users<\/h3>/);
  });

  test("an overview, every issue, the plan, the comparison when there's one, and the org documented", () => {
    assert.deepEqual(pages.map((p) => p.slug), ["overview", "issues", "plan", "compare", "org"]);
    for (const p of pages) assert.match(p.slug, /^[a-z0-9][a-z0-9-]{0,79}$/);
    const issues = pages.find((p) => p.slug === "issues")!.html;
    for (const f of report.teamFindings.filter((x) => x.status === "issue")) assert.ok(issues.includes(f.id) || issues.includes(f.title.replace(/'/g, "&#39;")), f.title);
  });

  test("nothing runs or loads from elsewhere, and no name token survives", () => {
    for (const p of pages) {
      assert.doesNotMatch(p.html, /<script|<iframe|<link|src=|href=|url\(|@import|on\w+=/i, p.slug);
      assert.doesNotMatch(p.html, /\[\[user:/, p.slug);
    }
  });

  test("every figure on every page traces to the report", () => {
    for (const p of pages) assert.deepEqual(checkNumbers(pageText(p.html), report.dataset).unmatched, [], p.slug);
  });

  test("a page with a figure the report doesn't hold stops the microsite", () => {
    const tampered = { ...report, value: { ...report.value, total: { ...report.value.total, expected: report.value.total.expected + 123_457 } } };
    assert.throws(() => buildMicrositePages(tampered, pseudonyms), /number check/);
  });
});
