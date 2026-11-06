import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { DOMAIN_LABELS } from "../../narrative/select.ts";
import { CPQ_DOMAINS, SALES_CLOUD_DOMAINS, SERVICE_CLOUD_DOMAINS } from "../../schemas/common.ts";
import { OURS, PRACTICE, practiceFor, practiceItems, sourceLabel } from "../index.ts";

const ALLOWED_HOSTS = ["architect.salesforce.com", "trailhead.salesforce.com", "help.salesforce.com"];
const DIGIT = /\d/;

describe("what good looks like", () => {
  test("every area has three to five statements, and nothing else has any", () => {
    const all = [...SALES_CLOUD_DOMAINS, ...SERVICE_CLOUD_DOMAINS, ...CPQ_DOMAINS];
    for (const domain of all) {
      const n = practiceFor(domain).length;
      assert.ok(n >= 3 && n <= 5, `${domain}: ${n}`);
    }
    assert.deepEqual(Object.keys(PRACTICE).sort(), [...all].sort());
    assert.deepEqual(Object.keys(DOMAIN_LABELS).sort(), [...all].sort());
  });

  test("every sourced statement has a public Salesforce URL, the date we read it, and what it says", () => {
    for (const [domain, items] of Object.entries(PRACTICE)) {
      for (const p of items) {
        if (!p.source) continue;
        const where = `${domain}: ${p.text}`;
        const url = new URL(p.source.url);
        assert.equal(url.protocol, "https:", where);
        assert.ok(ALLOWED_HOSTS.includes(url.host), `${where}: ${url.host}`);
        assert.match(p.source.checked, /^\d{4}-\d{2}-\d{2}$/, where);
        assert.ok(!Number.isNaN(new Date(p.source.checked).getTime()), where);
        assert.ok(p.source.title.length > 0 && p.source.says.length > 20, where);
        // A paraphrase, never a quotation.
        assert.doesNotMatch(p.source.says, /["“”]/, where);
      }
    }
  });

  test("most statements have a source, and ours say they're ours", () => {
    const all = Object.values(PRACTICE).flat();
    assert.ok(all.filter((p) => p.source).length > all.length / 2);
    for (const p of all.filter((x) => !x.source)) assert.equal(sourceLabel(p), OURS);
  });

  test("no statement holds a figure, so none can fail the number check", () => {
    for (const p of Object.values(PRACTICE).flat()) {
      assert.doesNotMatch(p.text, DIGIT, p.text);
      if (p.source) assert.doesNotMatch(`${p.source.name} ${p.source.title}`, DIGIT, p.source.title);
    }
  });

  test("statements are short lines in plain words", () => {
    for (const p of Object.values(PRACTICE).flat()) {
      assert.ok(p.text.length <= 120, p.text);
      assert.match(p.text, /^[A-Z].*\.$/, p.text);
    }
  });

  test("the raw notes number every statement of the areas shown, in order", () => {
    const ids = practiceItems(["security", "automation"]);
    assert.equal(ids.length, practiceFor("security").length + practiceFor("automation").length);
    assert.equal(ids[0], "practice:security:1");
  });
});
