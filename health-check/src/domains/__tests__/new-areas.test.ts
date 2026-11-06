import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { knownModuleIds, modulesForRun, SALES_CLOUD_MODULES } from "../index.ts";
import { ESSENTIALS_DOMAINS, modulesFor } from "../scope.ts";
import { AREA_WEIGHTS } from "../scoring.ts";
import { domainLabel, recordAreas } from "../../narrative/select.ts";
import { practiceFor } from "../../practice/index.ts";
import { SALES_CLOUD_DOMAINS } from "../../schemas/index.ts";

// Integrations and technical health (roadmap 3.2, DECISIONS.md 169 to 173).
const AREAS = ["integrations", "technical_health"];

describe("integrations and technical health", () => {
  test("are Sales Cloud areas with their own labels and weights", () => {
    for (const area of AREAS) assert.ok((SALES_CLOUD_DOMAINS as readonly string[]).includes(area), area);
    assert.equal(domainLabel("integrations"), "Integrations");
    assert.equal(domainLabel("technical_health"), "Technical health");
    assert.equal(AREA_WEIGHTS.integrations, 1);
    assert.equal(AREA_WEIGHTS.technical_health, 0.75);
  });

  test("come with the area they grew out of, so an older run's list still runs every check it had", () => {
    assert.deepEqual(modulesForRun(["automation"]).map((m) => m.id), ["automation", "technical_health"]);
    assert.deepEqual(modulesForRun(["security"]).map((m) => m.id), ["security", "integrations"]);
    assert.deepEqual(modulesForRun(["technical_health"]).map((m) => m.id), ["technical_health"]);
    assert.deepEqual(modulesForRun(["integrations"]).map((m) => m.id), ["integrations"]);
    for (const area of AREAS) assert.ok(knownModuleIds().has(area), area);
  });

  test("are set-up areas: in a set-up review whole, and never listed as measured from records", () => {
    for (const area of AREAS) {
      assert.ok(ESSENTIALS_DOMAINS.includes(area), area);
      const full = SALES_CLOUD_MODULES.find((m) => m.id === area)!;
      const essentials = modulesFor("essentials", SALES_CLOUD_MODULES).find((m) => m.id === area)!;
      assert.deepEqual(essentials.rules.map((r) => r.id), full.rules.map((r) => r.id), area);
      assert.ok(!full.extractors.some((e) => e.records), area);
    }
    const review = { scope: "essentials" as const, modules: SALES_CLOUD_MODULES.filter((m) => ESSENTIALS_DOMAINS.includes(m.id)).map((m) => m.id) };
    for (const area of AREAS) assert.ok(!recordAreas(review).includes(area), area);
  });

  test("each has five statements of what good looks like, sourced where Salesforce says it", () => {
    for (const area of AREAS) {
      assert.equal(practiceFor(area).length, 5, area);
      assert.ok(practiceFor(area).slice(0, 3).every((p) => p.source !== null), area);
    }
  });
});
