import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { leadsExtractor } from "../../extract/leads.ts";
import { activityExtractor } from "../../extract/activity.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import { loadCalibration } from "../../testing/replay.ts";
import type { Finding } from "../../schemas/index.ts";

let extracts: Map<string, unknown>;
let runDir: string;

before(async () => {
  runDir = (await runFromRecording()).dir;
  extracts = loadExtracts(runDir);
});

const one = (findings: Finding[], ruleId: string): Finding => {
  const found = findings.find((f) => f.ruleId === ruleId);
  assert.ok(found, ruleId);
  return found;
};
const run = (changed: Map<string, unknown>, ruleId: string): Finding => one(analyseExtracts(changed).findings, ruleId);
const gone = { status: "unavailable", reason: "INSUFFICIENT_ACCESS: no", source: { kind: "soql", api: "rest", query: "x" } };
type Editable = { leads: { data: any[]; truncated: boolean }; statusChanges: { data: any[] }; campaigns: { data: any[] }; members: { data: any[]; truncated: boolean } };
const leads = (edit: (l: Editable) => void): Map<string, unknown> => {
  const changed = clone(extracts);
  edit(changed.get(leadsExtractor.id) as Editable);
  return changed;
};

describe("conversion", () => {
  test("a source converting at under half the rate is found", () => {
    const f = run(extracts, "lead.conversion-by-source");
    assert.equal(f.status, "issue");
    assert.deepEqual(f.subjects.map((s) => s.label), ["Partner"]);
    assert.ok(f.facts.worstRate! < f.facts.overall! / 2);
  });

  test("even conversion stays quiet", () => {
    const changed = leads((l) => l.leads.data.forEach((lead, i) => (lead.IsConverted = i % 5 === 0)));
    assert.equal(run(changed, "lead.conversion-by-source").status, "passed");
  });

  test("leads too new to have converted aren't counted against the rate", () => {
    const changed = leads((l) => {
      for (const lead of l.leads.data) lead.CreatedDate = "2026-10-25T00:00:00.000+0000";
    });
    assert.equal(run(changed, "lead.conversion-by-source").status, "not_measurable");
  });

  test("no leads is not measurable, not a perfect funnel", () => {
    const changed = leads((l) => (l.leads.data = []));
    const f = run(changed, "lead.conversion-by-source");
    assert.equal(f.status, "not_measurable");
    assert.match(f.notMeasurableReason!, /no funnel/);
  });

  test("a truncated read is not measurable", () => {
    assert.equal(run(leads((l) => (l.leads.truncated = true)), "lead.conversion-by-source").status, "not_measurable");
  });

  test("unreadable leads are not measurable", () => {
    const changed = clone(extracts);
    (changed.get(leadsExtractor.id) as { leads: unknown }).leads = gone;
    assert.equal(run(changed, "lead.conversion-by-source").status, "not_measurable");
  });
});

describe("first response", () => {
  test("a slow source is found under a fast median", () => {
    const f = run(extracts, "lead.response-time");
    assert.equal(f.status, "issue");
    assert.ok(f.facts.medianHours! <= 24, `${f.facts.medianHours}`);
    assert.deepEqual(f.subjects.map((s) => s.label), ["Partner"]);
    assert.equal(f.confidence, "medium");
  });

  test("a converted lead's task, now on the contact, still counts as its response", () => {
    const { tasks } = extracts.get(activityExtractor.id) as { tasks: { data: Array<{ WhoId: string | null }> } };
    assert.ok(tasks.data.some((t) => t.WhoId?.startsWith("003")));
    const blind = clone(extracts);
    const edited = blind.get(activityExtractor.id) as { tasks: { data: Array<{ WhoId: string | null }> } };
    edited.tasks.data = edited.tasks.data.filter((t) => !t.WhoId?.startsWith("003"));
    assert.ok(run(extracts, "lead.no-first-response").facts.hits! < run(blind, "lead.no-first-response").facts.hits!);
  });

  test("a status change counts as a response when no task was logged", () => {
    const blind = leads((l) => (l.statusChanges.data = []));
    assert.ok(run(extracts, "lead.no-first-response").facts.hits! < run(blind, "lead.no-first-response").facts.hits!);
  });

  test("fast responses everywhere stay quiet", () => {
    const changed = clone(extracts);
    const edited = changed.get(leadsExtractor.id) as Editable;
    const { tasks } = changed.get(activityExtractor.id) as { tasks: { data: any[] } };
    const created = new Map(edited.leads.data.flatMap((l) => [[l.Id, l.CreatedDate], [l.ConvertedContactId, l.CreatedDate]]));
    for (const t of tasks.data) if (created.has(t.WhoId)) t.CreatedDate = String(created.get(t.WhoId)).replace(":00:00.000", ":30:00.000");
    assert.equal(run(changed, "lead.response-time").status, "passed");
  });

  test("activity older than a lead's creation isn't a response", () => {
    const changed = clone(extracts);
    const { tasks } = changed.get(activityExtractor.id) as { tasks: { data: any[] } };
    for (const t of tasks.data) if (t.WhoId) t.CreatedDate = "2020-01-01T00:00:00.000+0000";
    assert.ok(run(changed, "lead.no-first-response").facts.hits! > run(extracts, "lead.no-first-response").facts.hits!);
  });

  test("unreadable activity is not measurable, not an unanswered funnel", () => {
    const changed = clone(extracts);
    (changed.get(activityExtractor.id) as { tasks: unknown }).tasks = gone;
    assert.equal(run(changed, "lead.response-time").status, "not_measurable");
    assert.equal(run(changed, "lead.no-first-response").status, "not_measurable");
  });

  test("a lead older than the readable year isn't called unanswered", () => {
    const { leads: all } = extracts.get(leadsExtractor.id) as Editable;
    assert.ok(run(extracts, "lead.no-first-response").facts.n! < all.data.length);
  });

  test("a source that's going unanswered is found under a fine total", () => {
    const f = run(extracts, "lead.no-first-response");
    assert.equal(f.status, "issue");
    assert.ok(f.facts.rate! <= 15, `${f.facts.rate}`);
    assert.ok(f.subjects.some((s) => s.label === "Event"));
  });
});

describe("converted with no opportunity", () => {
  test("a source converting without deals is found", () => {
    const f = run(extracts, "lead.converted-no-opportunity");
    assert.equal(f.status, "issue");
    assert.ok(f.subjects.some((s) => s.label === "Referral"));
  });

  test("every conversion with a deal stays quiet", () => {
    const changed = leads((l) => {
      for (const lead of l.leads.data) if (lead.IsConverted) lead.ConvertedOpportunityId = "006000000000001AAA";
    });
    assert.equal(run(changed, "lead.converted-no-opportunity").status, "passed");
  });
});

describe("source return", () => {
  test("a source with leads and no wins is found", () => {
    const f = run(extracts, "lead.source-return");
    assert.equal(f.status, "issue");
    assert.deepEqual(f.subjects.map((s) => s.label), ["Partner"]);
    assert.equal(f.value.kind, "not_sized");
  });

  test("a win for every source stays quiet", () => {
    const won = (extracts.get("opportunities") as { opportunities: { data: Array<{ Id: string; IsWon: boolean }> } }).opportunities.data.find((o) => o.IsWon)!.Id;
    const changed = leads((l) => {
      for (const lead of l.leads.data) lead.ConvertedOpportunityId = won;
    });
    assert.equal(run(changed, "lead.source-return").status, "passed");
  });

  test("multi-currency is not measurable", () => {
    const changed = clone(extracts);
    (changed.get("opportunities") as { multiCurrency: boolean }).multiCurrency = true;
    assert.equal(run(changed, "lead.source-return").status, "not_measurable");
    assert.equal(run(changed, "lead.campaign-return").status, "not_measurable");
  });
});

describe("campaign return", () => {
  test("spend above return is found, sized, and carries its assumptions", () => {
    const f = run(extracts, "lead.campaign-return");
    assert.equal(f.status, "issue");
    assert.deepEqual(f.subjects.map((s) => s.label), ["Trade Show"]);
    assert.ok(f.value.kind === "revenue" && f.value.amount === f.facts.gap);
    assert.ok(f.value.kind === "revenue" && f.value.assumptions.length >= 3);
    assert.equal(f.confidence, "medium");
  });

  test("a campaign with no cost is left out and declared, never given one", () => {
    const f = run(extracts, "lead.campaign-return");
    assert.ok(f.evidence[0]!.exclusions.some((e) => e.includes("no actual cost")));
    assert.equal(f.segmentsChecked[0]!.segments, 2);
  });

  test("no costs at all is not measurable", () => {
    const changed = leads((l) => {
      for (const c of l.campaigns.data) c.ActualCost = null;
    });
    assert.equal(run(changed, "lead.campaign-return").status, "not_measurable");
  });

  test("a return above cost stays quiet", () => {
    const changed = leads((l) => {
      for (const c of l.campaigns.data) if (c.ActualCost !== null) c.ActualCost = 1;
    });
    assert.equal(run(changed, "lead.campaign-return").status, "passed");
  });
});

describe("what's read", () => {
  test("no name, email, phone or company is selected", () => {
    const { leads: rows, statusChanges, campaigns, members } = extracts.get(leadsExtractor.id) as Record<string, { source: { query: string } }>;
    for (const row of [rows!, statusChanges!, campaigns!, members!]) assert.doesNotMatch(row.source.query, /\b(Name|Email|Phone|Company|OldValue|NewValue|Title)\b/);
  });

  test("funnel metrics are measured and segmented", () => {
    const { metrics } = analyseExtracts(extracts);
    for (const id of ["lead.conversion_rate", "lead.median_response_hours", "lead.won_per_lead"]) {
      const metric = metrics.find((m) => m.id === id);
      assert.ok(metric?.status === "measured" && metric.segments.length > 0, id);
    }
  });

  test("a thin source has no rate of its own", () => {
    const calibration = loadCalibration();
    calibration.minSample = 70;
    const metric = analyseExtracts(extracts, { calibration }).metrics.find((m) => m.id === "lead.conversion_rate")!;
    assert.ok(metric.status === "measured");
    const partner = metric.segments.find((s) => s.dimension === "source" && s.key === "Partner")!;
    assert.equal(partner.value, null);
    assert.ok(partner.notMeasurableReason);
  });

  test("the raw notes stay readable with the new metrics", () => {
    assert.match(readFileSync(join(runDir, "raw-notes.md"), "utf8"), /Lead conversion/);
  });
});
