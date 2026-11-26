import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { analyseExtracts, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import { journeyRecords, richSpecs, withJourney, type LeadSpec } from "../../testing/lead-journey-org.ts";
import { daysBetween, FAST_DAYS } from "../lead-funnel/journey.ts";
import { INSIGHT_RULES, scoreDomains } from "../scoring.ts";
import { DURATION_MEDIANS } from "../average.ts";
import { leadFunnelModule, leadJourneyModule, withLeadJourney } from "../lead-funnel/index.ts";
import { SALES_CLOUD_MODULES } from "../index.ts";
import type { AnalysisOutput } from "../../engine/evaluate.ts";
import type { Finding, Metric } from "../../schemas/index.ts";

// The lead journey (DECISIONS.md 770), on constructed lead sets put through the
// real analysis with the switch on, and the synthetic org with it off.

let extracts: Map<string, unknown>;
before(async () => {
  extracts = loadExtracts((await runFromRecording()).dir);
});

const on = (specs: LeadSpec[], status?: Parameters<typeof journeyRecords>[1], options: Parameters<typeof analyseExtracts>[1] = {}): AnalysisOutput =>
  analyseExtracts(withJourney(extracts, journeyRecords(specs, status)), { ...options, leadJourney: true });
const finding = (a: AnalysisOutput, ruleId: string): Finding[] => [...a.findings, ...a.insights].filter((f) => f.ruleId === ruleId);
const one = (a: AnalysisOutput, ruleId: string): Finding => {
  const f = finding(a, ruleId);
  assert.equal(f.length, 1, `${ruleId}: ${f.length}`);
  return f[0]!;
};
const metric = (a: AnalysisOutput, id: string): Metric => a.metrics.find((m) => m.id === id)!;
const measured = (m: Metric) => {
  assert.equal(m.status, "measured", m.status === "not_measurable" ? m.reason : "");
  return m as Extract<Metric, { status: "measured" }>;
};
const medianOf = (values: number[]): number => {
  const s = [...values].sort((a, b) => a - b);
  return s.length % 2 ? s[(s.length - 1) / 2]! : (s[s.length / 2 - 1]! + s[s.length / 2]!) / 2;
};

describe("the switch", () => {
  test("off, the modules are the engine's own; on, only the lead funnel is swapped, its rules and metrics after the rest", () => {
    assert.deepEqual(withLeadJourney(SALES_CLOUD_MODULES).filter((m, i) => m !== SALES_CLOUD_MODULES[i]), [leadJourneyModule]);
    assert.deepEqual(leadJourneyModule.rules.map((r) => r.id), [...leadFunnelModule.rules.map((r) => r.id), "lead.source-win-rate", "lead.fast-conversion"]);
    assert.ok(INSIGHT_RULES.has("lead.fast-conversion") && !INSIGHT_RULES.has("lead.source-win-rate"));
    assert.ok(DURATION_MEDIANS.has("lead.days_to_won"));
  });

  test("off, the synthetic org's analysis has none of it, and is the analysis of the same data with no switch given", () => {
    const off = analyseExtracts(extracts);
    const given = analyseExtracts(extracts, { leadJourney: false });
    assert.deepEqual(given, off);
    const ids = JSON.stringify([off.findings.map((f) => f.ruleId), off.insights.map((f) => f.ruleId), off.metrics.map((m) => m.id), off.rulesRun]);
    for (const id of ["lead.source-win-rate", "lead.fast-conversion", "lead.days_to_won", "lead.won_lead_rate"]) assert.ok(!ids.includes(id), id);
  });

  test("on, the synthetic org's other findings and metrics are those with it off, but for the two first-response checks' versions", () => {
    const off = analyseExtracts(extracts);
    const a = analyseExtracts(extracts, { leadJourney: true });
    const strip = (f: Finding) => (f.ruleId === "lead.response-time" || f.ruleId === "lead.no-first-response" ? { ...f, ruleVersion: 0 } : f);
    assert.deepEqual(a.findings.filter((f) => f.ruleId !== "lead.source-win-rate").map(strip), off.findings.map(strip));
    assert.deepEqual(a.metrics.filter((m) => !["lead.days_to_won", "lead.won_lead_rate"].includes(m.id)), off.metrics);
    assert.deepEqual(a.rulesRun, [...off.rulesRun, "lead.source-win-rate"].sort());
    // The new check isn't measurable there, so the area's score doesn't move.
    assert.deepEqual(scoreDomains(["lead_funnel"], a.findings), scoreDomains(["lead_funnel"], off.findings));
  });
});

describe("time from a lead to a won deal (L1)", () => {
  test("median first and the average beside it, in whole days from the lead's day to the deal's close, overall and by source", () => {
    const specs = richSpecs();
    const m = measured(metric(on(specs), "lead.days_to_won"));
    const won = specs.filter((s) => s.outcome === "won");
    assert.equal(m.n, won.length);
    assert.equal(m.value, medianOf(won.map((s) => s.closedAfter!)));
    const mean = won.reduce((t, s) => t + s.closedAfter!, 0) / won.length;
    assert.equal(m.stats.mean, Math.round(mean * 10) / 10);
    assert.equal(m.unit, "days");
    const web = m.segments.find((s) => s.key === "Web")!;
    assert.equal(web.value, medianOf(won.filter((s) => s.source === "Web").map((s) => s.closedAfter!)));
  });

  test("day precision: a lead made at 23:30 UTC in a London summer is made the next day, so each journey is a day shorter", () => {
    const summer = richSpecs().map((s) => ({ ...s, made: daysBetween("2025-06-01", "2026-10-31") + (s.made % 60), at: "23:30" }));
    const noon = summer.map((s) => ({ ...s, at: "12:00" }));
    const [late, mid] = [measured(metric(on(summer), "lead.days_to_won")), measured(metric(on(noon), "lead.days_to_won"))];
    assert.equal(late.value, mid.value - 1);
  });

  test("a deal converted from two leads is counted once, from the first", () => {
    const specs: LeadSpec[] = [...richSpecs()];
    const firstWon = specs.findIndex((s) => s.outcome === "won");
    specs.push({ source: "Web", made: specs[firstWon]!.made - 5, convertedAfter: 1, sameDealAs: firstWon });
    const [before, after] = [measured(metric(on(richSpecs()), "lead.days_to_won")), measured(metric(on(specs), "lead.days_to_won"))];
    assert.equal(after.n, before.n);
  });

  test("too few won deals is not measurable, with the count", () => {
    const specs = richSpecs().map((s) => (s.outcome === "won" ? { ...s, outcome: "lost" as const } : s));
    specs.push(...Array.from({ length: 3 }, (_, i): LeadSpec => ({ source: "Web", made: 200 + i, convertedAfter: 3, outcome: "won", closedAfter: 40 })));
    const m = metric(on(specs), "lead.days_to_won");
    assert.equal(m.status, "not_measurable");
    assert.equal((m as { reason: string }).reason, "3 won deals from leads is below the calibrated minimum of 20.");
    assert.equal(m.notMeasurableKind, "too_little_data");
  });
});

describe("leads that became won deals (L2's measure)", () => {
  test("the share of the period's leads converted into a won deal, by source and by the quarter they were made", () => {
    const specs = richSpecs();
    const m = measured(metric(on(specs), "lead.won_lead_rate"));
    const won = specs.filter((s) => s.outcome === "won").length;
    assert.equal(m.n, specs.length);
    assert.equal(m.value, Math.round((won / specs.length) * 1000) / 10);
    assert.equal(m.segments.find((s) => s.key === "Webinar")!.value, 10);
    assert.equal(m.segments.find((s) => s.key === "Web")!.value, 18);
    assert.ok(m.series.length >= 6 && m.series.every((p) => /^\d{4}-(01|04|07|10)-01$/.test(p.date)));
    assert.deepEqual({ converted: m.stats.converted, closed: m.stats.closedDeals, open: m.stats.openDeals }, { converted: 220, closed: 220, open: 0 });
  });

  test("the censoring rule: leads too recent to have had the journey's time are left out and said, never counted as not won", () => {
    const base = measured(metric(on(richSpecs()), "lead.won_lead_rate"));
    const recent = [...richSpecs(), ...Array.from({ length: 100 }, (_, i): LeadSpec => ({ source: "Web", made: 5 + (i % 20) }))];
    const a = on(recent);
    const m = measured(metric(a, "lead.won_lead_rate"));
    assert.equal(m.value, base.value);
    assert.equal(m.n, base.n);
    assert.ok(m.exclusions.some((e) => /^100 leads created after .*, too recent to have had the \d+ days that 9 in 10 of the leads that became closed deals took$/.test(e)), m.exclusions.join(" | "));
    assert.deepEqual(one(a, "lead.source-win-rate").facts, one(on(richSpecs()), "lead.source-win-rate").facts);
  });

  test("the period ends the journey's lag before the run: the 90th percentile of days from lead to closed deal, never under 30", () => {
    const m = measured(metric(on(richSpecs()), "lead.won_lead_rate"));
    // Closes 33 to 80 days after the lead; the 90th percentile of those is 80.
    assert.equal(m.stats.lagDays, 80);
    assert.deepEqual(m.dateRange, { from: "2024-10-31", to: "2026-08-12" });
  });

  test("too few closed deals is not measurable, with the count", () => {
    const specs = richSpecs().map((s, i) => (i % 40 === 0 ? s : { source: s.source, made: s.made }));
    const a = on(specs);
    for (const m of [metric(a, "lead.won_lead_rate"), metric(a, "lead.days_to_won")]) assert.match((m as { reason: string }).reason, /^\d+ closed deals from leads is below the calibrated minimum of 20\.$/);
    assert.equal(one(a, "lead.source-win-rate").status, "not_measurable");
  });

  test("too recent: when no lead made since the window's start has had the journey's time, nothing is counted", () => {
    const specs = Array.from({ length: 30 }, (_, i): LeadSpec => ({ source: "Web", made: 300 - i, convertedAfter: 3, outcome: i % 2 ? "won" : "lost", closedAfter: 200 }));
    const a = on(specs, undefined, { window: { from: "2026-06-01", to: "2026-10-31" } });
    assert.equal((metric(a, "lead.won_lead_rate") as { reason: string }).reason, "Leads here can take 200 days to become closed deals, so none created since 1 June 2026 has had the time yet, and none can be counted.");
  });

  test("too few leads in the period, when slow deals set a long lag", () => {
    const specs: LeadSpec[] = [
      ...Array.from({ length: 22 }, (_, i): LeadSpec => ({ source: "Web", made: 50, convertedAfter: 1, outcome: i % 2 ? "won" : "lost", closedAfter: 5 })),
      ...Array.from({ length: 3 }, (): LeadSpec => ({ source: "Web", made: 650, convertedAfter: 1, outcome: "won", closedAfter: 400 })),
    ];
    assert.equal((metric(on(specs), "lead.won_lead_rate") as { reason: string }).reason, "3 leads made early enough to judge is below the calibrated minimum of 20.");
  });

  test("too many still open is not measurable, saying how many", () => {
    const specs = richSpecs().map((s, i) => (s.outcome && i % 9 === 0 ? { ...s, outcome: "open" as const } : s));
    const a = on(specs);
    assert.match((metric(a, "lead.won_lead_rate") as { reason: string }).reason, /^Of the 220 deals from leads made early enough to judge, \d+ are still open, too many to say how these deals end\.$/);
    assert.equal(one(a, "lead.source-win-rate").status, "not_measurable");
  });
});

describe("sources that convert well and win poorly, or the reverse (L2's check)", () => {
  test("a source converting more often than the rest whose deals are won less often is raised, with both rates and the org's words marked", () => {
    const f = one(on(richSpecs()), "lead.source-win-rate");
    assert.equal(f.status, "issue");
    assert.equal(f.title, "Leads from Webinar convert well, but their deals are won less often");
    assert.equal(
      f.summary,
      "Of the leads created between 31 October 2024 and 12 August 2026, 50% of the 200 from Webinar were converted, against 30% of the 400 from other sources. Their 100 closed deals were won 20% of the time, against 60% of the 120 closed deals from other sources. The gap pointed the same way in 2 of the 2 teams we could check it in. This shows how often they went together, not that one is the reason for the other.",
    );
    assert.deepEqual(f.subjects, [{ type: "lead source", label: "Webinar" }]);
    assert.deepEqual({ s: f.severity, c: f.confidence, d: f.domain }, { s: "low", c: "high", d: "lead_funnel" });
    assert.deepEqual(f.facts, { leads: 200, otherLeads: 400, conversionRate: 50, otherConversionRate: 30, deals: 100, otherDeals: 120, winRate: 20, otherWinRate: 60, open: 0, compared: 2, held: 2 });
  });

  test("the reverse: a source converting less often whose deals are won more often", () => {
    // Webinar converts 1 in 4 and wins 9 in 10.
    const specs = richSpecs().filter((s) => s.source !== "Webinar");
    for (let k = 0; k < 400; k++) specs.push(k % 4 === 0 ? { source: "Webinar", made: 100 + ((k * 7) % 600), owner: (k % 3) + 1, convertedAfter: 3, outcome: (k / 4) % 10 < 9 ? "won" : "lost", closedAfter: 40 } : { source: "Webinar", made: 100 + ((k * 7) % 600), owner: (k % 3) + 1 });
    const f = one(on(specs), "lead.source-win-rate");
    assert.equal(f.title, "Leads from Webinar convert less often, but their deals are won more often");
  });

  test("a source that converts poorly and wins poorly says nothing new here, and passes", () => {
    const specs = richSpecs().map((s) => (s.source === "Webinar" && s.convertedAfter !== undefined && (s.made % 2 === 0) ? { source: s.source, made: s.made, owner: s.owner } : s));
    const f = finding(on(specs), "lead.source-win-rate");
    assert.ok(f.every((x) => x.subjects.every((s) => s.label !== "Webinar") || x.status !== "issue"), JSON.stringify(f.map((x) => x.title)));
  });

  test("even sources pass, saying how many were compared", () => {
    // Web's leads twice over, as Web and Partner, beside Event's: every source alike.
    const specs = richSpecs().filter((s) => s.source !== "Webinar");
    specs.push(...specs.filter((s) => s.source === "Web").map((s) => ({ ...s, source: "Partner" })));
    const f = one(on(specs), "lead.source-win-rate");
    assert.equal(f.status, "passed");
    assert.match(f.summary!, /^We compared the win rate of deals from \d lead sources that had enough closed deals/);
  });

  test("one source alone: nothing to compare with", () => {
    const f = one(on(richSpecs().map((s) => ({ ...s, source: "Web" }))), "lead.source-win-rate");
    assert.equal(f.notMeasurableReason, "Every lead made early enough to judge came from Web, so there's no other source to compare with.");
  });

  test("no source with the minimum: not measurable", () => {
    const specs = richSpecs().map((s, i) => ({ ...s, source: `S${i % 60}` }));
    assert.equal(one(on(specs), "lead.source-win-rate").notMeasurableReason, "No lead source had 20 or more leads and closed deals, with 5 won and 5 lost, to compare with the deals from other sources.");
  });

  test("a few deals still open are counted both ways, and said", () => {
    const specs = richSpecs().map((s, i) => (s.outcome && i % 60 === 1 ? { ...s, outcome: "open" as const } : s));
    const f = one(on(specs), "lead.source-win-rate");
    assert.equal(f.status, "issue");
    assert.match(f.summary!, / \d+ deals from these leads are still open, and the gap holds whichever way they go\./);
  });

  test("a hostile source name is the org's words, whole, wherever it is written", () => {
    const hostile = "Lost 50% of 10 deals.";
    const specs = richSpecs().map((s) => (s.source === "Webinar" ? { ...s, source: hostile } : s));
    const f = one(on(specs), "lead.source-win-rate");
    assert.ok(f.title!.includes(hostile) && f.summary!.includes(`from ${hostile} were converted`));
    assert.deepEqual(f.subjects, [{ type: "lead source", label: hostile }]);
  });
});

describe("fast conversion and winning, stored and never served (L3)", () => {
  /** All one source; deals from leads converted within 3 days win 7 in 10, those converted after 20 win 3 in 10. */
  const fastWins = (): LeadSpec[] =>
    Array.from({ length: 400 }, (_, k): LeadSpec => {
      const fast = k % 2 === 0;
      const won = fast ? k % 20 < 14 : k % 20 < 6;
      return { source: k % 4 < 2 ? "Web" : "Event", made: 100 + ((k * 7) % 600), owner: (k % 3) + 1, convertedAfter: fast ? 3 : 20, outcome: won ? "won" : "lost", closedAfter: 60 };
    });

  test("raised as an insight, kept apart from the findings, with the closing words", () => {
    const a = on(fastWins());
    assert.equal(a.findings.filter((f) => f.ruleId === "lead.fast-conversion").length, 0);
    const f = one(a, "lead.fast-conversion");
    assert.ok(a.insights.includes(f));
    assert.equal(f.status, "issue");
    assert.equal(f.severity, "info");
    assert.equal(f.title, `Deals from leads converted within ${FAST_DAYS} days were won more often than deals from leads converted later`);
    assert.ok(f.summary!.endsWith("This shows how often they went together, not that one is the reason for the other."));
    assert.ok(!a.rulesRun.includes("lead.fast-conversion"));
  });

  test("both sides are converted leads' deals, never a group against survivors: the counts are exactly each side's closed deals", () => {
    const specs = fastWins();
    const f = one(on([...specs, ...Array.from({ length: 300 }, (_, i): LeadSpec => ({ source: "Web", made: 150 + i }))]), "lead.fast-conversion");
    const fast = specs.filter((s) => s.convertedAfter! <= FAST_DAYS);
    const later = specs.filter((s) => s.convertedAfter! > FAST_DAYS);
    assert.deepEqual({ deals: f.facts.deals, otherDeals: f.facts.otherDeals }, { deals: fast.length, otherDeals: later.length });
    assert.equal(f.facts.otherWinRate, Math.round((later.filter((s) => s.outcome === "won").length / later.length) * 1000) / 10);
  });

  test("not scored, and not in a score's findings", () => {
    const a = on(fastWins());
    const [score] = scoreDomains(["lead_funnel"], [...a.findings, ...a.insights]);
    const f = one(a, "lead.fast-conversion");
    assert.ok(score!.status !== "measured" || !score!.findingIds.includes(f.id));
  });

  test("with winning not following conversion speed, it passes, with the smallest gap it could have told", () => {
    const f = one(on(richSpecs()), "lead.fast-conversion");
    assert.equal(f.status, "passed");
    assert.match(f.summary!, /^We couldn't tell whether deals from leads converted within 7 days were won more or less often than deals from leads converted later: a gap in win rate would have had to be about \d+ points or more/);
  });

  test("too few on a side: not measurable", () => {
    const specs = fastWins().map((s) => ({ ...s, convertedAfter: 20 }));
    assert.match(one(on(specs), "lead.fast-conversion").notMeasurableReason!, /^Fewer than 20 closed deals, with 5 won and 5 lost, came from leads converted within 7 days/);
  });

  test("the creation-band check: a gap that is only the mix changing over the period is not raised", () => {
    // Early leads convert fast and win; late leads convert slowly and lose: inside each band speed says nothing.
    const specs = Array.from({ length: 400 }, (_, k): LeadSpec => {
      const early = k % 2 === 0;
      const e = Math.floor(k / 2);
      const made = early ? 500 + (e % 80) : 150 + (e % 80);
      const fast = early ? e % 5 < 4 : e % 5 < 1;
      const won = early ? Math.floor(e / 5) % 10 < 7 : Math.floor(e / 5) % 10 < 2;
      return { source: "Web", made, owner: (k % 3) + 1, convertedAfter: fast ? 3 : 20, outcome: won ? "won" : "lost", closedAfter: 60 };
    });
    assert.notEqual(one(on(specs), "lead.fast-conversion").status, "issue");
  });
});

describe("Lead Status history not kept (audit risk 10)", () => {
  const RESPONSE = ["lead.response-time", "lead.no-first-response"];
  const UNTRACKED = /^We couldn't measure first responses: Salesforce isn't keeping a history of your leads' Status field, as none of the \d+ leads converted in the window has a recorded status change\. A lead answered only by a change of status would look unanswered\. Switch on field history tracking for Lead Status, and later runs will measure from that day\.$/;

  test("no Status change at all while leads converted: the first-response checks and measure say they couldn't measure, and what unlocks it", () => {
    const a = on(richSpecs(), () => []);
    for (const id of RESPONSE) assert.match(one(a, id).notMeasurableReason!, UNTRACKED, id);
    assert.match((metric(a, "lead.median_response_hours") as { reason: string }).reason, UNTRACKED);
  });

  test("with the switch off, the same org reads as before: measured from activity", () => {
    const off = analyseExtracts(withJourney(extracts, journeyRecords(richSpecs(), () => [])));
    for (const id of RESPONSE) assert.ok(!UNTRACKED.test(off.findings.find((f) => f.ruleId === id)!.notMeasurableReason ?? ""), id);
  });

  test("no change and no conversion either: nothing says the history isn't kept, so nothing changes", () => {
    const specs = richSpecs().map((s) => ({ source: s.source, made: s.made, owner: s.owner }));
    const a = on(specs, () => []);
    for (const id of RESPONSE) assert.ok(!UNTRACKED.test(one(a, id).notMeasurableReason ?? ""), id);
  });

  test("a history that starts after a conversion: leads made before it are left out of the first-response checks, and said", () => {
    const keptFrom = (leads: ReturnType<typeof journeyRecords>["leads"]) => leads.filter((l) => l.CreatedDate >= "2026-01-01").map((l, i) => ({ Id: `017${String(950_000 + i).padStart(12, "0")}AAA`, LeadId: l.Id, CreatedDate: l.CreatedDate }));
    const a = on(richSpecs(), keptFrom);
    const f = one(a, "lead.no-first-response");
    assert.ok(f.evidence[0]!.exclusions.some((e) => /^Leads created before \d+ \w+ 2026, when Salesforce's history of lead status changes starts$/.test(e)), f.evidence[0]!.exclusions.join(" | "));
  });
});
