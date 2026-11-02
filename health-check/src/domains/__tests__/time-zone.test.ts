import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { localDay, todayIn, zoneNote, zoneOf } from "../time-zone.ts";
import { stateAt } from "../historic-pipeline/metrics.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import type { Deal } from "../historic-pipeline/cohort.ts";

let extracts: Map<string, unknown>;

before(async () => {
  extracts = loadExtracts((await runFromRecording()).dir);
});

const inZone = (zone: string): Map<string, unknown> => {
  const changed = clone(extracts);
  (changed.get("org") as { data: Array<{ TimeZoneSidKey: string }> }).data[0]!.TimeZoneSidKey = zone;
  return changed;
};
const ctxWith = (status: unknown) => ({ data: () => status, now: new Date("2026-04-01T02:00:00Z") }) as never;
const orgIn = (zone: string) => ctxWith({ status: "ok", data: [{ TimeZoneSidKey: zone }] });

describe("local days", () => {
  test("an instant after midnight UTC is still yesterday in Los Angeles", () => assert.equal(localDay("2026-04-01T02:00:00.000+0000", "America/Los_Angeles"), "2026-03-31"));
  test("and already tomorrow in Tokyo", () => assert.equal(localDay("2026-03-31T16:00:00.000+0000", "Asia/Tokyo"), "2026-04-01"));
  test("London follows summer time", () => {
    assert.equal(localDay("2026-06-30T23:30:00.000Z", "Europe/London"), "2026-07-01");
    assert.equal(localDay("2026-01-31T23:30:00.000Z", "Europe/London"), "2026-01-31");
  });
  test("half-hour and quarter-hour zones work", () => {
    assert.equal(localDay("2026-03-31T18:45:00.000Z", "Asia/Kolkata"), "2026-04-01");
    assert.equal(localDay("2026-03-31T18:14:00.000Z", "Asia/Kathmandu"), "2026-03-31");
    assert.equal(localDay("2026-03-31T18:15:00.000Z", "Asia/Kathmandu"), "2026-04-01");
  });
  test("the cache doesn't blur two sides of midnight", () => {
    assert.equal(localDay("2026-06-30T22:59:59.000Z", "Europe/London"), "2026-06-30");
    assert.equal(localDay("2026-06-30T23:00:00.000Z", "Europe/London"), "2026-07-01");
    assert.equal(localDay("2026-06-30T22:59:00.000Z", "Europe/London"), "2026-06-30");
  });
  test("no zone is the UTC day", () => assert.equal(localDay("2026-04-01T02:00:00.000Z", null), "2026-04-01"));
});

describe("reading the zone", () => {
  test("uses the org's zone, and says so", () => {
    assert.equal(zoneOf(orgIn("Europe/London")), "Europe/London");
    assert.match(zoneNote(orgIn("Europe/London")), /org's time zone, Europe\/London/);
  });
  test("an unreadable org falls back to UTC, and says so", () => {
    const ctx = ctxWith({ status: "unavailable" });
    assert.equal(zoneOf(ctx), null);
    assert.match(zoneNote(ctx), /in UTC, because/);
  });
  test("a zone name we can't apply falls back to UTC rather than crashing", () => {
    assert.equal(zoneOf(orgIn("Not/AZone")), null);
    assert.match(zoneNote(orgIn("Not/AZone")), /in UTC/);
  });
  test("today is the org's today", () => assert.equal(todayIn(orgIn("America/Los_Angeles")), "2026-03-31"));
});

describe("dates that have no zone", () => {
  test("a deal closed on CloseDate alone keeps that day, whatever the zone", () => {
    const changed = inZone("Pacific/Pago_Pago");
    const extract = changed.get("opportunities") as { opportunities: { data: any[] }; history: { data: any[] } };
    const deal = extract.opportunities.data.find((o) => o.IsWon)!;
    extract.history.data = extract.history.data.filter((h) => h.OpportunityId !== deal.Id);
    deal.CloseDate = "2026-07-01";
    const { findings, metrics } = analyseExtracts(changed);
    assert.ok(findings.length > 0);
    const winRate = metrics.find((m) => m.id === "historic.win_rate")!;
    assert.ok(winRate.status === "measured");
    assert.ok(winRate.segments.some((s) => s.dimension === "closed quarter" && s.key === "FY2026-Q3" && s.n > 0));
  });
});

describe("in the metrics", () => {
  const closingAt = (zone: string, at: string) => {
    const changed = inZone(zone);
    const extract = changed.get("opportunities") as { opportunities: { data: any[] }; history: { data: any[] } };
    for (const row of extract.history.data) if (row.StageName.startsWith("Closed")) row.CreatedDate = at;
    return analyseExtracts(changed).metrics.find((m) => m.id === "historic.win_rate")!;
  };
  const dealsIn = (metric: ReturnType<typeof closingAt>, quarter: string): number => {
    assert.ok(metric.status === "measured");
    return metric.segments.find((s) => s.dimension === "closed quarter" && s.key === quarter)?.n ?? 0;
  };

  // Deals with open history after this date fall back to CloseDate, so only the
  // deals that moved between the two zones are compared.
  test("a close at 02:00 UTC on 1 April falls in March in Los Angeles, and April in London", () => {
    const moment = "2026-04-01T02:00:00.000+0000";
    const pacific = closingAt("America/Los_Angeles", moment);
    const london = closingAt("Europe/London", moment);
    assert.ok(dealsIn(pacific, "FY2026-Q1") > 100);
    assert.equal(dealsIn(london, "FY2026-Q1"), 0);
    assert.equal(dealsIn(london, "FY2026-Q2") - dealsIn(pacific, "FY2026-Q2"), dealsIn(pacific, "FY2026-Q1"));
  });

  test("every metric states its calendar", () => {
    for (const m of analyseExtracts(inZone("Asia/Tokyo")).metrics) {
      if (m.status === "measured") assert.ok(m.assumptions.some((a) => a.includes("Asia/Tokyo")) || m.segmentExemptReason !== undefined, m.id);
    }
    const winRate = analyseExtracts(inZone("Asia/Tokyo")).metrics.find((m) => m.id === "historic.win_rate")!;
    assert.ok(winRate.assumptions.some((a) => a.includes("Asia/Tokyo")));
  });

  test("a month end's snapshot uses the org's day", () => {
    const row = { Id: "h", OpportunityId: "o", StageName: "A", Amount: 1, CloseDate: "2026-01-01", CreatedDate: "2026-04-01T02:00:00.000+0000" };
    const deal = { history: [row] } as unknown as Deal;
    assert.equal(stateAt(deal, "2026-03-31", null), null);
    assert.equal(stateAt(deal, "2026-03-31", "America/Los_Angeles")?.stage, "A");
  });

  test("the activity months use the org's day", () => {
    const changed = inZone("Asia/Tokyo");
    const { events } = changed.get("activity") as { events: { data: any[] } };
    for (const e of events.data) if (e.StartDateTime.startsWith("2026-06")) e.StartDateTime = "2026-06-30T20:00:00.000+0000";
    const metric = analyseExtracts(changed).metrics.find((m) => m.id === "activity.event_linked_share")!;
    assert.ok(metric.status === "measured");
    assert.equal(metric.series.find((p) => p.date === "2026-06-30")?.n, 0);
    assert.ok((metric.series.find((p) => p.date === "2026-07-31")?.n ?? 0) > 60);
  });

  test("a different zone doesn't crash any rule", () => {
    for (const zone of ["Asia/Kolkata", "America/St_Johns", "Pacific/Auckland"]) {
      const { findings } = analyseExtracts(inZone(zone));
      assert.deepEqual(findings.filter((f) => f.notMeasurableReason?.startsWith("This check failed")).map((f) => f.ruleId), []);
    }
  });
});
