import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { activityExtractor, type ActivityExtract } from "../../extract/activity.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import { userId } from "../../testing/synthetic-org.ts";
import type { Finding } from "../../schemas/index.ts";

let extracts: Map<string, unknown>;
let runDir: string;
const tokenOf = (n: number): string => `[[user:U${String(n).padStart(4, "0")}]]`;

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
type Editable = { tasks: { data: any[]; truncated: boolean }; events: { data: any[]; truncated: boolean }; emails: { data: any[] }; from: string };
const activity = (edit: (a: Editable) => void): Map<string, unknown> => {
  const changed = clone(extracts);
  edit(changed.get(activityExtractor.id) as Editable);
  return changed;
};
const monthOf = (e: { StartDateTime: string }): string => e.StartDateTime.slice(0, 7);

describe("unlinked events: the collapse a total hides", () => {
  test("the planted total really is flat, so a total-only check would pass", () => {
    const { events } = extracts.get(activityExtractor.id) as Editable;
    const perMonth = (from: string, to: string): number => {
      const rows = events.data.filter((e) => monthOf(e) >= from && monthOf(e) <= to);
      return rows.length / new Set(rows.map(monthOf)).size;
    };
    assert.equal(perMonth("2026-01", "2026-06"), perMonth("2026-07", "2026-09"));
  });

  test("the collapse is found under that flat total", () => {
    const f = run(extracts, "act.event-link-collapse");
    assert.equal(f.status, "issue");
    assert.equal(f.severity, "high");
    assert.ok(f.facts.priorShare! > 60);
    assert.ok(f.facts.recentShare! < 15);
    assert.equal(f.facts.priorEvents! / 6, f.facts.recentEvents! / 3, "volume is unchanged per month");
    assert.match(f.summary, /Total events held steady/);
  });

  test("the fall shows month by month", () => {
    const f = run(extracts, "act.event-link-collapse");
    assert.ok(f.facts["linkedShare 2026-06"]! > 60);
    assert.ok(f.facts["linkedShare 2026-07"]! < 15);
  });

  test("it links to the manual step on activity capture", () => {
    assert.deepEqual(run(extracts, "act.event-link-collapse").manualStepIds, ["ms-eac-advanced-sync"]);
  });

  test("a steady linked share stays quiet", () => {
    const changed = activity((a) => {
      a.events.data.forEach((e, i) => (e.WhatId = i % 4 === 0 ? null : "006000000000001AAA"));
    });
    assert.equal(run(changed, "act.event-link-collapse").status, "passed");
  });

  test("one team's collapse is found when the overall share looks fine", () => {
    const changed = activity((a) => {
      for (const e of a.events.data) {
        const recent = monthOf(e) >= "2026-07";
        e.WhatId = e.OwnerId === userId(3) && recent ? null : "006000000000001AAA";
      }
    });
    const f = run(changed, "act.event-link-collapse");
    assert.equal(f.status, "issue");
    assert.match(f.summary, /looks fine/);
    assert.ok(f.subjects.some((s) => s.label === "Sales EU"));
  });

  test("a fall in volume is called what it is", () => {
    const changed = activity((a) => {
      a.events.data = a.events.data.filter((e) => monthOf(e) < "2026-07" || e.Id.endsWith("0AAA"));
    });
    assert.match(run(changed, "act.event-link-collapse").summary, /Total events fell as well/);
  });

  test("too little history is not measurable", () => {
    const changed = activity((a) => (a.from = "2026-06-15"));
    assert.equal(run(changed, "act.event-link-collapse").status, "not_measurable");
  });

  test("the current part month isn't compared", () => {
    const changed = activity((a) => {
      for (const e of a.events.data) if (monthOf(e) === "2026-10") e.WhatId = null;
    });
    assert.equal(run(changed, "act.event-link-collapse").facts.recentEvents, run(extracts, "act.event-link-collapse").facts.recentEvents);
  });

  test("a truncated read is not measurable, not a smaller sample", () => {
    const changed = activity((a) => (a.events.truncated = true));
    assert.equal(run(changed, "act.event-link-collapse").status, "not_measurable");
  });

  test("unreadable events are not measurable", () => {
    const changed = activity((a) => ((a as unknown as { events: unknown }).events = gone));
    assert.equal(run(changed, "act.event-link-collapse").status, "not_measurable");
  });
});

describe("deals, won against lost", () => {
  test("a team closing deals with nothing logged is found", () => {
    const f = run(extracts, "act.deals-without-activity");
    assert.equal(f.status, "issue");
    assert.ok(f.subjects.some((s) => s.label === "Sales EU"));
  });

  test("activity on every deal stays quiet", () => {
    const changed = activity((a) => {
      const deals = (extracts.get("opportunities") as { opportunities: { data: Array<{ Id: string; OwnerId: string }> } }).opportunities.data;
      for (const d of deals) a.tasks.data.push({ Id: `00T${d.Id}`, OwnerId: d.OwnerId, WhoId: null, WhatId: d.Id, CreatedDate: "2026-10-01T00:00:00.000+0000", TaskSubtype: "Task" });
    });
    assert.equal(run(changed, "act.deals-without-activity").status, "passed");
  });

  test("a deal older than the readable year isn't called empty", () => {
    const f = run(extracts, "act.deals-without-activity");
    const closed = (extracts.get("opportunities") as { opportunities: { data: Array<{ IsClosed: boolean }> } }).opportunities.data.filter((d) => d.IsClosed).length;
    assert.ok(f.facts.n! < closed);
  });

  test("won deals with more activity than lost stay quiet", () => {
    const f = run(extracts, "act.won-vs-lost-activity");
    assert.equal(f.status, "passed");
    assert.ok(f.facts.wonMedian! > f.facts.lostMedian!);
  });

  test("lost deals with as much activity as won is an issue", () => {
    const changed = activity((a) => {
      const lost = (extracts.get("opportunities") as { opportunities: { data: Array<{ Id: string; OwnerId: string; StageName: string }> } }).opportunities.data.filter((d) => d.StageName === "Closed Lost");
      for (const d of lost) for (let i = 0; i < 20; i++) a.tasks.data.push({ Id: `00T${d.Id}${i}`, OwnerId: d.OwnerId, WhoId: null, WhatId: d.Id, CreatedDate: "2026-10-01T00:00:00.000+0000", TaskSubtype: "Task" });
    });
    const f = run(changed, "act.won-vs-lost-activity");
    assert.equal(f.status, "issue");
    assert.equal(f.recommendationType, "investigate");
  });

  test("an email that's also a task counts once", () => {
    const changed = activity((a) => {
      a.tasks.data = a.tasks.data.filter((t) => t.TaskSubtype === "Email");
      a.events.data = [];
    });
    const { tasks, emails } = changed.get(activityExtractor.id) as Editable;
    const distinct = new Set(emails.data.map((e) => e.ActivityId)).size - 1;
    assert.equal(tasks.data.length, distinct, "half the emails have a task");
    const metric = analyseExtracts(changed).metrics.find((m) => m.id === "activity.per_won_deal");
    assert.ok(metric);
  });

  test("won and lost medians and the linked share are published as metrics", () => {
    const { metrics } = analyseExtracts(extracts);
    for (const id of ["activity.per_won_deal", "activity.per_lost_deal", "activity.event_linked_share"]) assert.equal(metrics.find((m) => m.id === id)?.status, "measured", id);
  });

  test("the linked share has a monthly series that shows the fall", () => {
    const metric = analyseExtracts(extracts).metrics.find((m) => m.id === "activity.event_linked_share")!;
    assert.ok(metric.status === "measured");
    const at = (date: string): number | null | undefined => metric.series.find((p) => p.date === date)?.value;
    assert.ok(at("2026-06-30")! > 60);
    assert.ok(at("2026-07-31")! < 15);
  });
});

describe("reps", () => {
  test("a rep logging almost nothing is found", () => {
    const f = run(extracts, "act.rep-activity-spread");
    assert.equal(f.status, "issue");
    assert.deepEqual(f.subjects.map((s) => s.label), [tokenOf(14)]);
    assert.match(f.summary, /logging gap/);
  });

  test("with activity capture in use the finding can't be high confidence", () => {
    assert.equal(run(extracts, "act.rep-activity-spread").confidence, "medium");
  });

  test("with no activity capture it can", () => {
    const changed = clone(extracts);
    (changed.get("activity-capture") as { data: unknown[] }).data = [];
    assert.equal(run(changed, "act.rep-activity-spread").confidence, "high");
  });

  test("not being able to tell about capture counts as in use", () => {
    const changed = clone(extracts);
    changed.set("activity-capture", gone);
    assert.equal(run(changed, "act.rep-activity-spread").confidence, "medium");
  });

  test("even logging stays quiet", () => {
    const changed = activity((a) => {
      for (let i = 0; i < 80; i++) a.tasks.data.push({ Id: `00T14${i}`, OwnerId: userId(14), WhoId: null, WhatId: null, CreatedDate: "2026-10-20T00:00:00.000+0000", TaskSubtype: "Task" });
    });
    assert.equal(run(changed, "act.rep-activity-spread").status, "passed");
  });
});

describe("unlinked tasks", () => {
  test("one person logging against nothing is found under a fine total", () => {
    const f = run(extracts, "act.unlinked-tasks");
    assert.equal(f.status, "issue");
    assert.ok(f.facts.rate! <= 30);
    assert.ok(f.subjects.some((s) => s.label === tokenOf(15)));
  });

  test("a task on a person counts as linked", () => {
    const changed = activity((a) => {
      for (const t of a.tasks.data) t.WhoId = "003000000000001AAA";
    });
    assert.equal(run(changed, "act.unlinked-tasks").status, "passed");
  });

  test("no tasks at all is not measurable, not a clean record", () => {
    const changed = activity((a) => (a.tasks.data = []));
    assert.equal(run(changed, "act.unlinked-tasks").status, "not_measurable");
  });
});

describe("what's read", () => {
  test("no activity text is fetched, and archived rows aren't reached for", () => {
    const { tasks, events, emails } = extracts.get(activityExtractor.id) as unknown as ActivityExtract;
    for (const source of [tasks.source, events.source, emails.source]) {
      assert.ok(source.kind === "soql");
      assert.doesNotMatch(source.query, /Subject|Description|TextBody|HtmlBody|FromAddress|ToAddress|ALL ROWS/i);
    }
  });

  test("checkpoints hold ids and dates only", () => {
    const text = readFileSync(join(runDir, "checkpoints", readdirSync(join(runDir, "checkpoints")).find((f) => f === "activity.json")!), "utf8");
    assert.doesNotMatch(text, /Subject|Description/);
  });

  test("unreadable email is declared, and the rest still runs", () => {
    const changed = activity((a) => ((a as unknown as { emails: unknown }).emails = gone));
    const f = run(changed, "act.deals-without-activity");
    assert.equal(f.status, "issue");
    assert.ok(f.evidence[0]!.exclusions.some((e) => e.includes("EmailMessage")));
  });
});
