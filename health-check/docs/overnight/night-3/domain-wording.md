# Night 3: domain wording (Brief BF)

Worker A, 7 November 2026. Four items from B's reading-pass list (docs/overnight/night-3/reading-pass-2.md), as the coordinator decided them. The branch is `hc/domain-wording`, from health-check `9abf69e`, and nothing is pushed. It uses decisions 387 to 390 and 411 to 414.

## Done

| Item | What changed | Commit | Decision |
|---|---|---|---|
| 21 | A report counts as unused over the 90 days its metric defines, not 12 months | `49d3d36` | 387 |
| 21 | Stuck-in-stage says how many open deals it couldn't compare, and why ("6 more open deals couldn't be compared, as they're in stages with too few won deals to set a line") | `308f72e` | 388 |
| 16 | The three quiet open-deal issues are titled by their own test: "Open deals with no activity in 90 days", "… no stage change or activity in 30 days", "… in 60 days" | `31d7c42` | 389 |
| 13 | Findings name objects and fields by their labels: a new labels read, and nine rules | `b1e4a67` | 411 |
| 13 | The documented org's pages and the object inventory use labels, record types included | `8f8d6b2` | 412 |
| 13 | Installed apps are named by their package's name, not their namespace | `8776229` | 413 |
| 13 | CPQ's checks name fields by label: the price rule's field and the twin field | `06b8395` | 414 |

Each change has a test, and the test failed before its fix.
- The fingerprints moved with each commit, by the texts it changed and the rule versions. No count or figure moved, except the evidence periods in 387 and the metrics' label segments and record type keys in 412.
- Every sample renders and passes its layout and number checks.
- A scan of the full report, executive summary, one-pager and microsite, on the plain, CPQ and all-clouds samples, finds no API name.

**Item 16 differs from the suggestion.** The third title is "Open deals with no stage change or activity in 60 days", not "in one stage 60 days or more". `rev.stalled-deals` needs no activity in those 60 days as well, and a title that left that out wouldn't be its test.

**For B: a cliff.**
- The CPQ sample's full report sat at exactly its ceiling of 130.
- With the longer titles it's 131 at the first two-column step. The ladder then moves on to the next step, which gives 115 slides of denser cards.
- It still passes every check. But one slide over costs fifteen, which is item 23 of the reading pass.
- The slide that tips it is the revenue opportunities chapter, which holds `rev.stalled-deals`.

## Item 9: found, fix ready, held (landed later in Brief BH, on hc/synthetic-cycle)

Jul 2026's median cycle of 355 days, against about 70 elsewhere, is neither a pattern nor an engine bug. It's an artefact of how the synthetic org is built.
- The services block gives every deal a creation date over a year ago, so the activity checks, which read the last year, wouldn't move.
- So its wins closing in 2026 have cycles of 150 to 450 days. Before then they run 60 to 110, and the original deals run 70 to 117 throughout. The engine measures them correctly.
- No real org can look like this, so the chart shouldn't get a note. The generator should be fixed, as slide 86's activity was (decision 352).

The fix is below.
- The services block's deals take their usual one to four months, and about one in eight of them five to ten.
- Its deals created inside the year get their work logged, as the original deals' are.
- Every 2026 month's median comes out between 77 and 109 days.

It's held, not committed, for two reasons:
- It moves the synthetic data under about 25 tests. Most are figure pins whose conclusions hold: going back a stage still wins 0% of the time, and no logged activity is still a 43-point gap. But they include the one-pager's comparison and the win drivers' slide, which B is changing tonight.
- It exposes a render bug, which is B's. With the new data, "Open deals have no active owner" carries four weighting lines in its money working, and as the only card on its slide it overflows the page (page 45 of the full report). A real org with four stages affected would do the same.

So I recommend it lands after B's reading pass 4, with the overflow fixed first. Then:
1. Apply the patch.
2. Run `npm run fixtures`.
3. Update each pin whose conclusion holds, and the fingerprints, saying why.
Decision 390 records this.

```diff
diff --git a/health-check/src/testing/synthetic-org.ts b/health-check/src/testing/synthetic-org.ts
index d369477..28542fc 100644
--- a/health-check/src/testing/synthetic-org.ts
+++ b/health-check/src/testing/synthetic-org.ts
@@ -280,9 +280,10 @@ type PriorWin = { Id: string; AccountId: string; CloseDate: string; IsWon: boole
 /**
  * A services side to the business: account managers selling to retained
  * clients, mostly by word of mouth, with no lead source and a lot of repeat work.
- * It's added after the original deals so their seeded data doesn't move, and
- * every deal here was created over a year ago, so the activity checks, which
- * only read the last year, see exactly what they did before.
+ * It's added after the original deals so their seeded data doesn't move. A deal
+ * is created at its usual lead time before it closes, so its cycle is like the
+ * rest of the org's, and one created inside the last year has its work logged
+ * like the original deals' (buildActivity).
  */
 function buildServices(firstId: number): { opportunities: SyntheticOpportunity[]; history: SyntheticHistory[]; priorWins: PriorWin[] } {
   const random = seeded(SERVICES_SEED);
@@ -294,7 +295,6 @@ function buildServices(firstId: number): { opportunities: SyntheticOpportunity[]
     history.push({ Id: sfId("008", 500_000 + history.length), OpportunityId: oppId, StageName: stage, Amount: amount, CloseDate: close, CreatedDate: stampOf(at) });
   };
   const earliest = asOf - 725 * DAY;
-  const latest = asOf - 366 * DAY;
 
   /** One closed deal along `path`, with its time split across the stages. */
   const closedDeal = (path: string[], final: string, created: number, closed: number): void => {
@@ -349,9 +349,12 @@ function buildServices(firstId: number): { opportunities: SyntheticOpportunity[]
     const outcomes = [...Array<string>(won).fill("Closed Won"), ...Array<string>(lost).fill("lost"), ...Array<string>(noDecision).fill("Closed No Decision")];
     for (const outcome of outcomes) {
       const closed = dayMs(from) + between(0, Math.round((dayMs(to) - dayMs(from)) / DAY)) * DAY;
-      const usual = closed - between(40, 120) * DAY;
-      // A deal closing within the year was created before it, at some point over the months before.
-      const created = Math.max(earliest, usual <= latest ? usual : latest - between(0, 300) * DAY);
+      // Most deals take one to four months; a few retained clients' take five to ten,
+      // and sit in one stage for much of it. Every deal was once pushed back to over a
+      // year old instead, which gave the wins of 2026 cycles of a year or more (night 3
+      // reading pass, item 9).
+      const usual = closed - (random() < 0.12 ? between(150, 300) : between(40, 120)) * DAY;
+      const created = Math.max(earliest, usual);
       const final = outcome === "lost" ? (random() < 0.4 ? "Closed Lost - No Budget" : "Closed Lost") : outcome;
       const depth = final === "Closed Won" ? OPEN_PATH.length : between(1, OPEN_PATH.length);
       closedDeal(OPEN_PATH.slice(0, depth), final, created, closed);
@@ -486,6 +489,13 @@ function buildActivity(): { tasks: SyntheticTask[]; events: SyntheticEvent[]; em
     const logged = i % 2 === 0 ? addTask(deal.OwnerId, deal.Id, at, "Email") : null;
     emails.push({ Id: sfId("02s", i + 1), RelatedToId: deal.Id, ActivityId: logged, MessageDate: stamp(at), CreatedById: deal.OwnerId });
   }
+
+  // The services side's closed deals created inside the year, touched as the
+  // original deals are. Drawn last, so nothing above moves.
+  for (const deal of services.opportunities.filter((o) => o.IsClosed && new Date(o.CreatedDate.replace("+0000", "Z")).getTime() >= oneYearAgo)) {
+    const count = deal.IsWon ? between(5, 9) : between(1, 4);
+    for (let i = 0; i < count; i++) addTask(deal.OwnerId, deal.Id, new Date(deal.CreatedDate.replace("+0000", "Z")).getTime() + (i + 1) * DAY);
+  }
   return { tasks, events, emails };
 }
 
```
