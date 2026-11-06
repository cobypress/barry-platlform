# Night 3: the scale test (Brief AQ, part 2)

Worker A, 7 November 2026. Engine branch `hc/scale`, from health-check `2a46ad6`. Decisions 331 to 340.

## What was run

- **The org.** A generated 500-user org (`src/testing/scale-org.ts`, `FIVE_HUNDRED_USERS`). It's seeded, so every run makes the same one, and it's made in about 2 seconds. Nothing is stored. The record counts:

  | Record | Rows |
  |---|---:|
  | Opportunities | 200,000 |
  | Opportunity stage changes | 499,923 |
  | Leads | 300,000 |
  | Lead status changes | 89,833 |
  | Campaign members | 90,263 |
  | Tasks | 700,000 |
  | Events | 300,000 |
  | Emails | 60,000 |
  | Cases, with Service Cloud on | 50,000 |
  | Case history | 44,334 |
  | Case milestones | about 94,000 |
  | Primary CPQ quotes | 129,548 |
  | Open deals in the current quarter's forecast | 31,272 |
  | Logins over the past year | 76,041 |
  | Open leads untouched for 90 days | 69,093 |
  | Contacts with no email | 20,000 |

  The org keeps the synthetic org's set-up: its metadata, rules, licences and first 15 users, so the calibration still fits.
- **The run.** The whole engine, every module (Sales Cloud, Service Cloud, CPQ and Account Engagement, Complete scope), through the simulator, then every document rendered. `npm run scale -- <folder outside the repository>`.
- **The simulator** now answers as Salesforce does when given a page size (decision 331):
  - 2,000 rows a page, with a `nextRecordsUrl` for the rest;
  - a composite subrequest that returns one page and says there's more.

  Before this, every query came back in one page, so no test could show a read that assumed it would.
- **Latency.** There's no real waiting. Each call is costed at 200 ms, plus 0.3 ms for each row returned, so a full 2,000-row page costs 0.8 s. That's my estimate for a UK client against an EU Salesforce instance, not a measurement. The engine makes one call at a time (most at once: 1), so the per-call costs add up to the wall time.
- **Retrieves.** Metadata retrieves are polled every 5 s in real runs. The simulator answers on the second poll, so a retrieve's real wait, often a minute or two, isn't in the figures.
- **The daily allowance** is set at 600,000 calls, with 10% already used. That's an Enterprise org's 100,000 plus 1,000 for each of 500 Salesforce licences. That rule is my understanding, not checked against this org's edition.

## The numbers

These are from the committed code on `hc/scale`, at the default row caps.

| | |
|---|---|
| API calls | 1,236 |
| Calls by step | see below |
| Records returned | 2.1 million, 392 MB of JSON |
| Network time, at the latency above | 15 minutes |
| Engine compute (extract, analyse, write) | 33 s |
| Rendering every document | 13 s: 13 executive pages and 148 slides, and every layout check passes |
| Peak memory, whole process | 5.8 to 7.0 GB resident (sampled, and as the OS reports it), 4.9 GB heap |
| Of which the generated org itself | 0.95 GB heap |
| Run folder | 583 MB: checkpoints 536 MB, action lists 46 MB |
| Findings | 82 issues, 30 passed, 17 not measurable |

The memory figures are for one process holding both the generated org (about 2.4 million records, with the simulator's sorted copies and its answers) and the engine. So the engine's own peak is lower than the process's, roughly the heap peak less the org's 0.95 GB. The simulator's cached answers can't be split out cleanly, so treat about 4 GB of heap as the engine's ceiling at this size, not a measurement. Resident memory varied by about 1 GB from run to run.

### Calls by step

| Step | Calls | Network minutes |
|---|---:|---:|
| opportunities (deals and stage history) | 358 | 4.7 |
| activity (tasks, events, emails) | 235 | 3.1 |
| leads (leads, status changes, campaign members) | 196 | 2.6 |
| service-activity | 89 | 1.1 |
| cpq-quotes | 76 | 1.0 |
| login-activity | 40 | 0.5 |
| service-cases | 31 | 0.4 |
| next-steps | 21 | 0.3 |
| service-setup | 18 | 0.1 |
| won-accounts | 16 | 0.2 |
| forecast | 12 | 0.1 |
| pardot-leads | 11 | 0.1 |
| the other 39 steps | 133 | 0.6 |
| **All 51 steps** | **1,236** | **14.7** |

The run used 1,210 calls of its own against the per-run cap of 5,000. That's 24% of the cap, and 0.2% of the org's daily allowance.

### Reading activity and leads in full (an experiment, not committed)

At the default caps, activity and the lead funnel read 200,000 rows each and stop. A 500-user org has more, so the five activity checks and six lead-funnel checks are not measurable. They say so correctly: "There are too many activities to read in full, and a part would be a biased sample."

I raised the shared cap (`MAX_ROWS` in `extract/opportunities.ts`) to 1,000,000 for one run, then put it back:

| | Default caps | Activity and leads in full |
|---|---:|---:|
| API calls | 1,236 | 1,596 |
| Network minutes | 15 | 19 |
| Engine compute, before the fixes below | 60 s | 273 s |
| Engine compute, after them | 33 s | 44 s |
| Peak resident memory, whole process | 5.8 to 7.0 GB | 7.6 to 8.3 GB |
| Run folder | 583 MB | 751 MB |
| Not measurable | 17 | 6 |

The six left are:
- two that are never measurable on this data: dashboard use, and forecast accuracy;
- forecast Commit at risk and early-stage Commit, from the forecast cap (decision 333);
- the milestone check, from the cases cap (332);
- unowned prospects, from the synced-lead cap (334).

## What broke, and what was fixed

Each fix is one commit with a test.

1. **Checks passed on reads cut off at their row cap.** That breaks hard rule 5.
   - **Service Cloud** (decision 332). `svc.milestone-breaches` passed with "Milestones missed: 4850 of 50000 (9.7%)". The read had stopped at 50,000 of about 94,000. Six other case checks had the same gap.
   - **Forecasts** (333). ForecastingFact was read to 20,000 of 31,272 rows. A deal past the cut-off looked uncommitted, and the adjustment share came from part of the forecast.
   - **Account Engagement** (334). "All 20000 open leads synced from Account Engagement are owned by an active person". 20,000 was the cap, not the total.
   - **CPQ** (335). Nothing in CPQ looked at whether quotes or approvals were cut off. This org's 130,000 quotes are under the cap, so the run didn't hit it. The sweep test did. An approval past the cut-off would have shown its discount as unapproved.
   - **A guard** (336). `cut-off-reads.test.ts` marks every capped read as cut off in turn, in the synthetic org and in a small generated one, and fails on any pass that rests on it. A new capped read or a new check is covered without being listed.
2. **Grouping that copied each group's list for every record** (339). Six checks built groups with `[...(group ?? []), row]`, so their time grew with the square of a group's size. With 150,000 events and 150,000 leads, both under their caps, analysis took 53 seconds. With activity and leads read in full it took 273 seconds, against 44 after the fix. The rep activity check also filtered every touch once for each rep, and now counts in one pass.
3. **The scrub rebuilt its name list on every line** (337). Every action list line is scrubbed, so the cost was lines times users: 14 of the 50 seconds of analysis at 500 users. With this and the grouping fix, compute at the default caps went from 60 seconds to 33.
4. **An area's opening slide ran into its footer** (338). The panel didn't leave room for "1 more check couldn't be measured here". It showed on Service operations once its milestone check became not measurable. Any area with that mix would have hit it, at any size.

## What I looked at and left

- **Unpaged reads: none.**
  - Every record read pages, through the client's `nextRecordsUrl` loop or a keyset read.
  - The composite batch (`queryBatch`) doesn't follow `nextRecordsUrl`, and says the result is truncated instead. It's only used for COUNT() queries and the field-usage aggregates, which return one row.
- **Caps that make a check not measurable, correctly.**
  - Activity and leads: see the experiment above.
  - LoginHistory for the dormant-user guards, 20,000 rows over 90 days. A 500-user org sits just under it.
  - All of these say not measurable, or "unknown" for a guard, which blocks a deactivation. None passes.
- **Action lists.**
  - The count-based lists stop at 5,000 rows and say "the first 5,000 of N". Stale leads: 5,000 of 69,093.
  - The lists built from a check's own records aren't capped. The largest here had 53,203 rows, against Excel's limit of 1,048,576 a sheet.
  - The workbook was 9.5 MB and the tickets file 48 KB.
- **A ceiling on checkpoint size.**
  - Each step's checkpoint is written, and read back on resume, as one JSON string, pretty-printed.
  - The opportunities checkpoint was 242 million characters, against V8's limit of about 537 million. Compact JSON would make it 171 million.
  - So an org with somewhat over twice these deals and stage changes would fail writing it, and the run would stop at that step.
  - It's well clear at 500 users, so I haven't changed the format. Writing record checkpoints as JSON lines, as the partial checkpoints already are, would remove the ceiling.
- **Remaining hotspots.** None of these is quadratic in the org's size:
  - `loadPipeline` rebuilds 200,000 deals for each of about 30 rules, about 6 seconds in all. Caching it per run would save most of that.
  - CPQ's per-rep discount comparison filters every quote for each rep, about 6 seconds, and 11 with every record read.
  - The sales-process check, about 4 seconds.
- **Service metrics on a cut-off read.** "Open cases" is the rows read, with "Rows past the read limit" in its exclusions. So at more than 50,000 open cases it would print 50,000. It's labelled, and no check passes on it, but it's a figure from a part. Left for a decision on whether a metric should be not measurable instead.

## Recommendation: the per-run API budget for a 500-user org

- **Keep the per-run cap at 5,000 calls** (`BUDGET_DEFAULTS.maxCallsPerRun`).
  - A 500-user org with every module needs 1,236 calls at today's caps, and 1,596 with activity and leads read in full.
  - That leaves about three times the headroom for what the generated org doesn't have: more custom objects, bigger metadata, Tooling fallbacks and retries.
  - A 1,000-user org would roughly double the record steps, to about 2,500 to 3,000, which still fits.
- **Keep the stop at 50% of the daily allowance.** At 500 users the run uses about 0.2% of a day's calls. The 50% line only matters for an org whose integrations already use most of their allowance.
- **Plan for about 20 to 30 minutes of wall time.** That's 15 to 19 minutes of calls at the assumed latency, plus retrieves and about a minute of compute. The calls are made one at a time.
- **Next:** raise the activity and lead caps from 200,000 so a 500-user org gets those eleven checks. The cost is 360 more calls, 4 more minutes, and 7.6 to 8.3 GB resident for the whole test process. The worker's real peak is lower, without the generated org, but it should be measured on the worker host before the caps go up. I've left the caps as they are for that reason (decision 340).

## Memory (Brief AS, night 3, later)

Branch `hc/memory`, from health-check `879f9c8`, with health-check `7b64bd4` merged in. Decisions 341 to 347.

### How it's measured now

- The simulator and its generated org run in a child process (`scale-sim-child.ts`), so the figures are the engine's alone. In one process, the org's 1.6 GB hid them.
- The engine's resident and heap peak are sampled every 50 ms, and reported for each step, for analysis, and for rendering. Analysis is one synchronous block, so its true heap peak comes from V8's GC trace instead: about 630 to 760 MB live at its worst.
- `npm run scale` fails if the engine's resident peak is over `--limit-mb`, 2,500 by default. It runs under the worker's recommended heap cap, 1,536 MB, so the figure doesn't depend on the machine.
- `--parallel 2` runs two jobs at once in one process, as a worker with two job slots might.

### Before and after

Both runs were measured the same way: `7b64bd4`'s engine and this branch, under the new runner, on the 500-user org with all four clouds and every document rendered. Resident peaks, in MB:

| Step or phase | Before, default heap | After, default heap | Before, 1,536 MB cap | After, 1,536 MB cap |
|---|---:|---:|---:|---:|
| opportunities | 1,668 | 794 | 1,650 | 786 |
| leads | 1,372 | 890 | 1,610 | 842 |
| activity | 1,509 | 1,050 | 1,520 | 841 |
| service-activity | 1,427 | 1,068 | 1,435 | 895 |
| cpq-quotes | 1,548 | 1,119 | 1,562 | 924 |
| the last extraction step (facts) | 1,585 | 1,135 | 1,451 | 897 |
| analysis and writing | 3,234 | 1,683 | 1,716 | 1,472 |
| rendering the documents | 3,239 | 2,057 | 1,716 | 1,629 |
| **Peak** | **3,239** | **2,057** | **1,716** | **1,629** |
| Heap peak, sampled | 2,036 | 998 | 961 | 980 |
| Compute, seconds | 31 | 19 | 31 | 20 |
| Run folder | 593 MB | 438 MB | 593 MB | 438 MB |

- **Two 500-user jobs at once, after, under the 1,536 MB cap:** 2,045 MB resident, 1,271 MB heap, and both jobs reached the same conclusions.
- **Under a 768 MB cap** the run ran out of heap during analysis. 1,024 MB worked, so one run's live peak sits between those two.
- **Rendering.** Its row is the process's peak by then, run job included, because V8 doesn't hand freed memory back to the system. On its own, the documents job peaks at about 1.3 GB, most of it printing the decks. Those files are session B's, so I left them alone.

### What was holding memory, and what changed

1. **Analysis rebuilt the deals for every rule** (decision 343). `loadPipeline` made 200,000 deals with their history for each of the 21 rules and metrics that ask, and the garbage set the peak. They're now built once for each analysis. Analysis alone went from 3.3 GB to 2.3 GB resident, and ran 7 seconds faster.
2. **CPQ held every rep's peers at once** (344). About 470 lists of 65,000 quotes, 543 MB, were held while the comparisons were judged. Each is now made, judged and dropped.
3. **Checkpoints were one string** (345). The opportunities checkpoint was a 242-million-character string, held again as bytes. Checkpoints are now compact JSON, with long arrays an item to a line, written and read in pieces. That also removes the V8 string ceiling noted above. Extraction's peak went from about 1.65 GB to 0.8 GB.
4. **Repeated values were held once per row** (346). JSON.parse makes a new string for every value. Each step's output now holds each repeated id or short value once. What's kept for analysis went from about 600 MB to 470 MB.
5. **The heap cap** (347). With no cap, V8 sizes the heap from the machine's memory, 4.3 GB here, and lets garbage build up before collecting. Capped at 1,536 MB, it collects sooner, at a few seconds' cost.

Not changed: trimming fields from the rows, and streaming reads into running totals. Both would mean rules reading something other than the rows they read now, so their findings would need re-checking one by one. With the four changes above and the cap, a run is under 2 GB without them. Rendering, which is session B's area, wasn't touched.

### Holding the line

- **Recorded and generated orgs.** `fingerprint.test.ts` holds the synthetic org's recorded run and a small generated org to `7b64bd4`'s own fingerprints: findings, metrics, scores, manual steps, action lists and raw notes. The fingerprints were taken by running `7b64bd4`, and this branch reproduces them exactly.
- **The 500-user org.** Every run above, before and after, capped or not, one job or two, has the same fingerprint, report sections and layout included.
- **The samples.** All seven variants rendered on `7b64bd4` and on this branch match file for file: Sales Cloud alone, with Service, CPQ or Account Engagement, with all three, and Essentials alone and with all three.

### Recommended worker settings

Updated for the heavy gate (decision 371, Brief AV). The worker now works runs and documents one at a time, in a queue of their own. Connect and share jobs keep flowing through the console's queue beside them. So two job slots never mean two big jobs.

- **`NODE_OPTIONS=--max-old-space-size=1536`**, set explicitly.
  - Without it, Node picks a heap limit from the host's memory. On a big host that lets garbage reach 2 GB or more, and on a small one it could fall below a run's 0.65 to 0.75 GB live peak.
  - 1,536 MB is about twice one run's live peak.
- **`HC_WORKER_CONCURRENCY=2`**, as now. That's two connect or share jobs at once, beside the one heavy job.
- **A host with 2 GB of memory is enough for 500-user orgs.**
  - One heavy job peaks at 1.63 GB resident, rendering included.
  - Connect and share jobs add little: a token exchange, a describe or a Drive call.
  - Printing the documents starts Chromium as a process of its own, which isn't counted in that figure. If Render's 2 GB covers Chromium too, 2 GB is tight while documents are printing, so 3 GB is the safer choice wherever documents are made.
- **More than one worker process** each takes one heavy job. So it's one per process, not one across the service, and each process needs its own memory.

What the gate doesn't change:
- A run still takes 20 to 30 minutes at 500 users.
- A second run waits for the first. A queue of three 500-user runs takes about an hour and a half to clear.
- A documents make waiting behind a run waits too. Among the heavy jobs waiting, documents go first, by the console's own priority.

## Reproducing

```bash
npm run scale -- /path/outside/the/repo                # the 500-user org, every module, with rendering
npm run scale -- /path/outside/the/repo --factor 0.1   # a tenth of it
npm run scale -- /path/outside/the/repo --no-render --latency 300 --per-record 0.5
npm run scale -- /path/outside/the/repo --parallel 2                  # two jobs at once
node --expose-gc src/testing/scale-run.ts /path/outside/the/repo --diagnose   # what each step's output keeps
```

The summary goes to `scale-<factor>.json` in that folder, beside the run folder. The run folder holds the whole synthetic org's records in its checkpoints: about 0.6 GB at full size.
