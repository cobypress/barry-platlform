# Night 3, Brief BA: review of C's scale, memory and gate work

**Range:** the merges 879f9c8 (hc/scale), c2d2dab (hc/memory), 01a1a76 (hc/heavy-gate), a6b45e7 (hc/full-reads) and 70b95d8 (hc/full-reads-2), each against its first parent.

**Branch:** `hc/review-scale` from `health-check` a1a8988. Nothing is pushed or merged.

**Who read what:** I reviewed the job gate and the caps. A read-only reviewer took the checkpoint format and interning. Each confirmed bug got a failing test first, then a fix in its own commit.

## Fixed

| Commit | What | Row |
|---|---|---|
| 1e8f88e | A heavy job BullMQ gives up on is recorded, not left in progress for ever | 396 |
| 86112d4 | A checkpoint cut short never reads as complete | 397 |

### 1. The job gate (396)

**The defect.** BullMQ 5.81.5 is installed. I read its source, not the docs:
- `Worker` defaults: `lockDuration` 30 s, `lockRenewTime` half the lock, `stalledInterval` 30 s, `maxStalledCount` 1.
- When a heavy job's process dies (a deploy, a crash, or running out of memory), its lock lapses after `HEAVY_LOCK_MS`, 10 minutes. The stalled check (`moveStalledJobsToWait-9.lua`) then moves it back to waiting and increments `stc`.
- Past `maxStalledCount`, the same script marks the job `defa = "job stalled more than allowable limit"`.
- `Worker.getNextJob` then fails the job without running its processor (`worker.js:345-352`, `getUnrecoverableErrorMessage`). Only the `failed` event knows, and the gate only logged it.
- **Result:** the run stayed at "extracting" or "analysing" in the database for ever (documents at "making"), and nobody was told.

**How it happens.** Render restarts the worker on every deploy. SIGTERM's `close()` waits for the run in hand, but only until Render's grace period ends. So a run of 25 minutes or more across a night of merges can be killed twice, and a 500-user org that runs out of memory is killed every time.

**The fix.**
- `startQueueWorker` takes `onLost`, which the worker wires to `lostRecorder(store)`. A lost run is recorded as failed and lost documents as not made, with a reason the console shows and an offer to queue it again.
- The heavy worker allows two stalls (`HEAVY_MAX_STALLS`), because a run resumes from its checkpoints.
- **Tests:** they fail without the fix.
  - A stalled-too-often run calls `onLost`.
  - A failure the handler raised itself doesn't call it, since the handler already recorded it.
  - The heavy worker's `maxStalledCount` is 2.
  - The recorder writes runs and documents only.

**Checked and clean.**
- **Lost or run twice on hand-off.** The light processor awaits `heavy.add` before its job completes. A failed add throws, so the console's job retries with its own backoff.
  - A crash between the add and the completion re-delivers the console's job after its 30 s lock.
  - The heavy add then finds the same job id and adds nothing, as long as the heavy job still exists. That's the case unless it has already finished and been removed (see plausible item 1).
- **Waiting isn't an attempt.** A heavy job waits in Redis, not in a slot. Its options are copied, so attempts, backoff, priority and removal behave as before.
- **The console's progress reads.** The console never reads the queue: `lib/health-check/jobs.ts` only adds, with a new id per send (`kind-id-timestamp`), and progress comes from the database. Completing its job at hand-off changes nothing it can see.
- **The lock against long compute.** Analysis at 500 users is one synchronous block of about 33 s (`scale.md`). The lock renews every 5 minutes while the event loop is free, as it is through 25 minutes of network waits. A 10-minute lock gives about 18 times headroom.
- **Restart mid-run.** Light jobs are short. A killed heavy job resumes after its 10-minute lock lapses, up to twice, and is then recorded as failed. Before the gate, the 30 s lock resumed sooner, at the same single stall.

### 2. Checkpoints (397)

**The defects.**
- **No atomic write.** `writeCheckpoint` wrote over the real path in 4 MB pieces, with no temp file and rename. Resume was protected only by ordering: a step is "done" only after its checkpoint is written.
- **A cut list read as complete.** A top-level list of 1,000 items or more, cut at a line end, read as a valid shorter list. The reviewer showed this with a real SIGKILL at 8.4 MB: 93,455 items out of 400,000 read as complete.
  - It isn't reachable today: every extractor returns an object, and an object checkpoint cut anywhere fails to parse.
- **Half a line in a page log.** A crash mid-append left half a line in a page log (`.partial.jsonl`). Every later resume would throw on it, and the next append would have run on from it. This predates 345.

**The fix.**
- The checkpoint is written to `<name>.<pid>.writing` and renamed over the real file once whole.
- A file that ends inside a list is refused.
- A page log is cut back to its last whole line before it's read.
- **Tests:** they fail without the fix.
  - a cut top-level list;
  - a write that throws part-way, which leaves the previous checkpoint intact and no temp file;
  - a page log with half a line, which resumes and appends cleanly.

**Checked and clean** (the reviewer, with the pre-merge engine from `git archive c2d2dab^1`):
- **Old indented checkpoints resume.** A whole old-engine run resumed in the new engine to `in_review`.
- **Isolated from other changes.** Rewriting every checkpoint in the old format gives byte-identical findings, metrics, scores, manual steps, action lists and raw notes.
- **Mixed formats work.** A run half in the old format and finished in the new one gives the same outputs, at two stop points.

### 3. Interned strings (346): clean

- `shareRepeatedValues` pools strings only. Objects, arrays and Dates are walked in place, never shared.
- The reviewer's test changed one row's nested object, its array (push, sort and reverse) and its fields. The other row was unchanged, on both the fresh path and the resume path.
- No production code changes an extract row after its step ends. Every in-place sort is on a freshly built array. Tests that edit rows use `clone()` (`structuredClone`).

### 4. The caps (381, 383 to 385): clean

- **Cut-off flags.** Every capped read still says it's cut off at the new cap:
  - `readCapped`: `rows.length >= RECORD_READ_CAP`
  - `readSoql`'s `maxRecords`: `truncated` while a next page exists
  - synced leads: `>= PARDOT_LEAD_ROWS`
- **Consumers.** Each turns that into "not measurable" or "at least": activity, the lead funnel and campaign members, forecast items and facts, service, and Account Engagement.
- **Budget.** No step can pass `stopAtShare`, however small the daily allowance. `assertCanSpend` runs before every call, against both the per-run cap and the org's share.
- **Resume.** The million-row reads are keyset reads that checkpoint every page, so a run stopped at the line resumes where it stopped, not at the start of the step. The `readSoql` reads with new caps are small: forecast at most 50 calls, each case list at most 100.

## Plausible, not fixed

1. **A run made twice in a narrow window.** The console's job could be re-delivered after its heavy job has finished and been removed. That needs a crash between `heavy.add` and the console job's completion, and then the heavy job finishing within the 30 to 60 seconds before the re-delivery.
   - Runs take minutes, and a run that's already in review is skipped, so the harm is small.
   - **Recommend:** give the heavy job `removeOnComplete: { age: 86400 }` instead of the console's `true`, so its id is held for a day and catches any late re-delivery.
2. **Capped extracts reused across the deploy.** The cap raises (0dca6ac, a541311) didn't bump the extractors' versions.
   - A run started before the deploy and resumed after it reuses its capped reads. They're flagged cut off, so their checks say "not measurable" rather than giving wrong numbers.
   - **Recommend:** bump the versions of the activity, leads, forecast, service and synced-lead extractors. A resumed run then re-reads those steps, at the cost of their calls once.
3. **A done step whose checkpoint is missing.** If the checkpoint file is deleted, the step is re-extracted while `run.json` still says done. A crash during that rewrite now leaves the last good file, or none (397), and never a trusted cut one. This predates tonight.
4. **Shutdown waits for the run in hand.** On SIGTERM, `close()` waits for it, and Render kills the process at the end of its grace period. With 396 that costs a 10-minute wait before the run resumes, and a lost run is recorded.
   - **Recommend:** check whether Render lets the worker's shutdown wait be lengthened. I haven't verified that it does.

## Checks

- **Engine:** typecheck clean, 2,108 of 2,108 tests pass, the fingerprint unchanged.
