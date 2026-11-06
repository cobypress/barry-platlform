# Night 3: the scan cleanup's tally in one statement (Brief AQ, part 1)

Worker A, 7 November 2026. This fixes P1 from the console review (`review-console.md`).

The change is on the console: blackcloud-next `hc/tally-atomic`, from health-check `a0e58f9`, commit `d766f02`.

## What was wrong

The 30-day scan cleanup (`deleteExpiredScans` in `lib/health-check/postgres-repository.ts`) ran in one Prisma transaction at Postgres's default level, Read Committed, in four steps:

1. It read the expired requests.
2. It read their links, results and leads.
3. It upserted the tallies.
4. It deleted the requests.

Nothing locked the requests between the read and the delete. So two cleanups running at once could both read the same requests, and both add them to the tallies. The second delete then found nothing left to delete, but its tally had already been added.

## What it is now

One statement, `SCAN_CLEANUP_SQL` in `lib/health-check/scan/cleanup-sql.ts`:

```sql
WITH gone AS (
  DELETE FROM "HealthCheckScanRequest" WHERE "expiresAt" <= $1::timestamp(3)
  RETURNING "id", "runId", "createdAt", "status", "linkOpenedAt", "resultsViewedAt", "partnerId"
),
reached AS (SELECT g."createdAt"::date AS "day", g."partnerId", GREATEST(...) AS "furthest" FROM gone g),
steps ("i", "step") AS (VALUES (0, 'requested'), ..., (6, 'talk')),
counts AS (SELECT day, partnerId, step, count(*) FROM reached JOIN steps ON i <= furthest GROUP BY ...),
partner_tally AS (INSERT INTO "HealthCheckPartnerScanTally" ... ON CONFLICT (...) DO UPDATE SET "count" = "HealthCheckPartnerScanTally"."count" + EXCLUDED."count"),
tally AS (INSERT INTO "HealthCheckScanTally" ... GROUP BY day, step ON CONFLICT (...) DO UPDATE SET "count" = "HealthCheckScanTally"."count" + EXCLUDED."count")
SELECT "id", "runId" FROM gone
```

- The tallies are fed only by the rows the DELETE returned. The statement reads the scan requests nowhere else.
- The step a request reached is worked out in SQL, from the same facts as `furthest()` in `scan/funnel.ts`. A later step implies the earlier ones, and a result implies connected.
- The steps' order and names are generated from `FUNNEL_STEPS`, so they can't drift from the funnel.
- The day is the column's own date. The columns hold UTC, so this matches `tallyDay`.
- The scan runs are deleted next, in the same transaction, from the run ids the statement returned. The cascade takes their results, links and steps.

## Why two cleanups can't count a request twice

The PostgreSQL documentation, section 13.2.1, "Read Committed Isolation Level" (checked against https://www.postgresql.org/docs/current/transaction-iso.html, version 18.6, on 7 November 2026):

> UPDATE, DELETE, SELECT FOR UPDATE, and SELECT FOR SHARE commands behave the same as SELECT in terms of searching for target rows: they will only find target rows that were committed as of the command start time. However, such a target row might have already been updated (or deleted or locked) by another concurrent transaction by the time it is found. In this case, the would-be updater will wait for the first updating transaction to commit or roll back (if it is still in progress). If the first updater rolls back, then its effects are negated and the second updater can proceed with updating the originally found row. If the first updater commits, the second updater will ignore the row if the first updater deleted it [...]

So, for two cleanups at once:

- The second DELETE either waits for the first and then skips every row the first deleted, or deletes rows the first never reached.
- A row is therefore returned by exactly one DELETE. The tallies are fed only from `RETURNING`, so each request is counted by exactly one cleanup.
- At Repeatable Read or Serializable (sections 13.2.2 and 13.2.3), the second transaction fails with a serialization error instead of skipping. That's also never a double count.

Section 7.8.4, "Data-Modifying Statements in WITH", adds:

- every data-modifying sub-statement runs exactly once, to completion, whether or not the main query reads its output;
- all of them see the same snapshot.

So both inserts run, and the link, result and lead look-ups see the rows as they were before the statement.

The old upserts couldn't be made safe the same way. They counted rows read by a plain SELECT, which takes no lock and doesn't wait for anything.

## How it was checked

- **Unit tests** (`lib/health-check/__tests__/scan-cleanup-sql.test.ts`):
  - the statement's shape: it starts with the DELETE ... RETURNING, reads the requests nowhere else, and has both inserts with their conflict increments;
  - the steps match `FUNNEL_STEPS` in order, and each case in `GREATEST` gives its step's index;
  - the repository runs the one statement with the time as an ISO string, then deletes the returned scan runs, in one transaction.
- **On an in-memory Postgres.** I used pglite 0.4.1 from the console's own `node_modules`, in a scratch script that isn't committed. No server, no connection and no real data were involved.
  - Every console migration applied cleanly.
  - The seed was three expired requests and one live one, and a tally row already holding 5. One of the expired requests was made at 23:30 UTC.
  - Over the three expired requests, each step reached was counted once, implied steps included. Requested went from 5 to 8.
  - The 23:30 request was counted on its own UTC day, and the partner's tally got its two.
  - A second run returned nothing and changed nothing.
  - pglite has a single connection, so it can't show two cleanups at once. That part rests on the documentation above.

Console tests: 1,278 of 1,278, and `tsc` is clean.
