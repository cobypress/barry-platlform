# Lane C1: the worker side of Milestone 5

Branch `hc/worker`, tree `worktrees/hc-worker`. Nothing pushed, deployed or applied to a database. No Salesforce, database, Redis or Claude contact: everything runs on fakes and synthetic data.

## Results

- Baseline: 1184 of 1184 tests, typecheck clean.
- Final: 1282 of 1282 tests, none skipped. Typecheck clean.

## Done

1. **Engine.** `runHealthCheck` takes `runId` (start a new run under the console's id) and `onProgress` (awaited after each save of run.json, given a copy). A run id must be a plain name, because it becomes a folder name and the worker's come from a database. A new run won't start over a folder that already holds one. Nothing else in the engine changed.
2. **Connect secrets** (`src/worker/connect-crypto.ts`). Seal and open in the contract's `v1.<iv>.<tag>.<ciphertext>` format. Fixed vector: `docs/overnight/night-2/connect-crypto-vector.json` (key bytes 0 to 31, two cases, one must-fail case). The test opens each case with `createDecipheriv` and with WebCrypto, and re-seals each to the exact text.
3. **Job runner** (`src/worker/worker.ts`, `run-job.ts`, `connect-job.ts`, `lock.ts`, `types.ts`).
   - Run job: loads run, intake, calibration, connection name and manual answers through `WorkerStore`. Checks the set-up (calibration confirmed, OAuth connection kind, connection named and present, modules known, stored connection's org matches the run's). Runs the engine with `OAuthConnectionProvider`, writes `intake.json` and `report-sections.json` (from `buildReport`: engine text, no browser, no Claude), and saves findings, metrics, scores, manual steps, sections and versions in one write.
   - Connect job: opens the sealed code and pending authorisation, exchanges the code, stores the tokens with `TokenStore`, runs `checkReadOnlyUser`, then marks the link used (with a note if the check couldn't prove anything) or refused (revoked and forgotten if it can write).
4. **Retries and idempotency.** Payloads are ids only; anything more is refused and never retried. A file lock per run and per link stops two deliveries at once (the console gives each enqueue its own job id, so BullMQ would allow it). Run jobs resume through the engine's checkpoints, and results replace rather than add. In review or released: nothing is done. A finished status is only written with the results. Connect jobs do nothing once the link is settled, and skip the exchange when this sign-in's tokens are already stored (crash between storing and checking).
5. **BullMQ adapter** (`src/worker/bullmq.ts`), tested with a fake Worker. Returns only the outcome to Redis.
6. **`hc worker`** (`src/cli/worker.ts`). Refuses to start, naming each missing or wrong variable, before loading BullMQ or pg.
7. **Stores.** `MemoryWorkerStore` for tests. `PostgresWorkerStore` (**unverified**: never run against a database), plain SQL through the interface `pg.Pool` satisfies. Its column lists were checked by script against `blackcloud-next/migrations/20261103_health_check*.sql`: all match, apart from the new columns below.
8. DECISIONS.md rows 69 to 72 (68 to 71 on `hc/worker`; renumbered at merge).

## Columns and tables needed from C2

New SQL file in blackcloud-next, written by C2. Nothing here has been applied anywhere.

- `HealthCheckRun."connectionName" TEXT` (nullable): the stored connection a run reads through.
- `HealthCheckRun."runDir" TEXT` (nullable): the run folder on the worker's disk.
- New table `HealthCheckConnectLink`:
  - `ConnectLink`'s fields: `id` (PK, uuid text), `tokenHash`, `customerRef`, `connectionName`, `loginUrl`, `createdBy`, `createdAt`, `expiresAt`, `usedAt`, `revokedAt` (timestamps as `TIMESTAMP(3)`, UTC, like the other tables).
  - `state TEXT NOT NULL`, checked against `open, pending, used, refused, expired, withdrawn`.
  - `sealedPending TEXT`, `sealedCode TEXT` (nullable; the worker sets both to NULL when it settles a link).
  - `refusedReason TEXT`, `refusedAt TIMESTAMP(3)`, `checkNote TEXT`, `connectedOrgId TEXT`, `connectedUserId TEXT`.
- Run statuses: none added. A failure before the engine starts goes on a run step with id `worker`, status `failed`, and the reason in `error`.

## Environment variables (DECISIONS 72)

Required: `REDIS_URL`, `HC_WORKER_DATABASE_URL`, `HC_CONNECT_KEY`, `HC_TOKEN_KEY`, `HC_CONNECTIONS_DIR`, `HC_RUNS_DIR`, `HC_SF_CLIENT_ID`, `HC_SF_CLIENT_SECRET`, `HC_SF_REDIRECT_URI`. Optional: `HC_WORKER_CONCURRENCY`, `HC_API_BUDGET_SHARE`, `HC_SF_API_VERSION`. New ones: `HC_WORKER_DATABASE_URL`, `HC_CONNECT_KEY`, `HC_RUNS_DIR`, `HC_WORKER_CONCURRENCY`. The worker refuses one key for both `HC_CONNECT_KEY` and `HC_TOKEN_KEY`.

## Decisions

- **No BullMQ or pg dependency.** With pg they come to 34 packages and 13MB (measured in a scratch folder). They're loaded at start by `hc worker`. To deploy: `npm install bullmq@^5.70 pg@^8.19`.
- **Approval from the console's records.** In the worker, the connection name comes from the run or link, not `HC_APPROVED_ORG_ALIASES`. The org id match stands in for the typo guard. Coby should confirm this.
- **The console's current step payload** (`{ runId, stepId, correlation_id }`) is read as a run job. An optional `correlation_id` is allowed on the contract payloads too. Nothing else is.
- **Reconnects.** A read-only reconnect replaces the old connection, and the old refresh token is left to lapse rather than revoked, because I couldn't verify whether revoking it would reach the new grant. A reconnect that can write puts the old one back. A sign-in to a different org from the one held is refused and revoked.
- **The worker's fields only.** It writes status, rule set, calibration hash, as-of, window, API version, budget, steps, sandbox flag and finish time. Never customer reference, creator, benchmark consent, modules or org.
- **Locks** live in `<HC_RUNS_DIR>-locks`, because `hc purge --older-than` would list a folder inside the runs folder as skipped.
- **Lane B's note**: the worker never calls Claude. Nothing in `src/worker` or `src/cli/worker.ts` loads the narrative client.

## Doubts

- **BullMQ `attemptsMade`.** I've taken it to be the number of attempts already finished (n - 1 while attempt n runs), so the last delivery is `attemptsMade + 1 >= attempts`. Not checked against BullMQ's source tonight. If it's wrong, the last delivery is misread by one, and a transient failure is retried once more or recorded once early.
- **Sections are replaced** on every completed run job until the run is in review. If the console lets consultants edit sections while a run is awaiting manual steps, re-analysis drops those edits. C2 to confirm.
- A connect job doesn't check the link's expiry, only that it's withdrawn or used: the console checked expiry at the callback, and Salesforce expires the code anyway.
- A refused link can be retried (DECISIONS 63). The console must move it from refused back to pending on a new callback, and clear `refusedReason`.
- If the calibration changes while a run is awaiting manual steps, the engine refuses to resume ("start a new run"). That fails after three deliveries.
- Stopped at the API budget: the worker records it and stops. Nothing re-queues it the next day.
- The Postgres store's timestamp handling (ISO in, `to_char` out) is reasoned from Postgres's documented behaviour for `TIMESTAMP` without a zone, but not tried.

## Contract changes I'd suggest (worked to the contract as written)

- Associated data of `<linkId>:code` and `<linkId>:pending` instead of the bare link id, so the two sealed values can't be swapped. Harmless today: a swapped value fails to parse.
- Job payloads: drop `stepId` from the console's queue helper. The engine resumes its own steps, so one run job is enough.

## Review fixes (coordinator, after hand-over)

An independent review found five problems. Each has tests that fail on the old code (14 failed when I put the old files back) and pass now.

1. **The console withdraws the link during the check.** `markLinkUsed` threw, the job retried while still holding the tokens, and the next delivery skipped them. Now a failed `markLinkUsed` revokes and deletes the tokens before anything else. Then it either skips (the link was settled meanwhile) or refuses (the store failed). Also, any delivery that finds a link settled but never used revokes tokens still bound to it.
2. **Own tokens judged by clock.** The stored connection now carries `fromLink: { linkId, stateHash }` (optional, in `StoredConnection`). A retry treats tokens as its own only if both match. Clock skew no longer matters, and another link with the same connection name never matches. A connection from an earlier sign-in on the same link is released.
3. **A refusal lost to a store error.** The reason is written to `<locks>/connect-<linkId>.refusal.json` before the tokens go and before the console is told. Any later delivery with the link still pending records that reason without a second exchange. The note is deleted once the console has it, or once the link is settled.
4. **Lock ownership.** Locks hold a random token for each process. A lock with this pid but another token belongs to an earlier process (a restarted container), so it's stale. A stale lock is taken over only while holding a breaker file, and only if it's still the one first seen. A breaker over a minute old is cleared. Finishing never removes a lock someone else holds.
5. **Failures after the engine** (intake write, loading the run, building sections, writing results) are thrown for a retry, which reads no data again. On the last delivery they're recorded on the `worker` step.

Doubts from these fixes:

- If the worker crashes between noting a refusal and revoking, a retry still revokes any tokens bound to the link. But an older connection kept aside in memory can't be put back.
- The breaker's one-minute rule is itself a small race, but only when a process crashed within milliseconds of breaking a lock.
