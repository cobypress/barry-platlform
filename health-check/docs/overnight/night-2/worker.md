# Lane C1: the worker side of Milestone 5

Branch `hc/worker`, tree `worktrees/hc-worker`. Nothing pushed, deployed or applied to a database. No Salesforce, database, Redis or Claude contact: everything below runs on fakes and synthetic data.

## Baseline

- `npm ci`: clean.
- Before any change: 1184 of 1184 tests pass, typecheck clean.

## Progress

1. Engine: `runHealthCheck` takes `runId` (a new run with the console's id) and `onProgress` (awaited on each save of run.json). Nothing else changes; a run id must be a plain name, and a new run won't start over a folder that already has one.
2. `src/worker/connect-crypto.ts`: seal and open in the contract's `v1.` format, with the fixed vector in `docs/overnight/night-2/connect-crypto-vector.json`. The test opens every case through both `createDecipheriv` and WebCrypto, and re-seals each to the exact text.
3. `src/worker/`: job runner (`worker.ts`, `run-job.ts`, `connect-job.ts`), a per-run and per-link lock (`lock.ts`), the `WorkerStore` interface (`types.ts`), and `MemoryWorkerStore`.
