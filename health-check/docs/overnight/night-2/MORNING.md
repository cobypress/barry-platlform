# Morning hand-over, 4 November

**Do first:** the second refresh check on `bc-hc-scratch-web` (MILESTONES.md, "The package, from a second org"). Nothing touched that connection overnight, so the two hours count from your last check. If it says "refreshed", run `connect revoke`.

Nothing was pushed, deployed or applied to a database, and no customer org was contacted. Everything is committed locally on two branches.

| Branch | Repo | Tests | Before tonight |
| --- | --- | --- | --- |
| `hc/night-2` | barry-platlform | 1388 of 1388, type check clean | 1184 |
| `hc/wiring` | blackcloud-next | 704 of 704 unit, 204 of 204 Playwright, type check and lint clean | 557 unit, 156 Playwright |

## What was built

1. **Read-only gap closed, mostly.** A read-only connection used to lose automation and report definitions, because the Metadata API needs a write permission. The engine now reads them through Tooling, REST and the Analytics report describe. On `bc-hc-dev`, as the read-only user, findings that are not measurable fell from 41 to 32. The rest are an empty dev org, apart from one: Apex coverage.
   - The allowlist gained one route: GET a report's `/describe`. That doesn't run the report.
   - The permission set didn't change, so no new package version is needed.
   - **Still not readable read-only:** Apex test coverage, other people's private reports, `AuthProvider`, and approval processes and assignment rules. So on a customer connection, removing fields and deactivating users stay "investigate" rather than safe. That was already true because of private reports.
2. **First names no longer reach Claude.** "Sam said" is caught, and so are surnames, possessives, accents, hyphenated names, nicknames for about 60 names, and the company name. In the engine, a last check refuses to send any prompt that still holds part of a name, and every call to Claude goes through it. The console has its own guard for its review panel.
   - **Trade-off:** names that are also words (Will, May, Price) are masked whenever they're capitalised. So the month "May" gets masked in text sent to Claude. Reports aren't affected.
3. **The worker exists.** `hc worker` takes run and connect jobs from the `health-check-jobs` queue.
   - Run jobs drive the engine and report progress back.
   - Connect jobs exchange the admin's sign-in, store the tokens encrypted, prove the user can't write, and then mark the link used. If the user can write, they revoke the tokens.
   - It's tested against fakes only. Its Postgres code has never met a database.
4. **The console's connect pages exist.** The consultant makes a link, the admin opens it, signs in at Salesforce and comes back, and the console queues the connect job.
   - Starting a run queues a run job, behind `HC_JOBS=queue`.
   - The review panel's Claude client is real, but stays off unless `HC_ANTHROPIC_API_KEY` is set. It has never made a call.
5. **Docs.**
   - `health-check/docs/customer/INSTALL.md` is the guide for a customer's admin, with Salesforce sources for each step.
   - `health-check/docs/BULK-API.md` recommends not building the Bulk API yet. Three REST changes would cover the limits, at about a day's work.

## Reviews

Each branch had an independent review after it was built. All the defects they found are fixed, each with a test that failed before the fix.

- **Engine: eight found.** The worst:
  - workflow rules the read-only route couldn't read made the rule pass, instead of not measurable;
  - a link withdrawn during the connect check left live tokens stored and unrevoked;
  - a database error could lose the reason a user was refused.
- **Console: nothing serious.** Nobody can start, finish, read or withdraw a connection without the link's secret. Seven correctness gaps were fixed:
  - connect jobs now queue ahead of runs, and the worker runs two jobs at once, so an authorisation code doesn't expire behind a long run;
  - each run gets its own connection name, so a customer's production and sandbox runs don't share one;
  - a stuck pending link expires after 30 minutes and can be withdrawn;
  - a new link withdraws all older ones;
  - two links open in one browser can't cross;
  - the Claude guard also covers the customer reference and short forms of the company name;
  - callback errors don't leave the code in the address bar.
- **One accepted risk:** the link secret is in the link's path and the code in the callback's query string, so both reach Vercel's request logs. The secret lasts 72 hours and the code works only once, with our verifier.

## One slip to know about

During one console browser test run, the test browser followed a redirect to Salesforce's public sign-in page. That was six unauthenticated GETs with a made-up client id, and Salesforce answered `invalid_client_id`. No org, user or data was involved. The tests now stop at the redirect, and the test browser can't resolve Salesforce hosts.

## Before any of this is pushed

1. **Apply `blackcloud-next/migrations/20261104_health_check_connect.sql` to the Neon development branch.** It adds the connect link table and two run columns. The worker's queries were checked against it by script, and the console's repository by test, but it has never run on a database. It must reach Neon before the console code is pushed.
2. **Run `next build`** in blackcloud-next. It wasn't run.
3. **Register the callback in BlackCloudProd:** `<console origin>/internal/health-check/connect/callback`, in the Barometer app's global OAuth settings, next to localhost.
4. **Install the worker's database and queue packages** on its host: `npm install bullmq@^5.70 pg@^8.19`. They're loaded at start, not listed as dependencies.

**New environment variables.** None of these is needed to merge.

| Where | Variables |
| --- | --- |
| Console | `HC_CONNECT_KEY`, `HC_SF_CLIENT_ID`, optional `HC_SF_REDIRECT_URI`, `HC_JOBS` (`off` by default), optional `HC_ANTHROPIC_API_KEY` |
| Worker | `REDIS_URL`, `HC_WORKER_DATABASE_URL`, `HC_CONNECT_KEY` (same as the console's), `HC_TOKEN_KEY` (must differ), `HC_CONNECTIONS_DIR`, `HC_RUNS_DIR`, `HC_SF_CLIENT_ID`, `HC_SF_CLIENT_SECRET`, `HC_SF_REDIRECT_URI`, optional `HC_WORKER_CONCURRENCY` (default 2) |

## Decisions for you

1. **Where the worker runs** (QUESTIONS.md 1). It needs Node 24, a disk for the token store and run folders, Redis, and the dev branch's connection string. Nothing joins up end to end until it's somewhere.
2. **Who approves a connection in production.** The worker trusts the connection names in the console's records instead of `HC_APPROVED_ORG_ALIASES`. It checks that the stored connection's org matches the run's before reading anything.
3. **An OAuth run waits** until its link is used, then the consultant presses "Start run". It isn't queued automatically.
4. **The install guide has placeholders:** the install link (no version is promoted), the hosting region, zero data retention, and our outbound IP addresses. QUESTIONS.md 27 to 33 list what Salesforce's docs couldn't confirm. The first real install will answer most of them.
5. **Open questions from the read-only work** are QUESTIONS.md 34 to 38: paging at scale, workflow alerts, and private report counts in other orgs.

## Left behind

- **`bc-hc-dev` has test metadata** from the read-only work, all prefixed `BT_`: one object, flows, a trigger, validation rules, a workflow rule and reports. Its source is in `health-check/scripts/dev-test-metadata`. Leave it for the next live test, or delete it.
- **Worktrees:**
  - `worktrees/hc-readonly`, `hc-scrub`, `hc-worker`, `hc-docs` and `hc-night2` (barry-platlform);
  - `worktrees/hc-wiring` (blackcloud-next).
- **Logs** for each lane are in `docs/overnight/night-2/` in each tree.

## How to take it

```bash
# Engine
cd "/Users/cobypress/Desktop/Black Cloud/barry-platlform" && git merge hc/night-2

# Console: after the migration is on the Neon dev branch
cd "/Users/cobypress/Desktop/Black Cloud/blackcloud-next" && git merge hc/wiring
```

To throw a branch away, remove its worktree, then `git branch -D` it.
