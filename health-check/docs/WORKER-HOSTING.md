# Hosting the worker

The worker takes run and connect jobs from the console's queue. Until it's hosted, the live console can make connect links and queue runs, but nothing picks them up. It ran end to end on a developer's machine on 2026-11-04 (MILESTONES.md).

This sets it up on Render, next to the Barry worker, as the product plan's "existing Vercel, Neon, Redis and worker setup". Turn things on in the order below: each step is safe on its own.

## 1. Create the Render service

A **Background Worker**, not a web service: it listens to Redis and serves nothing.

| Setting | Value |
| --- | --- |
| Repository | `barry-platlform` |
| Branch | `health-check` for now, `main` once the engine is merged |
| Root directory | `health-check` |
| Runtime | Node. `health-check/.node-version` asks for 24 |
| Build command | `npm ci && npm install --no-save bullmq@^5.70 pg@^8.19` |
| Start command | `node src/cli/hc.ts worker` |
| Instances | One. The token store is on its disk, so a second copy wouldn't see it |
| Region | Frankfurt, the EU region nearest Neon's London (QUESTIONS.md 2) |
| Disk | Mounted at `/var/data`, 1 GB to start. Run folders hold CRM records, and nothing deletes them yet: the plan's retention periods aren't built (QUESTIONS.md 41) |

A persistent disk needs a paid instance type, so check the cost when you choose one. Set `PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1` too: the worker doesn't render, so it needs no browser.

## 2. Its environment variables

| Variable | Value |
| --- | --- |
| `REDIS_URL` | The same as Vercel's, so it reads the queue the console writes to |
| `HC_WORKER_DATABASE_URL` | The same as Vercel's `DATABASE_URL`. Production already has the health-check tables (applied 2026-11-04) |
| `HC_CONNECT_KEY` | 32 random bytes, base64: `openssl rand -base64 32`. The console gets the same value |
| `HC_TOKEN_KEY` | Another 32 random bytes, different from `HC_CONNECT_KEY`. Only the worker has it |
| `HC_CONNECTIONS_DIR` | `/var/data/connections` |
| `HC_RUNS_DIR` | `/var/data/runs` |
| `HC_SF_CLIENT_ID` | The Barometer app's consumer key from BlackCloudProd: External Client App Manager, Black Cloud Barometer, Settings, OAuth Settings, Consumer Key and Secret |
| `HC_SF_CLIENT_SECRET` | Its secret, from the same place |
| `HC_SF_REDIRECT_URI` | `https://www.black-cloud.com/internal/health-check/connect/callback`, already registered on the app |

Keep `HC_TOKEN_KEY` somewhere safe as well. Without it the stored tokens can't be opened, and every customer would have to connect again.

When it starts, the log says `Taking jobs from health-check-jobs, 2 at a time.` If a variable is missing it stops and names it.

## 3. Turn the console on, in Vercel

Production environment only:

| Variable | Value |
| --- | --- |
| `HC_REPOSITORY` | `production`. Runs are kept in the site's database instead of the sample data. Refused on previews and laptops |
| `HC_JOBS` | `queue` |
| `HC_CONNECT_KEY` | The same value as the worker's |
| `HC_SF_CLIENT_ID` | The same as the worker's |
| `HC_SF_REDIRECT_URI` | The same as the worker's, so the two always agree |

The console's sign-in needs `HEALTH_CHECK_SESSION_SECRET`, `HEALTH_CHECK_ALLOWED_EMAILS` and the Google client, with `<SCRAPE_BASE_URL>/api/auth/health-check/callback` as an authorised redirect URI on the Google OAuth client. If you've signed in to the live console, that's done.

Redeploy after setting them. The sample runs disappear from the live console, which then shows only real runs.

## 4. Check it

1. In the console, create an OAuth run against a scratch org and make a connect link.
2. Open it in a private window and sign in as that org's read-only user.
3. The worker's log shows `connected: Connected "<name>" read only`.
4. Confirm the calibration and press Start run. The progress bar moves as each step finishes.

## Turning it off

Set `HC_JOBS` to `off` in Vercel and stop the Render service. Queued jobs wait in Redis. To revoke a stored connection, run `node src/cli/hc.ts connect revoke --connection <name>` in the worker's shell, which has the same variables. It revokes the tokens at Salesforce and deletes them from the disk.
