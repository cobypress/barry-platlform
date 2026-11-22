# Hosting the worker

The worker takes run, connect and documents jobs from the console's queue. Until it's hosted, the live console can make connect links and queue runs, but nothing picks them up. It ran end to end on a developer's machine on 2026-11-04 (MILESTONES.md).

This sets it up on Render, next to the Barry worker, as the product plan's "existing Vercel, Neon, Redis and worker setup". Turn things on in the order below: each step is safe on its own.

## 1. Create the Render service

A **Background Worker**, not a web service: it listens to Redis and serves nothing.

| Setting | Value |
| --- | --- |
| Repository | `barry-platlform` |
| Branch | `health-check` for now, `main` once the engine is merged |
| Root directory | `health-check` |
| Runtime | Node. `health-check/.node-version` asks for 24 |
| Build command | `npm ci && npm install --no-save bullmq@^5.70 pg@^8.19 && npx playwright install --only-shell chromium` |
| Start command | `node src/cli/hc.ts worker` |
| Instances | One. The token store is on its disk, so a second copy wouldn't see it |
| Compute | 1 CPU and 2 GB. A run holds the org's data in memory, and 512 MB is enough only for test orgs |
| Region | Frankfurt, the EU region nearest Neon's London (QUESTIONS.md 2) |
| Disk | Mounted at `/var/data`, 1 GB to start. Run folders hold CRM records. The retention sweep deletes a run's `checkpoints/` 30 days after release and the whole folder at 12 months (`src/worker/retention.ts`, decisions 98 and 254) |

Set `NODE_VERSION=24` as well as the repo's pin, and `PLAYWRIGHT_BROWSERS_PATH=0`, so the build puts Chromium inside `node_modules`, where the running service can find it. Chromium prints the released documents. Don't set `PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD`, which stops that install (it was set until 2026-11-05, when the worker didn't render). The Barry worker is in Oregon, but that doesn't matter: this worker reaches Redis by the same public `REDIS_URL` Vercel uses, not Render's private network.

Set up this way on 2026-11-04 as `barometer-worker`.

### Recycling the worker (off until tested again)

With `HC_WORKER_RECYCLE_AT` set, the worker stops itself once its store of org-worded sentences holds that many: it finishes the run in hand, keeps serving connect jobs meanwhile, and exits 75 for Render to start a fresh one with an empty store (DECISIONS.md 666). Unset, it never does, and once the store is full the report marks for the customer's assistant fall back to unknown until the next deploy or restart.

**What happened on 22 Nov 2026.** With it at `100`, the worker recycled after one check and Render restarted the process in the same instance in about 2 seconds, so the restart works. The fresh process then couldn't sign in to Claude ("Token exchange failed with status 401"), so its runs would have had the engine's own text.

Why is not known. The leading guess is that the new process presented the same Render identity token the first one had already exchanged, and Anthropic takes a token with a single-use id (`jti`) once only. But nobody has confirmed that Render's tokens carry a `jti` at all, nor that Render hadn't rewritten the file in those 12 minutes. Other causes fit too: a signing key Render had just rotated, a clock difference beyond Anthropic's 30-second allowance, a token living longer than the issuer's maximum, or a file read while Render was writing it.

Since DECISIONS.md 667 the worker keeps its Claude access token on the disk (`/var/data/claude-sign-in`, a folder only the worker can read), so a process that starts again uses it instead of exchanging again, whatever the cause. That only works while the token lasts: with the Console's 10-minute default rule lifetime, the token from 08:11 would have run out before 08:23. It also hides the cause, which still matters for a worker that never restarts. The same applies to any restart in place, a crash included. At every start the log says whether the token can be kept ("Claude's access token is kept on the disk …" or "… can't be kept on the disk …").

Leave it unset until both of these are done:

1. In the Claude Console, open Workload identity, then Authentication history, and find the failed attempt at 08:23:55 UTC on 22 Nov 2026. Note its reason and tell the engine's owner. `jti_reused` would confirm the guess above; anything else (an expired token, a signature, a subject or audience that doesn't match) means the cause is something else, and it may also stop a worker that never restarts. While there, open the federation rule and check its token lifetime: set it to 60 minutes if it is less.
2. Deploy this version and look at its first lines: "Claude by OIDC federation: The identity token was issued … and expires in …; it carries …" says whether Render's token is single-use, and "Claude's access token is kept on the disk …" that the disk is there. Then set `HC_WORKER_RECYCLE_AT` to `100` and run one health check. After "Recycled: …", the fresh worker's log should say "Claude sign-in works: …", and the authentication history should show no new exchange at that time. If it does, set it to `50000`. If the log says "Claude sign-in failed at start", unset it and tell the engine's owner; the worker keeps trying for an hour and says "Claude sign-in works again" if it recovers.

**Claude on an ordinary day.** A worker that never restarts also exchanges Render's token again each time its access token runs out, and that only works if Render has written a new token by then. Nobody has yet seen it go past its first refresh. The log now says at start how old Render's token is, how long it has left and whether it is single-use, and says once if Claude's sign-in stops working ("Claude sign-in stopped working …") and once when it works again. After a deploy, look for either line over the next few hours. If "stopped working" appears, check the authentication history's reason for it and tell the engine's owner. Keeping the federation rule's token lifetime at 60 minutes, rather than the Console's 10-minute default, means fewer exchanges, so fewer chances to meet a token Render hasn't rewritten yet.

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

For released documents (DECISIONS.md 75), set all three of these or none. Without them the worker still runs, and a documents job fails saying what to set.

| Variable | Value |
| --- | --- |
| `HC_DRIVE_SA_EMAIL` | The Google service account's email. It needs the Google Drive API enabled in its Cloud project |
| `HC_DRIVE_SA_KEY` | Its private key: the `private_key` from the account's JSON key file, with `\n` for line breaks |
| `HC_DRIVE_FOLDER_ID` | The top "Health checks" folder: the last part of its address in Drive. It must be in a shared drive the service account is a Content manager of, because a service account has no Drive storage of its own |
| `HC_DRIVE_IMPERSONATE` | Optional. Only if the folder is in someone's My Drive instead: the user the account acts as, which needs domain-wide delegation with the `https://www.googleapis.com/auth/drive` scope |

For Claude's drafts of the report sections (DECISIONS.md 76). Without it the worker still runs, and the sections are the engine's own text.

| Variable | Value |
| --- | --- |
| `ANTHROPIC_FEDERATION_RULE_ID`, `ANTHROPIC_ORGANIZATION_ID`, `ANTHROPIC_SERVICE_ACCOUNT_ID`, `ANTHROPIC_WORKSPACE_ID` | How the worker signs in to Claude, which drafts the report sections: Render's OIDC, with no key on the service (render.com/docs/oidc). Render then sets `ANTHROPIC_IDENTITY_TOKEN_FILE` itself; don't set it by hand. The Anthropic side needs a Custom OIDC issuer `https://oidc.render.com/<the tea- workspace id>` (OIDC Discovery) and a federation rule for this service with audience `api.anthropic.com` and a token lifetime of at least 30 minutes (60 is better; see Recycling above). The worker keeps the access token it gets in `/var/data/claude-sign-in`, readable by the worker only (DECISIONS.md 667). Set up this way on 2026-11-05 |
| `ANTHROPIC_API_KEY` | Not with OIDC: a key on the service wins over federation, as it does in the SDK. Use it only instead of the four above |

For Claude's summaries of each health check's Apex classes and triggers (DECISIONS.md 490 to 492). Leave it unset until the privacy notice says Apex code is sent to Claude to write the documentation.

| Variable | Value |
| --- | --- |
| `HC_HANDBOOK_SUMMARIES` | `on`, exactly, to turn them on: the Apex step keeps the redacted code in the run folder until Claude has summarised it, and then deletes it. Anything else, or unset, is off. It needs Claude set up as above: without Claude no code is kept. The log says at every start which of the three it is: off, on, or on with Claude not set up. Kept code is deleted after seven days by the daily retention sweep even when `HC_CLEANUP_DRY_RUN` is 1: the dry run doesn't cover it |

Keep `HC_TOKEN_KEY` somewhere safe as well. Without it the stored tokens can't be opened, and every customer would have to connect again.

When it starts, the log says `Taking jobs from health-check-jobs, 2 at a time.`, then whether Claude drafts the sections. If a variable is missing it stops and names it.

## 3. Turn the console on, in Vercel

Production environment only:

| Variable | Value |
| --- | --- |
| `HC_REPOSITORY` | `production`. Runs are kept in the site's database instead of the sample data. Refused on previews and laptops |
| `HC_JOBS` | `queue` |
| `HC_CONNECT_KEY` | The same value as the worker's |
| `HC_SF_CLIENT_ID` | The same as the worker's |
| `HC_SF_REDIRECT_URI` | The same as the worker's, so the two always agree |

The console uses the internal tools' Google sign-in at `/internal/login` (blackcloud-next `lib/internal-login.ts`), so it needs nothing of its own. `SCRAPE_ALLOWED_EMAILS` must list everyone who may sign in with Google: an empty list now refuses everyone. `HEALTH_CHECK_ALLOWED_EMAILS` is optional, to narrow the console to some of them.

Redeploy after setting them. The sample runs disappear from the live console, which then shows only real runs.

## 4. Check it

1. In the console, create an OAuth run against a scratch org and make a connect link.
2. Open it in a private window and sign in as that org's read-only user.
3. The worker's log shows `connected: Connected "<name>" read only`.
4. Confirm the calibration and press Start run. The progress bar moves as each step finishes.
5. Approve every section and release. The Release tab shows the documents being made, then links to the folder and the three files in Drive. If Chromium can't start on Render's native runtime (the error names a missing library), the worker has to move to a Docker image with Playwright's system dependencies.

## Turning it off

Set `HC_JOBS` to `off` in Vercel and stop the Render service. Queued jobs wait in Redis. To revoke a stored connection, run `node src/cli/hc.ts connect revoke --connection <name>` in the worker's shell, which has the same variables. It revokes the tokens at Salesforce and deletes them from the disk.
