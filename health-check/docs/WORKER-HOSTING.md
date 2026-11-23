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

Why is not known. The leading guess is that the new process presented the same Render identity token the first one had already exchanged, and Anthropic takes a token with a single-use id (`jti`) once only. But nobody has confirmed that Render's tokens carry a `jti` at all, nor that Render hadn't rewritten the file in those 12 minutes. Other causes fit too: a signing key Render had just rotated, a clock difference beyond the 30-second allowance Anthropic's WIF reference gives ("JWT verification", platform.claude.com/docs/en/manage-claude/wif-reference), a token living longer than the issuer's maximum, or a file read while Render was writing it.

Since DECISIONS.md 667 the worker keeps its Claude access token on the disk (`/var/data/claude-sign-in`, a folder only the worker can read), so a process that starts again uses it instead of exchanging again, whatever the cause. That only works while the token lasts: with the 10-minute rule lifetime the Console's Connect workload wizard fills in (its docs say it "prefills … token_lifetime_seconds=600", platform.claude.com/docs/en/manage-claude/workload-identity-federation, "Set up federation"; the API's own default is 60 minutes), the token from 08:11 would have run out before 08:23. It also hides the cause, which still matters for a worker that never restarts. The same applies to any restart in place, a crash included. At every start the log says whether the token can be kept ("Claude's access token is kept on the disk …" or "… can't be kept on the disk …").

Leave it unset until both of these are done:

1. In the Claude Console, open Workload identity, then Authentication history, and find the failed attempt at 08:23:55 UTC on 22 Nov 2026. Note its reason and tell the engine's owner. `jti_reused` would confirm the guess above; anything else (an expired token, a signature, a subject or audience that doesn't match) means the cause is something else, and it may also stop a worker that never restarts. While there, open the federation rule and check its token lifetime: set it to 60 minutes if it is less.
2. Deploy this version and look at its first lines: "Claude by OIDC federation: The identity token was issued … and expires in …; it carries …" says whether Render's token is single-use, and "Claude's access token is kept on the disk …" that the disk is there. Then set `HC_WORKER_RECYCLE_AT` to `100` and run one health check. After "Recycled: …", the fresh worker's log should say "Claude sign-in works: …", and the authentication history should show no new exchange at that time. If it does, set it to `50000`. If the log says "Claude sign-in failed at start", unset it and tell the engine's owner; the worker keeps trying for an hour and says "Claude sign-in works again" if it recovers.

**Claude on an ordinary day.** A worker that never restarts also exchanges Render's token again each time its access token runs out, and that only works if Render has written a new token by then. Nobody has yet seen it go past its first refresh. The log now says at start how old Render's token is, how long it has left and whether it is single-use, and says once if Claude's sign-in stops working ("Claude sign-in stopped working …") and once when it works again. After a deploy, look for either line over the next few hours. If "stopped working" appears, check the authentication history's reason for it and tell the engine's owner. Keeping the federation rule's token lifetime at 60 minutes, rather than the wizard's 10 minutes, means fewer exchanges, so fewer chances to meet a token Render hasn't rewritten yet.

### PMD for the Apex code (off until installed)

Each health check runs PMD over the org's Apex classes and triggers beside our own scan, and stores where each finding is and which rule it broke, never a line of the code (DECISIONS.md 670). Nothing a customer sees uses it yet. PMD is Java, and Render's Node runtime has no Java, so until both are installed every health check records "PMD isn't installed on this worker." and makes no extra Salesforce call. With them, each health check spends a few more calls reading the Apex code again (one per page of classes and of triggers).

To install it, add this to the end of the build command (one line, after `npx playwright install --only-shell chromium`). It downloads PMD 7.29.0 and a Java 21 runtime into `health-check/.tools`, and stops the build if either file isn't the one we checked:

```
&& mkdir -p .tools && cd .tools && curl -fsSL -o pmd.zip https://github.com/pmd/pmd/releases/download/pmd_releases%2F7.29.0/pmd-dist-7.29.0-bin.zip && echo "d27177c185b837fa57f097e482d1253468aa035379c5eb2671fa0a279109b179  pmd.zip" | sha256sum -c - && unzip -q pmd.zip && rm pmd.zip && curl -fsSL -o jre.tar.gz https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.13%2B9/OpenJDK21U-jre_x64_linux_hotspot_21.0.13_9.tar.gz && echo "a535a1df9c6284d9618872984e026ae0352d1f7e9e0605425c0e6114f47351dd  jre.tar.gz" | sha256sum -c - && mkdir -p jre && tar -xzf jre.tar.gz -C jre --strip-components=1 && rm jre.tar.gz && cd ..
```

Then set these two variables. They assume Render builds and runs the service in `/opt/render/project/src`, which is what we understand of its native runtime but haven't seen on this service: check it in Render's shell (`pwd` in the service's root directory) before setting them. With the root directory `health-check`:

| Variable | Value |
| --- | --- |
| `HC_PMD_HOME` | `/opt/render/project/src/health-check/.tools/pmd-bin-7.29.0` |
| `HC_JAVA_HOME` | `/opt/render/project/src/health-check/.tools/jre` |

Check: after the deploy, the worker's first lines say "PMD 7.29.0 checks each health check's Apex code, and its findings are stored beside our own scan's." If they say PMD isn't installed, or isn't version 7.29.0, or that Java isn't there, the reason is in the line: check the paths with Render's shell (`ls $HC_PMD_HOME/lib/pmd-apex-7.29.0.jar $HC_JAVA_HOME/bin/java`). If the build stops at `unzip: command not found`, use `python3 -m zipfile -e pmd.zip .` in its place. Then run one health check on a scratch org: its run folder's `checkpoints/code-lint.json` should say PMD "ran".

What it costs: about 140 MB of PMD and 50 MB of Java on the build. While a run's PMD works, memory goes up by well more than its heap: Java's heap is capped at 384 MB and its class metadata at 128 MB, and Java needs code cache, thread stacks and its own housekeeping on top, so plan for about 650 to 700 MB more for up to 5 minutes (on a small test it used about 370 MB). On the 2 GB instance beside a large run's data that may be tight; if the system kills PMD for memory, the run records PMD as not run, "stopped by the system", and carries on. PMD gets the code in a private temporary folder deleted as soon as it finishes, and also if the worker exits or is stopped mid-run (its own shell deletes it within a second if the worker is killed outright, and every worker start sweeps any left older than 10 minutes). It gets none of the worker's variables, and no network on purpose (its connections are pointed at a closed port, a best effort rather than a firewall). Java writes no performance file to `/tmp`.

What was checked, and what wasn't. The PMD checksum is the one we computed from the file we downloaded and ran. The Java checksum is the one Adoptium publishes for that file: we did not download that file, so it has not been checked against a download. PMD 7.29.0 itself was run by hand on 23 Nov 2026, on macOS with Oracle Java 1.8.0_503 (the only Java on that machine; the Render steps install Java 21, which no one has run PMD on yet): `pmd check --dir <folder> --rulesets src/extract/lint/pmd-apex.xml --format json --report-file <file> --no-cache --no-progress --show-suppressed --no-fail-on-violation --no-fail-on-error`, then through the engine's own `runPmd` (which adds `--threads 1`, its wrapper shell and `PMD_JAVA_OPTS` of `-Xmx384m -XX:MaxMetaspaceSize=128m -XX:+UseSerialGC -XX:-UsePerfData` and the proxy settings) over the test fixtures. It accepted the ruleset with no configuration error; where the tests' stand-in and real PMD both report, they agree on rule, line and column, and real PMD found a few more that the stand-in doesn't imitate. To move to another PMD version, the engine has to change first (it runs only the version it names), so tell the engine's owner rather than editing the build.

To turn it off, unset `HC_PMD_HOME`.

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
