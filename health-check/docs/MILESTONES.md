# Milestones

Status after the overnight build: Milestones 3 and 4 are built on `hc/integration`, and the Milestone 5 front end is on `hc/console`, all on synthetic data. See `docs/overnight/MORNING.md`. Milestone 2's connection work is on `hc/connection` (below). Figures in the Milestone 1 section are from when it finished.

## Milestone 1: done, not yet proven on a live org

### What's done

- Zod schemas for everything in the brief, plus the review console's proposal, override, release and baseline records.
- A read-only Salesforce client with a route allowlist, an API budget, resumable paging and a `ConnectionProvider` interface. There's a development provider for the Salesforce CLI, which only uses approved aliases.
- Extractors for configuration, automation, users and licences, and Opportunity with OpportunityHistory.
- 34 rules: 6 configuration, 8 automation, 8 utilisation, 9 data quality and 3 pipeline, with guards for removing users and fields.
- 12 pipeline metrics from OpportunityHistory, with stage meaning taken only from the calibration file.
- 2 manual steps: Einstein Activity Capture advanced sync settings, and the Default Workflow User.
- `hc run`, which writes `findings.json`, `metrics.json`, `manual-steps.json` and `raw-notes.md`.
- A synthetic org with planted traps, recordings captured through the real client, and replay that refuses unrecorded requests.

### How it was checked

| Check | Result |
| --- | --- |
| `npm test` | 421 tests, all passing. No org needed |
| `npm run typecheck` | Clean |
| Mutation test: 47 deliberate breakages of safety controls and maths | All 47 caught by the tests |
| Real `hc` command over HTTPS to a local simulator | Finished, with GET and allowed POST requests only, and no token in any output |
| Pipeline metrics | Checked against a small data set worked out by hand |
| Independent code review | Found 13 defects, 4 of them high. 12 fixed with tests, and 1 disclosed. See below |

### Review fixes

A separate agent reviewed the read-only boundary and the pipeline maths without seeing how they were built. It found no way for the code to send a write, but it found real gaps, and passing tests had missed all of them.

- **Token could go to the wrong host:** the instance URL was prefix-checked, so `https://x.salesforce.com@evil.example` passed. It's now parsed, on connect and refresh.
- **Old deals closed in the window were never extracted**, which inflated win rate and undercounted past pipeline.
- **Deals created as Closed Won counted as reaching every stage**, which inflated conversion. They also gave 0-day sales cycles.
- **The fiscal year defaulted to January** and was never compared with the org.
- **Smaller boundary gaps:** a repeated `q`, raw tabs, `LIMIT 1FOR VIEW`, a case-sensitive override check, `nextRecordsUrl` reaching any allowed route, and a parse error that could quote the token.
- **Smaller pipeline gaps:** history gaps were judged across all months rather than month by month, blank amounts were unchecked at month ends and in value segments, skipped-stage figures ignored the minimum sample, and same-second rows could reverse.

The one not fixed at the time was the UTC boundary. The org's time zone is now applied, and only the analysis window's edge is still set in UTC.

### What hasn't been checked

**`hc` has never run against a real org.** The alias wasn't given, so that step was skipped as the brief asked. The fixtures are built from Salesforce's documented formats and the prototype's queries, not from recordings of a real org. Expect some queries to need adjusting on first contact. QUESTIONS.md lists the most likely ones.

### What's risky

| Risk | Detail | Response |
| --- | --- | --- |
| Real responses differ from fixtures | Every Salesforce shape here is from docs, or the prototype's use of it | First live run on the Black Cloud org, before any more domains rely on them |
| A rule is confidently wrong | Thresholds are our judgement. Only a real org shows false positives | Review the first live run finding by finding |
| Large orgs | REST paging only, and `LoginHistory` is capped at 20,000 rows. Ownership scans top out at 40 custom objects, which then blocks removal | Bulk API decision in Milestone 3 |
| Chat scrubbing misses first names | Covered for every person the run knows (its org users) and the customer's company name: first names, surnames, possessives, hyphenated, apostrophe and accented names, shared first names, common diminutives, and names that are also words (masked with a capital). A last check refuses any prompt with a name part left. Still not covered: nicknames beyond a short list, misspellings, camel case, lookalike letters, contacts and leads (not read), and screenshots. See ARCHITECTURE.md, "Free text and names" | Milestone 5 chat must go through `sendPrompt`. Add contacts and leads to the pseudonymiser if a run ever reads them |
| Time zones | The org's time zone is applied, but the analysis window's edge is set in UTC in `run.ts`, so it can differ by a day | Small, and stated. Change it when `run.ts` is next open |
| One person can run and approve | Noted in the plan | Sign-off checklist in Milestone 5 |
| Customer names in the plan document | `blackcloud-next/docs/health-check/Salesforce Health Check Product Plan.md` names a real customer and its figures, and is untracked | Remove them before that file is committed |

## Milestone 2: proposed plan

Prove the connection, because it's the part most likely to change the design. It needs Coby's Salesforce setup, so it can't be done unattended.

1. Run `hc` read-only on the Black Cloud org, then fix what real responses show. Replace assumptions in QUESTIONS.md with facts.
2. Create an External Client App in a Developer Edition org, and try connecting it to a separate test org.
3. Establish whether a managed connector package is needed for customer orgs, and record the answer in DECISIONS.md.
4. Build the web server flow with PKCE and refresh token rotation as a second `ConnectionProvider`, with encrypted token storage and revocation.
5. Build the one-time connect link for customer admins.
6. Build the read-only permission set, and test that it really can't write.
7. Decide on the Bulk API for large orgs.

**Exit test:** a Black Cloud consultant connects a test org through the link, and the run completes on a token that can't write.

## Milestone 2: the connection, as built on 2026-11-03

Built on `hc/connection` against `bc-hc-dev`, a Developer Edition org made for this. No other org was touched.

| Plan item | Status |
| --- | --- |
| 1. First live run on the Black Cloud org | Not done: out of scope here. The run below is the first live run, on `bc-hc-dev` |
| 2. External Client App, and a second test org | App built and deployed. Not tried from a second org (QUESTIONS.md 22) |
| 3. Managed package needed? | Answered yes, from Salesforce's docs (DECISIONS.md 65) |
| 4. Web server flow with PKCE and rotation | Done live on `bc-hc-dev` on 2026-11-03. Rotation proven on `bc-hc-scratch-web` on 2026-11-04 (below) |
| 5. One-time connect link | Link logic built and tested. Console pages are Milestone 5 |
| 6. Read-only permission set, proven unable to write | Done, proven on `bc-hc-dev` |
| 7. Bulk API decision | Not started |

### What's in `bc-hc-dev`

- **Black Cloud Barometer** External Client App: web server flow, PKCE and secret required, refresh token rotation, `api` and `refresh_token` only (DECISIONS.md 58). Salesforce accepts its authorisation request: a plain GET of the URL `hc connect start` builds goes to the login page, and a wrong callback or client id gets a 400.
- **Barometer Dev JWT**, a development-only app for JWT bearer, pre-authorised by the permission set (DECISIONS.md 61).
- **Barometer Read Only** permission set (DECISIONS.md 59).
- **Test Barometer Read Only** (`barometer-read-only@black-cloud.com.bc-health-check`, user `005d3000005ZjKLAA0`): Salesforce Integration licence, "Minimum Access - API Only Integrations" profile, the Salesforce API Integration permission set licence, and Barometer Read Only. Nothing else.

### Write test

`node scripts/write-test.ts`, run as the read-only user on a token from the dev JWT app. It's a separate script with its own `fetch`, because the engine has no way to send a write. The admin made one test Account first and deleted it afterwards.

| Attempt | HTTP | Salesforce's answer |
| --- | --- | --- |
| Read the test Account (control) | 200 | Visible |
| Create Account | 400 | CANNOT_INSERT_UPDATE_ACTIVATE_ENTITY: entity type cannot be inserted |
| Update Account | 400 | CANNOT_INSERT_UPDATE_ACTIVATE_ENTITY: entity type cannot be updated |
| Update Account with `?_HttpMethod=PATCH` | 400 | The same |
| Delete Account | 400 | INSUFFICIENT_ACCESS_OR_READONLY |
| Create Task on the Account | 400 | CANNOT_INSERT_UPDATE_ACTIVATE_ENTITY: entity type cannot be inserted |
| Create Opportunity | 400 | The same |
| Composite create Account | 200 | Subrequest refused: entity type cannot be inserted |
| Anonymous Apex update | 200 | `success: false`: Field is not writeable: Account.Name |
| Metadata API deploy (check only) | 500 | INSUFFICIENT_ACCESS: needs Modify All Data or Modify Metadata |

**Result: passed.** 9 of 9 writes refused, the Account unchanged, and no stray records. The connect-time check (DECISIONS.md 64) agrees from the org's own records: no create, edit or delete on any object, and no write system permission.

### Exit test, as far as it goes without a person

`hc connect jwt` connected the read-only user, then `HC_APPROVED_ORG_ALIASES=bc-hc-dev-read-only hc run --connection bc-hc-dev-read-only` ran through `OAuthConnectionProvider`, with tokens encrypted at rest and the key in the macOS keychain.

- **The run completed:** 31 steps, 10 issues, 25 passes, 41 not measurable. No token appears in any output.
- **Refresh works live:** with the stored access token replaced by a dead one, the client got a 401, the provider signed a new assertion, stored the new token and carried on.
- **What read-only couldn't see:** the Metadata API retrieve (automation and report definitions) and Tooling Apex (DECISIONS.md 60). Forecasting and Einstein Activity Capture aren't enabled in `bc-hc-dev`.
- **Everything else not measurable is the empty dev org.** It has no Opportunities, Leads or Apex.

### Web server flow, live

On 2026-11-03 Coby followed the steps below, through to revoking the connection.

- **The sign-in worked for the API-only user.** It used a private browser window, the Integration licence user, and the consent screen. That answers the old QUESTIONS.md 23.
- **`connect finish` exchanged the code with PKCE**, stored the tokens encrypted, and the read-only check passed.
- **The run completed** on that connection: run `hc-2026-11-03-cb98209a`, 32 steps, 10 issues, 25 passes and 41 not measurable. That's the same as the JWT run.
- **`connect revoke`** revoked the token at Salesforce and deleted the stored copy.

**Not exercised live:** refresh with rotation. That needs the session to time out twice before revoking, so it can only be done on a later connection. The unit tests cover it, and the JWT connection's refresh works live.

### The package, from a second org

Built on 2026-11-03 (DECISIONS.md 66):

- **The app is defined in BlackCloudProd.** Nobody there can use it.
- **Package version `0.1.0-1` (`04tQH00000NOKUzYAP`) is a beta.** Betas install only in scratch orgs and sandboxes.
- **`bc-hc-scratch` is a 7-day scratch org from the BlackCloudProd Dev Hub.** It has the package installed and a test user on the Integration licence, `barometer-read-only@black-cloud.com.bc-hc-scratch`. That user holds the Salesforce API Integration permission set licence and `blackcloud__Barometer_Read_Only`, and nothing else. The password is in `~/.barometer/dev/scratch-read-only-user-password.txt`.
- **The installed app has Salesforce's default policies.** They let any user self-authorise, with refresh tokens that last 365 days. Our install steps for customers will change both.

To connect from the scratch org:

1. In BlackCloudProd, open Setup, then External Client App Manager, then Black Cloud Barometer. On Settings, under OAuth Settings, click Consumer Key and Secret, and enter the code Salesforce emails. In `~/.barometer/oauth-client.json`, replace `clientId` and `clientSecret` with these. Leave `redirectUri` as it is. The `bc-hc-dev` app's values are kept in `oauth-client.bc-hc-dev.json`.
2. `HC_APPROVED_ORG_ALIASES=bc-hc-scratch-web npm run hc -- connect start --connection bc-hc-scratch-web --login-url https://page-momentum-12712-dev-ed.scratch.my.salesforce.com`
3. Sign in as the scratch user in a private window, click Allow, and then run `connect finish` as before, with `--connection bc-hc-scratch-web`.
4. `HC_APPROVED_ORG_ALIASES=bc-hc-scratch-web npm run hc -- run --connection bc-hc-scratch-web --calibration ~/.barometer/dev/calibration-bc-hc-dev.json --out ~/.barometer/dev-runs`
5. Leave the connection in place for at least four hours, then run `hc connect check --connection bc-hc-scratch-web` twice, two hours apart. That tests rotation live. Then revoke it.

If step 3 answers "Cross-org OAuth flows are not supported", the link to the source org isn't working. One Trailblazer Community post reports that error for a packaged app.

**Result, 2026-11-03:**

- **The cross-org sign-in worked.** The test user signed in to `bc-hc-scratch` through the installed app, and the token came from BlackCloudProd's global OAuth settings. The packaged route works, so QUESTIONS.md 22, about the local app, no longer matters.
- **The packaged permission set arrived empty.** Managed packages don't carry user permissions or standard object permissions, and the install drops them without a warning (2GP guide, "Permission Sets and Profile Settings in Packages"). Barometer Read Only is made of nothing else, so `blackcloud__Barometer_Read_Only` grants nothing. The first `connect finish` stored the token, and the check then failed with `API_DISABLED_FOR_ORG`.
- **The test carried on with an unpackaged copy.** We deployed Barometer Read Only from source as a plain permission set and assigned it, as a customer's admin would. The read-only check then passed.
- **The run completed** on that connection: run `hc-2026-11-03-1660d472`, with 9 issues and 37 not measurable. The empty scratch org explains the difference from `bc-hc-dev`.
- **The first live refresh worked.** About two hours after the last use, `connect check` found the access token expired. It refreshed through the installed app and stored the new tokens, and the read-only check passed. A check at 17:29 hadn't refreshed: the session timeout counts from the last use, not from sign-in.
- **The second refresh worked, on 2026-11-04.** It could only succeed with the refresh token the first refresh stored, so rotation works live. Not tested: that Salesforce refuses the old refresh token afterwards, because our store had already replaced it.
- **The connection is no longer needed.** Revoke it with `hc connect revoke --connection bc-hc-scratch-web`.

**One install: version `0.1.0-7` (`04tQH00000NOKy1YAH`), the same day.** The package no longer carries the permission set. Its install script creates Barometer Read Only instead (DECISIONS.md 67).

- **Tested by installing in a fresh scratch org, `bc-hc-scratch-5`, which lasts 3 days.** The install created `Barometer_Read_Only` as an ordinary permission set, with the ten permissions and read, View All Records and View All Fields on the six objects. It has no create, edit or delete, and no Modify All Data or View All Data.
- **The version passed full validation**, with 92% code coverage.
- **Versions 2 to 6 were failed or diagnostic betas and are deleted from the Dev Hub.**
- **Not yet done on `0.1.0-7`:** a sign-in by a user who holds only the created set. The set is the same one `bc-hc-scratch` used for its run, but this exact install hasn't had a sign-in.

**Before a customer can install:**

1. A version has to be promoted (released), because betas only install in scratch orgs and sandboxes. Once released, the app can't be taken out of the package.
2. Customer install steps:
   - Install the package.
   - Create the integration user and assign it Barometer Read Only.
   - In the app's policies, choose "admin approved users are pre-authorised" with Barometer Read Only, and expire refresh tokens after 30 days of inactivity.
3. The console callback URL replaces `localhost` in BlackCloudProd's global OAuth settings (Milestone 5).

### What Coby needs to do for the web server flow

1. In `bc-hc-dev`, open Setup, then External Client App Manager, then Black Cloud Barometer. On Settings, under OAuth Settings, click Consumer Key and Secret, and enter the code Salesforce emails. Copy the secret into `~/.barometer/oauth-client.json` as `"clientSecret"`, next to the `clientId` and `redirectUri` already there. Keep the file at mode 600.
2. Give the read-only user a password. Done on 2026-11-03: it's in `~/.barometer/dev/read-only-user-password.txt`, at mode 600. It was set as the admin with anonymous Apex, `System.setPassword`. `sf org generate password --on-behalf-of` doesn't work here, because the sf CLI has no sign-in of its own for this user.
3. From `health-check`, run `HC_APPROVED_ORG_ALIASES=bc-hc-dev-web npm run hc -- connect start --connection bc-hc-dev-web --login-url https://bc-health-check-dev-ed.develop.my.salesforce.com`.
4. Open the URL it prints in a private browser window. Sign in as the read-only user, not as yourself. You should see Black Cloud Barometer asking for "Manage user data via APIs" and "Perform requests at any time". Click Allow.
5. The browser lands on a localhost page that won't load. Copy the whole address from the address bar within 10 minutes and run `HC_APPROVED_ORG_ALIASES=bc-hc-dev-web npm run hc -- connect finish --connection bc-hc-dev-web --callback-url '<the address>'`. It exchanges the code, stores the tokens, and checks the user can't write.
6. Run `HC_APPROVED_ORG_ALIASES=bc-hc-dev-web npm run hc -- run --connection bc-hc-dev-web --calibration ~/.barometer/dev/calibration-bc-hc-dev.json --out ~/.barometer/dev-runs`.
7. Finish with `HC_APPROVED_ORG_ALIASES=bc-hc-dev-web npm run hc -- connect revoke --connection bc-hc-dev-web`.

If step 4 refuses the sign-in because the user is API-only, that answers QUESTIONS.md 23. Record it there. Then repeat with a second test user on the Salesforce Platform licence (20 are free in `bc-hc-dev`), with the "Minimum Access - Salesforce" profile and only Barometer Read Only.

To exercise rotation live, run `hc connect check --connection bc-hc-dev-web` once the session has timed out (two hours by default), and again after the next timeout. Each check has to refresh, and the second only works if the first stored the rotated refresh token.
