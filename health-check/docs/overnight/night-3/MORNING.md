# Morning hand-over, 7 November

Nothing was pushed, deployed or applied to production, and no Salesforce org was contacted. Tonight's work is committed locally, on the engine's `health-check` branch and on the console's release branch `hc/release-11`. The development database got one new table.

Tonight wasn't about new features. The engine had got well ahead of anything tested against a real org, so the night went on making what's there trustworthy:

- **Cross-review:** each session reviewed the other's code.
- **Scale and memory:** a 500-user test org, and cutting the worker's memory.
- **Reading pass:** every page a customer sees, read as their finance director would.

## Do first, in this order

1. **Add the facts table to production:**
   ```
   cd "/private/tmp/claude-501/-Users-cobypress-Desktop-Black-Cloud/6d2ad3dd-3f8a-48ab-b64f-3c5d11535480/scratchpad" && NEON_URL="$(neon connection-string production --project-id red-darkness-18425884)" node apply-migration.cjs 20261120_health_check_run_facts.sql HealthCheckRunFacts runId checkedAt facts
   ```
   It should end with "3 after".
2. **Release the console (release 11):**
   ```
   cd "/Users/cobypress/Desktop/Black Cloud/worktrees/hc-release" && git push origin hc/release-11:main
   ```
3. **In Render, on the worker:**
   - add `NODE_OPTIONS` = `--max-old-space-size=1536`;
   - tell me how much memory its plan has. A 500-user run peaks at about 1.8 GB; 2 GB works, and 3 GB is safer because printing the documents starts its own browser.
4. **Push the worker:**
   ```
   cd "/Users/cobypress/Desktop/Black Cloud/barry-platlform" && git push origin health-check
   ```
   Do this after step 3, so the new worker starts with the memory cap.

Then the four from yesterday:

- **The second-org test.** See `DEV-EDITION-TEST.md` in this folder: about 30 minutes, plus the scan. The run page now shows what the facts step found.
- **The two Google Drive settings:** the worker's service account needs Manager on the shared drive, and that drive must allow sharing outside Black Cloud.
- **The legal check** of the scan privacy notice, now with the install step and the partner line.
- **Partner details:** every consultant can see them. Should that be narrower?

## What changed overnight

### Found and fixed by the reviews

Every fix had a test that failed first.

**Money:**
- An integration seat that was also unused was counted twice in licence waste.
- The executive summary's money table had no total, and its "smaller lines" row had no amount, so it didn't add up on CPQ reports. It now lists every line once, with the total.
- "£2.79m at risk" was really the pipeline needed for 3x coverage. It's out of the money figures, and now reads "you'd need £2.79m more pipeline".
- A campaign figure called "spend" was really the shortfall.
- "What matters most" now ranks issues by the money each actually adds to the total.

**Wrong conclusions:**
- Seven checks passed on reads cut off at a row limit. For example, "4,850 of 50,000 milestones breached" when it was about 94,000.
- A user the login log showed signing in was counted as inactive.
- CPQ called draft quotes "never approved".
- A Pardot check drew a conclusion without breaking it down by lead source.
- Flows that run on their own, triggered by records or a schedule, were counted as actions an AI agent could call.

**Drive and access:**
- After 12 months, retention took a customer's folder access away, but the next share job gave it back.
- A share whose answer was lost left that person with the folder for good.
- The documents card hid people who still had access.

**The worker:**
- A run killed twice, for example by two deploys during a run, sat at "analysing" for ever. It's now recorded as failed, with the reason shown.
- Save files weren't safe against a crash mid-write.

### Scale and memory

On a synthetic 500-user org (200,000 deals, a million activities, 50,000 cases, 130,000 quotes):

- **API calls:** 1,629, about 0.3% of a typical daily allowance. A run takes about 20 minutes.
- **Memory:** peaks at 1.8 GB, down from 5.8 to 7 GB, with every finding identical.
- **Coverage:** only 2 checks are left "not measurable", down from 17. Those two (dashboard use and forecast accuracy) can't be measured from Salesforce data.
- **Job gate:** the worker now runs one big job at a time, so a customer's sign-in never waits behind a 25-minute run.
- **Size limit:** above about 700 users, the biggest reads become partial and say so.

### The reading pass

Every customer page was rendered and read as a finance director and a sales director would.

- **Fixed:**
  - garbled and machine-sounding sentences;
  - Salesforce field names such as `SBQQ__Discount__c`, now plain labels;
  - whole deals rather than "31.4 deals";
  - page numbers on the contents page;
  - readable appendix type;
  - jump links on the microsite's issues page;
  - quick wins as their own short list in the first 30 days;
  - a one-pager that only showed flattering benchmarks.
- **Report sizes:** Sales Cloud 120 slides, CPQ 129, all three modules 144, Essentials 63, and Essentials with CPQ 66. Every density step of every sample passes a stricter layout check, which now measures every element on a page.

## For you to decide

1. **Named reps on the microsite** when the customer consents at intake. Today it's always team-level.
2. **The product's names.** It appears as "Barometer Scan", "full Barometer check" and "Salesforce health check".
   - My recommendation: Barometer Scan, Barometer Essentials and Barometer Complete, with "Salesforce health check" as the description.
3. **The deal outlook** starts from today's date. If you'd rather it started from the first full month, that's one line, and it changes the £1.3m figure.
4. **Render's shutdown wait.** A deploy during a run makes it wait about 10 minutes before resuming. If Render lets the worker's shutdown wait be longer, that would avoid it.

## Where everything is

| What | Where |
| --- | --- |
| Engine | `barry-platlform` `health-check`, unpushed, 2,141 tests pass (2,014 → 2,141 tonight), five samples and the 500-user run pass every layout and number check |
| Console | `blackcloud-next` `hc/release-11` (= `health-check` with tonight's console work), 1,286 unit tests and 410 browser tests pass |
| Development database | `HealthCheckRunFacts` added |
| Session logs | this folder: `review-*.md`, `reading-pass*.md`, `scale.md`, `domain-wording.md`, `card-overflow.md` and others |
| Decisions | `docs/DECISIONS.md` rows 301 to 418 |
