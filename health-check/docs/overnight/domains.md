# Worker A: Domains log

Branch `hc/domains`. Newest status first in each section. Nobody has run any of this against a real org.

## Status

| # | Item | State |
| --- | --- | --- |
| 1 | Security and access | Done |
| 2 | Best practice | Done |
| 3 | Current pipeline | Done |
| 4 | Forecast | Done |
| 5 | Activity | Done |
| 6 | Lead funnel | Done |
| 7 | Revenue opportunities | In progress |
| 8 | Scoring | Not started |
| 9 | Value sizing | Not started |
| 10 | Report scanning | Not started |
| - | Org time zone | Not started |
| - | SOAP `INVALID_SESSION_ID` refresh | Not started |

## How facts were checked

`developer.salesforce.com` answers direct fetches with a 403. I didn't try to get around that. Facts come from the snippets WebSearch returns from those pages, which are partial and sometimes from older API versions. So "verified" below means "seen in a Salesforce docs snippet", not "read in full" and not "proven on an org".

## Merges

- Merged `hc/integration` (19dc9ec) before starting, as the coordinator asked. Fast-forward, no conflicts. New pipeline work uses `fiscalStartOf(ctx)` and treats a deal with no recorded open stage as win rate only.

## Decisions

- **Sharing defaults sit on the `org` step**, not a separate read. A second `FROM Organization` query failed a run test that proves the org step isn't repeated on resume. Adding four fields to the existing query also saves a call. `org` is now version 2.
- **`OauthToken` is not read.** It has `RequestToken`, which holds an authorisation code, and "secrets stay out". Personal integration accounts come from `LoginHistory.ApiType` instead, which we were already paying for.
- **Holders of Modify All Data are worked out from both the profile and the assignments**, so the rule doesn't depend on whether profile-owned sets appear in `PermissionSetAssignment`.

- **Best practice holds only what the `aut.*` rules don't already say.** Workflow, Process Builder, coverage and trigger counts already have rules and already quote Salesforce, so repeating them would double-count in scoring. Four new rules instead, each with a `Practice` record: position, source, date checked, and whether the source is Salesforce's own. Retired API versions was dropped: the retirement is about API calls, and I couldn't confirm any effect on stored Apex.

- **Next step text is never fetched.** The extractor filters on `NextStep != null` and selects only `Id`. A test greps every checkpoint and output for the planted text.
- **Age in stage is judged against the org's own won deals** (75th percentile of time in that stage), not a fixed number of days. A stage with fewer won deals than `minSample` has no line, and its deals are excluded and declared.
- **Coverage only counts quotas held by people who own deals.** A manager's quota repeats the team's. This is simple and could undercount a manager who also sells; the assumption is on the finding.
- **Coverage targets:** only `forecasting_quota` is read. `custom_object` and `spreadsheet` are not measurable, because the calibration has no amounts and those sources aren't read.
- **The synthetic calibration now says `forecasting_quota`,** because the synthetic org now has quotas.
- **The account token restores to the record id.** We don't read account names, so a consultant looks the id up.

- **Forecast accuracy is deliberately not measurable.** `ForecastingItem` shows the forecast as it stands, and nothing I could find says it keeps snapshots. Comparing today's rows with results would flatter the org. It would need a snapshot taken each period, which is a product decision.
- **Won and Commit against quota is worked out from deals, not forecast roll-ups,** so it doesn't depend on how categories are rolled up.
- **"In use" needs an active revenue type and forecast rows for today.** Anything else makes every forecast rule and metric not measurable, and tests prove all four ways of being off give no figure at all.
- **A renamed Commit category is not measurable.** Admins can rename categories, so finding no Commit deals can't be read as nothing at risk.

- **Activity reads 12 months, on purpose.** A plain query drops activities archived after 365 days, and reaching them needs `queryAll`, which isn't on the allowlist. I didn't widen it. Won and lost comparisons only use deals created inside the readable year, because an older deal has lost its early activity and would read as a deal nobody worked. Worth deciding later whether 12 months is enough.
- **The unlinked event trap was built first,** as the plan asked. Total events are flat at 60 a month, and the linked share falls from 74% to 6% from July. The rule compares three recent whole months with the six before, overall and by team and rep, and a test asserts the planted total really is flat.
- **Activity and lead funnel metrics are not measurable when Opportunity can't be read, even the ones that don't strictly need it.** This is a deliberate gate, and the reason is a test rather than the data. `run.test.ts` requires every metric to be not measurable in that case, which was true when all metrics were pipeline metrics. `activity.event_linked_share`, `activity.*`, `lead.conversion_rate` and `lead.median_response_hours` could be calculated without deals, but rule 8 says never weaken a test to get a pass, so I treated the test as the spec. It costs nothing in a normal run, and only loses those figures when the pipeline report has already failed. **Coordinator or Coby:** if you'd rather publish them, that test needs a named exception, and the gate is one line in each module. The linked shares are also in the finding's `facts`, so the rule still reports them.
- **A truncated activity read is not measurable,** not a smaller sample. Keyset order isn't random, so the rows we'd keep would be biased. The cap is 200,000 rows per object, about 100 calls.

- **First response is the earliest task, event or status change after creation,** on the lead or the contact it converted to. Conversion moves activity to the contact, so a lead id alone would call every converted lead unanswered, and there's a trap for that. Unlogged calls would make this look slower than it is, so confidence is medium.
- **Leads, status changes and campaign members use `readCapped`,** now shared with activity. Lead text fields, campaign names and status values are never selected.
- **Campaign return credits a deal in full to every campaign its lead was in,** and only counts leads, not influence on existing contacts. Both are stated as assumptions, and confidence is medium. A campaign with no cost is left out and declared.

## Unverified Salesforce facts

| Rule | What I couldn't verify | What I did |
| --- | --- | --- |
| `sec.health-check-*` | `RiskType` values beyond HIGH_RISK, MEDIUM_RISK, MEETS_STANDARD. Which permission reads these objects: newer docs say View Setup and Configuration, older ones Modify All Data | Other values are excluded and declared. A failed read is not measurable |
| `sec.health-check-score` | Salesforce's own score bands | 80 and 60 are stated as our working limits |
| `sec.personal-integration-accounts` | Whether the Salesforce mobile app or an email add-in sets `ApiType` | Confidence is medium, and the recommendation is investigate |
| `sec.personal-integration-accounts` | A connected user without Manage Users or Monitor Login History gets only their own rows, and that looks like success | Stated on the evidence. Needs checking at the first live run |
| `sec.sharing-defaults` | The full value list for `Default*Access` | Only `Edit` and `ReadEditTransfer` count as open. Anything else stays quiet |
| `bp.jobs-on-inactive-owners` | Only vendor and community sources say a job on a deactivated user fails | Marked secondary, so confidence is medium and the summary says so |
| `bp.after-save-same-record-updates` | A flow may need after-save for other work, and a lookup with no filters is read as same-record | Severity is low, and the recommendation keeps the rest of the flow in place |
| `cur.missing-next-steps` | That `NextStep` can be filtered in SOQL. It's a 255-character string, which normally can | If it can't, the read fails and the rule is not measurable |
| `cur.coverage` | `ForecastingType` and `ForecastingQuota` field names come partly from a Data Cloud mapping page. `OpportunityRevenue` as the standard name is from memory, and is only a tie-break when several revenue types are active | A wrong field fails the read, which is not measurable. Confidence is medium |
| `fct.*` | `ForecastCategoryName` on `ForecastingItem`, and `Period` field names, are from memory. Whether `ForecastCategoryName` returns the API value or a renamed label | A wrong field fails the read, which is not measurable. A renamed Commit is not measurable |
| `fct.forecast-accuracy` | That these objects keep no history. Salesforce's own Forecast snapshots may be a separate feature I can't see | Reported as not measurable, which is the safe side |
| `act.*` | That Einstein Activity Capture email isn't in `Task`, `Event` or `EmailMessage`. Sources are vendors selling replacements, and a Summer '25 option may change it | Where capture is in use or unknown, rep-level findings drop to medium confidence and metrics state that counts are a floor |
| `act.*` | That `IsChild` on `Event` marks invitee copies, and that `StartDateTime` can be filtered | A wrong field fails the read, which is not measurable |
| `lead.response-time` | That conversion moves a lead's tasks to the contact: the source is a community answer, not Salesforce's docs | We count both ids, so if it's wrong the answer is the same |
| `lead.*` | That `LeadHistory.Field` can be filtered, and that `Status` is the stored value. If field history isn't on, rows are empty | Empty or unreadable history means tasks and events only, and unreadable is declared in the exclusions |
| `lead.campaign-return` | `Campaign.ActualCost` and `CampaignMember.LeadId` are from memory plus a partial docs snippet | A wrong field fails the read, which is not measurable |
