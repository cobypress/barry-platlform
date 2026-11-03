# Questions

None of these blocked Milestone 1.

## For Coby

| # | Question | Why it matters | Needed by |
| --- | --- | --- | --- |
| 1 | Where is the Barry worker hosted, and can that host run Node 24 and Chromium? | The repo doesn't record it. This service needs both | Milestone 4 |
| 2 | Which regions do Neon, Redis, Vercel functions and the worker run in? | The data handling table assumes the UK or EU | Before any customer run |
| 3 | Can the Black Cloud org be used for the first live run, and is its alias `BlackCloudProd`? | It's the only way to prove real responses match our fixtures | Before Milestone 2 |
| 4 | Can you create a Neon development branch? | `.env.local` points at production, and migrations can't go there | Milestone 5 |
| 5 | AWS account for S3 in London, or a different choice? | See DECISIONS.md | Milestone 5 |
| 6 | Are the working thresholds acceptable? | For example 150 custom fields and 10 stages. They're our judgement, and they're stated in findings as such | Before the first report |
| 7 | Should rep-level segments appear in the raw notes? | The plan keeps executive reports at team level. Raw notes currently show reps | Before the first report |
| 8 | Has zero data retention been requested from Anthropic? | The plan wants it before the first paying customer | Before any customer run |

| 9 | Should private reports block field removal? | Other people's private reports can't be read, so the report guard stays unknown wherever any exist, which is most orgs. Field removal then stays "investigate". We could accept it as a stated risk, or add a manual step | Before the first report |
| 10 | Are the scoring weights and working limits acceptable? | A pass or critical issue is worth 8, with high 5, medium 3, low 1, times confidence. Limits such as 3 times coverage and 25% account share are our judgement | Before the first report |
| 11 | Should accuracy be measured with our own forecast snapshots? | Forecast rows are today's position, so accuracy is not measurable without one | After the first report |

## To confirm against a real org

Salesforce's docs couldn't all be read in full, so these are unproven until a live run.

| # | Question | If we're wrong |
| --- | --- | --- |
| 1 | Do the Tooling `Layout` and `ProfileLayout` queries work without an object filter? | The layout rule reports not measurable |
| 2 | Is the Default Workflow User readable through any metadata type? | Harmless. It stays a manual step |
| 3 | Does `Report.LastRunDate` reflect all users' runs? | The unused reports rule would overstate. It's rated medium confidence for this reason |
| 4 | Which reports can the connected user see? | Private reports are excluded, which is declared on the finding |
| 5 | Does `sf org display` always return a live token? | The run fails at the start with a clear error |
| 6 | Does the org use a custom fiscal year? | We read `Organization.FiscalYearStartMonth`, which assumes standard quarters. Custom fiscal years would need `FiscalYearSettings` |
| 7 | Does Salesforce read the first or last repeated `q`? | Harmless: repeats are refused |
| 8 | Is an expired session in a SOAP call always a fault with `INVALID_SESSION_ID`? | The client refreshes once on that fault. Salesforce's docs give the code, and the HTTP status is unconfirmed, so any other shape fails the retrieve with a clear error |
| 9 | Do `SecurityHealthCheck` and `SecurityHealthCheckRisks` read with our connected user, and are high, medium and meets standard the only risk types? The Tooling reference (API 68.0) says both need View Setup and Configuration, so only the risk types remain open | A failed read is not measurable. Other risk types are left out and declared |
| 10 | Does a connected user without Manage Users or Monitor Login History get only their own `LoginHistory` rows? The Object Reference (API 68.0) says yes, from API 37.0, so the risk is real. Only how to detect it remains open | It would look like success, and hide other people's API logins. Stated on the evidence |
| 11 | Does the mobile app or an email add-in set `LoginHistory.ApiType`? | The personal integration rule would over-report. It's medium confidence, and investigate only |
| 12 | Field names are now confirmed in the docs (see below), and so is `OpportunityRevenue`. Open: does `ForecastCategoryName` return the API value or a renamed label? `HasAdjustment` is only true when the connected user can read the adjustment | A wrong field is not measurable, and a renamed Commit category is not measurable |
| 13 | Do forecast rows really hold no history? | If they do, accuracy could be measured. It's reported as not measurable for now |
| 14 | Is Einstein Activity Capture email outside `Task`, `Event` and `EmailMessage`? Sources are vendors, and a Summer '25 option may change it | Rep-level activity findings drop to medium confidence where it's in use |
| 15 | Does conversion move a lead's tasks to the contact? (`LeadHistory.Field` is documented as filterable) | We count both ids, so the first is safe. If the second fails, first response uses tasks and events only |
| 16 | Can a retrieve reach private reports? (`Report.OwnerId` is documented as the folder id: a user id for a private folder, the org id for public) | A report we can't place counts as not read, and the guard blocks |
| 17 | Is every `TimeZoneSidKey` a name Node understands? | Falls back to UTC, stated on the metric |
| 18 | Does a job on a deactivated user fail? Only vendor and community sources say so | Medium confidence, and the finding says it rests on secondary sources |

## Field names checked against Salesforce's docs

Checked on 2026-11-03 against the API 68.0 (Winter '27) Object Reference, Tooling API and Metadata API guides. developer.salesforce.com answers automated fetches with a 403, so we read Salesforce's own PDF editions of the same guides, from `resources.docs.salesforce.com/latest/latest/en-us/sfdc/pdf/` (`object_reference.pdf`, `api_tooling.pdf`, `api_meta.pdf`). The web page for an object is at `developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_<object>.htm`, or `atlas.en-us.api_tooling.meta/api_tooling/tooling_api_objects_<object>.htm` for Tooling.

About 200 field references on 46 objects, and 6 Metadata types, were checked. Every field used in a WHERE clause is documented as filterable, and `LoginHistory` Status and Application are documented as not filterable, which matches `UNFILTERABLE`.

| Verdict | Names | Notes |
| --- | --- | --- |
| Wrong, fixed | Tooling `Layout.TableEnumOrId` | Not on Tooling `Layout`. Now `EntityDefinitionId`, which is documented, and holds the same object name or id |
| Wrong API, fixed | `ApexTrigger.Name`, `TableEnumOrId`, `NamespacePrefix` through Tooling | Not listed on Tooling `ApexTrigger`. All six fields are listed in the Object Reference, so the query now goes through REST |
| Confirmed | Every other field on `Organization`, `User`, `Profile`, `UserRole`, `UserLicense`, `PermissionSetLicense`, `LoginHistory`, `Dashboard`, `EmailServicesAddress`, `AuthProvider`, `Site`, `Report`, `Folder`, `OpportunityStage`, `Opportunity`, `OpportunityHistory`, `Lead`, `LeadHistory`, `Contact`, `Account`, `Task`, `Event`, `EmailMessage`, `Campaign`, `CampaignMember`, `SetupAuditTrail`, `DuplicateRule`, `PermissionSet`, `PermissionSetAssignment`, `CronTrigger`, `CronJobDetail`, `ApexClass`, `ApexTrigger`, `Period`, all four `Forecasting*` objects, and Tooling `SecurityHealthCheck`, `SecurityHealthCheckRisks`, `ProfileLayout`, `ApexClass`, `ApexOrgWideCoverage`, `ApexCodeCoverageAggregate`, `PlatformEventSubscriberConfig` | Also confirmed: `USING SCOPE allPrivate` on Report, `OpportunityRevenue` as a forecast type name (Metadata API, ForecastingSettings), the `Default*Access` value lists, and the Metadata types `Flow`, `Workflow`, `CustomObject`, `ApprovalProcess`, `AssignmentRules` and `Report` (no wildcard for Report, so it's retrieved by name, as we do) |
| Feature-dependent | `ForecastingType`, `ForecastingQuota`, `ForecastingItem`, `ForecastingFact` | Collaborative Forecasts, and View All Forecasts or Allow Forecasting |
| | `ActivityUsrConnectionStatus` | Einstein Activity Capture |
| | `Site` | Digital Experiences, Salesforce Sites or Site.com. View Setup and Configuration |
| | `EmailMessage` | Enhanced Email or Email-to-Case |
| | `Opportunity.CurrencyIsoCode` | Multi-currency. Already read only if describe shows it |
| | `LeadHistory` rows | Field history tracking on Lead status. The object is there regardless |
| | `AuthProvider` | Customize Application and Manage AuthProviders |
| | `Report USING SCOPE allPrivate` | Manage All Private Reports and Dashboards, and Enhanced Analytics Folder Sharing |
| | `LoginHistory`, `SecurityHealthCheck*`, `DuplicateRule`, `PermissionSet`, `ProfileLayout` | Permissions: Manage Users or Monitor Login History for all logins, otherwise View Setup and Configuration |
| Couldn't confirm | `PermissionSet.PermissionsViewAllData` | The docs give the pattern `Permissions<PermissionName>` and say to use describe. `PermissionsModifyAllData` appears literally, `PermissionsViewAllData` doesn't |
| | That Tooling `Layout` and `ProfileLayout` read without an object filter (item 1 above) | Nothing in the docs requires one |

The forecast items, login history and campaign reads are now described first, so a missing optional field there costs one rule, not the source. See DECISIONS.md 41.

