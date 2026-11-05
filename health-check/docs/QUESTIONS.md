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
| 12 | Which org should be the Dev Hub for the Barometer connector package, and what namespace? | Customer orgs need the app from a managed 2GP package (DECISIONS.md 65). A Developer Edition Dev Hub risks the package becoming un-updatable when the org expires, so `BlackCloudProd` or a partner business org is safer. The namespace can't be changed later | Before the first customer connects |
| 14 | Which user should customers connect as: an API-only Salesforce Integration user (free, 5 in Enterprise and up) or a full-licence user holding only Barometer Read Only? | Mostly answered on 2026-11-03: an API-only user can complete the browser consent (below), so `docs/customer/INSTALL.md` recommends the Integration user and offers a full-licence user as the alternative. Still open: how a customer sets that user's password (31 below) | Before the connect link pages are built |
| 39 | Should findings a read-only connection can't measure raise manual steps? | Only two checks raise one now (Einstein Activity Capture and Relate Synced Events), so every live run so far has had none. Apex test coverage and other people's private reports become not measurable instead, with no step asking a person to look. Hard rule 6 says anything that can't be read becomes a manual step | Before the first report |
| 40 | Where should the production console store runs? | On Vercel it uses its built-in sample data: Postgres is allowed only locally, against the Neon dev branch (`lib/health-check/repository-config.ts` in blackcloud-next). Production needs its own decision, and the worker needs the same database. Proposed on 2026-11-04: the site's own database, behind `HC_REPOSITORY=production` (blackcloud-next 7e886ed, off until set). See WORKER-HOSTING.md | Before any customer run |
| 41 | How long should the worker keep run folders, and who deletes them? | They hold the CRM records a run read, and nothing deletes them yet. The product plan's "Data that expires" sets periods, but no job carries them out | Before any customer run |

Answered on 2026-11-03, question 13: **a read-only connection is enough for nearly everything.** As the Barometer Read Only user on `bc-hc-dev`, automation metadata reads through Tooling and report definitions through the Analytics API, and the engine now does that when the Metadata API refuses (DECISIONS.md 60, 68 and 69). On `bc-hc-dev`, not measurable findings fell from 41 to 32. Of the 32, Apex coverage is the only one we know the connection causes; most of the rest are an empty development org. Forecasting and Einstein Activity Capture aren't switched on in `bc-hc-dev`, so whether this user can read them in an org that has them is still open (26). What stays out of reach on a read-only connection: Apex test coverage, other people's private reports (so field removal stays blocked, question 9), auth providers, and the ownership of custom objects' records unless the customer adds read on them. A second, write-capable permission isn't needed.

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
| 19 | When a stage picklist value is renamed, do old `OpportunityHistory.StageName` rows keep the old name, or change with it? | We assume they keep it, so the calibration must list old names. If they change, an old stage simply never appears, and stage win rates are unaffected |
| 20 | Is a history row written for every change to Amount and CloseDate, as well as stage, on every edition? | The "no change of any kind" cohort for quiet deals would be too large, so its win rate would read too high. Stated as a proxy on the finding |
| 21 | Does `IsWon = true OR StageName IN (...)` filter on Opportunity everywhere, and does the prior wins read stay small? | A failed read makes new and repeat customers not measurable, never all new |
| 22 | Does Tooling `CustomObject` also list custom metadata types, platform events or external objects, and is its `Id` the same id that `Layout.EntityDefinitionId` and `ApexTrigger.TableEnumOrId` hold for a custom object? The Tooling guide (API 68.0) documents `DeveloperName` without the suffix, and `EntityDefinitionId` as the EntityDefinition's id, but doesn't say either outright | If another kind shares a custom object's name, both are left unnamed and the inventory says how many layouts and triggers it couldn't place. If the ids differ, every custom object's layouts and triggers go unplaced, which the same note shows |
| 34 | Do Tooling `EntityDefinition`, `FieldDefinition` and `EntityParticle` page with queryMore in a large org? Some Salesforce sources say `EntityDefinition` doesn't support it. `bc-hc-dev` returned 229 entities in one page | A failed page makes the custom object list, or the Tooling route's fields, unavailable rather than wrong |
| 35 | Can the read-only user read `Metadata` on Tooling `WorkflowAlert`, `WorkflowTask` and `WorkflowOutboundMessage`? `WorkflowRule` and `WorkflowFieldUpdate` were proven on `bc-hc-dev`, which has no alerts, tasks or outbound messages. Their list queries need `Id` only (`Name` isn't a column on alerts or tasks) | A failed read leaves that kind unsearched, so reference guards read unknown. Nothing is guessed |
| 36 | Is `AuthProvider` in every org, so INVALID_TYPE always means the user can't see it? It's a setup object, and the docs list it without a feature dependency other than permissions | If an edition lacks it, the run-as scan reads incomplete there when it could be clean, which only blocks |
| 37 | Does a report describe fail for a report whose report type the user can't see, and does the user see every public report's definition? `bc-hc-dev`'s eight reports all described | A failed describe counts the report as not read, so the report guard is unknown |
| 38 | Is the allPrivate COUNT() behaviour (every visible report, no error) the same in every org without Manage All Private Reports? Seen on `bc-hc-dev` only | The row probe runs first everywhere, so the count is only trusted when the scope is honoured |

## The connection (Milestone 2)

Answered: **customer orgs need a managed 2GP package** for the External Client App. The evidence and sources are in DECISIONS.md, "Connecting customer orgs".

Answered on 2026-11-03: **an API-only user can complete the web server flow's browser sign-in.** Coby signed in to `bc-hc-dev` as the Salesforce Integration licence user and allowed the app, and the run completed on that token (MILESTONES.md). So customers can connect an integration user and don't need a full-licence user.

Answered on 2026-11-03: **Event needs no grants of its own** (32). After installing `0.1.0-7` in `bc-hc-scratch-5`, the created set's FieldPermissions held `Event.WhatId` and `Event.WhoId` as readable as well as the Task fields. Salesforce applies the Task grants to Event, because both share the Activity fields.

These are still open.

| # | Question | If we're wrong |
| --- | --- | --- |
| 22 | Can a user of another org authorise our local app at all? Salesforce's docs say no (local apps work only in their own org), and one vendor's notes say they're "authorized by other orgs as an uninstalled app". Not tried: this work could only touch `bc-hc-dev` | If it can, a pilot could run before the package exists. The package is still needed, because the uninstalled app restriction blocks new users who aren't administrators |
| 24 | Is there a grace period after refresh token rotation? None is documented | Two workers refreshing at once would kill the connection. One worker per connection, and the new token is stored before use (DECISIONS.md 62) |
| 25 | What exactly is the token response `signature` computed over? The docs say it "can include the concatenated ID and issued_at value" | Nothing: we don't check it, and rely on TLS to a checked Salesforce host (DECISIONS.md 62) |
| 26 | Is View All Forecasts allowed on the integration licence? `bc-hc-dev` has no Collaborative Forecasts, so the permission doesn't exist there to test | Forecast rules are not measurable on a read-only connection until it's added to Barometer Read Only |
| 27 | Does the app's "admin approved users are pre-authorised" policy, set by a customer's admin on the installed package, also gate the web server flow, so an administrator can't authorise as themselves? Salesforce documents it for pre-authorisation generally | If it doesn't, the connect-time read-only check (DECISIONS.md 64) still revokes a token that can write |
| 28 | In External Client App Manager, after Permitted Users is set to "Admin approved users are pre-authorized", what is the control for choosing the permission set called? The help article only says to "select the profile or permission set". The metadata field is `commaSeparatedPermissionSet` | The install guide (`docs/customer/INSTALL.md`, step 5) names no exact control, so an admin may need to hunt for it |
| 29 | Does a pre-authorised user see the approval page when we send `prompt=login consent`? Help says pre-authorised users "bypass the Allow or Deny permission window", and that `consent` makes the server "prompt the user for reapproval". Every live sign-in so far used "All users may self-authorize" | Harmless either way. The guide says the page "may" appear. Settle it on the first install of a released version |
| 30 | Does uninstalling the package leave Barometer Read Only behind, and does it revoke tokens already issued? The 2GP guide says only package components are deleted, and the set the script creates isn't one. Nobody has uninstalled it yet | The guide tells admins to revoke first and delete the set themselves. If uninstall does delete the set, that step is just unnecessary |
| 31 | For a new Salesforce Integration user: how many licences each edition includes (only secondary sources say five for Enterprise and above), whether the Salesforce API Integration permission set licence is needed when Barometer Read Only has no licence, and whether the user can follow Salesforce's welcome or password reset email in a browser. `bc-hc-dev`'s password was set with anonymous Apex | A customer could be stuck at "set a password" in step 3. Try it with the first install of a released version |
| 33 | Bulk API 2.0: does an `api` scope token for a user holding only Barometer Read Only (no Manage Data Integrations) create, poll and read its own query jobs? Do results pages count against `DailyApiRequests` one call each? Do Bulk queries apply sharing and field-level security as REST does? Can `LoginHistory` and `SetupAuditTrail` be read through Bulk? See `docs/BULK-API.md` | Only matters if Bulk is built. A test on `bc-hc-dev` answers all four |

## Published research (roadmap 1.6)

The figures we compare with are in `src/benchmark/published.ts`, each with its source, population and how it differs from ours (DECISIONS.md 84). Read on 2026-11-05.

| # | Question | If we're wrong |
| --- | --- | --- |
| 42 | Ebsta and Pavilion's 2025 GTM Benchmarks gives the new logo win rate as 19% on its overview and its new business and expansion pages, but 18% on a later page of the same report. We use 19% and say so | The comparison is a point out |
| 43 | Ebsta doesn't define its bases. For win rate, are no decision deals lost? Is the sales cycle won deals only, from creation? Is "deals slipped" closed deals whose date moved, or deals open past their date? Is each pooled across deals or averaged across companies? Could someone ask Ebsta (now Fullcast) for the definitions? | Each unknown is listed beside the figure in the raw notes. If a base is further from ours than they say, the comparison misleads |
| 44 | Fullcast's "2026 Benchmarks Report: The State of GTM in 2026" (March 2026) is behind a registration form, so we haven't read it. It would be the newest edition measured from CRM records. Someone needs to sign up for it | We compare with 2025 figures when newer ones exist |
| 45 | What we left out, and why. HubSpot's 2024 Sales Trends Report gives a 21% average win rate, but from a survey of B2B and B2C sellers, with no definition. Bridge Group's 2024 median win rate of 19% is only quoted on other sites. Validity's 2024 record audit says "over 50 percent" of account and contact records miss critical data, but that covers phone and address as well as email, and isn't an exact figure. Norwest's 2025 stage conversion figures are banded survey answers from North American companies. Ebsta's 2024 "31% of opportunities are past the close date" doesn't give a base. Nothing we found measures win rate by value, deals pushed into a later quarter, lead conversion as Salesforce records it, or contacts with no email. Should any of these be used, with a stronger caveat? | Those rows say there's no published research that measures them our way, which is true but thin |
| 46 | Who rechecks the published figures, and how often? Each records the date it was read | A report could quote a figure that has since been revised or withdrawn |

## What they pay (roadmap 1.3)

| # | Question | If we're wrong |
| --- | --- | --- |
| 47 | Which `Organization.OrganizationType` does an org on each of Salesforce's current plans report (Starter Suite, Pro Suite, Core, Advanced, Max)? The Object Reference only gives "Enterprise Edition or Unlimited Edition" as examples. Until it's known, no edition is priced at list (DECISIONS.md 91) | Nothing is mispriced: the list price stays not measurable, and the calibration or the customer's figure is used |
| 48 | What do the legacy editions (Professional, Enterprise, Unlimited) cost now in GBP? Salesforce says existing customers keep their pricing, and no longer publishes it. Should an existing Enterprise org be shown Core's price for context, clearly labelled as the price of the edition Salesforce now sells instead? | Without either, a report has no list price to set beside the customer's own figure |
| 49 | Can the read-only user query `PackageLicense`? It isn't in DECISIONS.md 60's list of what Barometer Read Only can or can't read. The Object Reference only says Customer Portal users can't access it | The package seat figures are not measurable, and say why |
| 50 | Is `AllowedLicenses` -1 for a package licensed to the whole org? The Object Reference only calls it "the number of users allowed to use the package". We count only positive values as seats | A site licence would be left out of package seats, which is what we want either way |
| 51 | List prices for other paid licences (Salesforce Platform, the Experience Cloud and partner licences, and permission set licences) aren't in the table. Their UK pricing pages either don't exist at the address we tried or weren't read | Those seats are priced only from the calibration, and a total with one of them unpriced is not measurable |

## Adoption (roadmap 3.2)

| # | Question | If we're wrong |
| --- | --- | --- |
| 53 | What does `LoginHistory.Application` say for the Salesforce mobile app on iOS and Android, and what does `Platform` say? The Object Reference lists neither field's values. We match "Salesforce for iOS", "Salesforce for Android", "Salesforce Mobile", "SalesforceA" and Chatter's apps, or an iPhone, iPad, iOS or Android platform with a Remote Access or OAuth login (DECISIONS.md 101) | Mobile logins would be counted as other, and the mobile share would read low. It never makes anyone inactive, because every channel but API counts as a login |
| 54 | How far back does `LoginHistory` go in a real org? The Object Reference doesn't say | We never assume: the months are worked out from the oldest login read, and falling adoption is not measurable with fewer than 4 whole months |
| 56 | Is `UserPreferencesLightningExperiencePreferred` readable for other users through the API? The Object Reference lists it on User as "Switch to Lightning Experience" | Handled either way (DECISIONS.md 112): a missing field, or a setting empty for every user, makes the Lightning share and the Classic check not measurable, and a user whose setting is empty is left out rather than counted as Classic |

## Your org, documented (roadmap 3.4)

| # | Question | If we're wrong |
| --- | --- | --- |
| 57 | Does `FieldDefinition.DataType` read "Master-Detail(...)" for a master-detail field? The Tooling API guide says only that it's the type as the user interface shows it, for example Text(80) | A master-detail relationship would show as a lookup. Nothing else depends on it |
| 58 | The Tooling API guide lists the sharing models as None, Read, Edit, ControlledByLeadOrContact and ControlledByCampaign. Is None the private default, and what do ReadEditTransfer, ControlledByParent and full access read as? | We show None as Private and the others in plain words. A value we don't know is shown as Salesforce gives it |
| 59 | Can the read-only user query `ConnectedApplication`, `NamedCredential` (View Setup and Configuration, per the Object Reference), Tooling's `RemoteProxy`, and `PermissionSetAssignment` filtered on `Assignee.IsActive`? | Each one that can't be read is left out of the part, and the part says why |
| 60 | Is there a read-only way to count sharing rules? They're only in the Metadata API, which a Barometer Read Only connection can't use | The security part shows each object's default and says sharing rules aren't counted |
| 61 | Does `LoginHistory.Application` hold a connected app's name, as Setup shows it, for logins through that app? We match the two by name to find which self-authorised apps carry API logins | An app in use would be rated medium rather than high. It's still raised |

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

