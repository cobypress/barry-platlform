# Questions

None of these blocked Milestone 1.

Each item has an id: its section's short name and its number or letter, such as `cpq.a` or `pay.49` (`SECTION_IDS` in `src/facts/questions.ts`). Every run's facts step checks the Salesforce names behind many of them, and `npm run questions -- <run folders>` says which those runs settled, contradicted or left open (DECISIONS.md 287 to 295).

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

Still to confirm on a real org:

a) `PermissionSet.PermissionsViewAllData`, beside `PermissionsModifyAllData`. The facts step checks both against the org's own PermissionSet describe.

The forecast items, login history and campaign reads are now described first, so a missing optional field there costs one rule, not the source. See DECISIONS.md 41.


## The team survey and the run (DECISIONS.md 147)

The survey's sections are built when the run job runs. Answers that come in afterwards are logged by the documents job and left out of the documents, because nobody approved their figures. Which does the console want: close the survey before the run starts, or a step that rebuilds the `team_survey` sections when the survey closes and sends them back to review? The engine can do the second (`buildReport` with the new answers), but the console would need to ask for it. Also: should a survey that was sent but that nobody answered say "fewer than 5 answered"? Today the worker can't tell it from no survey, because it only sees answer rows.

## Service Cloud (DECISIONS.md 155 to 158)

Facts the Object Reference didn't settle. Each check that depends on one is not measurable, with the reason, when the fact turns out otherwise.

a) `Case.EntitlementId`, `SlaExitDate` and `MilestoneStatus` aren't on the Case page of the reference: they come with Entitlement Management. We read milestones from `CaseMilestone` instead, and don't read those fields.
b) Whether `CaseHistory` records Status changes without field history tracking turned on for Status, and whether `OldValue` and `NewValue` hold the label or the API name. We match both. With no Status rows while cases were closed, `svc.reopened-cases` is not measurable.
c) The reference describes `User.UserPermissionsSupportUser` as "the user can use the Salesforce console". We treat it as the Service Cloud User feature licence, and price it under that name.
d) The `KnowledgeArticleVersion` page says Manage Articles is needed to use PublishStatus 'Online'. If the read fails for that reason, both knowledge checks are not measurable.
e) Whether Barometer Read Only, or the connected user, needs more than View Setup and Configuration for `MilestoneType`, `SlaProcess`, the Omni-Channel objects, `MacroUsage` and `QuickTextUsage`.
f) Escalation rules come only from the Metadata API. We know of no Tooling route, so a connection that can't use the Metadata API leaves `svc.escalation-targets` not measurable, and `svc.escalation-rules` passes only on an active entitlement process.
g) `KnowledgeArticleViewStat.Channel` values: we assume an "AllChannels" row, when present, totals the others. `MacroUsage.ExecutionState` and `AgentWork.Status` values aren't listed, so failed macros aren't checked and declined work is read from `DeclineDateTime`.

## Integrations and technical health (DECISIONS.md 169 to 178)

Facts we couldn't settle from the references we hold. Where a check depends on one, it's not measurable with the reason if the fact turns out otherwise, or the doubt is stated here.

a) How long Salesforce keeps `AsyncApexJob` rows. We read the last 7 days, which we believe is safely inside it. If rows are kept longer, a longer window would catch failures that recur weekly.
b) `PermissionSet.PermissionsApiUserOnly`: the Object Reference lists permission fields generically, and the Metadata API names the permission `ApiUserOnly`. If the field name is wrong, both API-only reads fail, `int.integration-users-interactive` is not measurable, and `int.integration-user-licences` relies on the calibration alone.
c) Whether the Salesforce Integration licence makes its users API only by itself. We assume it does, and leave those users out of `int.integration-users-interactive`.
d) The licence's name in `UserLicense` is "Salesforce Integration" (seen in a real org, and in `COMPLIMENTARY_LICENCES`). How many come free by edition, and the price of more, we don't hold, so a move with no spare seat isn't sized.
e) `WebLink.LinkType` values in SOQL. The Metadata API lists url, sControl, javascript, page and flow. We compare without regard to case.
f) `ApexPage.IsAvailableInTouch` is the "Available for Lightning Experience, Experience Builder sites, and the mobile app" box. We report a page without it as "not marked available in Lightning Experience", not as broken.
g) The Apex size limit. The Apex Developer Guide says 6 MB, with `@isTest` classes left out. We read it as Setup's 6,000,000 characters without comments, and sum `LengthWithoutComments` of active code. Whether inactive code counts, we don't know.
h) `/limits`: whether `DailyAsyncApexExecutions` is the async Apex allowance under that name in every edition, and whether going past `DailyApiRequests` refuses calls or is allowed for a while. Our text says Salesforce "can refuse" them.
i) `Scontrol` in an org that never had s-controls: if the query fails as an unknown object, `tech.classic-leftovers` is not measurable on an org with nothing else to report. We looked at reading that as "none" (DECISIONS.md 200) and didn't: Salesforce gives the same INVALID_TYPE for an object the org lacks and for one the user can't see. A real org's first run will show whether the query fails at all. If it does, `EntityDefinition WHERE QualifiedApiName = 'Scontrol'` might tell the two apart, but that is untested.
j) For the privacy owner: the scrub turns the words "integration user" into a user's token when an org has a user named "Integration User", which real orgs often do. Our integration text avoids the words (DECISIONS.md 175). Other text, and Claude's drafts, could still hit it.

## Action lists (DECISIONS.md 191 to 200)

a) The workbook was checked with openpyxl, not with Excel, Google Sheets or Numbers. Each follows the same standard, but the first real customer file should be opened in whichever the customer uses.
b) The ticket file was shaped from what we know of Jira's CSV importer: a repeated "Labels" column for several labels, and the default priority names. It hasn't been imported into a real Jira, Linear or Asana project. A project with renamed priorities will need them mapped at import.
c) A link is `https://<the connection's domain>/<id>`. We believe this opens a record in both Lightning Experience and Classic, but we haven't checked it on an org with Enhanced Domains switched on.

## Roadmap 3.2's remaining checks (DECISIONS.md 201 to 209)

a) A permission set group's assignment: whether `PermissionSetAssignment.PermissionSetId` is null or the group's own permission set. We handle both. If it's the group's own set, a group assigned to nobody may show that set as unused, under the group's name.
b) `SELECT Name, COUNT(Id) FROM Account GROUP BY Name HAVING COUNT(Id) > 1` through REST on a very large Account table may time out. If it does, `dq.duplicate-accounts` is not measurable, with Salesforce's reason.
c) Person accounts: an org that uses them has people's names as account names, and two people can share a name. `IsPersonAccount` exists only where person accounts are switched on, so we don't filter on it yet.
d) Storage: we say that past the allowance Salesforce "can stop new records or files being saved". We haven't confirmed how strictly each edition enforces it.
e) `PackageLicense.Status` values beyond Active, Expired, Free and Trial, and whether `NamespacePrefix` is ever empty for an app sold by the seat.
f) Territories, if a customer uses them: `Territory2Model` (State 'Active'), `Territory2` and `UserTerritory2Association` would show territories with nobody assigned. Fair balance would need the customer's own rules.

## Salesforce CPQ (DECISIONS.md 216 to 226)

CPQ's objects belong to a managed package and aren't in the Object Reference. Each is described before it's queried, so a wrong name makes the checks that need it not measurable rather than wrong. To confirm on a real CPQ org:

a) `SBQQ__Quote__c`: `SBQQ__Primary__c`, `SBQQ__Opportunity2__c`, `SBQQ__ListAmount__c`, `SBQQ__NetAmount__c` and `SBQQ__Ordered__c`. Also whether the list amount already includes contracted prices and partner discounts, which would put them in "discount off list".
b) `SBQQ__PriceRule__c` (`SBQQ__Active__c`, `SBQQ__EvaluationEvent__c`, `SBQQ__EvaluationOrder__c`, `SBQQ__TargetObject__c`), `SBQQ__PriceAction__c` (`SBQQ__Rule__c`, `SBQQ__Field__c`, `SBQQ__TargetObject__c`), `SBQQ__ProductRule__c` (`SBQQ__Active__c`, `SBQQ__Type__c`), and `Contract.SBQQ__RenewalOpportunity__c`.
c) Two active price rules with the same evaluation event and order: Trailhead says the larger order wins, and doesn't say what happens on a tie.
d) Whether CPQ always has a `PackageLicense` row with namespace SBQQ. If it doesn't, the describe finds it, at one extra call.
e) Advanced Approvals: how its approval records point to a quote, so `cpq.discount-approvals` could read them rather than going not measurable.
f) Revenue Cloud Advanced (Agentforce Revenue Management): its quote, pricing procedure and order objects, before we build for it.
g) Twin fields: whether CPQ treats any differing types as compatible, such as text and text area, which we'd report as a mismatch today.

## AI readiness (DECISIONS.md 236 to 245)

a) The Settings members we retrieve are "Knowledge", "EinsteinGpt", "AgentPlatform" and "CustomerDataPlatform". The guide names the last two files `AgentPlatformSettings.settings` and `CustomerDataPlatformSettings.settings`, so their member names may differ. We parse by each file's root element, so any file that comes back is read. One that doesn't is unknown, and its pillar says so.
b) Whether a connection holding only Barometer Read Only can read these settings any other way than the Metadata API.
c) `BotDefinition` when agents aren't enabled: the guide says agents must be enabled to read it. We show the agents as unreadable, never as none.
d) Counting published articles in a multilingual Knowledge base: whether `IsLatestVersion = true` without a language filter counts each article once.
e) Whether `enableCustomerDataPlatform` being true means Data Cloud (Data 360) is provisioned and in use, or only switched on.
f) Whether every active autolaunched flow can be an agent action, or only those with suitable inputs and outputs. We count them all as candidates, and say an agent needs a description to choose one.

## Account Engagement (DECISIONS.md 261 to 274)

The package's fields aren't in the Object Reference. Each is checked against the org's Lead describe, so a wrong name makes the lead checks not measurable, naming the field, rather than wrong. To confirm on a real org with Account Engagement:

a) The namespace is `pi`, and the package always has a `PackageLicense` row with it. If it doesn't, the Lead describe still finds `pi__url__c`.
b) `Lead.pi__url__c` is set on every lead synced with a prospect, and only on those. `Lead.pi__score__c` and `Lead.pi__grade__c` hold the score and grade. Contacts have the same fields, and we don't read them yet.
c) Whether `pi__score__c` is ever left empty, or is always 0 for a prospect with no scored activity. We count both as no score.
d) The Settings member for the campaign settings is "Campaign". We parse by the file's root element, `CampaignSettings`, so a different file name still parses, but a different member name returns nothing and the check goes not measurable.
e) The connector user: Salesforce calls it the "B2BMA Integration User". Whether its licence and permission sets can be identified reliably from `User` and `PermissionSetAssignment` without the Account Engagement API.
f) Sync errors: whether anything of them is visible from the Salesforce side, or only in Account Engagement's own connector error queue (which would need its API, a later decision).
g) The handover moment: which lead status, field or assignment each org uses for "marketing qualified", so the time from handover to first touch can be measured per org.

## Sharing the Drive folder (DECISIONS.md 251 to 256)

To check with the real service account, on a copy of the Health checks folder:

a) Does `drive.file` reach everything the documents job does? Creating a client folder inside "Health checks", which a person made, listing it, and reading it by id. If it does, the scope can narrow from `drive` (255).
b) Is the service account a Manager of the shared drive, or is "Content managers can share folders" on? Without one of them, every share fails with Drive's reason recorded on the console's share row.
c) Does the shared drive allow sharing with people outside the organisation? The customer's readers are outside it.
d) Does a reader without a Google account get in from Google's email, and how long does that invitation last?
e) When the service account can no longer see the folder, does deleting a permission on it answer 404? `unshare` reads 404 as "already gone" and the share is recorded as removed, so if it does, the person would keep access while the console says they don't (night 3 review).
f) Given a Google Group's address as a `type: "user"` reader, does Drive refuse it, or share with everyone in the group? A microsite reader's address is checked as an address only (night 3 review).
g) Creating a reader permission for someone who already has one on the folder: does Drive return the existing permission's id, and does it ever change a higher role, such as a writer's, down to reader? The sync shares again after a failure, and decision 312 relies on one permission a person (night 3 review).
h) Does `permissions.list` on a folder in a shared drive list a reader permission made on that folder with its `emailAddress` and `role: "reader"`, as `findReader` expects (312)?

## Labels in customer text (DECISIONS.md 411 to 414)

To check on a sandbox or a developer org, not a customer's:

a) Does the global describe (`GET /sobjects`) give every custom object's `label` as Setup shows it, renamed standard objects included (an Opportunity relabelled "Deal")?
b) Does a Tooling query of `FieldDefinition` accept `Label` beside `QualifiedApiName`, `DataType` and `ReferenceTo`, and give the label Setup shows?
c) Does `SELECT SubscriberPackage.Name, SubscriberPackage.NamespacePrefix FROM InstalledSubscriberPackage` through the Tooling API work for a read-only integration user, and give the app's name as the customer knows it? If it's refused, apps are named by namespace, as before.
d) In a retrieved object's metadata, is a record type's `<label>` always there, and is Tooling's `RecordType.Name` the same label?
e) For CPQ: is a price action's `SBQQ__TargetObject__c` "Quote" or "Quote Line", so the field it sets is on `SBQQ__Quote__c` or `SBQQ__QuoteLine__c`, and its label is that object's describe's?

## The org handbook's data (DECISIONS.md 451 to 457)

To check on a sandbox or a developer org, not a customer's. The route test in `src/handbook/__tests__/handbook.test.ts` feeds a hand-written Tooling flow built on these answers.

a) In Tooling's `Flow.Metadata`, are a value's unused members (`stringValue`, `numberValue`, `booleanValue` and the rest) null, rather than a default such as 0 or false? And is a number 5000 where the retrieve says 5000.0? If a default comes back, an entry condition's value could read as the wrong one.
b) Does every element on the canvas carry `locationX` on both routes, auto-layout flows included? An element of a kind the parser doesn't know is kept when it has one, or a connector.
c) Does Tooling's `ValidationRule.Metadata` give `errorMessage` and `errorDisplayField`, as the Metadata API does? If not, the Tooling route's rules show no message.
d) Does a retrieve write a quote inside a formula as `&quot;`? The handbook redacts each value after parsing either way, so this only says how often the whole-file redaction misses one.
e) Does the Metadata API's `*` for `CustomObject` bring any standard object in a real org? We assume none (decision 455): on the retrieve route the handbook's validation rules say they cover your custom objects and Account, Contact, Lead and Opportunity, the four the retrieve names, and on the Tooling route every object. `aut.undocumented-automation` counts validation rules by route for the same reason: custom objects and those four on the retrieve, every rule on Tooling. If the wildcard brings a standard object, its rules are read but the list says it covers less than it holds.
f) Is `ApexTrigger.TableEnumOrId` through REST the object's API name for a standard object (`Opportunity`) and, for a custom object, its id (`01I...`, 15 or 18 characters) rather than `Invoice__c`? The synthetic org assumes the id, as `real-org.22` does. The handbook keeps the value exactly as Salesforce gives it (decision 456), so if it's an id the documents have to name the object from the custom objects list before a reader sees it.
g) Does REST's `ApexTrigger` give all seven `Usage` fields (`UsageBeforeInsert` to `UsageAfterUndelete`) as true or false for every trigger, whatever its API version? The handbook lists the events that are true, in a fixed order, and says the events weren't read when none is, so a trigger whose fields come back null or all false shows no events rather than a wrong list.
h) Is a class or trigger whose code Salesforce withholds given a `Body` of exactly `(hidden)` through REST, or no `Body`? Either gives "couldn't be read" for what it touches (decision 457). Any other placeholder would be read as code, find nothing, and say it touches nothing.
i) Are `insert`, `update`, `upsert`, `delete`, `undelete` and `merge` still the only DML statements, and the `Database` methods of those names and `convertLead` the only ones that change records, in the current Apex reference? The reading counts no other: a class that only uses `Database.insertAsync`, `insertImmediate`, `deleteAsync` (external and big objects) or `EventBus.publish` shows no change. For queries built at run time it counts every `Database` method whose name starts `query`, `countQuery`, `getQueryLocator` or `getCursor`, and `Search.query`; a search, inline or through `Search.query` or `Search.find`, is marked as one, and the objects it reads aren't kept. Still missed: a query string run through a method the code names itself, or through a library's, reads as no query; `Search.find` isn't marked as a query built at run time; and the scan's own `queriesInLoops` doesn't count the `WithBinds` forms or `getCursor`, so a technical health figure can miss a query in a loop that the handbook marks. Anything inside an inline query's or search's brackets is never read as a change, so `UPDATE TRACKING` and `UPDATE VIEWSTAT` aren't updates.
