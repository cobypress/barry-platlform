import type { FixRule, HoursRule, PromptRule } from "./types.ts";

// Every rule that can raise an issue, with its hours to fix and the fixed text of
// its fix prompt (DECISIONS.md 628).
//
// ESTIMATES AWAITING THE OWNER'S REVIEW. The hours are the builder's first
// estimates for a fix led by a consultant who knows Salesforce. They are a
// commercial claim, so no document prints them until Coby has approved them.
// `npm run effort-hours` writes them all to fixtures/effort-hours.json for him to
// read and change in one place, and a test fails when the two differ.
//
// Hours cover the change a consultant makes: building, testing and handing it
// over. They don't count the time owners or reps spend updating their own
// records, or the time people spend agreeing a decision.

const fixed = (low: number, high: number, basis: string): HoursRule => ({ kind: "fixed", low, high, basis });
const each = (fact: string, one: string, many: string, minutes: readonly [number, number], cap: number, capReason: string): HoursRule => ({ kind: "each", fact, one, many, minutes, cap, capReason });

const change = (items: string, steps: string[], verify: string[]): PromptRule => ({ items, steps, verify });
const decide = (items: string, steps: string[], verify: string[]): PromptRule => ({ items, decision: true, steps, verify });

// Basis sentences reused across rules.
const SET_UP_REPORT = "Building the report and the check that keeps it clean; the owners' time updating each record isn't counted";
const PROCESS = "Preparing the analysis and options for the people who decide, and making the agreed change; their time deciding isn't counted";
const BULK = "past that the work is done in bulk, so it stops growing with the count";

/** The rules that only ever pass or are not measurable, so they declare nothing. A test reads each one's code to hold this. */
export const NEVER_RAISES: ReadonlySet<string> = new Set(["utl.dashboard-usage", "fct.forecasting-in-use", "fct.forecast-accuracy"]);

export const FIX_TABLE: Readonly<Record<string, FixRule>> = {
  // Configuration
  "cfg.custom-field-sprawl": {
    hours: fixed(4, 8, "A review of each object's fields with its owners, whatever the number of fields; removing fields afterwards is its own finding"),
    prompt: decide("objects", ["For each object named, list its custom fields with their fill rate, when each was created and last changed, and where each is used (layouts, flows, Apex, reports, validation rules).", "Group the fields into keep, review and candidate to retire, with the reason for each.", "Prepare the list for the object's owners to agree."], ["Every custom field on the objects named is in one of the three groups, with a reason."]),
  },
  "cfg.unused-custom-fields": {
    hours: each("unusedFields", "field", "fields", [30, 60], 16, BULK),
    prompt: change("fields", ["For each field, list everything that references it: page layouts, flows, Apex, formulas, validation rules, reports, list views and integrations.", "Export any data in it that must be kept.", "Remove the field from page layouts first and wait an agreed period for anyone who misses it.", "Only when nothing references it and its owner agrees, delete the field in a sandbox, run the tests, then do the same in production."], ["The deleted fields no longer appear in Object Manager, and every flow, Apex test and report that ran before still runs."]),
  },
  "cfg.unassigned-layouts": {
    hours: each("unassigned", "layout", "layouts", [15, 30], 8, BULK),
    prompt: change("page layouts", ["For each layout, confirm in the page layout assignment that no profile and record type uses it.", "Check it isn't named in a deployment, a managed package or documentation people rely on.", "Delete the ones nobody needs, in a sandbox first."], ["The page layout assignment shows every profile on a layout that still exists, and the deleted layouts are gone."]),
  },
  "cfg.undocumented-fields": {
    hours: each("bare", "field", "fields", [10, 20], 24, BULK),
    prompt: change("fields", ["List the custom fields with no description or help text on the objects named.", "Draft a description of what each field holds and why, and help text for the ones people fill in, from its label, type, values and where it's used.", "Mark any field whose purpose you can't work out for its owner to explain or retire.", "Apply the agreed text in a sandbox, then in production."], ["Every custom field on these objects has a description, and every field on a page layout has help text."]),
  },
  "cfg.stage-sprawl": {
    hours: fixed(4, 8, PROCESS),
    prompt: decide("opportunity stages", ["List each opportunity stage with its probability, forecast category, how many open and closed deals sit in it, and which sales processes use it.", "Propose a shorter set of stages that each mark real buyer progress, with exit criteria for each.", "Map every current stage to one in the proposal, and list what depends on stage names: flows, validation rules, reports and paths."], ["The proposal covers every stage in use, and every dependency on a stage name is listed."]),
  },
  "cfg.picklist-sprawl": {
    hours: fixed(2, 6, "Consolidating each list named, with a mapping of old values to new ones and an update of existing records"),
    prompt: change("picklists", ["For each picklist named, count how often each value is used on records made in the last 12 months.", "Propose a shorter list, and map each old value to a new one.", "Check flows, validation rules, reports and integrations that name a value.", "In a sandbox, update records to the new values, then deactivate the old ones rather than deleting them."], ["New records can only pick the agreed values, and reports on the field show no old values for new records."]),
  },
  "cfg.setup-change-concentration": {
    hours: fixed(4, 8, "Writing down how the org is changed and handing the work to a second person, whatever the size of the org"),
    prompt: decide("setup changes", ["From the Setup Audit Trail, summarise the kinds of change made in the period by area (users, fields, flows, security), without naming anyone.", "Draft a short change process: who can change what, where changes are tested, and how they're recorded.", "List what a second person needs to learn to cover the work."], ["The change process and the list of skills are ready for the business to agree."]),
  },

  // Automation and technical health
  "aut.legacy-workflow-rules": {
    hours: each("activeRules", "workflow rule", "workflow rules", [60, 180], 60, "past that the move is planned as a project of its own, object by object"),
    prompt: change("workflow rules", ["Group the active workflow rules by object, and for each list its criteria, field updates, emails, tasks and time-based actions.", "For each object, build one record-triggered flow that does the same work, using Migrate to Flow where it gives a like-for-like result.", "In a sandbox, activate the flow, deactivate the rules it replaces, and test each path with records that meet and miss the criteria."], ["On each object, the flow produces the same field updates, emails and tasks as the rules did, and the rules are inactive."]),
  },
  "aut.legacy-process-builder": {
    hours: each("activeProcesses", "process", "processes", [120, 240], 60, "past that the move is planned as a project of its own, object by object"),
    prompt: change("processes", ["List each active Process Builder process with its object, criteria and actions.", "Move each to a record-triggered flow on the same object, together with any workflow rules on it, so each object is migrated once.", "In a sandbox, activate the flow, deactivate the process, and test every criteria node."], ["Each object's flow does what its process did, and the process is inactive."]),
  },
  "aut.mixed-automation-on-object": {
    hours: each("objects", "object", "objects", [240, 480], 40, "past that the objects share one agreed pattern, so each further one is quicker"),
    prompt: change("objects", ["For each object named, list every trigger, flow, process and workflow rule that runs on it, with when it runs and what it changes.", "Propose which work belongs in Apex and which in Flow, and the order they run in.", "In a sandbox, move the older tools' work into that split one object at a time, and run the Apex tests and a test of each path."], ["Each object's automation runs in the agreed tools only, and records save with the same results as before."]),
  },
  "aut.hardcoded-ids-in-flows": {
    hours: each("affected", "flow", "flows", [60, 120], 16, "past that the same custom metadata or label serves many flows"),
    prompt: change("flows", ["Open each flow and list every record id and email address typed into it.", "Replace each with a custom metadata record, a custom label, or a lookup of the queue or user by its developer name.", "Save a new version in a sandbox and test each path that used a replaced value."], ["No flow version that is active holds a typed record id or email, and each path still reaches the same record or person."]),
  },
  "aut.flows-without-fault-paths": {
    hours: each("affected", "flow", "flows", [60, 120], 24, "past that a shared fault-handling subflow makes each further flow quicker"),
    prompt: change("flows", ["Build one subflow that records a failure (the flow, the element and the error message) and tells an admin.", "In each flow, add a fault path from every element that creates, updates or deletes records, leading to that subflow.", "Test in a sandbox by making each save fail, for example with a validation rule."], ["Every data element in these flows has a fault path, and a forced failure is recorded and reaches an admin."]),
  },
  "aut.undocumented-automation": {
    hours: each("missing", "item", "items", [10, 20], 24, BULK),
    prompt: change("flows, rules and classes", ["List the flows, workflow rules, processes and Apex classes with no description, busiest objects first.", "Draft a description of what each does and why, from its trigger, criteria and actions or code.", "Mark any you can't explain for its owner, and add the agreed descriptions in a sandbox, then in production."], ["Every active flow, rule and class has a description."]),
  },
  "aut.apex-coverage": {
    hours: each("weakClasses", "class", "classes", [120, 240], 40, "past that shared test data factories make each further class quicker"),
    prompt: change("Apex classes", ["List the classes below the coverage line, weakest first, and the lines each test doesn't reach.", "Write tests that create their own data and assert the outcome of each path, not just run the code.", "Run all tests in a sandbox."], ["Each class named is above the coverage line, every test asserts something, and the whole test run passes."]),
  },
  "tech.test-quality": {
    hours: fixed(4, 16, "Adding assertions and test data to the flawed test classes; more for a large set"),
    prompt: change("test classes", ["List the test classes with no assertion and those that read the org's data.", "Add assertions that check each test's outcome, and have each test create the records it needs instead of reading the org's.", "Run all tests in a sandbox."], ["No test class lacks an assertion or reads the org's own data, and the test run passes."]),
  },
  "tech.governor-limit-risk": {
    hours: each("risky", "class or trigger", "classes and triggers", [120, 240], 40, "past that the same pattern repeats, so each further one is quicker"),
    prompt: change("Apex classes and triggers", ["Find each query and save made inside a loop in the code named.", "Rewrite it to gather what the loop needs first, query once, and save the records together.", "Test with a bulk load of at least 200 records in a sandbox."], ["The bulk test runs within the query and save limits, and the existing tests pass."]),
  },
  "tech.apex-code-size": {
    hours: fixed(8, 24, "Finding and retiring unused Apex, and planning what's left; more where code has to be rewritten"),
    prompt: change("Apex classes", ["List Apex classes by size, with whether each is referenced, run or covered by tests.", "Mark classes and test classes nobody calls as candidates to retire, and check each with its owner.", "Retire the agreed ones in a sandbox and run all tests."], ["The org's Apex is further from its size limit, and every test still passes."]),
  },
  "tech.storage-use": {
    hours: fixed(4, 16, "A plan for what to archive or delete, and setting it up; more where data has to move to another store"),
    prompt: change("objects and files", ["List the objects and files using the most storage, with how much of each is old or closed.", "Propose what to archive or delete, what must be kept and for how long, and where archived data goes.", "Set up the agreed archive in a sandbox first."], ["Storage use is below the warning line, and nothing kept by policy was lost."]),
  },
  "aut.old-api-versions": {
    hours: each("total", "component", "components", [30, 60], 24, "past that the components are tested together in one release"),
    prompt: change("components", ["List the Apex classes, triggers and flows on old API versions.", "Raise each to the current version in a sandbox, reading the release notes for behaviour that changed between the two.", "Run all tests and test each flow."], ["None of these components is below the agreed version, and the tests pass."]),
  },
  "aut.multiple-triggers": {
    hours: each("objects", "object", "objects", [240, 480], 40, "past that the objects share one handler pattern, so each further one is quicker"),
    prompt: change("objects", ["For each object, list its triggers and the order of what they do.", "Combine them into one trigger per object that calls a handler class, keeping the order of the logic.", "Test in a sandbox with single and bulk saves."], ["Each object has one trigger, and records save with the same results as before."]),
  },
  "tech.classic-leftovers": {
    hours: fixed(4, 16, "Replacing the JavaScript buttons, s-controls and Classic-only pages found; depends on what each does"),
    prompt: change("buttons and pages", ["List each JavaScript button, s-control and Classic-only Visualforce page, what it does and whether it's still used.", "Replace the ones still needed with a quick action, flow or Lightning component, and retire the rest.", "Test in Lightning Experience in a sandbox."], ["Nothing in use depends on Classic, and users in Lightning Experience can do the same work."]),
  },

  // Utilisation and adoption
  "utl.unassigned-licence-seats": {
    hours: fixed(1, 2, "Confirming headroom with the hiring plan and preparing the change for the renewal"),
    prompt: decide("licences", ["From Company Information, list each licence type with its total and used seats.", "Set out the spare seats against the hiring plan the business gives you.", "Prepare the reduction to ask for at renewal."], ["The renewal request names each licence and the seats to keep."]),
  },
  "utl.package-seats": {
    hours: fixed(1, 3, "Checking each installed app with its owner and preparing the change for the renewal"),
    prompt: decide("installed apps", ["List each installed app with its seats, how many are assigned, and when its users last logged in.", "Prepare questions for each app's owner on whether it's still needed.", "Draft the cancellations and seat reductions to ask for at renewal."], ["Each app has an owner's decision: keep, reduce or cancel."]),
  },
  "utl.dormant-licensed-users": {
    hours: each("users", "user", "users", [15, 30], 8, "past that confirmed users are deactivated in bulk"),
    prompt: change("users", ["For each user listed, confirm with their manager that they've left or moved role.", "Check they own no records, aren't named in flows, approval processes, assignment rules or scheduled jobs, and that nothing runs as them; move anything that does to an active user first.", "Only then deactivate the user and release the licence."], ["The users are inactive, nothing they owned or ran has stopped, and the licences show as free."]),
  },
  "utl.never-logged-in": {
    hours: each("users", "user", "users", [15, 30], 8, "past that confirmed users are deactivated in bulk"),
    prompt: change("users", ["For each user listed, confirm with their manager whether they still need access.", "Check they own no records, aren't named in automation, and that nothing runs as them.", "Deactivate those who don't need access and release the licence; help the rest to log in."], ["Each user is either active and logging in, or inactive with the licence free."]),
  },
  "utl.unused-reports": {
    hours: each("unused", "report", "reports", [2, 5], 8, "past that reports are moved to an archive folder in bulk"),
    prompt: decide("reports", ["List reports not run in the period, with their folder and last change.", "Propose a core report set for each team.", "Prepare the move of the rest to a clearly labelled archive folder, checking none feeds a dashboard or a subscription."], ["The proposal lists the core set and the reports to archive, with none in use on a dashboard."]),
  },
  "utl.dashboards-inactive-running-user": {
    hours: each("affected", "dashboard", "dashboards", [15, 30], 8, BULK),
    prompt: change("dashboards", ["For each dashboard listed, check who should see its data.", "Set its running user to a current user with that access, or a dedicated reporting user, or make it a dynamic dashboard.", "Refresh it and compare the figures."], ["Each dashboard refreshes, and shows the data its viewers should see."]),
  },
  "utl.eac-connections": {
    hours: each("broken", "connection", "connections", [15, 30], 8, BULK),
    prompt: change("users", ["In Einstein Activity Capture's settings, list the connections in an error state and the action Salesforce recommends for each.", "Ask each person to reconnect, or fix the connection as Salesforce recommends."], ["Every connection shows as healthy, and new emails and events appear on records."]),
  },
  "utl.eac-advanced-sync": {
    hours: fixed(1, 2, "Turning on one setting and checking that new events link to records"),
    prompt: change("settings", ["In Einstein Activity Capture's settings, turn on Relate Synced Events, in a sandbox first if the org has one with the feature.", "Create a test meeting with a contact on a deal."], ["New synced events show on the related records."]),
  },
  "adp.falling-adoption": {
    hours: fixed(8, 16, "Interviews with the teams, an analysis of what gets in their way, and a plan; the fixes it finds are their own work"),
    prompt: decide("teams", ["From login history, show how logins and active days changed by month for the groups named.", "List what changed in the org over the same months: new fields, validation rules, page layouts or automation.", "Prepare questions to ask the people who've stopped, and options to fix what they report."], ["The analysis shows when the fall began and what changed then."]),
  },
  "adp.low-engagement-group": {
    hours: each("groups", "team", "teams", [240, 480], 24, "past that the teams share most of the findings, so each further one is quicker"),
    prompt: decide("teams", ["For each group named, summarise how often its members log in and what they create or update.", "List the reports, dashboards and page layouts they have.", "Prepare questions about what they use instead, and options to bring that work into Salesforce."], ["The analysis shows each group's use against the rest of the org."]),
  },
  "adp.classic-users": {
    hours: each("users", "user", "users", [30, 60], 16, "past that users move in groups with one round of training"),
    prompt: change("users", ["Find what keeps each user on Classic: a page, a button or a tool that only works there.", "Replace or fix those, then switch the users to Lightning Experience, with the option to switch back removed once they're settled."], ["The users work in Lightning Experience and can do what they did in Classic."]),
  },

  // Data quality
  "dq.open-opps-past-close": {
    hours: fixed(2, 4, SET_UP_REPORT),
    prompt: change("deals", ["Build a report of open deals with a close date in the past, grouped by owner.", "Add it to the weekly pipeline review, and consider a validation rule that stops an open deal being saved with a past close date.", "Share the report with each owner to close the deal or give it a real date."], ["The report shows no open deal with a close date in the past after the next review."]),
  },
  "dq.opps-inactive-owner": {
    hours: fixed(1, 3, "Moving the deals with Salesforce's mass transfer and adding a step to the leaver process"),
    prompt: change("deals", ["List open deals owned by inactive users, with the team each sat in.", "Agree a new owner for each with the sales manager, then transfer them.", "Add reassigning open deals to the leaver checklist."], ["No open deal is owned by an inactive user."]),
  },
  "dq.open-opps-missing-amount": {
    hours: fixed(2, 4, SET_UP_REPORT),
    prompt: change("deals", ["Build a report of open deals with no amount, by stage and owner.", "Add a validation rule that requires an amount from the stage where a price is known, in a sandbox first.", "Share the report so owners fill the gaps."], ["No open deal past that stage has an empty amount."]),
  },
  "dq.open-opps-no-activity": {
    hours: fixed(2, 4, SET_UP_REPORT),
    prompt: change("deals", ["Check activity capture works first: email and calendar sync, and whether tasks are logged against deals.", "Build a report of open deals with no activity in 90 days, by owner.", "Share it for each owner to re-engage the deal or close it."], ["The report shrinks at each pipeline review."]),
  },
  "dq.opps-without-account": {
    hours: fixed(2, 4, SET_UP_REPORT),
    prompt: change("deals", ["Build a report of open deals with no account.", "Make the account required on the deal page layout and with a validation rule, in a sandbox first.", "Share the report for owners to link each deal."], ["No open deal lacks an account."]),
  },
  "dq.loss-reason-missing": {
    hours: fixed(2, 4, "Adding the rule and agreeing a short list of reasons; past deals aren't filled in"),
    prompt: change("lost deals", ["List the loss reason values and how often each is used.", "Propose a short list of reasons, and a validation rule that requires one when a deal is closed lost.", "Build it in a sandbox and test closing a deal with and without a reason."], ["A deal can't be closed lost without a reason."]),
  },
  "dq.records-inactive-owner": {
    hours: fixed(2, 6, "Moving the records in bulk and adding a step to the leaver process"),
    prompt: change("records", ["Count accounts, contacts and leads owned by inactive users, by object.", "Agree new owners with the business, then transfer them in bulk.", "Add reassigning records to the leaver checklist."], ["No account, contact or lead is owned by an inactive user."]),
  },
  "dq.duplicate-rules-missing": {
    hours: each("missing", "object", "objects", [120, 240], 12, "past that the rules share one matching approach"),
    prompt: change("objects", ["For each object named, create a matching rule on the fields that identify a record, such as name and email.", "Create a duplicate rule that uses it, in alert mode, in a sandbox first.", "Watch the alerts for a few weeks before blocking."], ["Each object has an active duplicate rule, and creating a known duplicate shows an alert."]),
  },
  "dq.duplicate-accounts": {
    hours: each("groups", "group", "groups", [15, 30], 24, "past that a deduplication tool merges in bulk"),
    prompt: change("accounts", ["For each group of accounts that share a name, compare their addresses, contacts, deals and owners.", "Mark the true duplicates and which record has the most history.", "Check integrations that hold the account's id before merging.", "Merge only the confirmed duplicates, then turn on a duplicate rule for accounts."], ["The confirmed groups have one account each, and a new duplicate shows an alert."]),
  },
  "dq.stale-leads": {
    hours: fixed(3, 6, "Agreeing a follow-up rule by source and setting up the archive; the reps' follow-up time isn't counted"),
    prompt: decide("leads", ["Count open leads with no activity, by source and age.", "Propose a follow-up rule for each source and an age past which leads are archived or sent back to marketing.", "Prepare the list view or report that shows leads breaking the rule."], ["The proposal covers every lead source."]),
  },
  "dq.contact-completeness": {
    hours: fixed(3, 6, SET_UP_REPORT),
    prompt: change("contacts", ["Build reports of contacts with no account and contacts with no email.", "Propose how orphaned contacts are matched to accounts, for example by email domain.", "Agree how missing emails get filled, and require an email on new contacts where that fits."], ["The share of contacts with no account or email falls."]),
  },
  "dq.record-type-use": {
    hours: fixed(1, 4, "Checking each record type's dependencies and deactivating the ones nobody needs"),
    prompt: change("record types", ["For each record type, check which profiles and permission sets give it, which default to it, and which flows, Apex, validation rules and integrations name it.", "Confirm with the object's owner that it's no longer needed.", "Only when nothing depends on it, deactivate it in a sandbox, then in production."], ["The record types left active are the ones in use, and nothing that named a deactivated one fails."]),
  },

  // Historic pipeline
  "pipe.unmapped-stages": {
    hours: fixed(1, 2, "Adding each stage to the health check's calibration"),
    prompt: decide("opportunity stages", ["List each opportunity stage with its probability, forecast category and whether it's closed or won.", "Prepare a proposed mapping of each stage to open, won or lost for the people who own the sales process to confirm."], ["Every stage in use has a confirmed mapping."]),
  },
  "pipe.calibration-contradicts-org": {
    hours: fixed(1, 3, "Agreeing which is right, the calibration or the stage set-up, and correcting the other"),
    prompt: decide("opportunity stages", ["List each stage's closed and won flags as set in Salesforce.", "Set out where the way the business uses a stage differs from those flags.", "Prepare the options: change the stage set-up, or keep it and record why."], ["Each difference has an agreed answer."]),
  },
  "pipe.segment-divergence": {
    hours: fixed(2, 4, PROCESS),
    prompt: decide("deals", ["For the segment named, compare its deals with the rest: deal size, type, record type, source and stage history.", "Check whether the sample is large enough to draw a conclusion.", "Prepare what's different and the options, without naming anyone."], ["The analysis explains the gap or shows the sample is too small."]),
  },
  "pipe.skipped-stages": {
    hours: fixed(4, 8, PROCESS),
    prompt: decide("stages", ["For each stage named, count won deals that skipped it, by team and record type.", "Set out whether it's part of how those teams sell.", "Prepare the options: remove the stage from their path, or agree its exit criteria."], ["Each skipped stage has an agreed answer."]),
  },
  "pipe.going-backwards": {
    hours: fixed(4, 8, PROCESS),
    prompt: decide("stages", ["From opportunity history, count moves back to an earlier stage, by stage and team.", "Draft exit criteria for each stage: what must be true before a deal moves on.", "Prepare a report that shows backward moves for pipeline reviews."], ["The exit criteria and the report are ready for the sales leaders to agree."]),
  },
  "pipe.dead-end-stage": {
    hours: fixed(2, 4, "Setting a time limit for the stage and the report or alert that enforces it"),
    prompt: change("stage", ["From won deals, find how long three quarters of them spent in the stage named.", "Build a report or a scheduled flow that flags open deals in that stage past the limit for review.", "Test the flow in a sandbox."], ["Deals past the limit are flagged each week."]),
  },

  // Security, integrations and best practice
  "sec.health-check-score": {
    hours: fixed(8, 16, "Working through the high-risk settings and planning the rest, whatever their number"),
    prompt: change("security settings", ["Open Security Health Check and list the settings marked high risk, then medium.", "For each, describe the standard value, what changes for users if it's applied, and any integration that could break.", "Apply the agreed changes in a sandbox first."], ["The Health Check score rises, and no user or integration is locked out."]),
  },
  "sec.health-check-risks": {
    hours: fixed(4, 8, "Bringing the high-risk settings up to standard and planning the medium ones"),
    prompt: change("security settings", ["For each setting named, set out its current and standard values and who it affects.", "Plan the change, with notice for users where it changes how they log in.", "Apply it in a sandbox first."], ["Each setting matches Salesforce's standard value, or has a recorded reason not to."]),
  },
  "sec.modify-all-data": {
    hours: each("unexpected", "person", "people", [60, 120], 8, "past that people share the same access, so one permission set serves them"),
    prompt: change("people", ["List the profiles and permission sets that grant Modify All Data to people outside the admin team.", "For each, find which objects they really need to edit.", "Build a permission set with only that access, assign it, and remove Modify All Data, in a sandbox first."], ["Only admins and agreed system accounts hold Modify All Data, and the others can still do their work."]),
  },
  "sec.view-all-data": {
    hours: each("unexpected", "person", "people", [60, 120], 8, "past that people share the same access, so one permission set serves them"),
    prompt: change("people", ["List the profiles and permission sets that grant View All Data to people outside the admin team.", "For each, find which objects they really need to read.", "Build a permission set with only that access, assign it, and remove View All Data, in a sandbox first."], ["Only admins and agreed system accounts hold View All Data, and the others can still see what they need."]),
  },
  "sec.sharing-defaults": {
    hours: each("openObjects", "object", "objects", [240, 480], 24, "past that the objects share one sharing design"),
    prompt: decide("objects", ["For each object named, set out its default access and who needs to edit its records.", "Draft the sharing rules, roles or teams that would give that access if the default were tightened.", "Note that tightening recalculates sharing and can change what people see, and prepare the options."], ["Each object has an agreed default, or a recorded reason to keep it open."]),
  },
  "sec.unused-access": {
    hours: fixed(2, 4, "Checking each unused profile and permission set and deleting the ones nobody needs"),
    prompt: change("profiles and permission sets", ["For each profile and permission set named, confirm no user holds it, and check it isn't named in flows, Apex or deployments.", "Check with whoever set it up.", "Delete the agreed ones, widest access first, in a sandbox first."], ["None of the agreed profiles or permission sets remains."]),
  },
  "sec.peer-outlier-access": {
    hours: each("people", "person", "people", [30, 60], 8, BULK),
    prompt: change("access grants", ["For each grant listed, find the permission set or setting that gives it.", "Ask the person's manager whether it's still needed.", "Remove it where it isn't, and record why where it is."], ["Every grant listed is removed or has a recorded reason."]),
  },
  "sec.personal-integration-accounts": {
    hours: each("accounts", "integration", "integrations", [240, 480], 24, "past that the integrations share one account pattern"),
    prompt: change("integrations", ["Find which integrations log in through the personal accounts listed, from login history and connected apps.", "For each, create an integration user with the Salesforce Integration licence, an API-only profile and a permission set with only the access it needs.", "Move the integration's credentials to it, in a sandbox first, and test every data flow."], ["The integrations log in as their own accounts and no personal account has API logins."]),
  },
  "int.integration-users-interactive": {
    hours: each("accounts", "account", "accounts", [30, 60], 4, BULK),
    prompt: change("integration accounts", ["For each integration account listed, add the API Only User permission through its profile or a permission set.", "Test that the integration still connects."], ["The accounts can't log in through the browser, and the integrations still run."]),
  },
  "int.integration-user-licences": {
    hours: each("movable", "account", "accounts", [60, 120], 8, BULK),
    prompt: change("integration accounts", ["For each account listed, create or convert an integration user on the Salesforce Integration licence with an API-only profile.", "Give it a permission set with only the access it needs.", "Move the integration in a sandbox first, test it, then reduce the full seats at renewal."], ["The integrations run on integration licences, and the full seats are free."]),
  },
  "sec.self-authorised-connected-apps": {
    hours: each("open", "app", "apps", [60, 120], 8, BULK),
    prompt: change("connected apps", ["For each app named, list who has used it, from its OAuth usage.", "Set it to admin-approved users are pre-authorised.", "Give access through a permission set to the people and integration accounts that need it."], ["Only assigned users can use the apps, and the integrations still run."]),
  },
  "sec.insecure-remote-sites": {
    hours: fixed(1, 4, "Moving each site to HTTPS and turning off the mixed-protocol setting"),
    prompt: change("remote sites", ["For each remote site named, check whether the address works over HTTPS.", "Change it to HTTPS and turn off the setting that lets code mix HTTPS and HTTP.", "Test every callout to it in a sandbox."], ["Every remote site uses HTTPS and callouts still work."]),
  },
  "int.hardcoded-endpoints": {
    hours: each("addresses", "address", "addresses", [120, 240], 24, "past that the named credentials share one pattern"),
    prompt: change("Apex classes", ["Find each callout address written into the code named.", "Create a named credential for it, with its authentication in an external credential.", "Change the code to call it with callout: and the credential's name, and test in a sandbox."], ["No callout address is written into code, and each callout still works."]),
  },
  "int.api-usage": {
    hours: fixed(4, 16, "Finding which integrations and jobs use the most and planning the cuts; changing them is more"),
    prompt: change("integrations and jobs", ["From the API usage and event logs you can see, list which users, apps and jobs make the most calls.", "Find repeats: calls that could be combined, polled less often, or moved to bulk loads or change events.", "Propose the changes with their owners."], ["Daily use falls below the warning line."]),
  },
  "int.failed-async-jobs": {
    hours: each("classes", "class", "classes", [120, 240], 16, BULK),
    prompt: change("Apex classes", ["For each class named, read the failed jobs' errors.", "Find and fix the cause in a sandbox, with a test that reproduces it.", "Send future failures to someone who will act on them."], ["The jobs run without failing, and a failure reaches a person."]),
  },
  "bp.users-without-role": {
    hours: each("hits", "user", "users", [10, 20], 8, BULK),
    prompt: change("users", ["List active users with no role, with their profile.", "Agree a role for each with the business, or confirm they don't need one.", "Assign the agreed roles."], ["Every user who sells or needs team data has a role."]),
  },
  "bp.ownership-skew": {
    hours: each("pairs", "owner and object", "owners and objects", [240, 480], 24, "past that the same spreading approach serves each pair"),
    prompt: change("records", ["For each owner and object named, find why one owner holds so many records.", "Propose how to spread them across more owners, or where to place the owner in the role hierarchy and sharing groups if it has to stay.", "Make the change in a sandbox first, at a quiet time."], ["No owner holds more than 10,000 records of one object, or the owner is placed as Salesforce advises."]),
  },
  "bp.after-save-same-record-updates": {
    hours: each("flows", "flow", "flows", [120, 240], 24, BULK),
    prompt: change("flows", ["In each flow named, find the updates to the record that started it.", "Move them to a before-save flow on the same object, leaving after-save actions where they are.", "Test in a sandbox with single and bulk saves."], ["The flows no longer update their own record after save, and records end up the same."]),
  },
  "bp.jobs-on-inactive-owners": {
    hours: each("jobs", "job", "jobs", [30, 60], 8, BULK),
    prompt: change("scheduled jobs", ["List scheduled jobs owned by inactive users, with what each runs and when.", "Check each is still running and still needed.", "Reschedule the needed ones under a dedicated account, then remove the old schedule."], ["No scheduled job belongs to an inactive user, and each runs on time."]),
  },

  // Current pipeline and forecast
  "cur.stuck-in-stage": {
    hours: fixed(2, 4, SET_UP_REPORT),
    prompt: change("deals", ["Build a report of open deals in their stage longer than three quarters of won deals took, by stage and owner.", "Add it to the pipeline review, where each deal is moved on with a dated next step or closed."], ["The report shrinks at each review."]),
  },
  "cur.stale-deals": {
    hours: fixed(2, 4, SET_UP_REPORT),
    prompt: change("deals", ["Check activity capture works first.", "Build a report of open deals with no stage change and no activity in 30 days, by owner.", "Share it for owners to re-engage each deal or close it."], ["The report shrinks at each review."]),
  },
  "cur.close-date-pushes": {
    hours: fixed(2, 4, "Building the report or alert that flags a second push for a manager"),
    prompt: change("deals", ["From opportunity field history, find deals whose close date moved more than once.", "Build a report, or a flow that alerts the manager, when a close date is pushed a second time.", "Test the flow in a sandbox."], ["A second push reaches a manager."]),
  },
  "cur.missing-next-steps": {
    hours: fixed(2, 4, SET_UP_REPORT),
    prompt: change("deals", ["Build a report of open deals with an empty next step.", "Consider a validation rule that requires a next step from an agreed stage, in a sandbox first.", "Add the report to pipeline reviews."], ["No open deal past that stage lacks a next step."]),
  },
  "cur.account-concentration": {
    hours: fixed(2, 4, PROCESS),
    prompt: decide("pipeline", ["Show how open pipeline splits across accounts, and how much the largest holds by stage and close date.", "Set out how much of it is likely, from stage and history.", "Prepare options for building pipeline elsewhere."], ["The analysis shows the exposure and the options."]),
  },
  "cur.rep-concentration": {
    hours: fixed(2, 4, PROCESS),
    prompt: decide("pipeline", ["Show how open pipeline splits across owners, without naming anyone.", "Set out the risk if the largest holder's deals slip.", "Prepare options for spreading coverage."], ["The analysis shows the exposure and the options."]),
  },
  "cur.coverage": {
    hours: fixed(2, 4, PROCESS),
    prompt: decide("pipeline", ["Show this quarter's quota, won and open pipeline by team.", "Set out the shortfall against the coverage target.", "Prepare the options: build more pipeline, or reset the number."], ["The leaders have the figures and options to decide this month."]),
  },
  "fct.manager-adjustments": {
    hours: fixed(2, 4, PROCESS),
    prompt: decide("forecast adjustments", ["Summarise the size of hand adjustments to the Commit forecast by period, without naming anyone.", "Compare the adjusted figures with what closed.", "Prepare a simple way to record the reason behind each large adjustment."], ["The analysis and the proposal are ready for the sales leaders."]),
  },
  "fct.quota-gaps": {
    hours: each("hits", "owner", "owners", [15, 30], 8, "past that quotas are loaded in bulk"),
    prompt: change("deal owners", ["List active users who own open deals but have no quota this period.", "Agree with sales leadership who carries a number.", "Load their quotas, or move their deals to someone who does."], ["Every owner of an open deal has a quota, or their deals have moved."]),
  },
  "fct.commit-at-risk": {
    hours: fixed(1, 3, SET_UP_REPORT),
    prompt: change("deals", ["Build a report of Commit deals with warning signs: a past or pushed close date, no recent activity or no next step.", "Add it to the forecast call for each deal to keep a dated reason or leave Commit."], ["Each committed deal has a dated reason or has left Commit."]),
  },
  "fct.early-stage-commits": {
    hours: fixed(1, 3, SET_UP_REPORT),
    prompt: change("deals", ["Build a report of Commit deals in early stages.", "Consider a validation rule that stops early-stage deals being marked Commit, in a sandbox first."], ["No deal in an early stage is in Commit."]),
  },

  // Activity and leads
  "act.event-link-collapse": {
    hours: fixed(2, 6, "Finding what changed when the drop began and correcting the set-up"),
    prompt: change("events", ["Find what changed when the drop began: Einstein Activity Capture settings, calendar sync, or a new tool.", "Check the Relate Synced Events setting.", "Correct the set-up in a sandbox first where you can."], ["New events link to records at the rate they did before the drop."]),
  },
  "act.deals-without-activity": {
    hours: fixed(4, 8, "Agreeing what's worth recording and setting up capture or the steps to log it"),
    prompt: decide("deals", ["Show the share of won and lost deals with no activity, by team.", "Propose the few moments worth recording on a deal, or automatic capture of email and calendar.", "Prepare how each would be set up."], ["The proposal is ready for the sales leaders."]),
  },
  "act.won-vs-lost-activity": {
    hours: fixed(2, 4, PROCESS),
    prompt: decide("deals", ["Compare logged activity on won and lost deals, by team and stage.", "Check whether activity is logged evenly before drawing a conclusion.", "Prepare what the comparison shows and the options."], ["The analysis is ready for the sales leaders."]),
  },
  "act.rep-activity-spread": {
    hours: fixed(1, 3, "Checking the email and calendar connections and preparing the comparison"),
    prompt: change("reps", ["Check each listed user's email and calendar connection and activity capture settings.", "Fix any broken connection.", "Prepare a comparison of logged activity by deal, without naming anyone, for the manager."], ["Connections are healthy, and the gap is explained."]),
  },
  "act.unlinked-tasks": {
    hours: fixed(2, 4, "Finding which tools create unlinked tasks and fixing them"),
    prompt: change("tasks", ["Find which tools or automations create tasks with no related record.", "Fix those that can link to the right record.", "Prepare a short note on logging against the deal."], ["The share of unlinked tasks falls."]),
  },
  "lead.conversion-by-source": {
    hours: fixed(2, 4, PROCESS),
    prompt: decide("lead sources", ["For the source named, compare conversion, response time and follow-up with other sources.", "Set out whether the leads are poor or poorly handled.", "Prepare the options."], ["The analysis is ready for marketing and sales."]),
  },
  "lead.response-time": {
    hours: fixed(2, 6, "Setting a response rule by source and the routing or alert behind it"),
    prompt: change("lead sources", ["Show the median time to first response by source.", "Propose a response target for each source.", "Set up routing or an alert so slow sources reach someone who can meet it, in a sandbox first."], ["Response time for each source is within its target."]),
  },
  "lead.no-first-response": {
    hours: fixed(2, 4, "Checking routing and fixing the rules that leave leads unworked"),
    prompt: change("leads", ["List leads with no response, by source, owner type and queue.", "Check the lead assignment rules and queues that hold them.", "Fix the routing in a sandbox first."], ["New leads are assigned to someone and get a response."]),
  },
  "lead.converted-no-opportunity": {
    hours: fixed(1, 3, PROCESS),
    prompt: decide("leads", ["Show converted leads with no opportunity, by source and owner type.", "Propose when a lead should be converted, and how tidy-up conversions are kept out of conversion figures."], ["The rule is ready for sales and marketing to agree."]),
  },
  "lead.source-return": {
    hours: fixed(2, 4, PROCESS),
    prompt: decide("lead sources", ["For the source named, show leads, conversions and won value.", "Check how its leads are handled.", "Prepare the options for keeping or dropping it."], ["The analysis is ready for marketing."]),
  },
  "lead.campaign-return": {
    hours: fixed(2, 4, PROCESS),
    prompt: decide("campaign types", ["For the campaign type named, show spend against won value.", "Check the attribution: contacts on a campaign whose deals aren't counted.", "Prepare the options for the spend."], ["The analysis is ready for marketing."]),
  },

  // Revenue opportunities
  "rev.stalled-deals": {
    hours: fixed(2, 4, PROCESS),
    prompt: decide("deals", ["List the stalled open deals, by owner and amount, without naming anyone.", "Prepare a plan to give each owner a date to re-engage or close them."], ["Each stalled deal has a date."]),
  },
  "rev.win-backs": {
    hours: fixed(2, 4, PROCESS),
    prompt: decide("lost deals", ["List the lost and no decision deals worth another look, no decision first.", "Summarise why each group stopped, from loss reasons and history.", "Prepare a plan for checking each one."], ["The plan is ready for the sales leaders."]),
  },
  "rev.dormant-accounts": {
    hours: fixed(2, 4, PROCESS),
    prompt: decide("accounts", ["List past customers with no recent activity, with their last deal and whether their owner is active.", "Prepare a plan for who owns them and how to check in."], ["Each account has an owner and a next step."]),
  },

  // Service
  "svc.backlog-growth": {
    hours: fixed(4, 8, PROCESS),
    prompt: decide("cases", ["Show cases opened and closed by month, by priority, queue and type.", "Find where the growth sits and any cause such as a product fault or a missing article.", "Prepare the options: capacity, routing, or fixing the cause."], ["The analysis is ready for the service leaders."]),
  },
  "svc.backlog-age": {
    hours: fixed(2, 4, SET_UP_REPORT),
    prompt: change("cases", ["Build a report of open cases older than the agreed age, oldest first, by queue and priority.", "Add it to a weekly review, and set up escalation for the oldest."], ["The share of old cases falls each week."]),
  },
  "svc.priority-resolution": {
    hours: fixed(4, 8, "Routing high-priority cases to their own queue or routing priority, with a visible target"),
    prompt: change("cases", ["Show resolution time by priority.", "Route high-priority cases to their own queue or a higher routing priority, in a sandbox first.", "Add a resolution target the team can see."], ["High-priority cases are resolved faster than the rest."]),
  },
  "svc.milestone-breaches": {
    hours: fixed(4, 8, "Reviewing the milestones missed most and adding warning actions"),
    prompt: change("milestones", ["Show breaches by milestone type, entitlement process and business hours.", "Check whether the targets, the business hours or the staffing are wrong.", "Add warning actions so agents see a breach coming, in a sandbox first."], ["The breach rate falls below the line."]),
  },
  "svc.first-reply-email": {
    hours: fixed(2, 6, "Setting up an acknowledgement and routing so email cases get a timely first reply"),
    prompt: change("email cases", ["Show time to first reply on email cases, by queue.", "Set up an auto-response and routing so a person replies within the target, in a sandbox first."], ["Email cases get a first reply within the target."]),
  },
  "svc.reopened-cases": {
    hours: fixed(2, 6, "Finding which case types reopen and changing how they're closed"),
    prompt: change("cases", ["Show reopened cases by type, reason and queue.", "Propose a confirmation step before closing for the types that reopen most.", "Build it in a sandbox first."], ["The reopen rate falls below the line."]),
  },
  "svc.unassigned-cases": {
    hours: fixed(2, 6, "Giving queues members and routing work from busy queues"),
    prompt: change("queues", ["List each case queue with its members and its oldest open case.", "Give every queue members who watch it, and route work from busy queues automatically, in a sandbox first."], ["No case waits in a queue past the agreed time."]),
  },
  "svc.origin-missing": {
    hours: fixed(2, 4, "Setting Origin automatically for each channel and requiring it by hand"),
    prompt: change("cases", ["List each channel cases come in through.", "Set Origin automatically for each: Email-to-Case, web forms, messaging and phone.", "Require Origin when an agent creates a case by hand, in a sandbox first."], ["New cases all have an Origin."]),
  },
  "svc.case-classification": {
    hours: fixed(2, 4, "Agreeing a short list of types or reasons and requiring one on closing"),
    prompt: change("cases", ["Show how Type and Reason are used, and their values.", "Propose a short agreed list, and require one on closing, in a sandbox first."], ["Closed cases all record why the customer got in touch."]),
  },
  "svc.case-customer": {
    hours: fixed(2, 6, "Matching incoming cases to contacts automatically"),
    prompt: change("cases", ["Show open cases with no contact or account, by channel.", "Set up matching of incoming email and web cases to contacts, in a sandbox first."], ["New cases are linked to a customer."]),
  },
  "svc.escalation-rules": {
    hours: fixed(2, 6, "Adding one escalation rule or an entitlement process"),
    prompt: change("escalation rules", ["Agree the time after which an open case should escalate, by priority.", "Add an escalation rule that moves those cases to a team lead's queue and tells them, in a sandbox first."], ["A test case left open past the time escalates."]),
  },
  "svc.escalation-targets": {
    hours: each("dead", "escalation action", "escalation actions", [30, 60], 4, BULK),
    prompt: change("escalation actions", ["List escalation actions that point at an inactive user or an empty queue.", "Point each at a queue with members, not a named person.", "Add escalation targets to the leaver checklist."], ["Every escalation reaches someone who can act."]),
  },
  "svc.queue-sprawl": {
    hours: fixed(2, 4, "Merging or removing queues nobody works and giving the rest members"),
    prompt: change("queues", ["List each case queue with its members, open cases and the routing and rules that use it.", "Merge or remove queues nobody works, once nothing routes to them.", "Give every remaining queue members, in a sandbox first."], ["Every queue has members and is used."]),
  },
  "svc.omni-routing": {
    hours: each("unrouted", "queue", "queues", [60, 120], 8, BULK),
    prompt: change("queues", ["For each case queue without a routing configuration, decide whether Omni-Channel should push its work.", "Give those a routing configuration, in a sandbox first.", "Remove configurations no queue uses."], ["Every case queue is routed, or left out on purpose."]),
  },
  "svc.agent-licences": {
    hours: each("wasted", "licence", "licences", [15, 30], 4, BULK),
    prompt: change("Service Cloud users", ["List Service Cloud users who don't work cases.", "Confirm with each manager.", "Remove the licence, or move them to a licence that fits."], ["Every Service Cloud licence is held by someone who works cases."]),
  },
  "svc.knowledge-use": {
    hours: fixed(2, 6, "Putting Knowledge on the case page and setting up the reporting"),
    prompt: change("knowledge articles", ["Add the Knowledge component to the case page, in a sandbox first.", "Build a report of articles attached to closed cases, by case type.", "List the common case types with no article."], ["More closed cases have an article attached."]),
  },
  "svc.knowledge-current": {
    hours: each("stale", "article", "articles", [15, 30], 24, "past that articles are reviewed in batches by owner"),
    prompt: change("knowledge articles", ["List articles not reviewed in the period, with their views and case types.", "Give each an owner and a review date.", "Archive what nobody reads, and update the articles behind common case types first."], ["Most articles have been reviewed within the period."]),
  },
  "svc.macros-and-quick-text": {
    hours: fixed(2, 6, "Removing unused macros and quick text and rebuilding Classic-only macros"),
    prompt: change("macros and quick text", ["List macros and quick text with when each was last used.", "Remove those unused in a year, once their owner agrees.", "Rebuild Classic-only macros for Lightning, and give the rest an owner."], ["Every macro and quick text left is used and owned."]),
  },
  "aut.case-routing-overlap": {
    hours: fixed(4, 8, "Separating assignment rules and Omni-Channel so each case is routed once"),
    prompt: change("assignment rules", ["List case assignment rules and Omni-Channel configurations, and which queues each touches.", "Change rules so they only put cases into queues, and let Omni-Channel push them to agents.", "Test in a sandbox."], ["No rule assigns cases straight to people, and each case is routed once."]),
  },

  // CPQ
  "cpq.price-rule-conflicts": {
    hours: each("pairs", "pair", "pairs", [120, 240], 16, BULK),
    prompt: change("price rules", ["For each pair named, compare their conditions, actions and evaluation order.", "Give them different evaluation orders, or merge them.", "Compare quote totals before and after in a sandbox."], ["Quote totals are as intended and no two rules set the same field at the same point."]),
  },
  "cpq.twin-field-mismatch": {
    hours: each("mismatches", "pair", "pairs", [60, 120], 8, BULK),
    prompt: change("twin fields", ["For each field named, compare its type on the quote line and its twin.", "Give the pair the same type, or rename one if they aren't meant to be twins, in a sandbox first."], ["The values copy across as intended on a test quote."]),
  },
  "cpq.inactive-rules": {
    hours: each("inactive", "rule", "rules", [15, 30], 8, BULK),
    prompt: change("rules", ["List each inactive price and product rule with when it was last changed.", "Check each with whoever owns pricing.", "Delete the ones nobody will use, in a sandbox first."], ["Only rules someone will use again remain."]),
  },
  "cpq.discount-leakage": {
    hours: fixed(4, 8, PROCESS),
    prompt: decide("discounts", ["Show discount against win rate by deal size, without naming anyone.", "Draft a discount policy with limits by deal size.", "Prepare how deeper discounts would go through approval."], ["The policy is ready for the sales leaders."]),
  },
  "cpq.renewals-not-started": {
    hours: fixed(2, 6, "Creating the renewals due and setting CPQ to create them automatically"),
    prompt: change("contracts", ["List contracts ending soon with no renewal opportunity.", "Create each renewal and give it an owner.", "Set CPQ to create renewals automatically, in a sandbox first."], ["Every contract ending soon has a renewal."]),
  },
  "cpq.discount-approvals": {
    hours: fixed(8, 16, "Building an approval process on the quote for the agreed discount levels"),
    prompt: change("quotes", ["Agree the discount levels that need sign-off.", "Build an approval process on the quote for them, in a sandbox first.", "Prepare a report of past deep discounts and who agreed them."], ["A test quote over the level needs approval."]),
  },
  "cpq.quote-to-order": {
    hours: fixed(2, 4, SET_UP_REPORT),
    prompt: change("quotes", ["Build a report of won quotes with no order.", "Make ordering part of closing a deal, with a check or automation, in a sandbox first."], ["New won quotes become orders."]),
  },

  // Marketing handover
  "mkt.unworked-handovers": {
    hours: fixed(2, 4, SET_UP_REPORT),
    prompt: change("leads", ["Build a report of marketing leads handed to sales and not worked, by source and age.", "Agree a follow-up time, and send unworked leads back to marketing to nurture."], ["Handed leads are worked within the agreed time."]),
  },
  "mkt.scoring-grading": {
    hours: fixed(4, 8, "Setting up grading against the ideal customer profile and checking scoring"),
    prompt: change("leads", ["Show synced leads with no score or grade.", "Set up grading against the ideal customer profile, and check scoring rewards real buying signals.", "Hand leads to sales on both."], ["Synced leads carry a score and a grade."]),
  },
  "mkt.unowned-prospects": {
    hours: fixed(1, 3, "Reassigning the leads and fixing the assignment rules"),
    prompt: change("leads", ["List synced leads in a queue or with an inactive owner.", "Assign each to an active salesperson or back to marketing.", "Fix the assignment rules that left them there, in a sandbox first."], ["No synced lead sits unowned."]),
  },
  "mkt.campaign-influence": {
    hours: fixed(1, 2, "Turning on one setting and choosing the model to report on"),
    prompt: change("campaign settings", ["In the campaign influence settings, allow influence models from Account Engagement.", "Choose the model you'll report on."], ["Opportunities show Account Engagement's attribution."]),
  },
};
