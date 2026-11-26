import type { FixRule, HoursRule, PromptRule } from "./types.ts";

// Every rule that can raise an issue, with its hours to fix and the fixed text of
// its fix prompt (DECISIONS.md 628).
//
// ESTIMATES AWAITING THE OWNER'S REVIEW. The hours are the builder's first
// estimates for a fix led by a consultant who knows Salesforce. They are a
// commercial claim, so no document prints them until Coby has approved them.
// `npm run effort-hours` writes them all to fixtures/effort-hours.json for him to
// read and change in one place, and a test fails when the two differ. Too low is
// the dangerous direction: a range is never read as covering more than it says,
// so where work grows with every item the cap says how many items it covers.
//
// Each rule also declares its fix complexity (DECISIONS.md 636): how risky or
// skilled the fix is, which the hours don't say. The sort, in this order:
// complex when the fix deactivates a user, changes a user's role, profile or
// preferences, moves record ownership, removes a person's access or permission,
// changes sharing, security settings, or an integration's account, licence or
// calls, changes Apex or replaces a production page people work in, migrates or
// restructures automation, changes pricing, approvals or routing between two
// systems, or retypes, merges or remaps values, records or set-up others depend
// on (picklist values, stages, queues, duplicates, twin fields, scoring), or
// updates records in bulk; moderate when it is one contained declarative change
// built and tested in a sandbox (a validation rule, one flow's fault path or
// hardcoded value, a notification flow, a page component, routing within one
// tool, test classes), a seat or feature licence count, or a review of set-up or
// content whose dependencies must be traced before anything is removed (unused
// fields, layouts, record types, unused profiles, inactive pricing rules,
// reports, articles, macros, calibration); simple when it is a report, a
// description or help text, a setting that only changes how activity or
// attribution links, or analysis and a proposal for people to agree with nothing
// to trace. It is not the org's complexity score (DECISIONS.md 614).
//
// Beside it, `touches` says what else a fix reaches, for the difficulty a reader
// sees (DECISIONS.md 714): `leavesOut` where the hours count only our preparation
// and leave out the people deciding or making the change (every PROCESS and PLAN
// basis, and the two others that say so), `integration` where it changes how an
// integration or connected tool is set up, `automation` where it changes
// automation or what users can save (a flow, routing, escalation, assignment, or
// a validation or duplicate rule, even one built in a sandbox for the owners to
// turn on), and `mayNotUndo` where it turns on a setting we can't confirm can be
// turned off. A rule whose prompt `removes` set-up counts as well. Any of these,
// or a complex fix, is never a quick fix, so a moderate fix that changes
// automation is "Needs planning" however few hours it takes.
//
// Two principles bind every prompt here:
//
// A. A fix prompt never asks the assistant to change records. Reassigning
//    owners, merging, updating field values in bulk, moving data out, removing
//    records, making opportunities by hand, switching users off: none of these
//    is ever a step. Where the fix needs records changed, the prompt is analysis
//    only (`decide`): it prepares the list, the mapping and the plan for the
//    people who own the records, and stops. Metadata changes are steps only in a
//    sandbox first, and only where they can be undone; removing set-up can't be,
//    so it is analysis only too.
// B. A fix prompt never takes away anyone's access or stops an integration as a
//    first step. Where a security fix tightens access, the steps grant the
//    replacement access first, name what to check before tightening, and say
//    what would break if the order were reversed.

const fixed = (low: number, high: number, basis: string): HoursRule => ({ kind: "fixed", low, high, basis });
const each = (facts: readonly string[] | "all", one: string, many: string, minutes: readonly [number, number], cap: number, bulk?: string): HoursRule => ({ kind: "each", facts, one, many, minutes, cap, ...(bulk ? { bulk } : {}) });

const change = (items: string, steps: string[], verify: string[]): PromptRule => ({ items, steps, verify });
const decide = (items: string, steps: string[], verify: string[]): PromptRule => ({ items, decision: true, steps, verify });

// Basis sentences reused across rules.
const REPORT = "Building the report and the check that keeps it current; the owners' time updating their own records isn't counted";
const PROCESS = "Preparing the analysis and the options for the people who decide; their time deciding, and making the change, isn't counted";
const PLAN = "Preparing the list and the plan for the people who own the records; their time making the change isn't counted";

/** The rules that only ever pass or are not measurable, so they declare nothing. A test reads each one's code to hold this. */
export const NEVER_RAISES: ReadonlySet<string> = new Set(["utl.dashboard-usage", "fct.forecasting-in-use", "fct.forecast-accuracy"]);

export const FIX_TABLE: Readonly<Record<string, FixRule>> = {
  // Configuration
  "cfg.custom-field-sprawl": {
    complexity: "moderate",
    hours: each("all", "custom field on the objects named", "custom fields on the objects named", [5, 10], 35),
    prompt: decide("objects", ["For each object named, list its custom fields with their fill rate, when each was created and last changed, and where each is used (layouts, flows, Apex, reports, validation rules).", "Group the fields into keep, review and candidate to retire, with the reason for each.", "Prepare the list for the object's owners to agree."], ["Every custom field on the objects named is in one of the three groups, with a reason."]),
  },
  "cfg.unused-custom-fields": {
    complexity: "moderate",
    hours: each(["unusedFields"], "field almost never filled in", "fields almost never filled in", [30, 55], 14),
    prompt: {
      items: "fields",
      decision: true,
      removes: true,
      steps: ["For each field, list everything that references it: page layouts, flows, Apex, formulas, validation rules, reports, list views and integrations.", "List any data in it that must be kept, and where the owners could keep it.", "Prepare the plan for the owners: take the field off page layouts first, wait an agreed period for anyone who misses it, and only then remove it, in a sandbox before production."],
      verify: ["Each field has a list of what references it and a plan its owner has agreed."],
    },
  },
  "cfg.unassigned-layouts": {
    complexity: "moderate",
    hours: each(["unassigned"], "unassigned layout", "unassigned layouts", [15, 25], 7),
    prompt: decide("page layouts", ["For each layout, confirm in the page layout assignment that no profile and record type uses it.", "Check it isn't named in a deployment, a managed package or documentation people rely on.", "Prepare the list of layouts nobody needs for the org's owner to decide on."], ["Each layout has a confirmed assignment check and an owner's decision."]),
  },
  "cfg.undocumented-fields": {
    complexity: "simple",
    hours: each(["bare"], "field with no description or help text", "fields with no description or help text", [10, 20], 21),
    prompt: change("fields", ["List the custom fields with no description or help text on the objects named.", "Draft a description of what each field holds and why, and help text for the ones people fill in, from its label, type, values and where it's used.", "Flag any field whose purpose you can't work out for its owner to explain.", "Add the agreed text in a sandbox, then in production."], ["Every custom field on these objects has a description, and every field on a page layout has help text."]),
  },
  "cfg.stage-sprawl": {
    complexity: "complex",
    touches: { leavesOut: true },
    hours: fixed(4, 7, PROCESS),
    prompt: decide("opportunity stages", ["List each opportunity stage with its probability, forecast category, how many open and closed deals sit in it, and which sales processes use it.", "Propose a shorter set of stages that each mark real buyer progress, with exit criteria for each.", "Map every current stage to one in the proposal, and list what depends on stage names: flows, validation rules, reports and paths."], ["The proposal covers every stage in use, and every dependency on a stage name is listed."]),
  },
  "cfg.picklist-sprawl": {
    complexity: "complex",
    touches: { leavesOut: true },
    hours: fixed(2, 5, "Preparing the use of each value, a shorter list and the mapping from old values to new for the list's owners; changing the records isn't counted"),
    prompt: decide("picklists", ["For each picklist named, count how often each value is used on records made in the last 12 months.", "Propose a shorter list, and map each old value to a new one.", "List the flows, validation rules, reports and integrations that name a value.", "Prepare the mapping and the plan for the list's owners to agree before any record changes."], ["The mapping covers every value in use, and every dependency on a value is listed."]),
  },
  "cfg.setup-change-concentration": {
    complexity: "simple",
    hours: fixed(4, 7, "Writing down how the org is changed and what a second person needs to learn, whatever the size of the org"),
    prompt: decide("setup changes", ["From the Setup Audit Trail, summarise the kinds of change made in the period by area (users, fields, flows, security), without naming anyone.", "Draft a short change process: who can change what, where changes are tested, and how they're recorded.", "List what a second person needs to learn to cover the work."], ["The change process and the list of skills are ready for the business to agree."]),
  },

  // Automation and technical health
  "aut.legacy-workflow-rules": {
    complexity: "complex",
    hours: each(["activeRules"], "active workflow rule", "active workflow rules", [60, 160], 53),
    prompt: change("workflow rules", ["Group the active workflow rules by object, and for each list its criteria, field updates, emails, tasks and time-based actions.", "In a sandbox, build one record-triggered flow for each object that does the same work, using Migrate to Flow where it gives a like-for-like result.", "Still in the sandbox, activate the flow, switch off the rules it replaces, and test each path with records that meet and miss the criteria. Keep the rules, so they can be switched back on."], ["On each object, the flow produces the same field updates, emails and tasks as the rules did, and the rules are inactive."]),
  },
  "aut.legacy-process-builder": {
    complexity: "complex",
    hours: each(["activeProcesses"], "active process", "active processes", [120, 210], 53),
    prompt: change("processes", ["List each active Process Builder process with its object, criteria and actions.", "In a sandbox, move each to a record-triggered flow on the same object, together with any workflow rules on it, so each object is migrated once.", "Still in the sandbox, activate the flow, switch off the process, and test every criteria node. Keep the process, so it can be switched back on."], ["Each object's flow does what its process did, and the process is inactive."]),
  },
  "aut.mixed-automation-on-object": {
    complexity: "complex",
    hours: each(["objects"], "object with mixed automation", "objects with mixed automation", [240, 420], 35),
    prompt: change("objects", ["For each object named, list every trigger, flow, process and workflow rule that runs on it, with when it runs and what it changes.", "Propose which work belongs in Apex and which in Flow, and the order they run in.", "In a sandbox, move the older tools' work into that split one object at a time, and run the Apex tests and a test of each path."], ["Each object's automation runs in the agreed tools only, and records save with the same results as before."]),
  },
  "aut.hardcoded-ids-in-flows": {
    complexity: "moderate",
    touches: { automation: true },
    hours: each(["affected"], "flow with hardcoded values", "flows with hardcoded values", [60, 105], 14),
    prompt: change("flows", ["Open each flow and list every record id and email address typed into it.", "In a sandbox, replace a queue's or public group's id with a lookup of it by its developer name; for a person, use a custom setting, a custom metadata record or a role instead of a user id, and never type a username into the flow; put any other value in a custom label or custom metadata record. Save each as a new flow version.", "Test each path that used a replaced value."], ["No flow version that is active holds a typed record id or email, and each path still reaches the same record, group or person."]),
  },

  "aut.flows-without-fault-paths": {
    complexity: "moderate",
    touches: { automation: true },
    hours: each(["affected"], "flow with no fault path", "flows with no fault path", [60, 105], 21),
    prompt: change("flows", ["In a sandbox, build one subflow that records a failure (the flow, the element and the error message) and tells an admin.", "In each flow, add a fault path from every element that creates, updates or removes records, leading to that subflow, as a new flow version.", "Test by making each save fail, for example with a validation rule in the sandbox."], ["Every data element in these flows has a fault path, and a forced failure is recorded and reaches an admin."]),
  },
  "aut.undocumented-automation": {
    complexity: "simple",
    hours: each(["missing"], "flow, rule or class with no description", "flows, rules and classes with no description", [10, 20], 21),
    prompt: change("flows, rules and classes", ["List the flows, workflow rules, processes and Apex classes with no description, busiest objects first.", "Draft a description of what each does and why, from its trigger, criteria and actions or code.", "Flag any you can't explain for its owner, and add the agreed descriptions in a sandbox, then in production."], ["Every active flow, rule and class has a description."]),
  },
  "aut.apex-coverage": {
    complexity: "moderate",
    hours: each(["weakClasses"], "class below the coverage line", "classes below the coverage line", [120, 210], 35),
    prompt: change("Apex classes", ["List the classes below the coverage line, weakest first, and the lines each test doesn't reach.", "In a sandbox, write tests that make their own data and assert the outcome of each path, not just run the code.", "Run all tests."], ["Each class named is above the coverage line, every test asserts something, and the whole test run passes."]),
  },
  "tech.test-quality": {
    complexity: "moderate",
    hours: each(["withoutAssertions", "seeAllData"], "test class problem (no assertion, or reading the org's data)", "test class problems (no assertion, or reading the org's data)", [60, 160], 35),
    prompt: change("test classes", ["List the test classes with no assertion and those that read the org's data.", "In a sandbox, add assertions that check each test's outcome, and have each test make the records it needs instead of reading the org's.", "Run all tests."], ["No test class lacks an assertion or reads the org's own data, and the test run passes."]),
  },
  "tech.governor-limit-risk": {
    complexity: "complex",
    hours: each(["risky"], "class or trigger with a query or save in a loop", "classes and triggers with a query or save in a loop", [120, 210], 35),
    prompt: change("Apex classes and triggers", ["Find each query and save made inside a loop in the code named.", "In a sandbox, rewrite it to gather what the loop needs first, query once, and save the records together.", "Test with a bulk load of at least 200 test records in the sandbox."], ["The bulk test runs within the query and save limits, and the existing tests pass."]),
  },
  "tech.apex-code-size": {
    complexity: "complex",
    hours: fixed(8, 21, "Finding the Apex nobody calls and preparing the plan for its owners; rewriting code is more"),
    prompt: decide("Apex classes", ["List Apex classes by size, with whether each is referenced, run or covered by tests.", "Flag classes and test classes nobody calls as candidates to retire.", "Prepare the list for each class's owner to decide."], ["Each candidate has its references listed and an owner's decision."]),
  },
  "tech.storage-use": {
    complexity: "complex",
    hours: fixed(4, 14, "Preparing the plan for what can leave Salesforce and what must be kept; carrying it out is more"),
    prompt: decide("objects and files", ["List the objects and files using the most storage, with how much of each is old or closed.", "Propose what is no longer needed, what must be kept and for how long, and where data that leaves Salesforce would be kept.", "Prepare the plan for the data's owners to agree."], ["The plan covers the largest objects and files, and their owners have agreed it."]),
  },
  // PMD's six groups (DECISIONS.md 673): hours for each class or trigger the finding names.
  "tech.lint-data-access": {
    complexity: "complex",
    hours: each(["components"], "class or trigger that reads or changes data without checking access", "classes and triggers that read or change data without checking access", [60, 150], 35),
    prompt: change("Apex classes and triggers", ["List each place in the code named that reads or changes records without checking access, and who uses it: which pages, flows, integrations and users run it.", "Before adding any check, list who would lose access once it is in place and what they would no longer be able to do, and agree that list with the data's owner.", "Don't grant any new permission to make a check pass; list who would lose access and agree it with the data's owner.", "In a sandbox, once the owner has agreed, check object and field permissions before each read and save, and say on each class whether it runs with sharing, writing down why where it must run without.", "Run all tests, and test each changed path as a user with limited access."], ["Each path reads and changes only what the running user may, the data's owner has agreed who loses access, and the tests pass."]),
  },
  "tech.lint-limits": {
    complexity: "complex",
    hours: each(["components"], "class or trigger with a limit risk", "classes and triggers with limit risks", [60, 150], 35),
    prompt: change("Apex classes and triggers", ["Find each limited or costly call inside a loop, and each query with no filter and no limit, in the code named.", "In a sandbox, move each call out of its loop, and give each query a filter or a limit.", "Test with a bulk load of at least 200 test records in the sandbox."], ["The bulk test runs within Salesforce's limits, and the existing tests pass."]),
  },
  "tech.lint-injection": {
    complexity: "complex",
    hours: each(["components"], "class or trigger open to injection or an unsafe redirect", "classes and triggers open to injection or unsafe redirects", [60, 180], 35),
    prompt: change("Apex classes and triggers", ["List each place in the code named, and what a user can supply there.", "In a sandbox, bind what users supply into queries, escape what pages show, check each redirect against the addresses you allow, move fixed keys into protected settings, and, once each other system is confirmed to accept https, call it over https.", "Run all tests, and test each changed page, query and callout."], ["No query, page or redirect takes what a user supplies unchecked, every callout uses https, and the tests pass."]),
  },
  "tech.lint-tests": {
    complexity: "moderate",
    hours: each(["components"], "test class with test methods to fix", "test classes with test methods to fix", [30, 90], 35),
    prompt: change("test classes", ["List the test methods in the classes named that have no assertion or read the org's data.", "In a sandbox, add assertions that check each method's outcome, and have each test make the records it needs.", "Run all tests."], ["Every test method checks its outcome without reading the org's data, and the test run passes."]),
  },
  "tech.lint-swallowed-errors": {
    complexity: "complex",
    hours: each(["components"], "class or trigger that ignores errors it catches", "classes and triggers that ignore errors they catch", [20, 60], 35),
    prompt: change("Apex classes and triggers", ["List each catch block in the code named that does nothing, and what can fail there.", "In a sandbox, log each error, and tell the user or an admin where it matters, or let it fail where people will see it.", "Run all tests, and make each error happen once to see it reported."], ["No error is caught and ignored, and the tests pass."]),
  },
  "tech.lint-complexity": {
    complexity: "complex",
    hours: each(["components"], "class or trigger too complex to change safely", "classes and triggers too complex to change safely", [120, 360], 21),
    prompt: change("Apex classes and triggers", ["List the most complex methods in the code named, with how often each has changed.", "Plan to split them into smaller methods, each with its own tests, starting with the code that changes most.", "In a sandbox, make the first split and run all tests."], ["The first methods are split, each with its own tests, and the test run passes."]),
  },
  "aut.old-api-versions": {
    complexity: "complex",
    hours: each(["total"], "component on an old API version", "components on an old API version", [30, 55], 21),
    prompt: change("components", ["List the Apex classes, triggers and flows on old API versions.", "In a sandbox, raise each to the current version, reading the release notes for behaviour that changed between the two.", "Run all tests and test each flow."], ["None of these components is below the agreed version, and the tests pass."]),
  },
  "aut.multiple-triggers": {
    complexity: "complex",
    hours: each(["objects"], "object with more than one trigger", "objects with more than one trigger", [240, 420], 35),
    prompt: change("objects", ["For each object, list its triggers and the order of what they do.", "In a sandbox, combine them into one trigger per object that calls a handler class, keeping the order of the logic.", "Test with single and bulk saves."], ["Each object has one trigger, and records save with the same results as before."]),
  },
  "tech.classic-leftovers": {
    complexity: "complex",
    hours: each(["javascriptButtons", "scontrols", "classicPages"], "JavaScript button, s-control or Classic-only page", "JavaScript buttons, s-controls and Classic-only pages", [120, 315], 35),
    prompt: change("buttons and pages", ["List each JavaScript button, s-control and Classic-only Visualforce page, what it does and whether it's still used.", "In a sandbox, build a quick action, flow or Lightning component for each one still needed.", "Test in Lightning Experience, and list the ones nobody uses for their owner to decide."], ["Everything still in use works in Lightning Experience."]),
  },

  // Utilisation and adoption
  "utl.unassigned-licence-seats": {
    complexity: "moderate",
    hours: fixed(1, 2, "Setting the spare seats against the hiring plan and preparing the request for the renewal"),
    prompt: decide("licences", ["From Company Information, list each licence type with its total and used seats.", "Set out the spare seats against the hiring plan the business gives you.", "Prepare the reduction to ask for at renewal."], ["The renewal request names each licence and the seats to keep."]),
  },
  "utl.package-seats": {
    complexity: "moderate",
    hours: fixed(1, 3, "Preparing each installed app's use for its owner and the request for the renewal"),
    prompt: decide("installed apps", ["List each installed app with its seats, how many are assigned, and when its users last logged in.", "Prepare questions for each app's owner on whether it's still needed.", "Draft the cancellations and seat reductions to ask for at renewal."], ["Each app has an owner's decision: keep, reduce or cancel."]),
  },
  "utl.dormant-licensed-users": {
    complexity: "complex",
    hours: each(["users"], "dormant user", "dormant users", [15, 25], 7),
    prompt: {
      items: "users",
      decision: true,
      removes: true,
      steps: ["For each user listed, check they own no records, aren't named in flows, approval processes, assignment rules or scheduled jobs, and that nothing runs as them.", "Prepare a question for each user's manager: have they left or moved role?", "Prepare the list for an admin to act on once each manager has confirmed, with anything to move to an active user first."],
      verify: ["Each user has the checks done and the question ready for their manager."],
    },
  },
  "utl.never-logged-in": {
    complexity: "complex",
    hours: each(["users"], "user who has never logged in", "users who have never logged in", [15, 25], 7),
    prompt: {
      items: "users",
      decision: true,
      removes: true,
      steps: ["For each user listed, check they own no records, aren't named in automation, and that nothing runs as them.", "Prepare a question for each user's manager: do they still need access?", "Prepare the list for an admin to act on once each manager has answered."],
      verify: ["Each user has the checks done and the question ready for their manager."],
    },
  },
  "utl.unused-reports": {
    complexity: "moderate",
    hours: each(["unused"], "unused report", "unused reports", [2, 5], 7),
    prompt: decide("reports", ["List reports not run in the period, with their folder and last change.", "Propose a core report set for each team.", "List the rest for their owners to decide on, checking none feeds a dashboard or a subscription."], ["The proposal lists the core set and the reports for their owners, with none in use on a dashboard."]),
  },

  "utl.dashboards-inactive-running-user": {
    complexity: "complex",
    hours: each(["affected"], "dashboard", "dashboards", [15, 25], 7),
    prompt: decide("dashboards", ["For each dashboard listed, set out who views it and what data they should see.", "Propose a running user for each: a current user with that access, a dedicated reporting user, or a dynamic dashboard.", "Prepare the proposal for each dashboard's owner."], ["Each dashboard has a proposed running user its owner has agreed."]),
  },
  "utl.eac-connections": {
    complexity: "simple",
    hours: each(["broken"], "broken connection", "broken connections", [15, 25], 7),
    prompt: decide("connections", ["In Einstein Activity Capture's settings, list the connections in an error state and the action Salesforce recommends for each.", "Prepare a short note for each person on how to reconnect."], ["Each broken connection has the note its person needs."]),
  },
  "utl.eac-advanced-sync": {
    complexity: "simple",
    hours: fixed(1, 2, "Turning on one setting and checking that new events link to records"),
    prompt: change("settings", ["In a sandbox with the feature, turn on Relate Synced Events in Einstein Activity Capture's settings.", "Make a test meeting with a test contact on a test deal, and check the event links.", "Then turn it on in production; it can be turned off again."], ["New synced events show on the related records."]),
  },
  "adp.falling-adoption": {
    complexity: "simple",
    hours: fixed(8, 14, "Interviews with the teams, an analysis of what gets in their way, and a plan; the fixes it finds are their own work"),
    prompt: decide("teams", ["From login history, show how logins and active days changed by month for the groups named.", "List what changed in the org over the same months: new fields, validation rules, page layouts or automation.", "Prepare questions to ask the people who've stopped, and options to fix what they report."], ["The analysis shows when the fall began and what changed then."]),
  },
  "adp.low-engagement-group": {
    complexity: "simple",
    hours: each(["groups"], "team", "teams", [240, 420], 21),
    prompt: decide("teams", ["For each group named, summarise how often its members log in and what they make or update.", "List the reports, dashboards and page layouts they have.", "Prepare questions about what they use instead, and options to bring that work into Salesforce."], ["The analysis shows each group's use against the rest of the org."]),
  },
  "adp.classic-users": {
    complexity: "complex",
    hours: each(["users"], "user on Classic", "users on Classic", [30, 55], 14),
    prompt: decide("users", ["Find what keeps each user on Classic: a page, a button or a tool that only works there.", "List what would need replacing before they move, and the order.", "Prepare the plan for the users' managers to agree."], ["Each user has the reason they're on Classic and a plan to move."]),
  },

  // Data quality
  "dq.open-opps-past-close": {
    complexity: "moderate",
    hours: fixed(2, 4, REPORT),
    prompt: change("deals", ["Build a report of open deals with a close date in the past, grouped by owner, for the weekly pipeline review, where each owner decides on their own deals.", "In a sandbox, build a validation rule that stops an open deal being saved with a past close date, written so it applies only when a deal is created or its close date or stage changes, never to an edit of a record that already lacks the value, and test it." , "Hand the owners the tested rule and a report of the existing records that would fail it; they decide when it is turned on in production."], ["The report shows no open deal with a close date in the past after the next review, and the owners have the tested rule."]),
  },

  "dq.opps-inactive-owner": {
    complexity: "complex",
    touches: { leavesOut: true },
    hours: fixed(1, 3, PLAN),
    prompt: decide("deals", ["List open deals owned by inactive users, with the team each sat in.", "Prepare the list for the sales manager to name a new owner for each.", "Draft a step for the leaver checklist so this is handled when someone leaves."], ["Each deal has a proposed new owner for the sales manager to confirm."]),
  },
  "dq.open-opps-missing-amount": {
    complexity: "moderate",
    hours: fixed(2, 4, REPORT),
    prompt: change("deals", ["Build a report of open deals with no amount, by stage and owner, so owners can fill their own gaps.", "In a sandbox, build a validation rule that requires an amount from the stage where a price is known, written so it applies only when a deal is created or moves into that stage, never to an edit of a record that already lacks the value, and test it.", "Hand the owners the tested rule and a report of the existing records that would fail it; they decide when it is turned on in production."], ["The report shrinks at each review, and the owners have the tested rule."]),
  },

  "dq.open-opps-no-activity": {
    complexity: "simple",
    hours: fixed(2, 4, REPORT),
    prompt: change("deals", ["Check activity capture works first: email and calendar sync, and whether tasks are logged against deals.", "Build a report of open deals with no activity in 90 days, by owner.", "Share it for each owner to decide on their own deals."], ["The report shrinks at each pipeline review."]),
  },
  "dq.opps-without-account": {
    complexity: "moderate",
    hours: fixed(2, 4, REPORT),
    prompt: change("deals", ["Build a report of open deals with no account, for owners to link their own.", "In a sandbox, build a validation rule that requires an account, written so it applies only when a deal is created, never to an edit of a record that already lacks the value, and test it.", "Hand the owners the tested rule and a report of the existing records that would fail it; they decide when it is turned on in production."], ["The report shrinks, and the owners have the tested rule."]),
  },

  "dq.loss-reason-missing": {
    complexity: "moderate",
    touches: { automation: true },
    hours: fixed(2, 4, "Proposing a short list of reasons and building the rule for the owners to turn on; past deals aren't filled in"),
    prompt: change("lost deals", ["List the loss reason values and how often each is used, and propose a short list for sales to agree.", "In a sandbox, build a validation rule that requires a reason, written so it applies only when a deal's stage changes to closed lost, never to an edit of a record that already lacks the value, and test closing a test deal with and without one.", "Hand the owners the tested rule and a report of the existing records that would fail it; they decide when it is turned on in production."], ["The owners have the agreed list and the tested rule."]),
  },

  "dq.records-inactive-owner": {
    complexity: "complex",
    touches: { leavesOut: true },
    hours: fixed(2, 5, PLAN),
    prompt: decide("records", ["Count accounts, contacts and leads owned by inactive users, by object.", "Prepare the list for the business to name new owners.", "Draft a step for the leaver checklist so this is handled when someone leaves."], ["Each group of records has a proposed new owner for the business to confirm."]),
  },
  "dq.duplicate-rules-missing": {
    complexity: "moderate",
    touches: { automation: true },
    hours: each(["missing"], "object with no duplicate rule", "objects with no duplicate rule", [120, 210], 11),
    prompt: change("objects", ["In a sandbox, for each object named, build a matching rule on the fields that identify a record, such as name and email.", "Build a duplicate rule that uses it, in alert mode only, so nothing is blocked.", "Test with a known duplicate, then hand the tested rules to the data's owners, who decide when they are turned on in production, in alert mode."], ["In the sandbox, entering a known duplicate shows an alert, and the owners have the tested rules."]),
  },

  "dq.duplicate-accounts": {
    complexity: "complex",
    hours: each(["groups"], "group of accounts that share a name", "groups of accounts that share a name", [15, 25], 21),
    prompt: {
      items: "accounts",
      decision: true,
      removes: true,
      steps: ["For each group of accounts that share a name, compare their addresses, contacts, deals and owners.", "Flag which look like true duplicates and which record has the most history.", "List the integrations that hold an account's id.", "Prepare the list for the accounts' owners to decide on each group."],
      verify: ["Each group has a comparison and an owner's decision."],
      review: {
        steps: ["This task is a review for the accounts' owners.", "For each group of accounts that share a name, compare their addresses, contacts, deals and owners, and flag which look like true duplicates and which record has the most history.", "List the integrations that hold an account's id, and anything that would be lost if two records became one.", "Bring the list to the accounts' owners, so they can decide on each group."],
        verify: ["Each group has a comparison and an owner's decision."],
      },
    },
  },
  "dq.stale-leads": {
    complexity: "complex",
    touches: { leavesOut: true },
    hours: fixed(3, 5, "Preparing a follow-up rule by source for agreement; the reps' follow-up time isn't counted"),
    prompt: decide("leads", ["Count open leads with no activity, by source and age.", "Propose a follow-up rule for each source and an age past which leads go back to marketing.", "Prepare the list view or report that would show leads breaking the rule."], ["The proposal covers every lead source."]),
  },
  "dq.contact-completeness": {
    complexity: "complex",
    touches: { leavesOut: true },
    hours: fixed(3, 5, PLAN),
    prompt: decide("contacts", ["Build reports of contacts with no account and contacts with no email.", "Propose how contacts with no account could be matched to accounts, for example by email domain, as a list for their owners to check.", "Propose how missing emails get filled, and whether new contacts should require one."], ["The proposals are ready for the contacts' owners."]),
  },
  "dq.record-type-use": {
    complexity: "moderate",
    hours: fixed(1, 4, "Checking each record type's dependencies and preparing the list for its object's owner"),
    prompt: {
      items: "record types",
      decision: true,
      removes: true,
      steps: ["For each record type, list which profiles and permission sets give it, which default to it, and which flows, Apex, validation rules and integrations name it.", "Prepare the list for the object's owner to decide which are no longer needed."],
      verify: ["Each record type has its dependencies listed and an owner's decision."],
    },
  },

  // Historic pipeline
  "pipe.unmapped-stages": {
    complexity: "moderate",
    hours: fixed(1, 2, "Preparing a mapping of each stage for the health check's calibration"),
    prompt: decide("opportunity stages", ["List each opportunity stage with its probability, forecast category and whether it's closed or won.", "Prepare a proposed mapping of each stage to open, won or lost for the people who own the sales process to confirm."], ["Every stage in use has a confirmed mapping."]),
  },
  "pipe.calibration-contradicts-org": {
    complexity: "moderate",
    touches: { leavesOut: true },
    hours: fixed(1, 3, PROCESS),
    prompt: decide("opportunity stages", ["List each stage's closed and won flags as set in Salesforce.", "Set out where the way the business uses a stage differs from those flags.", "Prepare the options: change the stage set-up, or keep it and record why."], ["Each difference has an agreed answer."]),
  },
  "pipe.segment-divergence": {
    complexity: "simple",
    touches: { leavesOut: true },
    hours: fixed(2, 4, PROCESS),
    prompt: decide("deals", ["For the segment named, compare its deals with the rest: deal size, type, record type, source and stage history.", "Check whether the sample is large enough to draw a conclusion.", "Prepare what's different and the options, without naming anyone."], ["The analysis explains the gap or shows the sample is too small."]),
  },
  "pipe.skipped-stages": {
    complexity: "moderate",
    touches: { leavesOut: true },
    hours: fixed(4, 7, PROCESS),
    prompt: decide("stages", ["For each stage named, count won deals that skipped it, by team and record type.", "Set out whether it's part of how those teams sell.", "Prepare the options: take the stage off their path, or agree its exit criteria."], ["Each skipped stage has an agreed answer."]),
  },
  "pipe.going-backwards": {
    complexity: "simple",
    touches: { leavesOut: true },
    hours: fixed(4, 7, PROCESS),
    prompt: decide("stages", ["From opportunity history, count moves back to an earlier stage, by stage and team.", "Draft exit criteria for each stage: what must be true before a deal moves on.", "Prepare a report that would show backward moves for pipeline reviews."], ["The exit criteria and the report are ready for the sales leaders to agree."]),
  },
  "pipe.path-insights": {
    complexity: "simple",
    touches: { leavesOut: true },
    hours: fixed(2, 4, PROCESS),
    prompt: decide("paths", ["From opportunity history, list the closed deals on the path named and on the usual path, by team and record type.", "Set out what the deals on each path have in common, and what happens in the stage named.", "Prepare the options for the sales leaders: a change to the stage's exit criteria, or to when a stage can be left out."], ["The comparison and the options are ready for the sales leaders to agree."]),
  },
  "pipe.probability-vs-reality": {
    complexity: "simple",
    touches: { leavesOut: true },
    hours: fixed(2, 4, PROCESS),
    prompt: decide("stage probabilities", ["List each open stage with the probability set on it, how many closed deals reached it and how many of those were won, by record type.", "Set out what each stage's probability is used for: weighted pipeline, forecasts, reports and dashboards.", "Prepare the comparison for the sales leaders, without proposing new values, so they can agree what each probability is meant to say."], ["The comparison is ready for the sales leaders to agree what each probability means."]),
  },
  "pipe.dead-end-stage": {
    complexity: "moderate",
    hours: fixed(2, 4, "Setting a time limit for the stage and the report that shows deals past it"),
    prompt: change("stage", ["From won deals, find how long three quarters of them spent in the stage named.", "Build a report of open deals in that stage past the limit, for the pipeline review.", "If a reminder is wanted, build a scheduled flow in a sandbox that only sends a notification, and test it."], ["Deals past the limit show each week for their owners to review."]),
  },

  // Security, integrations and best practice
  "sec.health-check-score": {
    complexity: "complex",
    hours: fixed(8, 14, "Preparing, for each high and medium risk setting, who and what a change affects and the order to make them in"),
    prompt: decide("security settings", ["Open Security Health Check and list the settings marked high risk, then medium.", "For each, set out the standard value, what changes for users if it's applied, and any login or integration that could stop working (IP ranges, session length, password rules).", "Propose the order to make them in, and the notice people would need, for the people who own security to decide."], ["Each setting has who and what it affects, and a proposed order, ready for the owners."]),
  },

  "sec.health-check-risks": {
    complexity: "complex",
    hours: fixed(4, 7, "Preparing, for each setting named, who and what a change affects and the order to make them in"),
    prompt: decide("security settings", ["For each setting named, set out its current and standard values, and who and what a change affects, integrations included.", "Propose the order to make them in, and the notice people would need, for the people who own security to decide."], ["Each setting has who and what it affects, and a proposed order, ready for the owners."]),
  },

  "sec.modify-all-data": {
    complexity: "complex",
    hours: each(["unexpected"], "person outside the admin team who holds it", "people outside the admin team who hold it", [180, 315], 21),
    prompt: change("people", ["List the profiles and permission sets that grant Modify All Data to people outside the admin team, and anything that runs as them: flows, scheduled jobs and integrations.", "For each person, find which objects they really need to edit, and in a sandbox build a permission set with only that access.", "In the sandbox, assign it first, and confirm the person, and anything that runs as them, can still do the work.", "Only then take Modify All Data away, and only from a permission set that nobody who needs it holds. Never change a profile or a user's profile: where a person holds it through a profile others share, list for an admin each such person, with what else that profile gives them, so the owners can decide.", "Taking it away first stops their work and anything that runs as them.", "Hand the tested change, and its order, to the admin who owns access, to make in production in the same order."], ["Only admins and agreed system accounts hold Modify All Data, or each remaining holder is on the list for the owners, and the others can still do their work."]),
  },

  "sec.view-all-data": {
    complexity: "complex",
    hours: each(["unexpected"], "person outside the admin team who holds it", "people outside the admin team who hold it", [180, 315], 21),
    prompt: change("people", ["List the profiles and permission sets that grant View All Data to people outside the admin team, and anything that runs as them: reports, dashboards, scheduled jobs and integrations.", "For each person, find which objects they really need to read, and in a sandbox build a permission set with only that access.", "In the sandbox, assign it first, and confirm the person, and anything that runs as them, can still do the work.", "Only then take View All Data away, and only from a permission set that nobody who needs it holds. Never change a profile or a user's profile: where a person holds it through a profile others share, list for an admin each such person, with what else that profile gives them, so the owners can decide.", "Taking it away first hides data their work and reports rely on.", "Hand the tested change, and its order, to the admin who owns access, to make in production in the same order."], ["Only admins and agreed system accounts hold View All Data, or each remaining holder is on the list for the owners, and the others can still do their work."]),
  },

  "sec.sharing-defaults": {
    complexity: "complex",
    hours: each(["openObjects"], "object with an open default", "objects with an open default", [240, 420], 21),
    prompt: decide("objects", ["For each object named, set out its default access and who needs to edit its records.", "Draft the sharing rules, roles or teams that would give that access if the default were tightened.", "Note that tightening recalculates sharing and can change what people see, and prepare the options."], ["Each object has an agreed default, or a recorded reason to keep it open."]),
  },
  "sec.unused-access": {
    complexity: "moderate",
    hours: each(["emptyProfiles", "emptySets"], "unused profile or permission set", "unused profiles and permission sets", [15, 25], 7),
    prompt: decide("profiles and permission sets", ["For each profile and permission set named, confirm no user holds it, and check it isn't named in flows, Apex or deployments.", "Prepare the list, widest access first, for whoever set each up to decide."], ["Each one has its checks done and an owner's decision."]),
  },
  "sec.peer-outlier-access": {
    complexity: "complex",
    hours: each(["people"], "person with access their peers don't have", "people with access their peers don't have", [30, 55], 7),
    prompt: decide("access grants", ["For each grant listed, find the permission set or setting that gives it, and what the person uses it for.", "Prepare a question for the person's manager: is it still needed?", "Where it isn't, note what to check before it goes, such as reports or jobs that rely on it."], ["Every grant has the question ready for the manager."]),
  },
  "sec.personal-integration-accounts": {
    complexity: "complex",
    hours: each(["accounts"], "personal account used for an integration", "personal accounts used for integrations", [240, 420], 21),
    prompt: decide("integrations", ["Find which integrations log in through the personal accounts listed, from login history and connected apps.", "For each, set out an integration user on the Salesforce Integration licence, with an API-only profile and a permission set holding only the access it needs.", "Prepare the plan for the integration's owner: the new account is set up and tested in a sandbox first, the integration moved to it and tested, and only then the personal account's API access taken away. Taking it away first stops the integration."], ["Each integration has a plan its owner has agreed."]),
  },
  "int.integration-users-interactive": {
    complexity: "complex",
    hours: each(["open"], "integration account not set to API only", "integration accounts not set to API only", [60, 160], 7),
    prompt: change("integration accounts", ["For each integration account listed, check first whether anyone signs in to it through the browser, from login history; if anyone does, find out why before going on.", "In a sandbox, build a permission set holding only the API Only User permission, and assign it to that integration account alone. Never change a profile other users hold.", "Test that the integration still connects.", "Hand the tested change to the admin who owns access, to make in production once the integration's owner agrees."], ["In the sandbox the accounts can't log in through the browser and the integrations still run, and the admin has the change."]),
  },

  "int.integration-user-licences": {
    complexity: "complex",
    hours: each(["onFull"], "integration account on a full licence", "integration accounts on a full licence", [60, 105], 14),
    prompt: decide("integration accounts", ["For each account listed, set out an integration user on the Salesforce Integration licence, with an API-only profile and a permission set holding only the access it needs.", "Note how many Salesforce Integration licences are free and how many would have to be bought.", "Prepare the plan for each integration's owner: the new account tested in a sandbox first, the integration moved and tested, and only then the full seat given up at renewal."], ["Each integration has a plan its owner has agreed."]),
  },
  "sec.self-authorised-connected-apps": {
    complexity: "complex",
    hours: each(["open"], "connected app users can authorise", "connected apps users can authorise", [120, 210], 14),
    prompt: change("connected apps", ["For each app named, list who and what uses it today, from its OAuth usage and login history: people and integration accounts. The finding says when an app carries data now.", "In a sandbox, build a permission set that gives access to the app, and assign it to every person and integration account on that list.", "Only then set the app to admin-approved users are pre-authorised, and test that each of them, and each integration, still connects.", "Switching the policy first cuts off everyone using the app now, integrations included.", "Hand the tested change, and its order, to the admin who owns access, to make in production in the same order."], ["In the sandbox only assigned users can use the apps and every integration still runs, and the admin has the change and its order."]),
  },

  "sec.insecure-remote-sites": {
    complexity: "complex",
    hours: fixed(1, 4, "Moving each site to HTTPS and turning off the mixed-protocol setting, in a sandbox, for the admin to repeat"),
    prompt: change("remote sites", ["For each remote site named, check whether the address works over HTTPS, and which callouts use it.", "In a sandbox, change it to HTTPS and test every callout to it.", "Only then turn off the setting that lets code mix HTTPS and HTTP, in the sandbox. Turning it off first stops any callout still on HTTP.", "Hand the tested change, and its order, to the admin, to make in production in the same order."], ["Every remote site uses HTTPS in the sandbox, callouts still work, and the admin has the change."]),
  },

  "int.hardcoded-endpoints": {
    complexity: "complex",
    hours: each(["addresses"], "address written into code", "addresses written into code", [120, 210], 21),
    prompt: change("Apex classes", ["Find each callout address written into the code named.", "In a sandbox, set up a named credential for it, with its authentication in an external credential.", "Change the code to call it with callout: and the credential's name, and test each callout."], ["No callout address is written into code, and each callout still works."]),
  },
  "int.api-usage": {
    complexity: "complex",
    hours: fixed(4, 14, "Finding which integrations and jobs use the most and planning the cuts; changing them is more"),
    prompt: decide("integrations and jobs", ["From the API usage and event logs you can see, list which users, apps and jobs make the most calls.", "Find repeats: calls that could be combined, polled less often, or moved to bulk loads or change events.", "Propose the changes to each integration's owner."], ["Each heavy user of the API has a proposal its owner has seen."]),
  },
  "int.failed-async-jobs": {
    complexity: "complex",
    hours: each(["classes"], "class with failed jobs", "classes with failed jobs", [120, 210], 14),
    prompt: change("Apex classes", ["For each class named, read the failed jobs' errors.", "In a sandbox, find and fix the cause, with a test that reproduces it.", "Send future failures to someone who will act on them. Don't run the jobs again on production data until the owner agrees."], ["The jobs run without failing in the sandbox, and a failure reaches a person."]),
  },
  "bp.users-without-role": {
    complexity: "complex",
    hours: each(["hits"], "user with no role", "users with no role", [10, 20], 7),
    prompt: decide("users", ["List active users with no role, with their profile and what they own.", "Propose a role for each, or a note that they don't need one, for the business to agree."], ["Each user has a proposed role the business has seen."]),
  },
  "bp.ownership-skew": {
    complexity: "complex",
    hours: each(["pairs"], "owner and object above the limit", "owners and objects above the limit", [240, 420], 21),
    prompt: decide("records", ["For each owner and object named, find why one owner holds so many records.", "Propose how they could be spread across more owners, or where the owner should sit in the role hierarchy and sharing groups if it has to stay.", "Prepare the plan for the records' owners, noting that any change should be made at a quiet time."], ["Each owner and object has a plan the records' owners have agreed."]),
  },
  "bp.after-save-same-record-updates": {
    complexity: "complex",
    hours: each(["flows"], "after-save flow updating its own record", "after-save flows updating their own record", [120, 210], 21),
    prompt: change("flows", ["In each flow named, find the updates to the record that started it.", "In a sandbox, move them to a before-save flow on the same object, leaving after-save actions where they are.", "Test with single and bulk saves."], ["The flows no longer update their own record after save, and records end up the same."]),
  },
  "bp.jobs-on-inactive-owners": {
    complexity: "complex",
    hours: each(["jobs"], "scheduled job on an inactive owner", "scheduled jobs on inactive owners", [30, 55], 7),
    prompt: decide("scheduled jobs", ["List scheduled jobs owned by inactive users, with what each runs, when, and whether it still runs.", "Propose a dedicated account to own each job still needed.", "Prepare the plan for the job's owner: schedule it under the new account and check it runs before the old schedule goes."], ["Each job has a plan its owner has agreed."]),
  },

  // Current pipeline and forecast
  "cur.stuck-in-stage": {
    complexity: "simple",
    hours: fixed(2, 4, REPORT),
    prompt: change("deals", ["Build a report of open deals in their stage longer than three quarters of won deals took, by stage and owner.", "Add it to the pipeline review, where each owner decides on their own deals."], ["The report shrinks at each review."]),
  },
  "cur.stale-deals": {
    complexity: "simple",
    hours: fixed(2, 4, REPORT),
    prompt: change("deals", ["Check activity capture works first.", "Build a report of open deals with no stage change and no activity in 30 days, by owner.", "Share it for each owner to decide on their own deals."], ["The report shrinks at each review."]),
  },
  "cur.close-date-pushes": {
    complexity: "moderate",
    hours: fixed(2, 4, "Building the report, or the alert, that flags a second push for a manager"),
    prompt: change("deals", ["From opportunity field history, find deals whose close date moved more than once.", "Build a report of them, or in a sandbox a flow that only sends the manager a notification when a close date is pushed a second time, and test it."], ["A second push reaches a manager."]),
  },
  "cur.missing-next-steps": {
    complexity: "moderate",
    hours: fixed(2, 4, REPORT),
    prompt: change("deals", ["Build a report of open deals with an empty next step, for the pipeline review.", "In a sandbox, build a validation rule that requires a next step from an agreed stage, written so it applies only when a deal is created or moves into that stage, never to an edit of a record that already lacks the value, and test it.", "Hand the owners the tested rule and a report of the existing records that would fail it; they decide when it is turned on in production."], ["The report shrinks at each review, and the owners have the tested rule."]),
  },

  "cur.account-concentration": {
    complexity: "simple",
    touches: { leavesOut: true },
    hours: fixed(2, 4, PROCESS),
    prompt: decide("pipeline", ["Show how open pipeline splits across accounts, and how much the largest holds by stage and close date.", "Set out how much of it is likely, from stage and history.", "Prepare options for building pipeline elsewhere."], ["The analysis shows the exposure and the options."]),
  },
  "cur.rep-concentration": {
    complexity: "simple",
    touches: { leavesOut: true },
    hours: fixed(2, 4, PROCESS),
    prompt: decide("pipeline", ["Show how open pipeline splits across owners, without naming anyone.", "Set out the risk if the largest holder's deals slip.", "Prepare options for spreading coverage."], ["The analysis shows the exposure and the options."]),
  },
  "cur.coverage": {
    complexity: "simple",
    touches: { leavesOut: true },
    hours: fixed(2, 4, PROCESS),
    prompt: decide("pipeline", ["Show this quarter's quota, won and open pipeline by team.", "Set out the shortfall against the coverage target.", "Prepare the options: build more pipeline, or reset the number."], ["The leaders have the figures and options to decide this month."]),
  },
  "fct.manager-adjustments": {
    complexity: "simple",
    touches: { leavesOut: true },
    hours: fixed(2, 4, PROCESS),
    prompt: decide("forecast adjustments", ["Summarise the size of hand adjustments to the Commit forecast by period, without naming anyone.", "Compare the adjusted figures with what closed.", "Prepare a simple way to record the reason behind each large adjustment."], ["The analysis and the proposal are ready for the sales leaders."]),
  },
  "fct.quota-gaps": {
    complexity: "complex",
    hours: each(["hits"], "deal owner with no quota", "deal owners with no quota", [15, 25], 7),
    prompt: decide("deal owners", ["List active users who own open deals but have no quota this period.", "Prepare the list for sales leadership to say who carries a number, and what happens to the deals of those who don't."], ["Each owner has a decision from sales leadership."]),
  },
  "fct.commit-at-risk": {
    complexity: "simple",
    hours: fixed(1, 3, REPORT),
    prompt: change("deals", ["Build a report of Commit deals with warning signs: a past or pushed close date, no recent activity or no next step.", "Add it to the forecast call, where each owner keeps a dated reason or takes the deal out of Commit."], ["Each committed deal has a dated reason or has left Commit."]),
  },
  "fct.early-stage-commits": {
    complexity: "moderate",
    hours: fixed(1, 3, REPORT),
    prompt: change("deals", ["Build a report of Commit deals in early stages, for the forecast call.", "In a sandbox, build a validation rule that stops an early-stage deal being marked Commit, written so it applies only when a deal's forecast category or stage changes, never to an edit of a record that already lacks the value, and test it.", "Hand the owners the tested rule and a report of the existing records that would fail it; they decide when it is turned on in production."], ["The report is in the forecast call, and the owners have the tested rule."]),
  },


  // Activity and leads
  "act.event-link-collapse": {
    complexity: "simple",
    hours: fixed(2, 5, "Finding what changed when the drop began and correcting the setting"),
    prompt: change("events", ["Find what changed when the drop began: Einstein Activity Capture settings, calendar sync, or a new tool.", "Check the Relate Synced Events setting, and if it's off, turn it on in a sandbox first and check that new events link.", "Then turn it on in production; it can be turned off again."], ["New events link to records at the rate they did before the drop."]),
  },
  "act.deals-without-activity": {
    complexity: "simple",
    touches: { leavesOut: true },
    hours: fixed(4, 7, PROCESS),
    prompt: decide("deals", ["Show the share of won and lost deals with no activity, by team.", "Propose the few moments worth recording on a deal, or automatic capture of email and calendar.", "Prepare how each would be set up."], ["The proposal is ready for the sales leaders."]),
  },
  "act.won-vs-lost-activity": {
    complexity: "simple",
    touches: { leavesOut: true },
    hours: fixed(2, 4, PROCESS),
    prompt: decide("deals", ["Compare logged activity on won and lost deals, by team and stage.", "Check whether activity is logged evenly before drawing a conclusion.", "Prepare what the comparison shows and the options."], ["The analysis is ready for the sales leaders."]),
  },
  "act.rep-activity-spread": {
    complexity: "simple",
    hours: fixed(1, 3, "Checking the email and calendar connections and preparing the comparison"),
    prompt: decide("reps", ["Check each listed user's email and calendar connection and activity capture settings, and note any that are broken.", "Prepare a comparison of logged activity by deal, without naming anyone, for the manager."], ["Broken connections are listed, and the gap is explained."]),
  },
  "act.unlinked-tasks": {
    complexity: "moderate",
    touches: { integration: true },
    hours: fixed(2, 4, "Finding which tools make unlinked tasks and fixing their set-up"),
    prompt: change("tasks", ["Find which tools or automations make tasks with no related record.", "In a sandbox, change those that can link to the right record, and test them.", "Prepare a short note on logging against the deal."], ["The share of unlinked tasks falls."]),
  },
  "lead.conversion-by-source": {
    complexity: "simple",
    touches: { leavesOut: true },
    hours: fixed(2, 4, PROCESS),
    prompt: decide("lead sources", ["For the source named, compare conversion, response time and follow-up with other sources.", "Set out whether the leads are poor or poorly handled.", "Prepare the options."], ["The analysis is ready for marketing and sales."]),
  },
  "lead.response-time": {
    complexity: "moderate",
    hours: fixed(2, 5, "Agreeing a response target by source and the routing behind it"),
    prompt: change("lead sources", ["Show the median time to first response by source, and propose a target for each for sales to agree.", "In a sandbox, change lead routing or add a notification so slow sources reach someone who can meet the target, and test it with test leads."], ["Response time for each source is within its target."]),
  },
  "lead.no-first-response": {
    complexity: "moderate",
    touches: { automation: true },
    hours: fixed(2, 4, "Checking routing and correcting the rules that leave leads unworked"),
    prompt: change("leads", ["List leads with no response, by source, owner type and queue.", "Check the lead assignment rules and queues that hold them.", "In a sandbox, correct the routing for new leads and test it with test leads."], ["New leads are assigned to someone and get a response."]),
  },
  "lead.converted-no-opportunity": {
    complexity: "simple",
    touches: { leavesOut: true },
    hours: fixed(1, 3, PROCESS),
    prompt: decide("leads", ["Show converted leads with no opportunity, by source and owner type.", "Propose when a lead should be converted, and how tidy-up conversions are kept out of conversion figures."], ["The rule is ready for sales and marketing to agree."]),
  },
  "lead.source-return": {
    complexity: "simple",
    touches: { leavesOut: true },
    hours: fixed(2, 4, PROCESS),
    prompt: decide("lead sources", ["For the source named, show leads, conversions and won value.", "Check how its leads are handled.", "Prepare the options for keeping or dropping it."], ["The analysis is ready for marketing."]),
  },
  "lead.campaign-return": {
    complexity: "simple",
    touches: { leavesOut: true },
    hours: fixed(2, 4, PROCESS),
    prompt: decide("campaign types", ["For the campaign type named, show spend against won value.", "Check the attribution: contacts on a campaign whose deals aren't counted.", "Prepare the options for the spend."], ["The analysis is ready for marketing."]),
  },
  // Behind HC_LEAD_JOURNEY (DECISIONS.md 770).
  "lead.source-win-rate": {
    complexity: "simple",
    touches: { leavesOut: true },
    hours: fixed(2, 4, PROCESS),
    prompt: decide("lead sources", ["For the source named, show leads, conversions, closed deals and won deals, against the other sources, by team.", "Check how its leads are qualified before they are converted.", "Prepare the options for how its leads are converted and what is spent on it."], ["The analysis is ready for sales and marketing to agree."]),
  },
  // Stored and never served (703, 770): declared, as every rule that can raise an issue declares some, and used by nothing.
  "lead.fast-conversion": {
    complexity: "simple",
    touches: { leavesOut: true },
    hours: fixed(2, 4, PROCESS),
    prompt: decide("lead conversion", ["List the deals from leads converted within a week and from leads converted later, by source and team.", "Set out what happens to the leads in each group before they are converted."], ["The comparison is ready for sales and marketing to agree."]),
  },

  // Revenue opportunities
  "rev.stalled-deals": {
    complexity: "simple",
    touches: { leavesOut: true },
    hours: fixed(2, 4, PROCESS),
    prompt: decide("deals", ["List the stalled open deals, by owner and amount, without naming anyone.", "Prepare a plan to give each owner a date to re-engage or close them."], ["Each stalled deal has a date."]),
  },
  "rev.win-backs": {
    complexity: "simple",
    touches: { leavesOut: true },
    hours: fixed(2, 4, PROCESS),
    prompt: decide("lost deals", ["List the lost and no decision deals worth another look, no decision first.", "Summarise why each group stopped, from loss reasons and history.", "Prepare a plan for checking each one."], ["The plan is ready for the sales leaders."]),
  },
  "rev.dormant-accounts": {
    complexity: "complex",
    touches: { leavesOut: true },
    hours: fixed(2, 4, PROCESS),
    prompt: decide("accounts", ["List past customers with no recent activity, with their last deal and whether their owner is active.", "Prepare a plan for who owns them and how to check in."], ["Each account has an owner and a next step."]),
  },

  // Service
  "svc.backlog-growth": {
    complexity: "simple",
    touches: { leavesOut: true },
    hours: fixed(4, 7, PROCESS),
    prompt: decide("cases", ["Show cases opened and closed by month, by priority, queue and type.", "Find where the growth sits and any cause such as a product fault or a missing article.", "Prepare the options: capacity, routing, or fixing the cause."], ["The analysis is ready for the service leaders."]),
  },
  "svc.backlog-age": {
    complexity: "simple",
    hours: fixed(2, 4, REPORT),
    prompt: change("cases", ["Build a report of open cases older than the agreed age, oldest first, by queue and priority.", "Add it to a weekly review, where the service team decides on each case."], ["The share of old cases falls each week."]),
  },
  "svc.priority-resolution": {
    complexity: "moderate",
    hours: fixed(4, 7, "Routing high-priority cases to their own queue or routing priority, with a visible target"),
    prompt: change("cases", ["Show resolution time by priority.", "In a sandbox, route new high-priority cases to their own queue or a higher routing priority, and test it with test cases.", "Add a resolution target the team can see."], ["High-priority cases are resolved faster than the rest."]),
  },
  "svc.milestone-breaches": {
    complexity: "moderate",
    hours: fixed(4, 7, "Reviewing the milestones missed most and adding warning actions"),
    prompt: change("milestones", ["Show breaches by milestone type, entitlement process and business hours.", "Check whether the targets, the business hours or the staffing are wrong, and set that out for the service leaders.", "In a sandbox, add warning actions so agents see a breach coming, and test them."], ["The breach rate falls below the line."]),
  },
  "svc.first-reply-email": {
    complexity: "moderate",
    hours: fixed(2, 5, "Setting up an acknowledgement and routing so email cases get a timely first reply"),
    prompt: change("email cases", ["Show time to first reply on email cases, by queue.", "In a sandbox, set up an auto-response and routing so a person replies within the target, and test it with test emails."], ["Email cases get a first reply within the target."]),
  },
  "svc.reopened-cases": {
    complexity: "moderate",
    hours: fixed(2, 5, "Finding which case types reopen and preparing a confirmation step for the service leaders"),
    prompt: change("cases", ["Show reopened cases by type, reason and queue.", "Propose a confirmation step before closing for the types that reopen most, for the service leaders to agree.", "Build it in a sandbox and test it, and hand it to the service leaders, who decide when it is turned on in production."], ["The service leaders have the analysis and the tested step."]),
  },

  "svc.unassigned-cases": {
    complexity: "moderate",
    touches: { leavesOut: true },
    hours: fixed(2, 5, PROCESS),
    prompt: decide("queues", ["List each case queue with its members and its oldest open case.", "Propose members for each queue that has none, and routing from busy queues, for the service leaders to agree."], ["Every queue has proposed members the service leaders have agreed."]),
  },
  "svc.origin-missing": {
    complexity: "moderate",
    touches: { automation: true },
    hours: fixed(2, 4, "Setting Origin automatically for each channel and building the rule for the owners to turn on"),
    prompt: change("cases", ["List each channel cases come in through, and how Origin is set for each.", "In a sandbox, set Origin automatically for new cases from each channel (in the Email-to-Case routing address, the web form's hidden Origin field, and the messaging channel's settings), and test each.", "In the sandbox, build a validation rule that requires Origin, written so it applies only when a case is created, never to an edit of a record that already lacks the value, and test it.", "Hand the owners the tested rule and a report of the existing records that would fail it; they decide when it is turned on in production."], ["New cases in the sandbox all get an Origin, and the owners have the tested rule."]),
  },

  "svc.case-classification": {
    complexity: "moderate",
    touches: { automation: true },
    hours: fixed(2, 4, "Proposing a short list of types or reasons and building the rule for the owners to turn on"),
    prompt: change("cases", ["Show how Type and Reason are used, and their values, and propose a short list for the service leaders to agree.", "In a sandbox, build a validation rule that requires one, written so it applies only when a case's status changes to closed, never to an edit of a record that already lacks the value, and test it.", "Hand the owners the tested rule and a report of the existing records that would fail it; they decide when it is turned on in production."], ["The service leaders have the agreed list and the tested rule."]),
  },

  "svc.case-customer": {
    complexity: "moderate",
    hours: fixed(2, 5, "Checking how incoming cases are matched to contacts and building the matching that's missing"),
    prompt: change("cases", ["Show open cases with no contact or account, by channel.", "Check whether Email-to-Case and Web-to-Case find the contact from the sender's email, and where they don't, set out why.", "In a sandbox, build what's missing, for example a flow on new cases that looks up the contact by the sender's email, and test it with test cases."], ["New test cases in the sandbox are linked to a customer."]),
  },

  // Behind HC_CASE_PATHS (DECISIONS.md 776).
  "svc.stuck-in-status": {
    complexity: "simple",
    touches: { leavesOut: true },
    hours: fixed(2, 4, PROCESS),
    prompt: decide("open cases", ["List the open cases that have been in their status longest, by status, priority and origin, without naming anyone.", "For the status named, set out what a case is waiting for there and who moves it on.", "Prepare the options for moving these cases on or closing them."], ["The list and the options are ready for the service leaders."]),
  },
  "svc.reopened-time": {
    complexity: "simple",
    touches: { leavesOut: true },
    hours: fixed(2, 4, PROCESS),
    prompt: decide("reopened cases", ["List the reopened cases, with the time before their first close and after it, by priority and origin.", "Set out what these cases had in common when they were first closed.", "Prepare the options for a check with the customer before closing cases like them."], ["The analysis is ready for the service leaders to agree."]),
  },
  "svc.closed-straight": {
    complexity: "simple",
    touches: { leavesOut: true },
    hours: fixed(2, 4, PROCESS),
    prompt: decide("cases closed straight", ["Take a sample of the cases closed straight from the status they started in, by starting status, priority and origin.", "Sort the sample into duplicates, spam, cases closed by automation and questions answered at once.", "Prepare the options for each group."], ["The sample and the options are ready for the service leaders."]),
  },
  // Stored and never served (703, 776): declared, as every rule that can raise an issue declares some, and used by nothing.
  "svc.sent-back-cases": {
    complexity: "simple",
    touches: { leavesOut: true },
    hours: fixed(2, 4, PROCESS),
    prompt: decide("cases sent back", ["List the cases sent back between the two statuses named, and the cases that reached the same point without it, by priority and origin.", "Set out what happens to the cases in each group after that point."], ["The comparison is ready for the service leaders to agree."]),
  },
  "svc.escalation-rules": {
    complexity: "moderate",
    hours: fixed(2, 5, "Adding one escalation rule or an entitlement process"),
    prompt: change("escalation rules", ["Propose the time after which an open case should escalate, by priority, for the service leaders to agree.", "In a sandbox, add an escalation rule that sends those cases to a team lead's queue and tells them, and test it."], ["A test case left open past the time escalates."]),
  },
  "svc.escalation-targets": {
    complexity: "moderate",
    touches: { automation: true },
    hours: each(["dead"], "escalation action with no one to act", "escalation actions with no one to act", [60, 105], 7),
    prompt: change("escalation actions", ["List escalation actions that point at an inactive user or an empty queue.", "Propose a queue with members for each, for the service leaders to agree.", "In a sandbox, point each at the agreed queue and test it, and draft a line on escalation targets for the leaver checklist."], ["Every escalation in the sandbox reaches someone who can act."]),
  },

  "svc.queue-sprawl": {
    complexity: "complex",
    touches: { leavesOut: true },
    hours: fixed(2, 4, PROCESS),
    prompt: decide("queues", ["List each case queue with its members, open cases and the routing and rules that use it.", "Flag queues nobody works and what routes to them.", "Propose which to keep, which to fold together and who should be a member, for the service leaders to decide."], ["Every queue has a decision from the service leaders."]),
  },
  "svc.omni-routing": {
    complexity: "moderate",
    touches: { automation: true },
    hours: each(["unrouted"], "case queue Omni-Channel doesn't route", "case queues Omni-Channel doesn't route", [60, 105], 7),
    prompt: change("queues", ["For each case queue without a routing configuration, ask whether Omni-Channel should push its work.", "In a sandbox, give those a routing configuration, and test it.", "List configurations no queue uses for their owner to decide."], ["Every case queue is routed, or left out on purpose."]),
  },
  "svc.agent-licences": {
    complexity: "moderate",
    hours: each(["wasted"], "Service Cloud seat held for no casework", "Service Cloud seats held for no casework", [15, 25], 4),
    prompt: decide("Service Cloud users", ["List Service Cloud users who don't work cases, and what they do instead.", "Prepare a question for each manager, and the licence that would fit."], ["Each user has a manager's decision."]),
  },
  "svc.knowledge-use": {
    complexity: "moderate",
    hours: fixed(2, 5, "Putting Knowledge on the case page and setting up the reporting"),
    prompt: change("knowledge articles", ["In a sandbox, add the Knowledge component to the case page, and test it.", "Build a report of articles attached to closed cases, by case type.", "List the common case types with no article."], ["More closed cases have an article attached."]),
  },
  "svc.knowledge-current": {
    complexity: "moderate",
    hours: each(["stale"], "article not reviewed in the period", "articles not reviewed in the period", [15, 25], 21),
    prompt: decide("knowledge articles", ["List articles not reviewed in the period, with their views and case types.", "Propose an owner and a review date for each.", "Flag those nobody reads, and those behind common case types, for their owners to decide on first."], ["Every article has a proposed owner and review date."]),
  },
  "svc.macros-and-quick-text": {
    complexity: "moderate",
    touches: { leavesOut: true },
    hours: fixed(2, 5, PROCESS),
    prompt: decide("macros and quick text", ["List macros and quick text with when each was last used.", "Flag those unused in a year, and the Classic-only macros that would need rebuilding for Lightning.", "Propose an owner for each, and prepare the list for them to decide."], ["Every macro and quick text has an owner and a decision."]),
  },
  "aut.case-routing-overlap": {
    complexity: "complex",
    hours: fixed(4, 7, "Separating assignment rules and Omni-Channel so each case is routed once"),
    prompt: change("assignment rules", ["List case assignment rules and Omni-Channel configurations, and which queues each touches.", "In a sandbox, change the rules so they only put new cases into queues, and let Omni-Channel push them to agents.", "Test with test cases."], ["No rule assigns cases straight to people, and each case is routed once."]),
  },

  // CPQ
  "cpq.price-rule-conflicts": {
    complexity: "complex",
    hours: each(["pairs"], "pair of competing price rules", "pairs of competing price rules", [120, 210], 14),
    prompt: decide("price rules", ["For each pair named, compare their conditions, actions and evaluation order.", "Propose a different evaluation order or one combined rule, and the quote totals each would give on test quotes.", "Prepare it for whoever owns pricing to decide."], ["Each pair has a proposal and a decision from pricing."]),
  },
  "cpq.twin-field-mismatch": {
    complexity: "complex",
    hours: each(["mismatches"], "pair of mismatched twin fields", "pairs of mismatched twin fields", [60, 105], 7),
    prompt: decide("twin fields", ["For each field named, compare its type on the quote line and its twin.", "Set out what a change of type would do to the data already held, and propose the fix.", "Prepare it for the field's owner to decide."], ["Each pair has a proposal its owner has agreed."]),
  },
  "cpq.inactive-rules": {
    complexity: "moderate",
    hours: each(["inactive"], "inactive rule", "inactive rules", [15, 25], 7),
    prompt: decide("rules", ["List each inactive price and product rule with when it was last changed.", "Prepare the list for whoever owns pricing to decide which will be used again."], ["Each rule has a decision from pricing."]),
  },
  "cpq.discount-leakage": {
    complexity: "complex",
    touches: { leavesOut: true },
    hours: fixed(4, 7, PROCESS),
    prompt: decide("discounts", ["Show discount against win rate by deal size, without naming anyone.", "Draft a discount policy with limits by deal size.", "Prepare how deeper discounts would go through approval."], ["The policy is ready for the sales leaders."]),
  },
  "cpq.renewals-not-started": {
    complexity: "moderate",
    hours: fixed(2, 5, "Checking in a sandbox how CPQ makes renewals, and preparing the list for the contracts' owners"),
    prompt: change("contracts", ["List contracts ending soon with no renewal opportunity.", "In a sandbox, find the setting on the contract that has CPQ produce its renewal opportunity (in Salesforce CPQ this is the contract's renewal forecast; check its label in your org), set it on a test contract, and check the renewal CPQ produces.", "Look in CPQ's package settings for the option that sets this on new contracts, and set out what it would change.", "Prepare the list of contracts for their owners, who set it on each in production."], ["A test contract in the sandbox gets its renewal from CPQ, and each contract on the list has an owner."]),
  },
  "cpq.discount-approvals": {
    complexity: "complex",
    hours: fixed(8, 14, "Building an approval process on the quote for the agreed discount levels, for the owners to turn on"),
    prompt: change("quotes", ["Propose the discount levels that need sign-off, for the sales leaders to agree.", "In a sandbox, build an approval process on the quote for the agreed levels, and test it with test quotes.", "Hand the tested process to the sales leaders, who decide when it is turned on in production, and prepare a report of past deep discounts and whether each went through approval."], ["The sales leaders have the tested process and the report."]),
  },

  "cpq.quote-to-order": {
    complexity: "simple",
    hours: fixed(2, 4, REPORT),
    prompt: change("quotes", ["Build a report of won quotes with no order, for their owners.", "Propose how ordering becomes part of closing a deal, for the sales and finance owners to agree."], ["The owners have the report and the proposal."]),
  },


  // Marketing handover
  "mkt.unworked-handovers": {
    complexity: "complex",
    touches: { leavesOut: true },
    hours: fixed(2, 4, PROCESS),
    prompt: decide("leads", ["Build a report of marketing leads handed to sales and not worked, by source and age.", "Propose a follow-up time, and what happens to leads nobody works, for sales and marketing to agree."], ["The proposal is ready for sales and marketing."]),
  },
  "mkt.scoring-grading": {
    complexity: "complex",
    touches: { leavesOut: true },
    hours: fixed(4, 7, PROCESS),
    prompt: decide("leads", ["Show synced leads with no score or grade.", "Propose a grading profile against the ideal customer profile, and changes to scoring so it rewards real buying signals.", "Prepare it for marketing to agree."], ["The proposal is ready for marketing."]),
  },
  "mkt.unowned-prospects": {
    complexity: "complex",
    touches: { leavesOut: true },
    hours: fixed(1, 3, PLAN),
    prompt: decide("leads", ["List synced leads in a queue or with an inactive owner.", "Find the assignment rules that left them there.", "Prepare the list, with a proposed owner for each, and the rule changes, for sales and marketing to agree."], ["Each lead has a proposed owner and the rule changes are agreed."]),
  },
  "mkt.campaign-influence": {
    complexity: "simple",
    touches: { mayNotUndo: true },
    hours: fixed(1, 2, "Turning on one setting and choosing the model to report on"),
    prompt: change("campaign settings", ["In a sandbox, allow influence models from Account Engagement in the campaign influence settings, and check the attribution.", "Propose the model to report on, for marketing to choose, then turn it on in production; it can be turned off again."], ["Opportunities show Account Engagement's attribution."]),
  },
};
