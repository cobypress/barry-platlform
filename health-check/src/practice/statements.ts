import type { Practice, PracticeSource } from "./index.ts";

// Each statement is ours to write, in plain words; each source's "says" is what
// the page says, paraphrased, never quoted. Read on the date given. Most
// relevant first: the chapter slide shows the first three.

const CHECKED = "2026-11-05";
const page = (name: PracticeSource["name"], title: string, url: string, says: string): PracticeSource => ({ name, title, url, checked: CHECKED, says });

const WA_TRUST = (says: string) => page("Salesforce Well-Architected", "Trust", "https://architect.salesforce.com/docs/architect/well-architected/guide/trust.html", says);
const WA_OPS = (says: string) => page("Salesforce Well-Architected", "Operational Excellence", "https://architect.salesforce.com/docs/architect/well-architected/guide/operational-excellence.html", says);
const WA_COST = (says: string) => page("Salesforce Well-Architected", "Resource and Cost Optimization", "https://architect.salesforce.com/docs/architect/well-architected/guide/resource-and-cost-optimization.html", says);
const RECORD_TRIGGERED = (says: string) => page("Salesforce Architects", "Record-Triggered Automation decision guide", "https://architect.salesforce.com/docs/architect/decision-guides/guide/record-triggered", says);
const SHARING = (says: string) => page("Salesforce Architects", "Platform Sharing Architecture", "https://architect.salesforce.com/docs/architect/fundamentals/guide/platform-sharing-architecture.html", says);
const OBJECTS = (says: string) => page("Trailhead", "Optimize Customer Data with Standard and Custom Objects", "https://trailhead.salesforce.com/content/learn/modules/data_modeling/objects_intro", says);
const USAGE = (says: string) => page("Trailhead", "Measure Salesforce Usage", "https://trailhead.salesforce.com/content/learn/modules/user-adoption-metrics/measure-salesforce-usage", says);
const DATA_HABITS = (says: string) => page("Trailhead", "Delve into Data Management", "https://trailhead.salesforce.com/content/learn/modules/essential-habits-for-salesforce-admins/delve-into-data-management", says);
const PATH = (says: string) => page("Trailhead", "Optimize Sales Processes with Path in Salesforce", "https://trailhead.salesforce.com/content/learn/modules/sales_admin_optimize_salesforce_for_selling/sales_admin_optimize_for_selling_unit_1", says);
const PIPELINE = (says: string) => page("Trailhead", "Build a Healthy Sales Pipeline", "https://trailhead.salesforce.com/content/learn/modules/sales-pipeline-basics/build-a-healthy-sales-pipeline", says);
const FORECASTING = (says: string) => page("Trailhead", "Configure Forecasting in Salesforce", "https://trailhead.salesforce.com/content/learn/modules/sales-forecasting/configure-sales-forecasting-in-salesforce", says);
const FORECAST_SETUP = (says: string) => page("Trailhead", "Review Your Pipeline Forecasts Setup", "https://trailhead.salesforce.com/content/learn/modules/revenue-intelligence-setup/review-your-collaborative-forecasts-setup", says);
const EMAIL_TOOLS = (says: string) => page("Trailhead", "Learn About Email and Calendar Integration Tools", "https://trailhead.salesforce.com/content/learn/modules/email-and-calendar-integrations-quick-look/learn-about-email-and-calendar-integration-tools", says);
const LEADS = (says: string) => page("Trailhead", "Track Prospects with Salesforce Leads", "https://trailhead.salesforce.com/content/learn/modules/prospect-tracking-with-salesforce-leads-quick-look/track-prospects-with-salesforce-leads", says);
const CASE_QUEUES = (says: string) => page("Trailhead", "Create Case Queues and Assignment Rules", "https://trailhead.salesforce.com/content/learn/projects/set-up-case-escalation-entitlements/create-case-queues-assignment-rule", says);
const ESCALATION = (says: string) => page("Trailhead", "Create Case Escalation Rules for Efficient Support", "https://trailhead.salesforce.com/content/learn/projects/set-up-case-escalation-entitlements/create-case-escalation-rule", says);
const ENTITLEMENTS = (says: string) => page("Trailhead", "Create an Entitlement Process", "https://trailhead.salesforce.com/content/learn/projects/set-up-case-escalation-entitlements/create-entitlement-process", says);
// Read for integrations and technical health (DECISIONS.md 173).
const READ_6_NOV = "2026-11-06";
const TRUST_6_NOV = (says: string): PracticeSource => ({ ...WA_TRUST(says), checked: READ_6_NOV });
const OPS_6_NOV = (says: string): PracticeSource => ({ ...WA_OPS(says), checked: READ_6_NOV });
const STALLED = (says: string) => page("Trailhead", "Empower Your Sales Team to Get Stalled Deals Back on Track", "https://trailhead.salesforce.com/content/learn/modules/sales-deal-acceleration/get-deals-back-on-track", says);

export const PRACTICE: Record<string, Practice[]> = {
  configuration: [
    { text: "Give every custom object and field a description, so others can see what it's for.", source: OBJECTS("Descriptions on custom objects and fields are one of the habits it recommends when you build them.") },
    { text: "Agree opportunity stages with sales leaders, and set the key fields reps fill in at each one.", source: PATH("You work out with your sales operations lead which stages reps see, then set the key fields and guidance for each stage.") },
    { text: "Put set-up changes through the same review and release process as code.", source: WA_OPS("Configuration should be versioned and reviewed in the same way as code changes, with the reason for each decision written down.") },
    { text: "Add help text to fields that are specialised or easy to get wrong.", source: OBJECTS("Help text gives users more detail on specialised or complicated fields.") },
    { text: "Use restricted picklists, so integrations and other apps can't add stray values.", source: page("Trailhead", "Get Started with Picklists", "https://trailhead.salesforce.com/content/learn/modules/picklist_admin/picklist_admin_start", "A restricted picklist stops new values being added, whether through the API or from other apps.") },
  ],
  automation: [
    { text: "Move what's left in Workflow Rules and Process Builder to Flow.", source: page("Trailhead", "Understand Automation Changes in Salesforce", "https://trailhead.salesforce.com/content/learn/projects/migrate-workflows-and-processes-to-flows/understand-automation-changes", "Flows are easier to maintain, troubleshoot and debug than Workflow Rules and processes, which are being retired.") },
    { text: "Give each object one way into its automation, rather than mixing flows and triggers.", source: RECORD_TRIGGERED("For each object, pick one mechanism as the entry point into automation.") },
    { text: "Give flows a fault path wherever they read, write or call out, so failures are handled.", source: page("Trailhead", "Handle Flow Errors with Fault Paths", "https://trailhead.salesforce.com/content/learn/modules/flow-implementation-2/handle-flow-errors-with-fault-paths", "Fault paths can be added to the Get, Create, Update and Delete Records elements and to actions, so a flow can deal with its own errors.") },
    { text: "Find record ids in flows as they run, rather than typing them in.", source: page("Trailhead", "Make the Most of Global Variables and Values", "https://trailhead.salesforce.com/content/learn/modules/data-and-actions-in-flows/global-variables-values", "Flows shouldn't hard-code ids, because they differ from one org to the next.") },
    { text: "Write Apex tests that prove the code works, not just enough to pass the minimum.", source: page("Trailhead", "Get Started with Apex Unit Tests", "https://trailhead.salesforce.com/content/learn/modules/apex_testing/apex_testing_intro", "Tests should check that code behaves as it should, rather than being written only to meet the coverage requirement.") },
  ],
  technical_health: [
    { text: "Keep queries and saves out of loops, so code stays inside Salesforce's limits as volumes grow.", source: { ...page("Trailhead", "Effective Bulk Apex Trigger Design Techniques", "https://trailhead.salesforce.com/content/learn/modules/apex_triggers/apex_triggers_bulk", "A query inside a for loop can run into the limit on queries, and saving records one at a time wastes resources. Bulk code queries and saves once, and stays within the governor limits."), checked: READ_6_NOV } },
    { text: "Treat Salesforce's minimum test coverage as the floor, and write tests that check what the code does.", source: OPS_6_NOV("Aim above the 75% coverage minimum from unit tests alone, and treat coverage as a floor rather than a ceiling.") },
    { text: "Have tests create their own data, rather than depend on what's in the org.", source: OPS_6_NOV("Apex unit tests should check methods and classes in isolation from the org's existing data and from other systems.") },
    { text: "Replace JavaScript buttons and s-controls, which Lightning Experience doesn't run.", source: null },
    { text: "Bring code, flows and pages forward to a recent API version as part of each release.", source: null },
  ],
  security: [
    { text: "Grant access through permission sets, with only the access each person needs.", source: WA_TRUST("Access should come through permission sets, built from documented least-privilege needs.") },
    { text: "Set org-wide defaults to Private for objects that hold sensitive data.", source: WA_TRUST("Start from Private defaults for sensitive objects, and keep anything more open for documented needs.") },
    { text: "Run Health Check regularly, and work through the risks it raises.", source: WA_TRUST("Health Check should be run regularly against Salesforce's security baseline, and its findings dealt with.") },
    { text: "Prefer object-level View All and Modify All to org-wide View All Data and Modify All Data.", source: SHARING("Object-level View All and Modify All are often a better choice than the administrative View All Data and Modify All Data.") },
    { text: "Require multi-factor authentication for everyone who logs in through the user interface.", source: WA_TRUST("Enforce multi-factor authentication for user interface logins.") },
  ],
  integrations: [
    { text: "Run each integration as its own integration account, with only the access that integration needs.", source: TRUST_6_NOV("Give each integration a dedicated integration account with a least-privilege permission set, so its access is scoped to what it does.") },
    { text: "Make integration accounts API only, and use the Salesforce Integration licence for new ones.", source: TRUST_6_NOV("Restrict integration and service accounts to API-only access with the API Only User permission, and for new builds use the API-only integrations profile with the Salesforce Integration licence.") },
    { text: "Keep callout addresses and credentials in named credentials, not in code.", source: TRUST_6_NOV("Manage authentication centrally with named and external credentials, which also lets credentials be rotated.") },
    { text: "Let only users an admin has approved use each connected app.", source: TRUST_6_NOV("Set an app's permitted users so that only admin-approved users are pre-authorised.") },
    { text: "Watch API use against the daily allowance, and make sure failed background jobs reach someone.", source: OPS_6_NOV("Monitoring should flag API requests approaching the daily allocation, and logging and alerts should turn batch jobs that fail silently into problems someone can diagnose.") },
  ],
  best_practice: [
    { text: "Put users in roles that match the data they need to see, not just the org chart.", source: page("Trailhead", "Create a Role Hierarchy", "https://trailhead.salesforce.com/content/learn/modules/data_security/data_security_roles", "Each role stands for the level of data access a user or group needs, which needn't follow the organisation chart.") },
    { text: "Spread record ownership, so no one user owns a very large share of an object.", source: page("Trailhead", "Design Your Data Model", "https://trailhead.salesforce.com/content/learn/modules/large-data-volumes/design-your-data-model", "Plan record ownership carefully to avoid data skew.") },
    { text: "Use a before-save flow when automation only changes fields on the record being saved.", source: RECORD_TRIGGERED("When automation only changes field values on the triggering record, a before-save update avoids a second save.") },
    { text: "Where one user must own a great many records, keep that user out of the role hierarchy.", source: SHARING("A user who owns a very large number of records shouldn't hold a role, or should sit alone in a separate branch at the top.") },
    { text: "Run scheduled jobs as active users, and reassign them before anyone is deactivated.", source: null },
  ],
  data_quality: [
    { text: "Turn on matching and duplicate rules for the objects your teams rely on.", source: DATA_HABITS("Matching and duplicate rules cut duplication and keep records consistent.") },
    { text: "Use validation rules to stop records being saved when they break your business rules.", source: page("Trailhead", "Get Started with Validation Rules", "https://trailhead.salesforce.com/content/learn/modules/validation-rules/get-started-with-validation-rules", "Validation rules are a safeguard that stops users saving records that don't meet your business rules.") },
    { text: "Review opportunities stuck in one stage, or still open after their close date.", source: USAGE("Opportunities stuck in the same stage for too long are a sign that users aren't keeping their data up to date.") },
    { text: "Make the fields that matter required, so records aren't saved incomplete.", source: OBJECTS("Making important fields required avoids incomplete data.") },
    { text: "Move records owned by deactivated users to active owners.", source: page("Trailhead", "Control Access to the Org", "https://trailhead.salesforce.com/content/learn/modules/data_security/data_security_org", "A deactivated user's records stay in the org, and can still be transferred to other users.") },
  ],
  utilisation: [
    { text: "Match each user to the licence their work actually needs.", source: WA_COST("Licence optimisation means matching every user to the right licence type.") },
    { text: "Audit licence use on a regular cycle, not only at renewal.", source: WA_COST("Licence assignments should be audited on a regular cadence, not only when the contract renews.") },
    { text: "Review reports and dashboards as part of regular clean-up, and retire what nobody uses.", source: DATA_HABITS("Regular clean-up includes reviewing reports, dashboards, list views and templates.") },
    { text: "Check each dashboard's running user, because viewers see data as that person.", source: page("Trailhead", "Visualize Your Data with the Lightning Dashboard Builder", "https://trailhead.salesforce.com/content/learn/modules/lex_implementation_reports_dashboards/lex_implementation_reports_dashboards_visualizing_data", "People who open a dashboard see its data as the user it runs as.") },
    { text: "Retire features that were built but never taken up.", source: WA_COST("Unused functionality builds up when features are implemented but never fully adopted, and it carries a cost.") },
  ],
  adoption: [
    { text: "Track who logs in, and follow up with the people who don't.", source: USAGE("Look at who is logging in, and above all who isn't, and why.") },
    { text: "Watch record creation and updates to see whether people really work in Salesforce.", source: USAGE("Records created and updated show whether, and how often, users work with the data.") },
    { text: "Build adoption dashboards around the use you want, and review them regularly.", source: USAGE("Adoption dashboards should be built around the pattern of use you're aiming for.") },
    { text: "Use the Lightning Usage App to follow how people take up Lightning Experience.", source: page("Trailhead", "Measure Lightning Experience Performance and Experienced Page Time", "https://trailhead.salesforce.com/content/learn/modules/lightning-experience-performance-optimization/measure-lightning-experience-performance-and-experience-page-time-ept", "The Lightning Usage App tracks adoption and use of Lightning Experience.") },
    { text: "Use in-app guidance to help people get started and find what they need.", source: page("Trailhead", "Get Started with User Engagement", "https://trailhead.salesforce.com/content/learn/modules/user-engagement/get-started-with-user-engagement", "User engagement means onboarding and helping users through guidance inside the app.") },
  ],
  current_pipeline: [
    { text: "Keep every open deal's close date in the future, and move it when the deal changes.", source: page("Trailhead", "Work Your Opportunities", "https://trailhead.salesforce.com/content/learn/modules/leads_opportunities_lightning_experience/work-your-opportunities", "An opportunity's close date is set to a date in the future.") },
    { text: "Agree how long a deal may sit in each stage, and report on the deals that go over.", source: page("Trailhead", "Identify Stuck Sales Deals", "https://trailhead.salesforce.com/content/learn/modules/sales-deal-acceleration/identify-stuck-sales-deals", "Many companies set acceptable times in each stage as part of their sales process, which makes stuck deals easy to find.") },
    { text: "Give each deal its next actions and a timeline, so the next step is always known.", source: PIPELINE("A healthy pipeline has action items and priorities for each deal, with a timeline.") },
    { text: "Use Path on opportunities, with the key fields and guidance for success at each stage.", source: PATH("Path shows reps the fields that matter at each stage of the sales process, with guidance for success.") },
    { text: "In pipeline reviews, look at overdue deals and at recent changes to close date, stage and amount.", source: page("Trailhead", "Review Deal Health with Pipeline Inspection", "https://trailhead.salesforce.com/content/learn/modules/introduction-to-revenue-intelligence-for-sales-cloud/review-deal-health-with-pipeline-inspection", "Pipeline inspection highlights recent changes to amounts, close dates and stages, so reviews can focus on them.") },
  ],
  historic_pipeline: [
    { text: "Make each stage match how your team decides a deal has really moved on.", source: page("Trailhead", "Customize Agentforce Sales for Your Business", "https://trailhead.salesforce.com/content/learn/modules/sales-cloud-configuration-basics/customize-sales-cloud", "The opportunity stages should reflect the way your sales team decides a prospect has moved to the next stage.") },
    { text: "Agree clear exit criteria for every stage, so each one means the same to every rep.", source: PIPELINE("Confirm what has to be true for a deal to leave each stage of the pipeline.") },
    { text: "Require a lost reason, from a fixed list, whenever a deal is closed as lost.", source: page("Trailhead", "Configure the System", "https://trailhead.salesforce.com/content/learn/modules/competitor-tracking-in-salesforce/configure-the-system", "A validation rule makes the lost reason field required when a deal's stage is Closed Lost.") },
    { text: "Report wins and losses by competitor and lost reason, and share what you learn with sales.", source: page("Trailhead", "Build Reports and Dashboards", "https://trailhead.salesforce.com/content/learn/modules/competitor-tracking-in-salesforce/build-reports-and-dashboards", "Reports can show the win and loss ratio by competitor, so sales can see where deals are won and lost.") },
  ],
  forecast: [
    { text: "Map each opportunity stage to the forecast category that matches its chance of closing.", source: FORECASTING("Each opportunity stage lines up with a forecast category, which shows how likely a deal is to close.") },
    { text: "Load quotas for everyone who forecasts, so the forecast shows progress against target.", source: FORECASTING("Quota data is optional, but strongly recommended for each forecast period and type.") },
    { text: "Keep the forecast hierarchy up to date as people join, leave or change roles.", source: FORECASTING("The forecast hierarchy needs updating as people's roles change.") },
    { text: "Let managers adjust the forecast itself, rather than editing the deals underneath it.", source: FORECAST_SETUP("Adjustments let a team change its rolled-up number without changing the underlying opportunities.") },
    { text: "Turn on forecasting only for the people who take part in it.", source: FORECAST_SETUP("Only users who take part in forecasting should be enabled for it.") },
  ],
  activity: [
    { text: "Log calls and meetings against the record they're about, such as the opportunity.", source: page("Trailhead", "Empower Your Users with Quick Actions", "https://trailhead.salesforce.com/content/learn/modules/lex_customization/lex_customization_actions", "Actions let users record calls, meetings and other interactions against the specific record they relate to.") },
    { text: "Capture emails and meetings automatically, so they show on the records they relate to.", source: page("Trailhead", "Set Up the Gmail Integration and Einstein Activity Capture", "https://trailhead.salesforce.com/content/learn/modules/gmail_integration/gmail_integration_unit_2", "With Einstein Activity Capture, the emails and events reps send and receive are added to the activity timeline of related records.") },
    { text: "Log email conversations to the right records, so the rest of the team stays up to date.", source: EMAIL_TOOLS("Logging email conversations to the relevant records keeps teammates up to date.") },
    { text: "Check each deal's activity timeline for recent activity and next steps.", source: page("Trailhead", "Understand Deal Health with Insights and Scores", "https://trailhead.salesforce.com/content/learn/modules/sell-smarter-with-pipeline-inspection/understand-deal-health-with-insights-and-scores", "The activity timeline shows recent and upcoming activities and next steps for a deal.") },
  ],
  lead_funnel: [
    { text: "Follow up new inbound leads promptly, because a slow response loses deals.", source: page("Trailhead", "Transform Your Business with Sales Engagement", "https://trailhead.salesforce.com/content/learn/modules/high-velocity-sales/manage-successfully-with-high-velocity-sales", "Every inbound lead could become a sale, and delays in following up can lose revenue.") },
    { text: "Route new leads automatically, by territory or to a queue, through a set assignment process.", source: LEADS("Many companies assign leads automatically, for example to owners by where the lead is.") },
    { text: "Keep lead status current, so you can see where every lead is in your process.", source: LEADS("A lead's status shows where it is in the sales process.") },
    { text: "Write down what makes a lead qualified, and convert it once it meets that bar.", source: LEADS("The criteria for qualifying and converting leads are part of each company's own business process.") },
    { text: "Use campaigns to see how many leads and how much pipeline each marketing effort brings.", source: page("Trailhead", "Meet Salesforce Campaigns", "https://trailhead.salesforce.com/content/learn/modules/campaign_basics/campaigns_basics_unit_1", "Campaigns show marketers how many leads they generate and how much pipeline they build.") },
  ],
  revenue_opportunities: [
    { text: "Review stalled deals with each rep regularly, as part of coaching.", source: STALLED("Sales managers should regularly review stalled opportunities with their reps.") },
    { text: "Agree a plan to move each stalled deal on, or lower its priority if it isn't worth it.", source: STALLED("For each stalled deal, develop an action plan to move it along, or reprioritise it.") },
    { text: "Keep stalled deals on a dashboard that managers and reps both see.", source: STALLED("A dashboard is the best way to keep track of idle opportunities.") },
    { text: "Keep account plans for key accounts, with clear objectives, and track progress against them.", source: page("Trailhead", "Explore and Create Account Plans", "https://trailhead.salesforce.com/content/learn/modules/account-based-selling/explore-and-create-account-plans", "Account plans set clear objectives backed by measures, and progress against them is monitored over time.") },
    { text: "Review lost deals and lapsed customers regularly, and agree specific steps to win them back.", source: null },
  ],
  service_operations: [
    { text: "Route every case to a team's queue with assignment rules, so none waits for someone to notice it.", source: CASE_QUEUES("Queues hold a support team's cases, and case assignment rules decide which queue each new case goes to, for example by its record type.") },
    { text: "Escalate cases that stay open too long to a team that can close them.", source: ESCALATION("An escalation rule moves a case that has stayed open past a set time to another queue and can notify people, so stalled cases don't get forgotten.") },
    { text: "Set first response and resolution targets as milestones, so agents see each deadline coming.", source: ENTITLEMENTS("An entitlement process is a timeline of the steps to resolve a case, and its milestones are the deadlines in it, such as how soon to reply first and how soon to resolve.") },
    { text: "Record where every case came from and why the customer got in touch.", source: null },
    { text: "Review the backlog every week by age and priority, and work the oldest high-priority cases first.", source: null },
  ],
  // Salesforce CPQ (DECISIONS.md 223). Read on 6 November 2026.
  cpq_quoting: [
    { text: "Set discount limits by deal size, and send anything deeper through approval.", source: null },
    { text: "Compare each team's and rep's discounts with what they win, so you know what discounting buys.", source: null },
    { text: "Create renewal opportunities well before contracts end, so renewals are in the forecast.", source: null },
    { text: "Order every won quote, if orders are how you bill and deliver.", source: null },
    { text: "Keep one primary quote on each opportunity, so reports read the price that was agreed.", source: null },
  ],
  cpq_setup: [
    { text: "Give price rules that set the same field different evaluation orders, so it's clear which one wins.", source: { ...page("Trailhead", "Sequence Price Rules for Correct Calculations", "https://trailhead.salesforce.com/content/learn/modules/price-rules-in-salesforce-cpq/sequence-price-rules-for-correct-calculations", "CPQ orders price rules by their evaluation event, then their evaluation order, then each action's order, and within an event the rule with the larger evaluation order wins a conflict."), checked: "2026-11-06" } },
    { text: "Keep a quote line field and its twin on the opportunity or order product to the same API name and data type.", source: { ...page("Trailhead", "Create Twin Fields on the Quote Line", "https://trailhead.salesforce.com/content/learn/projects/personalize-smartwatches-with-salesforce-cpq-attributes/create-twin-fields-on-the-quote-line", "CPQ copies a value between twin fields automatically, as long as their API names and data types match exactly."), checked: "2026-11-06" } },
    { text: "Put a price rule that feeds another rule's conditions in an earlier evaluation event.", source: { ...page("Trailhead", "Sequence Price Rules for Correct Calculations", "https://trailhead.salesforce.com/content/learn/modules/price-rules-in-salesforce-cpq/sequence-price-rules-for-correct-calculations", "Rules in an earlier evaluation event always run before rules in a later one, so a rule whose result another rule depends on belongs in an earlier event."), checked: "2026-11-06" } },
    { text: "Delete price and product rules you've switched off for good, so the active set tells the whole story.", source: null },
    { text: "Try pricing changes in a sandbox against real quotes before they go live.", source: null },
  ],
  service_setup: [
    { text: "Give each case queue a routing configuration, so work is pushed to agents with room for it.", source: page("Trailhead", "Optimize Workflows with Enhanced Omni-Channel Routing", "https://trailhead.salesforce.com/content/learn/modules/enhanced-omni-channel/improve-work-distribution-with-enhanced-omni-channel", "Work lands in a queue, and the routing engine passes it to an available member of that queue by remaining capacity, following the queue's routing configuration.") },
    { text: "Attach the article that solved a case, so the next agent can see what the customer already tried.", source: page("Trailhead", "Close Cases with Articles", "https://trailhead.salesforce.com/content/learn/modules/lightning-knowledge-setup-and-customization/close-cases-with-articles", "Agents should attach knowledge articles to cases, so the case shows what helped and a new agent on a reopened case can see what was tried.") },
    { text: "Keep common replies and repeated steps as macros, so agents don't write each answer again.", source: page("Trailhead", "Learn Macros, Service Console, and Case Feed", "https://trailhead.salesforce.com/content/learn/modules/service-cloud-essentials-features/learn-about-macros-service-console-and-case-feed", "Macros let agents save common responses and reuse them, rather than writing a new answer to every repeated problem.") },
    { text: "Give every case queue members, and keep each agent to the few queues they really work.", source: null },
    { text: "Give Service Cloud licences to people who work cases, and review them when someone moves on.", source: null },
  ],
};
