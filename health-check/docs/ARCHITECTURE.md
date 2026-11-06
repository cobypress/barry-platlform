# Black Cloud Barometer: architecture

Barometer is a premium Salesforce health check, run by Black Cloud consultants. This service holds all extraction, analysis, narrative and rendering. The `blackcloud-next` site only reads Neon, adds jobs and shows results.

## Where things live

| Part | Location | Status |
| --- | --- | --- |
| Engine (this service) | `barry-platlform/health-check` | Milestone 1 |
| External Client App and permission set | `barry-platlform/health-check/salesforce` | Milestone 2 |
| Console pages | `blackcloud-next/app/internal/health-check` | Milestone 5 |
| Queue | BullMQ `health-check-jobs`, separate from `barry-jobs` | Milestone 5 |
| Database | Neon through Prisma, with SQL files in `blackcloud-next/migrations` | Milestone 5 |
| Files | Private object storage in the UK (see DECISIONS.md) | Milestone 5 |

## How a run flows

```
hc run ──> ConnectionProvider ──> SalesforceClient ──> Extractors ──> checkpoints/
                                   (allowlist,                            │
                                    API budget)                           v
raw-notes.md <── renderRawNotes <── findings, metrics, manual steps <── analyse()
      ^                              (tokens only)                  metrics, rules, guards
      └── names restored here and nowhere else
```

Extraction talks to Salesforce and costs API calls. Analysis is a pure function of the checkpoints, so it can be re-run for free and always gives the same result.

## Modules

```
src/
  schemas/      Zod schemas, including the ones the console will need
  salesforce/   Read-only client, allowlist, API budget, connection providers, OAuth and token storage
  connect/      Connect links for customer admins, and the check that a connection can't write
  extract/      One extractor per step. Each output is a checkpoint
  guards/       Checks a destructive recommendation has to pass
  domains/      Plug-in modules: extractors, metrics, rules
  engine/       Run orchestration, rule evaluation, manual steps
  privacy/      Pseudonymiser and secret redaction
  notes/        Markdown raw notes
  numbercheck/  Reads every figure in text and traces it to the findings dataset
  narrative/    Section builder, ranking and plan, Claude drafting and its guards
  design/       Design tokens: the only place colours and type are set
  render/       Charts, Word and PDF output, and the layout checks
  money/        What a seat costs (calibration, then list price), list prices with their sources, and the annual cost a report states
  pulse/        The monthly pulse's result: what changed since its baseline, for the console to show
  practice/     What good looks like: each area's statements, with their public sources, as data
  benchmark/    Anonymised rows for the benchmark set, with consent, and their summary; published research and the comparison a report shows
  retention/    Purging raw extracts, and whole run folders, from released runs (the worker's daily sweep is worker/retention.ts)
  cli/          hc
  testing/      Synthetic org, simulator, recording and replay
```

### Adding a domain

A domain is a `DomainModule` (`src/domains/types.ts`): extractors, metric builders and rules. Add it to the list in `src/domains/index.ts`. Nothing in `src/engine` changes, and a test proves this with a stand-in Service Cloud module. A Sales Cloud area also needs its place in `SALES_CLOUD_DOMAINS`, a label in `DOMAIN_LABELS` and a weight in `AREA_WEIGHTS`, and goes in `ESSENTIALS_DOMAINS` when it reads no CRM records, as adoption does (DECISIONS.md 99).

Integrations and technical health (`src/domains/integrations/`, `src/domains/technical-health/`, DECISIONS.md 169 to 178) were made from checks that sat in security and automation. `AREAS_WITHIN` in `src/domains/index.ts` makes a run that names the older area run the newer one too, so module lists saved before them still run every check. Checks that need to know what Apex code does read facts `src/extract/apex-scan.ts` works out while the extractor holds the body. The body itself is never kept.

The action lists (`src/actions/`, DECISIONS.md 191 to 200) are built in the worker. A rule hands the records it judged to the engine as `records` on its issue: ids and what's wrong, never a name. The engine keeps them out of the finding and joins names from the extracts into `action-lists.json`, which the documents turn into a workbook and a ticket file.

Service Cloud is the first module beyond Sales Cloud (`src/domains/service/`, DECISIONS.md 155 to 158): two areas in `SERVICE_CLOUD_DOMAINS`, `SERVICE_CLOUD_MODULES` alongside `SALES_CLOUD_MODULES`, and `modulesForRun`, which turns a run's `service` module into both. Its synthetic data and traps are at the end of `src/testing/synthetic-org.ts`.

Salesforce CPQ (`src/domains/cpq/`, DECISIONS.md 216 to 226) is built the same way, as the `cpq` module. Its objects belong to a managed package, so `src/extract/cpq.ts` describes each one before querying it, and reads nothing at all unless the package is installed.

## The rules, and where each is enforced

| Rule | Enforced by |
| --- | --- |
| 1. Code calculates, Claude explains | No Claude in Milestone 1. `Finding.facts` holds every figure for the number check |
| 2. Every finding has evidence | `Finding` schema. A finding without it can't parse |
| 3. Guards before destructive advice | `applyGuards` in the engine, and again in the `Finding` schema. Unknown blocks like failed |
| 4. Segment before concluding | `runRule` turns an unsegmented pass into not measurable. `Metric` has the same rule |
| 5. Not measurable, never estimated | `Sourced` keeps "couldn't read" apart from "nothing there". `Metric` has no value field when not measurable |
| 6. Read-only | `authorise()` runs on every request immediately before it's sent |
| 7. CRM stays in the worker | Findings hold tokens. `scrubDeep` is a backstop on everything the engine writes. Everything sent to Claude goes through `sendPrompt`, which masks names and their parts, then refuses the send if any part is left (see "Free text and names") |
| 8. No customer data in repos | The CLI refuses to write inside a git work tree. Fixture tests require reserved domains and zero-padded ids |
| 9. API budget and resume | `ApiBudget` stops before the call that crosses the share. Steps and Opportunity pages are checkpointed |
| 10. Approval before release | `ReportSection` schema and `canRelease()`, used in Milestone 5 |
| 11. Benchmarks separate | `BenchmarkRow` is strict: no extra fields, whole units, bands and quarters only. `hc benchmark` builds rows from an allowlist, needs consent, and records it in the run folder. A report shows our own customers only as a range from 8 orgs, and groups by industry and size from 10 (`src/benchmark/compare.ts`) |

## The read-only boundary

Filtering by HTTP method wouldn't work in Salesforce. `GET tooling/executeAnonymous` runs Apex, a GET on an Apex REST class can do anything, and `POST ?_HttpMethod=PATCH` is an update. So `allowlist.ts` lists exact routes.

- GET is allowed for: versions, limits, record counts, query and query paging (REST and Tooling), describe, and a report's describe in the Analytics API (`analytics/reports/<00O id>/describe`, nothing else under `analytics`: a GET on the report runs it).
- POST is allowed for two endpoints, each with its body inspected:
  - Composite batch: every subrequest must be a GET to the REST query route with safe SOQL.
  - Metadata SOAP: exactly one operation, either `retrieve` or `checkRetrieveStatus`, matching the `SOAPAction`.
- SOQL must be a single SELECT. `FOR VIEW`, `FOR REFERENCE`, `FOR UPDATE`, `UPDATE TRACKING` and `UPDATE VIEWSTAT` are refused because they change data.

`client.ts` has one send path, and the client has no generic request method. Tests pin the list of client methods, try each allowed path with every other method, and scan the source for any other route to the network.

The OAuth token and revoke endpoints are the only other way out. They sit in `transport.ts` too, held to those two paths on a Salesforce host (DECISIONS.md 57).

## Connecting an org

```
consultant ──> connect link ──> customer admin opens it ──> Salesforce sign-in as any user of the org
                                                                     │  (PKCE, state, prompt=login consent)
hc run <── OAuthConnectionProvider <── TokenStore <── code exchange ─┘
              │  refresh with rotation,       (AES-256-GCM,      └──> permission check: a note for the consultant
              │  same org and user only        key from HC_TOKEN_KEY or the keychain)
              v
       SalesforceClient (unchanged)
```

- **Two providers.** `SfCliConnectionProvider` borrows a consultant's CLI login, for development. `OAuthConnectionProvider` uses tokens a customer's admin granted to Barometer's External Client App, as whichever user they signed in as. Both need the name in `HC_APPROVED_ORG_ALIASES`.
- **Read-only is the allowlist.** Any user may connect, an administrator included (DECISIONS.md 151), so the allowlist alone stops the engine sending a write. A customer who connects as a user holding only Barometer Read Only adds Salesforce's own refusal. `checkReadOnlyUser` reads the user's permissions at connect time, and the result is a note for the consultant, never a refusal.
- **The app** ships to customers in the Black Cloud Barometer managed package, built from `salesforce/force-app` (DECISIONS.md 65 and 66). The package's install script creates Barometer Read Only (67). `salesforce/source-org` is what stays in BlackCloudProd, the app's source org. `salesforce/dev-only` holds the reference permission set and a JWT bearer app for `bc-hc-dev` only.

### The connect link, and what the console adds in Milestone 5

`src/connect/link.ts` has the logic. The console adds storage and four pages.

1. **The consultant makes a link** in the console for a customer: it stores the `ConnectLink` (the secret's hash, 72-hour expiry) and shows the URL once, to send by email.
2. **The admin opens it.** The page says who's asking and why, and that Barometer only reads. A user holding only Barometer Read Only is optional. It names the two permissions they'll be asked for: "Manage user data via APIs" (`api`) and "Perform requests at any time" (`refresh_token`).
3. **They click Continue.** The console runs `beginAuthorisation`, keeps the `PendingAuthorisation` encrypted (it holds the PKCE verifier), and redirects to Salesforce, which asks them to sign in afresh.
4. **Salesforce sends them back** to the console's callback. `completeAuthorisation` checks the state and timing, the worker exchanges the code, stores the tokens and runs the read-only check. Only then is the link marked used. The page says whether it worked, and the consultant sees what the user could change.

The link can also be withdrawn. A link that's expired, used or withdrawn tells the admin to ask their consultant for a new one.

## Platform behaviours handled

| Behaviour | Handling |
| --- | --- |
| `SetupAuditTrail` Section, Display, Action can't be filtered | `assertSafeSoql` stops the query before it costs a call. Rows are filtered in code |
| `LoginHistory` Status, Application can't be filtered | The same |
| Some Tooling queries come back empty through composite batch | Tooling is refused in composite. Tooling queries go direct |
| `LastViewedDate` is per user | Dashboard use is reported as not measurable, because the query would only show the connected user's views |
| Aggregate queries can't page | A capped ownership result marks the scan incomplete, and the guard reads unknown |
| A detail object has no `OwnerId` | `INVALID_FIELD` means not applicable, not a gap in the scan |
| A plain query skips activities archived after 365 days | Activity reads 12 months and declares it. `queryAll` isn't on the allowlist, and won and lost comparisons only use deals created in that year |
| Conversion moves a lead's activity to its contact | First response looks at both ids |
| Other people's private reports can't be read | They're counted with `USING SCOPE allPrivate`. If any exist, the report guard is unknown, and field removal stays blocked |
| Without Manage All Private Reports, `USING SCOPE allPrivate` counts every visible report and refuses rows | One row is asked for first. If that fails, the count is unknown, and so is the report guard |
| A read-only connection can't use the Metadata API | Automation metadata falls back to Tooling, one `Metadata` read per item, and report definitions to the Analytics API's report describe, one call per report. Both checkpoint each item. Approval processes and assignment rules can't be read that way, so reference guards stay unknown when any exist (DECISIONS.md 73) |
| Tooling `ApexClass` and `CustomObject` need more than View Setup | Classes are read through REST, and custom objects through Tooling `EntityDefinition` |
| Forecast rows show today's position | Forecast accuracy is not measurable without a snapshot |
| Admins can rename forecast categories | No Commit rows is not measurable, not a clean Commit |
| `CloseDate` and `LastActivityDate` have no time zone | Instants become days in the org's zone, and date-only fields are left alone |
| A field the org doesn't have fails the whole query | `field-describe` checks feature-dependent objects first. A missing optional field is dropped and named in `missingFields`, and only rules that need it are not measurable |
| An expired session reaches SOAP as a fault, not a 401 | The client refreshes once, then rebuilds the envelope with the new token |
| `Layout` and `ApexTrigger` can name a custom object by its id, in 15 or 18 characters | Tooling `CustomObject` maps the first 15 characters to a name. Two ids that would make the same name are both left unnamed. The inventory and the trigger rules share this, and each says how many it couldn't place |

## Data handling

Run folders default to `~/.barometer/runs/<run id>`, with files readable only by the owner.

The benchmark set is kept apart, in `~/.barometer/benchmarks/rows.jsonl` (folder 700, file 600). It holds `BenchmarkRow` lines only, with nothing that links a row to a run.

| File | Holds | Goes to Claude later? |
| --- | --- | --- |
| `checkpoints/` | CRM rows. Deleted by `hc purge` 30 days after delivery | Never |
| `purged.json` | When the checkpoints were deleted, and how many files | No |
| `benchmark-consent.json` | Who agreed to the benchmark export, how, when, and which metric keys went | No |
| `pseudonym-map.json` | Names by token, and a token for each first name two people share | Never |
| `findings.json`, `metrics.json`, `manual-steps.json` | Aggregates, metadata names and tokens | Yes |
| `raw-notes.md` | Real names | Never: customer deliverable |
| `raw-notes.docx`, `executive-summary.pdf`, `full-health-check.pdf` | Real names in the notes, team level in the PDFs | Never: customer deliverables |
| `report-sections.json`, `layout-report.json`, `narrative-rejections.json` | Section text, tokens and check results | No: review records |

## Milestone 3 notes on the Claude API

Checked against the current Anthropic docs on 2026-11-02. Model ids live in `src/config.ts`.

- `claude-opus-5-5` for synthesis, `claude-sonnet-5-5` for notes and screenshots.
- Thinking can't be turned off on Opus 5.5. Set `output_config.effort` explicitly, because its default is `medium`.
- Forced `tool_choice` returns a 400 on both models. Use structured outputs (`output_config.format`) for narrative JSON.
- `temperature`, `top_p` and `top_k` return a 400. Assistant prefill also returns a 400.
- Handle `stop_reason: "refusal"`, and use streaming for long output.
- The site is on `@anthropic-ai/sdk` 0.87. This service should take the current major when it first needs the SDK.

## From findings to documents

```
findings, metrics ──> buildReport ──> sections and versions ──> PDF and Word
                          │                   ^
                     figure dataset           │ accepted drafts only
                          │                   │
                          └──> number check <── draftSection <── Claude (tokens only)
```

`hc render --run <folder>` does this for a finished run. It writes into the run folder, so it refuses a folder inside a git work tree.

- Every section starts with text the engine wrote, so a report exists without Claude. A draft that fails the number check is dropped, and the section goes to a person.
- `sendPrompt` is the only way to Claude. It runs `guardPrompt` on the prompt, then `findNames` checks the result, and evidence isn't sent. See "Free text and names".
- Names return in one place for each document: `pseudonyms.restore` in the Word builder, and `Render.t` in the PDF templates. Executive and full reports describe people as "a single rep", so only the raw notes name anyone.
- The CEO one-pager and the readout kit come from the same built report: the one-pager through `Render.t`, the kit through `pseudonyms.restore`, as the raw notes are. The kit is Black Cloud's only, and goes to its own Drive folder, never a client's (DECISIONS.md 138).
- Chromium runs with no network access. It only lays out HTML we built, with fonts embedded.
- Before a PDF is written, `checkLayout` measures every page in the browser for overflow, missing charts, empty sections and leftover tokens, and runs the number check again on the text as printed. Problems go to `layout-report.json`, and `hc render` exits non-zero.
- Every unapproved document is marked as a draft on every page. `canRelease` decides that, and the release step itself belongs to the console.

## Free text and names

Free text that people write can reach Claude. Today that's metadata labels and picklist values inside findings and metrics, and a consultant's reason for skipping a manual step, which becomes a finding's `notMeasurableReason`. Intake, overrides and calibration text are kept out of every brief. Consultant chat and call notes arrive in Milestone 5, and must use `sendPrompt` like everything else. `docs/overnight/night-2/scrub.md` maps every route.

```
text ──> scrub() ──> maskNames() ──> redactSecrets() ──> findNames() ──> client
         whole       parts of        keys and            anything left?
         names, ids, every known     tokens              refuse, and record
         emails      name                                a rejection
```

- `scrub()` replaces whole names, usernames, ids and any email, in any case.
- `maskNames()` (`src/privacy/names.ts`) replaces parts of every person's name: first names, surnames, runs such as "Mary Ann" or "van der Berg", the pieces of hyphenated and apostrophe names, the 15-character form of an id, and a short list of English diminutives (Sam, Samuel). Each part gets its owner's token, so a restored report says "Sam Reed said". It matches after folding: accents, full-width letters and invisible characters don't hide a name, and possessives, quotes and API names (`Sam_Reed_Approval`) are handled. The customer's company name is masked the same way, with a capital only.
- A part two or more people share (two Sams, or one person's first name and another's surname) gets a token of its own. It restores to the part as written ("Sam"), so the report reads exactly as the writer put it and Claude is never told the wrong person. It's a user token, so executive text still says "a single person". `hc run` saves these tokens in the pseudonym map.
- Parts that are also words (Will, May, Mark, Rose, Price, Cross and about 200 more, in `COMMON_WORDS`) are masked only with a capital. A capital at the start of a sentence, a month ("in May") or a product ("Price Book") is masked too: we chose over-scrubbing. Inside a hyphenated word such a part is left, so "Cross-sell" survives. Shared-user words (Sales, Integration) and lone particles (van, de) are never masked.
- `findNames()` looks at every run of letters on its own, not words and phrases, and refuses the send if any part of a known name, a 15-character id or an email is left. It names tokens, never what it found. Claude's replies go through the same two checks before they're accepted. Only our own constant system prompts may be sent.
- Not covered: nicknames outside the short list, initials, misspellings, names run together in camel case (`SamReedApproval`), lookalike letters from other alphabets, and people the run doesn't know. A run knows its org's users and the customer's company name; contacts and leads aren't read, so their names in free text get through. Images, such as manual-step screenshots, can't be scrubbed as text.

## Later milestones

- **OAuth:** built as a new `ConnectionProvider` (above). The console pages and the managed package remain.
- **Jobs:** each extractor step is already a unit of work. A BullMQ job carries only a run id and step id, as `lib/funnels/sync.ts` does.
- **Run state:** `run.json` and the output files become Neon rows and object storage keys, using the same schemas.
- **Review console:** `ReportSection`, `SectionVersion`, `WordingProposal`, `NumberOverride` and `Release` are already defined.
- **Repeat checks:** finding ids are hashes of rule and subject, so they line up across runs. `Run.baselineRunId` links the two.
