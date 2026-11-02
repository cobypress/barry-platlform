# Black Cloud Barometer: architecture

Barometer is a premium Salesforce health check, run by Black Cloud consultants. This service holds all extraction, analysis, narrative and rendering. The `blackcloud-next` site only reads Neon, adds jobs and shows results.

## Where things live

| Part | Location | Status |
| --- | --- | --- |
| Engine (this service) | `barry-platlform/health-check` | Milestone 1 |
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
  salesforce/   Read-only client, allowlist, API budget, connection providers
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
  cli/          hc
  testing/      Synthetic org, simulator, recording and replay
```

### Adding a domain

A domain is a `DomainModule` (`src/domains/types.ts`): extractors, metric builders and rules. Add it to the list in `src/domains/index.ts`. Nothing in `src/engine` changes, and a test proves this with a stand-in Service Cloud module.

## The rules, and where each is enforced

| Rule | Enforced by |
| --- | --- |
| 1. Code calculates, Claude explains | No Claude in Milestone 1. `Finding.facts` holds every figure for the number check |
| 2. Every finding has evidence | `Finding` schema. A finding without it can't parse |
| 3. Guards before destructive advice | `applyGuards` in the engine, and again in the `Finding` schema. Unknown blocks like failed |
| 4. Segment before concluding | `runRule` turns an unsegmented pass into not measurable. `Metric` has the same rule |
| 5. Not measurable, never estimated | `Sourced` keeps "couldn't read" apart from "nothing there". `Metric` has no value field when not measurable |
| 6. Read-only | `authorise()` runs on every request immediately before it's sent |
| 7. CRM stays in the worker | Findings hold tokens. `scrubDeep` is a backstop on everything the engine writes |
| 8. No customer data in repos | The CLI refuses to write inside a git work tree. Fixture tests require reserved domains and zero-padded ids |
| 9. API budget and resume | `ApiBudget` stops before the call that crosses the share. Steps and Opportunity pages are checkpointed |
| 10. Approval before release | `ReportSection` schema and `canRelease()`, used in Milestone 5 |
| 11. Benchmarks separate | `BenchmarkRow` is strict: no extra fields, whole units, bands and quarters only |

## The read-only boundary

Filtering by HTTP method wouldn't work in Salesforce. `GET tooling/executeAnonymous` runs Apex, a GET on an Apex REST class can do anything, and `POST ?_HttpMethod=PATCH` is an update. So `allowlist.ts` lists exact routes.

- GET is allowed for: versions, limits, record counts, query and query paging (REST and Tooling), describe.
- POST is allowed for two endpoints, each with its body inspected:
  - Composite batch: every subrequest must be a GET to the REST query route with safe SOQL.
  - Metadata SOAP: exactly one operation, either `retrieve` or `checkRetrieveStatus`, matching the `SOAPAction`.
- SOQL must be a single SELECT. `FOR VIEW`, `FOR REFERENCE`, `FOR UPDATE`, `UPDATE TRACKING` and `UPDATE VIEWSTAT` are refused because they change data.

`client.ts` has one send path, and the client has no generic request method. Tests pin the list of client methods, try each allowed path with every other method, and scan the source for any other route to the network.

## Platform behaviours handled

| Behaviour | Handling |
| --- | --- |
| `SetupAuditTrail` Section, Display, Action can't be filtered | `assertSafeSoql` stops the query before it costs a call. Rows are filtered in code |
| `LoginHistory` Status, Application can't be filtered | The same |
| Some Tooling queries come back empty through composite batch | Tooling is refused in composite. Tooling queries go direct |
| `LastViewedDate` is per user | Dashboard use is reported as not measurable, because the query would only show the connected user's views |
| Aggregate queries can't page | A capped ownership result marks the scan incomplete, and the guard reads unknown |
| A detail object has no `OwnerId` | `INVALID_FIELD` means not applicable, not a gap in the scan |

## Data handling

Run folders default to `~/.barometer/runs/<run id>`, with files readable only by the owner.

| File | Holds | Goes to Claude later? |
| --- | --- | --- |
| `checkpoints/` | CRM rows | Never |
| `pseudonym-map.json` | Names by token | Never |
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
- `guardPrompt` runs on everything sent to Claude. Evidence isn't sent.
- Names return in one place for each document: `pseudonyms.restore` in the Word builder, and `Render.t` in the PDF templates. Executive and full reports describe people as "a single rep", so only the raw notes name anyone.
- Chromium runs with no network access. It only lays out HTML we built, with fonts embedded.
- Before a PDF is written, `checkLayout` measures every page in the browser for overflow, missing charts, empty sections and leftover tokens, and runs the number check again on the text as printed. Problems go to `layout-report.json`, and `hc render` exits non-zero.
- Every unapproved document is marked as a draft on every page. `canRelease` decides that, and the release step itself belongs to the console.

## Later milestones

- **OAuth:** a new `ConnectionProvider`. Nothing else changes.
- **Jobs:** each extractor step is already a unit of work. A BullMQ job carries only a run id and step id, as `lib/funnels/sync.ts` does.
- **Run state:** `run.json` and the output files become Neon rows and object storage keys, using the same schemas.
- **Review console:** `ReportSection`, `SectionVersion`, `WordingProposal`, `NumberOverride` and `Release` are already defined.
- **Repeat checks:** finding ids are hashes of rule and subject, so they line up across runs. `Run.baselineRunId` links the two.
