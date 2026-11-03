# Bulk API: options

For Coby. DECISIONS.md 5 says there's no Bulk API yet, because a bulk query is a POST that creates a job, and that widens the read-only boundary. This note covers what we'd gain, what it would take, and the alternatives.

**Recommendation: don't build it yet.** Most of our caps are our own settings rather than Salesforce limits. The one that is a Salesforce limit, grouped counts that can't page, is something Bulk can't do either. Two smaller REST changes cover the likely gaps for about a day's work. Build Bulk only once a real org shows that REST can't keep within the API budget.

## Where REST paging stops today

A REST query page holds up to 2,000 rows, and each page is one API call (REST API guide, `Sforce-Query-Options`). Every cap below is set in our code. When a cap is hit, the rules that depend on it report not measurable and give the reason.

| Read | Cap | Where | When it's hit |
| --- | --- | --- | --- |
| `LoginHistory`, last 90 days | 20,000 rows | `extract/users.ts` `LOGIN_MAX_ROWS` | The personal account used as an integration rule is not measurable |
| Ownership scan | 40 custom objects, chosen by record count | `extract/users.ts` `MAX_CUSTOM_OBJECTS` | The rest are marked incomplete, so the user removal guard reads unknown |
| Ownership scan, per object | One page of `GROUP BY OwnerId` results | `client.queryBatch` | An object with more owners than one page holds is marked incomplete. "In REST API, queries that include a GROUP BY clause can't use the query locator" (SOQL guide). **This is a Salesforce limit** |
| `SetupAuditTrail`, last 180 days | 2,000 rows | `extract/configuration.ts` `AUDIT_MAX_ROWS` | The evidence says only the most recent rows were read |
| `ForecastingItem`, `ForecastingFact` | 20,000 rows each | `extract/forecast.ts` | The finding is based on part of the data |
| `Report` | 20,000 | `extract/report-definitions.ts` | The report scan is unavailable |
| Leads, lead status history, campaign members, tasks, events, emails and won accounts | 200,000 rows each | `readCapped` in `extract/opportunities.ts` | Activity is unavailable, because a part would be a biased sample. Campaign members and won accounts are not measurable |
| Opportunities and their history, prior wins | None | `readKeyset` | Limited only by the API budget: 5,000 calls a run, and half the org's daily allowance |

At 2,000 rows a call, 200,000 rows costs 100 calls. A million tasks would cost 500 calls, which is within a run's budget. So for every cap except the grouped one, REST can read more if we let it. What it costs is time and calls, not possibility.

## What Bulk API 2.0 query jobs need

From the Bulk API 2.0 Developer Guide (`api_asynch.pdf`, API 68.0) and the limits cheat sheet (`salesforce_app_limits_cheatsheet.pdf`), both at `resources.docs.salesforce.com/latest/latest/en-us/sfdc/pdf/`.

| Step | Method and path | Notes |
| --- | --- | --- |
| Create | `POST /services/data/vXX.X/jobs/query` | JSON body: `operation` (`query` or `queryAll`), `query`, and optionally `contentType` (CSV only), `columnDelimiter` and `lineEnding` |
| Poll | `GET /jobs/query/{jobId}` | Until the state is `JobComplete` |
| Results | `GET /jobs/query/{jobId}/results?locator=…&maxRecords=…` | CSV, paged until `Sforce-Locator` is `null`. You must use the same API version that created the job, or you get a 409 |
| Results in parallel | `GET /jobs/query/{jobId}/resultPages` | API 58.0 and later. Up to five result URLs per response |
| Abort | `PATCH /jobs/query/{jobId}` with `{"state":"Aborted"}` | "you must be the job's creator or have the Manage Data Integrations permission" |
| Delete | `DELETE /jobs/query/{jobId}` | Only once the job is complete, aborted or failed |
| List | `GET /jobs/query` | "information about all query jobs in the org" |

- **Deleting isn't needed.** Jobs in a final state "that are older than seven days are deleted", and results can be read "within 7 days of job completion" (limits cheat sheet). So we'd never send DELETE or PATCH.
- **Limits.** 10,000 query jobs in a rolling 24 hours, and 1 TB of results. Query jobs don't use the 15,000 batch allowance. Bulk API 2.0 counts toward the daily API allocation, but the docs don't say whether each results page counts as one call (QUESTIONS.md 33).
- **What it can't do:** "GROUP BY, OFFSET, or TYPEOF clauses", aggregate functions "such as COUNT()", `FIELDS()`, and parent-to-child subqueries. Large jobs are chunked automatically on supported objects, but "LIMIT and ORDER BY disable PKChunking". Our keyset reads all use `ORDER BY Id`, so a Bulk version would drop it.
- **Permissions.** The `api` scope we already request covers "APIs, such as REST API and Bulk API 2.0" (help article "OAuth Tokens and Scopes"). The guide asks only for API Enabled. Nothing we found requires Manage Data Integrations, View All Data or anything else for a query job. The general API rule says data outside the user's sharing and field-level security isn't returned, but no Bulk-specific statement confirms it. These would be proved on `bc-hc-dev` (QUESTIONS.md 33).
- **Visible to the customer.** Each job creates a record the customer's admin can see in Setup, on the Bulk Data Load Jobs page, for seven days. That's a change in the org, though not to their data. The install guide would need to say so.

## An allowlist that admits only query jobs

Three routes, each held as tightly as composite batch and the Metadata retrieve are now:

1. **`POST /services/data/vXX.X/jobs/query`.** Content type `application/json`, and no query string, which also rules out `_HttpMethod`. The body is parsed strictly: no duplicate keys, and only the five known keys. `operation` must be exactly `query`, so `queryAll` and every ingest operation are refused. `query` must pass `assertSafeSoql`, plus a Bulk check that refuses GROUP BY, aggregates, OFFSET and subqueries before they cost a job.
2. **`GET /jobs/query/{id}`**, where the id matches the job id shape (the guide's examples start `750`; to be confirmed).
3. **`GET /jobs/query/{id}/results`**, with only `locator` and `maxRecords` allowed in the query string.

Everything else stays refused: PATCH, DELETE, the job list, `resultPages` unless we want it, and all of `/jobs/ingest`. The client gets one new method, `bulkQuery(soql)`, with no general route.

**What the tests would prove:**

- **Unit tests.** Each new route refuses every other method. A POST is refused for `queryAll`, `insert`, `upsert`, `delete` and `hardDelete`, for an extra or repeated key, a non-JSON body, an unsafe SOQL statement (`FOR UPDATE`, `UPDATE TRACKING`), a path under `/jobs/ingest`, and an `_HttpMethod` override. The pinned list of client methods gains exactly one entry, and the source scan still finds a single send path.
- **On `bc-hc-dev`, as the read-only user.** A query job is created, polled and read. A `POST /jobs/ingest` sent by the write test script, not the engine, is refused or its job fails without writing. Rows match the same REST query, so sharing and field security behave the same. Results pages show up in `/limits` as calls.

## Alternatives

1. **Raise the REST caps where the budget allows.** Read up to what the run's remaining API budget covers, rather than a fixed 200,000. `ApiBudget` already stops a run before it crosses its share. This is the cheapest change: a few lines and tests.
2. **Complete, shorter windows instead of a biased part.** When a read would pass its cap, read a shorter recent window in full, for example 30 days of logins instead of part of 90, and say so in the evidence. That's better than a partial year in Id order, which is the bias the activity rule refuses.
3. **Split the ownership scan by what each rule asks.** Three rules use the scan today. The user removal guard (utilisation) only needs candidate users, so it could use `WHERE OwnerId IN (…candidates…) GROUP BY OwnerId`. Ownership skew (best practice) only needs owners over 10,000 records, so it could add `HAVING COUNT(Id) > 10000`. Records owned by inactive users (data quality) need a filter on the owner's status, and whether SOQL accepts that on the polymorphic `Owner` is unchecked. Each of these returns few rows, so it never needs to page. The 40-object cap can also rise, because each object is one subrequest in a composite batch. One cost: the scan is per object today so that its cost doesn't grow with the number of users, and a long candidate list would need splitting. Bulk couldn't help here, because it has no GROUP BY.
4. **PK chunking.** That's a Bulk API 1.0 header (`Sforce-Enable-PKChunking`) and Bulk 2.0 does it automatically, so it brings the same job-creation boundary. Over REST, our keyset reads already split by Id.
5. **Accept the caps and state them.** This is what happens today. It's honest, but a large customer could get several not measurable results for the same reason.

## Cost

| Option | Work | Boundary change |
| --- | --- | --- |
| Alternatives 1 to 3 | About a day, with tests and fixtures | None |
| Bulk API 2.0 query jobs | Two to three days. That covers the allowlist and its tests, the client method, CSV parsing, polling with resume from a checkpoint, the live test on `bc-hc-dev`, a DECISIONS.md row, and a line in the install guide | Yes: one POST route that creates a record in the customer's org |

**Suggested order:** build alternatives 1 to 3. Record in each run which caps fired, and how many calls each read used. Revisit Bulk if a real org runs out of budget before its reads are complete.
