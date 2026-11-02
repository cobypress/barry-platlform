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
| 8 | Does an expired session in a SOAP call need refreshing? | The client only refreshes after a REST 401, so a very long run could fail at retrieve |
