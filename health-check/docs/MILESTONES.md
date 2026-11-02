# Milestones

Status after the overnight build: Milestones 3 and 4 are built on `hc/integration`, and the Milestone 5 front end is on `hc/console`, all on synthetic data. See `docs/overnight/MORNING.md`. Milestone 2 hasn't started. Figures in the Milestone 1 section are from when it finished.

## Milestone 1: done, not yet proven on a live org

### What's done

- Zod schemas for everything in the brief, plus the review console's proposal, override, release and baseline records.
- A read-only Salesforce client with a route allowlist, an API budget, resumable paging and a `ConnectionProvider` interface. There's a development provider for the Salesforce CLI, which only uses approved aliases.
- Extractors for configuration, automation, users and licences, and Opportunity with OpportunityHistory.
- 34 rules: 6 configuration, 8 automation, 8 utilisation, 9 data quality and 3 pipeline, with guards for removing users and fields.
- 12 pipeline metrics from OpportunityHistory, with stage meaning taken only from the calibration file.
- 2 manual steps: Einstein Activity Capture advanced sync settings, and the Default Workflow User.
- `hc run`, which writes `findings.json`, `metrics.json`, `manual-steps.json` and `raw-notes.md`.
- A synthetic org with planted traps, recordings captured through the real client, and replay that refuses unrecorded requests.

### How it was checked

| Check | Result |
| --- | --- |
| `npm test` | 421 tests, all passing. No org needed |
| `npm run typecheck` | Clean |
| Mutation test: 47 deliberate breakages of safety controls and maths | All 47 caught by the tests |
| Real `hc` command over HTTPS to a local simulator | Finished, with GET and allowed POST requests only, and no token in any output |
| Pipeline metrics | Checked against a small data set worked out by hand |
| Independent code review | Found 13 defects, 4 of them high. 12 fixed with tests, and 1 disclosed. See below |

### Review fixes

A separate agent reviewed the read-only boundary and the pipeline maths without seeing how they were built. It found no way for the code to send a write, but it found real gaps, and passing tests had missed all of them.

- **Token could go to the wrong host:** the instance URL was prefix-checked, so `https://x.salesforce.com@evil.example` passed. It's now parsed, on connect and refresh.
- **Old deals closed in the window were never extracted**, which inflated win rate and undercounted past pipeline.
- **Deals created as Closed Won counted as reaching every stage**, which inflated conversion. They also gave 0-day sales cycles.
- **The fiscal year defaulted to January** and was never compared with the org.
- **Smaller boundary gaps:** a repeated `q`, raw tabs, `LIMIT 1FOR VIEW`, a case-sensitive override check, `nextRecordsUrl` reaching any allowed route, and a parse error that could quote the token.
- **Smaller pipeline gaps:** history gaps were judged across all months rather than month by month, blank amounts were unchecked at month ends and in value segments, skipped-stage figures ignored the minimum sample, and same-second rows could reverse.

The one not fixed at the time was the UTC boundary. The org's time zone is now applied, and only the analysis window's edge is still set in UTC.

### What hasn't been checked

**`hc` has never run against a real org.** The alias wasn't given, so that step was skipped as the brief asked. The fixtures are built from Salesforce's documented formats and the prototype's queries, not from recordings of a real org. Expect some queries to need adjusting on first contact. QUESTIONS.md lists the most likely ones.

### What's risky

| Risk | Detail | Response |
| --- | --- | --- |
| Real responses differ from fixtures | Every Salesforce shape here is from docs, or the prototype's use of it | First live run on the Black Cloud org, before any more domains rely on them |
| A rule is confidently wrong | Thresholds are our judgement. Only a real org shows false positives | Review the first live run finding by finding |
| Large orgs | REST paging only, and `LoginHistory` is capped at 20,000 rows. Ownership scans top out at 40 custom objects, which then blocks removal | Bulk API decision in Milestone 3 |
| Chat scrubbing misses first names | `scrub()` catches full names, usernames, emails and ids. "Sam said..." gets through | Needs solving before Milestone 5 sends chat to Claude |
| Time zones | The org's time zone is applied, but the analysis window's edge is set in UTC in `run.ts`, so it can differ by a day | Small, and stated. Change it when `run.ts` is next open |
| One person can run and approve | Noted in the plan | Sign-off checklist in Milestone 5 |
| Customer names in the plan document | `blackcloud-next/docs/health-check/Salesforce Health Check Product Plan.md` names a real customer and its figures, and is untracked | Remove them before that file is committed |

## Milestone 2: proposed plan

Prove the connection, because it's the part most likely to change the design. It needs Coby's Salesforce setup, so it can't be done unattended.

1. Run `hc` read-only on the Black Cloud org, then fix what real responses show. Replace assumptions in QUESTIONS.md with facts.
2. Create an External Client App in a Developer Edition org, and try connecting it to a separate test org.
3. Establish whether a managed connector package is needed for customer orgs, and record the answer in DECISIONS.md.
4. Build the web server flow with PKCE and refresh token rotation as a second `ConnectionProvider`, with encrypted token storage and revocation.
5. Build the one-time connect link for customer admins.
6. Build the read-only permission set, and test that it really can't write.
7. Decide on the Bulk API for large orgs.

**Exit test:** a Black Cloud consultant connects a test org through the link, and the run completes on a token that can't write.
