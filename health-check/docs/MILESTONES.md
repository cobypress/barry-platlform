# Milestones

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
| `npm test` | See the end of this section for the latest count. No org needed |
| `npm run typecheck` | Clean |
| Mutation test: 34 deliberate breakages of safety controls and maths | All 34 caught by the tests |
| Real `hc` command over HTTPS to a local simulator | Finished, with GET and allowed POST requests only, and no token in any output |
| Pipeline metrics | Checked against a small data set worked out by hand |
| Independent code review | See "Review fixes" below |

### What hasn't been checked

**`hc` has never run against a real org.** The alias wasn't given, so that step was skipped as the brief asked. The fixtures are built from Salesforce's documented formats and the prototype's queries, not from recordings of a real org. Expect some queries to need adjusting on first contact. QUESTIONS.md lists the most likely ones.

### What's risky

| Risk | Detail | Response |
| --- | --- | --- |
| Real responses differ from fixtures | Every Salesforce shape here is from docs, or the prototype's use of it | First live run on the Black Cloud org, before any more domains rely on them |
| A rule is confidently wrong | Thresholds are our judgement. Only a real org shows false positives | Review the first live run finding by finding |
| Large orgs | REST paging only, and `LoginHistory` is capped at 20,000 rows. Ownership scans top out at 40 custom objects, which then blocks removal | Bulk API decision in Milestone 3 |
| Chat scrubbing misses first names | `scrub()` catches full names, usernames, emails and ids. "Sam said..." gets through | Needs solving before Milestone 5 sends chat to Claude |
| Time zones | Month ends and day boundaries are UTC, which can move a deal across a month end | Use the org's time zone in Milestone 3 |
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
