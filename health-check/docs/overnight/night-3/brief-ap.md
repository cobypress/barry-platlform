# Night 3, Brief AP (round two): Worker C

Branches: engine `hc/night-3-b` from `health-check` 74ee598; console `hc/facts-view` from `health-check` ca2251b. Nothing is pushed or merged.

## 1. Engine decisions (DECISIONS.md 322 and 323)

**Discount leakage gets its own value group (322).**
- `cpq.discount-leakage` is in group `discount-leakage`:
  - kind: revenue recoverable
  - label: "Potential: discount given beyond what peers gave"
  - realisation: the long-shot one, 10%, 25% and 50%, as the segment gaps use
- Before, it had no group, so it fell to exposure and never reached the headline.

**Do the two pots share any synthetic deal? None does.**
- I checked this in code, on the recording, with the CPQ modules on.
- Leakage flags one rep, U0001, who sells in **Sales UK**.
- The segment gaps are the Sales EU team and two reps there (U0014 and U0003). Their base is 88 won and lost deals closed in the window.
- None of U0001's 79 won deals in the window is among those 88.
- One correction to the brief's reasoning: the segment gap's base is **not** only deals that weren't won.
  - `divergenceValue` sizes the gap on the amount of won *and* lost deals in the segment.
  - On a real org, a rep flagged for discounting who sits in a team flagged for winning less would put the same won deal in both bases.
  - The money still differs: one is wins not made, the other is margin on wins made. Decision 322 says this.

**One reading of who is API only (323).**
- `apiOnlyUsers` reads the API-only extract once a run, cached by the extract's data object.
- Three checks use it:
  - `sec.personal-integration-accounts`
  - `int.integration-user-licences`
  - `int.integration-users-interactive`
- An API-only account is an integration account, never a person's.
- The wording, as `API_ONLY_ACCOUNT`: "set to API only (API Only User on its profile or a permission set)".
- The evidence note for the read: "Profiles and permission sets with API Only User".
- Before, the personal-accounts check could name, as a person running an integration on their own login, the same account that the licence check counted as an integration account.

## 2. Facts in the console (engine DECISIONS.md 324)

**Engine.**
- `factsReport` (`facts/summary.ts`) is built from the run folder's facts.json. It holds:
  - `checkedAt`
  - counts of confirmed, contradicted, open and not checked
  - for each contradicted fact: its id, its QUESTIONS.md id (`question`, such as "integrations.i"), and the rule ids resting on it
- It holds names and counts only.
- The worker sends it in `RunOutputRecord.facts`, scans included, before a scan's folder is deleted.
- The Postgres store writes `HealthCheckRunFacts` in the results' transaction, DELETE then INSERT, inside a savepoint.
  - On 42P01 (no table yet), it rolls back to the savepoint and the results still save.
  - Any other error rolls the whole run back, as before.

**Console.**
- **Migration: `migrations/20261120_health_check_run_facts.sql`.** Additive, one new table, written and not applied:
  - `"runId"` is the primary key, with a foreign key to the run, ON DELETE CASCADE
  - `"checkedAt"` TIMESTAMP(3)
  - `"facts"` JSONB
- Prisma model `HealthCheckRunFacts`. The zod schema in `lib/health-check/run-facts.ts` accepts only names (`/^[a-z0-9][a-z0-9._-]*$/`) and a list matching the count.
- `getRunFacts` and `putRunFacts` are in the fixture and Postgres repositories, covered by the contract test. An expired scan's facts go with it.
- The run page, which is for consultants only (`requireConsultantPage`), shows a card "How this org is built":
  - "We checked N things about how this org is built; M differed."
  - the open and not-checked counts
  - each differing fact by its QUESTIONS id, with the checks resting on it, named by their findings' titles, or by rule id when the run has no finding for that check
- A scan's run page, reached from the Scans page, shows the same card. The prospect's results page doesn't.
- Development data: run-0001 and the dev scans have made-up facts, with one differing fact (Scontrol).

## 3. "Plausible" items (engine DECISIONS.md 325)

**Confirmed with a failing test, then fixed, in order of how much they could mislead:**
1. `cpq.discount-approvals` called quotes unapproved when the approvals read had stopped at its 50,000-row cap. It's now not measurable in that case (version 3).
2. `sec.personal-integration-accounts` stated counts from a cut-off login log as totals. They're now "at least" (version 4).
3. `sec.peer-outlier-access` evidence said the Modify and View All Data sets were left out when they couldn't be read. It now says they were compared like any other (version 2).
4. The tickets' Records column showed the rows on the sheet (2) for a part list, not the total (7,210).

**Ruled out:**
- The facts line's "the check that rests on it says so". Every check resting on a contradicted fact is not measurable or states the gap. The facts test now also asserts this for `utl.dormant-licensed-users`.
- Excel's hyperlink limit. Each sheet holds one list, capped at `ACTION_ROWS` (5,000).

**Left, with the reason:**
- *Duplicate accounts grouped ignoring case but sized by exact name.* Whether SOQL's `GROUP BY` and `IN` ignore case is a Salesforce fact. A case-folded lookup would be wrong if they don't, so I haven't guessed. This belongs in QUESTIONS.md.
- *Advanced Approvals detected only by its licence row.* It needs a describe of `sbaa__Approval__c` on a real org.
- *Renewals keyed on `Status = 'Activated'` and `SBQQ__RenewalOpportunity__c`.* These are Salesforce facts. They belong in QUESTIONS.md.
- *`int.integration-users-interactive` treats a null `ApiType` as a browser login.* This is unverified Salesforce behaviour (QUESTIONS 11).
- *Quotes read by `CreatedDate >=` the window start.* This understates rather than overstates, and fixing it changes the SOQL and the simulator.
- *The AI platform pillar reads "nearly" when Agentforce couldn't be read.* Its reason says so, and "nearly" is the lower of the two possible verdicts, so it never claims more than we know. Not measurable would also change the overall verdict. That's a design choice.
- *`mkt.scoring-grading`'s nominal segment.* Segmenting it needs a new query.
- *`mkt.unworked-handovers`'s base.* A design choice, and its wording matches its base.
- *Rule 3 typing for package seats and storage.* This follows the engine's convention.
- *The uncapped access read.* A cost (rule 9), not a statement to the customer.
- *"No role" for queue-owned records, and "A set-up change".* Wording only.
- *A sole role-holder being identifiable without consent.* This is inherent to decision 194, and is a QUESTIONS.md line.

## Checks

- **Engine:**
  - typecheck clean
  - 2,024 of 2,024 tests pass (from 2,015)
  - each new test fails without its fix
- **Console:**
  - typecheck clean
  - 1,274 of 1,274 unit tests pass
  - lint clean on the changed files
- **Playwright:** port 3100 was free. runs.spec and scan.spec pass on desktop and phone: 66 of 66.
- No database, Redis, sf, .env or Claude call was used.
