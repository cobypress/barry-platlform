# Night 3: review of session C's engine work (Brief AN)

An independent review of barry-platlform health-check `8b900ce..b9c28a2`, leaving out the interviews, action-docs and drive-share merges, which I wrote. Branch `hc/review-engine` from `b9c28a2`.

In scope:
- integrations and technical health (987b683);
- Salesforce's own users' names (946abb6);
- action lists (434de87);
- the roadmap 3.2 checks (408565b);
- CPQ (902cf79);
- AI readiness (0f15d3b);
- Account Engagement (80d3bc9);
- the deck range (92bba54, af6888b);
- the facts step (42b960b).

How I worked:
- Four read-only reviews ran in parallel, one per group of merges.
- I confirm every candidate myself with a test that fails on `b9c28a2` before fixing it, each fix in its own commit.
- Anything I can't confirm is listed as plausible, with the reason.

Baseline at `b9c28a2`: typecheck clean, 1,992 of 1,992 tests pass.

## Log

- Rule 6 at the transport. `src/salesforce/allowlist.ts` is unchanged in the range. `client.ts` gained three getters (`limitsAtConnect`, `limitNames` and `instanceUrl`) and keeps what the first `/limits` read returned. None sends anything, and `CLIENT_METHODS` lists them, so the surface test still pins the client. Clean.
- Rule 7 and 946abb6. The exemption for Salesforce's own users' names is an exact, whole-phrase match on six system-user names, and it applies to whole names only: usernames, emails and ids are still scrubbed, and tokens are unchanged. I found no way for a real person's name to get through `scrub()` or `guardPrompt`. Clean.

## Confirmed and fixed

Each was confirmed by a test that fails on `b9c28a2`, then fixed in its own commit. The behaviour changes are decisions 301 to 310.

| # | Commit | What was wrong | Rule | Decision |
|---|---|---|---|---|
| 1 | 4faa1d8 | AI readiness's legacy share counted Workflow Rules it couldn't read (the Tooling route) and Apex triggers it couldn't read as none. The share moved from 30.8% to 10% or 50%, and the shape pillar with it. Now not measurable, with the reason. | 5 | 301 |
| 2 | 9aa0e51 | Record-triggered, scheduled and platform event flows (all `AutoLaunchedFlow`) counted as actions an agent could call: 4 on the synthetic org, really 1. The test asserted the overcount. | wrong result | 302 |
| 3 | 501528e | An API-only account on a full seat that's also dormant or never used was counted twice in licence waste, once to release and once to move. `int.integration-user-licences` joins the `unused-seats` group. | 5 | 303 |
| 4 | 268dc54 | `mkt.unworked-handovers` passed on the overall share while claiming lead source as a checked segment. 120 well-worked leads from one source hid a source nobody worked. Now segmented with `segmentRate`. | 4 | 304 |
| 5 | 3f58128 | `int.failed-async-jobs` stated "3 of 7" as the week's totals from a cut-off job log, read in no order. Now "At least N", with an exclusion. | 5 | 305 |
| 6 | 0da93ca | The action lists' front sheet and tickets ignored approved number overrides, so a ticket kept "21 of 76" where the decks said the corrected "19 of 76". | R17 | 306 |
| 7 | 3e0baeb | `cpq.discount-approvals` counted a deep draft quote on a deal still open as "never went through approval", and listed it to chase. It now counts quotes on won deals only. | wrong result | 307 |
| 8 | f464c62 | `mkt.unowned-prospects` counted a lead owned by a partner user, or anyone outside the Standard users read, as "owned by someone who has left". | wrong result | 308 |
| 9 | fd6d677 | The Apex scan didn't recognise `@isTest` beside another annotation, such as `@SuppressWarnings`. A data factory became a non-test class with an assertion-less test method, which counted against test quality and the code-size limit. | wrong result | 309 |
| 10 | ce5893c | An action list read as partial whenever it was shorter than an earlier count, even if it was complete ("the first 117 of 120"). The contact list, two id-ordered lists, read as one "first 5,300 of 12,300". It is now partial only when the read hit its ceiling, and contacts say it gap by gap. | wrong result | 310 |
| 11 | b962dd7 | AI readiness's actions reason said "too few have one" with every flow described, when a finding caused the "nearly". The shape reason blamed a share inside the working line when a serious finding caused it. Wording only; the verdicts are unchanged, so no decision row. | wrong text | none |

## Tests that passed without testing the thing: now they test it

Each new test was checked by mutation: with the code it guards removed, the test fails.

- **cd65200.** Without consent, a person's token in a finding's owner, effort note or fix, or in a list's fix or problems, becomes a role in the workbook and the CSV. Removing `teamLevelText` from the files' wording passed every test before. Now it fails with "tickets names Alex Morgan".
- **7b5d3f6.** Test classes never count for `tech.governor-limit-risk` or `int.hardcoded-endpoints`. No synthetic test class had a loop query or a literal address, so dropping the exclusion passed.
- **a857929.** Clean twins, a test that the rule stays quiet on clean data, for `sec.unused-access`, `utl.package-seats`, `dq.opps-without-account`, `cpq.inactive-rules` and `cpq.quote-to-order`. All five pass.
- **New pass twins.** The fixes above added twins for `cpq.discount-approvals` (a draft on an open deal) and `mkt.unworked-handovers` (by source).

## Confirmed, not fixed: needs a decision

- **CPQ discount leakage goes in the "At risk" pot.** `cpq.discount-leakage` is in neither `RULE_GROUPS` nor `EXPOSURE_RULES`, and its value kind is revenue, so the fallback makes it exposure in a group of its own. It's shown under "At risk" beside `cur.coverage` and never reaches the headline total. On the synthetic org it's 417,736 a year.
  - Money already given away on won deals isn't pipeline at risk.
  - Moving it to a headline pot needs Coby's call on which one. It also needs a check against `pipe.segment-divergence` (the same teams, the same window), or the same money would count twice.
- **Two integration checks disagree about one account.** An API-only account that isn't calibrated, with a two-word name and five or more API logins, is both a "personal account" in `sec.personal-integration-accounts` and an integration account in `int.integration-user-licences`. The first should read the API-only set the second uses. That changes behaviour, and the decision rows are used.
- **The Essentials deck cap doesn't follow its own rule.** It is 81, but 66 + 3 × 6 = 84. Decision 286 writes down 81, so it's a recorded choice, not a bug. If "the caps rise to match" means 84, it's a one-line change.

## Plausible, not confirmed

Each has the reason it isn't confirmed.

**CPQ**
- Approvals are read with a 50,000-row cap and `truncated` is never checked. Missing approvals would read as unapproved. Reasoning only: the synthetic org has few.
- Advanced Approvals is detected only by its `sbaa` licence row. Where it has none, or the licence read failed, its approvals would read as missing. A describe of `sbaa__Approval__c` would settle it. Not checked against a real org.
- Quotes are read by `CreatedDate >=` the window start. A deal that closed early in the window with an older quote drops out of leakage and quote-to-order. Reasoning only; the synthetic org has none.
- `cpq.renewals-not-started` is high severity and high confidence, but a renewal opportunity made by hand and not linked through `SBQQ__RenewalOpportunity__c` reads as missing. `Status = 'Activated'` is a renamable label, where `StatusCode` would be stable. This is a Salesforce fact to verify, so it belongs in QUESTIONS.md.

**Integrations**
- `int.integration-user-licences` treats a login with no `ApiType` as a browser login. The engine's own `channelOf` (decision 101) treats OtherApi and Remote Access OAuth logins as API too. The rule logic was confirmed with a script, but whether real orgs return null `ApiType` for OAuth integrations is an unverified Salesforce behaviour (QUESTIONS 11).
- `sec.personal-integration-accounts` states counts from a login log that may have been cut off, the same pattern as fix 5. Not reproduced.

**AI readiness and facts**
- The platform pillar reads "nearly" when Agentforce couldn't be read, which guesses at an unknown. It is arguably not measurable. Reasoning only.
- The facts line can say a check "says so" when that check passed without mentioning the gap, or doesn't read the missing object (e.g. NamedCredential). Reasoning only.

**Account Engagement**
- `mkt.scoring-grading` passes on overall shares with a nominal segment. Its counts are COUNT queries with no source to split by, so segmenting needs a new query.
- `mkt.unworked-handovers` takes its rate over every handed-over lead, including ones too new (under 14 days) or too old (over 90) to count as unworked, which dilutes it. The finding's wording is consistent with that base. Changing it is a design choice, so I've left it.

**Rule 3**
- `utl.package-seats` ("Cancel the apps nobody uses") and `tech.storage-use` ("Archive or delete…") are typed `change`. So are older rules with "remove" advice, such as `sec.modify-all-data` and several Service Cloud checks. The engine's settled reading is that guards apply to removing org objects (users, fields, flows). These follow that convention, and both carry a dependency to check first.

**Smaller**
- The access read takes every active user's assignments, community users included, uncapped and not resumable (rule 9).
- `dq.duplicate-accounts` groups names ignoring case but sizes them by exact name.
- `sec.peer-outlier-access` keeps privileged sets in when they can't be read, while its evidence says they're excluded.
- In the action lists, queue-owned or partner-owned records show "No role", and an issue with no list is called "A set-up change".
- An opportunity sheet past Excel's 66,530 hyperlinks would need a cap.
- The tickets' Records column shows rows, not the total, for a partial list.
- Without consent, a role held by one person still identifies them. This is inherent to decision 194; worth a line in QUESTIONS.md.

## Checked and clean

- **Rule 6.** No allowlist change. The new reads are describes, SOQL through `readSoql` or `readKeyset`, one more Settings retrieve on the existing route, and `/limits`, which was already read. There's no generic request method.
- **Rule 7.**
  - Opportunity, lead and contact names stay in the action-list directory, which no rule or prompt reads.
  - Reps appear as `ctx.userLabel` tokens, and `BotDefinition` names never leave the extract.
  - The consent default is no names. Without consent, a whole-word scan of the xlsx and CSV for every synthetic user's names finds nothing.
- **Value groups.** Besides fix 3, the unworked-leads and stalled-deals groups hold their new members, and `mkt.unowned-prospects` isn't sized.
- **Discount maths.** Discount is (list − net) / list. A null, zero or negative list gives null. Only primary quotes count. Each deal is counted once, at its largest excess. A multi-currency org isn't sized. A sample of 20 is needed on both sides.
- **Storage, packages, permissions.**
  - Storage is in MB, as `/limits` reports it. A Max of 0 is skipped.
  - Site licences, free, trial and expired apps are left out.
  - Holders through a permission set group are counted.
- **SOQL windows.** `AsyncApexJob` uses `CreatedDate >= now - 7d`, in correct ms. Keyset reads use a strict `Id >` with `ORDER BY Id`. The list filters match the count filters.
- **Action files.**
  - xlsx: inline strings only, no formulas, escaped XML.
  - CSV: RFC 4180, CRLF, an apostrophe before a leading = + - @ tab or CR, and priority as decision 195 sets it.
  - The release gate holds, and `action-lists.json` is 0600, outside `checkpoints/`.
- **Deck range.** `deckRangeFor` drives both the density ladder and the layout check, so they can't disagree.
- **Unreadable sources.** Every new rule handles `status !== "ok"`.

At the end: typecheck clean, 2,014 of 2,014 tests pass (from 1,992).
