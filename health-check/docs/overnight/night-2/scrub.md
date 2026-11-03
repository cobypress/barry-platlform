# Lane B: scrubbing first names, night 2

Branch `hc/scrub`, tree `worktrees/hc-scrub`. Synthetic data only. No Salesforce, database, Redis or Claude contact. No new environment variables. No SQL files.

## Done

- `src/privacy/names.ts`: `maskNames()` replaces parts of every known name; `findNames()` is the independent last check; `registerSharedNames()` labels tokens for shared first names.
- `src/narrative/guard.ts`: `guardPrompt()` now uses `maskNames()` in place of the old capitalised-part masking. New `sendPrompt()`, the only caller of `client.complete()`, guards the prompt, runs `promptProblems()` and throws `PromptRefused`. Only our constant system prompts may be sent. `holdsNameOrSecret()` checks Claude's replies.
- `src/narrative/draft.ts`: sends through `sendPrompt()`; a refusal becomes a rejection with its reasons, so `applyNarrative` records it and the section goes to a person.
- `src/engine/run.ts`: calls `registerSharedNames()` once users and the customer are labelled, so shared tokens are saved in `pseudonym-map.json`.
- Tests: `src/privacy/__tests__/names.test.ts` (adversarial cases plus the golden runner), `src/narrative/__tests__/routes.test.ts` (source scan, end to end, refusal).
- Golden file: `fixtures/scrub-golden.json`, 30 cases, beside `numbercheck-golden.json`. Not copied into blackcloud-next (not my tree).
- Docs: ARCHITECTURE.md ("Free text and names", rule 7, data handling), MILESTONES.md risk row, DECISIONS.md 68.

Tests: 1262 passing (1184 before), typecheck clean. Mutation check: with `maskNames()` removed from `guardPrompt()`, the narrative and routes suites fail.

## Routes by which free text reaches Claude

Everything ends in `draftSection()` and now `sendPrompt()`. `buildPrompt()` is called only from `draft.ts`.

| # | Text | Who writes it | Path | Sent today? |
| --- | --- | --- | --- | --- |
| 1 | Metadata labels: flow, report, dashboard, field, role, profile, record type and stage names | Admins in the org | Rule output, then `Finding` title, summary, subjects and facts, then `findingSource`, then the brief | Yes |
| 2 | Picklist values and segment keys (lead source, type, team or role names) | Admins | `Metric` segments, then `metricSource` | Yes |
| 3 | Manual-step `skipReason` | Consultant | `ctx.manual()`, then `notMeasurableReason` in utilisation rules, then `findingSource`. The routes test proves a first name survives `scrubDeep` into the finding | Yes |
| 4 | Manual-step `values.username`, `pastedText`, `answeredBy` | Consultant | Read by guards or kept in evidence; evidence isn't sent | No |
| 5 | Claude's own reply | Claude | Problems quote figures from it, then `feedback`, then the second prompt | Yes, guarded |
| 6 | Section title and purpose, including a plug-in's domain id | Our code | `SectionBrief` | Yes, guarded |
| 7 | Intake: summary, stated problems, assessments, `raisedBy`, `capturedBy` | Consultant from the customer | Raw-notes and full-report sections with no brief, and the number-check dataset | No. The routes test plants names here and checks none reach a prompt |
| 8 | Override reasons and `overriddenBy`; calibration `confirmedBy` | Consultant | Raw-notes sections with no brief | No |
| 9 | Consultant chat, call notes the customer shares | Consultant | Not built (Milestone 5) | Must use `sendPrompt` |
| 10 | Manual-step screenshots read by Claude | Customer admin | Not built (`ManualPrefill` schema only) | Images can't be text-scrubbed. Flag for Milestone 5 |

The routes test fails if any file other than `guard.ts` calls `.complete(`, or anything other than `client.ts` touches the SDK. **Coordinator:** lane C's Claude client and job runner will trip this when merged if they call `complete()` directly. That's intended: route them through `sendPrompt()`, or add the file to the test only with a guard of its own.

## Who the worker knows

`pseudonym-map.json` holds org users (`Name`, `Username`, `Email`, from `usersExtractor`), "Unknown user" for ids not in the directory, the customer's company name, and accounts labelled by id only (their names aren't read). No contacts or leads are read, so their names aren't known and get through. `PERSON_KINDS` already includes `contact` and `lead` for when they are.

## Decisions

- **Where:** at the edge (`guardPrompt`), not in `scrub()`. Findings keep metadata labels as the org has them, so raw notes don't turn "Price Book" into "Will Price Book".
- **Shared first name:** its own user token, id `shared-name:<key>`, restoring to the name as written ("Sam"). Either person's token would misname someone; a generic token would lose that a person was meant. Being a user token, executive text still says "a single person". Also applies when one person's first name is another's surname.
- **Exact beats derived:** a part someone actually has outranks a nickname or mailbox name derived for someone else, so "Samuel" is Samuel Okafor even when Sam Reed exists.
- **Common words:** 214 names that are also words are masked only with a capital. Lower-case "will", "may", "price" survive; "Will", "May" (also the month), "Price Book" and a sentence-start "Will the..." are masked. We prefer over-scrubbing. Two letters or fewer ("Jo", "Al") follow the same rule. Inside a hyphenated word such a part is left ("Cross-sell"); an uncommon part isn't ("Reed-approved").
- **Company names** need a capital, as before; generic words (Ltd, Holdings, Group) are no longer masked alone.
- **Nicknames:** a short table of about 60 common English diminutives, both ways, first name only. Cheap, partial.
- **Last check:** `findNames()` looks at every run of letters on its own, with the same vocabulary but different matching, so a gap in one shows in the other. It reports tokens, never the text.

## Not covered

Nicknames outside the table, initials, misspellings, camel case (`SamReedApproval`), lookalike letters from other alphabets, people the run doesn't know, and images. Full names were already matched in any case by `scrub()`, so "you will price it" loses "will price" when Will Price exists. That's existing behaviour; the golden file records it.

## Doubts

- Over-scrubbing can confuse Claude (a JSON key or a product name turned into a token). It's safe, but expect some odd drafts in orgs with users named Price, Case or Close.
- DECISIONS.md row 68 may collide with another lane's number.
- `findNames()` and `maskNames()` share `fold()` and the vocabulary. A bug there would affect both.
