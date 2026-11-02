# Overnight build plan

Three Claude Code workers build in separate git worktrees while Coby sleeps, and a coordinator reviews and integrates. Nothing is pushed or deployed. Coby reviews everything in the morning.

## Trees and branches

| Worker | Worktree | Branch | Base | Builds |
| --- | --- | --- | --- | --- |
| A: Domains | `Black Cloud/worktrees/hc-domains` | `hc/domains` | barry-platlform `health-check` | Remaining domains, scoring, value sizing |
| B: Reports | `Black Cloud/worktrees/hc-reports` | `hc/reports` | barry-platlform `health-check` | Narrative with number check, Word and PDF output |
| C: Console | `Black Cloud/worktrees/hc-console` | `hc/console` | blackcloud-next `health-check` | Console pages on fixtures, Playwright tests, schema and SQL files |
| Coordinator | `Black Cloud/worktrees/hc-integration` | `hc/integration` | barry-platlform `health-check` | Reviews, merges A and B, reports |

## Rules for every worker

These add to `CLAUDE.md`, and they override anything else.

1. **Stay on your branch and in your tree.** Commit there, in small steps, with a message like `new - health check - forecast domain`. Never touch another tree.
2. **Never push, deploy or open a PR.** Never run `git push`, `vercel` or anything that reaches production.
3. **Never touch Salesforce.** Don't run `sf` or `sfdx`. This machine is logged in to customer orgs. Use the synthetic org only.
4. **Never open, copy or print a `.env` file.** `blackcloud-next/.env.local` points at the production database. Your tree has none, so keep it that way. Don't connect to any database or Redis.
5. **No real Claude calls.** There's no key, and tests must not need one. Use a stub client.
6. **No customer data.** The folder `Desktop/Clients Orgs` is off limits, and so is the plan document in `blackcloud-next/docs/health-check`. Everything you need is in the sections below.
7. **Don't change existing behaviour** of Barry, proposals, funnels, Slack or existing `/internal` pages.
8. **Tests stay green.** Run the tests and the type check before every commit. Never skip, weaken or delete a test to get a pass. If a test finds a real bug outside your area, record it and move on.
9. **Don't guess about Salesforce.** If a rule rests on a fact you can't verify from the Salesforce docs, say so in `docs/overnight/<worker>.md`, and lower the finding's confidence or return not measurable.
10. **If you're stuck for 30 minutes, write down why and move to the next item.** Never wait for an answer: nobody is awake.
11. **Write your log as you go** in `docs/overnight/<worker>.md`: what's done, what's left, decisions, and what you weren't sure of. Keep it current, because it's how the coordinator follows you.

## Frozen interfaces

Workers A and B share `barry-platlform`. To keep merges clean:

- `src/schemas/*`, `src/salesforce/*`, `src/engine/evaluate.ts`, `src/engine/run.ts` and `src/engine/rule.ts` are frozen. If you need a change, make the smallest additive one, in its own commit, and record it in your log. Never rename or remove anything.
- Worker A owns `src/domains/*`, `src/extract/*`, `src/guards/*`, `src/testing/*` and `fixtures/*`.
- Worker B owns new folders `src/narrative/`, `src/render/`, `src/numbercheck/` and `src/design/`.
- Worker B reads findings through the `Finding` and `Metric` schemas only, so it doesn't depend on which domains exist yet.

## Worker A: Domains (Milestone 3, engine side)

Add one domain at a time, in this order, each as a `DomainModule` with extractors, metrics, rules, synthetic traps and tests. Each domain is a commit.

1. **Security and access:** SecurityHealthCheck and its risks through Tooling, profiles and permission sets with Modify All Data, personal accounts used for integrations, sharing defaults. Secrets stay out.
2. **Best practice:** versioned rules across existing extracts, each stating Salesforce's position.
3. **Current pipeline:** age in stage, stale deals, close date pushes, missing next steps, concentration, coverage where targets exist. Only report text fields as filled or empty.
4. **Forecast:** ForecastingItem, ForecastingQuota, ForecastingFact. Where Forecasting isn't in use, report not measurable, and never estimate a score.
5. **Activity:** Task, Event, EmailMessage, per deal and per rep, won against lost. Segment unlinked events: a real check once missed a collapse in them because the total looked normal, so build this trap first.
6. **Lead funnel:** Lead, LeadHistory, Campaign, CampaignMember. Conversion, response time, source return.
7. **Revenue opportunities:** stalled deals, win-backs, dormant accounts. Every figure carries its assumptions.
8. **Scoring:** a 0 to 100 `Score` per domain from weighted findings. A domain with no measurable findings is not measurable, never zero.
9. **Value sizing:** revenue, licence cost or hours, with assumptions shown, or not sized with the reason.
10. **Report scanning:** read report definitions so the field guard can resolve. Each new read needs a line in DECISIONS.md.

Also fix these known gaps if time allows: use the org's time zone for day and month boundaries, and refresh a session on a SOAP `INVALID_SESSION_ID`.

## Worker B: Reports (Milestones 3 and 4)

1. **Number check:** extract every figure from generated text, and fail anything not traceable to the findings dataset. Handle percentages, currency, rounding, words such as "half" and "three", dates, and ids. Err on the side of blocking. Test it adversarially, because it's the last line of defence against an invented figure.
2. **Narrative:** drafts section text from findings and metrics through an injected client. Uses `MODELS` in `config.ts`. See the Claude API notes in `docs/ARCHITECTURE.md`, because several older patterns now return a 400. The input is tokens only: add a test that a real name can't reach the prompt. Generated text passes the number check or it's rejected.
3. **Section builder:** the three outputs from one findings dataset, as `ReportSection` and `SectionVersion` records.
4. **Design tokens:** one file, `src/design/tokens.ts`. Poppins, Chartreuse #A8E503, Carbon #21221C, Ivory #FCFFF3, Snow #F7F7F7, red #F3261C for alerts only. No gradients, no emoji, mild rounding, flat fills.
5. **Charts:** server-rendered SVG with no browser. Win rate by segment, stage funnel, pipeline over time, scorecard.
6. **Raw notes in Word:** `docx` library. Intake summary, assumptions, every finding with evidence, queries, exclusions and limits. Overrides appear with their reason.
7. **Executive summary PDF:** about 6 pages through Playwright: verdict, scorecard, top five issues with value, 90-day plan.
8. **Full health check PDF:** 16:9, 40 to 60 slides: stated problems, every domain, pipeline deep dive, stated vs actual, roadmap, appendix.
9. **Layout checks:** every page tested for overflow, missing charts and empty sections.

Render from the synthetic run, and keep sample output out of git. Names are restored only at render time. Word and PDF output goes in the same run folder, outside the repo.

## Worker C: Console (Milestone 5, front end)

The console is built against an in-memory fixture repository, because no database is available tonight. The engine's Zod schemas are copied to `docs/health-check/engine-schemas`, and the Prisma models and pages must match them. Read `app/internal/proposals`, `app/internal/scrape`, `lib/internal-auth.ts`, `lib/scrape/auth.ts` and `middleware.ts` first, and match their look and patterns.

1. **Google sign-in** for `/internal/health-check`, reusing the scrape approach. Add a matching branch in `middleware.ts` for this path only. Every server action and API route checks the session itself, and a test proves each one refuses without it.
2. **A repository interface** with a fixture implementation holding synthetic data, so swapping in Prisma later changes one file.
3. **Pages:** run list, intake, notes upload, calibration, manual steps, review console, release.
4. **Review console:** each section's draft, evidence, status and a Claude panel with a stubbed response. Wording proposals show before and after. Accept and reject record who and when. Numbers can't be edited, except through an override with a reason.
5. **Release:** blocked until every section is approved. Name and time recorded.
6. **Data model, written not applied:** `prisma/schema.prisma` additions and `migrations/20261103_health_check.sql`, matching the Zod schemas. Don't run them, and don't run `prisma migrate` or `prisma db push`. `prisma generate` is fine.
7. **Queue helper:** a `health-check-jobs` helper in the shape of `lib/funnels/sync.ts`, carrying only ids, with no worker connected.
8. **UI tests:** Playwright tests for each flow, at desktop and phone widths, against a local dev server on port 3100, with no `.env` file. Include a test that an unauthenticated user can't reach any page or action.

If a page needs an environment variable to start, add a safe local default for tests and list it in your log. Never copy `.env.local`.

## Coordinator

Works in `worktrees/hc-integration`, and wakes about every 30 minutes.

1. Read each worker's log and commits.
2. Review new commits against the rules above, especially anything touching `allowlist.ts`, guards or `Finding`.
3. Merge `hc/domains` and `hc/reports` into `hc/integration` when their tests pass, and run the full suite there. Never merge into `health-check` or `main`.
4. If a merge conflicts or a test fails, fix it on `hc/integration`, or write the problem in `docs/overnight/coordinator.md` for the worker.
5. Re-run the mutation test against safety controls after each large merge.
6. When workers have finished, write `docs/overnight/MORNING.md`: what was built, real test results, what's risky, what wasn't finished, and what Coby needs to decide. Then stop.

## Morning hand-over

Nothing lands on `health-check` until Coby reviews it. `hc/integration` and `hc/console` wait for him. To throw the night away, delete the branches and worktrees. Nothing else was changed.
