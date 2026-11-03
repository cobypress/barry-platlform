# Second overnight build, 3 to 4 November

Four workers build in separate worktrees, and a coordinator (black-cloud-48) reviews and integrates. Nothing is pushed or deployed. Coby reviews in the morning.

## Trees and branches

| Lane | Worktree (under `Black Cloud/worktrees`) | Branch | Base | Builds |
| --- | --- | --- | --- | --- |
| A: Read-only gap | `hc-readonly` | `hc/readonly` | barry-platlform `health-check` | Read automation and report definitions without the Metadata API |
| B: Scrubbing | `hc-scrub` | `hc/scrub` | barry-platlform `health-check` | First names in chat and notes text |
| C: Wiring | `hc-worker` and `hc-wiring` | `hc/worker`, `hc/wiring` | barry-platlform and blackcloud-next `health-check` | Job runner, console run start, connect link pages, Claude client |
| D: Docs | `hc-docs` | `hc/docs` | barry-platlform `health-check` | Customer install guide, Bulk API options note |

## Rules for every worker

These add to `health-check/CLAUDE.md` and override anything else.

1. **Stay on your branch and in your tree.** Commit there in small steps, with a message like `new - health check - read flows through Tooling`. Never touch another tree, `health-check` or `main`.
2. **Never push, deploy or open a PR.** No `git push`, `vercel`, or anything that reaches production.
3. **Salesforce:** only lane A may contact Salesforce, and only as set out in its brief. Everyone else uses synthetic data. This machine is logged in to customer orgs.
4. **Never use the `bc-hc-scratch-web` connection.** Coby is part way through a live rotation test on it, and any use resets the clock. Don't run any `hc` command that names it, and don't read or change its file.
5. **No database or Redis connection of any kind.** Write SQL files if you need a schema change, never apply them, and list each one in your log. A column that reaches code without reaching the database takes the site down.
6. **Never open, copy or print a `.env` file or a secret.** List new environment variables in your log.
7. **No real Claude calls.** Tests use a stub client.
8. **No customer data.** `Desktop/Clients Orgs`, the `build_` files and anything from a customer org are off limits. Synthetic data only, ids zero-padded, emails at example.com.
9. **Don't change existing behaviour** of Barry, proposals, funnels, Slack or existing `/internal` pages.
10. **Tests stay green.** Run the tests and type check before every commit. Never skip, weaken or delete a test to get a pass.
11. **Don't guess about Salesforce.** Verify against Salesforce's docs or, in lane A, the dev org. Record anything unverified in `docs/QUESTIONS.md`.
12. **Stuck for 30 minutes: write down why and move on.** Nobody is awake to answer.
13. **Keep a log** in `docs/overnight/night-2/<lane>.md` in your tree: done, left, decisions, doubts. Write it as you go, in plain UK English.
14. **Read-only stays read-only.** `src/salesforce/allowlist.ts` only widens with a test for the addition and a row in `docs/DECISIONS.md`. No write operation, ever.

## Coordinator

Reviews each lane as it finishes, merges the engine lanes into `hc/night-2` (from `health-check`), runs the full suite there, and writes `docs/overnight/night-2/MORNING.md`. Never merges into `health-check` or `main`.
