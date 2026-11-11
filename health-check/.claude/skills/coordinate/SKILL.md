---
name: coordinate
description: Start an orchestrator session for the Black Cloud health check (engine and console). Reads the method and the ledger, reconciles them with git, then plans, delegates, reviews and merges the next work until a wave of merges is done or half the context is used.
---

You are the orchestrator. Read `docs/orchestration.md` before anything else: it's the method, and it overrides habit.

The ledger folder is `~/Desktop/Black Cloud/orchestration/health-check` (call it LEDGER below).

## Start

1. **One orchestrator at a time.** If `LEDGER/orchestrator.lock` exists and the process it names is alive (`kill -0 <pid>`), stop at once and say so. Otherwise write the lock: your shell's parent process id, the time, and "coordinate".
2. **Stop if Coby is needed.** If `LEDGER/WAITING-FOR-COBY.md` exists, add anything new to it, remove the lock and stop.
3. **Read** `LEDGER/ledger.md`, the newest `LEDGER/morning-*.md`, the last 20 rows of `docs/lessons.md`, and `CLAUDE.md`.
4. **Reconcile with git; git wins.** For both repos, `git log --oneline -15 <working branch>`, `git worktree list` and `git branch --no-merged <working branch>`. Correct the ledger wherever it disagrees: tips, open branches, worktrees and what each waits on. Branches already merged into the working branch with a clean worktree can be removed (`git worktree remove`, `git branch -d`); note each in the morning report.
5. **Check Coby's side.** `git fetch -q` and compare `origin/health-check` (engine) and `origin/main` (console) with the ledger's "Waiting for Coby" list, and tick off what he has done.

## Work

6. **Pick from "Next"** in the ledger, in order, skipping what's parked. Decide everything except Coby's list in `docs/orchestration.md` ("Waiting for Coby"). Record each product or design decision in `docs/DECISIONS.md` with its reason.
7. **Plan, then brief.** For each lane (at most three at once): the plan with a "done when" checkable without judgement, then a self-contained builder brief. Use a scout for any code fact you'd otherwise state from memory. A decision's premise is a code fact too ("the report has no ceiling", "nothing else reads this"): have a scout confirm it with path:line before the row is written, because a builder builds exactly what the row says. A figure or a reason carried over from the ledger or an earlier report (a pixel count, a line count, "the microsite is team-level") is a premise as well: have it measured or looked up again on the current tip before it goes in a row, and where it can't be, write the row's "done when" so the builder measures first and reports the number. Where a document or file goes (the customer's Drive folder, the console, Claude, or nowhere beyond the worker's run folder) is a premise too: have a scout follow it to its upload or send, with path:line, before a row or brief calls anything internal. When a lane raises an extract step's version or changes its query, the brief asks what a run in flight reads when it resumes (the step runs again, at the run's original date): its window needs an end at that date, and the "done when" has a resume test. When a lane changes what a read takes in without raising its step's version, the brief also asks what a run in flight already holds from before the deploy (a reused checkpoint, a keyset's kept pages) beside the steps it reads after it, and the "done when" tests that mixed state; if the answer is wrong figures, raise the version. A limit a decision row admits ("a deal closed since shows as…") is a planted case in the figure test, with what the customer then reads asserted: a fixture that steers round it proves nothing. When a lane bounds, filters or otherwise changes what a read takes in, have a scout list every figure that sets that read against another one (a child read reached through its parent, or a second read of the same object), and put each pair in the brief: both reads change together, and the "done when" tests the figure, not only the query's text. What moves the fingerprint is a premise as well: before a brief says a file "must not move", have a scout say what that file quotes (metrics quote their source query, as findings do). Write every lane's decision rows in `docs/DECISIONS.md` yourself and commit them before the builders branch, and tell each builder not to edit that file: rows appended by parallel lanes conflict at merge. When two lanes meet at a contract (one repo writes what the other reads: a column, a file's shape, an error's text), put the contract in both briefs word for word, and have the writing side hold a test of the reading side's limits against everything it can really produce: a reader stricter than its writer fails silently in production.
8. **Run the loop** from `docs/orchestration.md` for each lane: build, gate (runner), review (adversary, then verifier, then the builder fixes what's CONFIRMED, then gate again), shots for anything a person sees, merge. Start agents in parallel where lanes are independent. Read the whole diff yourself for high-risk changes.
9. **Merge** into the repo's working branch from its main checkout. After resolving a conflict, run the touched suites and read the summary's `fail` count before `git commit`: never chain the commit after a grep, which succeeds on a failing summary too. Then continue as below (`barry-platlform` on `health-check`; the console's `health-check` lives in `worktrees/hc-console-merge`). Read git's output, run the full gate on the merge through a runner, and fix forward or revert if it fails. A console change that Coby should ship goes on a new `hc/release-N` from `origin/main` with `health-check` merged in, full gate run, and its push command listed for him.
10. **Never end with an agent still running** whose report you need. Wait for it.

## Get better

11. When something went wrong, nearly went wrong or was slow, tighten the rule, brief template or agent file that would have prevented it. Never loosen without Coby. Log a row in `docs/lessons.md` and commit that change alone as `playbook: <what>`.

## End

12. End after a wave of merges or at half your context window, whichever is first.
13. Update `LEDGER/ledger.md`: tips, open branches and worktrees, lanes and decision blocks in use, "Waiting for Coby" (each with the exact command or step for him), and "Next".
14. Append to `LEDGER/morning-<today>.md`: what was decided (with decision numbers), merged (hashes) and built, the gate results with counts, playbook changes, and "Still yours".
15. If nothing at all can move without Coby, write `LEDGER/WAITING-FOR-COBY.md`: what's blocked, his exact steps, and what you'll do once he's done.
16. Remove `LEDGER/orchestrator.lock`.
