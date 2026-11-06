# Night 3: review of the reading passes (Brief AZ)

Worker A, 7 November 2026. This is an independent review of session B's three reading passes, which change how money is shown and ranked:

| Branch | Merge | Against |
|---|---|---|
| hc/reading-pass | `fc0b81a` | its first parent |
| hc/reading-pass-2 | `7b64bd4` | its first parent |
| hc/reading-pass-3 | `780f4d6` | its first parent |

B's unmerged hc/plan-quick-wins is left out.

Branch `hc/review-reading`, from health-check `70b95d8`. Decisions 391 to 393. Each finding I could reproduce got a failing test first, then a fix in its own commit.

## Confirmed and fixed

**1. The money table's last row had no amount** (decision 391, `fbb32a4`).
- With more lines than a format holds (7 on the full report's slide, 8 on the executive summary's page), the table said "And 2 smaller lines, in the raw notes" with no figure. So its rows didn't add up to the total beneath them.
- Every sample with CPQ has 8 lines, so it showed on each one.
- The row now carries the rest's amount and range, and those sums are in the number check's figures.
- The CPQ samples, alone and with all three clouds, render and pass every layout and number check.
- Test: `money-table.test.ts`.

**2. Issues the plan merges into one action lost their reference and their place** (392, `3cda682`).
- The plan merges issues from the same rule with the same action (`alsoFor`). Where the action fell after 90 days, the full report's list showed only the action's own reference, so the merged issues vanished from the deck. The 90-day pages and the microsite already showed them.
- The 90-day plan's and the roadmap's sections left merged issues out of their findings, wherever the action fell. The synthetic org's own merged issue, `f-84c2bbde4ed6`, was one.
- A list line may now carry several references, and both sections list merged issues.
- Test: `plan-merged.test.ts`, which also checks the synthetic org's own merge.

**3. The login log didn't cover a longer dormant period** (393, `71c4d25`).
- Decision 361 leaves out a user the log shows logging in inside the dormant period, but the log was always read for 90 days.
- With a calibrated period of 180, a login 120 days ago went unseen, and its user was counted dormant on the last-login date alone.
- The log now reaches back over the longer period. At the default of 90 nothing changes: the read and the period start at the same moment, and a login at that moment is "inside" on both sides.
- Test: `logins-window.test.ts`.
- One existing test built an extract context without a calibration, which a real run always has. It now has one.

The synthetic and generated fingerprints are unchanged by all three, and all 2,097 tests pass.

## Plausible, and what became of them

- **The data-quality checks' "open pipeline today" figures aren't marked "At risk".** Mistaken, as Brief BB found: the helper that values affected deals as exposure lives in data quality, but no data-quality check uses it. The five checks that give a figure for open pipeline today are all exposure rules and already read "At risk". A test now holds that (decision 395).
- **Service Cloud's agent-licence check counts dormant users on the last-login date alone.**
  - `svc.agent-licences` doesn't use the login log as 361 does. A Service Cloud user the log clears is left out of the dormant-user count, but still counted as a seat held for nobody's casework.
  - It's a separate check with its own licence price, so it's not double money, but the two can disagree about one person.
  - Fixed in Brief BB (decision 394): both checks now judge a user through one helper.
  - One difference is left: a Service Cloud user created inside the dormant period who hasn't logged in is counted by the licence check, but not by the dormant-user count, which leaves new users out.
- **The money table's "how it was reached" line says "if all came back" for seats and hours too.**
  - It's true as arithmetic: the figure before the share is counted.
  - But "came back" reads oddly for licences and time. That's wording, in B's area.
  - Fixed in Brief BB (decision 395): each kind of money has its own words.

## Checked and clean

- **The money total.**
  - The table's total is the value model's own.
  - On every sample and on the 500-user scale org, the rows add up to it, and so do the per-issue contributions (`addedByFinding`).
  - The executive summary, full report, one-pager, microsite and readout kit all take the same `value.total`.
  - The microsite lists every line, with no limit.
- **"How it was reached".** For each line, the figure before any share is `counted.annual`. "The largest of overlapping issues" appears only for a group with more than one line, and "a year's worth" only for a figure the model divides over the window. The share counted is the group's realisation. Each is true for its line.
- **Ranking by money added.**
  - Contributions sum to the total by construction.
  - Two issues with the same figure in one group are credited once: the group counts the first, by a strict "larger than".
  - Ties fall to the issue's own figure, then confidence, then id, so the same input gives the same order.
- **Coverage.**
  - Only `cur.coverage` is a target or pipeline need. It's kept out of the model (`NOT_MONEY_RULES`), ranks as nothing added, and its card says "pipeline to build", not money.
  - Of the other figures valued as revenue:
    - the open-pipeline ones are exposure, labelled in their assumptions and left out of the total;
    - campaign spend not coming back is an exposure rule;
    - the rest are recoverable money by design: stalled deals, past customers, segment gaps and discount leakage.
- **The exclusion's consistency (361).** A user the log clears is left out of the count, the action list, the value and the release guard's "what's in the way" list together, since all four come from the same remaining users.
- **"Well below" and "well above".** A segment only becomes an issue when it's at least the set number of points worse in its metric's adverse direction: lower for win rate, higher for slippage. So "below" for win rate and "above" for slippage are always the true direction.
- **The one-pager's "which is better" and "which is worse".** These come from the comparison's own side, and the chosen comparisons always include one that's behind when there is one.

## Already known, not this review's

The scale org's full report fails its number check on "£1.99m", the quarter's remaining quota from `cur.coverage`. The coordinator has passed it to B.
