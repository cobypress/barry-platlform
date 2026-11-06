# Night 3, Brief AT: the reading pass, round two

Branches: engine `hc/reading-pass-2` from `health-check` fc0b81a. The console needed no change: its branch `hc/reading-pass-2` (from c4bf4dc) has no commits. Nothing is pushed or merged.

## The coordinator's five decisions

| # | Decision | Done | Row |
|---|---|---|---|
| 1 | "£2.79m at risk" isn't money | `cur.coverage` leaves the value model: it's in neither the total nor what's kept apart. It reads "To reach 3x coverage of your £1.2m quota this quarter, you'd need £2.79m more pipeline". In its severity, it ranks after money and before unsized issues, so its "decide this month" stays in the first 30 days. No other coverage or quota-gap figure exists. What's kept apart is now the largest real exposure, £2.42m. | 351 |
| 2 | Slide 86 against R20.2 and R18.5 | **Activity was a bug**, in the synthetic org (details below). **Skipping isn't a bug:** the two use different definitions on purpose, and the slide now says why. | 352 |
| 3 | The one-pager's benchmarks | Chosen by distance from what they're compared with, whichever way, with at least one behind when any is. On the sample, close-date slippage now shows, and the sentence says "43 points higher, which is worse". | 354, 356 |
| 4 | The compare pages' empty column | Left as it is. | – |
| 5 | The re-check's executive ceiling | 15 pages, from 14, with the 10% headroom decision 286 gave the decks. The re-check now renders at 14 of 15. | 353 |

**Slide 86, activity.** The synthetic org gave every closed deal its close day as `LastActivityDate`, while giving most of the EU team's recent closed deals no task or event. Salesforce sets that field from the deal's own tasks and events, so no real org can show both.
- The fix: closed deals created inside the readable year now take the date of their last task or event, or none.
- The fixtures are re-recorded. One exchange changed: the opportunities read.
- The driver now finds the same 24 deals as R20.2. They won 8.3% of the time against 49.4%, a 41.1-point driver, linked to the open deals with no activity.
- A test pins the two together, and it fails on the old recording.

**Slide 86, skipping a stage.** R18.5 counts won deals that never entered an expected stage, closing included. The driver counts only moves on to a later open stage. Counting a close would make every won deal "skip" and no lost deal could, so the comparison would only measure winning.

**Slide 86, chips.** Each driver's chip now says which way it points ("goes with winning" or "goes with losing").

**For Session C:** if `fixtures/synthetic-org/recording.json` conflicts at merge, run `npm run fixtures` again. The recorder writes the whole file.

## Applied from the rest of the list (wording or presentation, easy to reverse; DECISIONS.md 355)

**Figures and labels**
- "At risk:" now sits beside pipeline-at-risk figures, wherever an issue's own figure is shown. The campaign shortfall is left without it, as that spend is already gone. (Old items 2 and 9.)
- The investment page's seat total says "all licence types", which explains why it's 40 there and 30 in R15.3. (Item 34.)
- "Not counted here" replaces "No count kept" in the documented org. (Item 23.)

**Slides and charts**
- The drivers' chips give their direction. (Item 12.)
- The wins slides say what they count, against the lead funnel and the new-and-repeat split. (Item 13.)
- Area dividers no longer repeat the next slide's counts on another basis. (Item 17.)
- Chart captions, on both the roomy and the dense layouts (item 21):
  - "Monthly active users"
  - "Median days in each stage, won and lost deals"
  - "The value of your open pipeline at the end of each month"
  - the stage funnel counts "deals that reached each stage or a later one"
  - "Win rate by team and kind of deal" replaces "Win rate by segment"

**Titles**
- Discount leakage's title is singular when one rep is named. (Item 25, title only.)

**Tried and reverted**
- Renaming the raw notes' "Queries" and "Overrides". The brief names those sections, and a test holds them to it.

## Correction to my last report

The "Service + CPQ + Account Engagement" sample I gave as 107 slides was the base sample. zsh didn't split the flags. The real figure is 145, as the coordinator found.

## Checks

- **Engine:** typecheck clean, 2,061 of 2,061 tests pass (from 2,052).
- **Layout and number checks** pass on every sample (executive summary pages / full-report slides / one-pager pages):
  - Sales Cloud: 13 / 107 / 1
  - CPQ: 14 / 130 / 1
  - all three modules: 14 / 145 / 1
  - Essentials: 12 / 63 / 1
  - Essentials with CPQ: 12 / 67 / 1
  - the re-check: 14 of 15 / 118 of 120 / 1

## For Coby, with my recommendation

These change a figure, a verdict, scope or policy, or weren't done tonight for the reason given.

1. **"What matters most" ranks by severity, then by each issue's own figure**, across kinds of £. Executive page 2 and deck slide 10.
   - Coverage no longer leads, and exposure now says "At risk".
   - Recommend: within a severity, rank by the money counted in the total, not by the issue's own figure.
2. **The investment page with no annual cost is four tiles on an A4 page.** Executive page 4, and the one-pager's "What you pay" tile.
   - Recommend: fold both into the money page when there's no cost.
   - Not done tonight: it changes the summary's structure and page count.
3. **"£170 Manual work" sits in the headline and the waterfall.**
   - Recommend: keep it in the table, but leave kinds under £1k out of the waterfall's bars.
4. **"About 31.4 deals" and "0.4 deals" in a month that's already over.** Executive page 7.
   - Recommend: whole deals, and start the outlook from the current month. Both change printed figures, which the number check traces.
5. **The deck's two-column issue cards drop their money working.** Deck slides 39-76.
   - Recommend: the first assumption in small type under the meta line.
   - Not done tonight: it needs a pass of the density ladder.
6. **R18.1-R18.3's gap figures are two-year amounts that may overlap.** Deck slide 63.
   - Recommend: show "about £X a year" from the value model's line, and say when a rep sits inside a team that's also listed.
7. **Interviews.** Deck slides 53-56.
   - Re-keying is paired with a connected-apps setting, and "Agrees" is given at a 28% gap.
   - Recommend: pair re-keying with the duplicate and integration checks, and tighten the tolerance to 15%.
8. **R15.9 says "haven't logged in for 90 days: 6", then "has logged in recently (1)".** Deck slide 47.
   - Possibly a guard bug in `guards/user-guards.ts:30`.
   - Recommend: treat it as a bug, as with slide 86.
9. **Jul 2026's median cycle is 355 days**, against about 70 elsewhere. Deck slide 81.
   - Recommend: check whether it's an artefact of the synthetic org's services trap. If it's real, add a note.
10. **Roadmap phasing.**
    - Very small licence savings fall after 90 days.
    - R15.5, R18.2 and R18.3 are in no phase.
    - Recommend: bring XS savings forward, and give every High issue a phase.
11. **Small type.** Appendix definitions are about 6.5pt printed, and references are 0.72em grey.
    - Recommend: 13px and 0.85em, after checking each density step.
12. **Single cards stranded on near-empty slides.** Deck slides 41, 43, 64.
    - Recommend: fold a trailing single card back onto the previous slide. That's a pagination change.
13. **Salesforce API names in the deck** (Invoice__c, Lead.LeadSource, SBQQ__Discount__c, "acmecpq").
    - Recommend: carry object and field labels through the reads.
    - That's in the extractors, which are Session C's area.
14. **The microsite's "Every issue" is one 28,000px scroll.**
    - Recommend: jump links, areas in scorecard order, and "What good looks like" after the issues.
    - Not done tonight.
15. **Discount leakage's org-wide line undercuts its own headline:** "deeper discounts won 56%, against 40.2%".
    - Recommend: drop the org-wide line from the summary, and keep it in the working.
16. **Three near-identical open-deal titles** (R14.4, R17.3 and R22.3).
    - Recommend: retitle each by its test ("no activity for 90 days", "no stage change or activity for 30 days", "in one stage 60 days or more"). The numbers in titles need the number check's sources.
17. **Names.** Team-level text says "one rep … another rep". Whether names should come back on the microsite with consent is a policy decision.
18. **The product has three names:** "Barometer Scan", "full Barometer check" and "Salesforce health check".
19. **The deck's contents page has no page numbers.**
    - Recommend: add them from the slide order the deck already knows.
    - Not done tonight.
20. **R17.1 is "Effort: small".** That's an effort rating, and wording can't fix it.
21. **Wording held in the domains:**
    - R17.2's "58 of 70" against 76 open deals; it should say 6 couldn't be compared.
    - Deck slide 47 against 101: reports "haven't been run in 12 months" against a 90-day definition.
    - The raw notes' "Queries" and "Overrides" are named by the brief.
22. **Slide 82: 167 wins** against 164 elsewhere. The basis note explains type and source but not this difference. Probably the services side's prior wins; please check.
23. **At their ceilings, but not cliffs:**
    - The CPQ and all-modules executive summaries take 14 of 14, by the plan shrinking to fit.
    - The re-check's full report takes 118 of 120.
24. **Scan: the grade letter didn't show in a captured image.** Probably its 0.25s entry animation. Check it in a browser.
