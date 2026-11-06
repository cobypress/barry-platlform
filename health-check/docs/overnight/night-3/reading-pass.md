# Night 3, Brief AR: the reading pass

Branches: engine `hc/reading-pass` from `health-check` 0cbfe5d; console `hc/reading-pass` from `health-check` ce10448. Nothing is pushed or merged.

## What was read

**Samples rendered to images:**
- the full report: Sales Cloud, 107 slides; with CPQ, 130
- the executive summary: 13 pages
- the one-pager
- the microsite: overview, every issue, plan, compare, org, action lists, at 1280 and 390px
- the scan's results page: from an essentials run, at both widths
- a re-check: the decks, plus the microsite's "What has changed" page

**How it was read:** each page was read as a 200-user UK firm's finance director and sales director. I read the money pages and the re-check myself. Three reviewers read the rest: the executive summary, the one-pager and slides 1-35; slides 36-107; and the microsite, the scan and the CPQ slides. Every quote below was checked against the rendered text.

**The render script:** it builds each document's HTML the way `renderRun` does, screenshots every `.page`, and writes each page's visible text. The `--recheck` flag fixes three issues against the same run, as the visuals test does. It isn't in the repo; it lives in the session scratchpad.

## Fixed, one commit per kind (DECISIONS.md 326 to 330)

| Commit | Kind | Row |
|---|---|---|
| d15fc08 | Money: every line behind the total, a total row, how each line was reached, and one name for the unassigned seats | 326 |
| b9dba5c | The deck's template wording | 327 |
| a827bf7 | Finding text in the customer's words (mostly `plainWords`) | 328 |
| 9ad0f80 + console 65112bb | Microsite at phone width, the scan's locked issues, and the re-check figure's name | 329 |
| f171832 | Naming what couldn't be measured, the open-deal overlap, and references on the later roadmap | 330 |

No figure, share, verdict or ranking changed.

A first version of the team-level wording changed "a single rep" in titles too. That would have moved findings about one person into the top issues, and the overrides test caught it. Titles keep the phrase.

## Before and after: the five that matter most

1. **The money table didn't add up to the headline.** Executive page 3 and deck slide 7.
   - Before:
     - The executive table showed 5 lines (£794.5k against £795k); the deck slide showed 4 (£781k).
     - Neither had a total row.
     - The note said "the expected case is the conservative one", which is wrong: the low end is.
   - After:
     - Every line is listed: 7 here, 8 with CPQ. A slide holds 7 rows, so with CPQ it shows 6 and "And 2 smaller lines, in the raw notes".
     - A "Total, each counted once" row matches £795k (£346k to £1.58m).
     - On the executive summary, each line says how it was reached. For example: "£475k over 12 months if all came back, the largest of overlapping issues. We count 50% (25% to 100%)."
     - The microsite overview has the same lines, the total, and what's kept apart.
2. **The same 15 seats were priced three ways, under four names.** Executive page 4.
   - Before: "Seats nobody uses cost £18k a year", beside "We don't have a figure for what you pay a year".
   - After: "The 15 seats nobody is assigned to cost £18k a year. We count a share of it, £13.5k, in what fixing this is worth." The page opens "We don't have a figure for your whole Salesforce bill…"
3. **A shortfall was labelled as spend.** Deck slide 73 and microsite R21.1.
   - Before: "£315,000 campaign spend to date", next to "Trade Show cost £900,000 and returned £585,000".
   - After: "£315,000 more spent than won back, to date".
4. **Broken and machine-made sentences.**
   - "The expected stages are the ones what we agreed with you marks as required" became "…the ones you told us are required" (deck slide 63).
   - "Worst by rep: a single person, at 93.3%" became "The worst rep is at 93.3%" (executive page 8).
   - "Win rate for the Sales EU team is well away from the overall figure" became "…is well below the overall figure".
   - Also fixed: "2026-02" months, "1200 GBP a year", "Every guard passed", "Your Health Check score is 62", "1 deals affected", "impute", and two reps in one sentence who both read "a single rep".
5. **The deck's area pages misdescribed themselves.** Deck slide 12 and every area.
   - Before:
     - "What's working: Custom fields that are almost never filled in" (a passed check under its problem title).
     - "And 4 more in the raw notes." (they were on the next slides).
     - Every card read "Medium · Effort: medium · Medium risk".
     - Each area's first issue page was titled "Other issues".
   - After:
     - "Checked and fine: …"
     - "And 4 more on the pages that follow."
     - "Not costed" instead of the severity repeated as a risk.
     - "The issues in detail".

**Also fixed:**
- **Microsite tables.** The org page's save-order table had squashed its Object column to one letter a line ("O p p o r t u n i t y"). Cells now wrap between words, and a wide table scrolls inside itself.
- **Microsite interviews.** On a phone, the interviews table stacks each row.
- **Microsite charts.** The adoption charts go one to a row, under headings.
- **Microsite references.** A reference no longer runs into its word ("Win rateR29.1").
- **The scan.** "44 more issues" and "7 more in Integrations", with the line about the full check said once, and the reports' dates and words.
- **The re-check.** Its figure is "What the open issues are worth": £795k to £629k no longer reads as value lost.

## Checks

- **Engine:**
  - typecheck clean
  - 2,043 of 2,043 tests pass (from 2,030)
  - The layout and number checks pass on every sample: Sales Cloud (13 pages, 107 slides, 1 page), CPQ (13, 130, 1), Service + CPQ + Account Engagement (13, 107, 1), essentials (12, 63, 1), and the re-check (14 pages, at its limit of 14).
  - Nothing scrolls sideways on any microsite or scan page at 1280 or 390px.
- **Console:**
  - typecheck clean
  - 1,281 of 1,281 tests pass
  - Playwright scan.spec and microsite.spec: 32 of 32, desktop and phone. Port 3100 was free.

## For Coby: judgement calls, with where each is

1. **Executive page 3, deck slide 7: "Kept apart £2.79m, At risk".**
   - R17.1 is pipeline you'd need to add to reach 3x coverage. The quarter's whole quota is £1.2m, so a finance director will reject "£2.79m at risk".
   - It's typed as revenue, which is also why it ranks first in "What matters most".
   - Relabel it ("Pipeline you'd need to add"), or take it off the money page.
2. **Executive page 2, deck slide 10: "What matters most" ranks by severity, then by value, across four kinds of £.**
   - The four kinds are pipeline needed, open-pipeline exposure, a two-year gap ceiling, and weighted potential.
   - I added a note that these are each issue's own figures at full size, some at risk. Ranking by counted money, or labelling each figure's kind, is a product decision.
3. **One-pager: only the two flattering benchmarks.**
   - It shows win rate (28 points above) and cycle (14 days shorter), and leaves out close-date slippage (79% against 36%).
   - The cause is `COMPARED = 2` with a stable sort.
4. **Executive page 4, the investment page, with no annual cost:** four tiles and one line on an A4 page. The one-pager's "What you pay" tile says only that we have no figure. Fold both into the money page when there's no cost?
5. **"£170 Manual work" in the headline.** Executive page 3, deck 7, microsite overview.
   - It's 9.8 hours a year of unused reports, beside £614k.
   - Staff told us 2-4 hours a week go on re-keying, and that isn't counted. Leave out kinds under, say, £1k, or relabel it?
6. **Executive page 7: "about 31.4 deals", and "Oct 2026: 0.4 deals" when October has already passed.** Rounding changes figures the number check traces.
7. **The compare pages' empty "Black Cloud customers" column.** Executive page 6, deck 9, microsite compare.
   - Every row says "Not shown yet" or "Not enough customers yet".
   - The code says showing the count is a decision, so I left it. Drop the column until there are peers?
8. **Deck two-column issue cards drop the money working.** For example, "£782,540 one-off, if these leads are worked" with no basis. The single-issue slide shows it; the cards don't have room at that density.
9. **Exposure figures on cards look like money.** Deck slides 59, 60, 63, 67: "£2,423,000 open pipeline today" beside "£1,200 per year". Prefix them "At risk:", or style them differently?
10. **Deck slide 63, R18.1-R18.3.** The three gap figures (£1.33m, £704k, £621k) are two-year amounts, and the reps may sit inside the EU team. Show "about £X a year", and say which overlap?
11. **Deck slide 86 contradicts other slides. A possible logic bug; please check.**
    - "Had logged activity: Not measurable. 0 deals without it", against R20.2's 24 of 73 closed deals with nothing logged.
    - "Moved past an expected stage without entering it: 0 deals", against R18.5's 164 of 164 won deals that never entered Legal Review.
    - Source: `historic-pipeline/drivers.ts`.
12. **Deck slide 86's "Driver" chips point the wrong way for the reader.**
    - Pushed deals win more (55.8% against 45.3%), next to R17.5's advice that a moving date means the deal isn't on track.
    - Label the direction ("linked to winning" / "linked to losing")?
13. **Deck slide 82's counts disagree with other slides.** "Partner £1.43m, 34 wins" against R21.4's "60 leads and no won deals"; 167 wins against 164 elsewhere. Different bases (the deal's source against converted leads); one line on each basis would do.
14. **Interviews.**
    - Deck 54-56: re-keying is matched to "Any user can authorise some connected apps".
    - Deck 53: "Agrees" at 60 days against 77 measured, because the tolerance is 25%.
15. **R15.9, deck 47:** "haven't logged in for 90 days: 6… What's in the way: has logged in recently (1)". Check the guard (`guards/user-guards.ts:30`).
16. **Deck 81: Jul 2026's median cycle is 355 days**, against about 70 in every other quarter. Real, or a data artefact?
17. **Chapter dividers repeat the next slide's counts on a different basis.** Deck 44 says "9 Issues / 0 Passed / 1 Not measurable"; slide 45 says "10 Checks / 0 Passed / 9 Issues".
18. **The roadmap's phasing.**
    - Small licence savings ("Licences you can release: dormant users", effort very small) come after 90 days.
    - R15.5, R18.2 and R18.3 (two of them High) are in no phase.
    - Deck 92-95, microsite plan.
19. **Small type.** Deck 101-104's metric definitions are about 6.5pt printed, and references are 0.72em light grey. Raising them changes density across every deck, so the layout ladder needs a look first.
20. **Single cards stranded on near-empty slides.** Deck 41, 43, 64. Pagination runs by count, then by height; folding a trailing card back is a pagination change.
21. **Charts with no title.** Deck 50 (monthly active users), 83 (median days in each stage), 87 (open pipeline at month end); 85's drop-out lines have no numbers; "Win rate by segment" has an unlabelled overall line. A clear fix, not done tonight: it touches the chart frame shared by every chart.
22. **Salesforce API names in a director's deck.** Invoice__c, Project_Line__c, Lead.LeadSource, SBQQ__Discount__c, "acmecpq", "callout:". Labels may not be in every extract, so this needs a check of what each read holds.
23. **Deck 97: "No count kept" for Contact, Lead and Opportunity**, when the report counts them elsewhere.
24. **Microsite "Every issue" is one 28,000px scroll** (48,700px on a phone). The high issues sit two-thirds down, and there are no jump links. Order areas as the scorecard does, and add a contents list?
25. **CPQ: discount leakage's own evidence undercuts it.** "Those discounted more than the median won 56% of the time and those discounted less 40.2%". The title says "Some teams and reps" when one rep is named. Executive page 8, deck 93.
26. **Executive page 7: three near-identical open-deal titles** (R14.4 gone quiet, R17.3 isn't moving, R22.3 stalled). Retitle each by its test?
27. **Names.** Team-level text now says "one rep … another rep". Whether names should come back on the microsite with consent is for you; pages.ts's header suggests they do.
28. **The product has three names on one journey:** "Barometer Scan", "full Barometer check" and "Salesforce health check".
29. **Deck 2's contents has 27 sections and no page numbers.**
30. **Executive page 8: R17.1 is "Effort: small" for £2.8m of pipeline.** That's an effort rating, and the wording can't fix it.
31. **Wording I left in the domains, to stay clear of Session C's work there:**
    - R17.2's "58 of 70" against 76 open deals elsewhere; it should say 6 couldn't be compared.
    - Deck 47 against 101: reports "haven't been run in 12 months" against a definition of 90 days.
    - Deck 107: R30 "Queries" and R32 "Overrides" section names.
32. **The re-check executive summary is 14 pages, at its maximum.** One more page of anything, and it fails its layout check.
33. **Scan: the grade letter didn't show in the captured image.** This is probably its 0.25s entry animation. Worth a look in a browser.
34. **Seats you pay for: 40 (executive page 4) against "15 of 30 Salesforce" (R15.3).** Is 40 all licence types and 30 one type? If so, say so.
