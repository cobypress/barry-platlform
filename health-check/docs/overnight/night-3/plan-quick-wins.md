# Night 3, Brief AW: quick wins as their own box

Branch: engine `hc/plan-quick-wins` from `health-check` 01a1a76, with a6b45e7 merged in. Nothing is pushed or merged elsewhere.

**Commits:**
- fcaac5a: the box (DECISIONS.md 366-368)
- 87ba68c: coverage traced as money (369)
- a581b4a: the deck's box points to each action's card (370)

## Quick wins

**The list (366).** `Plan.quickWins` holds at most 8, the most money first (what each adds to the total), then the most serious. The rest stay in "Stabilise", outside its capacity, so no other action moves later.

**Where it shows.** The box is headed "Quick wins, also in days 1 to 30":
- *Executive summary:* under "Stabilise", one line each. It shows as many as fit in the pages the plan takes without it, and two if even one doesn't fit, with the longest phases giving up an action each.
- *Full report:* on "The first 90 days" overview. A dense deck has no overview, so there it follows "Stabilise" on its pages, and takes a slide only if they're full.
- *Microsite:* after "Stabilise".

**Unpriced savings (367).** A licence saving we couldn't price, with very small or small effort, is a quick win, and says "The saving isn't priced."

**"After 90 days" (368)** shows every reference an action covers.

**Layout.** The executive summary and every deck keep their page and slide counts:
- A first version gave the box a slide of its own, and the CPQ deck, at exactly its 130 cap, dropped a density step to 115.
- Writing each action into the deck's box did the same.
- So the deck's box lists titles and says each one's card has what to do (370).

## Coverage as money (369)

- The scale run's "£1.99m" (what's left of quota) was held as a plain number. It was printed as money only since 351.
- `MONEY_FACTS` now holds coverage's money facts as money.
- A quarter that's covered overall, but short for some reps, now says so rather than "£0 more pipeline".
- Pinned by a test with the scale org's facts. The scale org at 0.5% and 5% passes every check.

## Fingerprint

The test passes unchanged: findings, metrics, scores, manual steps, action lists and raw notes are all the same. The plan reaches only the report sections and the rendered documents, and neither is fingerprinted. So there was nothing to update.

## Checks

- **Engine:** typecheck clean, 2,083 of 2,083 tests pass.
- **Samples:** all clean (executive summary pages / full-report slides / one-pager pages):
  - Sales Cloud: 13 / 108 / 1
  - CPQ: 14 / 130 / 1
  - all three modules: 14 / 146 / 1
  - Essentials: 12 / 65 / 1
  - Essentials with CPQ: 12 / 68 / 1
