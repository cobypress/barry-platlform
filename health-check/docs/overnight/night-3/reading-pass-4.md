# Night 3, Brief BE: reading pass 2's decided items

**Branches**
- Engine: `hc/reading-pass-4`, from `health-check` 3fa8515.
- Console: `hc/reading-pass-4`, from `health-check` c4bf4dc. It has no commits: item 14's page is built in the engine, and the console serves it as written.

Nothing is pushed or merged.

## Done, most customer-visible first

| Item | What changed | Row | Commit |
|---|---|---|---|
| 2 | **No annual cost.** The executive summary has no investment page. Its seat facts sit under the money table as "Your seats", and the one-pager's box shows the seats nobody is assigned to. | 401 | cdd0a5a |
| 3 | **Kinds under £1k** stay in the money table, but have no bar in the waterfall. On the sample, that's manual work at £170. | 402 | cdd0a5a |
| 15 | **Discount leakage's org-wide line** is out of the summary. It moves to the note on the closed-deals evidence, which the raw notes print, and stays in the facts. | 403 | 993f7a7 |
| 4 | **The delivery outlook counts whole deals.** The summary says "about 31 deals" and the chart says "4 deals". A month with value that rounds to no deals reads "under 1 deal". The range starts on the report date, and the summary says "By 31 Mar 2027, …". | 404 | 40be6d4 |
| 6 | **Segment gaps read as a year:** "About £664k a year in wins the gap may have cost", from the value model's line. A listed rep whose team is also listed says the two figures share deals. | 405 | aaea446 |
| 7 | **Re-keying** is read against the duplicate checks and the `int.` checks, with an integration on a personal login, not the connected-apps setting. "Agrees" now means within 15%, so "about 60 days" against 77 measured disagrees. | 406 | d139adc |
| 19 | **The deck's contents page** gives each entry's page number, in page order. | 407 | 615630b |
| 11 | **Type sizes.** Definitions are 13px and references 0.85em. List estimates now count a title's references. Every density step was checked. | 408 | b335f37 |
| 12 | **A lone card** left by the count goes back onto the slide before it, where it fits. | 409 | b10ef78 |
| 14 | **The microsite's "Every issue" page** has links by area, the areas in the scorecard's order, and "What good looks like" after the issues. | 410 | 1eef7a4 |

## Not done

**Item 5 (the first assumption on two-column cards): tried and dropped, as the brief says.**
- It was set at 11px under the meta line, and the card estimate counted it.
- With it, Sales Cloud needs 121 slides at the roomier step that item 12 gave it. That's one over its 120, so it drops back to 108 slides at the next step.
- The other samples kept their steps.
- Commit 5f6ae2a recorded this as row 410. Row 410 later went to item 14, so the item 5 record is here instead.

## Where I had to read the decision

- **Item 4, "start the outlook from the current month".** The outlook already started from the month holding the report date. On the sample, October has one day left, because the report date is 31 Oct.
  - I kept that start. The range now begins on the report date itself, and the text says "By 31 Mar 2027", so the first month reads as what's left of it.
  - "From 31 Oct 2026 to 31 Mar 2027" made page 6 overflow by a line.
  - **For Coby:** if you meant the first full month (November here), it's a one-line change in `monthsFrom`. It would change the £1.3m figure.
- **Item 7, "the integration checks".** The integrations area also holds two access settings: connected apps anyone can authorise, and remote sites over plain HTTP. I paired on the `int.` checks by id, plus `sec.personal-integration-accounts`, not on the whole area. Pairing on the area would have left the connected-apps setting in.
- **Item 14, "scorecard order".** I used the documents' scorecard ranking: weight times shortfall, with unscored areas last. The microsite's own "How each area stands" table used a fixed order, so it now uses the same ranking and the site agrees with itself.

## For Coby

- **Two lone cards remain on the Sales Cloud deck,** in Security and access and in Utilisation.
  - They're split by height, not count. The card estimate says they don't fit, though the slide before has room.
  - Folding them needs a less cautious card estimate, which would change every deck's pagination.
- **Sales Cloud is now at its ceiling, 120 of 120,** at the roomier step. One more slide sends it back to 106 at the next step. That's a step back, not a failure.
  - Its re-check is 116 of 120.
  - CPQ is 128 of 130, down from its cap of 130.
- **Exposure figures on the microsite and in the top-issues tables print to the pound,** for example "At risk: £2,423,000". This was already so before this brief, and I didn't change it.

## Fingerprints

They moved only where findings or notes moved, and each commit says why:
- **403:** recorded `findings.json` and `raw-notes.md`, for `cpq.discount-leakage` alone.
- **404:** recorded and generated `metrics.json` and `raw-notes.md`, for the two delivery outlook metrics alone.
- **405:** recorded `findings.json` and `raw-notes.md`, for the two rep findings of `pipe.segment-divergence` alone.

Every other item is render-only, and the fingerprints are unchanged for those. Each move was checked by diffing the recorded run's files before and after.

## Checks

- **Engine:** typecheck clean, and 2,125 of 2,125 tests pass, up from 2,113.
- **Samples:** every sample passes the layout and number checks (executive summary pages / full-report slides / one-pager pages).

  | Sample | Before | After |
  |---|---|---|
  | Sales Cloud | 13 / 108 / 1 | 12 / 120 / 1 |
  | CPQ | 14 / 130 / 1 | 12 / 128 / 1 |
  | All three modules | 14 / 146 / 1 | 12 / 144 / 1 |
  | Essentials | 12 / 65 / 1 | 11 / 63 / 1 |
  | Essentials with CPQ | 12 / 68 / 1 | 11 / 66 / 1 |
  | The re-check | — | 13 of 15 / 116 of 120 / — |

- **Every density step:** all 12 steps of all five samples pass the layout and number checks. `fullReportHtmlAt` renders a single step for this.
- **Console:**
  - Port 3100 was free.
  - `e2e/health-check/microsite.spec.ts` passes 14 of 14, on desktop and phone. It ran in `hc-reading-pass-2-console`, which is the same tree at c4bf4dc, because the new worktree has no `node_modules`.
  - Separately, the engine's new issues page went through the console's `reportPage` under `SITE_CSP` in Chromium.
    - All 15 area links land their area at the top, and "Back to the list of areas" returns to the list.
    - Nothing scrolls sideways at 390px, and there were no console errors.
- No database, Redis, sf, .env or Claude call was used.
