# Night 3, Brief BI: the re-check's plan page

**Branch:** `hc/recheck-plan`, from `health-check` cbb6ff1. One fix, 391c589, row 418. Nothing is pushed or merged.

## What was wrong

The Sales Cloud sample's re-check ran its executive summary's 90-day plan page 17px into the footer. The re-check had the intake and interviews, with three issues fixed.

- **Where it came from.** The quick wins' box couldn't fit the pages the plan had without it, so the plan took the fallback. It was forced onto one page with a box of two. The estimate said 911 of 915px; the page took 925px.
- **Why the estimate was short.** Each action's height was counted from its words alone. It left out the reference tags beside the action: its own, and those of the issues it also covers. Those tags have been 0.85em since row 408. It also left out the "no figure yet" note. An action near the end of a line wrapped once its tags were in, and three actions in the Build phase each took a line more than counted.
- **The quick wins' box** was the pressure that sent the plan to the fallback, but its own estimate was right.

## The fix (418)

- **Tags are counted.** Each action's tags count at about 5 characters each, and the note is counted too.
- **Calibration.** I checked line counts against 135 real actions on the five samples' plans and their re-checks.
  - The old count was short 4 times.
  - Counting tags at 6 characters was never short, but it was long 9 times. That pushed Sales Cloud and CPQ to an extra summary page.
  - At 5 characters, the count is never short and is long only 4 times.
  - On the final layouts: 128 actions, never short. The old count would have been short 3 times.
- **Heights and room.** The fixed heights now sit just above the measurements: 19.8px a line, 41px a phase heading. The page has its measured 925px, not 915px.
- **Margin.** Sales Cloud's plan now uses 922 of its 925px. That's tight, but every estimate sits at or above what was measured, and the layout check measures the real page.

## Test

- A test builds the re-check the way the sample's is made: the recorded run with the intake and the synthetic interviews, and the first three issues fixed.
- It puts the summary and the deck through the layout and number checks. The summary fails without the fix.
- It asserts no problems, not counts, so C's synthetic-org change shouldn't break it.

## Checks

- **Engine:** typecheck clean; 2,140 of 2,140 tests pass.
- **Samples:** all clean, and every summary keeps its page count (summary pages / deck slides):
  - Sales Cloud: 12 / 120
  - CPQ: 13 / 129
  - All three modules: 13 / 144
  - Essentials: 11 / 63
  - Essentials with CPQ: 11 / 66
- **The samples' re-checks:** all clean (summary pages / deck slides):
  - Sales Cloud: 13 of 15 / 116
  - CPQ: 13 of 15 / 124
  - All three modules: 13 of 15 / 142
  - Essentials: 11 of 15 / 61
  - Essentials with CPQ: 11 of 15 / 70
- **500 users:** 12 summary pages and 150 of 150 slides; engine peak 1,841 MB.
- **Fingerprints:** unchanged (render only).
- No database, Redis, sf, .env or Claude call was used.
