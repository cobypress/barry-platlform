# Night 3, Brief BG: an issue's slide running off the page

**Branch:** `hc/card-overflow`, from `health-check` 942609a. Nothing is pushed or merged.

## What was wrong

An issue with a slide of its own (`issueSlide`) prints its whole money working in the value panel, and nothing limited its length. Cards don't print the working, so only these slides could overflow this way.

**The reproduction.** A test gives "Open deals have no active owner" a 13-line working, as a real org's weighting can run (one line per stage), and makes it critical so it has a slide of its own. Before the fix, it ran off the slide at step 1. Its value panel was 841px tall, against about 507px of room.

## Fixed

| Commit | What | Row |
|---|---|---|
| ac9b946 | The panel prints the working only as far as it fits, then "The full working is at R14.2". | 416 |
| 06ded50 | The layout check measures every element against the footer, not only the page's own children. | 417 |

Row 418 isn't used.

### The panel (416)

- **What's kept.** Whole lines of the working, in order, while they fit.
- **What's always kept.** The figure and its reference stay beside each other whatever is cut.
- **How it's estimated.** From measurements in Chromium:
  - 507px from the columns to the footer below a one-line title, plus 43px for each further title line.
  - About 170px of padding, headings and owner around a two-line figure.
  - Lines of 21px, about 56 characters each.
  - The text is wrapped at word breaks to 54 characters, as printed (with dates written out), and 495px is allowed. That holds a line or so back.
- **Effect on the Sales Cloud sample.** "Open deals have no active owner" keeps 5 of its 7 lines at steps 1 to 4 and 7. It had fitted with 23px to spare, which is under the line this holds back. C's held synthetic-org fix would have lengthened it anyway.
- **Tests.**
  - At every density step, the issue's slide passes the layout and number checks.
  - The figure keeps its reference.
  - A cut working says where the rest is.
  - A three-line working is printed whole.

### The layout check (417)

- **The gap.** A block inside a 10px parent could run into the footer unseen, because only the parent was measured. At 1,000px and 1,020px the old check reported nothing.
- **The fix.** Every element is now measured against the footer.
- **Tests.** A test fails on the old check and passes now. Every density step of all five samples still passes.

## Found, not fixed

**The re-check's executive summary overflows on page 11.** This is the 90-day plan page, and it runs 17px into the footer.
- It already does so on 942609a, with the same output.
- It shows on the Sales Cloud sample's re-check, built as the visuals test makes one: three issues fixed.
- The suite's re-check is built from the recorded run, without the intake and interviews, so it doesn't reach this.
- It's the summary's plan layout, not an issue card, so I've left it out of this brief.

## Checks

- **Engine:** typecheck clean; 2,138 of 2,138 tests pass.
- **Samples:** all clean (executive summary pages / full-report slides / one-pager pages):
  - Sales Cloud: 12 / 120 / 1
  - CPQ: 13 / 129 / 1
  - All three modules: 13 / 144 / 1
  - Essentials: 11 / 63 / 1
  - Essentials with CPQ: 11 / 66 / 1
- **Every density step:** all 12 steps of all five samples pass the stricter check.
- **The re-check's deck:** 116 of 120, clean.
- **500 users:** 150 of 150 slides, with an engine peak of 1,827 MB.
- **Fingerprints:** unchanged (render only).
- No database, Redis, sf, .env or Claude call was used.
