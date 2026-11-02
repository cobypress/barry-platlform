# Worker B: Reports log

Branch `hc/reports`. Newest status first in each section.

## Status

| # | Item | State |
| --- | --- | --- |
| 1 | Number check | Done |
| 2 | Narrative | Not started |
| 3 | Section builder | Not started |
| 4 | Design tokens | Not started |
| 5 | Charts | Not started |
| 6 | Raw notes in Word | Not started |
| 7 | Executive summary PDF | Not started |
| 8 | Full health check PDF | Not started |
| 9 | Layout checks | Not started |

## Log

- Merged `hc/integration` (19dc9ec) at the coordinator's request, by fast-forward, before writing anything. 421 tests passing, type check clean.
- Added `docx`, `playwright`, `@anthropic-ai/sdk` and `@fontsource/poppins`. Playwright 1.64 wants Chromium build 1248, and only 1243 was cached, so `npx playwright install chromium-headless-shell` added it to the shared user cache. Nothing outside that cache changed.

### 1. Number check

`src/numbercheck`: `extractFigures` reads text, `buildDataset` lists what's allowed, `checkNumbers` compares them. Reasons are in DECISIONS.md, R1 to R4.

Known limits, on purpose:

- It can't tell whether a true figure is attached to the right claim. Sections are scoped to their own sources to narrow this.
- Direction isn't checked. "Fell 35%" passes if 35% exists.
- Vague words aren't figures: "most", "many", "several". "Hundreds" and "thousands" are blocked.
- A bare month ("since December"), "first" and "second" aren't read as figures. Blocking them would reject ordinary sentences.
- Roman numerals and numbers in other languages aren't read. Non-Latin digits are blocked outright.
- Engine text adds its own figures to the dataset, so a figure in a reason such as "12 of 20" becomes quotable within that source's scope.
