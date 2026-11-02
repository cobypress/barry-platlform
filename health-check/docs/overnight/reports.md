# Worker B: Reports log

Branch `hc/reports`. Newest status first in each section.

## Status

| # | Item | State |
| --- | --- | --- |
| 1 | Number check | Done |
| 2 | Narrative | Done |
| 3 | Section builder | Done |
| 4 | Design tokens | Done |
| 5 | Charts | Done |
| 6 | Raw notes in Word | Done |
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

### 2. Narrative

`src/narrative`: `draftSection` takes a brief, an injected `NarrativeClient` and the pseudonymiser. `anthropicClient()` is the only code that touches the SDK, and no test calls it against the network. Reasons are in DECISIONS.md, R5 to R9.

- Found by a test: redacting secrets after JSON encoding misses `apiKey = "..."`, because the quotes are escaped by then. Sources are now guarded field by field first.
- The skill guidance recommends `fallbacks: "default"` for Opus 5.5. I left it out on purpose (R7). Coby can overturn that.
- The API request shape comes from the Claude API reference bundled with the tooling and from the SDK's types. It has never been sent to the real API, because there's no key tonight. First real call may need adjusting.
- Prompt injection: customer metadata goes into the prompt inside a sources block, marked as data. The number check and human review are what protect against it, not the marking.

### 3. Section builder

`buildReport()` in `src/narrative/sections.ts` gives all three outputs as `ReportSection` and `SectionVersion` records, with an engine-written first version for each. `applyNarrative()` adds Claude drafts as later versions. The ranking, plan and verdict in `select.ts` are code.

- **For Worker C and the coordinator:** there's no intake schema anywhere, so I added `Intake` in `src/narrative/intake.ts`. Stated problems, the intake summary and stated vs actual come back `needs_input` until it's supplied. The console's intake page needs to produce this shape, or tell me what it produces.
- **For Worker A:** the scorecard reads `Score[]`. There's nothing producing it on this branch, so domains say "Not scored in this run". `renderRun` will read a `scores.json` from the run folder if one exists. If scoring lands elsewhere, the coordinator only has to pass the array in.
- Executive and full reports stay at team level. People become "a single rep" or "a single person", because the plan says so (QUESTIONS.md, question 7). Raw notes keep names. A finding about one person's results can't be a top issue.
- Phases in the plan come from effort alone: XS and S in days 1 to 30, M in 31 to 60, L and XL in 61 to 90. That's a judgement a consultant may want to change.
- Verdict headlines are fixed text chosen by the worst severity.

### 4 to 6. Tokens, charts, Word

- `src/design/tokens.ts` uses the values in OVERNIGHT.md. DECISIONS.md 25 says tokens should come from `colors_and_type.css` in the design system artifact. No link to it was in the brief, so I couldn't compare them. Secondary tints (`muted`, `rule`, `panelMuted`, `panelRule`) are mine, as flat tints of Carbon.
- Charts are plain SVG strings. A chart with no data returns its reason. Rep segments are left out of charts.
- Found by looking at a screenshot, not by a test: a value label under the overall line, and "0 issues" on unassessed domains. Both fixed, and now tested.
- Word notes have every finding, evidence item, guard, metric table and override. Poppins is named but not embedded, so a machine without it shows a fallback font.
