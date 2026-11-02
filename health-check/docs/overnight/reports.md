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
| 7 | Executive summary PDF | Done |
| 8 | Full health check PDF | Done |
| 9 | Layout checks | Done |

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

- `src/design/tokens.ts` uses the values in OVERNIGHT.md. The coordinator read `colors_and_type.css`: the five brand colours match, so I took its neutrals for muted text (`#555555`) and hairlines (`#EBEBEB`), and its font stack. It has no neutrals for Carbon, so `panelMuted` and `panelRule` stay mine. I haven't seen that file myself.
- Charts are plain SVG strings. A chart with no data returns its reason. Rep segments are left out of charts.
- Found by looking at a screenshot, not by a test: a value label under the overall line, and "0 issues" on unassessed domains. Both fixed, and now tested.
- Word notes have every finding, evidence item, guard, metric table and override. Poppins is named but not embedded, so a machine without it shows a fallback font.

### 7 to 9. PDFs and layout checks

`hc render --run <folder>` writes the Word notes and both PDFs into the run folder. `npm run sample` does it for the synthetic org, into `~/.barometer/sample-runs`, outside the repo. On this branch the sample gives 6 pages and 58 slides, with no layout or number check problems. I also looked at the rendered pages myself.

Bugs found while building this, all fixed with tests:

- A finding about one rep reached the top five, because the filter looked for a token that team-level text had already replaced. Seen on a screenshot.
- `document.fonts.check` returns true for a font that was never registered, so the font check couldn't fail.
- Flexbox squeezed oversized content rather than overflowing, which hid it from the check.
- `textContent` ran neighbouring table cells into one "figure". Rendered text is now read node by node.
- An unscored scorecard counted as a missing chart.
- A very large org gave 141 slides.

### After merging hc/integration (coordinator's review)

Merged 12 domains in, ran `npm install`, then fixed the following. Worker A is changing the scoring formula, and nothing here hard-codes a score: `scores.json` is read as data.

- **Deck for an ordinary org.** The sample has 69 issues and was dropping to compact tables. Non-issue slides took about 47 of the 60. It's now 59 slides with every summary and recommendation on an issue card, and no cuts. Dense mode sets cards, roadmap and metric definitions in two columns. A further step swaps the domain openers for one summary slide, which leaves room: an 84-issue org keeps its detail too. Compact tables only appear for a very large org, and say so. 59 is close to the ceiling, but going over moves to the summary step rather than losing detail.
- **Test assumptions.** Tests that assumed forecast wasn't assessed now remove a domain from their input. The executive test reads its values from the data.
- **Raw invisible characters** in the `normalise` regex in `extract.ts` are now `\\u` escapes. They worked, but nobody could review them.

### Mutation test

50 breakages of my safety controls, then 6 more after fixes. All are caught now except one, listed below.

- The first run found three test gaps: the prompt-wide guard, engine text failing the number check, and the draft mark on the deck. All closed.
- Still survives: removing the final sweep for stray digits in `extractFigures`. I couldn't find an input that reaches it, including 2,000 random strings, so it looks like a redundant backstop. I've kept it because it fails closed.

The mutation script isn't in the repo. The coordinator's own run won't include these unless it's rebuilt from this list of controls.

### For Worker A or the coordinator

These reach customer documents, and they're rule text, so I left them alone.

- Rule text describes our own build state: "which isn't built yet" in `historic-pipeline/index.ts`, "until the activity domain is built" in `utilisation/index.ts`, and "needs the lead funnel domain's conversion rates" in `data-quality/index.ts`. With all domains merged, these are out of date and shouldn't be customer-facing.
- A lead funnel summary prints money with no formatting: "Trade Show cost 900000 GBP and returned 585000". The second figure has no currency at all.

### What hasn't been proven

- No call has been made to Claude. The request shape is from the bundled reference and the SDK's types.
- `hc render` has only run on synthetic data.
- Word output was checked through its XML and one Quick Look preview, not in Word.
- PDF page size and count are checked. The PDFs themselves weren't opened in a viewer: I reviewed Chromium screenshots of the same HTML.
- Chromium build 1248 was downloaded for this machine. The worker host needs one too (QUESTIONS.md, question 1).
- `Intake` is my own shape, so the console has to match it.
- Phasing by effort, verdict wording and the top-five order are judgement calls for Coby to review.
- A deck under 40 slides gets flagged and `hc render` exits non-zero. A very clean org will hit that.
