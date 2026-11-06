# Night 3, Brief AU: reading pass, round three

Branch: engine `hc/reading-pass-3` from `health-check` 7b64bd4. Nothing is pushed or merged, and no console change was needed.

## The coordinator's four decisions

**R15.9 was a bug (361).**
- *What it said:* "haven't logged in for 90 days: 6", and then "has logged in recently (1)".
- *The cause:* the count included a user whose last-login date is 150 days old while our own login log shows three logins since. That user is the synthetic org's trap.
- *The fix:* the dormant and never-logged-in counts now leave out anyone the login log shows logging in, and say so: "One more user's last-login date is over 90 days old, but the login log shows them logging in since, so they aren't counted."
- *Pinned:* a test pins the contradiction.
- **For Session C:** the dormant count on the synthetic org is now 5, not 6, so it's a fingerprint change.

**Ranking (362, 365).**
- Issues now rank within a severity by what each adds to the total, after the overlap rules (`addedByFinding`, `rankBy`).
- Ties, such as issues that add nothing, follow their own figures.
- The contributions add up to the total.
- R-numbers keep their order, so no reference moves.

**The roadmap (363).** There were two causes:
- **Same action.** De-duplicating on the recommendation's wording alone dropped R18.2 and R18.3 behind R18.1's identical action, and R15.5 behind the dormant users' "Deactivate 1 user…".
  - Now an issue from the same rule with the same action joins it, and the action shows every reference ("R18.1 R18.2 R18.3").
  - Different rules keep their own actions.
- **Capacity.** Very small work used up the first month's capacity.
  - Quick, cheap wins (very small effort, at least medium or saving money) now go in the first 30 days, outside its capacity of 8.
- A test checks that every issue with something to do is planned.

**167 against 164 (364).** These are two honest bases:
- 167 is every deal won in the window.
- 164 is the won deals with stage history. Three were entered straight as won.

Each page now says which, and a test pins both.

## Checks

- **Engine:** typecheck clean, 2,064 of 2,064 tests pass.
- **Every sample is clean** (executive summary pages / full-report slides / one-pager pages):
  - Sales Cloud: 13 / 108 / 1
  - CPQ: 14 / 130 / 1
  - all three modules: 14 / 146 / 1
  - Essentials: 12 / 65 / 1
  - Essentials with CPQ: 12 / 68 / 1
  - the re-check: 14 of 15 / 118 of 120 / 1
- **No leaks:** no internal reference reaches a customer page.

## New for Coby

1. **The first 30 days now hold 22 actions on the synthetic org: 8 urgent and 14 quick wins.** Each is very small, but the list is long.
   - The executive summary's plan shrinks to fit, so it shows the first few.
   - Recommend: show the quick wins as their own short list beside "Stabilise", or cap them at about 8 a month.
2. **Unpriced package seats stay after 90 days.** `utl.package-seats` is a licence saving, but it's low severity, and its seats have no price (acmecpq), so it doesn't count as a quick win.
   - Recommend: count an unpriced licence saving as a quick win too.
3. **The "After 90 days" list shows only each item's own reference**, not the others its action covers. None does on the synthetic org.
