# Morning hand-over

**Run the migration before pushing `hc/console`.** It adds a Prisma schema change, and the SQL in `migrations/20261103_health_check.sql` hasn't been applied anywhere. Use a Neon development branch first. Don't deploy the console yet in any case: it runs on an in-memory fixture, so data would vanish between requests.

**Nothing was pushed, deployed or applied to a database, and no Salesforce org or live Claude API was contacted.** Your `health-check` branch holds only the Milestone 1 commit.

## What was built

All of it ran in about 50 minutes, so it needs your review more than its size suggests.

| Branch | Contents | Checked by the coordinator |
| --- | --- | --- |
| `hc/integration` (barry-platlform) | Milestone 1 fixes, all 12 domains, 71 rules, scoring, value sizing, report scanning, number check, Claude narrative (stubbed), Word and PDF outputs, layout checks | 929 of 929 tests. Type check clean. 55 of 55 deliberate breakages caught. Read-only allowlist unchanged |
| `hc/console` (blackcloud-next) | `/internal/health-check` on fixture data, Google sign-in, review console, release, Prisma schema and SQL, queue helper | 384 of 384 unit tests. 156 of 156 Playwright tests at desktop and phone widths. Removing the session check fails 50 tests |

The synthetic org gives a 6-page executive summary and a 59-slide deck, both passing the layout check. To see them, run `npm run sample` in `worktrees/hc-integration/health-check`. They're written to `~/.barometer/sample-runs`.

## Fix this first: a security problem on your live site

These are on `main` today, in `/internal/scrape` sign-in. They aren't new, and we didn't change them because the brief says not to touch existing behaviour. We confirmed the first two by reading the code.

1. **Open redirect.** `app/api/auth/scrape/callback/route.ts` checks `next.startsWith("/")`, so `//evil.example` passes and sends a signed-in user to another site.
2. **An empty allowlist lets everyone in.** If `SCRAPE_ALLOWED_EMAILS` is empty, any verified Google account is accepted. The same callback can also set the admin `links_session` cookie. We can't read your environment variables, so **please check that this variable is set in Vercel.**
3. **No CSRF protection.** The OAuth `state` has no nonce.

The new console sign-in doesn't share any of these. `lib/health-check/auth` shows the safer pattern.

## What the coordinator caught

The workers' tests passed on their own branches. These only showed up on review or once the branches were combined.

- **Scoring:** any domain with no passes scored 0, so five low-severity issues scored the same as five critical ones. Seven of twelve domains read 0. Scores now run from 56 to 77 on the synthetic org.
- **Deck:** an ordinary org of 69 issues fell into a last-resort mode that dropped every summary. Each issue now keeps its summary and recommendation.
- **Milestone 1 review:** an independent review found 13 defects, 4 of them high, including a way to send the token to the wrong host, and win rate and conversion both overstated. 12 are fixed. The 13th, time zones, was then fixed by Worker A.
- **By eye:** raw ISO dates in customer text, and issue cards using half the slide.

## What hasn't been proven

The whole product has only ever seen invented data.

- **No real org.** Every Salesforce response is synthetic. Worker A's list of unverified fields is in `docs/overnight/domains.md`, and several field names are from memory. A wrong field fails safely to not measurable, but expect a batch of them on the first live run.
- **No real Claude call.** The request shape comes from the docs and the SDK's types.
- **No real Google sign-in.**
- **Full `next build` didn't finish.** The console compiled, then the build stopped on existing routes that need real credentials (`/api/csat/respond`, `/sitemap.xml`). Run it yourself before pushing.
- **Word output** was checked through its XML and one preview, not in Word.

## Not finished

- **The console and engine aren't connected.** Starting a run in the console clones synthetic analysis and starts no job. The Prisma repository, the worker service and real Claude panel are still to do.
- **Two things need joining up.** The console has a stand-in number check that misses swapped figures, and it must be replaced with the engine's before customer use. The console's `RunIntake` and the engine's `Intake` are different shapes.
- **Screenshot answers on manual steps** aren't built, nor is notes file storage.
- **Milestone 2 wasn't touched,** because it needs your Salesforce setup.
- **A very clean org** would give a deck under 40 slides, which `hc render` treats as a failure. That needs a decision.

## Decisions for you

1. **First live run:** is `BlackCloudProd` the right alias? It's the next step, and it matters more than any further building. Set `HC_APPROVED_ORG_ALIASES=BlackCloudProd`, and we'll go through the findings together.
2. **Customer names:** done on 3 Nov. The plan document and both deal-to-cash docs are clean but uncommitted. The name is still in `blackcloud-next` history at `02e9c83`, which only a history rewrite would remove.
3. **Scoring and thresholds** are our judgement: the formula, verdict wording, top-five order, the 90-day phasing by effort, and limits such as 150 custom fields. Read one sample report as a customer would.
4. **Second approver:** should one person be able to run and approve a report? The console allows it, as the plan says.
5. The earlier questions in `docs/QUESTIONS.md`: hosting, regions, Neon branch, S3, zero data retention.

## Update, 3 Nov

Committed locally only. Nothing was pushed, and no org or live Claude API was contacted.

- **Report wording**, after reading the sample as a customer would:
  - Plan actions now name their issue.
  - The "decide now" coverage action has moved into days 1 to 30.
  - Effort reads in words.
  - Amounts are formatted.
  - Divider slides name the first issue.
  - Licence blockers no longer show internal guard ids.
- **Salesforce field names** were checked against Salesforce's own reference PDFs. 4 were wrong and are fixed. `PermissionSet.PermissionsViewAllData` is the only name still unconfirmed. LoginHistory, ForecastingItem and Campaign now drop a missing optional field instead of failing the whole read. The details are in `docs/QUESTIONS.md`.
- **Console** (`hc/console`) uses a copy of the engine's number check. It maps intake to the engine's shape, and in development it can open real engine run folders through `HC_DEV_RUN_DIR`. Both repos run the same 36 golden number-check cases.
- **Tests:** engine 974 of 974, console 455 of 455 unit and 156 of 156 Playwright. The sample gives 6 pages and 60 slides, and the layout check passes.

New decisions for you:

- **Swapped figures pass.** The number check can't tell "fell to 18% from 31%" from "rose to 31% from 18%" when both figures are in the same finding. Rule 1 doesn't require catching this, but a check on direction would be new work.
- **Days 1 to 30 holds 47 of 66 actions**, because phasing is by effort alone. Capping that phase, or phasing by severity too, is your call.

## How to take it or leave it

```bash
# Take the engine
cd "/Users/cobypress/Desktop/Black Cloud/barry-platlform" && git merge hc/integration

# Take the console (after running the migration on a development branch)
cd "/Users/cobypress/Desktop/Black Cloud/blackcloud-next" && git merge hc/console

# Throw a branch away: remove its worktree, then delete the branch
git worktree remove "/Users/cobypress/Desktop/Black Cloud/worktrees/hc-domains"
git branch -D hc/domains
```
