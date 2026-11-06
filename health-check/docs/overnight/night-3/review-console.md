# Night 3: review of session B's console work (Brief AO)

Worker A, 6 to 7 November 2026. This is an independent review of what session B built on 6 November, followed by fixes for what it confirmed. I didn't write any of the code under review.

## Scope

- Console: blackcloud-next health-check, `3a96678..ca2251b`. That covers interviews, the action lists on the documents card, the scan funnel and tally, Drive sharing on the console side, the microsite's action lists page, and partners.
- Engine: the worker's Drive sharing (`src/worker/drive.ts`, `share-job.ts`, the documents job's share step, and retention), and its interviews read.

## Branches

- Console: `hc/review-console`, from health-check `ca2251b`, in `worktrees/review-console`.
- Engine: `hc/review-worker`, from health-check `b9c28a2`, in `worktrees/review-worker`. This log is on this branch.

Nothing is pushed.

## How I worked

I read every server action, route handler and page in the range, then the worker's sharing and retention code. Each finding I could reproduce got a failing unit test first, then a fix in its own commit. The one exception is noted under C2 and C3.

Anything that needs a real database, Drive or Slack to show is listed under "plausible", with the reason it couldn't be confirmed. I ran no database, Redis, Drive, Slack, Salesforce or Claude call, and opened no `.env`.

## Confirmed and fixed

### Console (hc/review-console)

**C1. The documents card hid anyone who still had the folder after being taken off the list.** `bd5a8cb`, then `a8c1b87`.

- `shareRows` listed only the current readers. So someone was invisible on the card in any of these cases:
  - their removal failed in Drive;
  - their share job was never queued (`enqueueShare` swallows a queue failure);
  - the job never ran.
- Now anyone off the list whose access hasn't been taken away is listed as "Off the list, access not taken away", with Drive's reason when there is one.
- After W2, that includes a share recorded as failed. Drive may have made it, until the worker has looked.
- Test: `drive-share.test.ts`, "someone taken off the list who still has the folder is shown".

**C2. The Scans page and a partner's page could count a request twice while the 30-day cleanup ran.** `5f8c2f8`.

- Both pages read the live requests first, then the tally. The cleanup adds a request to the tally in the same transaction that deletes it. If that transaction commits between the two reads, the request appears in both.
- The tally is now read first, then the live requests, then the tally again (`readFunnel` in `scan/funnel.ts`). If the tally moved, the cleanup ran in between, and everything is read again.
- After three tries it keeps the tally from before the last live read. That can miss a request but never counts one twice.
- The Scans page's reads moved into `scansView` (`scan/console-view.ts`) so they can be tested.
- To confirm the bug, I first wrote `scansView` in the page's original order. With `partnerStats` as it was, the cleanup-mid-read tests then failed with 2 where 1 was expected.

**C3. The funnel's period counted the same request differently depending on whether it had been deleted yet.** Same commit as C2.

- Live requests were counted from the exact moment 7, 30, 90 or 365 days ago. Tallied ones were counted from the whole of that UTC day, because a tally row knows only its day.
- A period is now whole UTC days for both (`funnelFirstDay`).
- This went in one commit with C2 because C2's guarantee depends on it: with mismatched boundaries, a request can sit inside one read's period and outside the other's.
- Test: "the period is whole days, for the requests still kept as for the tallied ones" (failed with `[0, 1]`).

**C4. The scan request's Slack notification text wasn't escaped.** `651872e`.

- The blocks were escaped, but the `text` field carried the prospect's company and the partner's name raw. Slack shows that field in notifications and reads it as mrkdwn, so `<!channel> Co` went to Slack unescaped.
- Both are now escaped as the blocks are.
- Whether Slack acts on a mention in the fallback text when blocks are present isn't verified (P4). The fix doesn't depend on it.

### Worker (hc/review-worker)

**W1. Twelve months after release, a later share could hand the folder out again.** `17fedad`, decision 311.

- The retention sweep took everyone's access away at 12 months, but nothing stopped a later share. The next share job, after a reader was added, or the next documents make shared the folder again with whoever was on the list.
- The microsite can outlive those 12 months, because its own 12 months start from its last make.
- The share sync now reads the run's last release date and, past 12 months, wants nobody, recording the reason as `retention`.

**W2. A share Drive made, but whose answer was lost, was never taken away.** `25b5141`, decision 312.

- In these cases the row was recorded as failed, with no permission id:
  - a network error after Drive committed;
  - an answer with no id;
  - a store write failing after the share.
- Removal and retention only looked at rows recorded as shared, so that person kept the folder for good.
- Now a failed row is looked for before it's closed, using `findReader`. That's a read-only `permissions.list` that matches only a `type: "user"`, `role: "reader"` permission by address.
- The sweep counts a failed row as possibly holding access. A look-up that fails is recorded as "Couldn't check whether Drive shared it", and the job retries.

**W3. A reader taken off while documents were being made kept the folder if the make then failed.** `68f00b6`, decision 313.

- The share job found the run locked by the documents job and was given up after its third try, about 90 seconds in.
- The documents job only synced sharing once the documents were in Drive.
- It now syncs however it ends: on success, and on a final failure.

## Plausible, not fixed

**P1. Two cleanups at once could add a request to the tally twice.**

- `deleteExpiredScans` finds the expiring requests, tallies them, then deletes them, in a READ COMMITTED transaction with no row lock.
- Two overlapping cleanups would both find the same rows and both add to the tally. The second delete would just find nothing left.
- Vercel's cron fires once a day, so this needs a manual call or a retry overlapping a slow run.
- The fix I'd make:
  - lock the rows first, with `SELECT ... FOR UPDATE` on the expiring requests through `$queryRaw`;
  - or tally only what the delete returned, with `DELETE ... RETURNING`.
- Not confirmed: it needs Postgres's concurrency, and I connect to no database.

**P2. `unshare` reads every 404 as "already gone".**

- If the service account loses sight of the folder, Drive may answer 404 while the permission still stands. The person would then be recorded as removed while keeping access.
- Needs a real Drive. Engine QUESTIONS.md, Drive e).

**P3. A microsite reader whose address is a Google Group.**

- The worker shares as `type: "user"`. Whether Drive refuses that or shares with the whole group isn't known.
- Engine QUESTIONS.md, Drive f).

**P4. Slack's fallback text.** Whether a mention in `text` notifies when blocks are present. Fixed regardless (C4).

**P5. Two worker hosts.** The run lock is a file, so two hosts could run two share jobs for one run at once. `lock.ts` says it assumes one host, and the token store assumes the same. With one host, the jobs serialise and each reads the list inside the lock.

**P6. Re-creating a reader permission.**

- After a failure, the sync shares again. Whether Drive then returns the existing permission's id isn't known.
- Nor is whether it would ever lower someone's higher role, such as a Black Cloud writer put on the reader list.
- Engine QUESTIONS.md, Drive g) and h).

## Checked and clean

- **Every server action checks the consultant session.** All new actions go through `action()` in `lib/health-check/actions.ts`, which verifies the token before parsing anything:
  - `saveInterview`, `deleteInterview`;
  - `savePartner`;
  - `markReferral`, `removeReferral`;
  - the three reader actions that now queue sharing.

  The `(console)` layout calls `requireConsultantPage`, the middleware guards `/internal/health-check`, and each new page calls `requireConsultantPage` too.
- **Interview ownership.** `saveInterview` and `deleteInterview` refuse an interview that isn't on the run named. The Postgres write also refuses to move an interview to another run. Neither works on a released run.
- **Public routes.** The scan form's `?ref` is written into the page only when it matches `^[A-Za-z0-9_-]{16}$`, and escaped. A code is looked up only on submit, and the answer is the same whether or not it matches. So the form tells a visitor nothing about partners, and it never names one. The results page shows no partner.
- **Partners see nothing.** No page, email or message goes to a partner. Their page shows counts only, behind the consultant session.
- **The microsite's action lists page:**
  - it's built per reader, from the signed-in session's email;
  - it appears only when the documents are done and that address has a live share on the run's current folder;
  - links are limited to `https://drive.google.com` and `https://docs.google.com`, with `noopener noreferrer`;
  - everything is escaped;
  - a worker page's slug can't be replaced.
- **Drive permissions.**
  - The only permission ever created is the frozen `{ type: "user", role: "reader" }`.
  - The address is checked against a strict pattern and goes only into the JSON body.
  - Folder and permission ids are checked before they go into a path.
  - The existing code scan finds no other kind or role in the worker. `findReader` reads only.
- **Queueing.** Every console job has its own BullMQ id, so a second share job is never dropped as a duplicate of a running one. Each job reads the list inside the run lock.
- **Retention order.** Access goes before the local run folder, and the folder is kept while anyone may still have access, now including failed rows.
- **First touch.**
  - Repeat requests from one address keep the first request, and with it the first partner.
  - A referral is one per run (primary key), and a duplicate becomes `ReferralExists`.
  - A lead keeps its first partner.
  - A partner's code can't change: the store refuses it.
  - Hardening I'd add later: `ScanRequestPatch` still allows `partnerId`. Nothing writes it, but leaving it out of the patch type would make an overwrite impossible to write.
- **Migrations against the Prisma schema.** The five migrations match the models: columns, nullability, keys, cascades and indexes.
  - The SQL has CHECK constraints that Prisma can't express.
  - The referral's partner key has no `ON UPDATE CASCADE`, which Prisma would add. Ids never change, so it makes no difference.
- **Interviews and Claude** (decisions 179 to 188).
  - The worker selects `id`, `role`, `heldOn`, `notes`, `answers` and `version`, and never reads `interviewer` or `createdBy`.
  - Claude gets each interview's role, its answers as said sentences, its notes and the readings, through the usual guard. The date, the interviewer and the ids stay out.
  - No document prints the notes.
- **XSS.** The scan and microsite HTML escape every interpolated value, and the new pages add no script. The console pages are React.
- **The CSV formula escape.** The tickets CSV prefixes a cell starting with `=`, `+`, `-`, `@`, a tab or a carriage return, and quotes every cell. The workbook writes text as text.
- **Slack mrkdwn.** Prospect- and consultant-typed text is escaped (`&`, `<`, `>`) in the request blocks, the settled message and the lead text. Mentions need `<`, so escaping it is enough. The text field is now fixed too (C4).

## Console decisions

- **R1.** The documents card lists anyone off the list whose share isn't recorded as removed, under one label: "Off the list, access not taken away". One label, because "failed" may mean Drive made the share and lost the answer.
- **R2.** A funnel period is whole UTC days, from the first day the period touches. The tally only knows days.
- **R3.** The funnel reads the tally three times at most, and on the last try prefers missing a request to counting one twice.

## Tests

- **Console unit tests:** 1,273 of 1,273 (1,267 at `ca2251b`, plus the new ones). `tsc` is clean.
- **Engine:** 1,998 of 1,998 tests, and `tsc` is clean.
- **Browser tests** (port 3100), on `hc/review-console`, both widths:
  - documents, drive-share, scan and partners: 28 of 28 (44 seconds);
  - auth, which visits every console page and checks that an anonymous POST to each action is refused: 158 of 158 (15.6 minutes).

  I didn't run the full suite of 406.

## Practical notes

- A worktree's `node_modules` symlinked from another checkout works for `tsc` and the unit tests. Turbopack refuses it, though: "Symlink node_modules is invalid, it points out of the filesystem root". For the browser tests I copied it with `cp -cR`, an APFS clone, from `worktrees/hc-console-merge`, whose Prisma client is generated for `ca2251b`'s schema.
- The main checkout's client is generated for an older schema, so it fails `tsc` on the new tables. I didn't run `prisma generate` there, because it would change the main checkout's client.
