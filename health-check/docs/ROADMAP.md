# Roadmap: the best Salesforce health check on the market

Written with Coby on 5 November 2026. A living plan: stages are in order of the value they add, and each one ends in something a customer can see. Decisions are recorded at the end with the date they were made.

## What "a 10" means

A customer finishes the readout knowing three numbers: their Barometer score, the money we found, and where they rank against similar companies. They repeat those numbers to other CEOs, and they know they got more than they paid for. In practice:

- **Every pound is counted.** The executive deck says what they spend on Salesforce, how much money is being missed, where, and how much they could make. Every figure carries a reference to the numbered page of the raw notes that shows the working.
- **Nothing is left unexamined.** The full report runs to about 100 pages and goes through every part of the org: what's good, what's below par, what's broken, what it costs, what best practice says, and how they compare.
- **It's theirs.** It answers what they told us, in their terms, against their goals, and reads as written by a consultant who knows their business.
- **It proves itself.** We measure again after 90 days and show what changed, in money.

The rules that make it trustworthy don't change: code calculates and Claude explains, every figure traces to evidence, nothing is estimated where the data can't support it, and every assumption is written down.

### Why we can beat the big consultancies

| | Big consultancy | Barometer |
| --- | --- | --- |
| Coverage | A sample, through interviews | Every record, rule and user, plus interviews |
| Proof | Experience and opinion | Every pound traced to a calculation anyone can check |
| Speed | 6 to 8 weeks | About a week from connect to readout |
| Benchmarks | Anecdotal | Measured, from real orgs |
| Afterwards | A new statement of work | A 90-day re-check that proves what changed |

We keep the human layer (interviews, judgement, the live readout) and put machine depth underneath it.

## Who it's for

UK companies with 20 to 500 Salesforce users: big enough to have invested in the platform, and to invest in getting it right. Global later.

## The offer

| | Barometer Scan | Barometer Essentials | Barometer Complete |
| --- | --- | --- | --- |
| Price (proposed) | Free | About £7,500 | £18,000 to £22,000 |
| What we read | Set-up only | Set-up only | Set-up, records and usage |
| What they get | Their score and a handful of real findings; every other area shown as locked, with the real count of issues we found in it | The set-up report: automation, security, technical health, licences, permissions, and their org documented | Everything: the money model, benchmarks, every area in depth, interviews, the user survey, the microsite, and a live readout |
| 90-day re-check | No | No | Included |
| Purpose | Brings in leads | The way in for cautious buyers | The flagship |

**Barometer Ongoing** follows either paid check: a 12-month programme paid monthly (proposed £1,500 to £2,000 a month). Each quarter there's a full re-check reviewed by a consultant, with a short readout. Each month there's an automated pulse: the score, and anything that got worse. Monthly full checks would be noise, because orgs don't change that fast and each one needs a consultant. Quarters give a story the customer can tell their board.

**Where the revenue really comes from.** Each Complete check ends with a costed 90-day plan, and that plan is Black Cloud's next piece of work. We track how many checks turn into projects. Proposed: part of the check fee is credited against fix work booked within 60 days (to decide).

## Stage 1: Money, the score and references

The single biggest jump. A CEO reads numbers, so the executive deck becomes a money document.

### 1.1 References and numbered raw notes (done, 5 November 2026)

- The raw notes are laid out as numbered sections and items: `R3` is the third area, and `R3.4` is the fourth item in it. Each item holds the inputs, the formula, the sample, the assumptions and, where useful, the named records.
- Every figure in the decks carries its reference in small type. The last page of each deck, "Where to find the detail", lists the references with their page in the raw notes.
- Named records (the actual deals, accounts and users) appear in the raw notes and in exports, never in the decks.

### 1.2 The Barometer score (done, 5 November 2026)

One number out of 100 with a grade, built from the area scores the engine already calculates, weighted by business impact. Shown on the cover, with the area breakdown inside, and later against benchmarks.

### 1.3 The investment: what they pay

- Read seat counts by licence type (`UserLicense`, `PermissionSetLicense`), the edition, and installed packages with their seats.
- Price them from a list-price table we keep, overridden by what the customer tells us they pay (decision 4).
- Show what they pay, what they use, and what they pay for and don't use.

### 1.4 The value model: every issue gets a figure

| Kind | Example | Method |
| --- | --- | --- |
| Revenue recoverable | Stalled deals, dormant accounts, win-backs, unworked leads | Value at the org's own historical conversion rates |
| Conversion uplift | Deals with more activity win more often | Measured from their own won and lost deals, labelled as potential based on their data, with the sample shown (decision 10) |
| Revenue at risk | Forecast misses, slippage, commit at risk | From forecast history and close-date behaviour |
| Licence waste | Unused and inactive seats, unused paid apps | Seats × price from 1.3 |
| Time cost | Manual work, duplicates, admin debt | Hours × a loaded hourly cost (decision 3) |
| Risk exposure | Security gaps, single points of failure | A rated risk with the reason, not a pound figure |

The headline is one annual expected figure, with the low-to-high range beneath it, over 12 months (decision 9). The expected case is the conservative one, and overlapping figures are never added together.

### 1.5 The executive deck, rebuilt around money

About 12 pages: the cover with the score; the number, as a waterfall by kind; their investment; the five biggest leaks; how they compare; adoption; risk; what fixing it is worth over 12 months; the 90-day plan; the return on fixing it; what happens next; and where to find the detail.

### 1.6 How they compare (first version, done 5 November 2026)

Published research for the headline figures, labelled with its source, combined with our own figures as they arrive (decision 2). The comparison page shows where they sit: bottom quarter, middle half or top quarter, with the median and the group used.

## Stage 2: Intake, Essentials and housekeeping

### 2.1 Intake from the discovery call

1. When a consultant creates a run, Barry creates a Slack canvas for the discovery call with the intake questions, grouped as a call script.
2. The consultant runs the call.
3. The consultant pastes the call notes into the console. Claude matches them to the questions and marks each as answered (with the line from the notes it came from), partly answered or not answered. The consultant confirms.
4. Whatever is still unanswered goes onto the customer's connect page.

### 2.2 Essentials mode

A run scope that reads set-up only and skips records and usage. It reuses the engine as it is, makes the Essentials report, and is the base for the free scan.

### 2.3 Keeping data only as long as we need it

A scheduled clean-up (decision 16): raw extracts deleted 30 days after release; findings and documents kept 12 months; benchmark rows kept, since they're anonymous. It's also a strong answer to customers' security questionnaires.

## Stage 3: Depth, towards about 100 pages

### 3.1 Every area gets a chapter

The same shape each time: what we looked at, what's good, what's below par, what's broken, what it costs, what best practice says (cited), how you compare, and what to do, in order.

### 3.2 New areas and checks

| Area | Examples |
| --- | --- |
| Adoption and usage | Logins by role and team, feature use, daily and weekly active users, seats paid for and unused |
| Process | The real sales process from stage history (skipped stages, deals going backwards, time in stage), as a flow, against the designed process |
| Data model | Real duplicates, field fill rates per object, record type sprawl, orphaned records |
| Integrations | Connected apps, API users and limits, what writes to what, failing integrations |
| Technical health | Apex test coverage and quality, governor-limit risk, Classic leftovers, API versions |
| Permissions | Profile and permission set sprawl, access reviews, least privilege |
| Packages | Installed apps, their seats and use, overlap |
| Storage and limits | Data and file storage, growth rate, cost of growth |
| Territory and quotas | Coverage, balance, quota against pipeline |

Rep-level insights are team-level by default, with named reps only when the customer opts in at intake (decision 7).

### 3.3 Root causes

Findings are linked into causes, not just listed as symptoms: for example, "your win rate is low because deals without a next step lose three times as often".

### 3.4 Your org, documented

The data model as a diagram, the automation on each object in the order it runs, the integrations, the users, roles and profiles, and the security model. Useful long after the report is read.

### 3.5 Benchmarks, filled in

Industry and size groups as the set reaches 10 orgs per group, and new figures as the money model lands: spend per user, active-user share, licence waste share, revenue per user.

## Stage 4: The experience

- **The microsite.** Each Complete run gets its own private site: the score, the money, every finding, and a drill-down from each figure to its working, with exports. Customers sign in with a link sent to their email address. It stays up for 12 months. The Google Drive folder is shared with the customer as well.
- **The people inputs.** Stakeholder interviews: four or five structured 30-minute conversations (for example the sales director, sales ops, the admin and two reps), recorded as notes the report draws on. The user survey: five minutes, anonymous, sent to every Salesforce user (how easy it is, what slows them down, how many hours a week go on admin). The report then sets what people say against what the data shows, and the survey's hours feed the time cost.
- **The readout kit.** A presentation version of the deck and talking points for the consultant.
- **Design.** A designer's pass on the page system and charts once Stage 1 sets the shape.
- **Action lists.** Exports of every record that needs fixing, ready for their team or a ticketing tool.

## Stage 5: Proof and growth

- **The 90-day re-check.** Run again and show the change in money, adoption and the score. Needed by early February 2027 for the first three customers.
- **Barometer Ongoing.** Quarterly re-checks and the monthly pulse.
- **Case studies.** "£X recovered, adoption up Y%", with consent recorded at intake.
- **The free scan.** A page on the Black Cloud site: connect read-only, a set-up scan, a results page showing the score and a few real findings with the other areas locked, and a lead alert to Slack.
- **Partners.** Referral partners from the start of this stage; no white-labelling for at least 12 months (decision 13).
- **A one-page CEO summary,** built to be forwarded.
- **"State of Salesforce".** A yearly report from the anonymised benchmark set.
- **AppExchange.** Decided at the end of this stage, once the free scan shows how well it converts.

## Stage 6: Beyond Sales Cloud

Service Cloud, CPQ and Revenue Cloud, Marketing Cloud and Account Engagement, Experience Cloud, and readiness for Data Cloud and Agentforce.

## Running throughout

- **Quality gates.** The number check, the layout check, no automated tells in the wording, a second consultant's review before release, and sample decks re-rendered and compared on every change.
- **Who runs it.** Black Cloud consultants, signed in with Google on a black-cloud.com address.
- **Speed.** From connect link to readout in about a week, with the run itself under a day.

## Decisions

### 5 November 2026 (Coby)

1. **Seeding benchmarks:** ask past and current customers to consent to contributing their anonymised, banded figures, with one read-only run each. Three orgs are expected within days.
2. **Published research:** combined with our own figures as they arrive, always labelled with its source. Our own figures appear as a "Black Cloud customers" range from 5 orgs, with the count shown, and never below: at 3 orgs the middle value is one company's real figure, which another customer could read as theirs (rule 11). Industry and size groups appear from 10 orgs (`BENCHMARK_MIN_GROUP`).
3. **Hourly cost:** asked on the connect page, with a cited UK salary benchmark by role as the fallback. Their figure wins.
4. **Salesforce spend:** an optional connect-page question, with list price for their seats and edition as the fallback, labelled as list price.
5. **Customers:** UK, 20 to 500 Salesforce users.
6. **Price:** the full check at around £15,000 to £20,000, with a cheaper set-up-only check. Two paid options plus a free scan, and a recurring option to be shaped (proposal above).
7. **Readout:** every Complete check has a live readout with a consultant.
8. **Score:** a single headline score.
9. **Microsite:** one per run, and the Drive folder shared with the customer.
10. **Named records:** in the raw notes and exports only. The decks carry numbered references to the raw notes.
11. **Rep-level insights:** team-level by default, named with the customer's consent at intake.
12. **Money:** one annual expected figure as the headline, a low-to-high range beneath, 12 months. Correlation-based uplift is shown, labelled as potential based on their own data.
13. **Partners:** exclusive for now.
14. **Who runs checks:** Black Cloud consultants signed in with Google.
15. **The first three orgs:** paying Black Cloud customers, given the check at no charge, and likely to agree to be case studies.
16. **Retention:** raw extracts deleted 30 days after release, findings and documents kept 12 months, benchmark rows kept.

### 5 November 2026, later (Coby)

17. **Prices:** Essentials about £7,500, Complete £18,000 to £22,000, Ongoing £1,500 to £2,000 a month, as proposed. The fee credit against fix work is still open.
18. **Free scan:** each scan waits for a one-tap approval from a Black Cloud consultant in Slack before it runs.
19. **Call notes to Claude:** names we know (the customer's contacts, Salesforce users and the company) are swapped for labels before the notes go to Claude, and put back in the answers, as the pseudonymiser does for everything else. The consultant is asked to write roles, not names.
20. **The intake canvas:** shared with the customer through Slack Connect, so they can answer some questions themselves. It reads as Black Cloud's, under the consultant's name, with nothing saying it was generated.
21. **The microsite:** named content is stored on our side, encrypted and deleted after 12 months, and customers sign in through a link emailed to approved addresses.
22. **The order of the stages:** as written above.
