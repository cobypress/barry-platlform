# Roadmap: the best Salesforce health check on the market

Written with Coby on 2026-11-05. A living plan: phases are in order of the value they add, not a schedule. Each phase ends in something a customer can see.

## What "a 10" means

A customer finishes the report knowing they got more than they paid for, and tells another CEO about it. In practice:

- **Every pound is counted.** The executive summary says what they spend on Salesforce, how much money is being missed, where, and how much they could make, and every figure points to the page of the raw notes that shows the working.
- **Nothing is left unexamined.** The full report runs to about 100 pages and goes through every part of the org: what's good, what's below par, what's broken, what it costs, what best practice says, and how they compare with similar companies.
- **It's theirs.** It answers what they told us, in their terms, with their goals, and reads as written by a consultant who knows their business.
- **It proves itself.** We measure again after 90 days and show what changed, in money.

The rules that make it trustworthy don't change: code calculates and Claude explains, every figure traces to evidence, nothing is estimated where the data can't support it, and every assumption is written down.

## Where we are (5 November 2026)

| | Today |
| --- | --- |
| Coverage | About 75 checks in 12 areas, Sales Cloud only |
| Money | Revenue, licence cost or hours on some issues; most issues show a risk level instead |
| Benchmarks | Opt-in exporter built (about 25 figures, banded, no names). One org in the local store; nothing collected in production; no comparison page |
| Documents | Executive summary (about 10 pages), full report (about 40), raw notes (Word). Claude drafts the prose; a consultant approves every section |
| Delivery | Connect link, customer questions, calibration, review, release, documents to Drive |

## Phase 1: Money (start here)

The single biggest jump. A CEO reads numbers, so the executive summary becomes a money document.

### 1.1 The investment: what they pay

- Read seat counts by licence type (`UserLicense`, `PermissionSetLicense`), the edition, and installed packages with their seats.
- Price them from a list-price table we keep, by edition and licence, overridden by what the customer tells us they actually pay (a new connect-page question: annual Salesforce spend, optional).
- Show it as: what you pay, what you use (active seats, features in use), and what you pay for and don't use.

### 1.2 The value model: every issue gets a figure

One model with six kinds of value, each with its own method, written up in the raw notes:

| Kind | Example | Method |
| --- | --- | --- |
| Revenue recoverable | Stalled deals, dormant accounts, win-backs, unworked leads | Value at the org's own historical conversion rates (already used for stalled deals) |
| Conversion uplift | Deals with more activity win more often | Measured from their won and lost deals: if the bottom quartile performed at the median, this much more would have been won. Only where the link is measured, with the sample shown |
| Revenue at risk | Forecast misses, slippage, commit at risk | From forecast history and close-date behaviour |
| Licence waste | Unused and inactive seats, unused paid apps | Seats × price from 1.1 |
| Time cost | Manual work, duplicates, admin debt, slow processes | Hours × a loaded hourly cost by role (customer's figure, or a cited UK salary benchmark) |
| Risk exposure | Security gaps, single points of failure | Not a pound figure: a rated risk with the reason, as today |

Each figure has a low, expected and high case, the expected case is the conservative one, and overlapping figures are never added together (the model knows which ones overlap).

### 1.3 References

Every figure in the executive summary and the full report carries a short reference, such as `[R12]`, that matches a numbered calculation in the raw notes: the inputs, the formula, the sample, and the assumptions.

### 1.4 The executive deck, rebuilt around money

About 12 pages:

1. Cover
2. **The number**: "We found £X a year being missed", as a waterfall by kind
3. **Your investment**: what you pay, and how much of it is working
4. **Where the money is going**: the five biggest leaks, each with its figure and reference
5. **How you compare**: benchmarks against similar companies (Phase 2)
6. **Adoption**: who uses Salesforce and how, and what that costs
7. **Risk**: what could hurt you, unpriced but ranked
8. **The opportunity**: what fixing it is worth over 12 months, as a ramp
9. **The plan**: 90 days, with the value each phase unlocks
10. **Return on fixing it**: value against effort
11. What happens next
12. How we worked it out (pointing to the raw notes)

## Phase 2: Benchmarks

### 2.1 Collect (now, needs no decisions)

- The worker writes a consenting run's benchmark figures to `HealthCheckBenchmarkRow` when the run is released (consent is already asked at intake).
- The exporter's rules stand: whole-org figures only, rounded, banded, no names, org ids or dates beyond the quarter.

### 2.2 Compare

- A comparison is shown only when its group has at least 10 orgs (`BENCHMARK_MIN_GROUP`).
- Groups widen until there are enough: industry and size, then industry, then size, then every Black Cloud customer. The page says which group was used and how many orgs are in it.
- Shown as where they sit: bottom quarter, middle half or top quarter, with the median.
- New figures join the benchmark list as the money model lands: spend per user, active-user share, licence waste share, revenue per user.

### 2.3 Seed

10 orgs per group takes time. Until then:

- Past and current Black Cloud customers, with their written consent, run once and contribute their figures.
- Published research for a handful of headline figures (win rate, sales cycle, CRM adoption), always labelled as published research with the source, never mixed with our own data.

### 2.4 Use it to sell

Once there are enough orgs: a yearly "State of Salesforce" report from the anonymised set. It markets the health check and gives CEOs something to pass around.

## Phase 3: Depth (rolling, towards about 100 pages)

### 3.1 Every area gets a chapter

The same shape each time:

1. What we looked at
2. What's good
3. What's below par
4. What's broken
5. What it costs
6. What best practice says (cited)
7. How you compare
8. What to do, in order

### 3.2 New areas and checks

| Area | Examples |
| --- | --- |
| Adoption and usage | Logins by role and team, feature use (reports, mobile, Chatter, Lightning), daily and weekly active users, who's paying for a seat they don't use |
| Process | The real sales process from stage history (stage skipping, going backwards, time in stage), as a flow diagram, against the process they designed |
| Data model | Real duplicates (not just whether a rule exists), field fill rates per object, record type sprawl, orphaned records |
| Integrations | Connected apps, API users and limits, what writes to what, failing integrations |
| Technical health | Apex test coverage and quality, governor-limit risk, Visualforce and Classic leftovers, API versions, release readiness |
| Permissions | Profile and permission set sprawl, access reviews, least privilege |
| Packages | Installed AppExchange apps, their seats and use, overlap |
| Storage and limits | Data and file storage, growth rate, cost of growth |
| Territory and quotas | Coverage, balance, quota against pipeline |

### 3.3 Your org, documented

A section consultants usually charge separately for: the data model as a diagram, the automation on each object in the order it runs, the integrations, the users, roles and profiles, and the security model. It's useful long after the report is read.

### 3.4 Visuals

Process flow diagrams, an automation map per object, adoption heatmaps, a benchmark position for every figure, and value waterfalls.

### 3.5 The people side

An optional short survey sent to the customer's users (five minutes): how they rate Salesforce, what slows them down. The report then sets what the team says against what the data shows, which no tool-only check can do.

## Phase 4: Proof and growth

- **The 90-day check.** We run again and show the change in money and adoption against the first run (baselines already exist in the engine). This is what turns a report into a result a CEO repeats.
- **Case studies.** With consent, the outcome in figures: "£X recovered, adoption up Y%".
- **A one-page CEO summary.** The number, the comparison, the plan. Built to be forwarded.
- **Packaging.** A one-off health check, a full check with a workshop, and a quarterly Barometer subscription that re-runs and tracks progress.
- **Referral.** A referral ask at the 90-day check, when the value is proven.

## Phase 5: Beyond Sales Cloud

Service Cloud, CPQ and Revenue Cloud, Marketing Cloud and Account Engagement, Experience Cloud, and readiness for Data Cloud and Agentforce.

## Running throughout

- **Quality gates.** The number check, the layout check, no automated tells in the wording, a second consultant's review before release, and sample decks that are re-rendered and compared on every change.
- **Design.** A designer's pass once Phase 1 sets the shape: a consistent page system, charts that read at a glance.
- **Speed.** From connect link to documents in under a week, with the run itself under a day.

## Decisions (Coby, 5 November 2026)

1. **Seeding benchmarks:** yes. Ask past and current customers to consent to contributing their anonymised, banded figures, with one read-only run each. Three orgs are expected within days of this decision.
2. **Published research:** yes, combined with our own figures as they arrive. Published figures are always labelled with their source. Our own figures appear as a "Black Cloud customers" range from 5 orgs, with the count shown, and never below: at 3 orgs the middle value is one company's real figure, which another customer could read as theirs (rule 11). Industry and size groups appear from 10 orgs (`BENCHMARK_MIN_GROUP`).
3. **Hourly cost:** ask the customer on the connect page, with a cited UK salary benchmark by role as the fallback. Their figure wins.
4. **Salesforce spend:** an optional connect-page question, with list price for their seats and edition as the fallback, labelled as list price.
