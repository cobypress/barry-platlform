// The only words a closed stage's name may hold to be sent to a model
// (DECISIONS.md 766). The rule is an allow rule, privacy over coverage: a stage
// goes only if every word of both its label and its API name, normalised as the
// word rule normalises them (camel case split, lower case, anything but a letter
// or digit a space), is on this list, or is a number from 0 to 99 or a year from
// 2000 to 2099. Anything else, so every company, surname, given name, place,
// initialism not here, longer number and amount, keeps the stage at home and
// undecided. This is the one place to extend it, after a review: a word that is
// also a common name (a given name, a surname, a place) doesn't belong here, and
// month names are left out for that reason (April, June, August, May), as are
// surnames that are also business words (Cross, Day, House, Little, Park, Small).
// Kept on purpose, though also surnames, because no stage list reads without
// them: close, deal, price, sale. "general" and "channel" are left out as
// parts of company names (Legal & General, Channel 4).
// English only. A test snapshots the list.

/** The list as written, each word once (a test holds that). */
export const STAGE_WORDS: readonly string[] = [
  // Outcomes and stage states
  "closed", "close", "closing", "lost", "lose", "loss", "losses", "won", "win", "wins", "open", "opened", "reopen", "reopened", "parked", 
  "hold", "held", "pending", "paused", "pause", "stalled", "stall", "stuck", "abandoned", "dead", "inactive", "active", "cold", "warm", "dormant",
  "deferred", "defer", "postponed", "delayed", "delay", "nurture", "nurturing", "recycled", "recycle", "disqualified", "unqualified", "qualified",
  "qualification", "qualify", "rejected", "reject", "declined", "decline", "churned", "churn", "expired", "expiry", "cancelled", "canceled",
  "cancel", "withdrawn", "withdrew", "duplicate", "duplicated", "error", "invalid", "void", "merged", "converted", "lapsed", "terminated", "ended",
  "gone", "quiet", "ghosted", "unresponsive", "silent", "dropped", "outcome", "result", "decision", "decided", "undecided", "response", "responded",
  "reply", "replied", "answer", "answered", "unreachable", "reachable", "uncontactable", "stopped", "finished", "complete", "completed", "done",
  // Reasons
  "budget", "budgets", "funding", "funded", "unfunded", "price", "pricing", "priced", "cost", "costs", "expensive", "cheaper", "discount", "value",
  "roi", "legal", "procurement", "purchasing", "finance", "financial", "security", "compliance", "risk", "review", "reviewed", "approval",
  "approved", "unapproved", "contract", "contracts", "contracted", "terms", "renewal", "renewals", "renew", "renewed", "timing", "time",
  "timeline", "timescale", "priority", "priorities", "deprioritised", "deprioritized", "fit", "requirements", "requirement", "needs", "need",
  "interest", "interested", "uninterested", "engagement", "engaged", "disengaged", "capacity", "bandwidth", "resource", "resources", "scope",
  "change", "changes", "changed", "reorganisation", "reorganization", "restructure", "restructuring", "merger", "acquisition", "acquired",
  "insolvent", "insolvency", "bankrupt", "bankruptcy", "closure", "freeze", "frozen", "hiring", "headcount", "size", "large", "big",
  "lack", "lacking", "missing", "insufficient", "without", "too", "much", "feature", "features", "functionality", "product",
  "products", "service", "services", "solution", "solutions", "integration", "technical", "technology", "tech", "internal", "external",
  "inhouse", "build", "built", "own", "diy", "chose", "chosen", "selected", "another", "elsewhere", "alternative", "incumbent",
  "competitor", "competitors", "competition", "competitive", "status", "quo",
  // People by role, never by name
  "champion", "sponsor", "stakeholder", "stakeholders", "executive", "exec", "buyer", "buyers", "customer", "customers", "client", "clients",
  "prospect", "prospects", "lead", "leads", "account", "accounts", "contact", "contacts", "maker", "makers", "owner", "board",
  "management", "leadership", "team", "teams", "user", "users", "partner", "partners", "vendor", "vendors", "supplier", "suppliers", "reseller",
  "resellers", "distributor", "direct", "indirect", "sales", "sale", "sold", "marketing", "ops", "operations", "it", "hr", "admin",
  // The sale's steps
  "trial", "pilot", "poc", "demo", "demos", "proposal", "proposals", "quote", "quoted", "quotes", "negotiation", "negotiating", "negotiate",
  "verbal", "signed", "signature", "sign", "unsigned", "order", "ordered", "purchase", "purchased", "deal", "deals", "opportunity",
  "opportunities", "opp", "opps", "pipeline", "stage", "stages", "process", "project", "projects", "follow", "followup", "up", 
  "evaluation", "evaluating", "assessment", "discovery", "onboarding", "implementation", "rollout", "upsell", "new", "existing",
  "business", "logo", "expansion", "downgrade", "upgrade", "migration",
  // Time
  "quarter", "quarters", "quarterly", "q1", "q2", "q3", "q4", "h1", "h2", "year", "years", "yearly", "annual", "month", "months", "monthly",
  "week", "weeks", "weekly", "days", "next", "last", "this", "that", "these", "those", "current", "prior", "later", "future", "revisit",
  "revisited", "again", "soon", "now", "today", "end", "start", "fiscal", "fy", "season", "seasonal", "holiday", "holidays",
  // Small words
  "a", "an", "the", "to", "of", "for", "by", "on", "in", "at", "with", "from", "into", "out", "off", "over", "under", "after", "before",
  "due", "per", "via", "and", "or", "but", "nor", "as", "is", "was", "are", "were", "be", "been", "being", "has", "had", "have", "all", "any",
  "some", "other", "others", "more", "less", "than", "then", "until", "yet", "still", "only", "just", "so", "because", "if", "when", "no",
  "not", "none", "never", "non", "yes", "tbd", "na", "misc", "unknown", "reason", "reasons", "see", "notes",
];

export const STAGE_VOCABULARY: ReadonlySet<string> = new Set(STAGE_WORDS);

/** Whether one normalised word may go: on the list, a number from 0 to 99, or a year from 2000 to 2099. */
export const allowedStageWord = (word: string): boolean => STAGE_VOCABULARY.has(word) || /^\d{1,2}$/.test(word) || /^20\d\d$/.test(word);
