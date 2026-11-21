// The rules that don't yet write their sentences with said and org (DECISIONS.md
// 655, 657), held by org-words-coverage.test.ts and the digest's test. This list
// may only shrink, as rules are converted.

export const UNCONVERTED_RULES: ReadonlySet<string> = new Set<string>([
  "act.deals-without-activity", "act.event-link-collapse", "act.rep-activity-spread", "act.unlinked-tasks", "act.won-vs-lost-activity",
  "aut.apex-coverage", "aut.case-routing-overlap", "aut.old-api-versions", "cpq.discount-approvals", "cpq.discount-leakage",
  "cpq.inactive-rules", "cpq.price-rule-conflicts", "cpq.quote-to-order", "cpq.renewals-not-started", "cpq.twin-field-mismatch",
  "cur.account-concentration", "cur.close-date-pushes", "cur.coverage", "cur.missing-next-steps", "cur.rep-concentration",
  "cur.stale-deals", "cur.stuck-in-stage", "fct.commit-at-risk", "fct.early-stage-commits", "fct.forecast-accuracy",
  "fct.forecasting-in-use", "fct.manager-adjustments", "fct.quota-gaps", "int.api-usage", "int.failed-async-jobs",
  "int.hardcoded-endpoints", "int.integration-user-licences", "int.integration-users-interactive", "lead.campaign-return", "lead.conversion-by-source",
  "lead.converted-no-opportunity", "lead.no-first-response", "lead.response-time", "lead.source-return", "mkt.campaign-influence",
  "mkt.scoring-grading", "mkt.unowned-prospects", "mkt.unworked-handovers", "pipe.calibration-contradicts-org", "pipe.dead-end-stage",
  "pipe.going-backwards", "pipe.segment-divergence", "pipe.skipped-stages", "pipe.unmapped-stages", "rev.dormant-accounts",
  "rev.stalled-deals", "rev.win-backs", "sec.health-check-risks", "sec.health-check-score", "sec.insecure-remote-sites",
  "sec.modify-all-data", "sec.peer-outlier-access", "sec.personal-integration-accounts", "sec.self-authorised-connected-apps", "sec.sharing-defaults",
  "sec.unused-access", "sec.view-all-data", "svc.agent-licences", "svc.backlog-age", "svc.backlog-growth",
  "svc.case-classification", "svc.case-customer", "svc.escalation-rules", "svc.escalation-targets", "svc.first-reply-email",
  "svc.knowledge-current", "svc.knowledge-use", "svc.macros-and-quick-text", "svc.milestone-breaches", "svc.omni-routing",
  "svc.origin-missing", "svc.priority-resolution", "svc.queue-sprawl", "svc.reopened-cases", "svc.unassigned-cases",
  "tech.apex-code-size", "tech.classic-leftovers", "tech.governor-limit-risk", "tech.storage-use", "tech.test-quality",
  "utl.dashboard-usage", "utl.dashboards-inactive-running-user", "utl.dormant-licensed-users", "utl.eac-advanced-sync", "utl.eac-connections",
  "utl.never-logged-in", "utl.package-seats", "utl.unassigned-licence-seats", "utl.unused-reports",
]);
