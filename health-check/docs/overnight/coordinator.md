# Coordinator log

## 21:30 first integration

- Merged `hc/domains` (to c2e7fd1) and `hc/reports` (651d0cb) into `hc/integration`. No conflicts.
- Type check clean. Tests 857 of 862. The 5 failures only appear once both branches are together.
- Read-only boundary reviewed: `allowlist.ts`, `client.ts`, `soql.ts`, schemas, guards and privacy are unchanged on both branches. The only frozen-file change is a small additive scoring hook in `run.ts`.
- Sent to Worker A: scoring gives 0 to any domain with no passes, whatever the severity.
- Sent to Worker B: an ordinary org falls to compact tables and loses issue summaries, plus 4 tests resting on old assumptions, plus the design system's neutrals.
