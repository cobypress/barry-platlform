# Lessons

One row per change to the playbook (`docs/orchestration.md`, `.claude/agents/*`, `.claude/skills/coordinate`, `scripts/orchestrate-*`). Tighten freely; loosening needs Coby.

| Date | Event that taught it | Change | File |
| --- | --- | --- | --- |
| 2026-11-10 | Three lanes started together, each briefed to record its own decision in `docs/DECISIONS.md`: three rows appended at the same place would conflict at every merge | The orchestrator writes and commits the lane decisions before briefing; builders don't edit the file unless the brief says to | `.claude/skills/coordinate/SKILL.md`, `.claude/agents/builder.md` |
| 2026-11-10 | A builder added a required field to a hand-built brief in `src/narrative/__tests__/routes.test.ts`, the privacy test that holds every route to Claude through the guard. Type-only, nothing weakened, but the guard didn't lock that file and the builder's rule named no files | The guard locks `routes.test.ts`; the builder's rule names the files and says "not even to satisfy a type" | `scripts/orchestrate-guard.ts`, `scripts/__tests__/orchestrate-guard.test.ts`, `.claude/agents/builder.md` |
