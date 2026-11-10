# Lessons

One row per change to the playbook (`docs/orchestration.md`, `.claude/agents/*`, `.claude/skills/coordinate`, `scripts/orchestrate-*`). Tighten freely; loosening needs Coby.

| Date | Event that taught it | Change | File |
| --- | --- | --- | --- |
| 2026-11-10 | Three lanes started together, each briefed to record its own decision in `docs/DECISIONS.md`: three rows appended at the same place would conflict at every merge | The orchestrator writes and commits the lane decisions before briefing; builders don't edit the file unless the brief says to | `.claude/skills/coordinate/SKILL.md`, `.claude/agents/builder.md` |
