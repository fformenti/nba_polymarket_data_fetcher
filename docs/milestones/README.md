# Project Milestones

NBA Polymarket Data Fetcher — planning index.

## Legend

| Symbol | Meaning |
|--------|---------|
| `- [x]` | Task complete |
| `- [ ]` | Task not started |
| `🚨 BLOCKER` | Other tasks depend on this — do first |
| `⚠️` | Implemented but needs review/fix |

## Progress Overview

| # | Milestone | Status | Est. Complete |
|---|-----------|--------|---------------|
| M1 | [Core Infrastructure](M1-core-infrastructure.md) | ✅ Complete | — |
| M2 | [Data Fetchers](M2-data-fetchers.md) | ✅ Complete | — |
| M3 | [Pipeline & Orchestration](M3-pipeline-orchestration.md) | ✅ Complete | — |
| M4 | [Data Quality & Reliability](M4-data-quality.md) | 🟡 In Progress (~90%) | — |
| M5 | [NBA Historical Coverage](M5-nba-historical-coverage.md) | ✅ Complete | — |

## Dependency Chain

```
M1 (Infrastructure)
  └─▶ M2 (Fetchers)
        └─▶ M3 (Pipeline)
              └─▶ M4 (Quality)
                    └─▶ M5 (Coverage)
```

Each milestone's implementation tasks can only safely begin once the blocking milestone is complete.

## Active Blockers (update as resolved)

None.

## Files

| File | Coverage |
|------|----------|
| `src/polymarket/client.py` | Spec: `.claude/specs/client.md` |
| `src/polymarket/models.py` | Spec: `.claude/specs/models.md` |
| `src/polymarket/fetchers/` | Spec: `.claude/specs/fetchers.md` |
| `src/polymarket/storage/` | Spec: `.claude/specs/storage.md` |
| `src/polymarket/pipeline.py` | Spec: `.claude/specs/pipeline.md` |
