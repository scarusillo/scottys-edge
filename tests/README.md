# Tests — `generate_predictions()` refactor harness (v26.0)

This directory holds the safety net for the v26.0 pipeline refactor.

## Why this exists

`scripts/model_engine.py:generate_predictions()` is a 2,190-line god-function. It's being decomposed into testable pipeline stages (Fetch → Score → Gate → Channels → Route → Merge). Every refactor phase must produce **byte-equivalent** picks vs. a captured baseline on the same input — otherwise we have a regression and roll back.

## Files

```
tests/
├── shadow_predict.py     # Capture / replay / determinism CLI
├── golden/
│   ├── baseline/         # Captured before any refactor (the source of truth)
│   ├── _det_run1/        # Determinism check artifacts (transient)
│   └── _det_run2/        # Determinism check artifacts (transient)
└── README.md
```

## CLI

```bash
PYTHONIOENCODING=utf-8 python tests/shadow_predict.py capture --label baseline
PYTHONIOENCODING=utf-8 python tests/shadow_predict.py replay  --label baseline
PYTHONIOENCODING=utf-8 python tests/shadow_predict.py determinism
```

## Phase 0 acceptance criteria

Phase 0 is "done" when:

1. `capture --label baseline` writes a non-empty golden for every active sport
2. `determinism` returns 0 diffs (function is replay-stable)
3. `replay --label baseline` immediately after capture returns 0 diffs

If determinism check shows diffs, the function has hidden non-determinism (datetime calls, dict iteration order, time-of-day branching). Those must be addressed before Phase 1 begins, otherwise every refactor phase will produce false-positive diffs.

### Known limitation: wall-clock drift across captures

The harness freezes the DB (via `_snapshot.db`) but does NOT freeze `datetime.now()`. The function uses the wall clock for the "today only" game window — `window_start = now + 30 min`. If you capture a baseline and then replay an hour later, games whose commence_time is now in the past (or within 30 min of now) will be filtered out by the live run but were included in the original baseline.

This shows up as `removed` picks in the diff that are entirely in-progress / about-to-start games.

**Workaround for now:** capture the baseline IMMEDIATELY before the refactor change you're about to make. Don't reuse a baseline captured more than ~30 min before the planned replay. Determinism checks (which capture twice within seconds) are not affected.

**Proper fix (TODO):** monkey-patch `datetime.now()` in the harness so replays use the same wall-clock as the original capture. Will require either a context manager or an env var the function reads. Punt until needed for Phase 3+ where multi-day replay starts to matter.

## Phase advance gates

After each refactor phase:

```bash
# 1. Re-run capture to refresh post-phase output
python tests/shadow_predict.py capture --label phase{N}

# 2. Compare phase{N} vs baseline
python tests/shadow_predict.py replay --label baseline
```

Gate is **0 diffs**. Any non-zero count = regression = revert that phase.

## What "byte-equivalent" means here

Compared fields per pick:
- `sport, event_id, market_type, selection, book` (identity)
- `line, odds, units, edge_pct, side_type, market_tier, confidence` (rounded to 2 decimals)
- `context_factors` (string, normalized whitespace)

Ignored:
- timestamps, db ids, derived/display fields, grading outputs (clv, closing_line, result)

If a pick's `units` shifts by 0.01u or its `edge_pct` rounds differently after a refactor, the harness treats that as a real diff. Tolerance is intentionally tight.
