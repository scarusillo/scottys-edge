# Shadow Factors — Disabled Context Adjustments

Disabled on 2026-03-29 (v21). These factors are still calculated and recorded
in `context_factors` with a `[SHADOW]` prefix, but their point adjustments are
zeroed out so they do not influence picks.

## Disabled Factors

| # | Factor | Record | Units | Location in context_engine.py |
|---|--------|--------|-------|-------------------------------|
| 1 | Home fast-paced | 3W-7L | -22.4u | Section 7 (pace_of_play_adjustment) — only home_pace > 0 is shadowed |
| 2 | Away bounce-back | 3W-4L | -11.5u | Section 6 (motivation_adjustment) — away_bounceback key |
| 3 | Altitude | 2W-3L | -6.1u | Section 5 (altitude_adjustment) — all altitude adjustments |
| 4 | Home hot streak | 1W-2L | -5.4u | Section 11 (_recent_form_adjustment) — entire form factor is shadow-only |
| 5 | Away revenge game | 5W-5L | -4.4u | Section 6 (motivation_adjustment) — away_revenge key |

## What Remains Active

- **Home bounce-back** — still applied (separate from away bounce-back)
- **Home revenge** — still applied (separate from away revenge)
- **Letdown spots** — still applied (both home and away)
- **Home slow-paced** — still applied (only home fast is shadowed)
- **Away pace (fast or slow)** — still applied
- **All other context factors** (travel, refs, H2H, familiarity, weather, etc.)

## How to Query Shadow Performance

Look for `[SHADOW]` in the `context_factors` column of `graded_bets`:

```sql
SELECT result, pick, context_factors, units
FROM graded_bets
WHERE context_factors LIKE '%[SHADOW]%'
ORDER BY date DESC;
```

To check if a shadow factor would have improved a pick:

```sql
SELECT
  result,
  SUM(CASE WHEN result='W' THEN units ELSE -units END) as net_units,
  COUNT(*) as picks
FROM graded_bets
WHERE context_factors LIKE '%[SHADOW] Home fast-paced%'
  AND date >= '2026-03-29';
```

## Re-enabling a Factor

To re-enable a shadow factor, remove it from the `SHADOW_MOTIVATION` set or
reverse the shadow logic in `get_context_adjustments()` in context_engine.py.
Each shadow section has a comment marking it.
