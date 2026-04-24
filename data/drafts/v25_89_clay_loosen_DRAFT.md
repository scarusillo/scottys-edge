# DRAFT — v25.89 clay filter loosening (NOT SHIPPED)

Prepared 2026-04-24 evening for tomorrow's DIVERGENCE_GATE review.

**Ship criteria (per agent_todo):**
- Retroactive grade on all Apr 21+ blocked clay picks expands sample to n≥8
- Combined WR holds ≥ 70%
- Rome R32 sample also supports

**If triggers hit — three changes:**

## Change 1: `scripts/model_engine.py:180`

```diff
-        'clay': {'logistic_scale': 2.5, 'spread_std': 5.5, 'home_court': 0.0,
-                 'max_spread_divergence': 2.5, 'ml_scale': 2.5},  # v24: tightened from 4.5
+        'clay': {'logistic_scale': 2.5, 'spread_std': 5.5, 'home_court': 0.0,
+                 'max_spread_divergence': 3.5, 'ml_scale': 2.5},  # v25.89: post-v25.81 Sackmann backfill + 4-1 blocked-pick pattern justifies looser cap
```

## Change 2: `scripts/model_engine.py:1795` (approx)

```diff
-                    _seasoning_min = 7 if sp.startswith('tennis_') else 10
+                    _seasoning_min = 5 if sp.startswith('tennis_') else 10
```

## Change 3: `scripts/config.py` (clay edge floor)

Currently PLAY_THRESHOLDS for clay = 20%. If 20% edge floor is not the primary blocker on the 4-1 cohort (verify tomorrow), leave untouched. Only consider 20 → 17 if the loosened gates still produce zero fires on typical clay slates.

## Commit message (draft)

```
v25.89: loosen clay DIVERGENCE_GATE — post-backfill Elo is sharper than
gate assumes

Combined 2-day monitoring: 4-1 (80% WR) on blocked clay picks after
v25.81 Sackmann backfill seeded historical data. At -110 juice that's
+14% above break-even.

Changes:
  - max_spread_divergence: clay 2.5 → 3.5 (model_engine.py:180)
  - insufficient_elo_games: tennis 7 → 5 (model_engine.py:1792)

The v24 tightening to 2.5 was done on a pre-backfill sample (4 Monte
Carlo picks, -14.7u). Backfill fundamentally changed Elo quality. The
backtest and live monitoring both support a less aggressive filter.

Scope: tennis only. Team-sport divergence caps untouched.

Backtest evidence:
  - 2026-04-23 retroactive: Cristian + Atmane ML, both WIN, +12.9u
  - 2026-04-24 virtual: Prizmic/Buse/Grant spreads, 2-1 at -110
  - Combined 4-1 (80%), +14% above BE

Kill-switch: revert within 3 days if clay live WR drops below 50% on
next 10 fires.
```

## Backtest to run tomorrow before shipping

```python
import sqlite3, re
conn = sqlite3.connect('data/betting_model.db')
c = conn.cursor()

# Retrograde all Apr 21+ DIVERGENCE_GATE blocks on clay
rows = c.execute('''
    SELECT sport, event_id, selection, reason_detail, DATE(created_at)
    FROM shadow_blocked_picks
    WHERE sport LIKE 'tennis_%_french_open'
       OR sport LIKE 'tennis_%_madrid_open'
       OR sport LIKE 'tennis_%_italian_open'
       OR sport LIKE 'tennis_%_monte_carlo%'
      AND reason_category = 'DIVERGENCE_GATE'
      AND DATE(created_at) >= '2026-04-21'
    GROUP BY event_id
''').fetchall()

# For each, look up actual match result and compute hypothetical outcome
# Report: how many would have won if we'd bet, aggregate W/L and P/L
```

## Notes

- Do NOT ship if `results` table hasn't backfilled enough Apr 21-24 matches
- Do NOT ship if WTA and ATP split very differently (may need tour-specific thresholds)
- Do NOT ship if Rome R1 (Apr 26) shows a cluster of failures
