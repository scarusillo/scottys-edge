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
| 6 | Away letdown spot | 13W-10L | +4.1u (1st half +20.6u, 2nd half -16.5u) | Section 6 (motivation_adjustment) — away_letdown key. Shadowed v24 (4/6/2026). Threshold was -10u 2nd half; hit -16.5u. Clay tennis losses (-10u on 4/5) accelerated collapse. |

## What Remains Active

- **Home bounce-back** — still applied (separate from away bounce-back)
- **Home revenge** — still applied (separate from away revenge)
- **Home letdown spot** — still applied (2nd half -7.8u, threshold -15u, 7.2u cushion)
- **Home slow-paced** — still applied (only home fast is shadowed)
- **Away pace (fast or slow)** — still applied
- **All other context factors** (travel, refs, H2H, familiarity, weather, etc.)

## Model Changes Log (for agent reference)

### v22 — 2026-04-01

**NCAA Baseball UNDER filters (model_engine.py):**
- **Block all Friday NCAA baseball UNDERs** — were 2W-5L, -17.7u
- **Block NCAA baseball UNDERs with line > 12.0** — lines 12.5+ were 8W-10L, -15.4u
- Kept picks (Saturday/midweek, lines ≤ 12.0): 7W-2L, +19.1u (78%)
- Net backtest impact: +29.8u saved

**Grader resilience (grader.py):**
- Fresh-connection retry when primary score lookup fails
- Selection-name parsing fallback (extracts teams from bet selection string)
- Fixes race condition where concurrent grade processes could miss scores

**Schedule changes:**
- Grade moved from logon-trigger to 4:00am daily (BettingModel_Grade_4AM)
- Cloud agent moved from 5:00am to 4:45am ET (after grade pushes data)

### Issues Already Resolved — Do NOT Re-Recommend

- **Friday game factor (-22.6u):** Driven almost entirely by NCAA baseball UNDERs (now blocked). Remaining Friday non-baseball is 11W-9L (-0.6u) — no action needed.
- **NCAA Baseball UNDER concentration (7-8 per day):** Direction cap (max 4) was added 3/29. New UNDER filters further reduce volume.
- **Away bounce-back shadow bug (Islanders 3/31):** Code was correct, stale bytecache from scheduled task. One-time issue.
- **BELOW_CAP picks (-11.1u drag):** Already fixed by v21 (3/31) which raised all edge floors to 20%. Only 2 below-cap picks since v21, both winners. The -11.1u was all pre-v21 history. No further action needed.

### Active Monitoring — Agent Should Track These Daily

**Home letdown spot** — Was profitable early (+15.7u first half) but second half is -9.1u and fading fast. Still active (not shadowed). If cumulative second-half P/L drops below -15u, recommend shadowing. Query:
```sql
SELECT result, pnl_units, created_at FROM graded_bets
WHERE context_factors LIKE '%Home letdown%' AND DATE(created_at) >= '2026-03-18'
```

**Away letdown spot** — SHADOWED v24 (4/6/2026). 2nd half hit -16.5u, 6.5u past -10u threshold. Two clay tennis losses on 4/5 (-10u) accelerated collapse.

**Midweek game** — Almost done contributing: +13.9u first half, +1.2u second half. Not urgent but track.

### Day-of-Week Monitoring (added v24, 4/6/2026)

These combos were losing pre-v24 but are mostly fixed by 20% floor + gates. Monitor for recurrence:

- **NHL Saturday** — Was 7W-9L -18.4u. After v24: 4W-2L +4.1u (7 losses were below-cap/away letdown). If it dips negative again post-v24, investigate.
- **NBA Wednesday** — Was 1W-4L -14.3u. After v24: 1W-1L +0.2u. Small sample, keep watching.
- **NBA Sunday** — Was 0W-3L -14.0u. After v24: 0W-2L -9.0u. Still negative but tiny sample (2 picks). Track.

### v24 — Context Gates (not shadows — these are directional vetoes)

**Fast-paced / Altitude on NBA UNDERs** — GATE added v24 (4/6/2026). Fast-paced or altitude context on NBA UNDER picks vetoes the pick. Data: with pace/alt on unders 1W-4L -15.7u, without 3W-1L +7.8u. Pace/altitude still active on OVERs, spreads, and MLs (confirms direction). Vetoed picks logged to shadow_blocked_picks with PACE_GATE reason.

**MLB Park Factor on contradicting totals** — GATE added v24 (4/6/2026). Hitter's park (adj > +0.2) vetoes UNDERs, pitcher's park (adj < -0.2) vetoes OVERs. Park no longer inflates model_total (was double-counting market — 3W-6L -16.1u). Vetoed picks logged with PARK_GATE reason.

## How to Query Shadow Performance

Look for `[SHADOW]` in the `context_factors` column of `graded_bets`:

```sql
SELECT result, pick, context_factors, units
FROM graded_bets
WHERE context_factors LIKE '%[SHADOW]%'
ORDER BY date DESC;
```

## Re-enabling a Factor

To re-enable a shadow factor, remove it from the `SHADOW_MOTIVATION` set or
reverse the shadow logic in `get_context_adjustments()` in context_engine.py.
Each shadow section has a comment marking it.
