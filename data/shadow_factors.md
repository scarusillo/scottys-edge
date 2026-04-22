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
| 7 | NHL Away fast-paced | 6W-6L season / 1W-5L last 14d | -6.5u season, -20.6u last 14 days | Section 7 (pace_of_play_adjustment) — SPORT-GATED: only `sport == 'icehockey_nhl'` with `away_pace > 0` is shadowed. Shadowed v25.62 (2026-04-22). Baseball Away fast-paced (NCAA +10.2u, MLB +3.8u) KEPT active — surgical NHL-only shadow. Playoff pace dynamics drove the bleed. |

## What Remains Active

- **Home bounce-back** — still applied (separate from away bounce-back)
- **Home revenge** — still applied (separate from away revenge)
- **Home letdown spot** — still applied (2nd half -7.8u, threshold -15u, 7.2u cushion)
- **Home slow-paced** — still applied (only home fast is shadowed)
- **Away fast-paced for NCAA BB, MLB, NBA, soccer** — still applied (NHL-only shadow in v25.62)
- **Away slow-paced (all sports)** — still applied
- **All other context factors** (travel, refs, H2H, familiarity, weather, etc.)

## Monitoring (documented thresholds, not yet shadowed)

| Factor | Current 2nd-half P/L | Shadow trigger | Cushion |
|---|---|---|---|
| Steam: sharp opposes (MLB/NBA — outside v25.35 scope) | MLB: +3.56u (3 picks, 2-1) | Only extend to MLB/NBA if CLV turns negative AND n≥15 with P/L ≤ -10u. MLB currently profitable — do NOT block. | — |
| Home letdown spot | -7.8u (active) | -15u 2nd half | 7.2u |

## Model Changes Log (for agent reference)

### v22 — 2026-04-01 (PARTIALLY ROLLED BACK — see v25.4)

**NCAA Baseball UNDER filters (model_engine.py):** — ORIGINAL v22 description, superseded by v25.4
- ~~Block all Friday NCAA baseball UNDERs~~ — **ROLLED BACK v25.4 (4/10/2026)**
- ~~Block NCAA baseball UNDERs with line > 12.0~~ — **ROLLED BACK v25.4 (4/10/2026)**
- Original backtest (small sample): +29.8u saved
- 14-day re-backtest (Apr 10) showed the gates were blocking **winners**, not losers.

### v25.4 — 2026-04-10 (commit 58861af) — v22 NCAA UNDER filter rollback

**The v22 NCAA baseball UNDER filters were REMOVED after a 14-day backtest showed they were costing ~140u/month:**
- Friday UNDERs: **38W-20L (66%), +51.9u** — was being BLOCKED by v22
- line > 12.0 UNDERs: **79W-57L (58%), +54.4u** — was being BLOCKED by v22
- NCAA UNDER conviction floor rolled back 1.0 → 0.5 (|ms|>=0.5 unders: 60% win rate, +100.8u)

**Current behavior:** NCAA baseball UNDERs fire on all days of week and at all line sizes, provided they meet the model's 20% edge floor + conviction >= 0.5. Code: `model_engine.py:2711-2720` sets `_block_ncaa_under = False`.

**DO NOT flag this as a regression.** If NCAA UNDERs at lines > 12.0 or on Fridays fire, that is WORKING AS INTENDED.

**Grader resilience (grader.py):**
- Fresh-connection retry when primary score lookup fails
- Selection-name parsing fallback (extracts teams from bet selection string)
- Fixes race condition where concurrent grade processes could miss scores

**Schedule changes:**
- Grade moved from logon-trigger to 4:00am daily (BettingModel_Grade_4AM)
- Cloud agent moved from 5:00am to 4:45am ET (after grade pushes data)

### Issues Already Resolved — Do NOT Re-Recommend

- **v22 NCAA baseball UNDER line > 12.0 block:** REMOVED in v25.4 (4/10). 14-day backtest proved the gate was blocking +54.4u of winners (79W-57L). If line-12+ NCAA UNDERs fire today, that is INTENDED. Do not flag them as "gate not firing."
- **v22 Friday NCAA baseball UNDER block:** REMOVED in v25.4 (4/10). 14-day backtest: 38W-20L, +51.9u when unblocked. Friday UNDERs firing is INTENDED.
- **Friday game factor (-22.6u, old v22-era claim):** Stale. After v25.4 rollback, Friday NCAA UNDERs are a profit center, not a drag.
- **NCAA Baseball UNDER concentration (7-8 per day):** Direction cap (max 4) was added 3/29. Volume is self-regulated by the 20% edge floor + direction cap.
- **Away bounce-back shadow bug (Islanders 3/31):** Code was correct, stale bytecache from scheduled task. One-time issue.
- **BELOW_CAP picks (-11.1u drag):** Already fixed by v21 (3/31) which raised all edge floors to 20%. Only 2 below-cap picks since v21, both winners. The -11.1u was all pre-v21 history. No further action needed.
- **25%+ edge bucket -27.6u post-rebuild (1W-8L straight, 2W-8L incl. FADE_FLIP):** RESOLVED v25.13 (~4/10/2026). All 8 losses were PROP_OVER at odds > +140 from Apr 5–8 (Pages, O'Neil Cruz, Okamoto, Burleson, CJ Abrams ×2, Gorman, Drake Baldwin). `MAX_PROP_ODDS = 140` in `player_prop_model.py:80` now blocks these; **zero +140 PROP_OVERs have fired since Apr 10**. The "25%+" label is misleading — these picks sit at the `edge_pct` display cap of 25.0, not a true computed 25% edge. Cohort ages out of post-rebuild window on May 1. **DO NOT flag the 25%+ bucket as an active red flag.** If a post-Apr-10 +140 PROP_OVER appears, that IS a regression — investigate. Otherwise skip this in red-flag analysis.
- **"CLV=0 vs CLV=NULL conflation" claim:** NOT A BUG. Verified 2026-04-19. The grader correctly distinguishes: for SPREAD/TOTAL bets, `closing_line` is populated and `clv=0` means the line held (152 of 160 recent picks); for MONEYLINE bets, `closing_line` is NULL by design (MLs have no line) and CLV is computed from odds shift; only `closing_line IS NULL AND clv IS NULL` means missing data (3 of 160 recent = 1.9%). If a morning briefing claims CLV zeros are being conflated with missing data, it's misreading the schema. Do not propose adding distinguishing logic — the distinction already exists in the data.
- **NCAA Baseball OVERs -17.6u / DraftKings -25.6u post-rebuild:** RESOLVED v25.22/v25.23/v25.24 + v25.32 (4/17–4/18/2026). Two stacked pre-fix contaminations drive the whole deficit: (1) DraftKings routing 1W-6L -25.6u Apr 1–16, fully addressed by NCAA_DK_TIGHT_SKIP + NCAA_DK_FADE_FLIP + NCAA_NO_SHARP_SKIP + NCAA_DK_SHARP_VETO (zero DK NCAA BB OVERs have fired since Apr 17); (2) Stillman 0.2 IP + Harrison 5.4 IP thin-ERA OVERs on Apr 17–18, fully addressed by NCAA_ERA_RELIABILITY_GATE (`MIN_RELIABLE_IP_NCAA = 15.0` in `pitcher_scraper.py:1102`). Stripping both: **10W-5L, +18u** — the model as it stands today. Cohort ages out of post-rebuild window through May. **DO NOT flag NCAA BB OVER book-performance or midweek-vs-weekend as active red flags.** DO flag if a post-Apr-17 DK NCAA BB OVER fires, or if a post-Apr-18 pick lists a <15 IP starter in context_factors — those would be real regressions.

### Active Monitoring — Agent Should Track These Daily

**Home letdown spot** — Was profitable early (+15.7u first half) but second half is -9.1u and fading fast. Still active (not shadowed). If cumulative second-half P/L drops below -15u, recommend shadowing. Query:
```sql
SELECT result, pnl_units, created_at FROM graded_bets
WHERE context_factors LIKE '%Home letdown%' AND DATE(created_at) >= '2026-03-18'
```

**Away letdown spot** — SHADOWED v24 (4/6/2026). 2nd half hit -16.5u, 6.5u past -10u threshold. Two clay tennis losses on 4/5 (-10u) accelerated collapse.

**Midweek game** — SHADOWED v25.43 (2026-04-21). NCAA midweek `total_adj`
zeroed from +0.3 to 0.0 in `pitcher_scraper.py:1192` (matches MLB midweek
shadow). Pre-fix record on 13 post-rebuild NCAA midweek total picks:
7W-6L, -2.4u. March was 5-0 +20u (hot-end variance on 5 bets); April was
2-6 -22u (cold-end variance, driven entirely by 8 OVER picks firing at
the 20% edge cap while actual totals averaged -0.04 runs vs line). The
+0.3 adjustment was pushing marginal picks over the edge threshold in
the wrong direction.

Active monitoring lives in `agent_analyst.py:analyze_gate_health()` under
`NCAA_MIDWEEK_SHADOW (v25.43)`. Decision matrix at n >= 25 post-4/21 NCAA
midweek totals:
- Clearly positive P/L (>= +5u): consider restoring +0.15 (halfway) and re-monitor.
- Flat or negative: keep at 0.0 permanently.
- If sample shows strong UNDER bias post-shadow (reality < line): investigate whether a NEGATIVE adj (pitcher-heavy midweek) is warranted.

Picks fired post-4/21 with the `[SHADOW] Midweek game` tag use the
neutered 0.0 adjustment; tag is retained for tracking only.

### Day-of-Week Monitoring (added v24, 4/6/2026)

These combos were losing pre-v24 but are mostly fixed by 20% floor + gates. Monitor for recurrence:

- **NHL Saturday** — Was 7W-9L -18.4u. After v24: 4W-2L +4.1u (7 losses were below-cap/away letdown). If it dips negative again post-v24, investigate.
- **NBA Wednesday** — Was 1W-4L -14.3u. After v24: 1W-1L +0.2u. Small sample, keep watching.
- **NBA Sunday** — RESOLVED. All 4 pre-v24 Sunday losses were on one day (Apr 5). WAS@BKN UNDER 230.5 "Both teams fast-paced" is now blocked by v24 PACE_GATE. Schroder prop would re-generate under v25 prop overhaul. Post-v24 NBA Sunday picks: **0 fired** (as of 4/15). Do NOT flag as regression.

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
