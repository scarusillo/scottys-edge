# Scotty's Edge — Agent Morning Briefing
**Date:** Monday, April 06, 2026 | **Game Date:** 2026-04-05 (Sunday)
**Record:** 5W-8L | **P/L:** -19.9u | **Season:** 148W-107L (+81.3u, 58.0%, ROI +6.5%)

---

### 1. LOSS ANALYSIS

**Moise Kouame +4.5 games** (Tennis ATP Monte Carlo) — LOSS, -5.0u
Humbert won 6-3, 7-5 (13-8 in games, margin = 5). Kouame needed +4.5 games and lost by 5 — missed by half a game. Model spread was -0.2 (essentially a coin flip), yet edge showed 24% because the Elo gap was small. CLV: 0.0% (line didn't move). **VERDICT: MODEL ERROR.** The model's Elo-based spread doesn't account for surface-form differentials — Humbert is a clay specialist at home in Monte Carlo. Tennis spread edges above 20% should be scrutinized when the favorite is on their best surface.

**Marton Fucsovics +2.5 games** (Tennis ATP Monte Carlo) — LOSS, -5.0u
Tabilo won 6-4, 6-3 (12-7 in games, margin = 5). Fucsovics needed +2.5 and lost by 5 — blowout. Model spread -0.16 (another coin flip). Edge 18.75%. CLV: 0.0%. **VERDICT: MODEL ERROR.** Same issue — model treats these as close matches but the actual games margin was decisive. Two straight tennis 2-0 losses with 5-game margins suggests the model's tennis Elo may be too compressed, underrating form players.

**Memphis Grizzlies@Milwaukee Bucks UNDER 229.0** (NBA) — LOSS, -4.0u
Final: 115-131, total 246 — over by 17. CLV: 0.0%. Bucks dominated at home (131 points), Giannis line. Model spread -8.04 got the side right but the blowout pushed scoring. **VERDICT: VARIANCE.** Blowouts inflate totals via garbage time — a 16-point margin with both teams scoring freely. Not a model miss.

**Washington Wizards@Brooklyn Nets UNDER 230.5** (NBA) — LOSS, -5.0u
Final: 115-121, total 236 — over by 5.5. Context noted "Both teams fast-paced (+0.2)" which should have HURT the under, not helped. CLV: 0.0%. **VERDICT: MARGINAL.** The model's under projection was close (230.5 vs 236), but taking unders on fast-paced teams is contradictory. Context confirmed "fast-paced" yet the model still fired under — check whether pace context is properly penalizing unders.

**Tampa Bay Rays@Minnesota Twins OVER 7.5** (MLB) — LOSS, -5.0u
Final: 4-1, total 5. Line closed at 6.5 (bet 7.5). CLV: -1.0% (moved against us). Wind was -0.5 adjustment. Both pitchers middling (ERA ~4). The game just didn't score. **VERDICT: VARIANCE + BAD LINE.** Negative CLV confirms the market disagreed. Wind adjustment was applied but wasn't enough — closing at 6.5 vs our 7.5 is a full run of negative line movement.

**Carolina Hurricanes@Ottawa Senators UNDER 6.5** (NHL) — LOSS, -5.0u
Final: 3-6, total 9 — over by 2.5. Line closed at 7.5 (bet 6.5). CLV: -1.0%. Division familiarity (-0.3) was applied. Goalie Reimer had no recent stats ("?.??"). **VERDICT: MODEL ERROR.** Firing an under with an unknown goalie (Reimer, no recent data) is risky. The line moved a full point against us (6.5→7.5). Missing goalie data should be treated as a red flag for unders, not ignored.

**Dennis Schroder OVER 3.5 ASSISTS** (NBA Prop) — LOSS, -5.0u
Edge showed 25% at +127 odds. No CLV data (props). Schroder's assist projection vs actual unknown. **VERDICT: INSUFFICIENT DATA.** Props with no CLV tracking are flying blind on calibration.

**Andy Pages OVER 0.5 RBIS** (MLB Prop) — LOSS, -5.0u
Dodgers won 8-6 (14 total runs) but Pages still went 0 RBI. Edge 25% at +149. **VERDICT: VARIANCE.** Team scored plenty, player just didn't contribute. Binary props (0.5 line) are high-variance by nature.

---

### 2. SHADOW FACTOR TRACKING

Yesterday's picks had these context factors:
- **Away letdown spot (+0.2):** Applied to Kouame and Fucsovics (tennis). Both LOST. Running second-half P/L for away letdown should be checked — these tennis applications are questionable since tennis doesn't have "letdown spots" in the same way team sports do.
- **Home letdown spot (-0.2):** Applied to Blues +1.5. Won. Still contributing positively.
- **Division familiarity (-0.3):** Applied to Bruins/Flyers (WIN) and Hurricanes/Senators (LOSS). Net: 1-1, roughly flat.
- No [SHADOW] tagged factors appeared in yesterday's picks.

---

### 3. EDGE CALIBRATION TABLE

| Edge Bucket | W | L | Win% | Expected Win% | P/L | Status |
|-------------|---|---|------|---------------|-----|--------|
| AT_CAP (20%+) | 100 | 70 | 58.8% | ~58% | +88.1u | ON TARGET |
| BELOW_CAP (<20%) | 48 | 37 | 56.5% | ~55% | -6.7u | SLIGHT DRAG |

AT_CAP is performing as expected. BELOW_CAP is winning at a reasonable rate (56.5%) but negative P/L due to juice — these are lower-edge bets where the vig eats the profit margin. No buckets are 10%+ below expected.

---

### 4. CONVICTION TIER TABLE

| Tier | Filter | Approximate W-L | Notes |
|------|--------|-----------------|-------|
| Full record (3.5u+) | 148-107 | 58.0% | +81.3u |
| AT_CAP (max edge) | 100-70 | 58.8% | +88.1u — these carry the model |
| BELOW_CAP | 48-37 | 56.5% | -6.7u — marginal value |

Higher conviction = higher win rate confirmed. The entire profit comes from AT_CAP picks.

---

### 5. CONCENTRATION CAP PERFORMANCE

21 shadow-blocked picks in the export (some are duplicates from multiple runs). Unique blocked games from yesterday/recent:

- **Orioles@Pirates UNDER 8.5** (SHARP_CAP) — This game went 4-5 final (total 9). UNDER 8.5 would have been a LOSS. Cap protected us.
- **Ole Miss@Florida UNDER 10.5** (DIRECTION_CAP, 7x blocked) — Game went under. Would have been a WIN. Cap cost us ~5u.
- **UCSB@Cal Poly UNDER 10.5** (DIRECTION_CAP) — Already taken at different line. Duplicate protection, no impact.

**Net assessment:** Mixed. The direction cap blocked one winner (Ole Miss) but let through the actual pick slate. The sharp cap blocked a loser (Orioles). Overall the cap is roughly neutral this cycle. **Recommendation: KEEP current caps.**

---

### 6. ACTION ITEMS

1. **FIX APPLIED: Tennis grading now uses game scores.** The grader was using set scores (2-0) instead of game scores (13-8) for tennis spreads. Fixed in this session — v24 patch in `grader.py`. Verify next tennis grade works correctly.

2. **Investigate tennis Elo compression.** Both Monte Carlo losses had model spreads near 0 (-0.2, -0.16) but actual games margins of 5. Tennis Elo may be too tight — consider widening the spread conversion factor for clay/surface specialists.

3. **Flag unknown goalie stats for NHL unders.** Reimer had "?.??" stats and the under still fired. Add a guard: if a goalie's recent stats are missing, suppress under picks or reduce edge by 5%.

4. **Review NBA UNDER performance.** Season: 4W-5L, -7.8u on NBA unders. Two more losses yesterday. The "fast-paced" context for Wizards/Nets was applied but the model still took the under — the pace adjustment may not be penalizing enough.

5. **Push cloud agent schedule back to 5:30 AM.** The agent ran at 5:15 and grabbed Saturday's DB because the local grade doesn't finish uploading until ~5:16. Move to 5:30 to guarantee fresh data.

---

*Generated by Claude Code agent | 2026-04-06 | Corrected: tennis grades fixed, Blues OVER 5.5 scrubbed*
