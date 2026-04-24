# Tennis Block Backtest — Madrid R32 (2026-04-24)

**Purpose:** Grade today's blocked tennis picks as if we'd bet them. If WR ≥ 60% or P/L > +5u on n=11, the clay filters are too conservative and we lower thresholds. If WR < 50% or P/L < -10u, the gates are doing their job.

**Hypothesis under test (user flagged):** WTA is a soft market — our Elo-based model may have genuine edge against crude book lines, especially on qualifier-heavy draws where books can't price the unknowns well.

## Block scope

All picks blocked today by DIVERGENCE_GATE (`insufficient_elo_games` or `post_elo_rescue`) on Madrid R32. Excluded picks where model agreed with market direction strongly enough that the divergence was structural (e.g. Sinner -10.2 vs -6.5 on Bonzi — bet would be Sinner ML at -8000).

## The 11 virtual bets

Bet logic: model spread vs market spread — we bet the side the model thinks is underpriced. Default to ML on the dog unless model favors favorite more than market (then bet favorite ML).

**REVISED SCOPE (2026-04-24 afternoon):** Filtered to plays within odds window — `TENNIS_ML_CAP = ±200` per config.py, `MIN_ODDS = -150` globally. Bets outside that window wouldn't fire live, so excluded from grading scope.

| # | Tour | Matchup | Market | Model | Bet | Odds | In-scope |
|---|---|---|---|---|---|---|---|
| 1 | WTA | Linda Noskova vs Emiliana Arango | Noskova -5.5 | -0.2 | Arango ML | +610 | ❌ |
| 2 | WTA | Elena Gabriela Ruse vs Elena Rybakina | Rybakina -6.5 | +1.0 | Ruse ML | +1340 | ❌ |
| 3 | ATP | Dusan Lajovic vs Arthur Rinderknech | Rinderknech -2.5 | -2.0 | Lajovic ML | +143 | ❌ |
| 4 | WTA | Caty McNally vs Victoria Mboko | Mboko -4.5 | +0.3 | McNally ML | +250 | ❌ |
| 5 | ATP | Ben Shelton vs Dino Prizmic | Shelton -2.5 | -1.6 | **Prizmic +2.5 spread** | -110 | ✅ |
| 6 | WTA | Jessica Pegula vs Katie Boulter | Pegula -5.5 | -1.9 | Boulter ML | +520 | ❌ |
| 7 | ATP | Arthur Fils vs Ignacio Buse | Fils -4.5 | -1.0 | **Buse +4.5 spread** | -110 | ✅ |
| 8 | WTA | Sorana Cirstea vs Tyra Caterina Grant | Cirstea -4.5 | -1.0 | **Grant +4.5 spread** | -110 | ✅ |
| 9 | WTA | Yulia Putintseva vs Marta Kostyuk | Kostyuk -4.5 | +1.3 | Putintseva ML | +320 | ❌ |
| 10 | WTA | Janice Tjen vs Liudmila Samsonova | Samsonova -3.5 | +0.6 | Tjen ML | +176 | ❌ |
| 11 | ATP | Bonzi vs Sinner | Sinner -6.5 | -10.2 | Sinner ML | -8000 | ❌ |

**In-scope for grading: 3 bets (1 WTA + 2 ATP), all spreads at -110.** User's effective odds ceiling is MAX_PROP_ODDS = +140 (not the TENNIS_ML_CAP = +200). Bets 1,2,3,4,6,9,10 all excluded for odds > +140. Bet 11 excluded for exceeding MIN_ODDS floor (-150).

## WTA vs ATP split (in-scope only)

- **WTA:** 1 of 3 (Grant +4.5 spread only)
- **ATP:** 2 of 3 (Prizmic +2.5 spread, Buse +4.5 spread)

All 3 are spreads at -110. No ML fires in scope.

**WTA soft-market hypothesis not testable from this day.** At n=1 WTA we can't infer anything about a tour-level edge. Nearly all our WTA virtual bets fell outside live odds window, which is itself informational: if our WTA model edges keep concentrating in longshot MLs that live rules prevent, the "soft market" signal may be an Elo calibration artifact (thin clay seasoning overestimates upset probability) rather than an exploitable tour-level edge.

**Carry forward:** need Madrid R16/QF + Rome R32 samples before re-testing.

## Expected values (if each hits)

Sum if all 10 win at virtual 1u each:
- ML wins: +6.10 +13.40 +1.43 +2.50 +5.20 +3.20 +1.76 = +33.59u
- Spread wins (~+100 typical odds on these): ~+3.0u
- Total ceiling if 10-0: ~+36.6u
- Break-even: 3-7 or 4-6 depending on mix

## Grading plan

Run tomorrow AM (2026-04-25) after results backfill:

```bash
cd /c/Users/carus/OneDrive/Desktop/scottys_edge/betting_model
PYTHONIOENCODING=utf-8 python scripts/grade_tennis_blocks.py --date 2026-04-24
```

(Grade script needs to be written — or done manually by looking up each match in `results` table.)

## Decision matrix

| Outcome | Interpretation | Action |
|---|---|---|
| WR ≥ 70% (7-3+) | Filters are way too conservative | Backtest threshold drop; consider shipping |
| WR 55-70% (6-4 or 7-3) | Some edge exists, but thin sample | Continue tracking for Rome/French |
| WR 45-55% (5-5 or 6-4) | Filters approximately right | Hold current thresholds |
| WR < 45% | Filters correctly blocking losing picks | Do NOT loosen |

## WTA soft-market hypothesis

User's thesis: WTA tennis is a soft market we can exploit. Supporting:
- Lower public betting volume than ATP
- Books use cruder models for WTA (fewer sharps in WTA pools)
- WTA odds move later than ATP (books wait for steam)
- Qualifier-heavy R32 draws expose book pricing weakness

If this thesis is right, WTA WR should exceed ATP WR in the sample. Tracking for validation.

## Notes

- All 10 bets are DOG-oriented (model disagrees with market on favorite strength).
- 5 bets have the model identifying a DIFFERENT favorite than the market (Lajovic, McNally, Boulter, Putintseva, Tjen) — highest-conviction divergence.
- 5 bets have the model agreeing on direction but less extreme spread (Arango, Ruse, Prizmic, Buse, Grant).

## Why these were blocked live

| Block reason | Count | Fix lever |
|---|---|---|
| insufficient_elo_games (one player < 7 clay matches) | 6 | Drop min_games 7 → 5 |
| post_elo_rescue (divergence > 2.5 clay cap) | 4 | Raise max_spread_divergence 2.5 → 3.5 |

If WR is strong, lever candidates to test in sequence: (1) seasoning 7 → 5, (2) divergence 2.5 → 3.5, (3) clay edge floor 20% → 17%.

---

*Tracking file created 2026-04-24. Grade status: PENDING MATCHES. Next step: evening/morning check once `results` table populates.*
