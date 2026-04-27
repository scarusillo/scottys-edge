# Scotty's Edge — Master Agent To-Do List
**Last updated:** 2026-04-26 — v26.0 refactor COMPLETE (all phases shipped today)

## ✅ DONE — v26.0 generate_predictions refactor (shipped 2026-04-26)

All 7 phases shipped. `model_engine.py` 3,807 → 2,171 lines (−43%); `main.py`
5,077 → 4,300 lines (−15%). New `pipeline/` package: 11 files, 3,841 lines,
all replay-verified 0-diff at every chunk.

See `project_v26_refactor.md` in user memory for the full breakdown
(stage map, bug post-mortems, compat shims preserved, workflow notes).

**For agents:** the per-game loop body now lives in `scripts/pipeline/per_game.py`
and channel logic in `scripts/pipeline/channels/`. `model_engine.generate_predictions`
and `main._merge_and_select` are thin compat wrappers — calling them still works.
Use `pipeline.orchestrator.run(conn)` for new code.

---

## 📅 2026-04-26 morning to-do — items closed today

| # | Item | Decision |
|---|---|---|
| 2 | Bet 1063 COL@NYM rainout | Graded PUSH (direct DB update) |
| 3 | scottys_edge.py:325 OverflowError guard | Patched (try/except for extreme spreads) |
| 4 | odds_api.py orphan `_grade_bets()` | Removed (~88 lines) |
| 5 | Prop floor doc comment in config.py | Added (clarifies 8/10/20% three-tier policy is intentional, not a bug) |
| 6 | reel_generator.py — archive or document? | **Documented**: added "manual tool only" note in module docstring; kept as fallback to Sora workflow |
| 7 | BetMGM PROP gate? | **Defer**: post-Apr-15 stale-odds-fix data is n=9 (3W-6L, -17.28u, avg CLV +0.31) — too small. Re-evaluate at n=20 per the existing BetMGM CLV trip-wire monitor |
| 8 | CLV agent SOFT_LEAD reply | **Keep watching neutrally**: per the handled-list parking rule (n≥60 with full live trajectory data), do not act on n=10 signal regardless of direction |

---

## 🛑 AGENT: DO NOT RE-FLAG THESE (already handled)

Cloud agents (CLV Patterns, Morning Briefing, Code Auditor, Pre-Run Validator)
keep citing cohorts that are already gated. Before surfacing a "new" finding,
verify it's not one of these. Each entry lists the trigger phrase agents
typically use, and why it's already solved.

| Agent-surfaced claim | Status | Gate / Memory |
|---|---|---|
| "DK NCAA BB UNDERs -51u / 9-18, structural loss" | Handled | v25.56 HARD_VETO_DK_NCAA_BB_UNDERS (2026-04-21). Query cites pre-fix data. |
| "DK NCAA BB OVERs -21.8u / 2-6, propose new veto" | Handled | Combined v25.22/23/24 Option C + v25.35 SHARP_OPPOSES stack. ZERO DK NCAA OVERs have fired since 2026-04-19. |
| "NCAA BB OVER directional bias -28.8u, raise edge floor 20→22" | Handled | edge_pct stored capped at 20.0 — raising floor is meaningless. DK-specific bleed killed by v25.22-24 Option C stack (DK-only by design, main.py:1021). Non-DK NCAA OVERs DO still fire (FD/BR/BetMGM/Caesars n=27 post-rebuild, 11-13-3 -17.56u, all books +CLV) — variance against fair line, not gateable. Don't propose extending Option C to non-DK; different pricing regime. |
| "Week 15 (Apr 14-20) NCAA BB -30.3u, audit required" | Handled | -30.65u of the week's damage was the DK UNDER cohort killed by v25.56. |
| "STABLE cohort WR regression 73% → 55.6%, workhorse softening" | Parked | v25.84 backfill only covers 14 days; n=21 "baseline" was first 4 days. Do NOT act until n≥60 graded with full live trajectory data (per v25.83/84 memo). |
| "NBA SHARP_OPPOSES / NBA UNDERs -11.8u" | Watched | Per v24 review (2026-04-18): NBA variance — wait. Act only if CLV drops below +0.5 at n≥25 more picks. |
| "NBA PROP phantom CLV / EDGE_12_16/16_20 losing" | Partial | v25.30 PROP_DIVERGENCE_GATE + v25.66 odds-bucket gate + Apr 15 stale-odds fix cover it. Re-evaluate at n≥15 post-Apr-15 per `project_prop_review.md`. Currently n=4. |
| "DK NBA -24.1u routing problem" | Watched | Most damage pre-Apr-15 stale-odds bug. Post-fix: 3-5 / -9.15u on n=8. Subset of broader NBA variance watch. |
| "LINE_AGAINST_GATE: 0 entries, propose fix" | Handled | v25.86 (2026-04-24) — gate now populates reason_category. Pre-v25.86 queries returned zero because gate wrote only to free-text `reason` column. |
| "Tennis clay DIVERGENCE_GATE blocking 218 picks" | Partially handled | v25.85 (2026-04-24) lowered seasoning threshold 10 → 7 for tennis. Remaining blocks are legitimate (qualifiers with <7 clay matches). Divergence cap 2.5 + 20% edge floor untested post-backfill — park until n≥15 live clay picks. |
| "NBA OVER props losing on below-career lines (Naz Reid, Schroder, Merrill)" | Handled | v25.87 PROP_CAREER_FADE (2026-04-24) — flips NBA OVER→UNDER when market median ≥1.0 below career avg at 5u. Do not propose additional career-based prop gates for NBA. |
| "Josh Hart UNDER 4.5 AST model error" | Partially handled | Covered by PROP_CAREER_FADE investigation — Hart was outlier in UNDER-below-career cohort (2-1 +3.70u), not a structural problem. Real problem was OVER-below-career which v25.87 addresses. |
| "Midweek game factor -20u" | Shadow | Already in `shadow_factors.md`. Do not re-propose. |
| "Steam sharp opposes -15.6u" | Handled | v25.35 SHARP_OPPOSES_BLOCK for NHL + NCAA BB. Remaining MLB cohort 2-1 +3.56u — do NOT extend. |
| "NCAA BB SHARP_OPPOSES 1W-4L approaching gate threshold" | Handled | v25.35 SHARP_OPPOSES_BLOCK already gates NCAA BB. Live: 10 blocks Apr 24, 9 Apr 23, 4 Apr 22. Claim cites pre-v25.35 historical data. |
| "NCAA BB TOTAL EDGE_20_PLUS -21u CLV-1.2 propose new gate" | Handled | Cohort fixed by v25.22-24 + v25.35 + v25.56 stack. Pre-v25.56: 49 picks, -21.9u. Post-v25.56 (Apr 21+): 5 picks, **+11.1u** (4W-1L). |
| "LINE_AGAINST_GATE Cal@Miami logged 5x, propose dedup" | Not a bug | Hourly pipeline correctly re-evaluates each cycle (1hr apart). Every gate shows 1-16 fires/pick: CONTEXT_TOTAL_P2 16.3, PROP_DIVERGENCE 14.6, PROP_CAREER_FADE 3.9, etc. Expected behavior. |
| "SOFT_LEAD thesis weakening at n=10, action needed" | Parked | Same parking logic as STABLE entry above. v25.84 L2 originator channels (SOFT_LEAD, SHARP_LEAD, DIVERGENT, MIXED, originator_book) all need n≥60 with full live trajectory data before any thesis revision. Currently observation-only. |
| "Loosen clay DIVERGENCE_GATE — 4-1 retroactive signal" | Tested + Held (user revisiting) | Retroactive grade Apr 25 of full Apr 21-24 sample (n=31 SPREADs at -110): 13-17-1, 43.3% WR, -5.17u. WTA worst at 38.9%. ML in-scope [-150,+140] only n=5 (3-2 +1.4u — noise). Gate stays at max_spread_divergence=2.5, insufficient_elo_games=7. **USER FLAGGED FOR DEEPER ANALYSIS** — see TO-DO below. Do not re-propose until that analysis is run. See `project_wta_soft_market_thesis.md`. |
| "Tennis Caesars routing veto — 1-4 -15.67u" | False positive | **All 4 Caesars Monte Carlo losses (Apr 5-6) fired pre-fix:** before commit 9e185e4 (Apr 6 spread_per_elo 120→13 fix), v24 (Apr 7 clay edge floor + divergence tighten), v25.81 (Apr 23 Sackmann Elo backfill 4×), v25.85 (Apr 24 seasoning 10→7), v25.88 (Apr 24 ML cap 200→140). The 16-24% "edges" were phantom math from a 10× spread_per_elo bug + thin Elo seasoning. Current model post-Apr-23 is 4-1 +15.76u on tennis. **Any tennis routing/gate analysis MUST filter to created_at >= 2026-04-23** to exclude bug-era data. |

**How to use this table:** Before a cloud agent proposes a gate/veto/change,
match the claim against the left column. If any handled/parked item matches,
say so instead of proposing. Cite the gate name + ship date.

---

## Original content

## 🔬 USER FLAGGED — Deeper clay DIVERGENCE_GATE analysis (2026-04-25)

First-pass retro grade of n=31 Apr 21-24 blocks ran 2026-04-25 AM and showed the gate is doing its job (43.3% WR, -5.17u at SPREAD -110; WTA 38.9%). User wants to analyze further before accepting that conclusion.

**⚠️ Key constraint:** ANY tennis analysis must filter `created_at >= 2026-04-23` (post-v25.81 Sackmann backfill + post-Apr-7 v24 fixes). Pre-Apr-7 tennis picks were generated by a broken model (10× spread_per_elo bug fixed in commit 9e185e4 on Apr 6, thin Elo seasoning fixed Apr 23). Including pre-fix picks in any tennis backtest produces false signals — see "Tennis Caesars routing veto" entry in handled list above.

Possible angles to explore next session:

1. **Split by gate reason** — `insufficient_elo_games` vs `post_elo_rescue` may behave differently. The 31-pick aggregate may hide a profitable subset.
2. **Filter by divergence size** — div=2.5-3.0 (just over threshold) vs div=4.0+ (large divergence). Small-div picks may be the noise; large-div may be real signal or strong fade.
3. **Filter by player seasoning** — the picks where one player has 4-6 clay matches (just under the 7 floor) vs picks where one has <3. The latter may be the genuinely uncertain cases the gate should keep blocking.
4. **Round-stratified** — R32 vs R16 vs QF. Maybe the gate is right at R32 (large field, more qualifiers) but wrong at R16+ where Elo seasoning becomes meaningful.
5. **ML + MAX_PROP_ODDS=200 sensitivity** — re-run the ML grade with TENNIS_ML_CAP +200 instead of +140 to see if it expands the in-scope sample meaningfully.
6. **Add Apr 25 today picks** once Madrid R16 finishes (today), bumps sample by ~7-10 events.

Source data: `shadow_blocked_picks` rows where `sport LIKE 'tennis%'` AND `reason LIKE '%insufficient_elo%' OR '%post_elo%'`, deduped by event_id. Reason string format: `DIVERGENCE_GATE (insufficient_elo_games, div=X.X, ms=±X.X, mkt_sp=±X.X)`. Bet side determined by ms vs mkt_sp comparison (ms < mkt_sp → bet player1; ms > mkt_sp → bet player2).

Tracker: `data/tennis_block_backtest_20260424.md` + `project_wta_soft_market_thesis.md` (now marked INVALIDATED — re-open if angles 1-6 above flip the conclusion).

---

## 🔴 ORIGINAL TOMORROW MORNING (2026-04-25) — Review clay DIVERGENCE_GATE

**User priority.** Combined 2-day signal on clay picks the DIVERGENCE_GATE blocked live:

| Date | Source | Record | Notes |
|---|---|---|---|
| 2026-04-23 | Cristian + Atmane ML (retroactive backfill after v25.81) | 2-0 | +12.9u; blocked live by insufficient_elo_games, backfill cleared |
| 2026-04-24 | Prizmic +2.5, Buse +4.5, Grant +4.5 (virtual monitoring) | 2-1 | at -110 = +14% above break-even |
| **Combined** | | **4-1 (80% WR)** | |

At -110 juice, 4-1 = +14% above break-even on 5 picks. Still small sample but the signal is getting hard to dismiss.

**The two filters at play:**
1. `insufficient_elo_games` threshold = 7 (lowered from 10 this morning in v25.85)
2. `max_spread_divergence` = 2.5 for clay (tightened from 4.5 in v24)

All 3 blocked picks had:
- Seasoning: one player at 4-6 clay matches (below 7 floor)
- Divergence: 3.5-4.1 (above 2.5 cap)

**Tomorrow AM checklist:**
1. Confirm per-match results from `results` table once backfilled — identify which 2 of today's 3 covered (Prizmic, Buse, or Grant)
2. Pull ALL blocked tennis clay picks since v25.81 backfill (Apr 21+) with reason=`DIVERGENCE_GATE` — retroactive grade to see if the 4-1 pattern holds on larger sample
3. If sample expands to 8-10+ with 70%+ WR, strong case to loosen:
   - max_spread_divergence 2.5 → 3.5 for clay
   - insufficient_elo_games 7 → 5 for tennis
4. Don't ship on current n=5 — wait for Rome/French cross-check. But start drafting the config edit so it's ready to go if the pattern holds.

**Tracker:** `data/tennis_block_backtest_20260424.md`

---

## 🗓 NEXT WEEK — NBA Playoff Series Awareness

**Investigation flagged 2026-04-24.** Our NBA DATA_TOTAL Context Model uses regular-season form and H2H but ignores games played in the current playoff series. DEN/MIN Game 3 missed by 30 pts (we projected 239.2, actual 209) while Games 1-2 of same series already showed series avg -11 UNDER vs line. Pattern holds across most series in Apr 18-24 (15-5 UNDERs league-wide, 7/8 series opened UNDER).

**Proposal (not shipped):** `NBA_PLAYOFF_SERIES_ADJUSTMENT` — when Game 2+ of a series, dampen context projection by 50% of the series_avg_vs_line trend.

**Why waiting:** Only 3 Game 3+ data points; n too thin to ship per `feedback_no_panic_kill.md`.

**Decision by May 1 (also passive monitoring item):**
1. Ship standalone gate if sample supports at n=30+ playoff games
2. Fold into broader NBA market-baseline redesign
3. Drop if market closing totals catch up to actual avg (~213) before then

See `project_nba_playoff_series_awareness.md` for full analysis + sample DEN/MIN calculations.

---

## 📅 SATURDAY MORNING (user's next focused chunk)

User explicitly said Saturday morning is the next focused work block.
These are the top items ranked for that session.

**✅ DONE on 2026-04-23 (no longer Saturday work):**
- ~~Typed `shadow_blocked_picks` reason column~~ — shipped v25.71
- ~~Tiered odds/props/prop_snapshots storage to separate archive DB~~ — shipped v25.79

**Partial — Saturday to finish:**
1. **Proper CLV at fire time** — `bets.opener_line` and `bets.opener_move` shipped today (v25.80). Still TODO: `line_at_close` + `closing_odds` columns populated at grade time, + `clv_line_pct` and `clv_odds_pct` separate metrics. The opener side is done; the close side is still derived in reports.

**✅ Per-book line trajectory (Layer 2) — SHIPPED 2026-04-23 evening (v25.84)**

`scripts/per_book_trajectory.py` + columns `originator_book`, `move_breadth`,
`sharp_movers`, `soft_movers`, `sharp_soft_divergence`, `move_class` on bets.
Classifies every pick into STABLE / SHARP_LEAD / SOFT_LEAD / STEAM / DIVERGENT / MIXED.
Backfilled on 38 historical bets with trajectory data; nightly backfill in cmd_grade.

Forward signal to watch (n=21 graded, directional only):
- STABLE: 73% WR / +18.3u (the workhorse)
- SOFT_LEAD: 50% WR / -1.8u on 6 bets — forming fade signal, watch
- SHARP_LEAD: too thin (1 graded so far)
- DIVERGENT: 50% / -0.5u
- MIXED: -5u (1 graded)

Briefing now shows L1 + L2 tables daily + lists drift/soft-led/divergent picks
as avoid-candidates for human-in-loop review.

**Saturday step-up for Layer 2 (when sample reaches n>=60):**
Once forward data accumulates, ship gates/boosts based on classification:
- SOFT_LEAD picks at sub-20 edge → block (likely retail flow)
- SHARP_LEAD picks → consider stake boost (1u extra)
- DIVERGENT with sharp_soft_divergence >= 0.5 → route to the sharper book's price

**Saturday quick-check — LINE_AGAINST_GATE exemption review (10 min)**

Today (2026-04-23) we exempted DATA_SPREAD/DATA_TOTAL/SPREAD_FADE_FLIP/etc.
from LINE_AGAINST_GATE on the theory that anti-market channels have their
own logic. Backtest at ship time was non-conclusive: only 1 DATA_TOTAL had
opener_move ≤ -0.5 (it lost), and DATA_SPREAD had n=0 (helper doesn't compute
opener_move for `side_type='DATA_SPREAD'` — small bug, but DATA_SPREAD is
disabled per v25.70 so not urgent).

When n>=10 DATA_TOTAL picks have opener_move data (~2-3 weeks):
1. Re-run the backtest in this same agent_todo entry's notes
2. If would-be-blocked cohort underperforms by >5u with WR<45% → remove
   DATA_TOTAL from the LINE_AGAINST_GATE exemption list (main.py near line 3608)
3. If similar to non-blocked cohort → keep exemption, document as validated

Also: extend `_compute_opener_move_for_pick` to handle DATA_SPREAD direction
(infer from model_spread sign) so future DATA_SPREAD re-enablement can use
trajectory features.

**Other Saturday quick wins (1-2 hours each):**
2. **Didn't-fire observability (gate counters)** — for every gate, track fired/blocked
   per day. Current blind spot. Now easier with v25.71 typed reason_category.
3. **Regression-fit Context Model weights** — today's calibration backtest said optimal
   scale=0%. Fit weights from 90+ days of historical data instead of magic numbers.

**Big projects (NOT for Saturday — need planning session):**
4. Break `generate_predictions()` into pipeline stages (3-5 days)
5. Refactor `main.py` god-file (1-2 weeks)
6. Per-market sharp/soft tagging matrix (1 week)
7. Regression test suite (2 weeks)
8. Versioned ratings (2 weeks)
9. ML-based CLV prediction model (combines Layer 1 + Layer 2 features into a regression
   that predicts CLV at fire time; use as gate. Wait until n>=100 graded bets with full
   trajectory data before building — currently n=21.)

---

---

## 🛠 TECH DEBT + ARCHITECTURE REVIEW (2026-04-22)

Fresh-eyes review of the whole codebase (48K lines, 93 files, 35 tables).
Two masks: "first time seeing this" + "everything is wrong."

### Fresh-eyes observations (what a new collaborator would flag)

1. **`context_engine.py` vs `context_model.py`** — near-identical names, two files (2,457 + 938 lines). Which is canonical? Confuses everyone.
2. **`main.py` is a 5,230-line god-file** — CLI, orchestration, email, filters, cards, social, caps all in one.
3. **`model_engine.py` `generate_predictions()` is ~1,500 lines** — one function does everything from rating loading to pick firing.
4. **"Path 1" / "Path 2" are internal jargon** — leaked into code naming. User had to ask 3× what they mean in one session.
5. **92 version-stamped comments in `model_engine.py` alone** — code is a living museum. `v25.4 ROLLBACK`, `v22 REMOVED`, `v17 FIX`. Hard to distinguish load-bearing from archaeological.
6. **Tables of unclear lifecycle** — `team_ratings` vs `power_ratings`, `model_b_shadow` (never referenced elsewhere), `prop_openers` vs `openers` vs `prop_snapshots` vs `prop_snapshots_archive`.
7. **Odds data discarded after 7 days** — core project asset pruned on a cron. Backtests keep failing to reproduce Phase A numbers because the data is gone.
8. **Results ↔ odds event_id mismatch** — NCAA results use ESPN IDs, odds use API hash IDs. These tables CAN'T be joined. Blocked today's NCAA steam-chase backtest entirely.
9. **No unified backtest harness** — 10+ one-off backtest scripts, each reimplementing join logic. No shared test framework.
10. **Configuration scattered across 4+ files** — `config.py`, inline dicts in `model_engine.py`, constants in `player_prop_model.py`, weights in `context_model.py`. No single view of "what knobs can I tune."

### Adversarial review (ranked for engineering work)

**Tier A — Critical data infrastructure:**
- Odds retention 7 days → **archive to cold storage** *(1 day, starting now)*
- Results ↔ odds event_id mismatch → build event-matcher on home/away/date *(2-3 days)*
- No "didn't fire" observability → instrument every gate with counters *(1 week)*

**Tier B — Code structure:**
- Refactor `main.py` into focused modules (cmd_run, cmd_grade, cmd_opener, etc.) *(1-2 weeks)*
- Break `generate_predictions()` into pipeline stages *(1-2 weeks)*
- Rename Path 1 / Path 2 → semantic names; merge context_engine.py + context_model.py *(4-8 hours)*

**Tier C — Modeling:**
- Context Model weights are hardcoded magic numbers (`FORM_WEIGHT=0.5`, `SERIES_MOMENTUM_WEIGHT=0.25`). Never fit to data. Today's backtest said optimal scale = 0%. Regression-fit on 90d+ data. *(1 week)*
- No regression test suite — every ship is "backtest + pray" *(2 weeks)*
- Binary sharp/soft book tagging — books have different sharpness per market *(1 week)*
- Ship-cadence is too fast — 13 ships today, flip-flopped 3× on one channel. Slow to 2-3/week *(organizational)*

**Tier D — Observability:**
- `shadow_blocked_picks` conflates different block types in free-text reason column → split into typed columns *(3 days)*
- CLV not tracked at fire time (we just shipped clv_tracker.py as a report, not instrumentation) *(1 week)*
- Ratings updates aren't versioned → time-travel queries impossible *(2 weeks)*

---

## 🔍 ELO SPREAD CHANNEL DIAGNOSIS (2026-04-22, findings)

**Fact:** Zero edge-based SPREAD picks fired since 2026-04-06 (16 days).

**Diagnosis (not a model bug, not a gate bug):**

1. **NCAAB season ended April 6** — 20 of our 60 DOG winners were NCAAB. Channel dead until Nov.
2. **NBA + NHL in playoffs** — top teams converge in Elo, small differentials, rare 20% edges.
3. **MLB + NCAA Baseball active** but ±1.5 runlines are mathematically hard to hit 20% edge on.
   Today's predict: MLB 20 games → 30 spread picks "below threshold" (all filtered out).

**Current edge threshold ~15-20% is correct for basketball/college-season sports but may be too high for baseball runlines.**

**Possible actions (not urgent, verify first):**
- Backtest MLB spread at 10-12% edge threshold — if historically profitable, unlocks a big channel
- Accept seasonal volume dip (wait for November NCAAB return)
- Invest volume elsewhere (tennis 5am scheduler, totals, props)

**Do NOT:**
- Lower thresholds without a backtest
- Rewrite the Elo model (not broken)
- Add gates (more gates = even fewer fires)

---

## 🟢 TOMORROW MORNING — grade + decide

1. **Grade unscrubbed bets #1079 (SA@POR UNDER 219) and #1080 (SD@ARI OVER 15.0).** Both restored to 5u after positive-CLV cohort argument. Note for #1080: if it loses BIG against a high market line, investigate Mexico City altitude park-factor gap (model said 16.78 vs market 15.0 — verify whether that disagreement was altitude-aware or coincidence).

2. **Check v25.90 PROP_PLAYOFF_ROLE_GATE_SHADOW first daily report** — agent Section 6d should populate. 27 picks shadowed on 4/26 evening run. Track cohort outcomes in graded_bets.

3. **Grade 9 scrubbed soccer OVER picks** → pull actual totals for bets 1008-1014, 1021, 1022. If 6+/9 hit OVER, consider promoting soccer OVER cohorts from SHADOW to FOLLOW. If <=4 hit, shadow rule was right.

4. **Check 5am tennis run output.** Did `BettingModel_Tennis_Morning` fire? What edges? Confirm the scheduled task actually executed (PC was on). Tennis Elo green-lit via 3,376-match backtest (70-73% WR across all slices).

5. **Normal agent flow** — morning briefing + pre-run validator + code auditor should all work tomorrow now that `db-latest` release is fixed (v25.61). Check for fresh action items.

---

## 🚨 PRIORITY: NBA playoff regime mismatch (added 2026-04-26)

**The diagnosis from the 4/26 session**: NORMAL channel WR fell from
58.7% (March, n=211, +72.78u) to 51.2% (April, n=203, -24.25u) on stable
CLV (+0.35 → +0.29). Not a broken model — it's regime-blind. Three of four
high-volume sports shifted regime in April: NBA→playoffs, NHL→playoffs,
tennis→clay (Monte Carlo). Elo+Context was calibrated on regular-season.

**Hot fix candidates** (Path A — surgical, 1-2 days):
1. **NBA DATA_TOTAL UNDER threshold raise from 0.3 → ≥3.5** — would have
   flipped the 1-4 (-15.76u) playoff cohort to 1-0 (+4.24u). The one
   winning UNDER (BOS@PHI 4/24) had disagreement -4.68; all 4 losers had
   disagreement in -1.87 to -2.91 range. Tightening to ≥3.5 keeps the
   winner, drops the noise. See Apr 25 OKC@PHX UNDER 214.5 pick — same
   pattern.
2. **MLB ERA reliability tightening** for early-season — current MIN_IP
   thresholds set for mid-season; April still has thin pitcher samples.
3. **Tennis surface-Elo audit** — Monte Carlo went 1-3 -10.67u with CLV
   -0.25 (only negative-CLV cohort this month). Clay specialists likely
   under-weighted in Elo despite v25.81 backfill.

**Don't do:** retraining Elo, replacing NORMAL channel, panic-killing
specialty channels. CLV proves edge exists — just regime-blind.

**Reference:** `project_session_apr26.md`, `project_nba_playoff_series_awareness.md`

---

## 🔎 NEXT-SESSION INVESTIGATIONS — code needed

**Bench-player props (UPDATED 2026-04-26 — partially shipped as v25.90):**
Initial flag from Sam Merrill / Vucevic scrubs evolved into cohort study:
post-Apr-15 NBA props showed ROLE-tier (60d avg pts < 12) PROP_OVER picks
losing 1-4 (-16u) in playoffs vs 1-1 (+1.2u) reg-season. Sam Merrill lost
again 4/26 after scrub. Shipped `PROP_PLAYOFF_ROLE_GATE_SHADOW` (v25.90,
player_prop_model.py ~line 1195) — logs candidates to shadow_blocked_picks
without blocking. Tracked in agent Section 6d. Promote/kill rules: live
block when n≥15 AND WR<40% AND P/L<-3u; kill if n≥20 AND P/L>0u.

**Still open** (broader bench/usage gate, NOT shipped): minutes-based
filter requires usage rate or avg-min stats not currently in box_scores
(only pts/ast/reb/blk/stl/threes). Either backfill minutes via ESPN box
score scrape or rely on the v25.90 pts-tier proxy. Decide AFTER the
EDGE≥25% prop ceiling decision and after v25.90 shadow accumulates n≥15.

**Context Model completeness:**
- **Soccer Path 2 shadow data review** at n≥15 per sport × direction — promote OVER cohorts if backtest supports
- **Re-validate fade cohorts** (MLS UNDER, EPL UNDER) at n≥15 — consider explicit FADE logic vs BLOCK
- **Root-cause Phase A discrepancy** — v25.47 code comments cite MLS 15@66.7%, Serie A 12@66.7%; my 90d backtest found 8 and 10 respectively. Odds-table 7-day retention is partial answer; need to reproduce Phase A methodology precisely.

**Mexico City altitude park-factor audit (added 2026-04-26):** SD@ARI OVER
15.0 (bet #1080) was a Mexico City Series game played at ~7,300 ft (higher
than Coors at 5,200 ft). Model arrived at projection 16.78 vs market 15.0,
+1.78 disagreement (above DATA_TOTAL MLB 1.5 threshold). Verify whether
weather/park engine recognizes Mexico City venues — if it doesn't, the
model is blind to the largest single-venue altitude effect in baseball
and arrived at the elevated projection coincidentally. Check:
- `weather_engine.py` for venue-altitude lookup
- park factor table for Mexico City entry
- If missing, add Mexico City stadium with appropriate altitude multiplier

**Model channels never inverse-backtested:**
- **Primary Elo edge model** — our workhorse (385+ graded picks, +76u, 56% WR). Run per-sport × market_type inverse backtest.
- **SPREAD_FADE_FLIP** — backtest +140u at launch, never re-validated. 1 live fire.
- **BOOK_ARB** channels — multiple versions (v25.25-28), never re-validated.
- **DATA_SPREAD Path 2** — 90d backtest looked bad (-5u NBA, -7u NHL) but left live pending 30 days real fires (per user call — user correctly pushed back on overconfident halt recommendation from thin sample).

**Tennis Phase 2+ (after live sample from 5am task):**
- Trace why ATP 16-21% edges aren't firing in recent pipelines (suspect Elo confidence filter or tennis-specific edge threshold). Phase 1 diagnosis stopped halfway today.
- Add head-to-head tracker (tennis context model has H2H but may be under-weighted)
- Retirement/withdrawal risk flag
- Set-score distribution model for spread bets
- Inverse backtest at n≥15 live picks from new 5am schedule

**Support systems needing audit:**
- Referee engine (`referee_engine.py`) — low-scrutiny zone
- Weather engine — low-scrutiny zone
- Goalie form NHL — v25.50 was tested + reverted, worth revisiting

---

## 📊 PASSIVE MONITORING — no code, just watch

- **2026-05-01 NBA playoff total recalibration check.** Current window: playoff closings avg 219.8 vs actual avg 213.4 (UNDERs 15-5 / 75% league-wide). Our Context Model disagreement threshold 0.3 is firing correct direction but sample n=6, record 0-2 UNDER / 1-1 OVER. Re-verify at ~7-day mark. If closing totals drop another 5 pts toward actual while our disagreement shrinks, the un-priced UNDER edge is gone.

- **Tennis block backtest (2026-04-24 Madrid R32).** 11 blocked picks tracked as virtual bets in `data/tennis_block_backtest_20260424.md`. Grade tomorrow AM. **WTA soft-market hypothesis:** user flags WTA as a soft market (lower public volume, cruder book models, qualifier-heavy draws). If WTA WR > ATP WR by >20pts in this sample OR WTA hits ≥ 5 of 7, validates hypothesis — test threshold drops (seasoning 7→5, max_div 2.5→3.5, clay edge 20%→17%) in sequence at Rome next week.

- **PROP_CAREER_FADE live monitoring (v25.87, shipped 2026-04-24).** New NBA prop channel flipping OVER→UNDER when market median ≥ 1.0 below career avg. Shipped at 5u live (not shadow) based on 0-4 OVER / 4-0 flip backtest. Decision triggers:
  - **Kill-switch:** if WR < 40% on n≥10 live fires → disable
  - **Expand scope:** if WR ≥ 70% on n≥10, evaluate MLB (currently 2-0 on too-thin sample) at n≥5 matched
  - **Tune threshold:** if 7+ of 10 flips win, consider relaxing gap threshold from 1.0 → 0.75 to catch more
  - Track channel P/L separately in morning briefing — requires `side_type='PROP_CAREER_FADE'` segmentation



- **BetMGM CLV trip-wire** — alert if next 20 picks drop below 0 avg CLV
- **Steam sharp opposes** — shadow at -20u 2nd-half cumulative (currently -14u, 6u buffer)
- **Away fast-paced baseball** — shadow not applied (baseball factor profitable +14u); if baseball drops below +5u, revisit
- **Home letdown spot** — shadow at -15u 2nd-half (currently -8u, 7u buffer)
- **v25.43 NCAA midweek shadow** — decision at n=25 (currently 3)
- **MLB Wednesday** — revisit at n=25 (currently 10, -19u)

---

## 🟡 LONGER-HORIZON (from earlier sessions, not today)

- **Secondary data-driven spread model** — coexist with Elo, starts ~2026-05-11
- **NBA market-consensus baseline redesign** — swap Elo for market-consensus on NBA spreads (next big project per memory)
- **Context Model live-vs-backtest reconciliation** — v25.55 tracker comparing live P/L to Phase A backtest, evaluate at n≥100 live fires

---

## 📖 REFERENCE FILES (saved in repo)

- `data/CHANNELS.md` — every pick-generating channel (what it does, edge source, interactions, gates, live record)
- `data/MODEL_GLOSSARY.md` — broader glossary + version history
- `data/shadow_factors.md` — disabled context adjustments
- `scripts/*_backtest.py` — 6 inverse-backtest scripts saved for future recalibration

---

## 🟡 Soccer Context Path 2 calibration (v25.65 re-enabled with refined rules)

**Status:** v25.63 full halt **reversed** on 2026-04-22. v25.65 re-enables soccer Path 2 with per-sport × direction rules. Two validated UNDER cohorts fire live; everything else shadows or blocks.

**What inverse backtest revealed:**
- FADE loses everywhere at scale (-5.34u vs +101u FOLLOW on n=133 across all sports)
- Soccer FOLLOW is the MOST profitable Context sport: +55u on 37 picks (64.9% WR, +1.51u/pick)
- Two specific cohorts invert: EPL UNDER (fade +5.30u on 9), MLS UNDER (fade +2.80u on 5)
- Context projects UNDER ~90% historically — today's 9-OVER spike was the anomaly, not the norm
- Today's all-OVER slate reflects late-April scoring environment rising above market lines

**v25.65 rules (in `CONTEXT_TOTAL_P2_SOCCER_RULES`):**
| League | OVER rule | UNDER rule | Why |
|---|---|---|---|
| Serie A | shadow | 0.30 | +18.95u backtest (7-1), new to scope |
| Ligue 1 | shadow | 0.50 | +30.37u at 0.50+ (3-0) |
| Bundesliga | shadow | shadow | n=1 each direction |
| MLS | shadow | **block** | UNDER fade cohort (5 picks, 40% WR) |
| EPL | shadow | **block** | UNDER fade cohort (9 picks, 37.5% WR) |
| La Liga | shadow | shadow | n=1 each direction |
| UCL | shadow | shadow | n=1 each direction |

**Remaining work:**

1. **Build OVER-direction sample.** All OVER-side picks currently shadow-log to `shadow_blocked_picks` with reason `CONTEXT_TOTAL_P2_SHADOW_INSUFFICIENT_SAMPLE`. Re-evaluate at n≥15 per league × OVER direction. Compare shadow-logged projections to actual outcomes.

2. **Re-validate MLS / EPL UNDER at n>=15.** Current blocks are based on small samples (5 and 9). If the fade signal is real, we should see it in larger samples — then consider adding actual FADE logic (bet opposite direction) rather than just block.

3. **Validate Bundesliga and La Liga** UNDER at n>=10 before promoting from shadow to live.

4. **Add daily-per-sport cap on Context Path 2 picks** (target: max 5/sport/day) to prevent correlation pileup like today's 9-MLS-OVER scenario. Separate from direction rules.

5. **Monthly monitoring** of NBA/NHL/MLB Context Totals to make sure they stay calibrated.

6. **Root-cause the Phase A discrepancy** — v25.47 code comments said MLS was 15 picks at 66.7% WR but independent 90d backtest found only 8 MLS samples total. Odds-table 7-day retention is a partial answer but the Phase A 15-pick figure should be reproducible from somewhere.

---

---

## 📆 FROM 2026-04-22 AGENT SWEEP

**Closed same day:**
- ✅ `scripts/grader.py:29` — dropped unused `timezone` import
- ✅ `scripts/props_engine.py` — dropped unused `math` and `kelly_label` imports
- ✅ `scripts/main.py:2615` — `_validate_picks` bypass now includes `DATA_TOTAL`, `PROP_FADE_FLIP`, `FADE_FLIP` (aligns with `_passes_filter`)
- ✅ `scripts/model_engine.py:2027` — v25.60 fade-flip veto now calls `_log_divergence_block` + `skip_div += 1` before `continue` (observability fix)
- ✅ `scripts/upload_db.py` — compress SLIM DB instead of 3.2 GB full DB (slim is ~30 MB vs 279 MB); switch to `upload --clobber` to preserve release + tag if upload fails. Root cause of `db-latest` 404: 279 MB upload was timing out, leaving release in draft with 0 assets.
- ✅ IG card re-posted: bet 973 (TOR UNDER 224.5, +4.55u) added to 4/21 results card by bumping `created_at` from 4/18 to 4/21 (card filters on `MAX(DATE(created_at))`). `graded_at` untouched.

**Next-session investigations — CLOSED SAME DAY (2026-04-22 afternoon):**
- ✅ DraftKings post-Apr-17 CLV: full-season +0.2% (agent's -0.50 was pre-rebuild contamination). Post-fix (≥4/17) DK is +5.15u / 11-10 / +0.4% CLV. Remaining bleed was NCAA BB UNDERs; v25.56 HARD_VETO_DK_NCAA_BB_UNDERS already handles it. No new action.
- ✅ BetMGM: 63 bets, 33-30, -20.85u, CLV +0.23% (better than agent's -29u). Losses diffuse across sports/markets; no cohort n≥5 structural. Keep on WATCH with trip-wire: CLV < 0 on next 20 picks.
- ✅ MLB Wednesday: n=10 variance-scale; CLV positive; revisit at n≥25.
- ✅ NCAA Baseball Friday: UNDER drag was DK-concentrated (5-5-1 on non-DK books = neutral). v25.56 already handles. OVERs are -2.55u juice bleed, not structural.
- ✅ Pitcher_outs: 0 picks fired (gates catch all). Actual vs projected is 3-2 model-closer on 5 test pitchers; gaps small. Need real fires to evaluate.
- ✅ Away fast-paced: SPLIT BY SPORT. NCAA BB +10.24u, MLB +3.81u (keep active). NHL -6.5u season / -20.6u last 14d (playoff bleed). **Shipped v25.62 NHL-only shadow** (2026-04-22).
- ✅ Steam: sharp opposes: v25.35 already covers NHL + NCAA BB (shipped 2026-04-20 08:59). Remaining MLB cohort is 2-1 +3.56u — do NOT extend gate. NBA has no data. Documented in shadow_factors.md monitoring section.

**In-day watch items (today):**
- 👁 v25.43 NCAA midweek shadow — first full live day. Confirm midweek totals that previously fired at ~20% edge no longer do.
- 👁 Chase Field (1W-3L, -10.4u) — if DIA hosts a total, verify PARK_GATE fired correctly.
- 👁 Context-free props (Cortes UNDER 0.5 K pattern) — watch for multi-pick pattern of props firing with `context_confirmed=0`.

---

## 🔴 OPEN CRITICAL — TOP PRIORITY

### 1. Secondary spread model (data-driven) — coexist with Elo model + fade flip

**Concept:** Keep the existing Elo-based spread model AS-IS. It's our "divergence
detector" — its wrongness feeds `SPREAD_FADE_FLIP` (+140u backtest). Build a
SECOND spread model alongside it that uses real inputs to find genuine spread
edges the Elo model misses.

**Why not replace Elo:**
- Fade flip is actively printing money because Elo is broken in playoffs
- Replacing Elo would kill that edge for an unproven new model
- Both can coexist — different games get picked by different engines

**Proposed architecture:**
```
For each game:
    elo_spread      = existing Elo-based projection
    data_spread     = NEW model (injuries, lineup, rest, motivation, H2H, form)
    market_spread   = best market line

    Path 1 — FADE_FLIP (keep):
        IF |elo_spread - market_spread| > max_div:
            SPREAD_FADE_FLIP fires  (opposite side of Elo)

    Path 2 — DATA-DRIVEN PICKS (new):
        data_edge = (data_model vs market) at best book
        IF data_edge >= 20% AND |data_spread - market_spread| < max_div:
            Fire own-pick at market line  (real edge)

    Path 3 — BOTH models agree AND market disagrees (rare, high conviction):
        Stake boost (+1u) or fire at lower edge threshold
```

**Data inputs to build (ranked by impact):**
| Input | Source | Complexity | Impact |
|-------|--------|------------|--------|
| Injury list (starters out) | ESPN injuries API | Medium | 🔥 Biggest single win |
| Confirmed starting lineup | ESPN boxscore pre-game | Medium | Captures rest decisions |
| Rest days / back-to-back | Schedule data (have) | Easy | 1-2 pts per B2B |
| Motivation (seeding/tanking/elim) | Standings + rules | Hard | Big playoff impact |
| Recent form vs season avg | Existing game_results | Easy | Hot/cold streaks |
| H2H history | Existing game_results | Easy | Matchup-specific |

**Integration points:**
- `context_engine.py` already has `spread_adj` infrastructure — add new adjustment types
- Each input contributes spread_adj delta applied to `ms` (e.g., star out → -5 pts)
- Keep elo_spread computation untouched (for fade flip continuity)
- Add `data_spread` as a parallel output used for Path 2 picks

**Scope:** 2-4 weeks of evening work. Most time is API plumbing + player name normalization. Model changes are small.

**When to start:**
- Wait 3-4 weeks to let SPREAD_FADE_FLIP mature (2026-04-20 → 2026-05-11 minimum)
- If fade flip win rate drops below 55% before then → start earlier
- If fade flip holds 60%+ → start 2026-05-11 as planned

**Success metric:** Data-driven picks produce +20u+ over a 2-week backtest on historical spreads where fade flip didn't fire (i.e., games inside `max_div` threshold).

---

## 🔬 TOMORROW (2026-04-21) — Review today's picks against my concerns

Today I flagged concerns on 4 of 6 live picks. Each concern maps to a
potential new gate. Tomorrow morning, check which concerns validated
against actual results — if the feared outcome happened, promote that
concern to a backtest + potential gate. If picks won despite concerns,
note that the model was right and I was overcautious.

### Per-pick review checklist

**Bet 986 — ATL @ WSH OVER 8.0 (MLB) | Concern: hot pitcher / blend mismatch**
- Bryce Elder has 0.77 ERA in 2026 (4 starts, 23 IP); our model blended to 4.53
- If UNDER hit → add to backlog: `MLB_PITCHER_HOT_BLEND_GATE`
  - Trigger: block MLB totals when starter's last-5 ERA < 2.0 AND blended ERA > 3.5
  - Backtest: pull 30 days of similar mismatches
- If OVER hit → I was overcautious, blend is working

**Bet 987 — Zach Hyman UNDER 2.5 SOG (NHL prop) | Concern: zero context factors**
- No context factors shown at all on this prop pick
- If LOSS → add to backlog: `PROP_NO_CONTEXT_WARN`
  - Trigger: warn/shadow-block NHL props with empty context_factors
  - Risk: projection-engine props often have no context by design — may over-block
- If WIN → context absence is not a signal

**Bet 988 — Ayo Dosunmu UNDER 13.5 (PROP_FADE_FLIP) | Confidence: high**
- Third live PROP_FADE_FLIP; yesterday's pattern was 2-0 on Pritchard+Cunningham
- If WIN → fade-flip running 3-0 in 2 days — consider stake boost review at 15 picks
- If LOSS → first blemish; too early to change anything, note for tracking

**Bet 989 — Sam Merrill OVER 8.5 pts (NBA prop) | Concern: low-usage, top-of-range**
- Merrill is CLE's 3-point specialist; 8.5 is high for his usage
- If LOSS → add to backlog: `NBA_PROP_USAGE_GATE`
  - Trigger: NBA prop where player's season usage rate < 18% AND bet line is > player's P75
  - Backtest: pull 30 days of low-usage-high-line NBA props
- If WIN → low-usage at top range isn't systematically bad

**Bet 990 — UCSB @ Cal Baptist UNDER 13.5 (BOOK_ARB) | Concern: just-posted line**
- Lines posted ~3:00 PM, pick fired 3:13 PM — FD may have converged to DK by the time user saw email
- If LOSS → **escalate BOOK_ARB_LINE_STABILITY_GATE from TODO to this-week build**
  - Trigger: require 60 min of stable line data before firing BOOK_ARB
- If WIN → was a valid arb after all; stability gate stays on backlog

**Bet 991 — Minnesota Timberwolves +7.5 (SPREAD_FADE_FLIP) | First live NBA fade-flip**
- This is our first-ever live SPREAD_FADE_FLIP graded result
- If WIN → sample=1 toward the 15-pick pull-trigger threshold; confidence builds
- If LOSS → sample=1 L; note and continue to 15-pick sample before any action

### Additional systematic reviews tomorrow

1. **SPREAD_FADE_FLIP running total** after one full day of live data
2. **Context Model DATA_SPREAD fires** — did the 4pm+ runs produce any NHL/MLS/EPL DATA_SPREAD picks? What did they do?
3. **Concentration cap fix verification** — no cross-type blocks since v25.38 deployed
4. **Direction validator bypass verification** — no `value is on X` BLOCKED messages for fade/Context picks

Check morning_briefing.md + auto_run.log for each.

### NBA redesign — Phase A data inventory (1 hour scope job)

Before we start the multi-week NBA market-consensus baseline project,
verify the data pipeline has what we need:

1. Check `market_consensus` table schema (does it exist, what's stored?)
2. For last 30 days of NBA games: can we reconstruct sharp-book median
   spread (FD, BR) at any point in time from the `odds` table?
3. Identify the 3-4 most important book-fetches per event for a robust
   consensus (FD + BR minimum; DK + BetMGM for soft-side comparison)
4. Run one test: compute market_median spread for 10 recent NBA games
   and compare to actual closing line. Is our consensus close to sharp
   closing line?
5. Spec the data-side dependencies before any code changes.

**Why tomorrow:** clears runway for the NBA redesign when fade flip
matures (~2 weeks). Don't start the 3-4 week build until we know the
data foundation is solid.

**Effort:** 30-60 min of SQL exploration + a scratch script. No code
changes, no shipping. Just a scoping exercise.

## 🟡 OTHER OPEN

### ⚠️ Backtest accounting nuance — don't overstate Context Model edge

**Problem identified 2026-04-21:** the Phase A backtest for Context Model
Path 2 (v25.44 spreads +110u, v25.46-49 totals +206u, combined +360u/30d)
measured hypothetical Context picks in isolation. It did NOT subtract
overlap with the existing Elo edge model, which also fires on many of
the same games.

**The three cases (per-pick):**
1. **Duplicate (~60-70% of Context picks):** Elo edge also fires same
   direction. Concentration cap keeps the higher-edge pick (edge model's
   non-zero edge_pct beats Context's edge_pct=0). Context's P/L in this
   case is phantom — the edge model would have captured the same P/L.
2. **Replacement (~10-15%):** Elo fires opposite direction, Context
   disagrees. v25.52 veto blocks edge, Context fires own. Real swap.
3. **Net-new (~20-25%):** Elo doesn't fire at all (<20% edge), Context
   fires its own. Truly additive.

**Realistic net incremental edge from today's Context buildout:**
- Spread Path 2 backtest +110u → ~+30-50u actually new
- Total Path 2 backtest +206u → ~+60-80u actually new
- Plus v25.52 direction veto: +41u (clearly additive)
- Plus one-off gates (v25.42, v25.56, v25.41, bet 973): +50-80u
- **Total realistic: ~+150-200u/month of NET NEW edge**

**How to validate honestly:**
The v25.55 context_tracker compares live per-channel P/L vs backtest.
After 2 weeks of live data:
- If live P/L ≈ backtest → duplicates weren't a big issue (my estimate was pessimistic)
- If live P/L ~30-40% of backtest → duplicates dominated (my estimate was right)
- If live P/L ~zero → Context is almost entirely duplicating Elo

**Lesson for future backtests:** always estimate overlap with existing
channels before claiming "new" edge. A +X/30d backtest for a parallel
engine needs the duplicate subtraction, not just a raw P/L sum.

### Prop Context Model overhaul — Phase A complete (2026-04-21), parked

**Phase A audit finding:** Prop system is NOT broken. No big overhaul needed now.

- Post-v25.13 prop record: **17-10 (63% WR), +28.6u on 27 picks**
- No book × stat cohort shows structural loss (unlike DK NCAA UNDERs -61u)
- BLOWOUT_GATE + PROP_DIVERGENCE_GATE already catching failure modes (3K+ blocks each)
- All 4 channels profitable (PROP_OVER +1u, PROP_UNDER +11u, PROP_FADE_FLIP +9.5u, PROP_BOOK_ARB +3.2u)
- Biggest arguable issue: 15-20% edge bucket 3-5 -8u (small sample, possibly variance)

**Realistic overhaul ROI** (applying overlap-accounting lesson):
- 2-3 weeks of work
- **+15-30u/month net-new edge** (not the +50-80u earlier estimate)
- Worst ROI ratio of anything on the board vs tennis Context (-11.8u bleeding) or NCAA baseball extension

**Revisit triggers:**
- Overall prop WR drops below 50% on 30+ picks
- Specific book × stat / sport × stat cohort drops below 40% WR at n≥15
- v25.55 tracker shows specific channels regressing

**If/when we revisit:**
- Phase B: build context_prop_model.py with minutes projection, usage-shift, defender matchup
- Phase C: PROP_DIRECTION_VETO analog to v25.52
- Phase D: shadow-mode NBA first, then NHL/MLB

**Data gaps:** pre-game ESPN boxscore scrape for minutes, historical injury
snapshots (inferable from box_scores absence), defender matchup (needs lineup data).

### NCAA baseball — sport-specific Context Model extension (future project)

Context Model currently runs `compute_context_total()` on NCAA baseball
but doesn't trust its output — Phase A backtest (2026-04-21, four attempts
covering form, H2H, sparse pitcher_stats, team_pitching_quality, walk-
forward runs-allowed) all lost at every threshold. Context's directional
signal on NCAA baseball is essentially random (DISAGREE cohort +0.47u —
vetoing would hurt).

**What would be needed to make NCAA baseball Context-viable:**

1. **NCAA pitcher matchup signal (clean version)** — port edge-model's
   `pitcher_scraper.get_pitcher_context()` into Context. Uses
   `team_pitching_quality` day-of-week aggregation + confirmed starter
   ERA. Current sparse pitcher_stats fallback (7% coverage) doesn't cut
   it; team_pitching_quality (73% coverage but stale Mar 27) is closer
   but needs walk-forward updates.

2. **Conference-tier baseline** — NCAA baseball has huge scoring variance
   between SEC/ACC (high-scoring) and Ivy/WCC (low-scoring). A flat
   league_avg=11.5 doesn't work. Need conference-specific baselines.

3. **NCAA park factor catalog** — 250+ venues. Either scrape ESPN venue
   data or manually curate top-50 by volume. Context's MLB park signal
   (`_mlb_park_factor_delta`) doesn't exist for NCAA yet.

4. **Day-of-week ace rotation integration** — Friday = ace, Sat = #2,
   Sun = #3, Midweek = bullpen. Context's `_team_form_total_delta` is
   pace-agnostic; NCAA needs DOW-awareness in the baseline.

5. **Home/road scoring splits** — college teams show bigger home-field
   effects than MLB. Travel in mid-majors is harsh.

**Effort:** 3-5 days of signal engineering + backtest validation.

**Decision criteria:** If Phase A post-extension shows ≥55% WR +15u+/30d
on NCAA baseball with the expanded signal set, ship. Otherwise keep
NCAA baseball on the edge-based model (which IS profitable +14.4u season).

**Trigger to start:** when we have bandwidth after tennis Context or
when NCAA baseball regresses below +5u/season. Current DK UNDER veto
(v25.56) + existing DK gates already capture the biggest loss cohort.

### Tennis — separate Context Model engine (future project)

Tennis is structurally incompatible with the team-sport Context Model. It
needs its own module (`context_model_tennis.py`?) with tennis-specific
signals:
- Surface (clay / hard / grass) + player surface-specific Elo (partially exists)
- Round (R32 / R16 / QF / SF / F — late rounds differ in fatigue + intensity)
- Ranking gap + ranking trend
- Recent form on this specific surface
- Head-to-head on this surface (different from general H2H)
- Fatigue (previous-match duration, consecutive days played)
- Tournament tier (Grand Slam / Masters / 500 / 250)
- Home country advantage

**Current state:** Tennis runs on edge-based Elo model only. Season record
is -11.8u — a net drag. Surface-split Elo exists (`tennis_atp_clay` etc.)
but no Context layer.

**Effort:** 1-2 weeks — separate code path, backtest on 30+ days of
ATP/WTA data across all surfaces, ship as Path 2 own-picks.

**Trigger to start:** when tennis volume becomes material (Grand Slam
lead-up weeks) OR fade flip on spreads matures and we have bandwidth.

### ~~Absorb remaining sports into Context Model~~ — INVESTIGATION COMPLETE 2026-04-21

**Outcome:** Context Model has reached its natural sport scope. Remaining
sports either cannot absorb cleanly (NCAA baseball, NCAAB) or need a
separate engine (tennis).

**NCAA baseball — 4 Phase A attempts all lost:**
- Form + H2H only: 47% WR, -206u @ 0.30 thresh
- + sparse pitcher_stats (7% coverage): 47% WR, -170u
- + team_pitching_quality (stale Mar 27 snapshot, 73% coverage): 47% WR, -290u
- + walk-forward runs-allowed (73% coverage): 48% WR, -220u

**Extending CONTEXT_DIRECTION_VETO to NCAA baseball — also loses:**
Context AGREES cohort 36-33 -12u / DISAGREES cohort 13-11 +0.47u.
Veto would strip +0.47u from live P/L. Don't extend.

**Why Context can't absorb NCAA baseball:** the edge-based totals model
wins (+14.4u season) via integrated team_ratings + pitcher_scraper +
selective 20% edge threshold. Context's market-anchored formula doesn't
replicate that integration; porting it would essentially rebuild the
edge model under a new name.

**Decision:** Accept current Context scope (13 sport×market slices —
NHL/NBA/Serie A spreads; NBA/NHL/MLB/MLS/La Liga/Bundesliga/Ligue 1
totals). Remaining sports stay on edge-based model. Tennis parked as
separate project.

### STAKE_BOOST for Context+Elo hard stack picks (v25.53 candidate)

**Concept:** When both Elo edge AND Context Path 2 fire on the same event in
the same direction, boost stake above the default 5u. Backtest shows this
is the highest-EV cohort in the system.

**30-day backtest (Context-scope sports, post-v25.52 world):**
- **Hard stack** (Elo fires + Context past Path 2 threshold, same direction):
  28-15, **65.1% WR, +34.4u on 43 picks, ROI +16.0%**
- Soft agree (Elo fires + Context agrees but below threshold): 21-17, 55.3% WR,
  +2.5u on 39 picks, ROI +1.3%
- Disagree (Elo fires + Context disagrees — now blocked by v25.52): 15-20,
  42.9% WR, -41.2u on 36 picks

**Proposed stake ladder:**
- Hard stack: 7-8u (boost 40-60%)
- Soft agree: 5u (unchanged)
- Context-only Path 2: 5u (unchanged)
- Elo-only edge picks (Context out of scope): 5u (unchanged)

**Expected incremental: ~+15u/month** on top of current hard-stack contribution.

**Implementation:** Add a post-filter in _merge_and_select (or in model_engine
when firing) that checks if both Elo edge and Context Path 2 are triggering
the same event+direction. If yes, set units = HARD_STACK_UNITS (7-8).

**Why parked:** User wants to verify other models are working correctly first
before introducing stake-variable complexity.

### Player-prop investigation — continue beyond v25.41 starter-role gate

**Context:** Reid Detmers UNDER 4.5 HITS ALLOWED (bet 994, 4/20) lost 5u.
Investigation surfaced that pitcher props fire rarely (4 total live picks
ever) and UNDER pitcher props were enabled 4/14 with **no walk-forward
backtest** (`walk_forward_props.py` and `prop_backtest.py` both filter
`side='Over'`). Root cause for Detmers specifically: reliever→starter
role change — 20-game baseline was polluted with 2025 bullpen appearances
(0-1 hits/appearance), making UNDER 4.5 look like a 90% hit rate when
he's actually 2-for-4 as a starter.

**Shipped 4/21:** v25.41 starter-role gate in `player_prop_model.py` —
`get_player_baseline` and `get_full_season_rate` now filter pitcher
baselines to games with `pitcher_outs >= 12` (≥ 4.0 IP); require
`MIN_STARTER_GAMES = 6`. Apr 14-20 walk-forward: UNDER pitcher props
flip from -0.3u (13 picks, 31% hit) to +10u (31 picks, 61% hit);
Detmers correctly blocked.

**Still open — follow-ups user wants investigated:**
1. **UNDER props walk-forward backtest.** `walk_forward_props.py`
   hard-codes `side = 'Over'`. Extend to include UNDER and run against
   all available prop_snapshots + box_scores history. Until we have this
   we are running UNDER pitcher props with ~7 days of live data.
2. **Pitcher-prop volume diagnosis.** Only 4 pitcher props have ever
   fired live (Okamoto K, Goodman K, Lorenzen H-allowed W, Detmers
   H-allowed TAINTED). Dig into why — MLB_PITCHER_MIN_IP_CURRENT_SEASON,
   MLB_PROP_WINDOW_HOURS=3, BLOWOUT_GATE, MIN_PROP_ODDS=-150,
   MIN_EDGE_PCT=10, MAX_PROP_PICKS=3 cap. Are we blocking good picks?
3. **Rookie volatility.** Misiorowski (rookie starter) showed up as
   multi-time false-positive in the walk-forward (86% UNDER conviction,
   LOST). Consider a `games_as_starter < 12` penalty or separate
   rookie treatment.
4. **UNDER shadow mode consideration.** Even with v25.41 the UNDER
   sample is 31 picks over 7 days. User originally offered to
   shadow-mode UNDER pitcher props entirely; current choice is to keep
   them live with the starter gate. Revisit after 30 days of data.
5. **Role-change detection for batter-facing metrics.** Same pattern
   could exist for batters (pinch hitters, spot starters, position
   changes affecting batting order). Not an issue today but worth
   auditing once pitcher side is settled.

**Why this is parked here:** user is not done with player props but is
moving to the daily to-do list from this morning's agent outputs.
Return to items 1-5 after daily work is triaged.

### ~~Minimum line-stability time gate for BOOK_ARB~~ — SHIPPED v25.42 (2026-04-21)

Shipped BOOK_ARB_LINE_UNSETTLED gate. Requires each book's opener to
have been in our `openers` table for ≥ 60 min before a BOOK_ARB fires.
Implemented via a shared helper `_arb_lines_stable()` used by all three
BOOK_ARB assembly sites in `main.py` (NCAA baseball v25.25, multi-sport
v25.28 totals + spreads).

**Backtest on all 4 historical fires:**
- id=958 MLB TOR (TAINTED)  — Caesars 132 min old → ✓ fire
- id=959 MLB MIL (TAINTED)  — Caesars 132 min old → ✓ fire
- id=973 NBA TOR@CLE U (TAINTED -5u) — 11 min → 🚫 BLOCKED
- id=990 NCAA UCSB U (LOSS -5u) — 14 min → 🚫 BLOCKED

Both fresh-line trap losses (the UCSB case from the post-mortem and an
earlier NBA case) would have been blocked; the 2 mature-opener MLB arbs
still fire. Surgical.

**Knobs:** `BOOK_ARB_MIN_OPENER_AGE_MIN = 60` (inline constant, adjust
at the helper site).

**Monitoring:** watch `shadow_blocked_picks` for `BOOK_ARB_LINE_UNSETTLED`
reasons. If the gate blocks too aggressively over 2-3 weeks, consider
dropping to 30 min.

---

Other monitors unchanged from pre-session list.

---

### Instagram Graph API migration (future — required to unlock paid ads)

**Current state:** We post to Instagram via `instagrapi` (unofficial
library that logs in with username/password and mimics the Android
mobile app). This triggers Meta's "inauthentic activity" detection,
which blocks the account from running paid ads via Ads Manager.
Organic posting continues to work fine.

**Source (researched 2026-04-20):** Meta's Terms prohibit automated
account use except through "authorized routes" — specifically the
Instagram Graph API via Meta Business Suite. instagrapi is NOT an
authorized route. See:
- https://transparency.meta.com/policies/community-standards/account-integrity/
- https://transparency.meta.com/policies/ad-standards/

**What restores ad eligibility:** Migrate
`social_media.py:post_reel_to_instagram()` and the photo/story posters
to use Meta's **Instagram Graph API** instead of instagrapi:
- Requires a connected Facebook Page + approved Meta app
- Must use access tokens (not username/password)
- Some format restrictions — Stories have limited support via Graph;
  some Reel formats need specific structures
- After ~30 days of clean posting via authorized API, reapply for
  ad eligibility

**Effort:** 1-2 weeks dev time + Meta app review process

**When to revisit:** If/when paid ads become a real growth lever. Current
organic reach is fine; followers are growing on content quality. Ads
would accelerate but aren't critical today.

**Trigger:** follower growth plateau for 2+ weeks, OR we commit to paid
boosting as part of the growth playbook.

**Do NOT revisit before:** finishing NBA spread redesign + fade flip
maturing to 15+ live picks. Dev bandwidth needed elsewhere.

## ✅ COMPLETED (2026-04-11)

- [x] **Retasked all 15 hourly `ScottysEdge_XXAM/PM` scheduled tasks to use `auto_run_afternoon.bat` wrapper** — verified via `schtasks /Query`. All 15 tasks (06AM through 08PM) now point at the bat wrapper instead of calling `python.exe` directly. Every future run will capture both stdout and stderr to `auto_run.log`, and combined with v25.9's uncaught exception handler, the pipeline is now completely observable — no more silent failures.

## ✅ RESOLVED THIS SESSION (2026-04-11)

### Infrastructure + Observability
- [x] **db-latest upload pipeline** — fixed `upload_db.py` with `--draft=false` so cloud agents can fetch the DB
- [x] **`Retention pruning: timedelta not defined` silent bug** — fixed cmd_grade timedelta import (v25.7)
- [x] **ESPN backfill DB connection leak** — wrapped `backfill_missing` + `backfill_thin_teams` in try/finally (v25.7)
- [x] **Pipeline observability** — uncaught exception handler added to `main.py` (v25.9). Any future crash writes full traceback to `pipeline.log`.
- [x] **🔥 `_social_media_card` NameError** — **ROOT CAUSE of today's silent pipeline failures.** One undefined variable `tu` in an unwrapped code block killed every "has picks" scheduled run. Fixed in v25.10.

### Briefing + Stats Card Fixes
- [x] **Shadowed factor filter** — briefing stops showing phantom alerts (v25.7)
- [x] **Tennis lumping** — single "TENNIS" row in both stats card and morning briefing (v25.7 + v25.8)

### Twitter Cleanup Residue
- [x] `#GamblingTwitter` hashtag removed (v25.7)
- [x] 152-line `generate_thread()` dead function removed (v25.7)
- [x] X handle dropped from `SOCIALS` constant (v25.8)

### Dead Code / Unused Imports
- [x] `home_away_split_adjustment` removed from `context_engine.py` (v25.7)
- [x] `calculate_true_vig` removed from `scottys_edge.py` (v25.7)
- [x] 9 unused imports purged across 7 files (v25.7)

### Pre-Run Validator Agent Prompt Fixes
- [x] Removed phantom gate checks: `CONCENTRATION_CAP`, `MIN_EDGE`, `BLOWOUT_GATE` (zero-block false alarm), tennis `clay`
- [x] Agent now queries real gate names: `DIRECTION_CAP`, `SPORT_CAP`, `GAME_CAP`, `CROSS_RUN_CAP`, etc.
- [x] Saved `reference_gate_reason_strings.md` to memory so this discovery persists

### Apr 6–10 Drawdown Post-Mortem
- [x] Investigated, traced all -26.4u of losses to v24-v25.3 fixes already shipped. Net new action items: zero.

### Same-Day Rescues
- [x] Today's 10 picks manually posted to IG story (both card slides)
- [x] Glossary carousel posted to IG feed (pinnable — 2 slides)
- [x] Picks email sent manually (subject: "🎯 RESCUE — Morning Picks — 2026-04-11")
- [x] Daily Reel posted to IG with 6-2 +13.4u recap

## 🟡 Today-Specific (check if still relevant)

- [x] **TOR/ATL power ratings sanity check** — stale, resolved 2026-04-18. Picks fired on these teams post-4/11 went 2-1 (+3.4u); model calibration is fine.
- [x] **`pitcher_scraper` Imai classification** — stale, resolved 2026-04-18. Imai has only 7.5 IP total across 3 starts — well below the 30-IP ERA gate, so he can't affect model calculations. Original note confirmed "not a bug."

## 🟢 Monitor / Backlog

- [ ] **Daily check:** Home letdown spot 2nd-half P/L (currently -9.1u, threshold -15u, cushion 5.9u)
- [ ] H2H high-scoring factor — 1W-2L, -5.3u (watch for accumulation)
- [ ] Chase Field park double-counting — 1W-3L, -10.4u (already reduced from +0.9 → +0.6 in v23.1)
- [ ] Add tennis surface-specialist Elo override (deferred until tennis returns — clay tightening in v24 may already be sufficient)
- [x] **v24 two-week review** — completed Apr 18 (2 days early). Verdict: successful. CLV +0.21 → +0.41; record -18u is variance on 88 bets (CI [39.6%, 60.4%] includes pre-v24 56.8%). See `project_v24_review_apr18.md`. Next review: May 2.

### Cleanups completed 2026-04-18

- [x] **`config.py` SPORT_CONFIG drift** — removed dead-code copy. Runtime uses `model_engine.py` only; verified all live imports.
- [x] **Drop `prop_snapshots_pre_v25_2`** — already dropped in a prior session, confirmed via DB scan.
- [x] **Verify `model_total` writing to `market_consensus`** — column exists but 0/10,499 rows populated. Not a bug (nothing reads it), but documented: no live writer, low-priority cleanup if we ever reclaim the column.

### Added 2026-04-18 (moved from in-session task list — passive, sample-gated)

- [ ] **NBA props sample** — currently 3W-7L, -17.1u, avg CLV +1.32. Flag if WR stays &lt;40% through 20 picks. Trigger: n ≥ 20.
- [ ] **NCAA Baseball NO_MOVEMENT signal** — 1W-5L, -20.6u when sharps don't move the line. Possible fade candidate. Trigger: re-check at n ≥ 15 (currently n=6).
- [ ] **NBA SHARP_CONFIRMS stake-boost decision** — zero picks fired with this signal yet. Per `project_steam_monitor.md`, decision point at 15-20 picks.
- [ ] **NBA model calibration review** — 28W-27L, -7.7u season-to-date despite positive CLV. Audit recommends full calibration review at the May checkpoint.
- [ ] **ATP tennis surface/round model** — Monte Carlo closed 1-3, -10.7u (n=4, small sample but early indicator). Review surface-specific calibration and round-filter before the next clay event.

## 📝 Do NOT Act On (stale phantom alerts)

Morning briefing items referencing already-shadowed factors:
- Home fast-paced (shadowed v21)
- Away bounce-back (shadowed v21)
- Altitude (shadowed v21)
- Home hot streak (shadowed v21)
- Away revenge (shadowed v21)
- Away letdown (shadowed v24)
- Friday game (resolved — was driven by NCAA UNDERs that are now blocked)

These are ghost alerts from historical data. The briefing-script suppression fix (v25.7) prevents new phantom alerts going forward, but older briefings may still reference these.

---

## Commits This Session (2026-04-11)

- `6365678` **v25.7** — Audit cleanup (8 items: db-latest, timedelta, ESPN try/finally, tennis lump, Twitter cleanup, dead code, unused imports, shadowed factor filter)
- `bec00c6` **v25.8** — Lump tennis tournaments in morning briefing + drop X from socials
- `a584761` **v25.9** — Pipeline observability (main.py uncaught exception handler)
- `580fd33` **v25.10** — 🔥 CRITICAL FIX: `_social_media_card` NameError `tu` (root cause of pipeline failures)
