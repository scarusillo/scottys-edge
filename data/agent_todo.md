# Scotty's Edge — Master Agent To-Do List
**Last updated:** 2026-04-22 — post-agent sweep + small code fixes + v25.62/v25.63

---

## 🟡 ACTIVE — Soccer Context Path 2 calibration (v25.65 re-enabled with refined rules)

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
