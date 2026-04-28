# Scotty's Edge — Model & Channel Glossary

## Naming terminology (v25.76 rename, 2026-04-22)

Old code + DB rows may reference "Path 1" / "Path 2" — these have been
renamed for semantic clarity going forward:

| Old name | New name | What it is |
|---|---|---|
| `Path 1` | `ELO_DIVERGENCE_RESCUE` | Context acts as a safety check when Elo disagrees significantly with market. Either vetoes a bad Elo pick or fires SPREAD_FADE_FLIP. |
| `Path 2` | `CONTEXT_STANDALONE` | Context fires its OWN pick when Elo agrees with market. Produces `DATA_SPREAD` (killed v25.70) and `DATA_TOTAL` (live) side_types. |

Historical bets in the DB still contain "Path 1" / "Path 2" in their
context_factors text — we can't retroactively rewrite stored data.
New picks use the semantic names in comments and log output.



**Purpose:** Plain-English definitions of every model, channel, gate, and signal
used to generate picks. Reference this when you forget what something does.

---

## 1. The two "brains" — models that produce a projection

### Elo Model (`elo_engine.py`, `model_engine.py`)

**What it does:** Rates every team based on game results — win by a lot, your
rating goes up a lot. Loss to a weak team, big drop. Surface-split for tennis
(separate clay / hard / grass ratings). The output is a predicted spread for
every matchup.

**Example:** "Celtics Elo 1700, Pistons Elo 1500 → Celtics -9 as the projected
spread." If market is Celtics -7, we have a 2-point edge. If edge ≥ 20%, fire.

**Status:** This is the PRIMARY engine. 385+ live picks post-rebuild, running
at +76u / 56% WR. Never formally re-backtested with inverse methodology.

---

### Context Model (`context_model.py`, `context_engine.py`)

**What it does:** A SECOND opinion. Starts with the Elo spread, then applies
real-world adjustments: injuries, recent form (5-20 game window), playoff
momentum, home-court advantage, pitcher quality, weather, park factors, etc.
Output is an "adjusted spread" or "adjusted total" that usually differs from
Elo by 0-5 points.

**Example:** Elo says Celtics -9. Context sees Tatum is out (injury -3), Pistons
on 1-day rest (+1 fatigue), recent form gap favoring Celtics (-2 form). Context
spread = -13. Market at -7. Context says Celtics are severely underpriced.

**Status:** Live. Used in multiple channels (see ELO_DIVERGENCE_RESCUE / CONTEXT_STANDALONE below).

---

## 2. Pick-firing CHANNELS — the different ways a bet can get generated

Think of each channel as a separate source of picks, each with its own rules.

### A. Edge-based Elo picks (the workhorse)

**Side-type:** `OVER`, `UNDER`, `DOG`, `FAVORITE`, `PROP_OVER`, `PROP_UNDER`
**File:** `model_engine.py`, `player_prop_model.py`

**What it does:** Elo projects a number, we compare to market, compute edge %.
If edge ≥ 20% (varies by sport), fire a pick at market line.

**How many picks fire:** ~70-80% of our volume. This is the main channel.

**Status:** Running since rebuild (3/4). Never formally recalibrated with
inverse backtest. Listed as next priority after DATA_SPREAD.

---

### B. Context Model ELO_DIVERGENCE_RESCUE — divergence rescue

**Side-type:** `OVER`, `UNDER`, `DOG`, `FAVORITE` (same as edge-based)
**File:** `model_engine.py` around line 1925

**What it does:** When Elo disagrees with market by a LOT (big divergence),
normally the edge model would fire a high-edge pick. But extreme divergence
often means Elo is wrong. ELO_DIVERGENCE_RESCUE checks Context. If Context agrees with
market (i.e., Elo is the outlier), the edge pick is BLOCKED. If Context
agrees with Elo, it fires.

**Example:** Elo says Celtics -15. Market has Celtics -7 (8-point divergence).
Normally edge fires at 20%. Context Model checks: Context says Celtics -8
(agrees with market, not Elo). ELO_DIVERGENCE_RESCUE vetoes the pick — Elo was probably
wrong because of stale ratings.

**Status:** Live. Saved us from numerous bad high-edge fires. Not inverse-
backtested but the veto is a safety mechanism, not a pick generator, so
less urgent to re-validate.

---

### C. Context Model CONTEXT_STANDALONE TOTALS — `DATA_TOTAL`

**Side-type:** `DATA_TOTAL`
**File:** `model_engine.py` around line 2723

**What it does:** Even when Elo AGREES with market, Context might disagree.
If Context total disagrees with market total by a per-sport threshold (e.g.,
1.5 runs in MLB, 0.3 goals in soccer), **fire an OWN pick at the market line**
betting Context's direction.

**Example:** Market Astros/Guardians total at 8.0. Elo total also ~8.0 (no
divergence). But Context says 9.9 because Astros' hitters are hot, Cleveland's
starter has a bad ERA, wind blowing out. Gap = +1.9 > MLB threshold (1.5).
**Fire OVER 8.0** at 5u stake, `side_type=DATA_TOTAL`, `edge_pct=0`.

**Key difference from edge-based:** No explicit edge % — fires on model
disagreement alone.

**Status:** Today was the FIRST day this ever fired live (bug fix in v25.59
unblocked it). Soccer scope refined today via v25.65 (direction-specific
rules). NBA / NHL / MLB still live with original thresholds.

---

### D. Context Model CONTEXT_STANDALONE SPREADS — `DATA_SPREAD` ← YOU ASKED ABOUT THIS

**Side-type:** `DATA_SPREAD`
**File:** `model_engine.py` around line 2089

**What it does:** Identical to DATA_TOTAL but for spreads. When Elo and market
agree on the spread but Context disagrees by more than a per-sport threshold
(NBA 2.5 pts, NHL 0.5 pts, Serie A 0.5), **fire an OWN pick at the market
line** for whichever side Context thinks is underpriced.

**Example (from today's pipeline):** OKC vs Phoenix. Market has OKC -17. Elo
also ~-17 (no divergence). But Context adjusts for Phoenix injuries + OKC
playoff momentum (single 35-point blowout) and projects OKC -25.2. Gap =
8.2 > NBA threshold (2.5). **Fire OKC -17.0** at 5u, `side_type=DATA_SPREAD`.

**Why this channel is different from DATA_TOTAL:** Same idea, different market.
Totals ask "will the combined score go over/under a number." Spreads ask
"will one team beat the other by a number of points/goals/runs."

**Status (today's backtest finding):** **BROKEN.** Independent 90-day backtest
shows NBA -5.65u, NHL -7.43u, Serie A +5.12u (but fade wins more everywhere).
Phase A claimed +25u NBA, +73.6u NHL — those numbers don't reproduce. Today
I scrubbed both DATA_SPREAD picks (Magic +9, OKC -17) and now we're debating
halting this channel entirely via v25.68.

**Plain English:** DATA_SPREAD says "market and Elo are close, but our deeper
Context model thinks one team is more/less favored than the market sees."
The bet is that Context knows something the market doesn't. The data says
it doesn't — Context spread adjustments appear to be noise at firing-level
disagreement thresholds.

---

### E. SPREAD_FADE_FLIP

**Side-type:** `SPREAD_FADE_FLIP`
**File:** `model_engine.py` around line 1980

**What it does:** When Elo disagrees with market by a HUGE amount AND Context
ALSO disagrees with Elo, bet the OPPOSITE of what our own Elo says.

**Example:** Elo says Celtics -15. Market has Celtics -7 (big divergence).
Context is checked — Context says Celtics -6 (also disagrees with Elo).
Conclusion: both second-opinion checks say Elo is wrong, so we FADE our
own Elo model by betting Pistons +7.

**Why this works:** When our own model is known-unreliable (confirmed by
independent Context), the market's price is more accurate than Elo's.

**Status:** Launched v25.36 (+140u backtest). v25.60 added dual-model veto
(Context must agree we're wrong). Not inverse-backtested on live data.

---

### F. PROP_FADE_FLIP

**Side-type:** `PROP_FADE_FLIP`
**File:** `player_prop_model.py`

**What it does:** Prop version of SPREAD_FADE_FLIP. When our prop projection
vs market median differs by a big amount AND market disagrees with our
direction, FLIP to the OVER/UNDER side market implicitly supports.

**Example:** Model projects Maxey 21.9 pts, market line 25.5. Model says
bet UNDER 25.5. But market median for this prop across books is 26.5 (markets
think OVER is more likely). Both our model and market can't both be right —
we flip to OVER 25.5 (the market's direction at the easier number).

**Status:** 5-1, +13.58u in live data (small n=6). Today's Cunningham OVER
27.5 was a PROP_FADE_FLIP. Not formally re-backtested.

---

### G. BOOK_ARB — game lines

**Side-type:** `BOOK_ARB`
**File:** `model_engine.py`

**What it does:** Two books open the same game at different lines. Sharp book
opens Total 223.5, soft book opens 224.5. The 1-point gap is a market
inefficiency. Bet the better side at the softer book.

**Example (today's TOR/CLE UNDER 224.5 win):** FanDuel opened total at 223.5,
DraftKings at 224.5. Gap +1.0. Bet UNDER 224.5 at DK (easier UNDER number).
Win 4.5 units.

**Status:** Multiple variants shipped v25.25-v25.28 for NBA/NHL/MLB/NCAA
Baseball. Not inverse-backtested.

---

### H. PROP_BOOK_ARB

**Side-type:** `PROP_BOOK_ARB`
**File:** `props_engine.py` / `player_prop_model.py`

**What it does:** Same as BOOK_ARB but for player props. Cross-book disagreement
on player lines.

**Status:** 3 live picks, 2-1 +3.17u. Insufficient sample.

---

### I. CLV_MICRO_EDGE (v25.80, shipped 2026-04-23)

**Tag:** `CLV_MICRO_EDGE` in `context_factors` string
**File:** `main.py` (inside `_passes_filter` of `_merge_and_select`, ~line 3150)

**What it does:** Lowers the 20% edge floor down to 13% for picks where the
consensus line has already moved ≥ 0.5 since opener — either TOWARD us or
AGAINST us. SPREAD/TOTAL only.

**Why this works:** The 16-18% edge bucket has a 45.2% rate of positive CLV
outcomes — higher than any 20%+ bucket (which cluster at 27-30%). Our model is
often just as correct on sub-threshold picks; the 20% floor was excluding
signal, not just noise. Line movement confirms the model's direction matters.

**Fire rule:**
- `13.0 <= edge_pct < 20.0`
- `abs(opener_move) >= 0.5`
- `market_type in ('SPREAD', 'TOTAL')`
- Stake forced to **5u** (same as full-edge picks)

**Shadow variant:** `CLV_MICRO_EDGE_BORDERLINE` logs (but doesn't fire) picks
in the 0.25–0.5 move bucket — forward sample growth for future threshold tuning.

**Infrastructure dependencies:**
- `bets.opener_line` + `bets.opener_move` columns (v25.80 migration)
- AFTER INSERT trigger `bets_populate_opener_move` auto-computes on every new
  bet (pure SQL, no Python writer changes).
- Helper `_compute_opener_move_for_pick(conn, p)` in `main.py`.

**Kill-switch thresholds:**
- WR < 45% at n ≥ 10, OR P/L ≤ -15u at n ≥ 15

**Coverage gap:** Props + moneyline are NOT in scope — props use `prop_snapshots`
which doesn't join to `openers` yet, and ML has no "line" to move. Separate
future investigation for a prop-side version.

**Status:** Live as of 2026-04-23. n=0 graded.

---

### J. PROP_CAREER_FADE (v25.87, shipped 2026-04-24)

**Side-type:** `PROP_CAREER_FADE`
**File:** `player_prop_model.py` (~line 1424)

**What it does:** NBA prop fade. When books collectively price an OVER line
≥ 1.0 below the player's career (3‑season weighted) average, that's the
market signaling current‑situation decline. Flip OVER → UNDER at the best
NY‑legal book at 5u.

**Distinct from PROP_FADE_FLIP:** PROP_FADE_FLIP measures our model vs market.
PROP_CAREER_FADE measures market vs career history.

**v25.92 (2026-04-27):** best‑line routing — picks the highest UNDER × best
odds across NY‑legal books, not the source OVER's book/line.

**v25.93 (2026-04-28):** recency veto — blocks the fade when the player's
last‑10 box‑score average exceeds the market median. Prevents fading active
producers whose career number is inflated by prime years (Allen 4/27 lost
with L10=11.7 vs line 8.5; Clarkson 4/28 would have lost with L10=6.5 vs
line 5.5).

**v25.94 supersedes for sub‑12 players:** PROP_PLAYOFF_ROLE_GATE hard‑blocks
the OVER iteration before the fade can fire — Vucevic/Clarkson never reach
the fade step. v25.93 still covers the above‑12 declining‑vet cohort.

**Status:** n=2 graded (Johnson WIN, Allen LOSS) +0.83u. Small.

---

### K. RAW_EDGE_FLIP (v25.95, shipped 2026-04-28)

**Side-type:** `RAW_EDGE_FLIP`
**File:** `pipeline/per_game.py` (end of TOTAL block, ~line 1390)

**What it does:** TOTAL‑market only. When an edge‑model TOTAL pick has raw
edge (`model_prob − implied_prob`) ≥ 30%, the model is in structural
overconfidence territory. v25.95 checks the Context Model direction; if
Context disagrees with the original pick, replace with opposite‑side pick at
the best NY‑legal book within `MIN_ODDS=‑150` and `MAX_PROP_ODDS=140` bounds.
If Context agrees → fire the original (corroboration is real signal).

**Why it works:** `edge_pct` is **capped at 20%** in storage, masking
calibration failures. Above 30% raw, model claimed 81% WR with actual 35%.
Fade flip = (1 − model_prob) almost exactly — directional inversion.

**Cross‑sport.** Distinct from CONTEXT_DIRECTION_VETO (v25.52) which only
blocks and only on a sport whitelist that excludes NCAA baseball — the
biggest 30%+ raw‑edge cohort.

**Backtest 2026-04-15 to 2026-04-28 (n=24, units≥3.5):**
- FOLLOW: 8‑15‑1, 35% WR, ‑38.4u
- FADE FLIP: 15‑8‑1, 65% WR, +28.6u
- Δ vs FOLLOW: +67.0u
- Sport mix: NCAA BB (12), MLB (5), NHL (6), Serie A (1)

**Kill‑switch:** WR < 50% at n ≥ 15 → demote to block‑only and revisit.

**Status:** Live 2026-04-28. n=0 graded.

---

## 3. Gates — things that BLOCK or modify picks

These don't generate picks; they filter them.

### Concentration caps (`main.py`)

- **MAX_SHARP_PICKS = 6** — max 6 picks total in sharp markets (NBA, NHL, EPL,
  LaLiga, MLB) per run.
- **MAX_SOFT_PICKS = 10** — max 10 in soft markets (NCAAB, MLS, college baseball,
  tennis, Ligue 1, Bundesliga) per run.
- **MAX_PER_SPORT_SOFT = 5** — max 5 from any single soft sport.
- **MAX_PER_SPORT_DIRECTION = 4** — max 4 same-direction (all OVER or all UNDER)
  per sport per day. Prevents correlation risk.
- **GAME_CAP** — max 1 game-line pick per event. Stacking spread + total
  on the same game is too correlated. Props exempt.
- **MAX_CONTEXT_PER_SPORT_DAILY = 5** (v25.67, shipped today) — Context CONTEXT_STANDALONE
  picks limited to 5 per sport per day.

### PROP_DIVERGENCE_GATE

**What:** Blocks prop picks where model projection diverges from market median
by more than a per-stat threshold (3.0 pts, 1.5 ast, 1.5 reb, etc.).

**Why:** When the model disagrees wildly with the market's consensus, the
model is usually wrong, not the market.

### HARD_VETO_DK_NCAA_BB_UNDERS (v25.56)

**What:** Blocks ANY NCAA Baseball UNDER pick at DraftKings.
**Why:** 9-18 record post-rebuild, -51u. Sharpening threshold wasn't enough —
DK's NCAA UNDER lines are structurally bad for us.

### SHARP_OPPOSES_BLOCK (v25.35)

**What:** Blocks picks where sharp-book line movement is AGAINST our direction.
**Scope:** NHL + NCAA BB only. We tested extending to MLB today and found MLB
would actually lose money blocked (MLB sharp-opposes is 2-1 +3.56u).

### CONTEXT_DIRECTION_VETO (v25.52)

**What:** If Context Model's direction disagrees with a pick the edge engine
wants to fire, veto the pick. Context is primary brain; edge picks should
defer when they conflict with Context.
**Exempt:** Fade-flip, Context own-picks, arb picks (they have their own logic).

### PROP odds-bucket gate (v25.66, shipped today)

**What:** Blocks prop picks at odds in the losing buckets: `-120 to -116`,
`-109 to -101`, `+100 to +120`. Keeps `-150 to -121`, `-115`, `-151 or
tighter`, `+121 to +140`.
**Why:** Inverse backtest found prop odds distribution is bimodal — middle
zones lose money at scale.

### Soccer Context CONTEXT_STANDALONE direction rules (v25.65, shipped today)

**What:** Per-league × direction rules for soccer DATA_TOTAL:
- Serie A UNDER @ 0.30: FOLLOW
- Ligue 1 UNDER @ 0.50: FOLLOW
- MLS UNDER: BLOCK (known fade cohort)
- EPL UNDER: BLOCK (known fade cohort)
- All soccer OVER + other: SHADOW (log, don't fire)

### LINE_AGAINST_GATE (v25.80, shipped 2026-04-23)

**What:** Blocks game-line picks at 20%+ edge where the consensus line has
already moved **≥ 0.5 AGAINST our side** between opener and fire. SPREAD/TOTAL
only.

**Why:** Historical cohort analysis (2026-04-23, n=47 on SPREAD/TOTAL) showed
this subset lost -31.7u — concentrated in NCAA baseball (-24.9u on 29),
DraftKings (-22.5u on 10), Caesars (-19.0u on 8). Sharp lines moving against
us before we fire = we're buying stale-favorable prices that the market has
already corrected. Only 8 of 47 overlap existing v25.35 SHARP_OPPOSES_BLOCK
(which applies to Steam context tags on NHL+NCAAB only), so 39 net-new blocks.

**Rule:**
- `edge_pct >= 20.0`
- `opener_move <= -0.5`
- `market_type in ('SPREAD', 'TOTAL')`
- NOT in exempt `side_type` list: `SPREAD_FADE_FLIP`, `PROP_FADE_FLIP`,
  `DATA_SPREAD`, `DATA_TOTAL`, `BOOK_ARB`, `PROP_BOOK_ARB`, `FADE_FLIP`,
  `PROP_CAREER_FADE`, `RAW_EDGE_FLIP`.
  Those channels intentionally bet against market movement and have their own
  logic.

**Log entry format:** `LINE_AGAINST_GATE (edge=X.X%, opener_move=-X.XX)` in
`shadow_blocked_picks.reason`.

**File:** `main.py` (end of `_passes_filter` in `_merge_and_select`, ~line 3608).

### Tennis ML grader fallback (v25.82, shipped 2026-04-23)

**What:** Adds a fallback path in `grader.py` for tennis MONEYLINE selections.

**Why:** Tennis ML selection format is "Player Name ML" — no `@` separator —
so the existing home/away lookup never matched. Previously **no tennis ML bet
had ever been graded**; they stayed PENDING or were scrubbed.

**How:** If the event_id match fails AND the `@`-split lookup fails AND it's
tennis MONEYLINE, extract the player name (strip " ML", " (cross-mkt)") and
search `results` where `sport=?  AND (home=?  OR  away=?)` within ±1 day of
bet date. Additive — existing paths unchanged.

---

## 4. Shadow / monitoring factors (`shadow_factors.md`)

Context adjustments that are still calculated and logged but zeroed out
so they don't influence picks. Tagged `[SHADOW]` in context_factors string.

Currently shadowed:
1. Home fast-paced
2. Away bounce-back
3. Altitude
4. Home hot streak
5. Away revenge game
6. Away letdown spot
7. NHL Away fast-paced (v25.62, shipped today)

---

## 5. Supporting models (feed into above)

- **Pitcher scraper** (`pitcher_scraper.py`) — MLB/NCAA pitcher rotations, ERA
  blending (recent + season), day-of-week adjustments. Feeds context model.
- **Weather engine** (`weather_engine.py`) — OpenWeatherMap calls for outdoor
  games. Wind, precip, temp. Feeds context model's total adjustment.
- **Referee engine** (`referee_engine.py`) — NBA/NHL official tendencies
  (whistle rate, total points). Feeds context model.
- **Injury engine** — ESPN injuries feed. Player-level impact scoring.
- **Steam signal tracker** — detects sharp book line moves vs soft book moves.
- **Power ratings** (`elo_engine.py` output) — team strength rating.
- **Goalie form** — NHL goalie last-5 GAA / SV% blended.

---

## 6. Common terms

- **Post-rebuild / public record** — stats since 2026-03-04 with units ≥ 3.5.
  This is what goes on the website.
- **CLV** — Closing Line Value. How much the line moved in our favor (positive)
  or against us (negative) after we bet. Positive CLV = sharp; we got a better
  number than the closer. Doesn't guarantee win but correlates.
- **Gap / disagreement** — how far Context's projection is from market (for
  CONTEXT_STANDALONE picks). Large gap = high conviction.
- **Fade vs Follow** — fade = bet the OPPOSITE of the model's direction;
  follow = bet the same direction.
- **Shadow** — a factor/cohort that's computed and logged but doesn't
  influence firing. Used to collect data without risking bankroll.

---

## 7. Version history index

### 2026-04-22 cycle
- **v25.61** — Code auditor fixes; db-latest GitHub release 404 fixed
- **v25.62** — NHL Away fast-paced shadow (sport-gated pace adjustment)
- **v25.63** — Soccer Context CONTEXT_STANDALONE halted (same-day reversed)
- **v25.64** — Playoff series momentum capped at ±15 points
- **v25.65** — Soccer Context CONTEXT_STANDALONE re-enabled with direction rules
- **v25.66** — PROP odds-bucket gate (bimodal odds calibration)
- **v25.67** — Context CONTEXT_STANDALONE daily per-sport cap (max 5)
- **v25.68** — cmd_predict auto-detects tennis + tennis 5am scheduler
- **v25.69** — DATA_SPREAD dominance tagging (observability)
- **v25.70** — DATA_SPREAD CONTEXT_STANDALONE killed; DATA_TOTAL CONTEXT_STANDALONE retained; ELO_DIVERGENCE_RESCUE intact

### 2026-04-23 cycle (continued)
- **v25.83** — Line trajectory Layer 1 (SHAPE): `bets.late_move_share`, `n_steps`, `max_overshoot`. Helper at `scripts/line_trajectory.py`. cmd_grade backfill hook. Briefing surfaces stable vs drift cohorts.
- **v25.84** — Line trajectory Layer 2 (ORIGINATOR): per-book detection. `bets.originator_book`, `move_breadth`, `sharp_movers`, `soft_movers`, `sharp_soft_divergence`, `move_class`. Classifies each move as STABLE / SHARP_LEAD / SOFT_LEAD / STEAM / DIVERGENT / MIXED. Helper at `scripts/per_book_trajectory.py`. Sharp = FanDuel + BetRivers; Soft = DK + MGM + Caesars + Fanatics + ESPN BET. Same nightly backfill hook in cmd_grade as v25.83.

### 2026-04-23 cycle
- **v25.71** — `shadow_blocked_picks.reason_category` + `reason_detail` typed columns; AFTER INSERT trigger auto-populates from free-text `reason`; 13,096 rows backfilled. Unlocks clean gate-counter queries.
- **v25.74–v25.75** — Odds archive moved to separate `betting_model_archive.db` file to keep main DB query-hot.
- **v25.79** — Tiered storage: `props` + `prop_snapshots` also archived to `betting_model_archive.db`. Moved 5.79M rows of `prop_snapshots_archive` out of main DB. `scripts/archive_db.py` helper for backtests.
- **v25.80** — `bets.opener_line` / `bets.opener_move` columns. Additive trigger. Two new channels live:
  - **CLV_MICRO_EDGE** — fires 13-20% edge picks with `|opener_move| >= 0.5`
  - **LINE_AGAINST_GATE** — blocks 20%+ edge picks with `opener_move <= -0.5`
- **v25.81** — Tennis Elo historical backfill (Jeff Sackmann 2023-2024 ATP+WTA). Clay ratings: 8→114 strong ATP players, 3→98 WTA. Madrid players now have real clay Elo instead of default 1500. Fixes why tennis picks were being DIVERGENCE_GATE'd to zero.
- **v25.82** — Grader fallback for tennis MONEYLINE selection format ("Player Name ML"). No tennis ML had ever graded before today; this fixes future grading.

## 8. Unused strategies (wishlist — known-valuable, not built)

Industry-standard betting strategies we could add without invention — just implementation.

### 🔥 Reverse Line Movement (RLM)
Public bets 75% on Team A, line moves toward Team B. Sharp money overpowered public. Fade the public.
**Requires:** public-betting-% data (Action Network or VSiN API, ~$50/mo).
**Effort:** 2-3 days + data cost. High value.

### 🔥 MLB umpire tendencies
Specific umpires call wider/tighter strike zones, shifting totals by 3-5% WR. Industry-proven edge.
**Status:** We have `referee_engine.py` for NBA/NHL but nothing for MLB umpires.
**Effort:** 1 week — umpire scraper + tendency model + integration. High value for baseball.

### 🟡 Key number exploitation
Basketball lines at 3 and 7 (or 2/3 in halves), NHL at 1.5 (puck-line), NFL at 3/7. Crossing a key number is way more valuable than a half-point at 5.5 → 6.0.
**Status:** Our Elo naturally produces some key-number picks but we don't explicitly exploit.
**Effort:** Light (analyze existing picks for key-number asymmetry). Moderate value.

### 🟡 Middling / scalping
Bet both sides at different books after line moves to lock small profit. Full middle = both sides hit = huge win.
**Status:** BOOK_ARB opens the door; full middling needs real-time line tracking.
**Effort:** Medium. Requires "opportunity" tracking after first bet placed.

### 🟡 First-half / First-quarter markets
Half-game and quarter-game totals/spreads sometimes have specific edges (hot/cold starters, teams that fade late, etc.).
**Status:** We only bet full-game markets.
**Effort:** New model for partial-game lines. Moderate.

### 🟢 Alt line scanning
If main total is Over 8.5, book also offers Over 8 (-170) and Over 9 (+130). Asymmetries across alt lines can signal sharp action.
**Status:** We don't query alt lines.
**Effort:** API coverage + new channel. Niche edge.

When adding a new strategy, **check this section first** — it may already be on the wishlist with cost/value estimates.

---

## 9. Key decisions made 2026-04-22

- **Kill DATA_SPREAD CONTEXT_STANDALONE** — 90d backtest showed Context absolute error worse than market, optimal scaling 0%, cannot find threshold tuning or gate addition that restores profitability. Preserving code scaffolding for future re-enable.
- **Keep DATA_TOTAL CONTEXT_STANDALONE** — totals backtest +101u FOLLOW on n=133 holds up across 7 sports; architectural asymmetry (totals additive, markets less efficient than spreads).
- **Keep ELO_DIVERGENCE_RESCUE (SPREAD_FADE_FLIP + Context veto on edge picks)** — v25.60 veto saved +20u in Case B (both models agree); Case A (fade flip) mixed but user preference to retain pending more live data.
- **Tennis Elo validated** — 3,376 matches / 70-73% winner-pick accuracy across tour/surface; green-lit via 5am dedicated schedule.

---

**Last updated:** 2026-04-22 end-of-day
