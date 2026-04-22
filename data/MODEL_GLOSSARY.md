# Scotty's Edge — Model & Channel Glossary

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

**Status:** Live. Used in multiple channels (see Path 1 / Path 2 below).

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

### B. Context Model Path 1 — divergence rescue

**Side-type:** `OVER`, `UNDER`, `DOG`, `FAVORITE` (same as edge-based)
**File:** `model_engine.py` around line 1925

**What it does:** When Elo disagrees with market by a LOT (big divergence),
normally the edge model would fire a high-edge pick. But extreme divergence
often means Elo is wrong. Path 1 checks Context. If Context agrees with
market (i.e., Elo is the outlier), the edge pick is BLOCKED. If Context
agrees with Elo, it fires.

**Example:** Elo says Celtics -15. Market has Celtics -7 (8-point divergence).
Normally edge fires at 20%. Context Model checks: Context says Celtics -8
(agrees with market, not Elo). Path 1 vetoes the pick — Elo was probably
wrong because of stale ratings.

**Status:** Live. Saved us from numerous bad high-edge fires. Not inverse-
backtested but the veto is a safety mechanism, not a pick generator, so
less urgent to re-validate.

---

### C. Context Model Path 2 TOTALS — `DATA_TOTAL`

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

### D. Context Model Path 2 SPREADS — `DATA_SPREAD` ← YOU ASKED ABOUT THIS

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
- **MAX_CONTEXT_PER_SPORT_DAILY = 5** (v25.67, shipped today) — Context Path 2
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

### Soccer Context Path 2 direction rules (v25.65, shipped today)

**What:** Per-league × direction rules for soccer DATA_TOTAL:
- Serie A UNDER @ 0.30: FOLLOW
- Ligue 1 UNDER @ 0.50: FOLLOW
- MLS UNDER: BLOCK (known fade cohort)
- EPL UNDER: BLOCK (known fade cohort)
- All soccer OVER + other: SHADOW (log, don't fire)

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
  Path 2 picks). Large gap = high conviction.
- **Fade vs Follow** — fade = bet the OPPOSITE of the model's direction;
  follow = bet the same direction.
- **Shadow** — a factor/cohort that's computed and logged but doesn't
  influence firing. Used to collect data without risking bankroll.

---

## 7. Version history index (today, 2026-04-22)

- **v25.61** — Code auditor fixes; db-latest GitHub release 404 fixed
- **v25.62** — NHL Away fast-paced shadow (sport-gated pace adjustment)
- **v25.63** — Soccer Context Path 2 halted (same-day reversed)
- **v25.64** — Playoff series momentum capped at ±15 points
- **v25.65** — Soccer Context Path 2 re-enabled with direction rules
- **v25.66** — PROP odds-bucket gate (bimodal odds calibration)
- **v25.67** — Context Path 2 daily per-sport cap (max 5)
- **v25.68 (pending)** — DATA_SPREAD Path 2 halt (backtest failed)

---

**Last updated:** 2026-04-22
