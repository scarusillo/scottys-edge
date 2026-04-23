# Scotty's Edge — Pick-Generating Channels

**Purpose:** Single reference for every channel that can fire a pick. What it
does, how it finds edge, how it interacts with others, gates protecting it,
and live win record.

**Last updated:** 2026-04-22 (v25.67 shipped)

---

## Live win record summary (post-rebuild, units ≥ 3.5)

| Channel (side_type) | N | W-L | P/L | CLV | Status |
|---|---|---|---|---|---|
| **DOG** (edge SPREAD dog) | 73 | 43-30 | +25.51u | +0.48% | ⭐ Best channel by volume |
| **PROP_FADE_FLIP** | 6 | 5-1 | +13.58u | +0.60% | ⭐ Best per-pick, tiny n |
| **PROP_UNDER** | 8 | 5-3 | +6.19u | +0.49% | Working |
| **OVER** (edge TOTAL over) | 101 | 54-44 | +4.47u | +0.33% | Working |
| **UNDER** (edge TOTAL under) | 125 | 68-56 | +3.90u | +0.23% | Working |
| **PROP_OVER** (edge prop) | 43 | 21-22 | +1.21u | +0.67% | Juice-bleed breakeven |
| **PROP_BOOK_ARB** | 3 | 2-1 | +3.17u | 0% | n too small |
| **SPREAD_FADE_FLIP** | 1 | 1-0 | +4.55u | 0% | n too small |
| **BOOK_ARB** (game line) | 2 | 1-1 | -0.45u | 0% | n too small (TOR UNDER 224.5 was a BOOK_ARB win — graded as SPREAD_FADE_FLIP tag per bet 973) |
| **FAVORITE** (edge SPREAD fav) | 22 | 12-10 | -1.20u | +0.43% | Underperforming |
| **DATA_SPREAD** (Context CONTEXT_STANDALONE) | 0 graded | — | — | — | First 2 live fires scrubbed today |
| **DATA_TOTAL** (Context CONTEXT_STANDALONE) | 0 graded | — | — | — | First fires today — 9 scrubbed for tomorrow's review |

---

## Channel 1 — Edge OVER (TOTAL)

**What it does:** Fires OVER a game's total line when model projects more
combined points/runs/goals than market is pricing.

**How it finds edge:** `(model_total - market_total)` → implied prob → edge %.
Requires edge ≥ 20% (some sports 8-15%).

**Works with:** Context Model adjusts `model_total` via pace/form/weather/park.
PROP_DIVERGENCE_GATE does NOT affect game totals (props only).

**Gates applied:**
- `MIN_EDGE_PCT` per sport (8-15%)
- Concentration cap: max 1 per event, max 4 same-direction per sport
- Direction veto from Context Model if Context disagrees (v25.52)
- HARD_VETO_DK_NCAA_BB_UNDERS blocks this channel's NCAA UNDER at DK

**Record:** 101 picks, 54-44, +4.47u, CLV +0.33% ✅

---

## Channel 2 — Edge UNDER (TOTAL)

**What it does:** Mirror of OVER — fires UNDER when model projects a lower total.

**How it finds edge:** Same edge math as OVER.

**Works with:** Same as OVER.

**Gates:** Same as OVER, plus HARD_VETO_DK_NCAA_BB_UNDERS specifically blocks
this channel on DraftKings NCAA baseball (known -51u cohort).

**Record:** 125 picks, 68-56, +3.90u, CLV +0.23% ✅

---

## Channel 3 — Edge DOG (SPREAD underdog)

**What it does:** Fires the underdog side of a spread when model thinks the
market overestimates the favorite.

**How it finds edge:** Model spread vs market spread → win probability at the
line → edge vs market-implied probability.

**Works with:** Context Model adjusts model_spread. Elo provides base rating.
SPREAD_FADE_FLIP can fire the SAME dog from a different mechanism (when Elo
is extreme and Context agrees market is right).

**Gates:**
- `MIN_ODDS = -150` (block very heavy DOGs where juice eats profit)
- MAX spread — rejects lines beyond sport's typical spread range
- Concentration + direction caps
- Context direction veto

**Record:** **73 picks, 43-30, +25.51u, CLV +0.48% ⭐** — best volume channel.

---

## Channel 4 — Edge FAVORITE (SPREAD favorite)

**What it does:** Fires the favorite side when model projects even more
favorite than market.

**How it finds edge:** Same math as DOG, opposite side.

**Works with:** Same as DOG.

**Gates:** Same.

**Record:** 22 picks, 12-10, -1.20u, CLV +0.43% ⚠️ Underperforming — favorites
need to win by more than the spread, variance eats edge at tighter lines.

---

## Channel 5 — Edge PROP_OVER (player prop over)

**What it does:** Fires OVER on player-stat props (points, rebounds, strikeouts,
etc.) when model projects more than market line.

**How it finds edge:** Hit-rate model + Poisson projection + blend → probability
of going over → edge.

**Works with:** Cross-book scanner compares lines at multiple books to find best
line. Box-score scraper feeds projection engine.

**Gates:**
- `MIN_EDGE_PCT = 10%` (lowered from 20% in v25.13 after recalibration)
- `MIN_PROP_ODDS = -150`, `MAX_PROP_ODDS = 140`
- **NEW v25.66 odds-bucket gate**: blocks `-120 to -116`, `-109 to -101`, `+100 to +120`
- `MIN_PROJ_LINE_SEP_STD = 0.3` — projection must differ from line by meaningful stdevs
- `PROP_DIVERGENCE_GATE` — if model vs market-median differs by > per-stat threshold, block
- PROP_EVENT_CAP — max 1 prop per game event
- PROP_STAT_CAP — caps on same-stat same-run props

**Record:** 43 picks, 21-22, +1.21u, CLV +0.67% ⚠️ Juice-bleed breakeven.
v25.66 odds gate should improve going forward.

---

## Channel 6 — Edge PROP_UNDER (player prop under)

**What it does:** Mirror of PROP_OVER for the UNDER side.

**How it finds edge:** Same math.

**Works with:** Same.

**Gates:** Same as PROP_OVER.

**Record:** 8 picks, 5-3, +6.19u, CLV +0.49% ✅ Higher per-pick EV than OVER.

---

## Channel 7 — DATA_SPREAD (Context Model CONTEXT_STANDALONE spreads) — **DISABLED v25.70**

**Status (2026-04-22):** Channel killed via v25.70. `CONTEXT_PATH2_THRESHOLDS` dict in model_engine.py is empty; no sports fire DATA_SPREAD picks. Code preserved for future re-enable if new methodology proves out.

**Why killed:** 90d backtest showed Context absolute error worse than market on every slice. Optimal scaling factor = 0%. NBA -5.65u, NHL -7.43u, Serie A +5.12u on n=61. Loss dive found Context pulls projections away from actual 70% of the time.

**What it did (historical record):**

When Elo and market AGREE on the spread, but Context Model
disagrees by more than a per-sport threshold, fire an OWN pick at market
line betting Context's direction.

**How it finds edge:** Starts from Elo, applies Context adjustments (injuries,
form, playoff momentum, HCA). If Context disagrees with market by:
- NBA: 2.5 pts
- NHL: 0.5 pts
- Serie A: 0.5 pts

...fire a pick at the line. `edge_pct = 0` (no traditional edge — fires on
model disagreement).

**Works with:** Context Model (context_model.py). Elo provides baseline.
SPREAD_FADE_FLIP fires when Elo disagrees with market AND Context agrees
with market — opposite trigger from DATA_SPREAD.

**Gates:**
- Per-sport disagreement threshold
- `MIN_ODDS` floor
- Concentration caps (v25.67: max 5 Context picks per sport per day)
- MAX_CONTEXT_PER_SPORT_DAILY (v25.67)
- v25.64 momentum cap (single playoff-game margin capped at ±15 pts)

**Record:** 0 graded live picks. 2 fires today (Magic +9, OKC -17) both scrubbed.

**Note:** Independent 90d backtest showed LOSING record (-5.65u NBA, -7.43u NHL)
but sample limited by 7-day odds retention. Phase A claimed +25u NBA / +73u NHL.
Leaving live pending 30 days real data.

---

## Channel 8 — DATA_TOTAL (Context Model CONTEXT_STANDALONE totals, v25.47-49)

**What it does:** Same as DATA_SPREAD but for totals. When Elo and market
agree but Context disagrees on the total, fire OWN pick at market line.

**How it finds edge:** Context total (from form + pace + weather + park
+ pitcher/goalie quality) vs market total. Fires at per-sport threshold.

**Current thresholds:**
- NBA: 0.30 pts
- NHL: 1.00 goals
- MLB: 1.50 runs
- Serie A UNDER: 0.30 goals (v25.65 FOLLOW)
- Ligue 1 UNDER: 0.50 goals (v25.65 FOLLOW)
- MLS UNDER, EPL UNDER: BLOCKED (v25.65 known fade cohorts)
- All soccer OVER + other: SHADOW-LOGGED (v25.65 insufficient data)

**Works with:** Context Model. BookArb gate, concentration caps.

**Gates:**
- Per-sport × direction rule (v25.65 for soccer)
- `MIN_ODDS` floor
- `MAX_CONTEXT_PER_SPORT_DAILY = 5` (v25.67)

**Record:** 0 graded live picks (first fires today). 9 fires today all scrubbed
for review. Expected to deliver signal in MLB and proven soccer UNDER cohorts.

---

## Channel 9 — SPREAD_FADE_FLIP (v25.36, v25.60)

**What it does:** When Elo diverges from market significantly AND Context
ALSO disagrees with Elo, fire the OPPOSITE of Elo (betting the market's side).

**How it finds edge:** When both our second-opinion models confirm Elo is
wrong, the market is more accurate than Elo. Bet the market side.

**Works with:** Elo and Context. Replaces what would otherwise be a high-edge
Elo pick that ELO_DIVERGENCE_RESCUE would have vetoed.

**Gates:**
- Elo divergence ≥ `max_div` threshold
- Context agreement with market (dual-model veto v25.60)
- `MIN_ODDS` floor
- 5u stake cap
- v25.64 momentum cap

**Record:** 1 live pick graded, 1-0, +4.55u (this was TOR UNDER bet 973 — it
was flagged BOOK_ARB too; side_type ended up SPREAD_FADE_FLIP due to grader logic).

**Backtest claim:** +140u at launch (v25.36). Not re-validated.

---

## Channel 10 — PROP_FADE_FLIP (v25.31)

**What it does:** Prop version of SPREAD_FADE_FLIP. Model projects a prop
number very different from market median AND market median disagrees with
our direction. Flip to the OVER/UNDER side market supports.

**How it finds edge:** When model and market both agree we have edge but on
opposite sides of the line, take the market's direction at a favorable number.

**Example:** Model projects Maxey 21.9 pts, market line 25.5, market median
26.5. Model says UNDER but market median supports OVER. Flip to OVER 25.5
(the market direction at the easier number).

**Works with:** PROP_DIVERGENCE_GATE is the detection mechanism. Cross-book
median comparison feeds the flip logic.

**Gates:**
- Prop divergence threshold per stat
- `MIN_ODDS` floor
- 3.5u cap (lower than standard 5u)
- v25.66 odds-bucket gate

**Record:** 6 picks, 5-1, +13.58u, CLV +0.60% ⭐ Best per-pick EV. n=6 is small.

---

## Channel 11 — BOOK_ARB (game line)

**What it does:** Two books open a game at different lines (e.g., FanDuel
opens total at 223.5, DraftKings at 224.5). The gap is a market inefficiency.
Bet the better side at the softer book.

**How it finds edge:** Opener comparison. Gap ≥ per-sport threshold:
- NCAA Baseball: 2.0 opener gap (v25.25)
- NBA totals: 1.0 (v25.28)
- NHL + MLB spreads: 1.5 (v25.28)

**Works with:** Sharp-line definition (FanDuel, Pinnacle-style books) vs
soft-line (DraftKings tends to be soft on some markets). Requires opener
data capture from 8am pipeline.

**Gates:**
- Opener gap threshold
- 3.5-5u stake cap depending on variant
- Must be NY-legal book
- `MIN_ODDS` floor

**Record:** 2 picks graded, 1-1, -0.45u. TOR UNDER 224.5 (bet 973) was a
BOOK_ARB win — shows under SPREAD_FADE_FLIP tag in DB due to v25.4 grader
resilience fix.

---

## Channel 12 — PROP_BOOK_ARB (v25.31)

**What it does:** Same as BOOK_ARB but for player props. Cross-book disagreement
on player lines creates arbitrage-style opportunities.

**How it finds edge:** Find player prop priced at different lines across books.
Bet the softer book's line at the side sharp book supports.

**Works with:** Cross-book prop scanner (props_engine.py). MLB batter stats
excluded (main-line overlap detection not yet built).

**Gates:**
- `MIN_ODDS` floor
- v25.66 odds-bucket gate
- Sport scope (MLB batter excluded)

**Record:** 3 picks, 2-1, +3.17u, CLV 0%. n=3 too small.

---

## How channels interact (the decision flow)

Rough order of evaluation in `model_engine.py`:

1. **Elo computes baseline spread/total** for every game
2. **Context computes adjusted spread/total** via all adjustments
3. **Elo-divergence check**:
   - If |Elo - market| > `max_div`: high-edge pick territory
     - Context ELO_DIVERGENCE_RESCUE vetoes IF Context agrees with market (Elo is wrong)
     - SPREAD_FADE_FLIP fires IF Context confirms Elo is wrong (bet market side)
   - If Elo ≈ market: normal edge flow
     - Edge pick fires IF edge ≥ threshold
     - DATA_SPREAD fires IF Context disagrees with both by ≥ sport threshold
     - DATA_TOTAL fires IF Context total disagrees by ≥ sport threshold
4. **Player prop scanner runs independently** (not spread/total gated)
   - PROP_OVER / PROP_UNDER at edge threshold
   - PROP_FADE_FLIP when divergence + direction disagreement
   - PROP_BOOK_ARB on cross-book gaps
5. **Book arb scanner runs independently** for game lines
6. **All picks pass through cap/veto gates** before final list
7. **Final list goes to email / Discord / IG**

---

## Gate summary (non-channel-specific)

**Concentration caps** (`main.py`):
- `MAX_SHARP_PICKS = 6`, `MAX_SOFT_PICKS = 10` per run
- `MAX_PER_SPORT_SOFT = 5`, `MAX_PER_SPORT_DIRECTION = 4`
- `GAME_CAP` — max 1 game-line pick per event
- `MAX_CONTEXT_PER_SPORT_DAILY = 5` (v25.67)

**Direction / divergence gates:**
- `PROP_DIVERGENCE_GATE` — model vs market-median prop threshold
- `CONTEXT_DIRECTION_VETO` (v25.52) — edge picks must agree with Context direction
- `SHARP_OPPOSES_BLOCK` (v25.35) — block picks where sharp-book line moved against us (NHL + NCAA BB only)

**Odds floors/ceilings:**
- `MIN_ODDS = -150` game lines
- `MIN_PROP_ODDS = -150`, `MAX_PROP_ODDS = 140`
- Prop losing-buckets blocked v25.66

**Sport-specific hard blocks:**
- `HARD_VETO_DK_NCAA_BB_UNDERS` (v25.56) — blocks DK NCAA BB UNDER (-51u cohort)

**Shadow adjustments** (context_engine):
- Home fast-paced, Away bounce-back, Altitude, Home hot streak, Away revenge,
  Away letdown, NHL Away fast-paced (v25.62)

---

## Where each channel's code lives

| Channel | File | Approx line |
|---|---|---|
| Edge OVER/UNDER/DOG/FAV | `model_engine.py` | generate_predictions() |
| DATA_SPREAD | `model_engine.py` | ~2089 |
| DATA_TOTAL | `model_engine.py` | ~2723 |
| SPREAD_FADE_FLIP | `model_engine.py` | ~1980 |
| PROP_OVER/UNDER | `player_prop_model.py` | scan_props_for_game() |
| PROP_FADE_FLIP | `player_prop_model.py` | prop divergence + flip logic |
| BOOK_ARB (game) | `model_engine.py` | book_arb scanner |
| PROP_BOOK_ARB | `player_prop_model.py` / `props_engine.py` | cross-book scanner |

---

**Reference this file any time you forget what a side_type means or what gates
are protecting it.** Channels will evolve; update this doc when shipping a new
version or halting an existing one.
