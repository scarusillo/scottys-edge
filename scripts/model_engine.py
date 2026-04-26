"""
model_engine.py v9 — Scotty's Edge

Integrates the complete edge framework inspired by from "Gambler":
  - Key number point value system for edge calculation
  - Star system for bet sizing (0.5-3.0 stars)
  - Vig-adjusted spread calculation
  - Spread vs Moneyline recommendation
  - Stack injury multiplier (exponential, not linear)
  - Cross-zero penalty
  - Bet timing guidance (favorites early, dogs late)
  - 90/10 power rating update formula
"""
import sqlite3, math, os
from datetime import datetime, timedelta, timezone

# v26.0 Phase 7 final: date helpers moved to pipeline.dates.
from pipeline.dates import EASTERN, _to_eastern, _eastern_tz_label  # noqa: F401

# v26.0 Phase 7: sport-specific helpers moved to pipeline.sport_adjustments.
# Re-exported here so `from model_engine import X` keeps working.
from pipeline.sport_adjustments import (  # noqa: F401
    _tennis_surface_from_sport,
    _tennis_h2h_adjustment,
    _get_dynamic_league_avg_total,
    _weighted_team_stats,
    _mlb_pitcher_era_adjustment,
    _mlb_park_factor_adjustment,
    _mlb_bullpen_adjustment,
    _nhl_goalie_adjustment,
    estimate_model_total,
    _totals_confidence,
    calculate_point_value_totals,
    _total_prob,
    _divergence_penalty,
)
# v26.0 Phase 7 (continued): more helpers moved out of model_engine.py.
from pipeline.log_helpers import (  # noqa: F401
    _log_park_veto,
    _log_divergence_block,
)
from pipeline.pick_factory import (  # noqa: F401
    _mk,
    _mk_ml,
    _conf,
)
from pipeline.persistence import (  # noqa: F401
    save_picks_to_db,
    _ensure_bet_columns,
    _classify_side,
    _classify_spread_bucket,
    _classify_edge_bucket,
    _classify_market_tier,
)
from pipeline.display import (  # noqa: F401
    print_picks,
    picks_to_text,
    _render_sport_group,
)

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

from scottys_edge import (
    scottys_edge_assessment, calculate_point_value, get_star_rating,
    vig_adjusted_spread, bet_timing_advice,
    minimum_play_threshold,
    kelly_units, kelly_label,
)

# Context engine — schedule, travel, line movement, altitude, splits
try:
    from context_engine import get_context_adjustments, line_movement_signal
    HAS_CONTEXT = True
except ImportError:
    HAS_CONTEXT = False

# Elo engine integration (independent ratings from game results)
try:
    from elo_engine import get_elo_ratings, blended_spread, ELO_CONFIG
    HAS_ELO = True
except ImportError:
    HAS_ELO = False

# Pitcher context — day-of-week pitching quality + named starters
try:
    from pitcher_scraper import get_pitcher_context
    HAS_PITCHER = True
except ImportError:
    HAS_PITCHER = False

# MLB probable pitchers — gate MLB picks on confirmed starters
try:
    from pitcher_scraper import get_mlb_probable_starters
    HAS_MLB_PITCHERS = True
except ImportError:
    HAS_MLB_PITCHERS = False

# NHL probable goalies — gate NHL picks on confirmed starters
try:
    from pitcher_scraper import get_nhl_probable_goalies
    HAS_NHL_GOALIES = True
except ImportError:
    HAS_NHL_GOALIES = False

# Referee tendencies — dynamic adjustment from scraped ESPN data
try:
    from referee_engine import get_ref_adjustment
    HAS_REF = True
except ImportError:
    HAS_REF = False


SPORT_CONFIG = {
    'basketball_ncaab': {
        'logistic_scale': 7.5, 'spread_std': 13.0, 'home_court': 3.2,  # Was 6.3/11.0 — same as NBA but NCAAB has higher variance. 25%+ edge bucket was 1W-6L = fake edges.
        'max_spread_divergence': 4.5,   # v12 FIX: Was 6.0 — medium dogs (4-7.5 pts) went 2-8. 4.5 allows small dogs + favorites.
        'ml_scale': 7.5,  # Separate scale for moneyline win probability
    },
    'basketball_nba': {
        'logistic_scale': 6.3, 'spread_std': 11.0, 'home_court': 2.5,
        'max_spread_divergence': 4.0,   # Tightened from 5.0
        'ml_scale': 7.5,
    },
    'icehockey_nhl': {
        'logistic_scale': 0.49, 'spread_std': 2.2, 'home_court': 0.15,
        'max_spread_divergence': 2.5,   # v25.29: raised from 1.5. Puckline is rigid ±1.5;
                                         # any model spread >±3 goals is expected NHL divergence,
                                         # not model error. Backtest of 20 previously-blocked NHL
                                         # puckline picks at div 1.5-2.5: 17W-3L (85%), +27.4u at
                                         # 3.5u sizing. Picks in the 1.5-2.5 zone tagged DIV_EXPANDED.
        'ml_scale': 2.2,
    },
    'soccer_epl': {
        'logistic_scale': 0.40, 'spread_std': 1.3, 'home_court': 0.40,  # v12 FIX: Was 0.25 — massively undervaluing home teams, inflating away ML edges
        'max_spread_divergence': 1.0,  # v16: raised from 0.75 — was blocking all spread picks
        'ml_scale': 1.5,  # v13 FIX: Was 1.0 — too flat, underdogs kept ~50% win prob. Backtest: ML dogs 13W-34L (-8.0u). Steeper curve = realistic dog probs.
    },
    'soccer_italy_serie_a': {
        'logistic_scale': 0.40, 'spread_std': 1.3, 'home_court': 0.45,  # v12 FIX: Was 0.30 — Serie A has strong home advantage historically
        'max_spread_divergence': 1.0,  # v16: raised from 0.75 — was blocking all spread picks
        'ml_scale': 1.5,  # v13 FIX: Was 1.0
    },
    'soccer_spain_la_liga': {
        'logistic_scale': 0.40, 'spread_std': 1.3, 'home_court': 0.40,  # v12 FIX: Was 0.25
        'max_spread_divergence': 1.0,  # v16: raised from 0.75 — was blocking all spread picks
        'ml_scale': 1.5,  # v13 FIX: Was 1.0
    },
    'soccer_germany_bundesliga': {
        'logistic_scale': 0.40, 'spread_std': 1.3, 'home_court': 0.42,  # v12 FIX: Was 0.30 — Bundesliga has strong home support
        'max_spread_divergence': 1.0,  # v16: raised from 0.75 — was blocking all spread picks
        'ml_scale': 1.5,  # v13 FIX: Was 1.0
    },
    'soccer_france_ligue_one': {
        'logistic_scale': 0.40, 'spread_std': 1.3, 'home_court': 0.40,  # v12 FIX: Was 0.25
        'max_spread_divergence': 1.0,  # v16: raised from 0.75 — was blocking all spread picks
        'ml_scale': 1.5,  # v13 FIX: Was 1.0
    },
    'soccer_uefa_champs_league': {
        'logistic_scale': 0.40, 'spread_std': 1.3, 'home_court': 0.30,  # v12 FIX: Was 0.20 — UCL lower but still real
        'max_spread_divergence': 1.0,  # v16: raised from 0.75 — was blocking all spread picks
        'ml_scale': 1.5,  # v13 FIX: Was 1.0
    },
    'soccer_usa_mls': {
        'logistic_scale': 0.40, 'spread_std': 1.3, 'home_court': 0.45,  # v12 FIX: Was 0.35 — MLS travel distances = big home edge
        'max_spread_divergence': 1.0,  # v16: raised from 0.75 — was blocking all spread picks
        'ml_scale': 1.5,  # v13 FIX: Was 1.0
    },
    'soccer_mexico_ligamx': {
        'logistic_scale': 0.40, 'spread_std': 1.3, 'home_court': 0.50,  # Liga MX has strong home advantage (altitude, travel)
        'max_spread_divergence': 1.0,
        'ml_scale': 1.5,
    },
    # ── BASEBALL ──
    # Calibrated from Elo MAE = 6.2 runs (spread_std must exceed this)
    # NBA ratio: MAE/std = 4/11 = 0.36. Baseball: 6.2/10 = 0.62 (conservative)
    # Run lines are ±1.5 (variable juice). Primary value = MONEYLINES.
    'baseball_ncaa': {
        'logistic_scale': 1.8,    # Baseball: tighter scale, fewer blowouts than basketball
        'spread_std': 10.0,       # Calibrated: Elo MAE=6.2, match NBA conservatism ratio
        'home_court': 0.4,        # v14: Actual home win rate 65.6% — Elo home_advantage handles this
        'max_spread_divergence': 5.0,  # Was 2.0 — baseball totals at high div are 21W-10L +40u. Model's best sport needs room.
        'ml_scale': 3.5,          # v12 FIX: Was 2.2 (way too steep). Real baseball: 1 run diff = ~57% win
    },
    # v17: MLB — Opening Day 2026-03-26. Same structure as NCAA baseball
    # with tighter settings (pro markets are sharper than college).
    'baseball_mlb': {
        'logistic_scale': 1.8,    # Same as NCAA — baseball scoring distribution is similar
        'spread_std': 8.0,        # Tighter than NCAA (10.0) — pro lines are sharper
        'home_court': 0.3,        # MLB home advantage ~54% (weaker than college 65%)
        'max_spread_divergence': 4.0,  # Tighter than NCAA (5.0) — pro market more efficient
        'ml_scale': 3.5,          # Same as NCAA
    },
}

# Dynamically add tennis tournament configs from config.py
try:
    from config import TENNIS_SPORTS, TENNIS_SURFACES
    _TENNIS_PARAMS = {
        'hard': {'logistic_scale': 2.5, 'spread_std': 5.0, 'home_court': 0.0,
                 'max_spread_divergence': 4.0, 'ml_scale': 2.5},
        'clay': {'logistic_scale': 2.5, 'spread_std': 5.5, 'home_court': 0.0,
                 'max_spread_divergence': 2.5, 'ml_scale': 2.5},  # v24: tightened from 4.5
        'grass': {'logistic_scale': 2.5, 'spread_std': 4.5, 'home_court': 0.0,
                  'max_spread_divergence': 3.5, 'ml_scale': 2.5},
    }
    for _tk in TENNIS_SPORTS:
        _surf = TENNIS_SURFACES.get(_tk, 'hard')
        SPORT_CONFIG[_tk] = dict(_TENNIS_PARAMS.get(_surf, _TENNIS_PARAMS['hard']))
except ImportError:
    pass

def spread_to_win_prob(spread, sport):
    """Win probability for MONEYLINE bets. Uses ml_scale (calibrated to real win rates)."""
    s = SPORT_CONFIG.get(sport, SPORT_CONFIG['basketball_nba']).get('ml_scale', 7.5)
    return 1.0 / (1.0 + math.exp(spread / s))

def spread_to_cover_prob(model_spread, market_spread, sport):
    """Cover probability for SPREAD bets. Uses spread_std."""
    std = SPORT_CONFIG.get(sport, SPORT_CONFIG['basketball_nba'])['spread_std']
    return _ncdf((market_spread - model_spread) / std)

def _ncdf(z):
    if z > 6: return 1.0
    if z < -6: return 0.0
    a1,a2,a3,a4,a5 = 0.254829592,-0.284496736,1.421413741,-1.453152027,1.061405429
    p = 0.3275911; sign = 1 if z >= 0 else -1
    t = 1.0/(1.0+p*abs(z))
    y = 1.0-(((((a5*t+a4)*t)+a3)*t+a2)*t+a1)*t*math.exp(-z*z/2)
    return 0.5*(1.0+sign*y)

def american_to_implied_prob(odds):
    if odds is None: return None
    return 100.0/(odds+100.0) if odds > 0 else abs(odds)/(abs(odds)+100.0)


def devig_ml_odds(home_odds, away_odds, draw_odds=None):
    """
    Remove vig from ML odds to get fair probabilities.
    
    Raw implied probs sum to >100% (the vig). De-vigging normalizes
    them to 100% so edge comparisons are against true market probability.
    
    Without this, dogs always look like value because vig inflates
    favorites more in absolute terms.
    
    Returns: (home_fair, away_fair, draw_fair) or (home_fair, away_fair, None)
    """
    h_imp = american_to_implied_prob(home_odds)
    a_imp = american_to_implied_prob(away_odds)
    d_imp = american_to_implied_prob(draw_odds) if draw_odds else None
    
    if h_imp is None or a_imp is None:
        return None, None, None
    
    total = h_imp + a_imp + (d_imp or 0)
    if total <= 0:
        return h_imp, a_imp, d_imp
    
    h_fair = h_imp / total
    a_fair = a_imp / total
    d_fair = d_imp / total if d_imp else None
    
    return h_fair, a_fair, d_fair

def get_latest_ratings(conn, sport):
    rows = conn.execute("""
        SELECT team, base_rating, home_court, rest_adjust, injury_adjust,
               situational_adjust, manual_override, final_rating
        FROM power_ratings WHERE sport = ? ORDER BY run_timestamp DESC
    """, (sport,)).fetchall()
    ratings, seen = {}, set()
    for r in rows:
        t = r[0]
        if t in seen: continue
        seen.add(t)
        if r[1] is None: continue
        hca = r[2] or SPORT_CONFIG.get(sport, {}).get('home_court', 2.5)
        override = r[6]
        final = override if override is not None else (r[1] + (r[3] or 0) + (r[4] or 0) + (r[5] or 0))
        ratings[t] = {'base': r[1], 'home_court': hca, 'final': final}
    return ratings

def compute_model_spread(home, away, ratings, sport):
    h, a = ratings.get(home), ratings.get(away)
    if not h or not a: return None
    hca = h.get('home_court', SPORT_CONFIG.get(sport, {}).get('home_court', 2.5))
    return round(a['final'] - h['final'] - hca, 2)

def _soccer_draw_prob(abs_spread):
    """Estimate draw probability in soccer based on model spread.
    
    v12 FIX: Old decay (0.55) made draws crash from 30% to 14% at spread 1.0.
    Real data: draws stay ~20% even in moderately lopsided matches.
    Calibrated against EPL 2020-2024:
      Even: ~26%, Sm fav: ~24%, Med fav: ~19%, Big fav: ~15%
    """
    return 0.30 * math.exp(-0.30 * abs_spread)


def soccer_ml_probs(model_spread, sport):
    """
    Calculate 3-way soccer probabilities: (home_win, draw, away_win).
    
    CRITICAL: Soccer has 3 outcomes, not 2. Without this adjustment,
    the model overestimates underdog win probability by 15-25% because
    draw probability gets assigned to the underdog instead.
    
    Example: Inter Milan vs Genoa (spread -1.5)
      Without draw fix: Home=65%, Away=35% → Genoa looks like great value at +1100
      With draw fix:    Home=71%, Draw=13%, Away=16% → Genoa correctly filtered out
    """
    raw_home = spread_to_win_prob(model_spread, sport)
    draw = _soccer_draw_prob(abs(model_spread))
    home_win = raw_home * (1.0 - draw)
    away_win = (1.0 - raw_home) * (1.0 - draw)
    return home_win, draw, away_win


def get_team_injury_context(conn, team, sport):
    """Return injury context: (injury_count, position_cluster_count, total_point_impact).

    v17 FIX: Now returns actual point_impact sum instead of discarding it.
    Previously returned (len, min(len,3)) — treating Cade Cunningham out
    the same as a 12th man. Now the impact values drive spread adjustment.

    Status weighting:
      Out/Doubtful: full impact
      Day-To-Day/Questionable: 50% impact (may play)
    """
    today = datetime.now().strftime('%Y-%m-%d')
    rows = conn.execute("""
        SELECT player, status, point_impact FROM injuries
        WHERE sport=? AND team=? AND report_date=?
        AND status IN ('Out','OUT','Doubtful','DOUBTFUL','Day-To-Day','DAY-TO-DAY',
                       'Questionable','QUESTIONABLE')
    """, (sport, team, today)).fetchall()

    total_impact = 0.0
    for player, status, impact in rows:
        pts = impact or 0.0
        if status.upper() in ('DAY-TO-DAY', 'QUESTIONABLE'):
            pts *= 0.5  # May play — discount impact
        total_impact += pts

    return len(rows), min(len(rows), 3), round(total_impact, 2)

# ── Totals model ──


def generate_predictions(conn, sport=None, date=None):
    """Generate game picks for one sport (sport=X) or all sports (sport=None).

    Returns un-merged picks. Use pipeline.orchestrator.run for the full
    end-to-end pipeline including merge.

    v26.0 Phase 7 (final): function body is pure orchestration — sport loop
    + per-sport scoring + post-loop processing — all behavior lives in
    pipeline modules:
      - pipeline.orchestrator.compute_game_window: build today-only window
      - pipeline.orchestrator.score_one_sport:     run all per-game stages
      - pipeline.post_process.*:                   context-conf + CLV + final filter
    """
    from pipeline.orchestrator import compute_game_window, score_one_sport
    from pipeline.post_process import (
        apply_context_confirmation, apply_clv_gate, apply_final_filter)

    sports = [sport] if sport else list(SPORT_CONFIG.keys())
    now_utc = datetime.now(timezone.utc)
    window_start, window_end = compute_game_window(now_utc)
    print(f"  Game window: TODAY ONLY — {window_start} to {window_end}")

    all_picks = []
    for sp in sports:
        picks, _stats = score_one_sport(conn, sp, now_utc, window_start, window_end)
        all_picks.extend(picks)

    apply_context_confirmation(all_picks)
    apply_clv_gate(all_picks, conn)
    return apply_final_filter(all_picks)
def update_ratings_post_game(conn, sport, home, away, home_score, away_score,
                             home_inj=0, away_inj=0, hfa=None):
    """90/10 update: New = 90% old + 10% TGPL. Auto-seeds new teams."""
    ratings = get_latest_ratings(conn, sport)
    h, a = ratings.get(home), ratings.get(away)
    if hfa is None: hfa = SPORT_CONFIG.get(sport, {}).get('home_court', 2.5)
    
    # Auto-seed unrated teams at 0.0 (neutral) so they enter the update cycle
    if not h:
        h = {'base': 0.0, 'home_court': hfa, 'final': 0.0}
    if not a:
        a = {'base': 0.0, 'home_court': hfa, 'final': 0.0}
    
    inj_d = home_inj - away_inj
    tgpl_h = (home_score - away_score) + a['final'] + inj_d - hfa
    tgpl_a = (away_score - home_score) + h['final'] - inj_d + hfa
    new_h = round(0.9 * h['final'] + 0.1 * tgpl_h, 3)
    new_a = round(0.9 * a['final'] + 0.1 * tgpl_a, 3)
    now = datetime.now().isoformat()
    for team, nr in [(home, new_h), (away, new_a)]:
        conn.execute("""INSERT INTO power_ratings (run_timestamp, sport, team,
            base_rating, home_court, final_rating, games_used, iterations,
            learning_rate, regularization)
            VALUES (?,?,?,?,?,?,1,1,0,0)""", (now, sport, team, nr, hfa, nr))
    conn.commit()
    return new_h, new_a

if __name__ == '__main__':
    conn = sqlite3.connect(DB_PATH)
    picks = generate_predictions(conn)
    print_picks(picks)
    conn.close()
